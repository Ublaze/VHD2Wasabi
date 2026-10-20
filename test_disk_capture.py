"""Whole-disk capture tests — synthetic GPT disk image, injected fake
physical-disk and shadow-volume readers. No VSS, no S3.

Layout (300 MiB):
  [0,      1 MiB)  preamble (partition table) — live
  [1,    101 MiB)  ESP-like FAT region         — live (not snapshot-capable)
  [101,  291 MiB)  NTFS-like region C:         — shadow (content differs from live!)
  [291,  300 MiB)  unallocated gap             — live
Asserts the streamed bytes come from the SHADOW for the NTFS region and from
the LIVE disk everywhere else, plus pipeline E2E (segments -> 7z -> merge ->
restore) round-trip, resume, and layout-change abort.
"""
import hashlib
import os
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "app"))

from engine import manifest as manifest_mod
from engine import pipeline as pipeline_mod
from engine import restore as restore_mod
from engine.diskreader import DiskReader, LayoutChangedError, compute_regions, \
    frozen_layout, layouts_equal
from engine.uploader import LocalDirStore
from engine.vhdfooter import parse_footer

MIB = 1024 * 1024
DISK_LEN = 300 * MIB
ESP = (1 * MIB, 100 * MIB)          # offset, length
NTFS = (101 * MIB, 190 * MIB)
GAP = (291 * MIB, 9 * MIB)


def build_images():
    """Returns (live_bytes, shadow_bytes_for_ntfs_region)."""
    live = bytearray(DISK_LEN)
    live[0:ESP[0]] = b"\xEB" * ESP[0]                                  # table
    live[ESP[0]:ESP[0] + ESP[1]] = b"\xF8" * ESP[1]                    # ESP live
    live[NTFS[0]:NTFS[0] + NTFS[1]] = b"\x4C" * NTFS[1]                # C: live
    live[GAP[0]:] = b"\x00" * GAP[1]                                   # gap
    shadow_ntfs = b"\x53" * NTFS[1]  # frozen-at-snapshot content
    return bytes(live), shadow_ntfs


PARTITIONS = [
    {"number": 1, "offset": ESP[0], "size": ESP[1], "letter": None, "fs": "FAT32",
     "gpt_type": "{C12A7328-F81F-11D2-BA4B-00A0C93EC93B}", "type": "EFI system partition"},
    {"number": 2, "offset": NTFS[0], "size": NTFS[1], "letter": "C", "fs": "NTFS",
     "gpt_type": "{EBD0A0A2-B9E5-4433-87C0-68B6B72699C7}", "type": "Basic data partition"},
]
DISK = {"number": 0, "model": "SYNTHETIC NVMe", "size": DISK_LEN, "style": "GPT",
        "partitions": PARTITIONS}


class FakeDevice:
    """read_at(pos, n) over a bytes object."""

    def __init__(self, data):
        self.data = data

    def read_at(self, pos, n):
        return self.data[pos:pos + n]

    def close(self):
        pass


def make_reader(live, shadow_ntfs, log=None, mutate_layout=None):
    """DiskReader with fake devices + meta as build_disk_reader would produce."""
    disk = dict(DISK)
    if mutate_layout:
        disk["partitions"] = mutate_layout([dict(p) for p in PARTITIONS])
    regions = compute_regions(disk["partitions"], disk["size"])
    devices = {"PHYS": FakeDevice(live), "SHADOW0": FakeDevice(shadow_ntfs)}
    for r in regions:
        if r["kind"] == "partition" and r.get("letter") == "C" and \
                (r.get("fs") or "").upper() == "NTFS":
            r["source"] = "shadow"
            r["device"] = "SHADOW0"
            r["snapshot_id"] = "{FAKE-SHADOW-1}"
        else:
            r["source"] = "live"
            r["device"] = "PHYS"
    meta = {
        "layout": frozen_layout(disk),
        "model": disk["model"],
        "style": disk["style"],
        "regions": [{"offset": r["offset"], "length": r["length"],
                     "kind": r["kind"], "source": r["source"],
                     "letter": r.get("letter"), "partition": r.get("partition"),
                     "snapshot_id": r.get("snapshot_id")} for r in regions],
        "snapshot_ids": ["{FAKE-SHADOW-1}"],
        "letters": ["C"],
        "cleanup": lambda: None,
    }
    reader = DiskReader(disk["size"], regions,
                        opener=lambda dev: devices[dev], log=log)
    return reader, meta


def expected_stream(live, shadow_ntfs):
    exp = bytearray(live)
    exp[NTFS[0]:NTFS[0] + NTFS[1]] = shadow_ntfs
    return bytes(exp)


def test_reader_stream():
    print("== disk reader stream assembly ==")
    live, shadow = build_images()
    reader, meta = make_reader(live, shadow)
    got = bytearray()
    for c in reader.read_chunks():
        got.extend(c)
    reader.close()
    assert len(got) == DISK_LEN, f"length {len(got)} != {DISK_LEN}"
    assert bytes(got) == expected_stream(live, shadow), "stream bytes wrong"
    # prove region provenance explicitly
    assert got[ESP[0]] == 0xF8, "ESP not from live disk"
    assert got[NTFS[0]] == 0x53, "NTFS region not from shadow"
    assert got[GAP[0]] == 0x00
    # region coverage sanity
    assert len(meta["regions"]) == 4
    assert [r["kind"] for r in meta["regions"]] == \
        ["preamble", "partition", "partition", "tail"]
    print("   stream == expected layout bytes (shadow/live mixed correctly)")


def test_e2e(tmp):
    print("== disk capture E2E (segment/merge/restore) ==")
    live, shadow = build_images()
    store = LocalDirStore(os.path.join(tmp, "bucket"))
    spool = os.path.join(tmp, "spool")
    os.makedirs(spool)
    manifest_mod.STATE_ROOT = os.path.join(tmp, "state")
    server = "DISKBOX"
    p = pipeline_mod.Pipeline(
        store, "dpfx", server, segment_size_mb=64, level=5, spool_dir=spool,
        disk_reader_factory=lambda n: make_reader(live, shadow),
        merge_on_complete=True,
        callbacks={"log": lambda m: print("  ", m)})
    state = p.run_disk(0)
    assert state["status"] == "done"
    assert state["capture_kind"] == "disk"
    assert state["disk_number"] == 0
    assert state["model"] == "SYNTHETIC NVMe"
    assert state["partition_style"] == "GPT"
    assert state["snapshot_ids"] == ["{FAKE-SHADOW-1}"]
    assert state["layout"]["style"] == "GPT"
    assert len(state["layout"]["partitions"]) == 2
    assert state.get("merged_key"), "disk merge missing"
    assert state["volume"] == "Disk0"  # restore naming compatibility

    dest = os.path.join(tmp, "restore")
    res = restore_mod.restore(store, state, dest, log=lambda m: print("   ", m))
    assert res["footer"], "footer validation failed"
    assert os.path.basename(res["path"]) == "Disk0.vhd"
    with open(res["path"], "rb") as f:
        data = f.read()
    assert len(data) == DISK_LEN + 512
    expect = expected_stream(live, shadow)
    assert data[:-512] == expect, "restored disk bytes differ"
    assert hashlib.sha256(data[:-512]).hexdigest() == hashlib.sha256(expect).hexdigest()
    info = parse_footer(data[-512:])
    assert info["current_size"] == DISK_LEN
    print(f"   restore byte-identical, footer OK ({res['path']})")
    return store, live, shadow, state


def test_resume(tmp, store, live, shadow, state):
    print("== disk resume from mid-disk ==")
    st = manifest_mod.load_state("DISKBOX", "Disk0")
    last = st["segments"].pop()
    os.remove(os.path.join(tmp, "bucket", last["key"].replace("/", os.sep)))
    st["status"] = "in_progress"
    st["image_sha256"] = None
    manifest_mod.save_state("DISKBOX", "Disk0", st)
    p = pipeline_mod.Pipeline(
        store, "dpfx", "DISKBOX", segment_size_mb=64, level=5,
        spool_dir=os.path.join(tmp, "spool"),
        disk_reader_factory=lambda n: make_reader(live, shadow),
        merge_on_complete=False,
        callbacks={"log": lambda m: print("  ", m)})
    s2 = p.run_disk(0)
    assert s2["status"] == "done"
    assert len(s2["segments"]) == len(state["segments"])
    for a, b in zip(state["segments"][:-1], s2["segments"][:-1]):
        assert a["raw_sha256"] == b["raw_sha256"]
    print(f"   resumed at segment {last['index'] + 1} only — OK")


def test_layout_change(tmp, store, live, shadow):
    print("== layout change on resume aborts ==")
    st = manifest_mod.load_state("DISKBOX", "Disk0")
    last = st["segments"].pop()
    st["status"] = "in_progress"
    manifest_mod.save_state("DISKBOX", "Disk0", st)

    def grown(parts):
        parts[1]["size"] += MIB  # NTFS partition grew
        return parts
    p = pipeline_mod.Pipeline(
        store, "dpfx", "DISKBOX", segment_size_mb=64, level=5,
        spool_dir=os.path.join(tmp, "spool"),
        disk_reader_factory=lambda n: make_reader(live, shadow,
                                                  mutate_layout=grown),
        merge_on_complete=False)
    try:
        p.run_disk(0)
    except LayoutChangedError as e:
        print(f"   aborted as expected: {str(e)[:80]}…")
        return
    raise AssertionError("layout change must abort")


def test_layout_equality():
    a = frozen_layout(DISK)
    b = frozen_layout(dict(DISK, partitions=[dict(p) for p in PARTITIONS]))
    assert layouts_equal(a, b)
    b["partitions"][0]["size"] += MIB
    assert not layouts_equal(a, b)
    print("frozen layout equality: OK")


SR = (1 * MIB, 50 * MIB)      # System Reserved-like: NTFS, no letter
C2 = (51 * MIB, 200 * MIB)    # C:
TAIL2_LEN = 300 * MIB - (51 * MIB + 200 * MIB)
PARTITIONS2 = [
    {"number": 1, "offset": SR[0], "size": SR[1], "letter": None, "fs": "NTFS",
     "gpt_type": None, "type": "MBR:0x27"},
    {"number": 2, "offset": C2[0], "size": C2[1], "letter": "C", "fs": "NTFS",
     "gpt_type": None, "type": "MBR:0x07"},
]
GUID_SR = "\\\\?\\Volume{sr-guid}\\"
GUID_VOLUMES2 = [
    {"guid_path": GUID_SR, "letter": None, "fs": "NTFS", "capacity": SR[1],
     "label": "System Reserved"},
    {"guid_path": "\\\\?\\Volume{c-guid}\\", "letter": "C", "fs": "NTFS",
     "capacity": C2[1], "label": ""},
]


def test_two_ntfs_sources():
    """Two NTFS partitions (one letterless): each region must come from its
    OWN shadow — the letterless one via GUID path — and a size mismatch
    (letter-based wrong-source substitution) must ABORT."""
    print("== two NTFS partitions, letterless via GUID + mismatch abort ==")
    from engine.diskreader import SnapshotMismatchError, assign_sources
    regions = compute_regions(PARTITIONS2, 300 * MIB)
    calls = []

    def good_snapshot(path, log):
        calls.append(path)
        if path == "C:\\":
            return "{SID-C}", "DEV_C", C2[1]
        if path == GUID_SR:
            return "{SID-SR}", "DEV_SR", SR[1]
        raise AssertionError(f"unexpected snapshot target {path}")

    sids, letters, live_notes = assign_sources(
        regions, GUID_VOLUMES2, "PHYS", good_snapshot, print)
    assert calls == ["C:\\", GUID_SR] or calls == [GUID_SR, "C:\\"], calls
    by_part = {r.get("partition"): r for r in regions if r["kind"] == "partition"}
    assert by_part[1]["source"] == "shadow" and by_part[1]["device"] == "DEV_SR"
    assert by_part[1]["snapshot_id"] == "{SID-SR}"
    assert by_part[2]["source"] == "shadow" and by_part[2]["device"] == "DEV_C"
    assert letters == ["C"]
    assert not live_notes
    print("   each region maps to its OWN shadow (letter + GUID path): OK")

    # wrong-source substitution: 'C:' shadow returned for the letterless
    # partition (the production bug) -> length differs -> hard abort
    regions = compute_regions(PARTITIONS2, 300 * MIB)

    def buggy_snapshot(path, log):
        # simulates the off-by-one bug: SR region gets C:'s shadow (126 GB)
        if path == GUID_SR:
            return "{SID-C}", "DEV_C", C2[1] * 632  # wrong length
        return "{SID-C}", "DEV_C", C2[1]
    try:
        assign_sources(regions, GUID_VOLUMES2, "PHYS", buggy_snapshot, print)
    except SnapshotMismatchError as e:
        print(f"   size mismatch aborted as required: {str(e)[:70]}…")
    else:
        raise AssertionError("size mismatch must abort the capture")

    # no GUID candidate -> live fallback with warning (not wrong data)
    regions = compute_regions(PARTITIONS2, 300 * MIB)
    warns = []
    sids, letters, live_notes = assign_sources(
        regions, [], "PHYS", good_snapshot, warns.append)
    by_part = {r.get("partition"): r for r in regions if r["kind"] == "partition"}
    assert by_part[1]["source"] == "live" and by_part[1]["device"] == "PHYS"
    assert by_part[2]["source"] == "shadow"
    assert any("read LIVE" in w for w in warns)
    print("   no GUID match -> live fallback with loud warning: OK")


def main():
    test_layout_equality()
    test_two_ntfs_sources()
    test_reader_stream()
    tmp = tempfile.mkdtemp(prefix="vhd2w-disk-")
    try:
        store, live, shadow, state = test_e2e(tmp)
        test_resume(tmp, store, live, shadow, state)
        test_layout_change(tmp, store, live, shadow)
        print("test_disk_capture: ALL PASSED")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
