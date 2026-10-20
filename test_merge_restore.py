"""Merge + restore tests (no S3, no VSS):
1. E2E: synthetic capture (compressible data -> all segments tiny, total < 5 MiB)
   -> merge (single put_object path) -> restore from merged and from segments
   -> assert .vhd bytes == device bytes + footer, footer validates.
2. Multipart merge with UploadPartCopy: hand-built segment objects (one
   incompressible >= 5 MiB + small ones) -> merge -> assert part layout
   (no non-final part < 5 MiB) and merged bytes == concatenation.
3. Merge failure mid-way leaves segment objects intact.
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
from engine.merge import MIN_PART, merge_segments
from engine.uploader import LocalDirStore
from engine.vhdfooter import build_footer, parse_footer

SIZE = 200 * 1024 * 1024


class FakeVol:
    letter = "M"
    label = "MERGEVOL"
    total = SIZE
    free = SIZE // 2

    def safe_name(self):
        return "MERGEVOL"


class FileReader:
    def __init__(self, path, length, chunk=4 * 1024 * 1024, log=None):
        self.f = open(path, "rb")
        self.length = length
        self.chunk = chunk
        self.pos = 0
        self.log = log

    def seek(self, off):
        self.pos = off

    def read_at(self, pos, n):
        self.f.seek(pos)
        return self.f.read(n)

    def read_chunks(self):
        from engine.volumereader import tolerant_chunks
        for c in tolerant_chunks(self.read_at, self.length, self.chunk,
                                 self.log, start=self.pos):
            self.pos += len(c)
            yield c

    def close(self):
        self.f.close()


def gen_source(path):
    block = (b"merge restore test pattern 0123456789 " * 2000
             + bytes(range(256)) * 50 + b"\x00" * 50000)
    written = 0
    with open(path, "wb") as f:
        while written < SIZE:
            n = min(len(block), SIZE - written)
            f.write(block[:n])
            written += n


def read_all(store, key):
    return store.get_bytes(key)


def test_e2e(tmp):
    print("== E2E capture -> merge -> restore ==")
    src = os.path.join(tmp, "vol.img")
    gen_source(src)
    store = LocalDirStore(os.path.join(tmp, "bucket"))
    spool = os.path.join(tmp, "spool")
    os.makedirs(spool)
    manifest_mod.STATE_ROOT = os.path.join(tmp, "state")
    server = "MERGEBOX"
    p = pipeline_mod.Pipeline(
        store, "mpfx", server, segment_size_mb=64, level=5,
        spool_dir=spool, use_vss=False,
        reader_factory=lambda v: FileReader(src, SIZE),
        callbacks={"log": lambda m: print("  ", m)},
        merge_on_complete=True)
    vol = FakeVol()
    state = p.run_volume(vol)
    assert state["status"] == "done"
    assert state.get("merged_key"), "merged_key missing after merge-on-complete"
    assert state["manifest_version"] == 2
    assert state["merged"]["method"] == "put_object"  # highly compressible -> tiny
    # segments still present (delete flag off)
    for s in state["segments"]:
        assert store.head_object(s["key"]), "segment object missing"
    # merged bytes == concatenation of segment bytes, offsets consistent
    merged = read_all(store, state["merged_key"])
    concat = b"".join(read_all(store, s["key"]) for s in state["segments"])
    assert merged == concat, "merged object is not the plain concatenation"
    off = 0
    for o in state["merged"]["offsets"]:
        assert o["offset"] == off
        off += o["compressed_size"]
    assert off == len(merged)

    # expected VHD bytes: device bytes + fresh footer is NOT deterministic,
    # so compare against extracted device bytes and validate footer structure
    expect_dev = hashlib.sha256(open(src, "rb").read()).hexdigest()

    for mode, manifest in (("merged", state),
                           ("segments", {k: v for k, v in state.items()
                                         if k != "merged_key"})):
        dest = os.path.join(tmp, f"restore-{mode}")
        res = restore_mod.restore(store, manifest, dest,
                                  log=lambda m: print("   ", m))
        assert res["footer"], f"footer validation failed ({mode})"
        assert os.path.basename(res["path"]) == "MERGEBOX_M.vhd", res["path"]
        with open(res["path"], "rb") as f:
            data = f.read()
        assert len(data) == SIZE + 512
        dev_hash = hashlib.sha256(data[:-512]).hexdigest()
        assert dev_hash == expect_dev, f"restored device bytes differ ({mode})"
        info = parse_footer(data[-512:])
        assert info["current_size"] == SIZE
        print(f"   restore from {mode}: OK — {res['path']}, footer OK")
        # temp chunks cleaned up
        assert not [d for d in os.listdir(dest) if d.startswith(".vhd2w-chunks")]
    return state


def test_multipart_copy(tmp):
    print("== multipart merge with UploadPartCopy ==")
    store = LocalDirStore(os.path.join(tmp, "bucket2"))
    big = os.urandom(MIN_PART + 1024 * 1024)   # 6 MiB incompressible
    small1 = os.urandom(1024 * 1024)
    small2 = os.urandom(2 * 1024 * 1024)
    segs = []
    for i, (name, data) in enumerate([("big.7z", big), ("s1.7z", small1),
                                      ("s2.7z", small2), ("big2.7z", big)]):
        key = f"m/{name}"
        store.put_bytes(key, data)
        segs.append({"index": i, "key": key, "compressed_size": len(data)})
    res = merge_segments(store, segs, "m/full.7zcat", log=lambda m: print("  ", m))
    assert res["method"] == "multipart"
    assert res["parts"] >= 3  # big copy + small run flushed with big2 head + big2 rest
    merged = read_all(store, "m/full.7zcat")
    assert merged == big + small1 + small2 + big, "multipart merged bytes wrong"
    offs = res["offsets"]
    assert [o["offset"] for o in offs] == [0, len(big), len(big) + len(small1),
                                           len(big) + len(small1) + len(small2)]
    print(f"   multipart merge OK — {res['parts']} parts, bytes exact")


def test_merge_failure(tmp):
    print("== merge failure leaves segments intact ==")
    store = LocalDirStore(os.path.join(tmp, "bucket3"))
    big = os.urandom(MIN_PART)
    segs = []
    for i in range(2):
        key = f"f/seg{i}.7z"
        store.put_bytes(key, big)
        segs.append({"index": i, "key": key, "compressed_size": len(big)})

    class BoomStore(LocalDirStore):
        def upload_part_copy(self, *a, **k):
            raise RuntimeError("simulated copy failure")

    boom = BoomStore(os.path.join(tmp, "bucket3"))
    try:
        merge_segments(boom, segs, "f/full.7zcat")
    except Exception as e:
        print(f"   merge failed as expected: {e}")
    else:
        raise AssertionError("merge should have failed")
    for s in segs:
        assert store.head_object(s["key"]), "segment object lost after failed merge"
    assert store.head_object("f/full.7zcat") is None, "partial merged object left behind"
    # no leftover multipart temp dirs
    leftovers = [x for x in store.list_keys("f/") if ".mpu-" in x]
    assert not leftovers
    print("   segments intact, no partial merged object")


def test_copy_cap(tmp):
    print("== chunked UploadPartCopy with forced cap ==")
    store = LocalDirStore(os.path.join(tmp, "bucket4"))
    cap = 8 * 1024 * 1024
    big = os.urandom(20 * 1024 * 1024)   # > 2x cap
    small = os.urandom(1024 * 1024)
    segs = []
    for i, (name, data) in enumerate([("big.7z", big), ("s.7z", small)]):
        key = f"c/{name}"
        store.put_bytes(key, data)
        segs.append({"index": i, "key": key, "compressed_size": len(data)})

    copy_lens, put_lens = [], []

    class RecStore(LocalDirStore):
        def upload_part_copy(self, key, uid, pn, src, start, length, log=None):
            copy_lens.append(length)
            return super().upload_part_copy(key, uid, pn, src, start, length, log)

        def upload_part(self, key, uid, pn, data, log=None):
            put_lens.append(len(data))
            return super().upload_part(key, uid, pn, data, log)

    rec = RecStore(os.path.join(tmp, "bucket4"))
    res = merge_segments(rec, segs, "c/full.7zcat", copy_part_max=cap,
                         log=lambda m: print("  ", m))
    merged = read_all(store, "c/full.7zcat")
    assert merged == big + small, "chunked merge bytes wrong"
    assert copy_lens == [cap, cap], f"expected 2 capped copies, got {copy_lens}"
    all_parts = copy_lens + put_lens
    for sz in all_parts[:-1]:
        assert sz >= MIN_PART, f"non-final part {sz} < 5 MiB"
    for sz in copy_lens:
        assert sz <= cap, f"copy part {sz} exceeds cap"
    offs = res["offsets"]
    assert offs[0]["offset"] == 0 and offs[1]["offset"] == len(big)
    print(f"   chunked copy OK — copies {copy_lens}, memory parts "
          f"{[f'{x:,}' for x in put_lens]}, bytes exact")


def test_done_unmerged_rerun(tmp):
    print("== rerun of done-but-unmerged volume performs merge only ==")
    src = os.path.join(tmp, "vol2.img")
    gen_source(src)
    store = LocalDirStore(os.path.join(tmp, "bucket5"))
    spool = os.path.join(tmp, "spool5")
    os.makedirs(spool)
    manifest_mod.STATE_ROOT = os.path.join(tmp, "state5")
    server = "OLDBOX"
    # capture WITHOUT merge
    p = pipeline_mod.Pipeline(
        store, "pfx", server, segment_size_mb=64, level=5,
        spool_dir=spool, use_vss=False,
        reader_factory=lambda v: FileReader(src, SIZE),
        merge_on_complete=False)
    vol = FakeVol()
    vol.label = "MERGEVOL"  # same safe_name as state
    s1 = p.run_volume(vol)
    assert s1["status"] == "done" and not s1.get("merged_key")
    # rerun with merge ON — must not re-upload segments (reader must not even
    # be touched) and must merge under the ORIGINAL prefix
    def boom_reader(v):
        raise AssertionError("reader must not be opened for done volume")
    p2 = pipeline_mod.Pipeline(
        store, "pfx", server, segment_size_mb=64, level=5,
        spool_dir=spool, use_vss=False, reader_factory=boom_reader,
        merge_on_complete=True)
    s2 = p2.run_volume(vol)
    assert s2.get("merged_key"), "merge did not run on done-but-unmerged rerun"
    assert s2["merged_key"].startswith(s1["key_prefix"]), \
        "merged object not under original key prefix"
    assert store.head_object(s2["merged_key"])["size"] == s2["merged"]["total_size"]
    print(f"   merge-only rerun OK -> {s2['merged_key']}")


def test_output_naming(tmp):
    print("== restore output naming + no-overwrite ==")
    from engine.restore import _unique_path, output_name
    m_disk = {"server": "TRX3WEB", "volume": "Disk0", "capture_kind": "disk"}
    m_vol = {"server": "TRX3WEB", "volume": "C: (Windows)",
             "capture_kind": "volume", "drive_letter": "C"}
    m_vol_old = {"server": "TRX3WEB", "volume": "F: (Data)"}  # legacy manifest
    m_label_only = {"server": "TRX3WEB", "volume": "Windows"}
    m_bad_chars = {"server": "TR/X3:WEB", "volume": "Disk0",
                   "capture_kind": "disk"}
    assert output_name(m_disk) == "TRX3WEB_Disk0.vhd"
    assert output_name(m_vol) == "TRX3WEB_C.vhd"
    assert output_name(m_vol_old) == "TRX3WEB_F.vhd"
    assert output_name(m_label_only) == "TRX3WEB_Windows.vhd"
    assert "/" not in output_name(m_bad_chars) and ":" not in output_name(m_bad_chars)
    d = os.path.join(tmp, "naming")
    os.makedirs(d)
    p1 = _unique_path(d, "TRX3WEB_C.vhd")
    open(p1, "w").close()
    p2 = _unique_path(d, "TRX3WEB_C.vhd")
    assert p2 != p1 and "(2)" in p2 and not os.path.exists(p2)
    print("   TRX3WEB_C.vhd / TRX3WEB_F.vhd / TRX3WEB_Disk0.vhd, collision -> ' (2)': OK")


def main():
    tmp = tempfile.mkdtemp(prefix="vhd2w-mr-")
    try:
        test_e2e(tmp)
        test_output_naming(tmp)
        test_multipart_copy(tmp)
        test_copy_cap(tmp)
        test_done_unmerged_rerun(tmp)
        test_merge_failure(tmp)
        print("test_merge_restore: ALL PASSED")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
