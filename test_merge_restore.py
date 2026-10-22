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
        assert os.path.basename(res["path"]) == "MERGEBOX-M.vhd", res["path"]
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
    assert output_name(m_disk) == "TRX3WEB-Disk0.vhd"
    assert output_name(m_vol) == "TRX3WEB-C.vhd"
    assert output_name(m_vol_old) == "TRX3WEB-F.vhd"
    assert output_name(m_label_only) == "TRX3WEB-Windows.vhd"
    assert "/" not in output_name(m_bad_chars) and ":" not in output_name(m_bad_chars)
    d = os.path.join(tmp, "naming")
    os.makedirs(d)
    p1 = _unique_path(d, "TRX3WEB-C.vhd")
    open(p1, "w").close()
    p2 = _unique_path(d, "TRX3WEB-C.vhd")
    assert p2 != p1 and "(2)" in p2 and not os.path.exists(p2)
    print("   TRX3WEB-C.vhd / TRX3WEB-F.vhd / TRX3WEB-Disk0.vhd, collision -> ' (2)': OK")


def test_gated_delete(tmp):
    print("== gate-verified delete after merge ==")
    src = os.path.join(tmp, "vol3.img")
    gen_source(src)
    store = LocalDirStore(os.path.join(tmp, "bucket6"))
    spool = os.path.join(tmp, "spool6")
    os.makedirs(spool)
    manifest_mod.STATE_ROOT = os.path.join(tmp, "state6")
    server = "GATEBOX"
    vol = FakeVol()
    # capture + merge, delete OFF
    p1 = pipeline_mod.Pipeline(
        store, "pfx", server, segment_size_mb=64, level=5, spool_dir=spool,
        use_vss=False, reader_factory=lambda v: FileReader(src, SIZE),
        merge_on_complete=True, delete_segments_after_merge=False)
    s1 = p1.run_volume(vol)
    assert s1.get("merged_key") and not s1.get("segments_deleted")
    # rerun with delete ON (done+merged branch — the production bug path):
    # gate verifies merged object, then deletes. Wrap the store to prove the
    # gate downloads NOTHING (only tiny range-GETs for magic bytes).
    class CountingStore(LocalDirStore):
        bulk = 0

        def download_file(self, k, p):
            self.bulk += 1
            return super().download_file(k, p)

        def get_bytes(self, k):
            self.bulk += 1
            return super().get_bytes(k)

        def open_stream(self, k):
            self.bulk += 1
            return super().open_stream(k)

    cstore = CountingStore(os.path.join(tmp, "bucket6"))
    p2 = pipeline_mod.Pipeline(
        cstore, "pfx", server, segment_size_mb=64, level=5, spool_dir=spool,
        use_vss=False, reader_factory=lambda v: FileReader(src, SIZE),
        merge_on_complete=True, delete_segments_after_merge=True,
        callbacks={"log": lambda m: print("  ", m)})
    s2 = p2.run_volume(vol)
    assert cstore.bulk == 0, f"gate downloaded bulk data ({cstore.bulk} calls)"
    assert s2.get("segments_deleted") is True, "segments not deleted after gate"
    assert s2.get("merged_verified") is True and s2.get("merged_verified_at")
    for s in s2["segments"]:
        assert store.head_object(s["key"]) is None, f"{s['key']} still present"
    assert store.head_object(s2["merged_key"]), "merged object missing"
    # manifest in the bucket carries the flags too
    mkey = [k for k in store.list_keys("pfx", suffix=".manifest.json")][0]
    m = manifest_mod.download_manifest(store, mkey)
    assert m.get("segments_deleted") and m.get("merged_verified")
    # restore from merged still passes after deletion
    res = restore_mod.restore(store, m, os.path.join(tmp, "restore-gated"))
    assert res["footer"], "restore-from-merged failed after segment deletion"
    assert os.path.basename(res["path"]) == "GATEBOX-M.vhd"
    print(f"   segments deleted, merged verified, restore OK -> "
          f"{os.path.basename(res['path'])}")

    # Verify button path now uses the merged object
    ok = pipeline_mod.verify_manifest(store, mkey, log=lambda m: None)
    assert ok, "verify via merged object failed"


def test_gated_delete_corruption(tmp):
    print("== corrupted merged object -> deletion skipped, segments kept ==")
    src = os.path.join(tmp, "vol4.img")
    gen_source(src)
    store = LocalDirStore(os.path.join(tmp, "bucket7"))
    spool = os.path.join(tmp, "spool7")
    os.makedirs(spool)
    manifest_mod.STATE_ROOT = os.path.join(tmp, "state7")
    server = "CORRUPTBOX"
    vol = FakeVol()
    p1 = pipeline_mod.Pipeline(
        store, "pfx", server, segment_size_mb=64, level=5, spool_dir=spool,
        use_vss=False, reader_factory=lambda v: FileReader(src, SIZE),
        merge_on_complete=True, delete_segments_after_merge=False)
    s1 = p1.run_volume(vol)
    # metadata-detectable corruption: truncate the merged object
    mpath = store._path(s1["merged_key"])
    with open(mpath, "r+b") as f:
        f.truncate(os.path.getsize(mpath) - 100)
    p2 = pipeline_mod.Pipeline(
        store, "pfx", server, segment_size_mb=64, level=5, spool_dir=spool,
        use_vss=False, reader_factory=lambda v: FileReader(src, SIZE),
        merge_on_complete=True, delete_segments_after_merge=True,
        callbacks={"log": lambda m: print("  ", m)})
    s2 = p2.run_volume(vol)
    assert not s2.get("segments_deleted"), "deletion happened despite corruption"
    assert not s2.get("merged_verified")
    for s in s2["segments"]:
        assert store.head_object(s["key"]), "segment wrongly deleted"
    print("   truncated merged object: metadata gate held, segments retained")

    # interior corruption (size/magic intact) is caught by DEEP verify only
    store2 = LocalDirStore(os.path.join(tmp, "bucket7b"))
    spool2 = os.path.join(tmp, "spool7b")
    os.makedirs(spool2)
    manifest_mod.STATE_ROOT = os.path.join(tmp, "state7b")
    p3 = pipeline_mod.Pipeline(
        store2, "pfx", server, segment_size_mb=64, level=5, spool_dir=spool2,
        use_vss=False, reader_factory=lambda v: FileReader(src, SIZE),
        merge_on_complete=True, delete_segments_after_merge=False)
    s3 = p3.run_volume(vol)
    mpath2 = store2._path(s3["merged_key"])
    with open(mpath2, "r+b") as f:
        f.seek(100)
        b = f.read(1)
        f.seek(100)
        f.write(bytes([b[0] ^ 0xFF]))
    mkey = [k for k in store2.list_keys("pfx", suffix=".manifest.json")][0]
    ok_meta = pipeline_mod.verify_manifest(store2, mkey, log=lambda m: None)
    assert ok_meta, "metadata verify should pass (interior byte not visible)"
    ok_deep = pipeline_mod.verify_manifest(store2, mkey, deep=True,
                                           log=lambda m: None)
    assert not ok_deep, "deep verify must catch interior corruption"
    print("   interior byte flip: metadata passes (by design), deep verify "
          "catches it")


def test_inline_part_hash(tmp):
    print("== inline per-part MD5-vs-ETag check fails loudly on corruption ==")
    src = os.path.join(tmp, "vol5.img")
    gen_source(src)

    class CorruptAtRest(LocalDirStore):
        """Stores a corrupted copy but honestly reports MD5 of what it
        stored — exactly what the client-side ETag check exists to catch."""
        def upload_part(self, key, uid, pn, data, log=None):
            bad = bytes([data[0] ^ 0xFF]) + data[1:]
            return super().upload_part(key, uid, pn, bad, log)

    store = CorruptAtRest(os.path.join(tmp, "bucket8"))
    spool = os.path.join(tmp, "spool8")
    os.makedirs(spool)
    manifest_mod.STATE_ROOT = os.path.join(tmp, "state8")
    p = pipeline_mod.Pipeline(
        store, "pfx", "BADBOX", segment_size_mb=64, level=5, spool_dir=spool,
        use_vss=False, reader_factory=lambda v: FileReader(src, SIZE),
        callbacks={"log": lambda m: None})
    try:
        p.run_volume(FakeVol())
    except Exception as e:
        assert "ETag" in str(e) or "MD5" in str(e), str(e)
        print(f"   corruption caught at upload: {str(e)[:80]}")
        return
    raise AssertionError("corrupt-at-rest upload must fail loudly")


def test_magic_shift_detection(tmp):
    print("== merged-object magic check detects shifted content ==")
    src = os.path.join(tmp, "vol6.img")
    gen_source(src)
    store = LocalDirStore(os.path.join(tmp, "bucket9"))
    spool = os.path.join(tmp, "spool9")
    os.makedirs(spool)
    manifest_mod.STATE_ROOT = os.path.join(tmp, "state9")
    p = pipeline_mod.Pipeline(
        store, "pfx", "MAGICBOX", segment_size_mb=64, level=5, spool_dir=spool,
        use_vss=False, reader_factory=lambda v: FileReader(src, SIZE),
        merge_on_complete=True)
    s = p.run_volume(FakeVol())
    mkey = [k for k in store.list_keys("pfx", suffix=".manifest.json")][0]
    assert pipeline_mod.verify_manifest(store, mkey, log=lambda m: None)
    # shift merged content: overwrite segment-2 offset region (magic gone)
    mpath = store._path(s["merged_key"])
    off2 = s["merged"]["offsets"][1]["offset"]
    with open(mpath, "r+b") as f:
        f.seek(off2)
        f.write(b"\x00" * 6)
    ok = pipeline_mod.verify_manifest(store, mkey, log=lambda m: print("  ", m))
    assert not ok, "shifted/corrupted merged content must fail magic check"
    print("   corrupted segment boundary detected by magic check: OK")


def test_legacy_state_compat(tmp):
    print("== legacy label-based state/keys stay put; rerun merges in place ==")
    src = os.path.join(tmp, "vol7.img")
    gen_source(src)
    store = LocalDirStore(os.path.join(tmp, "bucket10"))
    spool = os.path.join(tmp, "spool10")
    os.makedirs(spool)
    manifest_mod.STATE_ROOT = os.path.join(tmp, "state10")
    server = "LEGBOX"
    vol = FakeVol()  # letter M, label MERGEVOL
    p1 = pipeline_mod.Pipeline(
        store, "pfx", server, segment_size_mb=64, level=5, spool_dir=spool,
        use_vss=False, reader_factory=lambda v: FileReader(src, SIZE),
        merge_on_complete=False)
    s1 = p1.run_volume(vol)
    new_prefix = s1["key_prefix"]
    assert new_prefix.endswith(f"/{server}-M")

    # convert to a LEGACY capture: label-based keys + label-named state file
    legacy_prefix = new_prefix[:new_prefix.rfind("/")] + "/MERGEVOL"
    st = manifest_mod.load_state(server, "M")
    st["key_prefix"] = legacy_prefix
    st.pop("drive_letter", None)  # legacy manifests have no drive_letter

    def move(old_key, new_key):
        os.replace(store._path(old_key), store._path(new_key))
        if os.path.isfile(store._path(old_key) + ".etag"):
            os.replace(store._path(old_key) + ".etag",
                       store._path(new_key) + ".etag")
    for seg in st["segments"]:
        nk = seg["key"].replace(f"/{server}-M.", "/MERGEVOL.")
        move(seg["key"], nk)
        seg["key"] = nk
    move(f"{new_prefix}.manifest.json", f"{legacy_prefix}.manifest.json")
    manifest_mod.save_state(server, "MERGEVOL", st)     # legacy state name
    manifest_mod.upload_manifest(store, f"{legacy_prefix}.manifest.json", st)
    os.remove(manifest_mod.state_path(server, "M"))     # only legacy exists

    # rerun: done-but-unmerged legacy state -> merge under ORIGINAL prefix,
    # nothing re-uploaded, nothing under the new basename
    p2 = pipeline_mod.Pipeline(
        store, "pfx", server, segment_size_mb=64, level=5, spool_dir=spool,
        use_vss=False, reader_factory=lambda v: (_ for _ in ()).throw(
            AssertionError("reader must not be opened")),
        merge_on_complete=True, delete_segments_after_merge=True,
        callbacks={"log": lambda m: print("  ", m)})
    s2 = p2.run_volume(vol)
    assert s2["merged_key"].startswith(legacy_prefix), s2["merged_key"]
    assert s2.get("segments_deleted") is True
    stray = [k for k in store.list_keys("pfx") if f"/{server}-M." in k]
    assert not stray, f"objects re-uploaded under new basename: {stray}"
    # legacy manifest still verifies + restores (keys read from inside it)
    mkey = f"{legacy_prefix}.manifest.json"
    assert pipeline_mod.verify_manifest(store, mkey, log=lambda m: None)
    m = manifest_mod.download_manifest(store, mkey)
    res = restore_mod.restore(store, m, os.path.join(tmp, "restore-legacy"))
    assert res["footer"], "legacy manifest restore failed"
    assert os.path.basename(res["path"]) == "LEGBOX-M.vhd"  # letter from "M: (MERGEVOL)"
    print(f"   legacy merge-in-place + verify + restore OK -> "
          f"{os.path.basename(res['path'])}")


def main():
    tmp = tempfile.mkdtemp(prefix="vhd2w-mr-")
    try:
        test_e2e(tmp)
        test_output_naming(tmp)
        test_multipart_copy(tmp)
        test_copy_cap(tmp)
        test_done_unmerged_rerun(tmp)
        test_gated_delete(tmp)
        test_gated_delete_corruption(tmp)
        test_inline_part_hash(tmp)
        test_magic_shift_detection(tmp)
        test_legacy_state_compat(tmp)
        test_merge_failure(tmp)
        print("test_merge_restore: ALL PASSED")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
