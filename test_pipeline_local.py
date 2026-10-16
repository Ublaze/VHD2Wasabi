"""Synthetic end-to-end pipeline test WITHOUT S3 and WITHOUT VSS:
- generates ~200 MiB of compressible bytes as a fake volume
- runs the real segment/compress(7-Zip)/multipart logic against LocalDirStore
- reassembles the segments, verifies per-segment sha256 + whole-image hash
- exercises the verify path (7za t + streamed extract hash)
"""
import hashlib
import os
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "app"))

from engine import manifest as manifest_mod
from engine import pipeline as pipeline_mod
from engine.uploader import LocalDirStore

SIZE = 200 * 1024 * 1024
SEG_MB = 32  # small segments to get several of them


class FakeVol:
    letter = "T"
    label = "FAKEVOL"
    total = SIZE
    free = SIZE // 2

    def safe_name(self):
        return "FAKEVOL"


class FileReader:
    """VolumeReader-compatible reader over a temp file (sector aligned)."""

    def __init__(self, path, length, chunk=4 * 1024 * 1024):
        self.f = open(path, "rb")
        self.length = length
        self.chunk = chunk
        self.pos = 0

    def seek(self, off):
        self.f.seek(off)
        self.pos = off

    def read_chunks(self):
        while self.pos < self.length:
            want = min(self.chunk, self.length - self.pos)
            data = self.f.read(want)
            if not data:
                break
            self.pos += len(data)
            yield data

    def close(self):
        self.f.close()


def gen_source(path):
    """Compressible but varied data (real text-ish patterns + repeats)."""
    block = (b"The quick brown fox jumps over the lazy dog. " * 1000
             + bytes(range(256)) * 100
             + b"\x00" * 100000)
    written = 0
    with open(path, "wb") as f:
        while written < SIZE:
            n = min(len(block), SIZE - written)
            f.write(block[:n])
            written += n
    return SIZE


def main():
    tmp = tempfile.mkdtemp(prefix="vhd2w-test-")
    try:
        src = os.path.join(tmp, "volume.img")
        gen_source(src)
        store = LocalDirStore(os.path.join(tmp, "bucket"))
        spool = os.path.join(tmp, "spool")
        os.makedirs(spool)

        # redirect local state into tmp so we don't pollute %LOCALAPPDATA%
        manifest_mod.STATE_ROOT = os.path.join(tmp, "state")

        server = "TESTBOX"
        p = pipeline_mod.Pipeline(
            store, "testprefix", server, segment_size_mb=SEG_MB, level=5,
            spool_dir=spool, use_vss=False,
            reader_factory=lambda vol: FileReader(src, SIZE),
            callbacks={"log": lambda m: print(" ", m)})
        vol = FakeVol()
        state = p.run_volume(vol)

        assert state["status"] == "done"
        assert state["total_size"] == SIZE + 512
        segs = state["segments"]
        assert len(segs) == (SIZE + 512 + p.segment_size - 1) // p.segment_size
        print(f"segments: {len(segs)}")

        # reassemble: extract every .7z part, concat, compare with source+footer
        date_dir = os.path.join(tmp, "bucket", "testprefix", server)
        day = os.listdir(date_dir)[0]
        partdir = os.path.join(date_dir, day, "FAKEVOL")
        expect = hashlib.sha256()
        with open(src, "rb") as f:
            while True:
                b = f.read(1 << 22)
                if not b:
                    break
                expect.update(b)
        expect_dev = expect.hexdigest()

        got = hashlib.sha256()          # hash of device bytes only
        last512 = bytearray()
        from engine.compressor import find_sevenzip
        sevenzip = find_sevenzip()
        import subprocess
        total_extracted = 0
        for s in segs:
            key_path = os.path.join(tmp, "bucket", s["key"].replace("/", os.sep))
            assert os.path.isfile(key_path), f"missing object {s['key']}"
            assert os.path.getsize(key_path) == s["compressed_size"]
            proc = subprocess.Popen([sevenzip, "x", "-so", key_path],
                                    stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
            seg_hash = hashlib.sha256()
            while True:
                b = proc.stdout.read(1 << 22)
                if not b:
                    break
                seg_hash.update(b)
                last512.extend(b)
                if len(last512) > 512:
                    overflow = len(last512) - 512
                    got.update(last512[:overflow])
                    del last512[:overflow]
                total_extracted += len(b)
            proc.wait()
            assert proc.returncode == 0
            assert seg_hash.hexdigest() == s["raw_sha256"], "segment hash mismatch"
        assert len(last512) == 512
        from engine.vhdfooter import validate_footer
        validate_footer(bytes(last512), expected_size=SIZE)
        assert got.hexdigest() == expect_dev, "device-bytes hash mismatch"
        print(f"reassembled {total_extracted:,} bytes — device hash MATCHES, footer valid")

        # verify mode (7za t + streamed re-hash per segment)
        mkey = [k for k in store.list_keys("testprefix", suffix=".manifest.json")][0]
        ok = pipeline_mod.verify_manifest(store, mkey)
        assert ok, "verify_manifest failed"

        # resume semantics: completed volume is skipped on a second run
        p2 = pipeline_mod.Pipeline(
            store, "testprefix", server, segment_size_mb=SEG_MB, level=5,
            spool_dir=spool, use_vss=False,
            reader_factory=lambda vol: FileReader(src, SIZE),
            callbacks={"log": lambda m: print(" ", m)})
        state2 = p2.run_volume(vol)
        assert state2["status"] == "done" and len(state2["segments"]) == len(segs)

        print("test_pipeline_local: ALL PASSED")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
