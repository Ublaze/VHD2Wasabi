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


class TruncTailReader(FileReader):
    """Simulates the production failure: the device's last TRUNC bytes are
    unreadable (read returns empty) although the reported length includes them."""

    TRUNC = 4096

    def read_at(self, pos, n):
        # serve the readable prefix; the bad tail block itself is unreadable
        good = max(0, min(n, (self.length - self.TRUNC) - pos))
        if good <= 0:
            return b""
        return super().read_at(pos, good)


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
        warnings = []
        # TruncTailReader simulates the production server's shadow device whose
        # last 4096 bytes are unreadable despite being in the reported length.
        p = pipeline_mod.Pipeline(
            store, "testprefix", server, segment_size_mb=SEG_MB, level=5,
            spool_dir=spool, use_vss=False,
            reader_factory=lambda vol: TruncTailReader(src, SIZE, log=warnings.append),
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
        # expected device bytes: full source with the unreadable tail zero-padded
        expect = hashlib.sha256()
        with open(src, "rb") as f:
            remaining = SIZE - TruncTailReader.TRUNC
            while remaining > 0:
                b = f.read(min(1 << 22, remaining))
                if not b:
                    break
                expect.update(b)
                remaining -= len(b)
        expect.update(b"\x00" * TruncTailReader.TRUNC)
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
        assert any("zero-padded" in w for w in warnings), \
            f"expected a zero-pad warning, got: {warnings}"
        print(f"reassembled {total_extracted:,} bytes — device hash MATCHES "
              f"(tail zero-padded), footer valid")

        # verify mode (7za t + streamed re-hash per segment)
        mkey = [k for k in store.list_keys("testprefix", suffix=".manifest.json")][0]
        ok = pipeline_mod.verify_manifest(store, mkey)
        assert ok, "verify_manifest failed"

        # resume semantics: completed volume is skipped on a second run
        p2 = pipeline_mod.Pipeline(
            store, "testprefix", server, segment_size_mb=SEG_MB, level=5,
            spool_dir=spool, use_vss=False,
            reader_factory=lambda vol: TruncTailReader(src, SIZE),
            callbacks={"log": lambda m: print(" ", m)})
        state2 = p2.run_volume(vol)
        assert state2["status"] == "done" and len(state2["segments"]) == len(segs)

        # partial resume: "N segments planned, N-1 complete" — drop the last
        # segment from state AND its object, rerun, only that segment is redone
        st = manifest_mod.load_state(server, "FAKEVOL")
        last = st["segments"].pop()
        os.remove(os.path.join(tmp, "bucket", last["key"].replace("/", os.sep)))
        st["status"] = "in_progress"
        st["image_sha256"] = None
        manifest_mod.save_state(server, "FAKEVOL", st)
        p3 = pipeline_mod.Pipeline(
            store, "testprefix", server, segment_size_mb=SEG_MB, level=5,
            spool_dir=spool, use_vss=False,
            reader_factory=lambda vol: TruncTailReader(src, SIZE),
            callbacks={"log": lambda m: print(" ", m)})
        state3 = p3.run_volume(vol)
        assert state3["status"] == "done"
        assert len(state3["segments"]) == len(segs)
        # all segments except the last must be byte-identical; the last segment
        # legitimately differs because the VHD footer embeds a fresh UUID/timestamp
        for a, b in zip(segs[:-1], state3["segments"][:-1]):
            assert a["raw_sha256"] == b["raw_sha256"], "resumed segment hash differs"
            assert a["key"] == b["key"]
        assert state3["segments"][-1]["raw_size"] == segs[-1]["raw_size"]
        ok = pipeline_mod.verify_manifest(
            store, [k for k in store.list_keys("testprefix", suffix=".manifest.json")][0],
            log=lambda m: None)
        assert ok, "verify after partial resume failed"
        print(f"partial resume OK — redid segment {last['index'] + 1} only, "
              "verify PASSED")

        # tail-pad window semantics (32 MiB): truncated tails inside the
        # window pad; anything larger is a hard error
        from engine.volumereader import ShortReadError, tolerant_chunks
        length = 64 * 1024 * 1024

        def truncated(trunc):
            def read_at(pos, n):
                good = max(0, min(n, (length - trunc) - pos))
                return b"\xAA" * good
            return read_at

        warns = []
        got = sum(len(c) for c in tolerant_chunks(truncated(3 * 1024 * 1024),
                                                  length, 4 * 1024 * 1024,
                                                  warns.append))
        assert got == length and any("zero-padded" in w for w in warns)
        print("3 MiB truncated tail: zero-padded within 32 MiB window — OK")

        try:
            for _ in tolerant_chunks(truncated(33 * 1024 * 1024), length,
                                     4 * 1024 * 1024):
                pass
        except ShortReadError:
            print("33 MiB truncated tail: hard ShortReadError — OK")
        else:
            raise AssertionError("33 MiB tail must raise ShortReadError")

        # mid-device short read must remain a hard error (no silent zero-fill)
        hole_at = 8 * 1024 * 1024

        def holey(pos, n):
            # reads touching the hole return only the readable prefix
            if pos < hole_at <= pos + n:
                return b"\xAA" * (hole_at - pos)
            if hole_at <= pos < hole_at + 4 * 1024 * 1024:
                return b""
            return b"\xAA" * n
        try:
            for _ in tolerant_chunks(holey, length, 4 * 1024 * 1024):
                pass
        except ShortReadError:
            print("mid-device short read correctly raised ShortReadError")
        else:
            raise AssertionError("mid-device short read must raise ShortReadError")

        # a gap that becomes readable again must NOT be tolerated either
        def gap_then_ok(pos, n):
            if hole_at <= pos < hole_at + 4 * 1024 * 1024:
                return b""
            return b"\xAA" * n
        try:
            for _ in tolerant_chunks(gap_then_ok, length, 4 * 1024 * 1024):
                pass
        except ShortReadError:
            print("gap-then-readable correctly raised ShortReadError")
        else:
            raise AssertionError("gap-then-readable must raise ShortReadError")

        print("test_pipeline_local: ALL PASSED")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
