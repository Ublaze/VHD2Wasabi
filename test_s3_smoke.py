"""Real S3 smoke test against Wasabi bucket banian-auto-sql, prefix vhd2wasabi-test/.
Uploads a few-MB synthetic segment through the REAL S3 multipart path
(S3Store + TailUploader + 7-Zip spool), HeadObject + streamed re-hash, then
DELETES the test objects."""
import hashlib
import os
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "app"))
from engine.compressor import SegmentCompressor, find_sevenzip
from engine.uploader import S3Store, TailUploader

BUCKET = os.environ.get("VHD2W_TEST_BUCKET", "banian-auto-sql")
ENDPOINT = os.environ.get("VHD2W_TEST_ENDPOINT", "s3.eu-central-1.wasabisys.com")
REGION = os.environ.get("VHD2W_TEST_REGION", "eu-central-1")
AK = os.environ.get("VHD2W_TEST_ACCESS_KEY", "")
SK = os.environ.get("VHD2W_TEST_SECRET_KEY", "")
KEY = "vhd2wasabi-test/smoke.part001.7z"

if not AK or not SK:
    sys.exit("Set VHD2W_TEST_ACCESS_KEY and VHD2W_TEST_SECRET_KEY to run this test.")

SIZE = 20 * 1024 * 1024  # 20 MiB -> multiple 8 MiB parts


def main():
    store = S3Store(ENDPOINT, BUCKET, AK, SK, REGION)
    store.head_bucket()
    print("head_bucket ok")

    tmp = tempfile.mkdtemp(prefix="vhd2w-s3smoke-")
    spool = os.path.join(tmp, "seg.7z")
    try:
        sz = find_sevenzip()
        comp = SegmentCompressor(sz, spool, 1)  # fast level for smoke
        up = TailUploader(store, KEY, spool, log=print)
        comp.start()
        up.start()
        hasher = hashlib.sha256()
        block = b"Wasabi smoke test data block. " * 4000  # ~120KB compressible
        fed = 0
        while fed < SIZE:
            take = block[: min(len(block), SIZE - fed)]
            comp.write(take)
            hasher.update(take)
            fed += len(take)
        csize = comp.finish()
        up.writer_done()
        etag = up.finish(120)
        print(f"uploaded: raw {fed:,} -> compressed {csize:,}, etag {etag}, "
              f"parts {len(up.parts)}")

        head = store.head_object(KEY)
        assert head and head["size"] == csize, f"head mismatch: {head}"

        # download + 7za t + streamed extract re-hash
        import subprocess
        dl = os.path.join(tmp, "dl.7z")
        store.download_file(KEY, dl)
        t = subprocess.run([sz, "t", dl], capture_output=True)
        assert t.returncode == 0, "7z integrity test failed"
        p = subprocess.Popen([sz, "x", "-so", dl], stdout=subprocess.PIPE,
                             stderr=subprocess.DEVNULL)
        h2 = hashlib.sha256()
        got = 0
        while True:
            b = p.stdout.read(1 << 22)
            if not b:
                break
            h2.update(b)
            got += len(b)
        p.wait()
        assert got == fed and h2.hexdigest() == hasher.hexdigest(), "hash mismatch"
        print("download + integrity + re-hash: PASS")
    finally:
        store.client.delete_object(Bucket=BUCKET, Key=KEY)
        left = store.list_keys("vhd2wasabi-test/")
        assert not left, f"cleanup failed: {left}"
        print("test objects deleted")
        shutil.rmtree(tmp, ignore_errors=True)
    print("S3 SMOKE TEST PASSED")


if __name__ == "__main__":
    main()
