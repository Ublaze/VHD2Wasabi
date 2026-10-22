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
    sys.exit("VHD2W_TEST_ACCESS_KEY/SECRET_KEY not set — real S3 smoke skipped.")

SIZE = 20 * 1024 * 1024  # 20 MiB -> multiple 8 MiB parts


def smoke_upload():
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


def smoke_merge_env():
    """Optional: real UploadPartCopy merge of two >=5MiB objects.
    Only runs when VHD2W_TEST_ACCESS_KEY / VHD2W_TEST_SECRET_KEY are set."""
    ak = os.environ.get("VHD2W_TEST_ACCESS_KEY")
    sk = os.environ.get("VHD2W_TEST_SECRET_KEY")
    if not ak or not sk:
        print("merge smoke: VHD2W_TEST_* env vars not set — skipped")
        return
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "app"))
    from engine.merge import MIN_PART, merge_segments
    store = S3Store(ENDPOINT, BUCKET, ak, sk, REGION)
    store.head_bucket()
    segs = []
    payload = os.urandom(MIN_PART)
    try:
        for i in range(2):
            key = f"vhd2wasabi-test/mseg{i}.bin"
            store.put_bytes(key, payload)
            segs.append({"index": i, "key": key, "compressed_size": len(payload)})
        res = merge_segments(store, segs, "vhd2wasabi-test/merged.bin", log=print)
        assert res["method"] == "multipart" and res["parts"] == 2
        assert store.get_bytes("vhd2wasabi-test/merged.bin") == payload * 2
        print("merge smoke: UploadPartCopy merge PASSED")
    finally:
        for k in ("vhd2wasabi-test/mseg0.bin", "vhd2wasabi-test/mseg1.bin",
                  "vhd2wasabi-test/merged.bin"):
            try:
                store.client.delete_object(Bucket=BUCKET, Key=k)
            except Exception:
                pass
        print("merge smoke: test objects deleted")


def smoke_etag_semantics(store):
    """Empirical: Wasabi ETag of an UploadPart == MD5 of the part bytes, and
    the completed object's ETag == md5(concat(binary part MD5s)) + '-N'."""
    from engine.uploader import recompute_multipart_etag
    key = "vhd2wasabi-test/etag-check.bin"
    uid = store.initiate(key)
    parts, md5s = [], []
    try:
        for i in range(2):
            data = os.urandom(5 * 1024 * 1024 + i)
            et = store.upload_part(key, uid, i + 1, data)
            md5 = hashlib.md5(data).hexdigest()
            assert et == md5, f"part ETag {et} != MD5 {md5}"
            parts.append((i + 1, et))
            md5s.append(md5)
        full = store.complete(key, uid, parts)
        expect = recompute_multipart_etag(md5s)
        assert full == expect, f"complete ETag {full} != recomputed {expect}"
        head = store.head_object(key)
        assert head["etag"] == expect
        print("etag semantics: part ETag == MD5, complete ETag == "
              "multipart formula — CONFIRMED on Wasabi")
    finally:
        try:
            store.abort(key, uid)
        except Exception:
            pass
        store.client.delete_object(Bucket=BUCKET, Key=key)


def main():
    smoke_upload()
    store = S3Store(ENDPOINT, BUCKET, AK, SK, REGION)
    smoke_etag_semantics(store)
    smoke_merge_env()


if __name__ == "__main__":
    main()
