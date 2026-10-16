"""Multipart upload abstraction: S3 (boto3) and a local-dir store for tests,
plus a tail-uploader that uploads a growing spool file concurrently."""
import base64
import hashlib
import json
import os
import threading
import time

PART_SIZE = 8 * 1024 * 1024  # 8 MiB minimum part size
RETRIES = 3


class UploadError(RuntimeError):
    pass


def _retry(fn, what, log, retries=RETRIES):
    delay = 2
    for attempt in range(1, retries + 1):
        try:
            return fn()
        except Exception as e:
            if attempt == retries:
                raise UploadError(f"{what} failed after {retries} attempts: {e}")
            if log:
                log(f"  retry {attempt}/{retries} for {what}: {e}")
            time.sleep(delay)
            delay *= 2


class S3Store:
    def __init__(self, endpoint, bucket, access_key, secret_key, region="eu-central-1"):
        import boto3
        from botocore.config import Config
        # generous read timeout: UploadPartCopy only answers when the
        # server-side copy finishes (multi-GB copies take minutes on Wasabi)
        cfg = Config(read_timeout=300, connect_timeout=30,
                     retries={"max_attempts": 4, "mode": "standard"})
        self.bucket = bucket
        self.client = boto3.client(
            "s3", endpoint_url=f"https://{endpoint}",
            aws_access_key_id=access_key, aws_secret_access_key=secret_key,
            region_name=region, config=cfg,
        )

    def head_bucket(self):
        self.client.head_bucket(Bucket=self.bucket)

    def initiate(self, key):
        r = self.client.create_multipart_upload(Bucket=self.bucket, Key=key)
        return r["UploadId"]

    def upload_part(self, key, upload_id, part_no, data, log=None):
        def do():
            return self.client.upload_part(
                Bucket=self.bucket, Key=key, UploadId=upload_id,
                PartNumber=part_no, Body=data)
        r = _retry(do, f"upload_part {part_no}", log)
        return r["ETag"].strip('"')

    def complete(self, key, upload_id, parts, log=None):
        def do():
            return self.client.complete_multipart_upload(
                Bucket=self.bucket, Key=key, UploadId=upload_id,
                MultipartUpload={"Parts": [
                    {"PartNumber": n, "ETag": f'"{e}"'} for n, e in parts]})
        r = _retry(do, "complete_multipart_upload", log)
        return r.get("ETag", "").strip('"')

    def abort(self, key, upload_id):
        try:
            self.client.abort_multipart_upload(
                Bucket=self.bucket, Key=key, UploadId=upload_id)
        except Exception:
            pass

    def head_object(self, key):
        try:
            r = self.client.head_object(Bucket=self.bucket, Key=key)
            return {"size": r["ContentLength"], "etag": r["ETag"].strip('"')}
        except Exception:
            return None

    def put_bytes(self, key, data):
        self.client.put_object(Bucket=self.bucket, Key=key, Body=data)

    def get_bytes(self, key):
        r = self.client.get_object(Bucket=self.bucket, Key=key)
        return r["Body"].read()


    def upload_part_copy(self, key, upload_id, part_no, src_key, start, length, log=None):
        def do():
            return self.client.upload_part_copy(
                Bucket=self.bucket, Key=key, UploadId=upload_id, PartNumber=part_no,
                CopySource={"Bucket": self.bucket, "Key": src_key},
                CopySourceRange=f"bytes={start}-{start + length - 1}")
        r = _retry(do, f"upload_part_copy {part_no}", log)
        return r["CopyPartResult"]["ETag"].strip('"')

    def get_range(self, key, start, length):
        r = self.client.get_object(Bucket=self.bucket, Key=key,
                                   Range=f"bytes={start}-{start + length - 1}")
        return r["Body"].read()

    def delete_object(self, key):
        self.client.delete_object(Bucket=self.bucket, Key=key)

    def open_stream(self, key):
        """Returns (readable_stream, total_size) for a single-pass download."""
        r = self.client.get_object(Bucket=self.bucket, Key=key)
        return r["Body"], r["ContentLength"]

    def download_file(self, key, path):
        self.client.download_file(self.bucket, key, path)

    def list_keys(self, prefix, suffix=None):
        keys = []
        token = None
        while True:
            kw = dict(Bucket=self.bucket, Prefix=prefix)
            if token:
                kw["ContinuationToken"] = token
            r = self.client.list_objects_v2(**kw)
            for o in r.get("Contents", []):
                if suffix is None or o["Key"].endswith(suffix):
                    keys.append(o["Key"])
            if not r.get("IsTruncated"):
                break
            token = r.get("NextContinuationToken")
        return keys


class LocalDirStore:
    """Test double: emulates multipart S3 semantics into a local directory."""

    def __init__(self, root):
        self.root = root
        os.makedirs(root, exist_ok=True)

    def _path(self, key):
        p = os.path.join(self.root, key.replace("/", os.sep))
        os.makedirs(os.path.dirname(p), exist_ok=True)
        return p

    def head_bucket(self):
        assert os.path.isdir(self.root)

    def initiate(self, key):
        uid = base64.b16encode(os.urandom(8)).decode()
        os.makedirs(self._path(key) + f".mpu-{uid}", exist_ok=True)
        return uid

    def upload_part(self, key, upload_id, part_no, data, log=None):
        partfile = os.path.join(self._path(key) + f".mpu-{upload_id}", f"{part_no:06d}")
        with open(partfile, "wb") as f:
            f.write(data)
        return hashlib.md5(data).hexdigest()

    def complete(self, key, upload_id, parts, log=None):
        d = self._path(key) + f".mpu-{upload_id}"
        md5s = []
        with open(self._path(key), "wb") as out:
            for n, _e in sorted(parts):
                with open(os.path.join(d, f"{n:06d}"), "rb") as f:
                    data = f.read()
                md5s.append(hashlib.md5(data).digest())
                out.write(data)
        for fn in os.listdir(d):
            os.remove(os.path.join(d, fn))
        os.rmdir(d)
        et = hashlib.md5(b"".join(md5s)).hexdigest() + f"-{len(parts)}"
        return et

    def abort(self, key, upload_id):
        d = self._path(key) + f".mpu-{upload_id}"
        if os.path.isdir(d):
            for fn in os.listdir(d):
                os.remove(os.path.join(d, fn))
            os.rmdir(d)

    def head_object(self, key):
        p = self._path(key)
        if not os.path.isfile(p):
            return None
        return {"size": os.path.getsize(p), "etag": ""}

    def put_bytes(self, key, data):
        with open(self._path(key), "wb") as f:
            f.write(data)

    def get_bytes(self, key):
        with open(self._path(key), "rb") as f:
            return f.read()


    def upload_part_copy(self, key, upload_id, part_no, src_key, start, length, log=None):
        with open(self._path(src_key), "rb") as f:
            f.seek(start)
            data = f.read(length)
        return self.upload_part(key, upload_id, part_no, data, log)

    def get_range(self, key, start, length):
        with open(self._path(key), "rb") as f:
            f.seek(start)
            return f.read(length)

    def delete_object(self, key):
        p = self._path(key)
        if os.path.isfile(p):
            os.remove(p)

    def open_stream(self, key):
        f = open(self._path(key), "rb")
        return f, os.path.getsize(self._path(key))

    def download_file(self, key, path):
        with open(self._path(key), "rb") as f, open(path, "wb") as o:
            while True:
                b = f.read(1 << 22)
                if not b:
                    break
                o.write(b)

    def list_keys(self, prefix, suffix=None):
        keys = []
        base = os.path.join(self.root, prefix.replace("/", os.sep))
        for dp, _dn, fns in os.walk(base if os.path.isdir(base) else self.root):
            for fn in fns:
                full = os.path.join(dp, fn)
                rel = os.path.relpath(full, self.root).replace(os.sep, "/")
                if rel.startswith(prefix) and (suffix is None or rel.endswith(suffix)):
                    keys.append(rel)
        return keys


class TailUploader(threading.Thread):
    """Uploads a growing spool file as S3 parts while 7-Zip writes it.
    Emits full PART_SIZE parts as data becomes available; call finish() after
    the writer is done to upload the final (possibly short) part and complete."""

    def __init__(self, store, key, spool_path, log=None, cancel_event=None):
        super().__init__(daemon=True)
        self.store = store
        self.key = key
        self.spool_path = spool_path
        self.log = log or (lambda m: None)
        self.cancel_event = cancel_event or threading.Event()
        self.parts = []
        self.uploaded_bytes = 0
        self.etag = None
        self.error = None
        self._upload_id = None
        self._pending = bytearray()
        self._writer_done = threading.Event()
        self._finalized = threading.Event()

    def run(self):
        try:
            self._upload_id = self.store.initiate(self.key)
            pos = 0
            part_no = 1
            f = None
            while f is None and not self.cancel_event.is_set():
                try:
                    f = open(self.spool_path, "rb")
                except OSError:
                    time.sleep(0.05)
            if f is None:
                return
            with f:
                while not (self._writer_done.is_set() and pos >= self._spool_size()):
                    if self.cancel_event.is_set():
                        return
                    f.seek(pos)
                    chunk = f.read(1 << 20)
                    if chunk:
                        pos += len(chunk)
                        self._pending.extend(chunk)
                        while len(self._pending) >= PART_SIZE:
                            self._send(part_no, bytes(self._pending[:PART_SIZE]))
                            part_no += 1
                            del self._pending[:PART_SIZE]
                    else:
                        time.sleep(0.05)
                # final short part (if any)
                if self._pending and not self.cancel_event.is_set():
                    self._send(part_no, bytes(self._pending))
                    self._pending.clear()
            if not self.cancel_event.is_set():
                self.etag = self.store.complete(self.key, self._upload_id,
                                                self.parts, self.log)
        except Exception as e:
            self.error = e
            self.log(f"  uploader error: {e}")
            self.abort()
        finally:
            self._finalized.set()

    def _spool_size(self):
        try:
            return os.path.getsize(self.spool_path)
        except OSError:
            return 0

    def _send(self, part_no, data):
        et = self.store.upload_part(self.key, self._upload_id, part_no, data, self.log)
        self.parts.append((part_no, et))
        self.uploaded_bytes += len(data)

    def writer_done(self):
        self._writer_done.set()

    def finish(self, timeout=None):
        """Wait for completion; returns final ETag. Raises on error."""
        self._finalized.wait(timeout)
        if not self._finalized.is_set():
            raise UploadError("uploader did not finalize in time")
        if self.error:
            raise UploadError(str(self.error))
        if self.cancel_event.is_set():
            raise UploadError("upload cancelled")
        return self.etag

    def abort(self):
        if self._upload_id:
            self.store.abort(self.key, self._upload_id)
