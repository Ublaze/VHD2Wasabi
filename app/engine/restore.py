"""Restore a captured volume from the bucket back to a local .vhd file.

- manifest with merged_key: the merged .7zcat object is stream-downloaded in
  one pass and split into per-segment temp chunk files at the exact byte
  offsets recorded in the manifest (RAM stays at a few MB; chunks live on the
  DESTINATION disk and are deleted as consumed).
- otherwise: segment objects are downloaded one by one.
Each chunk is extracted streaming (`7za x -so`) and appended to the output
VHD opened in 'ab' mode. Finally the VHD footer is validated.
"""
import hashlib
import os
import shutil
import subprocess
import tempfile

from .compressor import find_sevenzip
from .vhdfooter import validate_footer

DL_CHUNK = 4 * 1024 * 1024


class RestoreError(RuntimeError):
    pass


def _safe_name(manifest):
    vol = manifest.get("volume", "volume")
    # "C: (LABEL)" -> LABEL else C
    if "(" in vol and vol.endswith(")"):
        name = vol[vol.index("(") + 1:-1]
    else:
        name = vol.split(":")[0]
    return "".join(c if c.isalnum() or c in "-_." else "_" for c in name) or "volume"


def restore(store, manifest, dest_dir, log=lambda m: None,
            progress=None, cancel_event=None):
    """progress(stage_text, downloaded_bytes, extracted_bytes, seg_i, seg_n).
    Returns dict with output path, size, footer info."""
    sevenzip = find_sevenzip()
    if not sevenzip:
        raise RestoreError("7-Zip binary not found")
    segments = manifest.get("segments", [])
    if not segments:
        raise RestoreError("manifest has no segments")
    os.makedirs(dest_dir, exist_ok=True)
    out_path = os.path.join(dest_dir, f"{_safe_name(manifest)}.vhd")
    chunk_dir = tempfile.mkdtemp(prefix=".vhd2w-chunks-", dir=dest_dir)
    n = len(segments)
    downloaded = 0
    extracted = 0

    def pump_to_file(stream, size, path):
        nonlocal downloaded
        with open(path, "wb") as f:
            left = size
            while left > 0:
                if cancel_event and cancel_event.is_set():
                    raise RestoreError("restore cancelled")
                b = stream.read(min(DL_CHUNK, left))
                if not b:
                    raise RestoreError("download truncated")
                f.write(b)
                left -= len(b)
                downloaded += len(b)
                if progress:
                    progress("downloading", downloaded, extracted, None, n)

    try:
        chunk_files = []
        merged_key = manifest.get("merged_key")
        if merged_key:
            offsets = {o["index"]: o for o in manifest["merged"]["offsets"]}
            log(f"restore: streaming merged object {merged_key}")
            stream, _total = store.open_stream(merged_key)
            try:
                for s in segments:
                    o = offsets[s["index"]]
                    cf = os.path.join(chunk_dir, f"seg{s['index']:04d}.7z")
                    pump_to_file(stream, o["compressed_size"], cf)
                    chunk_files.append((s, cf))
            finally:
                try:
                    stream.close()
                except Exception:
                    pass
        else:
            for s in segments:
                cf = os.path.join(chunk_dir, f"seg{s['index']:04d}.7z")
                log(f"restore: downloading {s['key'].rsplit('/', 1)[-1]}")
                store.download_file(s["key"], cf)
                head = os.path.getsize(cf)
                downloaded += head
                if head != s["compressed_size"]:
                    raise RestoreError(f"download size mismatch for {s['key']}")
                if progress:
                    progress("downloading", downloaded, extracted, s["index"], n)
                chunk_files.append((s, cf))

        if os.path.exists(out_path):
            os.remove(out_path)
        with open(out_path, "ab") as out:
            for i, (s, cf) in enumerate(chunk_files):
                if cancel_event and cancel_event.is_set():
                    raise RestoreError("restore cancelled")
                if progress:
                    progress("extracting", downloaded, extracted, i + 1, n)
                p = subprocess.Popen([sevenzip, "x", "-so", cf],
                                     stdout=subprocess.PIPE,
                                     stderr=subprocess.DEVNULL)
                hasher = hashlib.sha256()
                got = 0
                while True:
                    b = p.stdout.read(DL_CHUNK)
                    if not b:
                        break
                    out.write(b)
                    hasher.update(b)
                    got += len(b)
                    extracted += len(b)
                p.wait()
                if p.returncode != 0:
                    raise RestoreError(f"7-Zip extract failed for segment {s['index'] + 1}")
                if got != s["raw_size"] or hasher.hexdigest() != s["raw_sha256"]:
                    raise RestoreError(
                        f"segment {s['index'] + 1} hash/size mismatch after extract")
                try:
                    os.remove(cf)
                except OSError:
                    pass

        size = os.path.getsize(out_path)
        footer_info = None
        with open(out_path, "rb") as f:
            f.seek(-512, os.SEEK_END)
            tail = f.read(512)
        try:
            footer_info = validate_footer(tail, expected_size=size - 512)
            log(f"restore: footer OK (type {footer_info['disk_type']}, "
                f"size {footer_info['current_size']:,})")
        except ValueError as e:
            log(f"restore: WARNING footer validation failed: {e}")
        if progress:
            progress("done", downloaded, extracted, n, n)
        return {"path": out_path, "size": size, "footer": footer_info}
    finally:
        shutil.rmtree(chunk_dir, ignore_errors=True)
