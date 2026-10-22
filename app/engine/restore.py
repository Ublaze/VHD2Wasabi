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


def _sanitize(s):
    return "".join(c if c.isalnum() or c in "-_" else "_" for c in str(s)) \
        or "unknown"


def output_name(manifest):
    """<server>_<identity>.vhd — identity is the original drive letter for
    volume captures (fallback: label), Disk<N> for disk captures."""
    server = _sanitize(manifest.get("server") or "unknown")
    vol = manifest.get("volume", "")
    if manifest.get("capture_kind") == "disk" or vol.startswith("Disk"):
        ident = vol or "Disk"
    else:
        ident = manifest.get("drive_letter")
        if not ident:
            # backward compat: "C: (LABEL)" -> letter C; "(LABEL)" or plain
            # -> label
            if ":" in vol:
                ident = vol.split(":")[0]
            elif "(" in vol and vol.endswith(")"):
                ident = vol[vol.index("(") + 1:-1]
            else:
                ident = vol or "volume"
    return f"{server}_{_sanitize(ident)}.vhd"


def _unique_path(dest_dir, filename):
    """Never silently overwrite: append ' (n)' if the file exists."""
    base, ext = os.path.splitext(filename)
    candidate = os.path.join(dest_dir, filename)
    n = 2
    while os.path.exists(candidate):
        candidate = os.path.join(dest_dir, f"{base} ({n}){ext}")
        n += 1
    return candidate


def fetch_chunks(store, manifest, chunk_dir, log=lambda m: None,
                 progress=None, cancel_event=None):
    """Download per-segment .7z chunk files into chunk_dir: from the merged
    object (streamed in one pass, split at the manifest's byte offsets) when
    merged_key exists, else per-segment objects. Returns [(seg, path)].
    Caller owns chunk_dir cleanup."""
    segments = manifest.get("segments", [])
    n = len(segments)
    chunk_files = []
    downloaded = 0

    def pump_to_file(stream, size, path):
        nonlocal downloaded
        with open(path, "wb") as f:
            left = size
            while left > 0:
                if cancel_event and cancel_event.is_set():
                    raise RestoreError("cancelled")
                b = stream.read(min(DL_CHUNK, left))
                if not b:
                    raise RestoreError("download truncated")
                f.write(b)
                left -= len(b)
                downloaded += len(b)
                if progress:
                    progress("downloading", downloaded, 0, None, n)

    merged_key = manifest.get("merged_key")
    if merged_key:
        offsets = {o["index"]: o for o in manifest["merged"]["offsets"]}
        log(f"streaming merged object {merged_key}")
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
            log(f"downloading {s['key'].rsplit('/', 1)[-1]}")
            store.download_file(s["key"], cf)
            head = os.path.getsize(cf)
            downloaded += head
            if head != s["compressed_size"]:
                raise RestoreError(f"download size mismatch for {s['key']}")
            if progress:
                progress("downloading", downloaded, 0, s["index"], n)
            chunk_files.append((s, cf))
    return chunk_files


def extract_check(sevenzip, chunk_path, seg, sink=None, read_cb=None):
    """`7za t` + streamed `7za x -so`; bytes go to `sink` (file obj) if given.
    Always verifies extracted size + SHA-256 against the manifest segment.
    Raises RestoreError on any failure; returns extracted byte count."""
    t = subprocess.run([sevenzip, "t", chunk_path], capture_output=True,
                       timeout=7200)
    if t.returncode != 0:
        raise RestoreError(f"7-Zip integrity test failed for segment "
                           f"{seg['index'] + 1}")
    p = subprocess.Popen([sevenzip, "x", "-so", chunk_path],
                         stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    hasher = hashlib.sha256()
    got = 0
    while True:
        b = p.stdout.read(DL_CHUNK)
        if not b:
            break
        if sink is not None:
            sink.write(b)
        hasher.update(b)
        got += len(b)
        if read_cb:
            read_cb(len(b))
    p.wait()
    if p.returncode != 0:
        raise RestoreError(f"7-Zip extract failed for segment {seg['index'] + 1}")
    if got != seg["raw_size"] or hasher.hexdigest() != seg["raw_sha256"]:
        raise RestoreError(
            f"segment {seg['index'] + 1} hash/size mismatch after extract")
    return got


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
    out_path = _unique_path(dest_dir, output_name(manifest))
    chunk_dir = tempfile.mkdtemp(prefix=".vhd2w-chunks-", dir=dest_dir)
    n = len(segments)
    extracted = 0

    try:
        log("restore: fetching segment data")
        chunk_files = fetch_chunks(store, manifest, chunk_dir, log=log,
                                   progress=progress, cancel_event=cancel_event)
        downloaded = sum(os.path.getsize(cf) for _s, cf in chunk_files)

        log(f"restore: writing {out_path}")
        with open(out_path, "wb") as out:
            for i, (s, cf) in enumerate(chunk_files):
                if cancel_event and cancel_event.is_set():
                    raise RestoreError("restore cancelled")

                def cb(cnt, i=i):
                    nonlocal extracted
                    extracted += cnt
                    if progress:
                        progress("extracting", downloaded, extracted, i + 1, n)
                extract_check(sevenzip, cf, s, sink=out, read_cb=cb)
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
