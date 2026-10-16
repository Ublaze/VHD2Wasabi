"""Server-side merge of per-segment objects into ONE object per volume.

The merged object (.7zcat) is the plain concatenation of the segment .7z
archives; the manifest records each segment's byte offset + compressed size
so restore can split the stream at exact boundaries without re-downloading
segment objects.

Rules:
- segments with stored size >= 5 MiB are added as UploadPartCopy parts
  (byte-range server-side copy, no data re-upload);
- runs of small segments are downloaded into memory and flushed as regular
  upload parts once the buffer reaches >= 5 MiB;
- a non-final part is never < 5 MiB; if the whole object is < 5 MiB a single
  put_object is used instead of multipart;
- any failure aborts the multipart upload and leaves segment objects intact.
"""

MIN_PART = 5 * 1024 * 1024      # S3 minimum part size (except final part)
COPY_PART_MAX = 1 * 1024**3     # cap per UploadPartCopy range (1 GiB):
# Wasabi only answers a copy when it finishes; multi-GB copies exceed client
# read timeouts, so big segments are copied as several consecutive parts.


class MergeError(RuntimeError):
    pass


def merge_offsets(segments):
    """Cumulative compressed offsets — deterministic from segment sizes."""
    offsets = []
    off = 0
    for s in segments:
        offsets.append({"index": s["index"], "offset": off,
                        "compressed_size": s["compressed_size"]})
        off += s["compressed_size"]
    return offsets, off


def merge_segments(store, segments, merged_key, log=lambda m: None,
                   cancel_event=None, copy_part_max=COPY_PART_MAX):
    """Compose segments into one object. Returns dict with merged_key,
    total_size, offsets, parts (multipart count or 1 for put_object)."""
    if not segments:
        raise MergeError("no segments to merge")
    offsets, total = merge_offsets(segments)
    log(f"merge: {len(segments)} segment(s), {total:,} compressed bytes -> {merged_key}")

    if total < MIN_PART:
        buf = b"".join(store.get_bytes(s["key"]) for s in segments)
        store.put_bytes(merged_key, buf)
        log("merge: total < 5 MiB — single put_object")
        return {"merged_key": merged_key, "total_size": total,
                "offsets": offsets, "parts": 1, "method": "put_object"}

    upload_id = store.initiate(merged_key)
    parts = []
    part_no = 1
    buf = bytearray()

    def emit(take):
        nonlocal part_no
        et = store.upload_part(merged_key, upload_id, part_no, take)
        parts.append((part_no, et))
        log(f"  merge: part {part_no} uploaded from memory ({len(take):,} bytes)")
        part_no += 1

    def flush_full():
        while len(buf) >= MIN_PART:
            take = bytes(buf[:MIN_PART])
            del buf[:MIN_PART]
            emit(take)

    def flush_final():
        if buf:
            emit(bytes(buf))
            buf.clear()

    try:
        for s in segments:
            if cancel_event and cancel_event.is_set():
                raise MergeError("merge cancelled")
            size = s["compressed_size"]
            if size >= MIN_PART:
                start = 0
                if buf:
                    # top the buffer up to MIN_PART from the head of this
                    # segment, flush it, then range-copy the remainder
                    need = MIN_PART - len(buf)
                    buf.extend(store.get_range(s["key"], 0, need))
                    flush_full()
                    start = need
                remaining = size - start
                while remaining > 0:
                    take = min(copy_part_max, remaining)
                    if take < MIN_PART:
                        # tail of this segment too small for a non-final part —
                        # stitch it through the memory buffer instead
                        buf.extend(store.get_range(s["key"], start, take))
                        flush_full()
                    else:
                        et = store.upload_part_copy(merged_key, upload_id,
                                                    part_no, s["key"],
                                                    start, take)
                        parts.append((part_no, et))
                        log(f"  merge: part {part_no} copied from "
                            f"{s['key'].rsplit('/', 1)[-1]} "
                            f"({take:,} bytes @ {start:,})")
                        part_no += 1
                    start += take
                    remaining -= take
            else:
                buf.extend(store.get_bytes(s["key"]))
                flush_full()
        flush_final()
        etag = store.complete(merged_key, upload_id, parts)
        log(f"merge: complete — {len(parts)} parts, etag {etag}")
        return {"merged_key": merged_key, "total_size": total,
                "offsets": offsets, "parts": len(parts), "etag": etag,
                "method": "multipart"}
    except Exception:
        store.abort(merged_key, upload_id)
        raise
