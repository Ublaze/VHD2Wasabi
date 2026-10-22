"""Orchestrates volume -> segments -> compress -> multipart upload,
with resume, cancel, progress callbacks, diff-area monitoring and verify."""
import hashlib
import os
import shutil
import subprocess
import tempfile
import threading
import time

from . import manifest as manifest_mod
from . import snapshot as snapshot_mod
from .compressor import SegmentCompressor, find_sevenzip
from .uploader import TailUploader, PART_SIZE
from .vhdfooter import build_footer
from .volumereader import VolumeReader

MIN_SEGMENT = 64 * 1024 * 1024
POLL_INTERVAL = 30
DIFF_WARN = 0.80
DIFF_ABORT = 0.95


def _san(s):
    """S3-safe basename component."""
    return "".join(c if c.isalnum() or c in "-_." else "_" for c in str(s)) \
        or "x"


class Cancelled(RuntimeError):
    pass


class PipelineError(RuntimeError):
    pass


def default_spool_dir():
    d = os.path.join(tempfile.gettempdir(), "VHD2Wasabi-spool")
    os.makedirs(d, exist_ok=True)
    return d


class Pipeline:
    """callbacks: log(msg), status(vol_name, stage_text),
    progress(vol_name, done_bytes, total_bytes, speeds{read,compress,upload} B/s),
    segment_done(vol_name, segdict), volume_done(vol_name, state),
    diff(vol_name, percent_used), verify_result(seg_index, ok, detail)
    All callbacks optional."""

    def __init__(self, store, key_prefix, server_name, segment_size_mb=10240,
                 level=5, start_fresh=False, spool_dir=None, callbacks=None,
                 use_vss=True, reader_factory=None, merge_on_complete=False,
                 delete_segments_after_merge=False, disk_reader_factory=None,
                 deep_verify=False):
        self.reader_factory = reader_factory
        self.disk_reader_factory = disk_reader_factory
        self.deep_verify = deep_verify
        self.merge_on_complete = merge_on_complete
        self.delete_segments_after_merge = delete_segments_after_merge
        self.store = store
        self.key_prefix = key_prefix.strip("/")
        self.server_name = server_name
        self.segment_size = max(MIN_SEGMENT, segment_size_mb * 1024 * 1024)
        if self.segment_size % 4096:
            self.segment_size -= self.segment_size % 4096
        self.level = level
        self.start_fresh = start_fresh
        self.spool_dir = spool_dir or default_spool_dir()
        self.cb = callbacks or {}
        self.use_vss = use_vss
        self.cancel_event = threading.Event()
        self.diff_abort = threading.Event()
        self._current_uploader = None
        self._current_compressor = None

    # ---- helpers ---------------------------------------------------------
    def log(self, msg):
        cb = self.cb.get("log")
        if cb:
            cb(msg)

    def _cb(self, name, *args):
        cb = self.cb.get(name)
        if cb:
            cb(*args)

    def cancel(self):
        self.cancel_event.set()
        if self._current_compressor:
            self._current_compressor.kill()
        if self._current_uploader:
            self._current_uploader.abort()

    # ---- diff area monitor ----------------------------------------------
    def _diff_monitor(self, vol_letter, stop):
        last_pct = None
        while not stop.wait(POLL_INTERVAL):
            try:
                entries = snapshot_mod.list_shadow_storage()
            except Exception:
                continue
            for e in entries:
                v = e.get("volume", "")
                if f"({vol_letter}:)" not in v and not v.startswith(f"{vol_letter}:"):
                    continue
                used, mx = e.get("used"), e.get("maximum")
                if not used or not mx or mx <= 0:
                    continue
                pct = used / mx
                pct_int = int(pct * 100)
                if pct_int != last_pct:  # only report on change
                    last_pct = pct_int
                    self._cb("diff", f"{vol_letter}:", pct)
                if pct >= DIFF_ABORT:
                    self.log(f"  DIFF AREA {pct:.0%} FULL on {vol_letter}: — aborting cleanly")
                    self.diff_abort.set()
                    self.cancel()
                    return
                if pct >= DIFF_WARN:
                    self.log(f"  WARNING: diff area {pct:.0%} full on {vol_letter}:")

    # ---- main entry -------------------------------------------------------
    def run_volumes(self, volumes):
        results = {}
        for v in volumes:
            if self.cancel_event.is_set():
                break
            results[v.letter] = self.run_volume(v)
        return results

    def run_volume(self, vol):
        date = time.strftime("%Y-%m-%d")
        # object basename = <server>-<letter> (e.g. TRX3APP-C); state is keyed
        # by drive letter, falling back to the legacy label-based state name
        # so completed/in-progress captures from older versions stay matched
        basename = f"{_san(self.server_name)}-{vol.letter}"
        state_name = vol.letter
        legacy_name = vol.safe_name()
        key_prefix = f"{self.key_prefix}/{self.server_name}/{date}/{basename}" \
            if self.key_prefix else f"{self.server_name}/{date}/{basename}"
        manifest_key = f"{key_prefix}.manifest.json"

        state = None
        if not self.start_fresh:
            state = manifest_mod.load_state(self.server_name, state_name) \
                or manifest_mod.load_state(self.server_name, legacy_name)
        if state and state.get("key_prefix"):
            # resume in place under the ORIGINAL key prefix (56b73e0 rule) —
            # never rename or re-upload existing objects
            key_prefix = state["key_prefix"]
            manifest_key = f"{key_prefix}.manifest.json"
        vname = state_name
        if state and state.get("status") == "done":
            orig_prefix = state.get("key_prefix") or key_prefix
            orig_manifest = f"{orig_prefix}.manifest.json"
            if self.merge_on_complete and not state.get("merged_key"):
                # use the ORIGINAL key prefix from the saved state, not today's
                # date — the segments/manifest live under the capture date
                self._merge_volume(f"{vol.letter}:", vname, state,
                                   orig_manifest, orig_prefix)
                return state
            if self.delete_segments_after_merge and state.get("merged_key") \
                    and not state.get("segments_deleted"):
                # merged in an earlier run — apply the verified-delete gate now
                self._delete_segments_gated(f"{vol.letter}:", vname, state,
                                            orig_manifest)
            self.log(f"{vol.letter}: already complete in saved state — skipped")
            self._cb("volume_done", f"{vol.letter}:", state)
            return state

        shadow_id = None
        reader = None
        stop_mon = threading.Event()
        try:
            if self.reader_factory:
                reader = self.reader_factory(vol)
                device = "(injected reader)"
            elif self.use_vss:
                self._cb("status", f"{vol.letter}:", "creating VSS snapshot…")
                shadow_id, device = snapshot_mod.create_snapshot(f"{vol.letter}:\\")
                self.log(f"{vol.letter}: snapshot {shadow_id} -> {device}")
            else:
                device = f"\\\\?\\{vol.letter}:"
            if not self.reader_factory:
                mon = threading.Thread(target=self._diff_monitor, args=(vol.letter, stop_mon),
                                       daemon=True)
                mon.start()
                reader = VolumeReader(device, log=self.log)
            return self._stream_segments(f"{vol.letter}:", vname, vol.letter,
                                         f"{vol.letter}: ({vol.safe_name()})", reader,
                                         state, key_prefix, manifest_key,
                                         extra={"capture_kind": "volume",
                                                "drive_letter": vol.letter,
                                                "label": vol.label})

        except Cancelled:
            self.log(f"{vol.letter}: cancelled — state preserved for resume")
            raise
        finally:
            stop_mon.set()
            if reader:
                reader.close()
            if shadow_id:
                try:
                    snapshot_mod.delete_snapshot(shadow_id)
                    self.log(f"{vol.letter}: snapshot deleted")
                except Exception as e:
                    self.log(f"{vol.letter}: WARNING snapshot delete failed: {e}")

    # ---- shared streaming core --------------------------------------------
    def _stream_segments(self, display, vname, tag, title, reader, state,
                         key_prefix, manifest_key, extra=None):
        """Segments + footer + manifest + optional merge. Shared by volume and
        whole-disk capture. `display` = log prefix (e.g. 'C:' or 'Disk0'),
        `vname` = state file name, `tag` = spool/callback tag, `title` =
        manifest 'volume' field, `extra` = extra manifest fields (disk mode)."""
        raw_len = reader.length
        total_size = raw_len + 512  # + fixed VHD footer
        n_segments = (total_size + self.segment_size - 1) // self.segment_size

        if state is None or state.get("segment_size") != self.segment_size:
            if state is not None:
                self.log(f"{display} segment size changed — starting fresh")
            state = manifest_mod.new_state(self.server_name, title,
                                           total_size, self.segment_size, key_prefix)
            state["n_segments"] = n_segments
        if extra:
            state.update(extra)

        done_segs = self._verified_segments(state)
        if len(done_segs) < len(state["segments"]):
            dropped = len(state["segments"]) - len(done_segs)
            if dropped:
                self.log(f"{display} {dropped} recorded segment(s) failed "
                         "remote verification — will redo")
            state["segments"] = done_segs

        resume_offset = sum(s["raw_size"] for s in done_segs)
        if resume_offset:
            self.log(f"{display} resuming at segment {len(done_segs) + 1} "
                     f"(byte offset {resume_offset:,})")
        reader.seek(resume_offset)

        seg_index = len(done_segs)
        offset = resume_offset
        t0 = time.time()
        done_bytes = resume_offset

        while offset < total_size:
            if self.cancel_event.is_set():
                raise Cancelled()
            target = min(self.segment_size, total_size - offset)
            key = f"{key_prefix}.part{seg_index + 1:03d}.7z"
            self._cb("status", display,
                     f"segment {seg_index + 1}/{n_segments} -> {os.path.basename(key)}")
            seg = self._run_segment(reader, tag, display, seg_index, offset,
                                    target, raw_len, key)
            state["segments"].append(seg)
            manifest_mod.save_state(self.server_name, vname, state)
            self._cb("segment_done", display, seg)
            offset += seg["raw_size"]
            seg_index += 1
            done_bytes = offset
            elapsed = max(time.time() - t0, 1e-6)
            self._cb("progress", display, done_bytes, total_size,
                     {"overall": (done_bytes - resume_offset) / elapsed})

        state["image_sha256"] = hashlib.sha256(
            "".join(s["raw_sha256"] for s in state["segments"]).encode()
        ).hexdigest()
        state["status"] = "done"
        state["finished"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        manifest_mod.save_state(self.server_name, vname, state)
        try:
            manifest_mod.upload_manifest(self.store, manifest_key, state)
            self.log(f"{display} manifest uploaded -> {manifest_key}")
        except Exception as e:
            self.log(f"{display} WARNING manifest upload failed: {e}")
        if self.merge_on_complete and not state.get("merged_key"):
            self._merge_volume(display, vname, state, manifest_key, key_prefix)
        self._cb("volume_done", display, state)
        return state

    # ---- whole-disk capture -------------------------------------------------
    def run_disks(self, disk_numbers):
        results = {}
        for n in disk_numbers:
            if self.cancel_event.is_set():
                break
            results[n] = self.run_disk(n)
        return results

    def run_disk(self, disk_number):
        from .diskreader import LayoutChangedError, build_disk_reader, layouts_equal
        date = time.strftime("%Y-%m-%d")
        vname = f"Disk{disk_number}"  # state name (stable identity)
        basename = f"{_san(self.server_name)}-{vname}"  # object basename
        key_prefix = f"{self.key_prefix}/{self.server_name}/{date}/{basename}" \
            if self.key_prefix else f"{self.server_name}/{date}/{basename}"
        manifest_key = f"{key_prefix}.manifest.json"
        display = vname

        state = None if self.start_fresh else manifest_mod.load_state(self.server_name, vname)
        if state and state.get("key_prefix"):
            key_prefix = state["key_prefix"]
            manifest_key = f"{key_prefix}.manifest.json"
        if state and state.get("status") == "done":
            orig_prefix = state.get("key_prefix") or key_prefix
            orig_manifest = f"{orig_prefix}.manifest.json"
            if self.merge_on_complete and not state.get("merged_key"):
                self._merge_volume(display, vname, state,
                                   orig_manifest, orig_prefix)
                return state
            if self.delete_segments_after_merge and state.get("merged_key") \
                    and not state.get("segments_deleted"):
                self._delete_segments_gated(display, vname, state,
                                            orig_manifest)
            self.log(f"{display}: already complete in saved state — skipped")
            self._cb("volume_done", display, state)
            return state

        reader = None
        cleanup = None
        stop_mon = threading.Event()
        try:
            if self.disk_reader_factory:
                reader, meta = self.disk_reader_factory(disk_number)
                cleanup = meta.get("cleanup")
            else:
                reader, meta, cleanup = build_disk_reader(
                    disk_number, log=self.log, snapshot=self.use_vss)
            layout_rec = meta["layout"]
            if state and state.get("layout"):
                if not layouts_equal(state["layout"], layout_rec):
                    raise LayoutChangedError(
                        f"{display}: live partition layout does not match the "
                        "frozen layout in saved state — aborting rather than "
                        "mixing layouts. Use 'Start fresh' to recapture.")
            extra = {
                "capture_kind": "disk",
                "disk_number": disk_number,
                "model": meta.get("model"),
                "partition_style": meta.get("style"),
                "layout": layout_rec,
                "regions": meta.get("regions"),
                "snapshot_ids": meta.get("snapshot_ids"),
            }
            for letter in meta.get("letters", []):
                threading.Thread(target=self._diff_monitor, args=(letter, stop_mon),
                                 daemon=True).start()
            return self._stream_segments(display, vname, vname, vname, reader,
                                         state, key_prefix, manifest_key,
                                         extra=extra)
        except Cancelled:
            self.log(f"{display}: cancelled — state preserved for resume")
            raise
        finally:
            stop_mon.set()
            if reader:
                reader.close()
            if cleanup:
                try:
                    cleanup()
                except Exception as e:
                    self.log(f"{display}: WARNING snapshot cleanup failed: {e}")

    # ---- merge -------------------------------------------------------------
    def _merge_volume(self, display, vname, state, manifest_key, key_prefix):
        """Verify segments, then compose one server-side object. Any failure
        leaves segment objects and state untouched."""
        from .merge import merge_segments
        merged_key = f"{key_prefix}.full.7zcat"
        try:
            self._cb("status", f"{display}:", "verifying before merge…")
            ok = verify_manifest(self.store, manifest_key, log=self.log,
                                 cancel_event=self.cancel_event,
                                 deep=self.deep_verify)
            if not ok:
                self.log(f"{display}: merge SKIPPED — verify failed")
                return
            self._cb("status", f"{display}:", "merging (server-side)…")
            res = merge_segments(self.store, state["segments"], merged_key,
                                 log=self.log, cancel_event=self.cancel_event)
            state["merged_key"] = merged_key
            state["merged"] = res
            state["manifest_version"] = 2
            manifest_mod.save_state(self.server_name, vname, state)
            manifest_mod.upload_manifest(self.store, manifest_key, state)
            self.log(f"{display}: merged -> {merged_key} "
                     f"({res['total_size']:,} bytes, {res['parts']} part(s))")
            if self.delete_segments_after_merge:
                self._delete_segments_gated(display, vname, state, manifest_key)
        except Exception as e:
            self.log(f"{display}: merge FAILED ({e}) — segments and state "
                     "are untouched")

    def _delete_segments_gated(self, display, vname, state, manifest_key):
        """Delete segment objects ONLY after the merged object verifies
        end-to-end (head size + per-chunk 7za t + streamed SHA-256 vs
        manifest). Failure keeps segments and logs a loud error."""
        if state.get("segments_deleted") or not state.get("merged_key"):
            return
        self._cb("status", f"{display}:",
                 "verifying merged object before deleting segments…")
        if self.deep_verify:
            self.log(f"{display}: DEEP verify of merged object before "
                     "deletion (downloads it once)")
            ok = verify_merged(self.store, state, log=self.log,
                               cancel_event=self.cancel_event)
        else:
            self.log(f"{display}: metadata verify of merged object before "
                     "deletion (no download)")
            ok = verify_meta(self.store, state, log=self.log,
                             cancel_event=self.cancel_event)
        if not ok:
            self.log(f"{display}: MERGED VERIFICATION FAILED — segment "
                     "objects KEPT (nothing deleted)")
            return
        for s in state["segments"]:
            try:
                self.store.delete_object(s["key"])
                self.log(f"  deleted {s['key']}")
            except Exception as e:
                self.log(f"  WARNING: could not delete {s['key']}: {e}")
        state["segments_deleted"] = True
        state["merged_verified"] = True
        state["merged_verified_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                                    time.gmtime())
        manifest_mod.save_state(self.server_name, vname, state)
        try:
            manifest_mod.upload_manifest(self.store, manifest_key, state)
        except Exception as e:
            self.log(f"{display}: WARNING manifest upload failed: {e}")
        self.log(f"{display}: merged object verified — segment objects "
                 "deleted (merged object retained)")

    def _verified_segments(self, state):
        ok = []
        for s in state.get("segments", []):
            if s.get("status") != "complete":
                break
            head = self.store.head_object(s["key"])
            if not head or head["size"] != s["compressed_size"]:
                break
            ok.append(s)
        return ok

    def _run_segment(self, reader, tag, display, seg_index, offset, target, raw_len, key):
        sevenzip = find_sevenzip()
        if not sevenzip:
            raise PipelineError("7-Zip binary not found (pre-flight should have caught this)")
        spool = os.path.join(self.spool_dir, f"seg-{tag}-{seg_index:04d}.7z")
        comp = SegmentCompressor(sevenzip, spool, self.level)
        up = TailUploader(self.store, key, spool, log=self.log,
                          cancel_event=self.cancel_event)
        self._current_compressor = comp
        self._current_uploader = up
        hasher = hashlib.sha256()
        fed = 0
        reader.seek(offset)  # absolute position; previous segment may have
        # stopped mid-chunk
        speeds = {"read": 0.0, "compress": 0.0, "upload": 0.0}
        try:
            comp.start()
            up.start()
            t_start = time.time()
            last_report = t_start
            footer = None
            # device bytes in this segment
            dev_end = min(offset + target, raw_len)
            remaining_dev = max(0, dev_end - offset)
            if offset + target > raw_len:
                footer = build_footer(raw_len)  # last 512 bytes of stream
            for chunk in reader.read_chunks():
                if self.cancel_event.is_set():
                    raise Cancelled()
                if remaining_dev <= 0:
                    break
                take = chunk[:remaining_dev]
                remaining_dev -= len(take)
                self._feed(comp, hasher, take)
                fed += len(take)
                now = time.time()
                if now - last_report >= 1.0:
                    elapsed = now - t_start
                    speeds = {
                        "read": fed / elapsed,
                        "compress": comp.compressed_size / elapsed,
                        "upload": up.uploaded_bytes / elapsed,
                    }
                    self._cb("progress", display, offset + fed,
                             offset + target, speeds)
                    last_report = now
            if footer is not None:
                self._feed(comp, hasher, footer)
                fed += len(footer)
            if fed != target:
                raise PipelineError(f"short read: fed {fed}, expected {target}")
            compressed_size = comp.finish()
            up.writer_done()
            etag = up.finish()
            return {
                "index": seg_index,
                "key": key,
                "offset": offset,
                "raw_size": fed,
                "raw_sha256": hasher.hexdigest(),
                "compressed_size": compressed_size,
                "etag": etag,
                "parts": up.part_details,
                "status": "complete",
            }
        except Exception:
            comp.kill()
            up.abort()
            raise
        finally:
            self._current_compressor = None
            self._current_uploader = None
            try:
                if os.path.exists(spool):
                    os.remove(spool)
            except OSError:
                pass

    def _feed(self, comp, hasher, data):
        hasher.update(data)
        try:
            comp.write(data)
        except (BrokenPipeError, OSError) as e:
            raise PipelineError(f"7-Zip stdin broke: {e}")


# ---- verify ---------------------------------------------------------------
SEVENZ_MAGIC = b"7z\xbc\xaf\x27\x1c"


def verify_meta(store, manifest, log=print, cancel_event=None):
    """ZERO-DOWNLOAD verification: HeadObject size + multipart-ETag recompute
    from the manifest's recorded part MD5s (old manifests without part hashes
    degrade to size-only with a note). Merged objects additionally get a
    range-GET of a few bytes at each segment offset to check the 7z magic —
    proving the merge assembled correctly. Bulk data is never downloaded."""
    from .uploader import recompute_multipart_etag
    all_ok = True

    def check(ok, msg):
        nonlocal all_ok
        all_ok = all_ok and ok
        log(f"  {'PASS' if ok else 'FAIL'} — {msg}")

    segs_deleted = bool(manifest.get("segments_deleted"))
    if segs_deleted:
        log("  segment objects were deleted after merge — verifying merged "
            "object only")
    for seg in manifest.get("segments", []):
        if cancel_event and cancel_event.is_set():
            log("verify cancelled")
            return False
        if segs_deleted:
            break
        head = store.head_object(seg["key"])
        label = seg["key"].rsplit("/", 1)[-1]
        if not head:
            check(False, f"{label}: object missing")
            continue
        if head["size"] != seg["compressed_size"]:
            check(False, f"{label}: size {head['size']:,} != manifest "
                         f"{seg['compressed_size']:,}")
            continue
        parts = seg.get("parts")
        if parts:
            recomputed = recompute_multipart_etag([p["md5"] for p in parts])
            check(head["etag"] == recomputed,
                  f"{label}: size ok, ETag recompute "
                  f"{'matches' if head['etag'] == recomputed else 'MISMATCH ' + head['etag'] + ' vs ' + recomputed}")
        else:
            check(True, f"{label}: size ok (no part hashes recorded — "
                        "size-only, captured before hash recording)")
    merged_key = manifest.get("merged_key")
    if merged_key:
        head = store.head_object(merged_key)
        expect = manifest.get("merged", {}).get("total_size")
        if not head:
            check(False, f"merged object {merged_key} missing")
        else:
            check(expect is None or head["size"] == expect,
                  f"merged size {head['size']:,}"
                  + (f" == manifest {expect:,}" if head["size"] == expect
                     else f" != manifest {expect:,}"))
            if head["size"] == expect or expect is None:
                for o in manifest.get("merged", {}).get("offsets", []):
                    magic = store.get_range(merged_key, o["offset"], 6)
                    check(magic == SEVENZ_MAGIC,
                          f"merged offset {o['offset']:,} (segment "
                          f"{o['index'] + 1}): 7z magic "
                          f"{'ok' if magic == SEVENZ_MAGIC else 'MISSING'}")
    log(f"metadata verify {'PASSED' if all_ok else 'FAILED'}")
    return all_ok


def verify_merged(store, manifest, log=print, cancel_event=None,
                  progress=None):
    """Verify the MERGED object end-to-end: HeadObject size, stream-download
    split into per-segment chunks at the manifest offsets, `7za t` + streamed
    `7za x -so` SHA-256 per chunk vs manifest. Proves restorability."""
    from .restore import extract_check, fetch_chunks
    merged_key = manifest.get("merged_key")
    if not merged_key:
        log("verify_merged: manifest has no merged_key")
        return False
    expected = manifest.get("merged", {}).get("total_size")
    head = store.head_object(merged_key)
    if not head:
        log(f"verify_merged: {merged_key} missing")
        return False
    if expected is not None and head["size"] != expected:
        log(f"verify_merged: size mismatch — remote {head['size']:,} vs "
            f"manifest {expected:,}")
        return False
    log(f"verify_merged: {merged_key} ({head['size']:,} bytes) — "
        "downloading and checking each chunk")
    sevenzip = find_sevenzip()
    tmpdir = tempfile.mkdtemp(prefix="vhd2w-vmerge-")
    all_ok = True
    try:
        chunks = fetch_chunks(store, manifest, tmpdir, log=log,
                              progress=progress, cancel_event=cancel_event)
        for seg, cf in chunks:
            if cancel_event and cancel_event.is_set():
                log("verify cancelled")
                return False
            try:
                got = extract_check(sevenzip, cf, seg)
                log(f"  merged part{seg['index'] + 1:03d}: PASS — "
                    f"{got:,} bytes, sha256 ok")
            except Exception as e:
                all_ok = False
                log(f"  merged part{seg['index'] + 1:03d}: FAIL — {e}")
        log(f"verify_merged {'PASSED' if all_ok else 'FAILED'}: {merged_key}")
        return all_ok
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def verify_manifest(store, manifest_key, log=print, result_cb=None,
                    cancel_event=None, use_merged=None, deep=False):
    """Verify a manifest. Default (deep=False): ZERO-DOWNLOAD metadata verify
    (sizes + recomputed multipart ETags + merged-object magic bytes). With
    deep=True: full download — merged object when present (else per-segment),
    `7za t` + streamed SHA-256 per chunk."""
    m = manifest_mod.download_manifest(store, manifest_key)
    if not deep:
        log(f"Verifying {manifest_key} (metadata-only, no download)")
        return verify_meta(store, m, log=log, cancel_event=cancel_event)
    if use_merged is None:
        use_merged = bool(m.get("merged_key"))
    if use_merged:
        if not m.get("merged_key"):
            log("no merged object in manifest — falling back to per-segment")
        else:
            log(f"Verifying {manifest_key} via MERGED object (deep) "
                f"{m['merged_key']}")
            return verify_merged(store, m, log=log, cancel_event=cancel_event)
    log(f"Verifying {manifest_key} (per-segment, deep)")
    log(f"  server={m.get('server')} volume={m.get('volume')} "
        f"size={m.get('total_size'):,} segments={len(m.get('segments', []))}")
    sevenzip = find_sevenzip()
    all_ok = True
    tmpdir = tempfile.mkdtemp(prefix="vhd2w-verify-")
    try:
        for seg in m.get("segments", []):
            if cancel_event and cancel_event.is_set():
                log("verify cancelled")
                return False
            ok, detail = _verify_segment(store, seg, sevenzip, tmpdir, log)
            all_ok = all_ok and ok
            if result_cb:
                result_cb(seg["index"], ok, detail)
            log(f"  part{seg['index'] + 1:03d}: {'PASS' if ok else 'FAIL'} — {detail}")
        log(f"verify {'PASSED' if all_ok else 'FAILED'}: {manifest_key}")
        return all_ok
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def _verify_segment(store, seg, sevenzip, tmpdir, log):
    head = store.head_object(seg["key"])
    if not head:
        return False, "object missing"
    if head["size"] != seg["compressed_size"]:
        return False, f"size mismatch: remote {head['size']} vs manifest {seg['compressed_size']}"
    tmp = os.path.join(tmpdir, f"seg{seg['index']:04d}.7z")
    try:
        store.download_file(seg["key"], tmp)
        if os.path.getsize(tmp) != seg["compressed_size"]:
            return False, "download truncated"
        t = subprocess.run([sevenzip, "t", tmp], capture_output=True, timeout=3600)
        if t.returncode != 0:
            return False, "7-Zip integrity test failed"
        hasher = hashlib.sha256()
        p = subprocess.Popen([sevenzip, "x", "-so", tmp], stdout=subprocess.PIPE,
                             stderr=subprocess.DEVNULL)
        got = 0
        while True:
            b = p.stdout.read(1 << 22)
            if not b:
                break
            hasher.update(b)
            got += len(b)
        p.wait()
        if p.returncode != 0:
            return False, "7-Zip extract failed"
        if got != seg["raw_size"]:
            return False, f"extracted size {got:,} != manifest {seg['raw_size']:,}"
        if hasher.hexdigest() != seg["raw_sha256"]:
            return False, "SHA-256 mismatch"
        return True, f"{seg['raw_size']:,} bytes, sha256 ok"
    finally:
        try:
            os.remove(tmp)
        except OSError:
            pass
