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
                 delete_segments_after_merge=False):
        self.reader_factory = reader_factory
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
        vname = vol.safe_name()
        key_prefix = f"{self.key_prefix}/{self.server_name}/{date}/{vname}" \
            if self.key_prefix else f"{self.server_name}/{date}/{vname}"
        manifest_key = f"{key_prefix}.manifest.json"

        state = None if self.start_fresh else manifest_mod.load_state(self.server_name, vname)
        if state and state.get("status") == "done":
            if self.merge_on_complete and not state.get("merged_key"):
                self._merge_volume(vol, state, manifest_key, key_prefix)
                return state
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
            raw_len = reader.length
            total_size = raw_len + 512  # + fixed VHD footer
            n_segments = (total_size + self.segment_size - 1) // self.segment_size

            if state is None or state.get("segment_size") != self.segment_size:
                if state is not None:
                    self.log(f"{vol.letter}: segment size changed — starting fresh")
                state = manifest_mod.new_state(self.server_name, f"{vol.letter}: ({vname})",
                                               total_size, self.segment_size, key_prefix)
                state["n_segments"] = n_segments

            done_segs = self._verified_segments(state)
            if len(done_segs) < len(state["segments"]):
                dropped = len(state["segments"]) - len(done_segs)
                if dropped:
                    self.log(f"{vol.letter}: {dropped} recorded segment(s) failed "
                             "remote verification — will redo")
                state["segments"] = done_segs

            resume_offset = sum(s["raw_size"] for s in done_segs)
            if resume_offset:
                self.log(f"{vol.letter}: resuming at segment {len(done_segs) + 1} "
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
                self._cb("status", f"{vol.letter}:",
                         f"segment {seg_index + 1}/{n_segments} -> {os.path.basename(key)}")
                seg = self._run_segment(reader, vol, seg_index, offset, target,
                                        raw_len, key)
                state["segments"].append(seg)
                manifest_mod.save_state(self.server_name, vname, state)
                self._cb("segment_done", f"{vol.letter}:", seg)
                offset += seg["raw_size"]
                seg_index += 1
                done_bytes = offset
                elapsed = max(time.time() - t0, 1e-6)
                self._cb("progress", f"{vol.letter}:", done_bytes, total_size,
                         {"overall": (done_bytes - resume_offset) / elapsed})

            state["image_sha256"] = hashlib.sha256(
                "".join(s["raw_sha256"] for s in state["segments"]).encode()
            ).hexdigest()
            state["status"] = "done"
            state["finished"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            manifest_mod.save_state(self.server_name, vname, state)
            try:
                manifest_mod.upload_manifest(self.store, manifest_key, state)
                self.log(f"{vol.letter}: manifest uploaded -> {manifest_key}")
            except Exception as e:
                self.log(f"{vol.letter}: WARNING manifest upload failed: {e}")
            if self.merge_on_complete and not state.get("merged_key"):
                self._merge_volume(vol, state, manifest_key, key_prefix)
            self._cb("volume_done", f"{vol.letter}:", state)
            return state

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

    # ---- merge -------------------------------------------------------------
    def _merge_volume(self, vol, state, manifest_key, key_prefix):
        """Verify segments, then compose one server-side object. Any failure
        leaves segment objects and state untouched."""
        from .merge import merge_segments
        merged_key = f"{key_prefix}.full.7zcat"
        try:
            self._cb("status", f"{vol.letter}:", "verifying before merge…")
            ok = verify_manifest(self.store, manifest_key, log=self.log,
                                 cancel_event=self.cancel_event)
            if not ok:
                self.log(f"{vol.letter}: merge SKIPPED — verify failed")
                return
            self._cb("status", f"{vol.letter}:", "merging (server-side)…")
            res = merge_segments(self.store, state["segments"], merged_key,
                                 log=self.log, cancel_event=self.cancel_event)
            state["merged_key"] = merged_key
            state["merged"] = res
            state["manifest_version"] = 2
            manifest_mod.save_state(self.server_name, vol.safe_name(), state)
            manifest_mod.upload_manifest(self.store, manifest_key, state)
            self.log(f"{vol.letter}: merged -> {merged_key} "
                     f"({res['total_size']:,} bytes, {res['parts']} part(s))")
            if self.delete_segments_after_merge:
                for s in state["segments"]:
                    try:
                        self.store.delete_object(s["key"])
                    except Exception as e:
                        self.log(f"  WARNING: could not delete {s['key']}: {e}")
                state["segments_deleted"] = True
                manifest_mod.save_state(self.server_name, vol.safe_name(), state)
                manifest_mod.upload_manifest(self.store, manifest_key, state)
                self.log(f"{vol.letter}: segment objects deleted "
                         "(merged object retained)")
        except Exception as e:
            self.log(f"{vol.letter}: merge FAILED ({e}) — segments and state "
                     "are untouched")

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

    def _run_segment(self, reader, vol, seg_index, offset, target, raw_len, key):
        sevenzip = find_sevenzip()
        if not sevenzip:
            raise PipelineError("7-Zip binary not found (pre-flight should have caught this)")
        spool = os.path.join(self.spool_dir, f"seg-{vol.letter}-{seg_index:04d}.7z")
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
                    self._cb("progress", f"{vol.letter}:", offset + fed,
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
def verify_manifest(store, manifest_key, log=print, result_cb=None,
                    cancel_event=None):
    """HeadObject + download + `7za t` + stream-extract hash check per segment."""
    m = manifest_mod.download_manifest(store, manifest_key)
    log(f"Verifying {manifest_key}")
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
