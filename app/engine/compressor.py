"""7-Zip compression of one segment.

The 7-Zip CLI cannot write an archive to stdout (`7z a ... -so` returns
"Not implemented"), so each segment is compressed to a spool file on disk
while the pipeline tail-uploads that file concurrently. Spool usage is
bounded to one compressed segment (not the whole image).
"""
import os
import subprocess
import sys
import threading
import time

# Configurable 7-Zip binary name (7za.exe bundled; 7zr.exe also works).
SEVENZIP = "7za.exe"


def find_sevenzip():
    """Locate the 7-Zip console binary: bundled (PyInstaller) or dev resources."""
    candidates = []
    if getattr(sys, "frozen", False):
        candidates.append(os.path.join(sys._MEIPASS, "resources", SEVENZIP))
        candidates.append(os.path.join(os.path.dirname(sys.executable), "resources", SEVENZIP))
    base = os.path.dirname(os.path.abspath(__file__))          # app/engine
    appdir = os.path.dirname(base)                             # app
    candidates.append(os.path.join(appdir, "resources", SEVENZIP))
    for c in candidates:
        if os.path.isfile(c):
            return c
    return None


class CompressError(RuntimeError):
    pass


class SegmentCompressor:
    """Feeds stdin of `7za a spool.7z -siData -mx=N` and tracks spool growth."""

    def __init__(self, sevenzip_path, spool_path, level=5):
        self.sevenzip_path = sevenzip_path
        self.spool_path = spool_path
        self.level = level
        self.proc = None
        self._stderr = bytearray()
        self._stderr_thread = None
        self.done = threading.Event()

    def start(self):
        if os.path.exists(self.spool_path):
            os.remove(self.spool_path)
        self.proc = subprocess.Popen(
            [self.sevenzip_path, "a", self.spool_path, "-siData",
             f"-mx={self.level}", "-t7z"],
            stdin=subprocess.PIPE, stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        self._stderr_thread = threading.Thread(target=self._drain, daemon=True)
        self._stderr_thread.start()

    def _drain(self):
        try:
            while True:
                b = self.proc.stderr.read(4096)
                if not b:
                    break
                self._stderr.extend(b)
        except Exception:
            pass

    def write(self, data):
        self.proc.stdin.write(data)

    @property
    def compressed_size(self):
        try:
            return os.path.getsize(self.spool_path)
        except OSError:
            return 0

    def finish(self):
        """Close stdin, wait for exit, return compressed size."""
        try:
            self.proc.stdin.close()
        except Exception:
            pass
        rc = self.proc.wait()
        self.done.set()
        if self._stderr_thread:
            self._stderr_thread.join(timeout=10)
        if rc != 0:
            raise CompressError(
                f"7-Zip exited {rc}: {bytes(self._stderr).decode(errors='replace')[-500:]}")
        size = self.compressed_size
        return size

    def kill(self):
        try:
            self.proc.kill()
        except Exception:
            pass
        self.done.set()

    def wait_flushed(self, timeout=30):
        """Wait until the spool file stops growing briefly (best effort)."""
        last = -1
        deadline = time.time() + timeout
        while time.time() < deadline:
            s = self.compressed_size
            if s == last:
                return
            last = s
            time.sleep(0.2)
