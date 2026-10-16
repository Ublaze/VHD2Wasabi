"""Volume enumeration and raw sequential reading of a VSS shadow device."""
import ctypes
import struct

import win32api
import win32con
import win32file
import pywintypes

IOCTL_DISK_GET_LENGTH_INFO = 0x0007405C
CHUNK = 4 * 1024 * 1024  # 4 MiB aligned
TAIL_PAD_WINDOW = 1 * 1024 * 1024  # zero-fill only allowed within last 1 MiB


class ShortReadError(RuntimeError):
    pass


def tolerant_chunks(read_at, length, chunk, log=None, start=0):
    """Yield exactly `length - start` bytes from `read_at(pos, n) -> bytes`.

    Raw devices (esp. VSS shadow devices) can report a length whose final
    sector(s) are unreadable. Like `dd conv=sync,noerror`: a short/failed
    read is retried once; if still short AND the gap lies within the last
    TAIL_PAD_WINDOW bytes of the device, the remainder is zero-filled with a
    loud warning. A short read anywhere earlier is a hard ShortReadError.
    """
    pos = start
    while pos < length:
        want = min(chunk, length - pos)
        if want % 512:
            want += 512 - (want % 512)
        try:
            data = read_at(pos, want)
        except Exception as e:
            data = b""
            read_err = e
        else:
            read_err = None
        if len(data) < want:
            # one retry at the position after what we got
            retry_at = pos + len(data)
            retry_want = min(chunk, length - retry_at)
            if retry_want % 512:
                retry_want += 512 - (retry_want % 512)
            if retry_want > 0:
                try:
                    data += read_at(retry_at, retry_want)
                    read_err = None
                except Exception:
                    pass
        got = len(data)
        pos += got
        if got:
            yield data
        short = want - got
        if short <= 0:
            continue
        # this read came back short; the gap runs from `pos` onward
        if pos >= length - TAIL_PAD_WINDOW:
            pad = length - pos
            if log:
                log(f"WARNING: tail {pad} bytes unreadable on shadow device"
                    + (f" ({read_err})" if read_err else "")
                    + " — zero-padded (dd conv=sync,noerror semantics)")
            pos = length
            if pad > 0:
                yield b"\x00" * pad
            return
        raise ShortReadError(
            f"short read at offset {pos:,}: {short:,} bytes unreadable "
            f"outside tail pad window"
            + (f" ({read_err})" if read_err else ""))



class VolumeInfo:
    def __init__(self, letter, label, total, free):
        self.letter = letter          # 'C'
        self.label = label or ""
        self.total = total
        self.free = free

    @property
    def used(self):
        return self.total - self.free

    @property
    def display_name(self):
        return self.label if self.label else f"Volume {self.letter}:"

    def safe_name(self):
        base = self.label if self.label else self.letter
        return "".join(c if c.isalnum() or c in "-_." else "_" for c in base) or "VOL"


def list_fixed_volumes():
    """All fixed (non-removable) volumes with drive letters."""
    out = []
    for d in win32api.GetLogicalDriveStrings().split("\x00"):
        if not d:
            continue
        root = d  # 'C:\\'
        try:
            if win32file.GetDriveType(root) != win32con.DRIVE_FIXED:
                continue
            label, _serial, _maxlen, _flags, _fs = win32api.GetVolumeInformation(root)
            free_user, total, _free_total = win32file.GetDiskFreeSpaceEx(root)
            out.append(VolumeInfo(root[0], label, total, free_user))
        except pywintypes.error:
            continue
    return out


class VolumeReader:
    """Sequential raw reader over a shadow device with seek support."""

    def __init__(self, device_path, chunk=CHUNK, log=None):
        self.device_path = device_path
        self.chunk = chunk
        self.log = log
        self.handle = win32file.CreateFile(
            device_path,
            win32con.GENERIC_READ,
            win32con.FILE_SHARE_READ | win32con.FILE_SHARE_WRITE,
            None,
            win32con.OPEN_EXISTING,
            0,
            None,
        )
        raw = win32file.DeviceIoControl(self.handle, IOCTL_DISK_GET_LENGTH_INFO, None, 8)
        self.length = struct.unpack("<q", raw)[0]
        self.pos = 0

    def seek(self, offset):
        if offset % 512:
            raise ValueError("seek offset must be sector aligned")
        win32file.SetFilePointer(self.handle, offset, win32con.FILE_BEGIN)
        self.pos = offset

    def read_at(self, pos, n):
        win32file.SetFilePointer(self.handle, pos, win32con.FILE_BEGIN)
        _rc, data = win32file.ReadFile(self.handle, n)
        return data

    def read_chunks(self):
        """Yield byte chunks until end of device (dd-style tail tolerance)."""
        for c in tolerant_chunks(self.read_at, self.length, self.chunk, self.log,
                             start=self.pos):
            self.pos += len(c)
            yield c

    def close(self):
        try:
            win32file.CloseHandle(self.handle)
        except Exception:
            pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()
