"""Volume enumeration and raw sequential reading of a VSS shadow device."""
import ctypes
import struct

import win32api
import win32con
import win32file
import pywintypes

IOCTL_DISK_GET_LENGTH_INFO = 0x0007405C
CHUNK = 4 * 1024 * 1024  # 4 MiB aligned


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

    def __init__(self, device_path, chunk=CHUNK):
        self.device_path = device_path
        self.chunk = chunk
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

    def read_chunks(self):
        """Yield byte chunks until end of device. Chunks are sector aligned,
        except possibly the final one which is padded to 512 bytes."""
        while self.pos < self.length:
            want = min(self.chunk, self.length - self.pos)
            if want % 512:
                want += 512 - (want % 512)
            _rc, data = win32file.ReadFile(self.handle, want)
            if not data:
                break
            self.pos += len(data)
            yield data

    def close(self):
        try:
            win32file.CloseHandle(self.handle)
        except Exception:
            pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()
