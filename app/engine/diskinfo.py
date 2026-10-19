"""Physical disk enumeration + layout.

Layout comes from IOCTL_DISK_GET_DRIVE_LAYOUT_EX (pywin32, no Storage CIM —
Get-Disk/Get-Partition hang on some servers). Model/size/letters/filesystems
come from WMI when responsive, with pywin32 fallbacks (IOCTL length, drive
probing) so enumeration still works when WMI/PowerShell is wedged.
BitLocker status uses the MicrosoftVolumeEncryption WMI namespace.
"""
import json
import re
import struct
import subprocess

import win32con
import win32file

IOCTL_DISK_GET_DRIVE_LAYOUT_EX = 0x00070050
IOCTL_DISK_GET_LENGTH_INFO = 0x0007405C
PARTITION_STYLE = {0: "MBR", 1: "GPT", 2: "RAW"}
LDM_GPT_TYPES = {
    "5808C8AA-7E8F-42E0-85D2-E1E90434CFB3",  # LDM metadata
    "AF9B60A0-1431-4F62-BC68-3311714A69AD",  # LDM data
}
VHD_MAX_BYTES = 2040 * 1024**3  # fixed-VHD practical limit (~2040 GiB)
_ENTRY_STRIDE = 144
_ENTRY_HDR = 48  # PartitionStyle(4) + PartitionCount(4) + layout union (40)
# PARTITION_INFORMATION_EX as returned by Win10/11 x64:
#   +0  PartitionStyle (4, +4 pad)
#   +8  StartingOffset (i64)
#   +16 PartitionLength (i64)
#   +24 PartitionNumber (u32)
#   +28 RewritePartition/IsServicePartition (2 x bool + pad)
#   +32 union: MBR {type u8 @32, boot u8 @33} / GPT {type GUID @32, id @48,
#       attrs @64, name WCHAR[36] @72}
WMI_TIMEOUT = 60


class DiskInfoError(RuntimeError):
    pass


def _guid(b):
    d1, d2, d3 = struct.unpack_from("<IHH", b, 0)
    d4 = b[8:10].hex()
    d5 = b[10:16].hex()
    return f"{{{d1:08X}-{d2:04X}-{d3:04X}-{d4[:2].upper()}{d4[2:].upper()}-{d5.upper()}}}"


def _wmi(query, timeout=WMI_TIMEOUT):
    """Run a WMI query via PowerShell, return list of dicts (JSON).
    Raises on timeout — callers must fall back."""
    cmd = ("[Console]::OutputEncoding=[Text.Encoding]::UTF8; "
           f"Get-WmiObject -Query \"{query}\" | ConvertTo-Json -Compress")
    p = subprocess.run(["powershell", "-NoProfile", "-Command", cmd],
                       capture_output=True, text=True, timeout=timeout)
    out = (p.stdout or "").strip()
    if not out:
        return []
    data = json.loads(out)
    return data if isinstance(data, list) else [data]


def _probe_physical_drives(max_n=16):
    """Existing \\.\\PhysicalDriveN numbers + sizes via IOCTL (no WMI)."""
    out = {}
    for n in range(max_n):
        path = f"\\\\.\\PhysicalDrive{n}"
        h = None
        for access in (win32con.GENERIC_READ, 0):
            try:
                h = win32file.CreateFile(
                    path, access,
                    win32con.FILE_SHARE_READ | win32con.FILE_SHARE_WRITE,
                    None, win32con.OPEN_EXISTING, 0, None)
                break
            except Exception:
                continue
        if h is None:
            continue
        try:
            raw = win32file.DeviceIoControl(h, IOCTL_DISK_GET_LENGTH_INFO, None, 8)
            out[n] = struct.unpack("<q", raw)[0]
        except Exception:
            out[n] = 0
        finally:
            win32file.CloseHandle(h)
    return out


def _drive_layout(disk_number):
    h = win32file.CreateFile(
        f"\\\\.\\PhysicalDrive{disk_number}", 0,  # no access needed for layout
        win32con.FILE_SHARE_READ | win32con.FILE_SHARE_WRITE,
        None, win32con.OPEN_EXISTING, 0, None)
    try:
        buf = win32file.DeviceIoControl(h, IOCTL_DISK_GET_DRIVE_LAYOUT_EX,
                                        None, 65536)
    finally:
        win32file.CloseHandle(h)
    style, count = struct.unpack_from("<II", buf, 0)
    parts = []
    for i in range(count):
        off = _ENTRY_HDR + i * _ENTRY_STRIDE
        if off + _ENTRY_STRIDE > len(buf):
            break
        pstyle = struct.unpack_from("<I", buf, off)[0]
        start, length, pnum = struct.unpack_from("<qqI", buf, off + 8)
        if length == 0:
            continue
        entry = {"number": pnum, "offset": start, "size": length,
                 "letter": None, "fs": None, "gpt_type": None,
                 "mbr_type": None, "type": None}
        if pstyle == 0:  # MBR
            ptype = buf[off + 32]
            boot = buf[off + 33]
            entry["mbr_type"] = ptype
            entry["type"] = f"MBR:0x{ptype:02X}"
            entry["is_boot"] = bool(boot)
        elif pstyle == 1:  # GPT
            gt = _guid(buf[off + 32:off + 48])
            entry["gpt_type"] = gt
            name_raw = buf[off + 72:off + 144]
            name = name_raw.decode("utf-16-le", errors="ignore").split("\x00")[0]
            entry["type"] = name or gt
        parts.append(entry)
    return PARTITION_STYLE.get(style, "RAW"), parts


def list_disks(log=lambda m: None):
    drives = _probe_physical_drives()
    models = {}
    try:
        for d in _wmi("SELECT Index, Model, Size FROM Win32_DiskDrive"):
            models[d["Index"]] = (d.get("Model") or "").strip()
    except Exception as e:
        log(f"WARNING: WMI disk query failed ({e}) — models unavailable")
    # letter + fs per logical disk (best effort)
    part_letter = {}
    ldisks = {}
    try:
        for ld in _wmi("SELECT DeviceID, FileSystem FROM Win32_LogicalDisk"):
            ldisks[ld["DeviceID"].rstrip(":")] = ld
        for a in _wmi("SELECT Antecedent, Dependent FROM Win32_LogicalDiskToPartition"):
            m = re.search(r'Disk #(\d+), Partition #(\d+)', a.get("Antecedent", ""))
            l = re.search(r'"([A-Z]:)"', a.get("Dependent", ""))
            if m and l:
                part_letter[(int(m.group(1)), int(m.group(2)))] = l.group(1)[0]
    except Exception as e:
        log(f"WARNING: WMI volume mapping failed ({e}) — partitions will be "
            "read live (no snapshots)")
    disks = []
    for idx, size in sorted(drives.items()):
        style, parts = _drive_layout(idx)
        if not size and parts:  # GET_LENGTH_INFO unavailable (non-admin probe)
            size = max(p["offset"] + p["size"] for p in parts)
        for p in parts:
            letter = part_letter.get((idx, p["number"]))
            if letter:
                p["letter"] = letter
                ld = ldisks.get(letter)
                if ld:
                    p["fs"] = ld.get("FileSystem")
        disks.append({"number": idx,
                      "model": models.get(idx, f"PhysicalDrive{idx}"),
                      "size": size,
                      "style": style,
                      "partitions": sorted(parts, key=lambda x: x["offset"])})
    return disks


def is_dynamic(disk):
    """LDM dynamic disk detection: MBR type 0x42 or GPT LDM type GUIDs."""
    for p in disk["partitions"]:
        gt = (p.get("gpt_type") or "").upper()
        if gt and gt.strip("{}") in LDM_GPT_TYPES:
            return True
        if p.get("mbr_type") == 0x42:
            return True
    return False


def bitlocker_letters():
    """Letters of BitLocker-protected volumes. Returns (set, error_or_None)."""
    try:
        p = subprocess.run(
            ["powershell", "-NoProfile", "-Command",
             "Get-WmiObject -Namespace root\\CIMv2\\Security\\MicrosoftVolumeEncryption"
             " -Class Win32_EncryptableVolume | Select-Object DriveLetter,"
             "ProtectionStatus | ConvertTo-Json -Compress"],
            capture_output=True, text=True, timeout=60)
        out = (p.stdout or "").strip()
        if not out:
            return set(), None
        data = json.loads(out)
        rows = data if isinstance(data, list) else [data]
        return {r["DriveLetter"].rstrip(":") for r in rows
                if r.get("ProtectionStatus") == 1}, None
    except Exception as e:
        return set(), str(e)


def preflight_disk(disk):
    """Returns (blocking_problems, warnings) for a whole-disk capture."""
    problems, warnings = [], []
    if disk["size"] > VHD_MAX_BYTES:
        warnings.append(
            f"Disk {disk['number']} is {disk['size']/1024**3:.0f} GiB — beyond the "
            "fixed-VHD limit (~2040 GiB); the image will NOT be usable in full")
    if disk["style"] == "RAW":
        problems.append(f"Disk {disk['number']} has no recognized partition table (RAW)")
    if is_dynamic(disk):
        problems.append(
            f"Disk {disk['number']} is a DYNAMIC disk (LDM) — not supported; "
            "capture its volumes individually instead")
    bl, err = bitlocker_letters()
    if err:
        warnings.append(f"could not check BitLocker status ({err}) — assuming none")
    else:
        letters = {p["letter"] for p in disk["partitions"] if p.get("letter")}
        hit = letters & bl
        if hit:
            problems.append(
                f"BitLocker-protected volume(s) {sorted(hit)} on disk "
                f"{disk['number']} — raw capture would store ciphertext; "
                "decrypt first (same limitation as Disk2vhd)")
    return problems, warnings
