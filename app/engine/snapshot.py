"""VSS snapshot create/resolve/delete via WMI (PowerShell) + vssadmin."""
import re
import subprocess

CREATE_PS = (
    "$r = (Get-WmiObject -List Win32_ShadowCopy).Create('{drive}', 'ClientAccessible');"
    " Write-Output (\"RET=\" + $r.ReturnValue); Write-Output (\"ID=\" + $r.ShadowID)"
)
DEVICE_PS = (
    "$s = Get-WmiObject Win32_ShadowCopy | Where-Object {{$_.ID -eq '{sid}'}};"
    " Write-Output (\"DEV=\" + $s.DeviceObject)"
)


class SnapshotError(RuntimeError):
    pass


def _run(cmd, timeout=300):
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)


def create_snapshot(drive_root):
    """drive_root like 'C:\\' -> (shadow_id, device_path like \\\\?\\GLOBALROOT\\Device\\HarddiskVolumeShadowCopyN)"""
    if not drive_root.endswith("\\"):
        drive_root += "\\"
    p = _run(["powershell", "-NoProfile", "-Command", CREATE_PS.format(drive=drive_root)])
    out = p.stdout + p.stderr
    m_ret = re.search(r"RET=(\d+)", out)
    m_id = re.search(r"ID=(\{[0-9A-Fa-f-]+\})", out)
    if not m_ret or not m_id:
        raise SnapshotError(f"VSS create failed to parse: {out.strip()}")
    if m_ret.group(1) != "0":
        raise SnapshotError(f"VSS create returned {m_ret.group(1)}: {out.strip()}")
    sid = m_id.group(1)

    p = _run(["powershell", "-NoProfile", "-Command", DEVICE_PS.format(sid=sid)])
    m_dev = re.search(r"DEV=(\\\\[^\s]+)", p.stdout)
    if not m_dev:
        delete_snapshot(sid)
        raise SnapshotError(f"could not resolve shadow device: {p.stdout} {p.stderr}")
    return sid, m_dev.group(1)


def delete_snapshot(shadow_id):
    _run(["vssadmin", "delete", "shadows", f"/shadow={shadow_id}", "/quiet"])


def list_shadow_storage():
    """Parse `vssadmin list shadowstorage` -> list of dicts."""
    p = _run(["vssadmin", "list", "shadowstorage"])
    entries = []
    cur = None

    def num(s):
        m = re.search(r"([\d.,]+)\s*([KMGT]?B)", s, re.I)
        if not m:
            return None
        v = float(m.group(1).replace(",", ""))
        mult = {"B": 1, "KB": 1e3, "MB": 1e6, "GB": 1e9, "TB": 1e12}
        return int(v * mult.get(m.group(2).upper(), 1))

    for line in p.stdout.splitlines():
        line = line.strip()
        if line.startswith("For volume:"):
            if cur:
                entries.append(cur)
            cur = {"volume": line.split(":", 1)[1].strip()}
        elif cur is not None:
            if line.startswith("Shadow Copy Storage volume:"):
                cur["storage_volume"] = line.split(":", 1)[1].strip()
            elif line.startswith("Used Shadow Copy Storage space:"):
                cur["used"] = num(line)
            elif line.startswith("Allocated Shadow Copy Storage space:"):
                cur["allocated"] = num(line)
            elif line.startswith("Maximum Shadow Copy Storage space:"):
                cur["maximum"] = num(line)
    if cur:
        entries.append(cur)
    return entries
