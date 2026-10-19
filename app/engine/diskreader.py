r"""Whole-disk sequential reader: assembles one byte stream of exactly
disk-length bytes by walking the partition layout. Snapshot-capable
NTFS/ReFS volumes are read from their VSS shadow device; the partition table,
gaps, ESP/system partitions and raw regions are read live from
\\\.\\PhysicalDriveN (tiny, static — a consistency note is logged).

Layout is derived ONCE at capture start and frozen into state; a resumed run
re-derives it and aborts if it changed (never silently mixes layouts).
"""
import win32con
import win32file

from . import snapshot as snapshot_mod
from .diskinfo import list_disks
from .volumereader import tolerant_chunks

SNAPSHOT_FS = {"NTFS", "REFS"}
CHUNK = 4 * 1024 * 1024


class LayoutChangedError(RuntimeError):
    pass


def compute_regions(partitions, disk_length):
    """Ordered regions covering [0, disk_length): partition / gap / tail."""
    regions = []
    pos = 0
    for p in sorted(partitions, key=lambda x: x["offset"]):
        if p["offset"] > pos:
            regions.append({"offset": pos, "length": p["offset"] - pos,
                            "kind": "gap" if pos else "preamble"})
        regions.append({"offset": p["offset"], "length": p["size"],
                        "kind": "partition", "partition": p["number"],
                        "letter": p.get("letter"), "fs": p.get("fs"),
                        "gpt_type": p.get("gpt_type"), "type": p.get("type")})
        pos = max(pos, p["offset"] + p["size"])
    if pos < disk_length:
        regions.append({"offset": pos, "length": disk_length - pos,
                        "kind": "tail"})
    return regions


def frozen_layout(disk):
    """The layout record stored in state: everything needed to detect a
    layout change on resume."""
    return {
        "number": disk["number"], "model": disk["model"],
        "size": disk["size"], "style": disk["style"],
        "partitions": [{"number": p["number"], "offset": p["offset"],
                        "size": p["size"], "letter": p.get("letter"),
                        "gpt_type": p.get("gpt_type"), "type": p.get("type")}
                       for p in disk["partitions"]],
    }


def layouts_equal(a, b):
    return a == b


class _DeviceHandle:
    def __init__(self, path):
        self.path = path
        self.handle = win32file.CreateFile(
            path, win32con.GENERIC_READ,
            win32con.FILE_SHARE_READ | win32con.FILE_SHARE_WRITE,
            None, win32con.OPEN_EXISTING, 0, None)

    def read_at(self, pos, n):
        win32file.SetFilePointer(self.handle, pos, win32con.FILE_BEGIN)
        _rc, data = win32file.ReadFile(self.handle, n)
        return data

    def close(self):
        try:
            win32file.CloseHandle(self.handle)
        except Exception:
            pass


class DiskReader:
    """Sequential reader over a whole disk, mixing shadow-volume and
    live-physical regions. `opener(device_path)` must return an object with
    read_at(pos, n) and close() — injectable for tests."""

    def __init__(self, length, regions, opener=None, chunk=CHUNK, log=None):
        self.length = length
        self.regions = regions          # each has offset/length + "device"
        self.chunk = chunk
        self.log = log
        self._open = opener or (lambda dev: _DeviceHandle(dev))
        self._devices = {}
        self.pos = 0

    def _dev(self, path):
        if path not in self._devices:
            self._devices[path] = self._open(path)
        return self._devices[path]

    def read_at(self, pos, n):
        out = bytearray()
        end = pos + n
        for r in self.regions:
            r_end = r["offset"] + r["length"]
            if r_end <= pos or r["offset"] >= end:
                continue
            s = max(pos, r["offset"])
            e = min(end, r_end)
            # shadow volume device offset 0 == partition/volume offset on
            # basic disks; live regions are absolute on the physical drive
            dev_off = 0 if r.get("source") == "shadow" else r["offset"]
            data = self._dev(r["device"]).read_at(dev_off + (s - r["offset"]),
                                                  e - s)
            if len(data) != e - s:
                # let tolerant_chunks handle short reads via retry/pad logic:
                # surface the shortfall by returning what we have
                out.extend(data)
                if len(out) < n:
                    return bytes(out)
            else:
                out.extend(data)
        return bytes(out)

    def seek(self, pos):
        if pos % 512:
            raise ValueError("seek offset must be sector aligned")
        self.pos = pos

    def read_chunks(self):
        for c in tolerant_chunks(self.read_at, self.length, self.chunk,
                                 self.log, start=self.pos):
            self.pos += len(c)
            yield c

    def close(self):
        for d in self._devices.values():
            try:
                d.close()
            except Exception:
                pass
        self._devices.clear()


def build_disk_reader(disk_number, log=lambda m: None, snapshot=True,
                      opener=None):
    """Derive layout, snapshot NTFS/ReFS volumes, and build a DiskReader.
    Returns (reader, meta, cleanup). meta carries the frozen layout record,
    per-region sources, snapshot IDs, model, style, and shadowed letters."""
    disks = [d for d in list_disks(log=log) if d["number"] == disk_number]
    if not disks:
        raise LayoutChangedError(f"disk {disk_number} not found")
    disk = disks[0]
    regions = compute_regions(disk["partitions"], disk["size"])
    physical = f"\\\\.\\PhysicalDrive{disk_number}"

    snapshot_ids = []
    letters = []
    live_notes = []
    for r in regions:
        if r["kind"] != "partition":
            r["source"] = "live"
            r["device"] = physical
            continue
        letter, fs = r.get("letter"), (r.get("fs") or "").upper()
        if snapshot and letter and fs in SNAPSHOT_FS:
            sid, device = snapshot_mod.create_snapshot(f"{letter}:\\")
            snapshot_ids.append(sid)
            letters.append(letter)
            r["source"] = "shadow"
            r["device"] = device
            r["snapshot_id"] = sid
            log(f"  disk {disk_number}: {letter}: snapshot {sid}")
        else:
            r["source"] = "live"
            r["device"] = physical
            live_notes.append(
                f"partition {r['partition']} ({letter or 'no letter'}, "
                f"{fs or 'no fs'}, {r['length'] / 1024**2:.0f} MiB)")
    if live_notes:
        log(f"  disk {disk_number}: read LIVE (not snapshot-consistent): "
            + "; ".join(live_notes)
            + " — these regions are tiny/static; also the partition table "
              "and gaps are read live")

    # region records for the manifest (strip callables/devices internals ok)
    region_rec = [{"offset": r["offset"], "length": r["length"],
                   "kind": r["kind"], "source": r["source"],
                   "letter": r.get("letter"),
                   "partition": r.get("partition"),
                   "snapshot_id": r.get("snapshot_id")} for r in regions]

    def cleanup():
        for sid in snapshot_ids:
            try:
                snapshot_mod.delete_snapshot(sid)
                log(f"  disk {disk_number}: snapshot {sid} deleted")
            except Exception as e:
                log(f"  disk {disk_number}: WARNING snapshot delete failed: {e}")

    reader = DiskReader(disk["size"], regions, opener=opener, log=log)
    meta = {
        "layout": frozen_layout(disk),
        "model": disk["model"],
        "style": disk["style"],
        "regions": region_rec,
        "snapshot_ids": snapshot_ids,
        "letters": letters,
    }
    return reader, meta, cleanup
