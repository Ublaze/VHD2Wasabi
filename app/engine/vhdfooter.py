"""Pure-Python fixed-VHD footer builder (Microsoft VHD spec, 512-byte footer).

Fields are big-endian. Checksum is the one's complement of the sum of all
footer bytes computed with the checksum field zeroed.
"""
import struct
import time
import uuid

VHD_EPOCH_DELTA = 946684800  # seconds between 1970-01-01 and 2000-01-01
FOOTER_SIZE = 512


def compute_geometry(size_bytes):
    """VHD CHS geometry algorithm from the VHD spec."""
    total_sectors = size_bytes // 512
    max_sectors = 65535 * 16 * 255
    if total_sectors > max_sectors:
        total_sectors = max_sectors
    if total_sectors >= 65535 * 16 * 63:
        sectors_per_track = 255
        heads = 16
        cylinder_times_heads = total_sectors // sectors_per_track
    else:
        sectors_per_track = 17
        cylinder_times_heads = total_sectors // sectors_per_track
        heads = (cylinder_times_heads + 1023) // 1024
        if heads < 4:
            heads = 4
        if cylinder_times_heads >= heads * 1024 or heads > 16:
            sectors_per_track = 31
            heads = 16
            cylinder_times_heads = total_sectors // sectors_per_track
        if cylinder_times_heads >= heads * 1024 or heads > 16:
            sectors_per_track = 63
            heads = 16
            cylinder_times_heads = total_sectors // sectors_per_track
    cylinders = cylinder_times_heads // heads
    return cylinders, heads, sectors_per_track


def checksum(footer_bytes):
    """One's complement of the sum of all bytes (checksum field zeroed)."""
    if len(footer_bytes) != FOOTER_SIZE:
        raise ValueError("footer must be 512 bytes")
    total = sum(footer_bytes)
    return (~total) & 0xFFFFFFFF


def build_footer(size_bytes, timestamp=None, unique_id=None, creator_app=b"v2w "):
    if size_bytes % 512 != 0:
        raise ValueError("VHD size must be a multiple of 512")
    if timestamp is None:
        timestamp = int(time.time()) - VHD_EPOCH_DELTA
    if unique_id is None:
        unique_id = uuid.uuid4().bytes
    cylinders, heads, sectors = compute_geometry(size_bytes)

    f = bytearray(FOOTER_SIZE)
    f[0:8] = b"conectix"                       # cookie
    struct.pack_into(">I", f, 8, 0x00000002)    # features: no temp + no ...
    struct.pack_into(">I", f, 12, 0x00010000)   # file format version 1.0
    struct.pack_into(">Q", f, 16, 0xFFFFFFFFFFFFFFFF)  # data offset (fixed)
    struct.pack_into(">I", f, 24, timestamp & 0xFFFFFFFF)
    f[28:32] = creator_app[:4].ljust(4, b" ")   # creator application
    struct.pack_into(">I", f, 32, 0x00010000)   # creator version
    f[36:40] = b"Wi2k"                          # creator host OS: Windows
    struct.pack_into(">Q", f, 40, size_bytes)   # original size
    struct.pack_into(">Q", f, 48, size_bytes)   # current size
    struct.pack_into(">H", f, 56, cylinders)    # geometry: cylinders
    f[58] = heads
    f[59] = sectors
    struct.pack_into(">I", f, 60, 2)            # disk type: fixed
    f[68:84] = unique_id                        # unique id
    f[84] = 0                                   # saved state: not saved
    # checksum field (64:68) computed last, over all other bytes
    struct.pack_into(">I", f, 64, checksum(bytes(f)))
    return bytes(f)


def validate_footer(footer_bytes, expected_size=None):
    """Sanity check used by tests. Returns dict of parsed fields."""
    if len(footer_bytes) != FOOTER_SIZE:
        raise ValueError("footer must be 512 bytes")
    if footer_bytes[0:8] != b"conectix":
        raise ValueError("bad cookie")
    stored = struct.unpack_from(">I", footer_bytes, 64)[0]
    zeroed = bytearray(footer_bytes)
    zeroed[64:68] = b"\x00\x00\x00\x00"
    calc = checksum(bytes(zeroed))
    if stored != calc:
        raise ValueError(f"checksum mismatch: stored {stored:#x} calc {calc:#x}")
    current = struct.unpack_from(">Q", footer_bytes, 48)[0]
    if expected_size is not None and current != expected_size:
        raise ValueError("size mismatch")
    return {
        "cylinders": struct.unpack_from(">H", footer_bytes, 56)[0],
        "heads": footer_bytes[58],
        "sectors": footer_bytes[59],
        "disk_type": struct.unpack_from(">I", footer_bytes, 60)[0],
        "current_size": current,
        "checksum": stored,
    }


def parse_footer(footer_bytes):
    """Parse + validate a 512-byte VHD footer (checksum included).
    Returns dict of fields; raises ValueError on any inconsistency."""
    return validate_footer(footer_bytes)
