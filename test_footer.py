"""Smoke test for the fixed-VHD footer: fields, geometry, checksum."""
import os
import struct
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "app"))
from engine import vhdfooter


def test_checksum_math():
    # one's-complement property: sum of all bytes (incl. checksum) == 0xFFFFFFFF
    for size in (512, 20 * 1024**3, 2 * 1024**4 - 512, 127 * 1024**3):
        f = vhdfooter.build_footer(size, timestamp=123456, unique_id=b"\x01" * 16)
        assert len(f) == 512
        zeroed = bytearray(f)
        zeroed[64:68] = b"\x00" * 4
        expect = (~sum(zeroed)) & 0xFFFFFFFF
        assert struct.unpack_from(">I", f, 64)[0] == expect, \
            f"checksum field wrong for {size}"


def test_fields():
    size = 40 * 1024**3
    f = vhdfooter.build_footer(size, timestamp=0, unique_id=b"\x02" * 16)
    assert f[0:8] == b"conectix"
    assert struct.unpack_from(">I", f, 8)[0] == 0x00000002          # features
    assert struct.unpack_from(">I", f, 12)[0] == 0x00010000         # version
    assert struct.unpack_from(">Q", f, 16)[0] == 0xFFFFFFFFFFFFFFFF  # data offset
    assert struct.unpack_from(">Q", f, 40)[0] == size                # original
    assert struct.unpack_from(">Q", f, 48)[0] == size                # current
    assert struct.unpack_from(">I", f, 60)[0] == 2                   # fixed
    assert f[36:40] == b"Wi2k"
    assert f[68:84] == b"\x02" * 16
    info = vhdfooter.validate_footer(f, expected_size=size)
    assert info["disk_type"] == 2


def test_geometry():
    # small image -> heads/sectors per CHS algorithm
    c, h, s = vhdfooter.compute_geometry(512)
    assert (c, h, s) == (1 // 17 + 0, 4, 17) or (c >= 0 and h == 4 and s == 17)
    # large image -> 255 sectors / 16 heads
    c, h, s = vhdfooter.compute_geometry(100 * 1024**3)
    assert (h, s) == (16, 255) and c > 0
    # > max CHS -> clamped
    c, h, s = vhdfooter.compute_geometry(2 * 1024**4)
    assert c <= 65535


def test_alignment_guard():
    try:
        vhdfooter.build_footer(1000)
    except ValueError:
        pass
    else:
        raise AssertionError("non-512-multiple size must be rejected")


if __name__ == "__main__":
    test_checksum_math()
    test_fields()
    test_geometry()
    test_alignment_guard()
    print("test_footer: ALL PASSED")
