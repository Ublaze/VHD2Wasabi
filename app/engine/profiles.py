"""Named Wasabi profiles: local store + portable export/import.

profiles.json layout (v2):
  {"format": 2, "last_used": name, "profiles": {name: {fields...}}}
A legacy flat single-profile dict is migrated to profile "default".

Export format (.vhd2wprofile): JSON with a small header + the full profile.
With a password the same JSON is wrapped in an AES-256, header-encrypted 7z
(`7za a -p<pw> -mhe=on`), built in a temp dir (7-Zip cannot write archives to
stdout) and cleaned up afterwards.
"""
import json
import os
import subprocess
import tempfile
import time

from .compressor import find_sevenzip

FORMAT_VERSION = 1
FIELDS = ["endpoint", "bucket", "access_key", "secret_key", "prefix",
          "segment_mb", "level", "merge", "delete_segments"]
REQUIRED = ["endpoint", "bucket", "access_key", "secret_key"]
DEFAULTS = {"endpoint": "s3.eu-central-1.wasabisys.com", "prefix": "vhd2wasabi",
            "segment_mb": "10240", "level": "5", "merge": True,
            "delete_segments": False}
APP_DIR = os.path.join(os.environ.get("LOCALAPPDATA", os.path.expanduser("~")),
                       "VHD2Wasabi")
PROFILES_PATH = os.path.join(APP_DIR, "profiles.json")


class ProfileError(RuntimeError):
    pass


def normalize(profile):
    out = dict(DEFAULTS)
    for k in FIELDS:
        if k in profile and profile[k] is not None:
            out[k] = profile[k]
    out["merge"] = bool(out["merge"])
    out["delete_segments"] = bool(out["delete_segments"])
    out["segment_mb"] = str(out["segment_mb"])
    out["level"] = str(out["level"])
    return out


def validate(profile):
    p = normalize(profile)
    missing = [k for k in REQUIRED if not str(p.get(k, "")).strip()]
    if missing:
        raise ProfileError(f"missing required field(s): {', '.join(missing)}")
    return p


def load_all():
    """Returns (profiles_dict, last_used_name_or_None)."""
    if not os.path.isfile(PROFILES_PATH):
        return {}, None
    try:
        with open(PROFILES_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}, None
    if isinstance(data, dict) and "profiles" in data:
        return data.get("profiles", {}), data.get("last_used")
    # legacy flat single-profile file
    if isinstance(data, dict) and ("bucket" in data or "endpoint" in data):
        return {"default": normalize(data)}, "default"
    return {}, None


def save_all(profiles, last_used=None):
    os.makedirs(APP_DIR, exist_ok=True)
    tmp = PROFILES_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({"format": 2, "last_used": last_used, "profiles": profiles},
                  f, indent=2)
    os.replace(tmp, PROFILES_PATH)


def save_profile(name, profile):
    profiles, _ = load_all()
    profiles[name] = normalize(profile)
    save_all(profiles, name)


def export_profile(name, profile, path, password=None, sevenzip=None):
    """Write a portable profile file. password=None/'' -> plain JSON."""
    payload = {
        "tool": "VHD2Wasabi",
        "format_version": FORMAT_VERSION,
        "exported_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "name": name,
        "profile": validate(profile),
    }
    raw = json.dumps(payload, indent=2).encode("utf-8")
    if not password:
        with open(path, "wb") as f:
            f.write(raw)
        return
    sevenzip = sevenzip or find_sevenzip()
    if not sevenzip:
        raise ProfileError("7-Zip not found — required for encrypted export")
    tmp = tempfile.mkdtemp(prefix="vhd2w-prof-")
    try:
        inner = os.path.join(tmp, "profile.json")
        with open(inner, "wb") as f:
            f.write(raw)
        r = subprocess.run(
            [sevenzip, "a", path, "profile.json", f"-p{password}", "-mhe=on"],
            cwd=tmp, capture_output=True, timeout=120)
        if r.returncode != 0:
            raise ProfileError(
                "7-Zip export failed: "
                + (r.stdout + r.stderr).decode(errors="replace")[-300:])
    finally:
        import shutil
        shutil.rmtree(tmp, ignore_errors=True)


def is_7z(path):
    with open(path, "rb") as f:
        return f.read(6) == b"7z\xbc\xaf\x27\x1c"


def import_profile(path, password=None, sevenzip=None):
    """Returns (suggested_name, normalized_profile)."""
    if is_7z(path):
        sevenzip = sevenzip or find_sevenzip()
        if not sevenzip:
            raise ProfileError("7-Zip not found — required for encrypted import")
        if not password:
            raise ProfileError("this file is encrypted — password required")
        tmp = tempfile.mkdtemp(prefix="vhd2w-prof-")
        try:
            r = subprocess.run(
                [sevenzip, "x", path, "-y", f"-p{password}", f"-o{tmp}"],
                capture_output=True, timeout=120)
            if r.returncode != 0:
                raise ProfileError(
                    "decrypt/extract failed (wrong password?)")
            candidates = [f for f in os.listdir(tmp) if f.endswith(".json")]
            if not candidates:
                raise ProfileError("archive contains no profile JSON")
            with open(os.path.join(tmp, candidates[0]), "rb") as f:
                raw = f.read()
        finally:
            import shutil
            shutil.rmtree(tmp, ignore_errors=True)
    else:
        with open(path, "rb") as f:
            raw = f.read()
    try:
        data = json.loads(raw.decode("utf-8-sig"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        raise ProfileError("not a valid VHD2Wasabi profile file")
    if isinstance(data, dict) and "profile" in data:
        name = data.get("name") or "imported"
        profile = data["profile"]
    elif isinstance(data, dict):
        # bare profile dict also accepted
        name = os.path.splitext(os.path.basename(path))[0]
        profile = data
    else:
        raise ProfileError("not a valid VHD2Wasabi profile file")
    return name, validate(profile)
