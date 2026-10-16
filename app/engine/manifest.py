"""Per-volume state + manifest JSON, local and in the bucket."""
import json
import os
import time

TOOL_VERSION = "1.0.0"

STATE_ROOT = os.path.join(os.environ.get("LOCALAPPDATA", os.path.expanduser("~")),
                          "VHD2Wasabi", "state")


def state_path(server, volume_name):
    safe = "".join(c if c.isalnum() or c in "-_." else "_" for c in volume_name)
    return os.path.join(STATE_ROOT, server, f"{safe}.json")


def load_state(server, volume_name):
    p = state_path(server, volume_name)
    if os.path.isfile(p):
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    return None


def save_state(server, volume_name, state):
    p = state_path(server, volume_name)
    os.makedirs(os.path.dirname(p), exist_ok=True)
    tmp = p + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)
    os.replace(tmp, p)


def delete_state(server, volume_name):
    p = state_path(server, volume_name)
    if os.path.isfile(p):
        os.remove(p)


def new_state(server, volume, total_size, segment_size, key_prefix):
    return {
        "tool": "VHD2Wasabi",
        "tool_version": TOOL_VERSION,
        "manifest_version": 1,
        "server": server,
        "volume": volume,
        "total_size": total_size,      # uncompressed incl. 512-byte VHD footer
        "segment_size": segment_size,  # uncompressed bytes per segment
        "key_prefix": key_prefix,
        "started": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "segments": [],                # {index,key,offset,raw_size,raw_sha256,compressed_size,etag,status}
        "image_sha256": None,
        "status": "in_progress",
    }


def upload_manifest(store, bucket_key, state):
    store.put_bytes(bucket_key, json.dumps(state, indent=2).encode("utf-8"))


def list_manifests(store, prefix):
    return sorted(store.list_keys(prefix, suffix=".manifest.json"))


def download_manifest(store, key):
    return json.loads(store.get_bytes(key).decode("utf-8"))
