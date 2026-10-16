"""Pre-flight checks: elevation, VSS service, 7-Zip presence, bucket access."""
import ctypes
import os

import win32service
import win32serviceutil

from . import compressor, snapshot


class PreflightError(RuntimeError):
    def __init__(self, problems):
        self.problems = problems
        super().__init__("; ".join(problems))


def is_admin():
    try:
        return bool(ctypes.windll.shell32.IsUserAnAdmin())
    except Exception:
        return False


def check_vss_service():
    """Returns (ok, detail). VSS is demand-start; only 'disabled' is fatal."""
    try:
        status = win32serviceutil.QueryServiceStatus("VSS")
        scm = win32service.OpenSCManager(None, None, win32service.SC_MANAGER_CONNECT)
        svc = win32service.OpenService(scm, "VSS", win32service.SERVICE_QUERY_CONFIG)
        cfg = win32service.QueryServiceConfig(svc)
        start_type = cfg[1]
        win32service.CloseServiceHandle(svc)
        win32service.CloseServiceHandle(scm)
        if start_type == win32service.SERVICE_DISABLED:
            return False, "VSS service is DISABLED"
        return True, f"VSS service start-type ok (status {status[1]})"
    except Exception as e:
        return False, f"cannot query VSS service: {e}"


def check_sevenzip():
    p = compressor.find_sevenzip()
    if p:
        return True, p
    return False, (f"{compressor.SEVENZIP} not found. Place it in app\\resources "
                   "(dev) or bundle it (PyInstaller). See README.")


def check_bucket(store):
    try:
        store.head_bucket()
        return True, "bucket reachable"
    except Exception as e:
        return False, f"bucket check failed: {e}"


def check_shadow_storage(volumes):
    """Returns (warnings, entries). Warn if free space < 5% of volume size."""
    warnings = []
    try:
        entries = snapshot.list_shadow_storage()
    except Exception as e:
        return [f"could not query shadow storage: {e}"], []
    for v in volumes:
        letter = f"{v.letter}:"
        if v.free < 0.05 * v.total:
            warnings.append(
                f"{letter} free space {v.free/1e9:.1f} GB is < 5% of volume size "
                f"({v.total/1e9:.1f} GB) — VSS snapshot may fail")
        match = [e for e in entries if e.get("volume", "").startswith(f"({letter})")
                 or e.get("volume", "").startswith(letter)]
        if not match:
            warnings.append(
                f"{letter} has no shadow storage configured — diff area will default "
                "to the same volume")
    return warnings, entries


def run_all(store, volumes, log=None):
    problems = []
    warnings = []
    if not is_admin():
        problems.append("not running as Administrator (required for VSS/raw disk access)")
    ok, detail = check_vss_service()
    (log or (lambda m: None))(f"preflight: {detail}")
    if not ok:
        problems.append(detail)
    ok, detail = check_sevenzip()
    (log or (lambda m: None))(f"preflight: 7-Zip: {detail}")
    if not ok:
        problems.append(detail)
    ok, detail = check_bucket(store)
    if not ok:
        problems.append(detail)
    w, entries = check_shadow_storage(volumes)
    warnings.extend(w)
    return problems, warnings, entries
