"""PyInstaller wrapper: builds dist\\VHD2Wasabi.exe (onefile, UAC admin, bundled 7za)."""
import os
import subprocess
import sys

ROOT = os.path.dirname(os.path.abspath(__file__))
SEVENZIP = os.path.join(ROOT, "app", "resources", "7za.exe")


def main():
    if not os.path.isfile(SEVENZIP):
        print(f"WARNING: {SEVENZIP} missing — the EXE will fail pre-flight. See README.")
        add_binary = []
    else:
        add_binary = ["--add-binary", f"{SEVENZIP};resources"]
    cmd = [
        sys.executable, "-m", "PyInstaller",
        "--onefile",
        "--noconsole",
        "--uac-admin",
        "--name", "VHD2Wasabi",
        "--paths", os.path.join(ROOT, "app"),
        "--hidden-import", "win32timezone",
        "--collect-submodules", "boto3",
        *add_binary,
        os.path.join(ROOT, "app", "main.py"),
    ]
    print(" ".join(cmd))
    r = subprocess.run(cmd, cwd=ROOT)
    if r.returncode != 0:
        sys.exit(r.returncode)
    exe = os.path.join(ROOT, "dist", "VHD2Wasabi.exe")
    print("built:", exe, os.path.getsize(exe), "bytes")


if __name__ == "__main__":
    main()
