"""VHD2Wasabi — Disk2vhd-style live capture, streamed through 7-Zip into
a Wasabi (S3-compatible) bucket. Tkinter UI, dark theme."""
import json
import os
import queue
import socket
import sys
import threading
import tkinter as tk
from tkinter import messagebox, ttk

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from engine import manifest as manifest_mod
from engine import pipeline as pipeline_mod
from engine import preflight
from engine.uploader import S3Store
from engine.volumereader import list_fixed_volumes

APP_DIR = os.path.join(os.environ.get("LOCALAPPDATA", os.path.expanduser("~")),
                       "VHD2Wasabi")
PROFILES_PATH = os.path.join(APP_DIR, "profiles.json")
BG = "#1e1e1e"
BG2 = "#252526"
FG = "#d4d4d4"
ACCENT = "#0e7490"
TOOL_VERSION = manifest_mod.TOOL_VERSION


def human(n):
    for u in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or u == "TB":
            return f"{n:.1f} {u}" if u != "B" else f"{int(n)} B"
        n /= 1024


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title(f"VHD2Wasabi v{TOOL_VERSION}")
        self.configure(bg=BG)
        self.geometry("860x760")
        self._style()
        self.volumes = []
        self.vol_vars = {}
        self.worker = None
        self.pipeline = None
        self.q = queue.Queue()
        self._build()
        self._refresh_volumes()
        self.after(100, self._drain)

    # ---- UI construction ---------------------------------------------------
    def _style(self):
        s = ttk.Style(self)
        s.theme_use("clam")
        s.configure(".", background=BG, foreground=FG, fieldbackground=BG2,
                    bordercolor="#3c3c3c")
        s.configure("TFrame", background=BG)
        s.configure("TLabel", background=BG, foreground=FG)
        s.configure("TCheckbutton", background=BG, foreground=FG)
        s.configure("TButton", background="#2d2d30", foreground=FG)
        s.map("TButton", background=[("active", "#3e3e42")])
        s.configure("TEntry", fieldbackground=BG2, foreground=FG,
                    insertcolor=FG)
        s.configure("TLabelframe", background=BG, foreground=FG)
        s.configure("TLabelframe.Label", background=BG, foreground=FG)
        s.configure("TProgressbar", background=ACCENT, troughcolor=BG2)

    def _build(self):
        top = ttk.Frame(self, padding=8)
        top.pack(fill="x")

        vf = ttk.LabelFrame(top, text="Volumes (fixed disks) — captured sequentially, one object set per volume", padding=6)
        vf.pack(fill="x")
        self.vol_frame = ttk.Frame(vf)
        self.vol_frame.pack(fill="x")
        ttk.Button(vf, text="Refresh", command=self._refresh_volumes).pack(anchor="e")

        wf = ttk.LabelFrame(top, text="Wasabi destination", padding=6)
        wf.pack(fill="x", pady=6)
        self.entries = {}
        fields = [
            ("endpoint", "Endpoint", "s3.eu-central-1.wasabisys.com"),
            ("bucket", "Bucket", ""),
            ("access_key", "Access key", ""),
            ("secret_key", "Secret key", ""),
            ("prefix", "Prefix", "vhd2wasabi"),
        ]
        for i, (k, label, default) in enumerate(fields):
            ttk.Label(wf, text=label).grid(row=i // 3, column=(i % 3) * 2,
                                           sticky="w", padx=4, pady=2)
            e = ttk.Entry(wf, width=28, show="*" if "key" in k and k != "access_key" else "")
            if k == "secret_key":
                e.config(show="*")
            if k == "access_key":
                e.config(show="")
            e.insert(0, default)
            e.grid(row=i // 3, column=(i % 3) * 2 + 1, sticky="w", padx=4, pady=2)
            self.entries[k] = e
        ttk.Label(wf, text="Segment MB").grid(row=1, column=4, sticky="w", padx=4)
        self.seg_mb = ttk.Entry(wf, width=10)
        self.seg_mb.insert(0, "10240")
        self.seg_mb.grid(row=1, column=5, sticky="w")
        ttk.Label(wf, text="Compression (0-9)").grid(row=2, column=0, sticky="w", padx=4)
        self.level = ttk.Entry(wf, width=10)
        self.level.insert(0, "5")
        self.level.grid(row=2, column=1, sticky="w")
        self.fresh_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(wf, text="Start fresh (ignore saved state)",
                        variable=self.fresh_var).grid(row=2, column=2, columnspan=2,
                                                      sticky="w", padx=4)

        btns = ttk.Frame(top)
        btns.pack(fill="x", pady=4)
        for text, cmd in [("Save profile", self._save_profile),
                          ("Load profile", self._load_profile),
                          ("Test connection", self._test_connection),
                          ("Start", self._start),
                          ("Verify…", self._verify_dialog),
                          ("Cancel", self._cancel)]:
            ttk.Button(btns, text=text, command=cmd).pack(side="left", padx=4)

        sf = ttk.Frame(top)
        sf.pack(fill="x", pady=2)
        self.status_var = tk.StringVar(value="Idle")
        ttk.Label(sf, textvariable=self.status_var).pack(anchor="w")
        self.speed_var = tk.StringVar(value="")
        ttk.Label(sf, textvariable=self.speed_var).pack(anchor="w")
        self.prog = ttk.Progressbar(top, mode="determinate")
        self.prog.pack(fill="x", pady=2)
        self.eta_var = tk.StringVar(value="")
        ttk.Label(top, textvariable=self.eta_var).pack(anchor="w")

        lf = ttk.LabelFrame(top, text="Log", padding=4)
        lf.pack(fill="both", expand=True, pady=4)
        self.logw = tk.Text(lf, bg="#121212", fg="#c8c8c8", insertbackground=FG,
                            height=14, state="disabled", wrap="word")
        sb = ttk.Scrollbar(lf, command=self.logw.yview)
        self.logw.configure(yscrollcommand=sb.set)
        sb.pack(side="right", fill="y")
        self.logw.pack(fill="both", expand=True)

    # ---- helpers ------------------------------------------------------------
    def _refresh_volumes(self):
        for w in self.vol_frame.winfo_children():
            w.destroy()
        self.volumes = list_fixed_volumes()
        self.vol_vars = {}
        for v in self.volumes:
            var = tk.BooleanVar(value=False)
            self.vol_vars[v.letter] = var
            ttk.Checkbutton(
                self.vol_frame,
                text=f"{v.letter}:  {v.label or '(no label)'}   "
                     f"total {v.total/1e9:.1f} GB, used {v.used/1e9:.1f} GB, "
                     f"free {v.free/1e9:.1f} GB",
                variable=var).pack(anchor="w")
        if not self.volumes:
            ttk.Label(self.vol_frame, text="No fixed volumes found").pack(anchor="w")

    def _selected(self):
        return [v for v in self.volumes if self.vol_vars[v.letter].get()]

    def log(self, msg):
        self.q.put(("log", msg))

    def _drain(self):
        try:
            while True:
                kind, *args = self.q.get_nowait()
                if kind == "log":
                    self.logw.configure(state="normal")
                    self.logw.insert("end", args[0] + "\n")
                    self.logw.see("end")
                    self.logw.configure(state="disabled")
                elif kind == "status":
                    self.status_var.set(args[0])
                elif kind == "speed":
                    self.speed_var.set(args[0])
                elif kind == "progress":
                    frac, eta = args
                    self.prog["value"] = frac * 100
                    self.eta_var.set(eta)
        except queue.Empty:
            pass
        self.after(100, self._drain)

    # ---- profiles ------------------------------------------------------------
    def _form(self):
        d = {k: e.get().strip() for k, e in self.entries.items()}
        d["segment_mb"] = self.seg_mb.get().strip() or "10240"
        d["level"] = self.level.get().strip() or "5"
        return d

    def _save_profile(self):
        os.makedirs(APP_DIR, exist_ok=True)
        with open(PROFILES_PATH, "w", encoding="utf-8") as f:
            json.dump(self._form(), f, indent=2)
        self.log(f"Profile saved to {PROFILES_PATH} (contains secrets — protect it)")

    def _load_profile(self):
        if not os.path.isfile(PROFILES_PATH):
            messagebox.showinfo("VHD2Wasabi", "No saved profile found.")
            return
        with open(PROFILES_PATH, "r", encoding="utf-8") as f:
            d = json.load(f)
        for k, e in self.entries.items():
            e.delete(0, "end")
            e.insert(0, d.get(k, ""))
        self.seg_mb.delete(0, "end")
        self.seg_mb.insert(0, d.get("segment_mb", "10240"))
        self.level.delete(0, "end")
        self.level.insert(0, d.get("level", "5"))
        self.log("Profile loaded")

    # ---- actions --------------------------------------------------------------
    def _store(self, form):
        return S3Store(form["endpoint"], form["bucket"],
                       form["access_key"], form["secret_key"])

    def _test_connection(self):
        form = self._form()

        def go():
            try:
                self._store(form).head_bucket()
                self.log("Connection OK — bucket reachable")
            except Exception as e:
                self.log(f"Connection FAILED: {e}")
        threading.Thread(target=go, daemon=True).start()

    def _start(self):
        if self.worker and self.worker.is_alive():
            messagebox.showinfo("VHD2Wasabi", "A job is already running.")
            return
        vols = self._selected()
        if not vols:
            messagebox.showwarning("VHD2Wasabi", "Select at least one volume.")
            return
        form = self._form()
        if not form["bucket"] or not form["access_key"]:
            messagebox.showwarning("VHD2Wasabi", "Fill in bucket and credentials.")
            return
        if not preflight.is_admin():
            messagebox.showerror("VHD2Wasabi",
                                 "Must run as Administrator (VSS + raw disk access).")
            return
        try:
            seg_mb = int(form["segment_mb"])
            level = int(form["level"])
        except ValueError:
            messagebox.showwarning("VHD2Wasabi", "Segment size / compression must be numbers.")
            return

        store = self._store(form)
        callbacks = self._callbacks()
        self.pipeline = pipeline_mod.Pipeline(
            store, form["prefix"], socket.gethostname(),
            segment_size_mb=seg_mb, level=level,
            start_fresh=self.fresh_var.get(), callbacks=callbacks)

        def go():
            try:
                problems, warnings, _e = preflight.run_all(store, vols, log=self.log)
                for w in warnings:
                    self.log(f"WARNING: {w}")
                if problems:
                    self.log("PRE-FLIGHT FAILED:")
                    for p in problems:
                        self.log(f"  - {p}")
                    return
                self.log("Pre-flight OK — starting capture")
                for v in vols:
                    st = manifest_mod.load_state(socket.gethostname(), v.safe_name())
                    if st and st.get("status") != "done" and not self.fresh_var.get():
                        self.log(f"{v.letter}: saved state found — resuming automatically")
                self.pipeline.run_volumes(vols)
                self.log("All done.")
                self.q.put(("status", "Idle"))
            except pipeline_mod.Cancelled:
                self.log("Cancelled. State preserved — run again to resume.")
                self.q.put(("status", "Cancelled"))
            except Exception as e:
                self.log(f"ERROR: {e}")
                self.q.put(("status", "Error"))
        self.worker = threading.Thread(target=go, daemon=True)
        self.worker.start()

    def _cancel(self):
        if self.pipeline:
            self.pipeline.cancel()
            self.log("Cancel requested — aborting multipart, deleting snapshot…")

    def _callbacks(self):
        def status(vol, text):
            self.q.put(("status", f"{vol} {text}"))

        def progress(vol, done, total, speeds):
            frac = done / total if total else 0
            s = speeds or {}
            spd = " | ".join(
                f"{k} {human(v)}/s" for k, v in s.items() if isinstance(v, (int, float)))
            self.q.put(("speed", f"{vol} {spd}"))
            rate = s.get("read") or s.get("overall") or 0
            eta = ""
            if rate > 0 and total > done:
                rem = (total - done) / rate
                eta = f"ETA {int(rem // 60)}m{int(rem % 60):02d}s  ({human(done)} / {human(total)})"
            self.q.put(("progress", frac, eta))

        def diff(vol, pct):
            self.log(f"Diff area {vol}: {pct:.0%} used")

        def seg_done(vol, seg):
            self.log(f"{vol} segment {seg['index'] + 1} done: "
                     f"{human(seg['raw_size'])} -> {human(seg['compressed_size'])} "
                     f"sha256 {seg['raw_sha256'][:16]}…")

        def vol_done(vol, state):
            self.log(f"{vol} COMPLETE — {len(state['segments'])} segment(s)")

        return {"log": self.log, "status": status, "progress": progress,
                "diff": diff, "segment_done": seg_done, "volume_done": vol_done}

    # ---- verify ---------------------------------------------------------------
    def _verify_dialog(self):
        form = self._form()
        if not form["bucket"]:
            messagebox.showwarning("VHD2Wasabi", "Fill in bucket and credentials.")
            return

        def fetch():
            try:
                store = self._store(form)
                keys = manifest_mod.list_manifests(store, form["prefix"])
                self.after(0, lambda: self._show_manifests(form, keys))
            except Exception as e:
                self.log(f"manifest list failed: {e}")

        threading.Thread(target=fetch, daemon=True).start()

    def _show_manifests(self, form, keys):
        win = tk.Toplevel(self)
        win.title("Verify — select manifest")
        win.configure(bg=BG)
        lb = tk.Listbox(win, bg=BG2, fg=FG, width=90, height=12)
        lb.pack(fill="both", expand=True, padx=8, pady=8)
        for k in keys:
            lb.insert("end", k)
        if not keys:
            lb.insert("end", "(no manifests found)")

        def run_verify():
            sel = lb.curselection()
            if not sel or not keys:
                return
            key = keys[sel[0]]
            win.destroy()
            self._run_verify(form, key)
        ttk.Button(win, text="Verify selected", command=run_verify).pack(pady=6)

    def _run_verify(self, form, key):
        store = self._store(form)
        cancel = threading.Event()

        def go():
            try:
                ok = pipeline_mod.verify_manifest(store, key, log=self.log,
                                                  cancel_event=cancel)
                self.q.put(("status", "Verify PASSED" if ok else "Verify FAILED"))
            except Exception as e:
                self.log(f"verify error: {e}")
        self.worker = threading.Thread(target=go, daemon=True)
        self.worker.start()


def main():
    app = App()
    app.mainloop()


if __name__ == "__main__":
    main()
