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
        nb = ttk.Notebook(self)
        nb.pack(fill="both", expand=True)
        capture = ttk.Frame(nb)
        restore = ttk.Frame(nb)
        nb.add(capture, text="Capture")
        nb.add(restore, text="Restore")

        top = ttk.Frame(capture, padding=8)
        top.pack(fill="both", expand=True)

        vf = ttk.LabelFrame(top, text="Capture source", padding=6)
        vf.pack(fill="x")
        self.mode_var = tk.StringVar(value="volumes")
        mrow = ttk.Frame(vf)
        mrow.pack(fill="x")
        ttk.Radiobutton(mrow, text="Volumes (data)", variable=self.mode_var,
                        value="volumes", command=self._mode_changed).pack(side="left", padx=4)
        ttk.Radiobutton(mrow, text="Whole disk (bootable)", variable=self.mode_var,
                        value="disk", command=self._mode_changed).pack(side="left", padx=4)
        ttk.Button(mrow, text="Refresh", command=self._refresh_sources).pack(side="right")
        self.vol_frame = ttk.Frame(vf)
        self.vol_frame.pack(fill="x")
        self.disk_frame = ttk.Frame(vf)

        wf = ttk.LabelFrame(top, text="Wasabi destination", padding=6)
        wf.pack(fill="x", pady=6)
        self.vars = {}
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
            var = tk.StringVar(value=default)
            self.vars[k] = var
            e = ttk.Entry(wf, width=28, textvariable=var,
                          show="*" if k == "secret_key" else "")
            e.grid(row=i // 3, column=(i % 3) * 2 + 1, sticky="w", padx=4, pady=2)
            self.entries[k] = e
        ttk.Label(wf, text="Segment MB").grid(row=1, column=4, sticky="w", padx=4)
        self.vars["segment_mb"] = tk.StringVar(value="10240")
        self.seg_mb = ttk.Entry(wf, width=10, textvariable=self.vars["segment_mb"])
        self.seg_mb.grid(row=1, column=5, sticky="w")
        ttk.Label(wf, text="Compression (0-9)").grid(row=2, column=0, sticky="w", padx=4)
        self.vars["level"] = tk.StringVar(value="5")
        self.level = ttk.Entry(wf, width=10, textvariable=self.vars["level"])
        self.level.grid(row=2, column=1, sticky="w")
        self.fresh_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(wf, text="Start fresh (ignore saved state)",
                        variable=self.fresh_var).grid(row=2, column=2, columnspan=2,
                                                      sticky="w", padx=4)
        self.merge_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(wf, text="Merge into single object after verify",
                        variable=self.merge_var).grid(row=3, column=0, columnspan=3,
                                                      sticky="w", padx=4)
        self.del_segs_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(wf, text="Delete segment objects after merge",
                        variable=self.del_segs_var).grid(row=3, column=3, columnspan=3,
                                                         sticky="w", padx=4)

        pf = ttk.Frame(top)
        pf.pack(fill="x", pady=2)
        ttk.Label(pf, text="Profile:").pack(side="left", padx=4)
        self.profile_var = tk.StringVar()
        self._profile_combos = []
        cb = ttk.Combobox(pf, textvariable=self.profile_var, width=24)
        cb.pack(side="left", padx=4)
        cb.bind("<<ComboboxSelected>>", self._on_profile_selected)
        self._profile_combos.append(cb)
        self.profile_state_lbl = tk.Label(pf, text="No profile saved",
                                          bg=BG, fg="#9d9d9d", anchor="w")
        self.profile_state_lbl.pack(side="left", padx=8)

        btns = ttk.Frame(top)
        btns.pack(fill="x", pady=4)
        for text, cmd in [("Save profile", self._save_profile),
                          ("Load profile", self._load_profile),
                          ("Export…", self._export_profile),
                          ("Import…", self._import_profile),
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

        self._build_restore_tab(restore)

        self._saved_snapshot = None
        self._warned_plain_export = False
        for v in list(self.vars.values()) + [self.merge_var, self.del_segs_var]:
            v.trace_add("write", lambda *a: self._update_profile_state())
        self._init_profiles()

    # ---- profile state ------------------------------------------------------
    def _init_profiles(self):
        from engine import profiles as profiles_mod
        self.profiles_mod = profiles_mod
        allp, last = profiles_mod.load_all()
        self._refresh_profile_combos(allp)
        if last and last in allp:
            self.profile_var.set(last)
            self._apply_profile(last, allp[last])
        else:
            self._update_profile_state()

    def _refresh_profile_combos(self, allp=None):
        if allp is None:
            allp, _ = self.profiles_mod.load_all()
        names = sorted(allp)
        for cb in self._profile_combos:
            cb["values"] = names

    def _on_profile_selected(self, _event=None):
        name = self.profile_var.get().strip()
        allp, _ = self.profiles_mod.load_all()
        if name in allp:
            self._apply_profile(name, allp[name])

    def _apply_profile(self, name, profile):
        p = self.profiles_mod.normalize(profile)
        for k in ("endpoint", "bucket", "access_key", "secret_key", "prefix",
                  "segment_mb", "level"):
            self.vars[k].set(str(p.get(k, "")))
        self.merge_var.set(p["merge"])
        self.del_segs_var.set(p["delete_segments"])
        self.profile_var.set(name)
        self._saved_snapshot = p
        self._update_profile_state()

    def _update_profile_state(self):
        if not hasattr(self, "profile_state_lbl"):
            return
        name = self.profile_var.get().strip()
        if self._saved_snapshot is None:
            allp, _ = self.profiles_mod.load_all()
            if not allp:
                self.profile_state_lbl.config(text="No profile saved", fg="#9d9d9d")
                return
            self.profile_state_lbl.config(text="unsaved changes", fg="#d7a000")
            return
        current = self.profiles_mod.normalize(self._form())
        if name and current == self._saved_snapshot:
            self.profile_state_lbl.config(text=f"Profile '{name}' — saved",
                                          fg="#6cc070")
        else:
            self.profile_state_lbl.config(text="unsaved changes", fg="#d7a000")

    def _build_restore_tab(self, parent):
        top = ttk.Frame(parent, padding=8)
        top.pack(fill="both", expand=True)
        pf = ttk.Frame(top)
        pf.pack(fill="x", pady=2)
        ttk.Label(pf, text="Profile:").pack(side="left", padx=4)
        cb = ttk.Combobox(pf, textvariable=self.profile_var, width=24)
        cb.pack(side="left", padx=4)
        cb.bind("<<ComboboxSelected>>", self._on_profile_selected)
        self._profile_combos.append(cb)
        ttk.Label(pf, text="(import a profile on the Capture tab — credentials "
                           "are shared with Restore)").pack(side="left", padx=8)
        mf = ttk.LabelFrame(top, text="Manifest (uses Wasabi credentials from the Capture tab)", padding=6)
        mf.pack(fill="x")
        ttk.Button(mf, text="List manifests", command=self._restore_list).pack(anchor="w", pady=2)
        self.restore_lb = tk.Listbox(mf, bg=BG2, fg=FG, height=8)
        self.restore_lb.pack(fill="x", pady=2)
        df = ttk.Frame(top)
        df.pack(fill="x", pady=4)
        ttk.Label(df, text="Destination folder").pack(side="left", padx=4)
        self.dest_var = tk.StringVar(value=os.path.expanduser("~\\Desktop"))
        ttk.Entry(df, textvariable=self.dest_var, width=50).pack(side="left", padx=4)
        ttk.Button(df, text="Browse…", command=self._browse_dest).pack(side="left")
        ttk.Button(df, text="Restore", command=self._restore_start).pack(side="left", padx=8)

    def _browse_dest(self):
        from tkinter import filedialog
        d = filedialog.askdirectory()
        if d:
            self.dest_var.set(d)

    def _restore_list(self):
        form = self._form()
        if not form["bucket"]:
            messagebox.showwarning("VHD2Wasabi", "Fill in bucket and credentials (Capture tab).")
            return

        def fetch():
            try:
                store = self._store(form)
                keys = manifest_mod.list_manifests(store, form["prefix"])
                self.after(0, lambda: self._fill_restore_list(keys))
            except Exception as e:
                self.log(f"manifest list failed: {e}")
        threading.Thread(target=fetch, daemon=True).start()

    def _fill_restore_list(self, keys):
        self.restore_lb.delete(0, "end")
        for k in keys:
            self.restore_lb.insert("end", k)
        if not keys:
            self.restore_lb.insert("end", "(no manifests found)")
        self._restore_keys = keys

    def _restore_start(self):
        if self.worker and self.worker.is_alive():
            messagebox.showinfo("VHD2Wasabi", "A job is already running.")
            return
        sel = self.restore_lb.curselection()
        keys = getattr(self, "_restore_keys", [])
        if not sel or not keys:
            messagebox.showwarning("VHD2Wasabi", "Select a manifest first.")
            return
        key = keys[sel[0]]
        dest = self.dest_var.get().strip()
        if not dest:
            messagebox.showwarning("VHD2Wasabi", "Choose a destination folder.")
            return
        form = self._form()
        store = self._store(form)
        cancel = threading.Event()
        self._restore_cancel = cancel

        def progress(stage, dl, ex, seg_i, seg_n):
            frac = None
            self.q.put(("speed",
                        f"restore: {stage} — downloaded {human(dl)}, "
                        f"extracted {human(ex)}"
                        + (f", segment {seg_i}/{seg_n}" if seg_i else "")))
        def go():
            try:
                m = manifest_mod.download_manifest(store, key)
                from engine import restore as restore_mod
                res = restore_mod.restore(store, m, dest, log=self.log,
                                          progress=progress, cancel_event=cancel)
                self.log(f"RESTORE COMPLETE: {res['path']} ({human(res['size'])})"
                         + (" — footer OK" if res.get("footer") else ""))
                self.q.put(("status", "Restore complete"))
            except Exception as e:
                self.log(f"RESTORE FAILED: {e}")
                self.q.put(("status", "Restore failed"))
        self.worker = threading.Thread(target=go, daemon=True)
        self.worker.start()

    # ---- helpers ------------------------------------------------------------
    def _mode_changed(self):
        if self.mode_var.get() == "disk":
            self.vol_frame.pack_forget()
            self.disk_frame.pack(fill="x")
        else:
            self.disk_frame.pack_forget()
            self.vol_frame.pack(fill="x")

    def _refresh_sources(self):
        if self.mode_var.get() == "disk":
            self._refresh_disks()
        else:
            self._refresh_volumes()

    def _refresh_disks(self):
        from engine import diskinfo
        for w in self.disk_frame.winfo_children():
            w.destroy()
        self.disk_var = tk.IntVar(value=-1)
        try:
            self.disks = diskinfo.list_disks()
        except Exception as e:
            ttk.Label(self.disk_frame, text=f"Disk enumeration failed: {e}").pack(anchor="w")
            self.disks = []
        for d in self.disks:
            parts = ", ".join(
                f"{p['letter'] + ':' if p.get('letter') else 'P' + str(p['number'])}"
                f" {p['size']/1024**3:.1f}G"
                + (f" {p['fs']}" if p.get("fs") else "")
                for p in d["partitions"])
            ttk.Radiobutton(
                self.disk_frame, variable=self.disk_var, value=d["number"],
                text=f"Disk {d['number']}  {d['model']}   "
                     f"{d['size']/1024**3:.1f} GiB  {d['style']}   [{parts}]"
            ).pack(anchor="w")
        if not self.disks:
            ttk.Label(self.disk_frame, text="No physical disks found").pack(anchor="w")

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
        d = {k: v.get().strip() for k, v in self.vars.items()}
        d["segment_mb"] = d["segment_mb"] or "10240"
        d["level"] = d["level"] or "5"
        d["merge"] = bool(self.merge_var.get())
        d["delete_segments"] = bool(self.del_segs_var.get())
        return d

    def _save_profile(self):
        name = self.profile_var.get().strip() or "default"
        try:
            self.profiles_mod.save_profile(name, self._form())
        except Exception as e:
            messagebox.showerror("VHD2Wasabi", f"Save failed: {e}")
            return
        self.profile_var.set(name)
        self._saved_snapshot = self.profiles_mod.normalize(self._form())
        self._refresh_profile_combos()
        self._update_profile_state()
        self.log(f"Profile '{name}' saved to {self.profiles_mod.PROFILES_PATH} "
                 "(contains secrets — protect it)")

    def _load_profile(self):
        name = self.profile_var.get().strip()
        allp, _ = self.profiles_mod.load_all()
        if not allp:
            messagebox.showinfo("VHD2Wasabi", "No saved profile found.")
            return
        if name not in allp:
            name = sorted(allp)[0]
        self._apply_profile(name, allp[name])
        self.log(f"Profile '{name}' loaded")

    def _export_profile(self):
        from tkinter import filedialog, simpledialog
        name = self.profile_var.get().strip() or "default"
        path = filedialog.asksaveasfilename(
            defaultextension=".vhd2wprofile",
            initialfile=f"{name}.vhd2wprofile",
            filetypes=[("VHD2Wasabi profile", "*.vhd2wprofile"),
                       ("JSON", "*.json"), ("7-Zip", "*.7z"), ("All files", "*.*")])
        if not path:
            return
        pw = simpledialog.askstring(
            "Export profile",
            "Password for AES-256 encryption (leave empty for plain JSON):",
            show="*", parent=self)
        if pw is None:
            return
        try:
            self.profiles_mod.export_profile(name, self._form(), path,
                                             password=pw or None)
        except Exception as e:
            messagebox.showerror("VHD2Wasabi", f"Export failed: {e}")
            return
        if not pw and not self._warned_plain_export:
            self._warned_plain_export = True
            messagebox.showwarning(
                "Unencrypted export",
                "This file contains your Wasabi SECRET KEY in plain text.\n"
                "Store it securely and delete it when no longer needed.")
        self.log(f"Profile '{name}' exported to {path} "
                 f"({'AES-256 encrypted' if pw else 'plain JSON'})")

    def _import_profile(self):
        from tkinter import filedialog, simpledialog
        path = filedialog.askopenfilename(
            filetypes=[("Profile files", "*.vhd2wprofile *.json *.7z"),
                       ("All files", "*.*")])
        if not path:
            return
        pw = None
        if self.profiles_mod.is_7z(path):
            pw = simpledialog.askstring("Import profile",
                                        "Password for this encrypted profile:",
                                        show="*", parent=self)
            if pw is None:
                return
        try:
            suggested, profile = self.profiles_mod.import_profile(path, password=pw)
        except Exception as e:
            messagebox.showerror("VHD2Wasabi", f"Import failed: {e}")
            return
        name = simpledialog.askstring("Import profile", "Save as profile name:",
                                      initialvalue=suggested, parent=self)
        if not name:
            return
        name = name.strip()
        allp, _ = self.profiles_mod.load_all()
        if name in allp and allp[name] != profile:
            if not messagebox.askyesno(
                    "VHD2Wasabi",
                    f"Profile '{name}' already exists. Overwrite it?"):
                return
        try:
            self.profiles_mod.save_profile(name, profile)
        except Exception as e:
            messagebox.showerror("VHD2Wasabi", f"Save failed: {e}")
            return
        self._refresh_profile_combos()
        self._apply_profile(name, profile)
        self.log(f"Profile '{name}' imported and saved — ready to use "
                 "(Capture and Restore tabs share these credentials)")

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
        disk_mode = self.mode_var.get() == "disk"
        if disk_mode:
            dn = getattr(self, "disk_var", None)
            if not dn or dn.get() < 0:
                messagebox.showwarning("VHD2Wasabi", "Select a disk.")
                return
            vols = []
        else:
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
            start_fresh=self.fresh_var.get(), callbacks=callbacks,
            merge_on_complete=self.merge_var.get(),
            delete_segments_after_merge=self.del_segs_var.get())

        def go():
            try:
                problems, warnings, _e = preflight.run_all(store, vols, log=self.log)
                disk_list = []
                if disk_mode:
                    from engine import diskinfo
                    dsel = self.disk_var.get()
                    disk_list = [d for d in getattr(self, "disks", [])
                                 if d["number"] == dsel]
                    if not disk_list:
                        self.log("PRE-FLIGHT FAILED: selected disk no longer present")
                        return
                    dprobs, dwarns = diskinfo.preflight_disk(disk_list[0])
                    problems.extend(dprobs)
                    warnings.extend(dwarns)
                    d = disk_list[0]
                    self.log(f"Disk {d['number']}: {d['model']}, "
                             f"{d['size']/1024**3:.1f} GiB, {d['style']}, "
                             f"{len(d['partitions'])} partition(s)")
                for w in warnings:
                    self.log(f"WARNING: {w}")
                if problems:
                    self.log("PRE-FLIGHT FAILED:")
                    for p in problems:
                        self.log(f"  - {p}")
                    return
                self.log("Pre-flight OK — starting capture")
                if disk_mode:
                    st = manifest_mod.load_state(socket.gethostname(),
                                                 f"Disk{self.disk_var.get()}")
                    if st and st.get("status") != "done" and not self.fresh_var.get():
                        self.log("saved state found — resuming automatically")
                    self.pipeline.run_disks([self.disk_var.get()])
                else:
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
        if getattr(self, "_restore_cancel", None):
            self._restore_cancel.set()
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
