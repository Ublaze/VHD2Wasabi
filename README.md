# VHD2Wasabi

Disk2vhd-style live Windows disk capture that streams the image through 7-Zip
compression **straight into a Wasabi (S3-compatible) bucket** — no local staging
of the full VHD. RAM buffer is only a few MB; disk spool is at most ONE
compressed segment (not the whole image).

Single self-contained EXE: `dist\VHD2Wasabi.exe` (PyInstaller onefile, UAC
admin manifest, bundled `7za.exe`).

## How it works

Per selected volume (multiple volumes are captured **sequentially**, each into
its own object set — never merged, by design):

1. **VSS snapshot** via WMI:
   `(Get-WmiObject -List Win32_ShadowCopy).Create("C:\","ClientAccessible")`,
   shadow ID resolved to `\\?\GLOBALROOT\Device\HarddiskVolumeShadowCopy<N>`.
   The shadow is always deleted in a `finally` (`vssadmin delete shadows`).
2. **Raw sequential read** of the shadow device with pywin32
   (`CreateFile`/`ReadFile`, 4 MiB aligned chunks). Volume length comes from
   `IOCTL_DISK_GET_LENGTH_INFO`.
3. **Fixed-VHD footer** appended in software: pure-Python 512-byte `conectix`
   footer (features, version, data offset `0xFFFFFFFFFFFFFFFF`, timestamp,
   creator, original/current size, CHS geometry per the VHD spec, disk type 2 =
   fixed, checksum = one's complement of the byte sum). Unit-tested in
   `test_footer.py`.
4. **Segmentation**: the uncompressed stream (volume bytes + footer) is cut
   into fixed-size segments (default 10 GiB uncompressed, configurable). Each
   segment is compressed independently with 7-Zip
   (`7za a spool.7z -siData -mx=N`).
5. **Multipart upload**: each segment becomes ONE S3 object, uploaded with
   S3 multipart (parts ≥ 8 MiB, 3 retries with backoff, abort on failure).
   `CompleteMultipartUpload` does the server-side merge of parts.
   Object keys: `<prefix>/<server>/<YYYY-MM-DD>/<VolumeLabel-or-letter>.partNNN.7z`.
6. **Hashing**: SHA-256 of uncompressed bytes per segment is computed on the
   fly; sizes and ETags are recorded. The whole-image hash in the manifest is
   the SHA-256 of the concatenated per-segment SHA-256 hex strings (a composite
   that is computable even for resumed runs).
7. **Manifest**: after each volume (and locally after each segment) a manifest
   JSON (server, volume, sizes, segment list with sha256/size/etag/key,
   timestamps, tool version) is written to the bucket next to the objects
   (`<keyprefix>.manifest.json`) and to
   `%LOCALAPPDATA%\VHD2Wasabi\state\<server>\<volume>.json`.

### Deviation: spool file instead of archive-to-stdout

### Unreadable shadow-device tail (dd-style tolerance)

Some VSS shadow devices report a length whose final sector(s) cannot actually
be read (observed in production: last 4 KiB block unreadable on bare metal;
~2 MiB on a source disk that is itself a Hyper-V VHD with unmapped tail
blocks). Like `dd conv=sync,noerror`, the reader retries once and then
zero-fills the remainder **only within the last 32 MiB of the device**
(`TAIL_PAD_MAX` in volumereader.py), logging a loud warning advising chkdsk
on first boot if the OS volume ends inside the padded region. Padding is safe
when the tail is unallocated space or holds only the NTFS backup boot sector.
A short read anywhere earlier — including a gap that becomes readable again —
raises a hard `ShortReadError`: silent mid-device padding would corrupt the
image. The VHD footer is still appended after the padded
bytes (total stream = device length + 512). Note: a resumed final segment
re-appends the footer, so its embedded UUID/timestamp differs from the
original attempt — harmless (the manifest is self-consistent).

The plan called for `7za a -siData -so` piping compressed bytes to stdout.
**The 7-Zip CLI does not implement `-so` for the `a` command** (returns
"System ERROR: Not implemented" — verified with both 7za 23.01 and the full
7z 23.01). A Windows named-pipe archive target was also tested: 7-Zip first
opens the archive for reading and expects a seekable file, so this hangs.
Therefore each segment is compressed to a **spool file** in `%TEMP%\VHD2Wasabi-spool`
which the uploader **tail-uploads concurrently** (near-streaming: upload
overlaps compression; spool never exceeds one compressed segment and is
deleted right after). Everything else follows the plan.

## Install / build

```sh
# dev run
python app/main.py

# tests
python test_footer.py
python test_pipeline_local.py

# build EXE
python build_exe.py        # -> dist\VHD2Wasabi.exe
```

`app\resources\7za.exe` is the 7-Zip standalone console from the official
`7z2301-extra.7z` (https://www.7-zip.org/download.html). If it is missing,
pre-flight fails with a clear message — place `7za.exe` there (the constant
`SEVENZIP` in `app/engine/compressor.py` controls the binary name; `7zr.exe`
also works).

## Usage

1. Run as Administrator (the EXE requests elevation via its manifest).
2. Tick the volumes to capture (letter, label, total/used/free GB shown).
3. Fill the Wasabi form: endpoint (default `s3.eu-central-1.wasabisys.com`),
   bucket, access/secret key, prefix, segment size MB, compression level.
   **Save/Load profile** stores this in
   `%LOCALAPPDATA%\VHD2Wasabi\profiles.json` — **it contains secrets**, protect
   the file.
4. **Test connection** (HeadBucket) → **Start**.
5. The status line shows read/compress/upload MB/s so the bottleneck is
   visible; per-volume progress bar with ETA; scrolling log.
6. **Cancel** aborts the multipart upload, deletes the snapshot, and preserves
   state for resume.

### Resume

State is kept per server/volume. On start, segments marked complete are
verified with HeadObject (size match) and skipped; streaming resumes at the
next segment (new snapshot, reader seeks to the segment offset). Completed
volumes are skipped entirely. Resume is automatic when state exists; tick
**Start fresh** to ignore it.

### Verify

**Verify…** lists manifests in the bucket; pick one. Each segment is:
HeadObject-checked, downloaded to a temp file, integrity-tested (`7za t`),
then extracted streaming (`7za x -so`) and re-hashed against the manifest
SHA-256. Per-segment PASS/FAIL is reported.

## Restore

```sh
# 1. download the parts (aws CLI, s3cmd, or the Wasabi console)
aws s3 sync s3://<bucket>/<prefix>/<server>/<date>/ . --endpoint-url https://s3.eu-central-1.wasabisys.com

# 2. extract each part (each is an independent .7z containing stream "Data")
7za x server.part001.7z     # produces file "Data"
7za x server.part002.7z     # rename between extracts!
...

# 3. concatenate in order -> fixed VHD
copy /b Data001+Data002+Data003 image.vhd

# 4. attach: Disk Management -> Action -> Attach VHD, or mount as a Hyper-V disk
```

(Extraction always writes a file named `Data`; rename after each part before
extracting the next.)

## VSS diff-area guidance

Pre-flight shows shadow storage allocation (`vssadmin list shadowstorage`) and
warns if free space < 5% of volume size. During capture the diff-area usage is
polled every ~30 s: warning at 80%, clean abort (snapshot deleted, multipart
aborted, state kept) at ~95%.

Point the diff area at another volume (recommended for busy/occupied disks):

```cmd
vssadmin add shadowstorage /for=C: /on=E: /maxsize=40GB
vssadmin list shadowstorage
vssadmin resize shadowstorage /for=C: /on=E: /maxsize=40GB
```

## Merge on completion (server-side)

With **"Merge into single object after verify"** (default ON), after a volume's
segments are uploaded **and verified** (full download + `7za t` + re-hash), the
tool composes ONE object per volume entirely server-side:
`<volume-folder>/<Volume>.full.7zcat`.

- The `.7zcat` format is the **plain concatenation of the segment .7z
  archives**; the manifest records each segment's byte offset + compressed
  size (`merged.offsets`), so restore splits the stream at exact boundaries.
- Segments ≥ 5 MiB are added as **UploadPartCopy** parts (byte-range
  server-side copy — no data is re-uploaded), chunked to at most
  `COPY_PART_MAX` (default **1 GiB**) per part: Wasabi answers a copy only
  when it finishes, and un-chunked multi-GB copies exceed client read
  timeouts. One source segment can therefore map to several consecutive
  parts. The S3 client uses `read_timeout=300` / `connect_timeout=30` with
  ×3 idempotent part retries; a final failure aborts the multipart cleanly.
- Runs of smaller segments are downloaded into memory (they're tiny) and
  flushed as regular parts once the
  buffer reaches ≥ 5 MiB; no non-final part is ever < 5 MiB. If the whole
  merged object is < 5 MiB a single `put_object` is used.
- The manifest is bumped to `manifest_version: 2` and gains `merged_key` +
  `merged` (offsets, total size, part count). Old fields are kept.
- **Segment objects are kept by default** for redundancy; tick "Delete segment
  objects after merge" to remove them once the merge completes.
- A merge failure/abort cancels the multipart upload and leaves all segment
  objects and state untouched.

## Restore tab

The **Restore** tab (uses the Capture tab's credentials): **List manifests**,
pick one, choose a destination folder, click **Restore**.

- Manifest with `merged_key`: the merged object is stream-downloaded in a
  single pass and split into per-segment temp chunk files at the manifest's
  byte offsets (RAM stays at a few MB; chunks live on the destination disk
  and are deleted as consumed).
- Otherwise the segment objects are downloaded one by one.
- Each chunk is extracted streaming (`7za x -so`) and appended to
  `<Volume>.vhd`; per-segment SHA-256 is verified against the manifest.
- At the end the VHD footer checksum is validated and "footer OK" reported.
- Output files are named `<server>_<identity>.vhd` (identity = original drive
  letter for volume captures, `Disk<N>` for disk captures), e.g.
  `TRX3WEB_C.vhd`, `TRX3WEB_Disk0.vhd`; existing files are never overwritten
  (` (2)` is appended).
- Progress (downloaded / extracted MB, current segment n/N) shows in the
  status line; Cancel aborts cleanly.

Manual restore still works exactly as before (download parts, `7za x` each,
`copy /b` concatenate) — for a `.full.7zcat` object, split it at the manifest
offsets first or just use the Restore tab.

## Moving profiles between machines

The Capture tab shows a **profile saved-state indicator**: green
"Profile '<name>' — saved" when the form matches the saved snapshot, amber
"unsaved changes" after any edit, grey "No profile saved" on a fresh install.
Profiles are named and stored in `%LOCALAPPDATA%\VHD2Wasabi\profiles.json`
(legacy single-profile files are migrated to profile "default"); the last used
profile loads automatically at startup and is shared by the Capture and
Restore tabs.

- **Export…** writes a portable `.vhd2wprofile` file (format version,
  export timestamp, profile name + all settings: endpoint, bucket, keys,
  prefix, segment size, compression, merge flags). Supply a password to wrap
  it in an **AES-256, header-encrypted 7z** (built with the bundled 7za in a
  temp dir, cleaned up afterwards). Without a password it's plain JSON — a
  one-time warning reminds you that **the file contains your Wasabi secret
  key**; store it securely and delete it when done.
- **Import…** accepts `.vhd2wprofile` / `.json` / `.7z` (prompts for the
  password when encrypted), validates required fields, asks for a profile
  name (confirming before overwriting an existing one), saves it into
  `profiles.json` and loads it into the form.

On a fresh restore machine: install/run the EXE → **Import…** the one file →
the Restore tab's manifest listing and restore work immediately. Secrets are
never written to the log on export/import.

## Whole disk (bootable) mode

Volume-level captures contain no partition table/bootloader, so they don't
**boot** in Hyper-V. Whole-disk mode fixes that: it captures the entire
physical disk — partition table, EFI/MSR/Recovery/system partitions, data
volumes, gaps — into a single bootable fixed VHD, streamed through the same
segment → 7z → Wasabi multipart pipeline (resume, verify, merge, restore all
work unchanged; object layout `<prefix>/<server>/<date>/Disk<N>.partNNN.7z`,
merged `Disk<N>.full.7zcat`, manifest `Disk<N>.manifest.json`).

How it works:

- **Layout**: enumerated once at capture start via
  `IOCTL_DISK_GET_DRIVE_LAYOUT_EX` (pywin32) + WMI for model/letters/
  filesystems — Storage CIM (`Get-Disk`) is avoided; WMI failures degrade to
  live reads with a warning rather than aborting. The frozen layout (style,
  partitions with offsets/lengths/letters/type GUIDs) is stored in the
  manifest (`capture_kind: "disk"`).
- **Snapshots**: every NTFS/ReFS volume on the disk gets its own VSS
  ClientAccessible snapshot before streaming starts — **letterless** system
  partitions (e.g. System Reserved) are snapshotted via their **volume GUID
  path** (`\\?\Volume{GUID}\`, matched to the partition by exact capacity).
  A hard invariant is enforced: the shadow volume's byte length MUST equal
  the partition's byte length, otherwise the capture **aborts loudly** —
  a wrong-source substitution is never written. If no snapshot is possible
  for an unlettered NTFS/ReFS partition, that region falls back to a live
  read with a loud consistency warning (still correct data, just not
  crash-consistent). Partition→letter mapping is by exact StartingOffset
  (WMI partition indexes are 0-based, IOCTL numbers are 1-based — an
  off-by-one there once caused C:'s shadow to be mapped onto a letterless
  system partition; the offset mapping + length invariant now make that
  class of bug impossible). The partition table, gaps, ESP/FAT32/raw and
  other non-snapshot regions are read **live** from `\\.\PhysicalDriveN`.
- **Resume**: per-segment as in volume mode. Snapshot IDs are not reused
  across runs — a resumed run takes fresh snapshots and re-derives the
  layout; if the live layout differs from the frozen one it aborts with a
  clear message (never silently mixes layouts).
- **Pre-flight**: blocks on dynamic (LDM) disks and BitLocker-protected
  partitions (same limitation as Disk2vhd), warns if the disk exceeds the
  ~2040 GiB fixed-VHD limit.
- **Manifest** stays backward compatible: `volume` = `Disk<N>`, so the
  Restore tab works unchanged and old v1/v2 volume manifests still restore.

**Hyper-V boot notes**: the VM generation must match the source firmware —
BIOS/MBR source → **Generation 1** VM; UEFI/GPT source → **Generation 2**
VM (Secure Boot on). Check the source with `msinfo32` ("BIOS Mode") or
`bcdedit`. Attach the restored `Disk<N>.vhd` as the VM's IDE (Gen 1) or SCSI
(Gen 2) boot disk.

## Limitations

- **BitLocker volumes unsupported** (same as Disk2vhd — raw sector capture of
  an unlocked volume is not attempted; the captured image would be ciphertext).
- **VHD 2 TB limit**: fixed VHD cannot address more than 2 TiB; larger volumes
  are rejected at the CHS geometry level (clamped) — split or wait for VHDX (v2).
- **Volume-level, not whole-disk**: v1 captures volumes (like Disk2vhd's
  per-volume mode), one VHD stream per volume; no partition table/MBR is
  included.
- Whole-image SHA-256 is a composite of per-segment hashes (see How it works).
- Compression happens on the spool path; ensure `%TEMP%` has room for one
  compressed segment (typically a few GB for a 10 GiB segment of real data).

## v2 roadmap

- Whole-disk mode (partition table + all volumes in one stream)
- Parallel volume capture
- VHDX output (4 KB sectors, > 2 TB)
- Optional client-side encryption before upload
- Delta/incremental capture

## Layout

```
app\main.py            Tkinter UI + wiring
app\engine\snapshot.py     VSS create/resolve/delete, shadow storage parsing
app\engine\volumereader.py volume enumeration + raw shadow device reader
app\engine\diskinfo.py     physical disk layout (IOCTL + WMI, with fallbacks)
app\engine\diskreader.py   whole-disk stream assembly (shadow + live regions)
app\engine\vhdfooter.py    fixed-VHD footer (pure Python)
app\engine\compressor.py   7-Zip segment compressor (spool)
app\engine\uploader.py     S3/local multipart stores + concurrent tail-uploader
app\engine\pipeline.py     orchestration: segments, resume, cancel, verify, merge
app\engine\merge.py        server-side .7zcat composition (UploadPartCopy)
app\engine\restore.py      restore to .vhd from merged object or segments
app\engine\manifest.py     state/manifest JSON
app\engine\preflight.py    admin/VSS/7za/bucket/shadow-storage checks
app\resources\7za.exe      bundled 7-Zip console
build_exe.py               PyInstaller wrapper
test_footer.py             footer checksum/field smoke test
test_pipeline_local.py     synthetic 200 MiB round-trip test (no S3)
test_disk_capture.py       synthetic 300 MiB GPT disk round-trip (no S3/VSS)
```
