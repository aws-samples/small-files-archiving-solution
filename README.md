# Small Files Archiver → S3 + S3 Tables Manifest

> 🌐 **한국어**: [README-ko.md](README-ko.md) · This is the English document.

An archiving solution that **bundles** large numbers of small filesystem files into tar objects on Amazon S3, and records each file's location (which tar, which byte range) in an **Amazon S3 Tables (managed Apache Iceberg)** table.

Based on [aws-samples/small-files-archiving-solution](https://github.com/aws-samples/small-files-archiving-solution), this version moves the manifest from CSV objects to S3 Tables and reworks the internals with single-pass MD5, streaming tar, batched commits, and multiprocess parallelism.

### Key features

- **File bundling** — packs many small files into large tar objects, cutting PUT requests and sidestepping the small-object storage penalty (e.g. Glacier IR 128 KB minimum billing).
- **S3 Tables manifest** — records every file's `tar_key` + byte range in a managed Apache Iceberg table, so an individual file stays locatable inside a tar.
- **Direct individual-file restore** — extracts a single file straight from its tar via byte-range GET, without downloading the whole tar (`v3/fss3-restore.py`).
- **Two input modes** — walk a directory (`--src-path`), or archive an explicit `--input-file` list of files (from a DB export, `find`, or a change-list).
- **Per-file MD5 integrity** — every file is hashed (single-pass, `--md5`) and the digest is stored in the manifest, so restores can be verified byte-for-byte. Integrity checking is essential for an archive, so always enable it.
- **Multiprocess parallelism** — tar packing and upload run across worker processes, bypassing the GIL; manifest commits are batched to avoid Iceberg small-file/snapshot explosion.
- **Optional parallel file scanning** — the source tree can be enumerated by `--scan-threads` threads feeding a bounded queue, so a latency-bound network filesystem doesn't cap the whole pipeline at one thread's stat rate. Serial by default; opt in when the source is remote.
- **Safe concurrent runs** — several archiver runs can write to one manifest table at the same time, each with its own tar namespace. See [§6](#6-running-several-archivers-at-once).

> ⚠️ Not official AWS documentation. This is field-test material.

---

## 1. Overview

### Why bundle

Uploading an archive of tens-of-KB files — hundreds of thousands to billions of them — as individual S3 objects blows up two costs:

- **Request cost**: one PUT per file → millions to billions of PUTs.
- **Small-object penalty**: Glacier Instant Retrieval and Standard-IA have a **128 KB minimum billable size**; Glacier Flexible Retrieval and Deep Archive add a **40 KB per-object metadata overhead**. A tens-of-KB file is billed far above its real size.

Bundling files into large tars (e.g. 20 MB) cuts the object count by hundreds to thousands of times, so both costs effectively disappear. The remaining problem — how to find and retrieve an individual file inside a tar — is solved by a **manifest (a file → tar + byte-range index)**.

### Architecture

```
File source (directory walk of --src-path, or an explicit --input-file list)
   └─ K scanner threads (--scan-threads) enumerate in parallel
        │  each closes a batch at --max-size / --max-files and pushes it
        └─ bounded batch queue
             └─ main thread assigns the tar name and submits
                  └─ ProcessPoolExecutor (N worker processes)
                        │  each worker: build tar (optional MD5) + S3 multipart upload
                        │  → returns manifest rows
                        └─ single writer thread → batched commit to S3 Tables
                                                  (default 50,000 rows)
```

Script: `v3/fss3-archiver-s3tables-mp.py` (multiprocess). CPU-bound work such as tar packing and MD5 runs in parallel across worker processes, bypassing the GIL; the scan runs on threads because it is latency-bound rather than CPU-bound.

Design points:

| Item | Approach | Why |
|---|---|---|
| MD5 | computed once while streaming into tar (optional) | avoids reading each file twice |
| tar buffer | `SpooledTemporaryFile` (spills to disk past a threshold) | prevents large tars from staying resident in RAM |
| byte offset | tar stream `tell()` position | real physical position including tar headers/padding → correct byte-range restore |
| file scan | K threads pulling directories off a shared queue | one `lstat` round-trip per entry is latency-bound; `getdents`/`lstat` release the GIL, so threads genuinely overlap the waiting |
| scan ↔ pack handoff | bounded queue, and the main thread drains whichever tar finished first | the walk keeps running while tars upload, instead of alternating with them; no single slow upload blocks the window |
| manifest write | a **single** writer per run commits in batches | keeps one run's commits serialized → minimal snapshot churn (concurrent *runs* are still safe, see [§6](#6-running-several-archivers-at-once)) |
| commit retry | full-jitter exponential backoff (`--commit-retries`) | a fixed backoff makes two conflicting writers collide again in lockstep |
| process start | `spawn` context | forking while the writer thread holds a lock would deadlock the child |
| S3 Tables connection | direct S3 Tables Iceberg REST endpoint | no Lake Formation setup required |

---

## 2. Installation

- **Python 3.9+** (tested on 3.12.1)
- A pre-created S3 bucket (for tars) and an S3 Tables table bucket (for the manifest). The namespace and table are created automatically (idempotent) by the script.

```bash
pip install "pyiceberg[pyarrow]" boto3
# Test-verified: pyiceberg 0.11.1 / pyarrow 18.1.0 / boto3 1.39.17
```

Required IAM permissions (manifest, S3 Tables direct endpoint):

```
s3tables:GetTableBucket, ListNamespaces, CreateNamespace, GetNamespace,
s3tables:ListTables, CreateTable, GetTable,
s3tables:GetTableMetadataLocation, UpdateTableMetadataLocation,
s3tables:PutTableData, GetTableData
+ s3:PutObject on the tar destination bucket
```

---

## 3. Usage

Every run combines a **file source** (walk `--src-path`, or an explicit `--input-file` list) with a **batching rule** (`--max-size` or `--max-files`). The scenarios below all share the same destination/S3 Tables flags — only the source and batching choices change.

> 💡 [`v3/run.sh`](v3/run.sh) packages every scenario below (plus restore) as ready-to-run bash functions — edit the shared vars at the top, uncomment one call, and run `bash run.sh`.

**Baseline — bundle a directory into uniform-size tars (the common case):**

```bash
python v3/fss3-archiver-s3tables-mp.py \
  --src-path         /data/nfsshare \
  --dst-bucket       your-own-dest-bucket \
  --dst-prefix       20260804 \
  --max-size         20MB \
  --num-workers      8 \
  --md5 \
  --region           ap-northeast-2 \
  --table-bucket-arn arn:aws:s3tables:ap-northeast-2:<account>:bucket/<table-bucket> \
  --namespace        archive \
  --table-name       manifest \
  --batch-size       50000 \
  --tar-storageclass GLACIER_IR
```

The remaining scenarios are the baseline **with the noted flags changed**:

**Fixed file count per tar** — cap each tar at N files instead of a size:

```bash
  --max-files 1000          # replaces --max-size
```

**Archive an explicit list of files** — from a DB export, a `find` result, or a change-list, instead of walking the tree:

```bash
  --input-file files-to-archive.txt --max-files 1000
```

One absolute path per line (blank lines and `#` comments skipped). Missing paths, symlinks, and non-regular files are skipped and logged, but don't stop the run. `--src-path` is still required — it's the base for each tar member's relative path (stored relative to `--src-path` when the file lies under it, else the full path with the leading `/` dropped).

**Cold, write-once data — compress:**

```bash
  --compress --tar-storageclass DEEP_ARCHIVE
```

⚠️ `--compress` disables byte-range individual restore (offsets recorded as `-1`), and `DEEP_ARCHIVE` requires a whole-tar restore before any read — pair them only for archives you don't expect to read back file-by-file. See [Limitations](#limitations-and-notes).

Restoring a single file back out is covered in [§4.2](#42-restore--extract-an-individual-file-via-byte-range-get) (LOOKUP by path, or DIRECT by known offsets).

### Key parameters

| Parameter | Description | Default |
|---|---|---|
| `--src-path` | source directory to archive (recursed); with `--input-file`, the base used to compute each file's relative path | (required) |
| `--input-file` | text file listing files to archive (one path per line; `#` comments and blank lines skipped) instead of walking `--src-path` | off (walk) |
| `--dst-bucket` / `--dst-prefix` | S3 bucket / prefix for tars | (required) |
| `--max-size` **or** `--max-files` | per-tar size (e.g. `512MB`) or file count (pick one) | (required) |
| `--num-workers` | tar-build/upload worker processes | `os.cpu_count()` |
| `--scan-threads` | threads enumerating the source tree (see below) | 1 |
| `--run-id` | token in tar names / log filename that keeps concurrent runs apart | `<timestamp>_<pid>_<rand>` |
| `--md5` | compute and store per-file MD5 for integrity verification | on (default) |
| `--region` | AWS region | (required) |
| `--table-bucket-arn` | S3 Tables table bucket ARN | (required) |
| `--namespace` / `--table-name` | manifest namespace / table | archive / manifest |
| `--batch-size` | rows per manifest commit | 50000 |
| `--flush-interval` | seconds before a partial manifest batch commits anyway | 15 |
| `--commit-retries` | attempts per manifest commit before the run aborts | 8 |
| `--tar-storageclass` | storage class for tar objects (see below) | STANDARD |
| `--spool-max-mb` | in-memory threshold for a tar (spills to disk above it) | 256 |
| `--compress` | gzip the tar (⚠️ see limitations) | off |

Storage layout: tars go to `s3://<bucket>/<prefix>/<YYYYMMDD>/archive_<run-id>_<NNNNN>.tar`; the manifest is written to the S3 Tables table.

#### `--scan-threads`

Enumerating small files costs one `lstat` round-trip per entry, so on a network filesystem the walk is **latency-bound, not CPU-bound** — a single thread caps out around 500–2,000 files/s no matter how many worker processes are packing tars. Threads help because `getdents`/`lstat` release the GIL.

**The default is 1 (serial walk).** Threads exist to hide latency, so they only pay off when there is latency to hide — and whether there is depends entirely on your source. Measured against a simulated 1 ms-per-entry stat (a cold-cache NFS shape) scaling is close to linear:

| `--scan-threads` | 1 | 4 | 8 | 16 | 32 |
|---|---|---|---|---|---|
| speedup (1 ms/entry simulated) | 1.0x | 4.1x | 8.0x | 16.2x | 30.0x |

On a **local NVMe** tree of 523,124 files with the page cache dropped before each measurement, the same code behaves very differently — 30,120 files/s serial, a 1.9x peak at 4 threads, and flat past 8:

| `--scan-threads` | 1 | 2 | 4 | 8 | 16 | 32 |
|---|---|---|---|---|---|---|
| speedup (local NVMe, cold) | 1.0x | 1.6x | **1.91x** | 1.54x | 1.54x | 1.54x |

And in four cold end-to-end 1M-file runs, total wall time was **unchanged** between 1 and 8 scan threads (57.3–58.5 MB/s in every case) — the volume's IOPS budget was the wall, not the walk. That is why the default is 1: a serial walk is the predictable baseline, and raising it is an opt-in for sources that are genuinely latency-bound (NFS/SMB over a network, cross-AZ, cross-region). Measure your mount's actual ceiling first; the server or the NFS RPC slot limit may cap it well before the thread count does:

```bash
# serial enumeration cost
sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
time find /data/nfsshare -type f -printf ''

# same work fanned out over top-level subdirectories
sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
time ls -d /data/nfsshare/*/ | xargs -P 8 -I{} find {} -type f -printf ''
```

If the two times are the same, the filesystem is the wall and more scan threads won't help.

#### `--tar-storageclass` values

The value is passed straight through to S3 `PutObject`, so any S3 storage class is accepted. For this solution the one that matters is **whether individual-file restore (§4.2) works immediately or needs a whole-tar restore first**:

| Class | Individual restore (§4.2) | Notes |
|---|---|---|
| `STANDARD` | ✅ immediate | **default.** No minimum storage duration and no retrieval fee, so nothing surprises you on a first run — but the most expensive per GB to keep. |
| `GLACIER_IR` | ✅ immediate | Glacier Instant Retrieval — archive pricing with millisecond ranged GET. **The right choice for a real cold archive**, at the cost of a 90-day minimum storage duration and a per-GB retrieval fee. |
| `STANDARD_IA` / `ONEZONE_IA` | ✅ immediate | infrequent access; per-GB retrieval fee. `ONEZONE_IA` is single-AZ (cheaper, lower durability). |
| `INTELLIGENT_TIERING` | ✅ immediate* | auto-tiers by access. *Ranged GET is immediate unless an object has moved to the optional Archive Access tiers. |
| `GLACIER` (Flexible Retrieval) | ⚠️ restore first | cheaper storage, but §4.2 fails until you restore the whole tar (minutes–hours). |
| `DEEP_ARCHIVE` | ⚠️ restore first | cheapest storage; whole-tar restore takes up to ~12 h. |

> **Rule of thumb:** the default `STANDARD` is the safe starting point — no minimum duration, no retrieval fee, nothing to regret on a test run. Once you know the data really is cold, `GLACIER_IR` is the one to set: far cheaper per GB and still immediate single-file restore. Only move to `GLACIER` / `DEEP_ARCHIVE` when the data is truly write-once and you accept a restore delay for the whole tar. Classes like `REDUCED_REDUNDANCY`, `EXPRESS_ONEZONE`, `OUTPOSTS`, `SNOW`, and `FSX_*` exist in the API but are not meaningful for this archiving workflow.

#### `--max-files` (`--max-size`) vs `--batch-size`

These two look similar but control **different stages** and never interact:

| Option | What it splits | Stage | Effect |
|---|---|---|---|
| `--max-files` / `--max-size` | files → **tar bundle** | upload (input) | number/size of **tar objects** in S3 |
| `--batch-size` | manifest rows → **one commit** | S3 Tables write (output) | number of Iceberg **commits (snapshots)** |

- **`--max-files` / `--max-size`** — how many files (or how many bytes) go into one tar. The scanner cuts a new tar when the running count/size hits the limit. Pick exactly one. This determines the size of each S3 tar object (and thus your PUT-request and small-object savings). Size-based (`--max-size 20MB`) is usually preferred so objects stay uniform even when file sizes vary.
- **`--batch-size`** — how many manifest rows (**one row per file**, not per tar) the single writer accumulates before appending to S3 Tables. Default 50,000. Small values create frequent commits → many small files / snapshots in the manifest → slower queries, so keep it large for big runs.

Worked example — 1M files:

```
--max-files 1000    → ~1,000 tar objects        (S3 object count)
--batch-size 50000  → ~21 manifest commits      (Iceberg snapshot count)
```

Think of it as: `--max-files` decides how many **boxes** you pack the files into; `--batch-size` decides how many **packing-list rows** you record per ledger entry. Changing one does not change the other.

### Manifest schema (S3 Tables)

| Column | Type | Description |
|---|---|---|
| `tar_key` | string | S3 key of the tar containing the file |
| `source_key` | string | original file relative path |
| `byte_start` / `byte_end` | long | byte range within the tar (for Range GET) |
| `md5` | string | MD5 of the original file (computed by default; null only if disabled) |
| `filesize` | long | original file size |
| `archive_ts` | timestamptz | archive time (partition: `day(archive_ts)`) |

---

## 4. Search and restore

### 4.1 Search — Athena or PyIceberg

```sql
SELECT tar_key, byte_start, byte_end, md5
FROM archive.manifest
WHERE source_key LIKE '%file0499%'
  AND archive_ts BETWEEN TIMESTAMP '2026-08-04' AND TIMESTAMP '2026-08-05';
```
> To query with Athena, the table bucket must have AWS analytics services (Glue Data Catalog) integration enabled. You can also query directly with PyIceberg (see 4.2).

### 4.2 Restore — extract an individual file via byte-range GET

Use `v3/fss3-restore.py`. It fetches only the byte range holding the file — the archiver records `byte_start` at the file's tar header and `byte_end` at the end of its padding, so that range is a self-contained single-member tar that `tarfile` opens directly. No full-tar download.

**Note:** the manifest stores `tar_key` but **not** the bucket, so `--tar-bucket` is always required.

**DIRECT mode** — you already have the offsets (e.g. from the 4.1 query):

```bash
python v3/fss3-restore.py \
  --tar-bucket       your-own-dest-bucket \
  --tar-key          20260804/20260804/archive_20260804_120000_31417_a3f1_00001.tar \
  --byte-start       1536 \
  --byte-end         21503 \
  --out              ./restored/file0499.dat
```

**LOOKUP mode** — you only know the original path; the script queries the manifest (exact `source_key` match with Iceberg predicate pushdown — no full-table scan) and resolves the offsets itself:

```bash
python v3/fss3-restore.py \
  --region           ap-northeast-2 \
  --table-bucket-arn arn:aws:s3tables:ap-northeast-2:<account>:bucket/<table-bucket> \
  --namespace        archive \
  --table-name       manifest \
  --source-key       sub/file0499.dat \
  --tar-bucket       your-own-dest-bucket \
  --out-dir          ./restored/ \
  --verify-md5
```

`--out` writes to an exact path; `--out-dir` preserves the file's archived relative path underneath. `--verify-md5` (LOOKUP) checks the extracted bytes against the manifest MD5; in either mode `--verify-md5 <hex>` checks an explicit value. If a `source_key` matches more than one row (same path archived in multiple runs), LOOKUP picks the newest `archive_ts` and warns — query 4.1 and use DIRECT with an explicit `--tar-key` to pick a specific one.

**Storage-class caveats:** this works with no wait on `STANDARD` (the default), `GLACIER_IR`, and the IA classes. **If the tar is in Glacier Flexible Retrieval / Deep Archive, you must first restore the entire tar (hours) before a ranged GET succeeds** — the script detects this and errors clearly. Files archived with `--compress` have offsets of `-1` and cannot be range-restored; extract the whole `.tar.gz` instead.

---

## 5. Performance and cost (measured in the test environment)

Test environment: EC2 (Amazon Linux, 8 vCPU, ap-northeast-2), source = NFS `/data/nfsshare` (avg ~19 KB/file), Python 3.12.1.

### 5.1 Throughput

1M files (fs1+fs2, 19.49 GB), 8 workers, MD5 on:

| Elapsed | Rate | Failures |
|---|---|---|
| 89.0s | 224.3 MB/s | 0 |

Single-pass hashing adds no extra read (the bytes are already being read to build the tar), so per-file MD5 is inexpensive on this transfer/I/O-dominated workload.

### 5.2 Object consolidation and request reduction

For the 1M-file run:

| Approach | Object PUTs | Consolidation |
|---|---|---|
| Individual upload | 1,006,108 | — |
| **Bundling (this solution)** | **1,000** (979 tars + 21 manifest commits) | **~1,028 : 1** |

→ About **1,000× fewer** PUT requests. Since each tar is 20 MB, it also avoids the 128 KB minimum-billing penalty of GLACIER_IR / IA entirely if you move the tars there.

### 5.3 Cost (approximate)

At this scale (~20 GB source, 979 tars, 1M manifest rows) the monthly storage cost — tars in GLACIER_IR plus the S3 Tables manifest — is **very low** (order of tens of cents/month). Keeping the manifest in S3 Tables does not inflate cost, because **batched commits (50,000 rows/commit)** avoid small-file / snapshot explosion, so it does not offset the bundling savings.

> Compute exact figures from region/period-specific rates (the above is approximate).

### 5.4 Integrity verification

Files were sampled from the first, middle, and last tars and restored from the actual tar via byte-range GET using the offset in the S3 Tables manifest. **Filename, size, and MD5 all matched.** The correctness of single-pass MD5 and physical tar offsets was confirmed against real S3.

`v3/fss3-restore.py` itself was also verified end-to-end against real S3 + S3 Tables (a 523,123-row manifest, MD5 on). Both **DIRECT** and **LOOKUP** modes restored individual files via byte-range GET; the extracted bytes matched the manifest MD5 and were **byte-identical (`cmp`) to the originals**. Failure paths behaved correctly too — a wrong `--verify-md5`, a missing `source_key`, and specifying both modes at once each exited non-zero with a clear message.

---

## 6. Running several archivers at once

Running one archiver per source directory, all writing into the **same** manifest table, is supported.

### Is the manifest still consistent?

Yes. Amazon S3 Tables explicitly supports concurrent writers to a single table ([S3 FAQs](https://aws.amazon.com/s3/faqs/)), and this workload is the case Iceberg handles best:

1. **Commits are atomic.** A worker writes its Parquet data files first, then the catalog swaps the table's metadata pointer conditionally on the base snapshot. Exactly one writer wins a given race; the loser gets a commit conflict and nothing is half-applied. Readers only ever see a complete snapshot.
2. **Append-only writes never logically conflict.** This solution only appends — no deletes, no overwrite, no MERGE. A losing commit just re-reads the latest metadata and retries; its already-written data files stay valid. (Parallel `DELETE`/`UPDATE` on the same files is the case that produces hard commit errors — not applicable here.)
3. **Retries are jittered.** `--commit-retries` (default 8) uses full-jitter exponential backoff, so two conflicting writers don't back off by the same amount and collide again.

### What you must get right

**Give each run a distinct tar namespace.** Tar keys are `<prefix>/<YYYYMMDD>/archive_<run-id>_<NNNNN>.tar`, and `--run-id` defaults to `<timestamp>_<pid>_<rand>` — unique per process, so the default is already safe even for runs launched in the same second from a shell loop. Only override `--run-id` if you need a reproducible name, and never reuse one value for two live runs sharing a `--dst-prefix`: they would overwrite each other's tars, leaving manifest rows pointing at a tar that no longer holds those files.

**Keep `--src-path` the same across the runs, and split the work with `--input-file`.** `source_key` is stored *relative to that run's `--src-path`*, so giving each run a different subtree as its `--src-path` makes the relative paths collide: three runs over `fs1/`, `fs2/`, `fs3/` all record `f000.dat`, and there is no longer anything in the row that says which subtree it came from. Measured on a 210-file, 4-run test: 210 rows but only **60 distinct `source_key`** — reconciliation undercounts by 150, and a LOOKUP restore for `f000.dat` is ambiguous between subtrees. Point every run at the common parent and hand each one a list instead:

```bash
# safe: one shared --src-path, so source_key stays globally unique;
# each run gets its own run_id automatically.
for sub in fs1 fs2 fs3; do
  find /data/nfsshare/$sub -type f > /tmp/list.$sub
  python3 v3/fss3-archiver-s3tables-mp.py \
      --src-path /data/nfsshare --input-file /tmp/list.$sub ... &
done
wait
```

The trade-off is that `--input-file` enumerates up front instead of streaming, so each `find` must finish before its run starts packing — on a slow mount that serial prefix can cost more than the concurrency wins. If you would rather keep the streaming walk, use per-subtree `--src-path` but treat the manifest as per-subtree too: keep the runs in **separate tables** (or separate `--namespace`s), because within one table their `source_key`s are no longer distinguishable.

**Pre-create the namespace and table once** before launching, so the runs don't race on creation:

```bash
python3 v3/fss3-archiver-s3tables-mp.py --src-path /empty/dir --max-files 1 ...  # creates ns + table
```

**Watch the commit rate.** Each run commits every `--batch-size` rows or `--flush-interval` seconds, whichever comes first. With many runs on one table, raise `--flush-interval` to cut the commit rate — and with it both the conflict probability and the snapshot churn. The run summary reports `N commits (M commit retries)`; `M` is your conflict gauge, and `manifest-writer: append failed` warnings in the log are the same signal live.

**Reconcile with `DISTINCT`.** If an append succeeds server-side but the response is lost, the retry re-appends the same rows. Those duplicates are harmless for restore (identical `tar_key` and byte range) but they inflate row counts, so verify with `COUNT(DISTINCT source_key)` rather than `COUNT(*)`. This only holds if `source_key` is globally unique — i.e. the runs share one `--src-path`, per the point above.

### Is more concurrency the right fix for slow throughput?

Often not. Concurrency only helps if the bottleneck is something each run gets its own copy of. If it is per-run software — one writer thread, one process's worth of packing — more runs help. If it is the **source device**, they don't: N runs share one IOPS budget, so they split the same ceiling N ways.

That is worth checking before you build a fan-out. In four cold 1M-file runs on a 200 GB gp2 volume (600 baseline / 3,000 burst IOPS, ~20 KB average file) throughput sat at 57.3–58.5 MB/s no matter what `--num-workers` or `--scan-threads` was set to; 3,000 IOPS × 20 KB ≈ 62 MB/s, i.e. the run was already at the device ceiling. The same tree warm was 222.90 MB/s. When the numbers look like that, the lever is the volume (gp3 with provisioned IOPS, io2, instance-store NVMe) — not more archivers, and not more scan threads.

If the source really is latency-bound (a network mount, not a local disk), raising `--scan-threads` within a single run addresses that bottleneck without N concurrent writers — see [`--scan-threads`](#--scan-threads). Measure the mount's ceiling first.

---

## Limitations and notes

- **With `--compress` (gzip), byte-range individual restore does not work.** Offsets are recorded as `-1` when compressing. Disable compression (plain tar) if you need individual-file restore.
- **For millisecond individual-file restore, keep tars in `STANDARD` (the default), `GLACIER_IR`, or an IA class.** Glacier Flexible Retrieval / Deep Archive require restoring the whole tar first (hours).
- **If you need Object Lock (WORM), configure versioning + Object Lock on the tar destination bucket separately.** (S3 Tables table buckets do not support Object Lock.)
- **Throughput depends on network bandwidth.** For on-premises → S3 transfers, bandwidth is likely the bottleneck.
- **For large runs, keep `--batch-size` large** (default 50,000). Small values create many small files in the manifest and can slow queries.
- **Tar upload and manifest commit are not atomic.** If the process crashes after a tar uploads but before its rows commit to S3 Tables, that tar is an orphan — archived but not findable/restorable. To detect orphans after a run, reconcile the tar objects under the destination prefix against the distinct `tar_key` values in the manifest.
- Tested with Python 3.12.1 / pyiceberg 0.11.1.
