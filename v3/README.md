# Small Files Archiver → S3 + S3 Tables Manifest

> 🌐 **한국어**: [README-ko.md](README-ko.md) · This is the English document.

An archiving solution that **bundles** large numbers of small filesystem files into tar objects on Amazon S3, and records each file's location (which tar, which byte range) in an **Amazon S3 Tables (managed Apache Iceberg)** table.

Based on [aws-samples/small-files-archiving-solution](https://github.com/aws-samples/small-files-archiving-solution), this version moves the manifest from CSV objects to S3 Tables and reworks the internals with single-pass MD5, streaming tar, batched commits, and multiprocess parallelism.

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
File scanner
   └─ splits the file list into batches by size (--max-size) or count (--max-files)
        └─ ProcessPoolExecutor (N worker processes)
              │  each worker: build tar (optional MD5) + S3 multipart upload
              │  → returns manifest rows
              └─ single writer thread → batched commit to S3 Tables (default 50,000 rows)
```

Script: `fss3-archiver-s3tables-mp.py` (multiprocess). CPU-bound work such as tar packing and MD5 runs in parallel across worker processes, bypassing the GIL.

Design points:

| Item | Approach | Why |
|---|---|---|
| MD5 | computed once while streaming into tar (optional) | avoids reading each file twice |
| tar buffer | `SpooledTemporaryFile` (spills to disk past a threshold) | prevents large tars from staying resident in RAM |
| byte offset | tar stream `tell()` position | real physical position including tar headers/padding → correct byte-range restore |
| manifest write | a **single** writer commits in batches | avoids Iceberg optimistic-concurrency (OCC) conflicts and small-file / snapshot explosion |
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

```bash
python fss3-archiver-s3tables-mp.py \
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

### Key parameters

| Parameter | Description | Default |
|---|---|---|
| `--src-path` | source directory to archive (recursed) | (required) |
| `--dst-bucket` / `--dst-prefix` | S3 bucket / prefix for tars | (required) |
| `--max-size` **or** `--max-files` | per-tar size (e.g. `512MB`) or file count (pick one) | (required) |
| `--num-workers` | tar-build/upload worker processes | `os.cpu_count()` |
| `--md5` / `--no-md5` | compute per-file MD5 (off = md5 column null, slightly faster) | off |
| `--region` | AWS region | (required) |
| `--table-bucket-arn` | S3 Tables table bucket ARN | (required) |
| `--namespace` / `--table-name` | manifest namespace / table | archive / manifest |
| `--batch-size` | rows per manifest commit | 50000 |
| `--tar-storageclass` | storage class for tar objects (see below) | GLACIER_IR |
| `--spool-max-mb` | in-memory threshold for a tar (spills to disk above it) | 256 |
| `--compress` | gzip the tar (⚠️ see limitations) | off |

Storage layout: tars go to `s3://<bucket>/<prefix>/<YYYYMMDD>/archive_*.tar`; the manifest is written to the S3 Tables table.

#### `--tar-storageclass` values

The value is passed straight through to S3 `PutObject`, so any S3 storage class is accepted. For this solution the one that matters is **whether individual-file restore (§4.2) works immediately or needs a whole-tar restore first**:

| Class | Individual restore (§4.2) | Notes |
|---|---|---|
| `GLACIER_IR` | ✅ immediate | **default.** Glacier Instant Retrieval — archive pricing, millisecond ranged GET. Best fit for cold archives you still occasionally read. |
| `STANDARD` | ✅ immediate | most expensive to store; use only for hot data. |
| `STANDARD_IA` / `ONEZONE_IA` | ✅ immediate | infrequent access; per-GB retrieval fee. `ONEZONE_IA` is single-AZ (cheaper, lower durability). |
| `INTELLIGENT_TIERING` | ✅ immediate* | auto-tiers by access. *Ranged GET is immediate unless an object has moved to the optional Archive Access tiers. |
| `GLACIER` (Flexible Retrieval) | ⚠️ restore first | cheaper storage, but §4.2 fails until you restore the whole tar (minutes–hours). |
| `DEEP_ARCHIVE` | ⚠️ restore first | cheapest storage; whole-tar restore takes up to ~12 h. |

> **Rule of thumb:** if you ever need to pull single files back without a wait, keep tars in `GLACIER_IR` (the default). Only move to `GLACIER` / `DEEP_ARCHIVE` when the data is truly write-once and you accept a restore delay for the whole tar. Classes like `REDUCED_REDUNDANCY`, `EXPRESS_ONEZONE`, `OUTPOSTS`, `SNOW`, and `FSX_*` exist in the API but are not meaningful for this archiving workflow.

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
| `md5` | string | MD5 of the original file (with `--md5`), else null |
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

Use `fss3-restore.py`. It fetches only the byte range holding the file — the archiver records `byte_start` at the file's tar header and `byte_end` at the end of its padding, so that range is a self-contained single-member tar that `tarfile` opens directly. No full-tar download.

**Note:** the manifest stores `tar_key` but **not** the bucket, so `--tar-bucket` is always required.

**DIRECT mode** — you already have the offsets (e.g. from the 4.1 query):

```bash
python fss3-restore.py \
  --tar-bucket       your-own-dest-bucket \
  --tar-key          20260804/20260804/archive_20260804_120000_00001.tar \
  --byte-start       1536 \
  --byte-end         21503 \
  --out              ./restored/file0499.dat
```

**LOOKUP mode** — you only know the original path; the script queries the manifest (exact `source_key` match with Iceberg predicate pushdown — no full-table scan) and resolves the offsets itself:

```bash
python fss3-restore.py \
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

**Storage-class caveats:** this works with `GLACIER_IR` (the default) with no wait. **If the tar is in Glacier Flexible Retrieval / Deep Archive, you must first restore the entire tar (hours) before a ranged GET succeeds** — the script detects this and errors clearly. Files archived with `--compress` have offsets of `-1` and cannot be range-restored; extract the whole `.tar.gz` instead.

---

## 5. Performance and cost (measured in the test environment)

Test environment: EC2 (Amazon Linux, 8 vCPU, ap-northeast-2), source = NFS `/data/nfsshare` (avg ~19 KB/file), Python 3.12.1.

### 5.1 Throughput

1M files (fs1+fs2, 19.49 GB), 8 workers:

| MD5 | Elapsed | Rate | Failures |
|---|---|---|---|
| off | 82.5s | **241.8 MB/s** | 0 |
| on | 89.0s | 224.3 MB/s | 0 |

### 5.2 MD5 on/off comparison (same conditions)

Same data (1M files, 19.49 GB), 8 workers, multiprocess, consecutive runs (warm file cache), **MD5 is the only variable**:

| | MD5 off | MD5 on | Delta |
|---|---|---|---|
| Elapsed | 82.5s | 89.0s | **+7.9%** |
| Rate | 241.8 MB/s | 224.3 MB/s | -7.3% |

**MD5 overhead is about 8%.** Single-pass hashing (computed while reading for the tar anyway) adds no extra read, and this environment is transfer/I/O dominated, so the MD5 CPU cost is relatively small. It is cheap relative to the integrity-check benefit, so **MD5 on is recommended** when restore-time integrity matters.

### 5.3 Object consolidation and request reduction

For the 1M-file run:

| Approach | Object PUTs | Consolidation |
|---|---|---|
| Individual upload | 1,006,108 | — |
| **Bundling (this solution)** | **1,000** (979 tars + 21 manifest commits) | **~1,028 : 1** |

→ About **1,000× fewer** PUT requests. Since each tar is 20 MB, it also avoids the GLACIER_IR 128 KB minimum-billing penalty entirely.

### 5.4 Cost (approximate)

At this scale (~20 GB source, 979 tars, 1M manifest rows) the monthly storage cost — GLACIER_IR tars plus the S3 Tables manifest — is **very low** (order of tens of cents/month). Keeping the manifest in S3 Tables does not inflate cost, because **batched commits (50,000 rows/commit)** avoid small-file / snapshot explosion, so it does not offset the bundling savings.

> Compute exact figures from region/period-specific rates (the above is approximate).

### 5.5 Integrity verification

Files were sampled from the first, middle, and last tars and restored from the actual tar via byte-range GET using the offset in the S3 Tables manifest. **Filename, size, and (when enabled) MD5 all matched.** The correctness of single-pass MD5 and physical tar offsets was confirmed against real S3.

`fss3-restore.py` itself was also verified end-to-end against real S3 + S3 Tables (a 523,123-row manifest, MD5 on). Both **DIRECT** and **LOOKUP** modes restored individual files via byte-range GET; the extracted bytes matched the manifest MD5 and were **byte-identical (`cmp`) to the originals**. Failure paths behaved correctly too — a wrong `--verify-md5`, a missing `source_key`, and specifying both modes at once each exited non-zero with a clear message.

---

## Limitations and notes

- **With `--compress` (gzip), byte-range individual restore does not work.** Offsets are recorded as `-1` when compressing. Disable compression (plain tar) if you need individual-file restore.
- **For millisecond individual-file restore, keep tars in `GLACIER_IR` (the default).** Glacier Flexible Retrieval / Deep Archive require restoring the whole tar first (hours).
- **If you need Object Lock (WORM), configure versioning + Object Lock on the tar destination bucket separately.** (S3 Tables table buckets do not support Object Lock.)
- **Throughput depends on network bandwidth.** For on-premises → S3 transfers, bandwidth is likely the bottleneck.
- **For large runs, keep `--batch-size` large** (default 50,000). Small values create many small files in the manifest and can slow queries.
- Tested with Python 3.12.1 / pyiceberg 0.11.1.
