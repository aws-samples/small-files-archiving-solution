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
| `--tar-storageclass` | storage class for tar objects | GLACIER_IR |
| `--spool-max-mb` | in-memory threshold for a tar (spills to disk above it) | 256 |
| `--compress` | gzip the tar (⚠️ see limitations) | off |

Storage layout: tars go to `s3://<bucket>/<prefix>/<YYYYMMDD>/archive_*.tar`; the manifest is written to the S3 Tables table.

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

```python
import boto3, io, tarfile
from pyiceberg.catalog import load_catalog

cat = load_catalog("s3tables", **{
    "type": "rest",
    "warehouse": "arn:aws:s3tables:ap-northeast-2:<account>:bucket/<table-bucket>",
    "uri": "https://s3tables.ap-northeast-2.amazonaws.com/iceberg",
    "rest.sigv4-enabled": "true", "rest.signing-name": "s3tables",
    "rest.signing-region": "ap-northeast-2"})
df = cat.load_table("archive.manifest").scan().to_pandas()
row = df[df.source_key.str.contains("file0499")].iloc[0]

s3 = boto3.client("s3")
resp = s3.get_object(Bucket="your-own-dest-bucket", Key=row.tar_key,
                     Range=f"bytes={int(row.byte_start)}-{int(row.byte_end)}")
tf = tarfile.open(fileobj=io.BytesIO(resp["Body"].read()))
data = tf.extractfile(tf.getnames()[0]).read()   # the individual file's bytes
```

This fetches only the byte range holding the file, without downloading the whole tar. **However, if the tar is in Glacier Flexible Retrieval / Deep Archive, you must first restore the entire tar (hours) before this works.** If you need millisecond, no-wait retrieval, keep tars in `GLACIER_IR` (the default).

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

---

## Limitations and notes

- **With `--compress` (gzip), byte-range individual restore does not work.** Offsets are recorded as `-1` when compressing. Disable compression (plain tar) if you need individual-file restore.
- **For millisecond individual-file restore, keep tars in `GLACIER_IR` (the default).** Glacier Flexible Retrieval / Deep Archive require restoring the whole tar first (hours).
- **If you need Object Lock (WORM), configure versioning + Object Lock on the tar destination bucket separately.** (S3 Tables table buckets do not support Object Lock.)
- **Throughput depends on network bandwidth.** For on-premises → S3 transfers, bandwidth is likely the bottleneck.
- **For large runs, keep `--batch-size` large** (default 50,000). Small values create many small files in the manifest and can slow queries.
- Tested with Python 3.12.1 / pyiceberg 0.11.1.
