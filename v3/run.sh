#!/bin/bash
# ---------------------------------------------------------------------------
# run.sh — example invocations for the S3 Tables archiver / restore (v3).
#
# Edit the shared variables below, then uncomment ONE function call at the
# bottom and run:  bash run.sh
#
# Each function is a self-contained scenario. They all share the same
# destination bucket + S3 Tables coordinates; only the source and batching
# flags differ. See README.md §3 for the narrative version.
# ---------------------------------------------------------------------------
set -euo pipefail

ARCHIVER="python3 fss3-archiver-s3tables-mp.py"
RESTORE="python3 fss3-restore.py"

# --- shared config (edit these) --------------------------------------------
REGION="ap-northeast-2"
DST_BUCKET="your-own-dest-bucket"
DST_PREFIX="20260804"
TABLE_BUCKET_ARN="arn:aws:s3tables:ap-northeast-2:<account>:bucket/<table-bucket>"
NAMESPACE="archive"
TABLE_NAME="manifest"
SRC_PATH="/data/nfsshare"
INPUT_FILE="files-to-archive.txt"     # used by fstos3_input
STORAGECLASS="STANDARD"               # STANDARD (default; no min duration / no
                                      # retrieval fee) | GLACIER_IR (real cold
                                      # archive: cheap + still ms ranged GET, but
                                      # 90-day min) | STANDARD_IA | DEEP_ARCHIVE
SCAN_THREADS=1                        # default: serial walk. Raise ONLY for a
                                      # latency-bound (network) source -- on a local
                                      # disk the extra threads just contend. Measure
                                      # with scan_benchmark first (see README).

# ===========================================================================
# ARCHIVE scenarios
# ===========================================================================

# Baseline: bundle a directory into uniform-size (20MB) tars. The common case.
# MD5 is on by default (integrity); manifest committed in 50k-row batches.
function fstos3_size () {
$ARCHIVER \
    --src-path         "$SRC_PATH" \
    --dst-bucket       "$DST_BUCKET" \
    --dst-prefix       "$DST_PREFIX" \
    --max-size         20MB \
    --num-workers      8 \
    --scan-threads     "$SCAN_THREADS" \
    --region           "$REGION" \
    --table-bucket-arn "$TABLE_BUCKET_ARN" \
    --namespace        "$NAMESPACE" \
    --table-name       "$TABLE_NAME" \
    --batch-size       50000 \
    --tar-storageclass "$STORAGECLASS"
}

# Fixed file count per tar (cap each tar at N files instead of a size).
function fstos3_count () {
$ARCHIVER \
    --src-path         "$SRC_PATH" \
    --dst-bucket       "$DST_BUCKET" \
    --dst-prefix       "$DST_PREFIX" \
    --max-files        1000 \
    --num-workers      8 \
    --region           "$REGION" \
    --table-bucket-arn "$TABLE_BUCKET_ARN" \
    --namespace        "$NAMESPACE" \
    --table-name       "$TABLE_NAME" \
    --tar-storageclass "$STORAGECLASS"
}

# Archive an explicit list of files (DB export / find result / change-list)
# instead of walking the tree. --src-path is still the base for relative paths.
function fstos3_input () {
$ARCHIVER \
    --src-path         "$SRC_PATH" \
    --input-file       "$INPUT_FILE" \
    --dst-bucket       "$DST_BUCKET" \
    --dst-prefix       "$DST_PREFIX" \
    --max-files        1000 \
    --num-workers      8 \
    --region           "$REGION" \
    --table-bucket-arn "$TABLE_BUCKET_ARN" \
    --namespace        "$NAMESPACE" \
    --table-name       "$TABLE_NAME" \
    --tar-storageclass "$STORAGECLASS"
}

# Cold, write-once data: gzip-compress the tars into Deep Archive.
# NOTE: --compress disables byte-range individual restore (offsets = -1);
# DEEP_ARCHIVE needs a whole-tar restore before any read.
function fstos3_compress () {
$ARCHIVER \
    --src-path         "$SRC_PATH" \
    --dst-bucket       "$DST_BUCKET" \
    --dst-prefix       "$DST_PREFIX" \
    --max-size         50MB \
    --num-workers      8 \
    --compress \
    --region           "$REGION" \
    --table-bucket-arn "$TABLE_BUCKET_ARN" \
    --namespace        "$NAMESPACE" \
    --table-name       "$TABLE_NAME" \
    --tar-storageclass DEEP_ARCHIVE
}

# Large tars: bigger objects + more headroom before spilling the in-memory
# tar buffer to local disk (--spool-max-mb). Fewer workers keeps peak RAM
# (~num-workers x tar-size) in check.
function fstos3_large_tar () {
$ARCHIVER \
    --src-path         "$SRC_PATH" \
    --dst-bucket       "$DST_BUCKET" \
    --dst-prefix       "$DST_PREFIX" \
    --max-size         512MB \
    --spool-max-mb     512 \
    --num-workers      4 \
    --region           "$REGION" \
    --table-bucket-arn "$TABLE_BUCKET_ARN" \
    --namespace        "$NAMESPACE" \
    --table-name       "$TABLE_NAME" \
    --tar-storageclass "$STORAGECLASS"
}

# Cold / latency-bound source: the walk, not the packing, is the bottleneck.
# Enumerating small files costs one lstat round-trip each, so a single-threaded
# scan caps out around 500-2000 files/s on a network mount. Measure your ceiling
# first with scan_benchmark below, then set --scan-threads near it.
function fstos3_cold_cache () {
$ARCHIVER \
    --src-path         "$SRC_PATH" \
    --dst-bucket       "$DST_BUCKET" \
    --dst-prefix       "$DST_PREFIX" \
    --max-size         20MB \
    --num-workers      8 \
    --scan-threads     32 \
    --region           "$REGION" \
    --table-bucket-arn "$TABLE_BUCKET_ARN" \
    --namespace        "$NAMESPACE" \
    --table-name       "$TABLE_NAME" \
    --tar-storageclass "$STORAGECLASS"
}

# Several runs in parallel, one per subtree, all writing to ONE manifest table.
# Safe: Iceberg appends commit atomically and append-only writes never logically
# conflict, so a losing commit just retries. Each run also gets its own --run-id
# by default, so their tar keys can't collide even sharing a --dst-prefix.
# Raising --flush-interval cuts the commit rate (and so the conflict rate) when
# many runs share one table. See README.md §6.
#
# NOTE the shape here: every run passes the SAME --src-path and is restricted to
# its subtree with --input-file. source_key is stored relative to --src-path, so
# giving each run its own subtree as --src-path would make the names collide
# (fs1/f000.dat and fs2/f000.dat both become "f000.dat"), breaking both
# COUNT(DISTINCT source_key) reconciliation and LOOKUP restore. The cost of doing
# it this way is that each find must finish before its run starts packing.
function fstos3_parallel_runs () {
  # Create the namespace + table ONCE first so the runs don't race on creation.
  local seed; seed="$(mktemp -d)"
  $ARCHIVER \
      --src-path         "$seed" \
      --dst-bucket       "$DST_BUCKET" \
      --dst-prefix       "$DST_PREFIX" \
      --max-files        1 \
      --region           "$REGION" \
      --table-bucket-arn "$TABLE_BUCKET_ARN" \
      --namespace        "$NAMESPACE" \
      --table-name       "$TABLE_NAME" \
      --tar-storageclass "$STORAGECLASS"
  rmdir "$seed"

  local listdir; listdir="$(mktemp -d)"
  for sub in fs1 fs2 fs3 fs4; do
    find "$SRC_PATH/$sub" -type f > "$listdir/$sub.txt"
    $ARCHIVER \
        --src-path         "$SRC_PATH" \
        --input-file       "$listdir/$sub.txt" \
        --dst-bucket       "$DST_BUCKET" \
        --dst-prefix       "$DST_PREFIX" \
        --max-size         20MB \
        --num-workers      4 \
        --scan-threads     "$SCAN_THREADS" \
        --flush-interval   60 \
        --region           "$REGION" \
        --table-bucket-arn "$TABLE_BUCKET_ARN" \
        --namespace        "$NAMESPACE" \
        --table-name       "$TABLE_NAME" \
        --tar-storageclass "$STORAGECLASS" &
  done
  wait
  rm -rf "$listdir"
  echo "All runs finished. Check each log for 'commit retries' (the conflict gauge),"
  echo "and reconcile with COUNT(DISTINCT source_key), not COUNT(*)."
}

# ===========================================================================
# Diagnostics
# ===========================================================================

# How fast can this mount be enumerated at all? Run BEFORE tuning
# --scan-threads: if the serial and parallel numbers match, the filesystem is
# the wall and more scan threads won't help.
# NOTE: drops the page cache (needs root) so the numbers reflect a cold read.
function scan_benchmark () {
  echo "--- serial walk ---"
  sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
  time find "$SRC_PATH" -type f -printf ''

  echo "--- parallel walk over top-level subdirectories (-P 8) ---"
  sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
  time ls -d "$SRC_PATH"/*/ | xargs -P 8 -I{} find {} -type f -printf ''
}

# ===========================================================================
# RESTORE scenarios (single-file, byte-range GET — no whole-tar download)
# ===========================================================================

# LOOKUP: you only know the original path. The script queries the manifest
# for its tar_key + byte range, then verifies against the manifest MD5.
function restore_lookup () {
$RESTORE \
    --region           "$REGION" \
    --table-bucket-arn "$TABLE_BUCKET_ARN" \
    --namespace        "$NAMESPACE" \
    --table-name       "$TABLE_NAME" \
    --source-key       sub/file0499.dat \
    --tar-bucket       "$DST_BUCKET" \
    --out-dir          ./restored/ \
    --verify-md5
}

# DIRECT: you already have the offsets (e.g. from an Athena/PyIceberg query).
# No manifest lookup — fastest path.
function restore_direct () {
$RESTORE \
    --tar-bucket       "$DST_BUCKET" \
    --tar-key          20260804/20260804/archive_20260804_120000_00001.tar \
    --byte-start       1536 \
    --byte-end         21503 \
    --out              ./restored/file0499.dat
}

# ===========================================================================
# Uncomment exactly ONE to run:
# ===========================================================================
fstos3_size            # bundle a directory into 20MB tars (baseline)
#fstos3_count          # N files per tar
#fstos3_input          # archive an explicit --input-file list
#fstos3_compress       # gzip -> Deep Archive (write-once)
#fstos3_large_tar      # 512MB tars, tuned spool/workers
#fstos3_cold_cache     # latency-bound source: many scan threads
#fstos3_parallel_runs  # several runs at once into one manifest table
#scan_benchmark        # measure the mount's enumeration ceiling first
#restore_lookup        # restore one file by its original path
#restore_direct        # restore one file by known offsets
