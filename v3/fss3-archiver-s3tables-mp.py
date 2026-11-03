"""
fss3-archiver-s3tables-mp.py
============================
Multiprocess variant of the filesystem -> S3 archiver with an Amazon S3 Tables
(managed Apache Iceberg) manifest.

What changed vs the threaded version:
  * tar build + upload run in a PROCESS pool (ProcessPoolExecutor) instead of a
    thread pool -> CPU-bound work (tar packing, MD5) runs in parallel across
    cores, bypassing the Python GIL.
  * MD5 is computed by default (--md5, on) for integrity verification; single-pass
    hashing adds no extra read. --no-md5 disables it (md5 column null) if needed.
  * Worker count defaults to os.cpu_count() (tunable via --num-workers).

Flow:
  main process
    K scanner threads (parallel walk) --> batch queue
      --> main thread submits --> ProcessPoolExecutor (N worker processes)
                                    each worker: build tar (+ MD5) + S3 upload
                                    returns manifest rows (small) back to main
    main thread collects finished tars --> manifest queue
      --> single writer thread --> S3 Tables batch commit

Design notes for multiprocessing correctness:
  * Each worker process creates its OWN boto3 S3 client (via pool initializer);
    boto3 clients are not fork/pickle safe to share.
  * tar filename is derived from a batch index assigned in the main process, so no
    cross-process sequence counter/lock is needed.
  * Only small manifest rows cross the process boundary (never tar bytes).
  * The single manifest writer stays in the main process (I/O bound) so commits
    remain serialized -> no Iceberg OCC conflicts, no small-file/snapshot explosion.

Why scanning is threaded (--scan-threads):
  On a network filesystem, enumerating small files is latency-bound: every entry
  costs an lstat round-trip, so a single-threaded walk caps out around 500-2000
  files/s no matter how many worker processes are packing tars. The walk runs on
  K threads that each pull a directory off a shared queue -- getdents/lstat
  release the GIL, so these are genuinely concurrent. Scanners feed a bounded
  batch queue, so the walk keeps running while the main thread waits on tar
  uploads instead of alternating with them.

  K defaults to 1 because that latency is what the threads exist to hide: on a
  local disk there is none to hide, and the extra threads only contend. Raise
  --scan-threads for a remote/high-latency mount, after measuring.

Concurrent runs against one manifest table are safe: Iceberg appends commit
atomically through the S3 Tables catalog and append-only writes never logically
conflict, so a losing commit just retries. Each run must get its own tar
namespace though -- see --run-id.

Durability caveat: a tar upload and its manifest commit are not atomic. A crash
after a tar uploads but before its rows commit leaves an orphan tar with no
manifest entry (archived but not findable/restorable). To detect this, reconcile
the tar objects under the destination prefix against distinct tar_key values in
the manifest after a run.

Requires: pip install "pyiceberg[pyarrow]" boto3   (Python 3.9+)
"""
import os
import sys
import socket
import csv
import gzip
import posixpath
import json
import stat
import time
import uuid
import queue
import random
import hashlib
import logging
import argparse
import tarfile
import threading
import tempfile
import multiprocessing
from datetime import datetime, timezone
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, TableAlreadyExistsError
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, LongType, TimestamptzType
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform


# --------------------------------------------------------------------------- #
# Size parsing
# --------------------------------------------------------------------------- #
def parse_size(size_str: str) -> int:
    size_str = size_str.strip().upper()
    units = {'B': 1, 'KB': 1024, 'K': 1024, 'MB': 1024 ** 2, 'M': 1024 ** 2,
             'GB': 1024 ** 3, 'G': 1024 ** 3, 'TB': 1024 ** 4, 'T': 1024 ** 4}
    if size_str.isdigit():
        return int(size_str)
    number, unit = '', ''
    for ch in size_str:
        if ch.isdigit() or ch == '.':
            number += ch
        else:
            unit += ch
    unit = unit.strip()
    if not number or unit not in units:
        raise argparse.ArgumentTypeError(
            f"Invalid size: {size_str}. Examples: 100MB, 2GB, 1TB")
    return int(float(number) * units[unit])


# --------------------------------------------------------------------------- #
# Single-pass MD5 reader (used when MD5 is enabled, which is the default)
# --------------------------------------------------------------------------- #
class HashingReader:
    def __init__(self, fileobj):
        self._f = fileobj
        self._md5 = hashlib.md5()

    def read(self, size=-1):
        chunk = self._f.read(size)
        if chunk:
            self._md5.update(chunk)
        return chunk

    @property
    def hexdigest(self) -> str:
        return self._md5.hexdigest()


# --------------------------------------------------------------------------- #
# Worker process: build one tar + upload it, return manifest rows.
# _WORKER holds per-process boto3 state created by the pool initializer.
# --------------------------------------------------------------------------- #
_WORKER = {}


def _init_worker(profile, region, storageclass, mp_threshold, mp_chunksize, upload_conc,
                 progress_q=None):
    import boto3
    from boto3.s3.transfer import TransferConfig
    _WORKER["progress_q"] = progress_q
    session = boto3.Session(profile_name=profile)
    _WORKER["s3"] = session.client("s3", region_name=region)
    _WORKER["storageclass"] = storageclass
    _WORKER["tconf"] = TransferConfig(
        multipart_threshold=mp_threshold,
        multipart_chunksize=mp_chunksize,
        max_concurrency=upload_conc,
        use_threads=True,
    )


class _ProgressPing:
    """Per-tar progress events from a worker process to the main process
    (--progress only). Events are throttled so a 10k-file tar doesn't flood the
    queue; put_nowait drops an event rather than ever stalling the worker."""
    INTERVAL = 0.1

    def __init__(self, tar_key, nfiles, nbytes):
        self._q = _WORKER.get("progress_q")
        self._tar = tar_key
        self._nfiles = nfiles
        self._nbytes = nbytes
        self._last = 0.0
        self._last_done = 0
        self._last_phase = None
        self._lock = threading.Lock()   # upload callbacks arrive on boto3 threads
        self.sent = 0

    def emit(self, phase, done, total, force=False, **extra):
        if self._q is None:
            return
        now = time.monotonic()
        # time-throttled, but every 10% step also gets through, so a tar that
        # bundles faster than INTERVAL still shows its bar moving
        step = total and phase == self._last_phase and \
            done - self._last_done >= total * 0.1
        if not force and not step and now - self._last < self.INTERVAL:
            return
        self._last, self._last_done, self._last_phase = now, done, phase
        ev = {"tar": self._tar, "phase": phase, "done": done, "total": total,
              "nfiles": self._nfiles, "nbytes": self._nbytes}
        ev.update(extra)
        try:
            self._q.put_nowait(ev)
        except Exception:
            pass

    def upload_start(self, tar_size):
        with self._lock:
            self.sent = 0
        self.emit("uploading", 0, tar_size, force=True)

    def upload_cb(self, tar_size):
        def cb(n):
            with self._lock:
                self.sent += n
                sent = self.sent
            self.emit("uploading", sent, tar_size)
        return cb


def _process_batch(task):
    """Runs in a worker process. task keys:
       files [(full, rel, size)...], tar_key, bucket, compress, do_md5, spool_max.
       Raises on upload failure so the main process attributes the whole batch as
       failed (nfiles). Skips the upload entirely if no member packed."""
    s3 = _WORKER["s3"]
    tconf = _WORKER["tconf"]
    sclass = _WORKER["storageclass"]

    archive_ts = datetime.now(timezone.utc)
    mode = "w:gz" if task["compress"] else "w:"
    offsets_valid = not task["compress"]
    spool = tempfile.SpooledTemporaryFile(max_size=task["spool_max"], mode="w+b")
    rows, failed, failed_files = [], 0, []
    nbytes_in = sum(f[2] for f in task["files"])
    ping = _ProgressPing(task["tar_key"], len(task["files"]), nbytes_in)
    packed_bytes = 0
    ping.emit("bundling", 0, nbytes_in, force=True)

    try:
        with tarfile.open(fileobj=spool, mode=mode) as tar:
            for full, rel, size in task["files"]:
                packed_bytes += size
                ping.emit("bundling", packed_bytes, nbytes_in,
                          files=len(rows) + failed + 1)
                try:
                    ti = tar.gettarinfo(full, arcname=rel)
                    start_pos = spool.tell()
                    with open(full, "rb") as f:
                        if task["do_md5"]:
                            reader = HashingReader(f)
                            tar.addfile(ti, reader)
                            md5 = reader.hexdigest
                        else:
                            tar.addfile(ti, f)
                            md5 = None
                    end_pos = spool.tell() - 1
                    rows.append({
                        "tar_key": task["tar_key"],
                        "source_key": rel,
                        "byte_start": start_pos if offsets_valid else -1,
                        "byte_end": end_pos if offsets_valid else -1,
                        "md5": md5,
                        "filesize": size,
                        "archive_ts": archive_ts,
                        "parent_dir": posixpath.dirname(rel),
                    })
                except Exception as e:
                    failed += 1
                    failed_files.append((full, f"{type(e).__name__}: {e}"))

        # Nothing packed (every file in the batch failed) -> don't upload an
        # empty tar; just report the per-file failures.
        if not rows:
            return {"tar_key": task["tar_key"], "rows": [], "failed_files": failed_files,
                    "failed": failed, "total_bytes": 0, "count": 0}

        # upload tar with retry. Keep the FIRST error: boto3 closes the fileobj
        # on a failed transfer, so a later spool.seek(0) would raise a generic
        # "I/O operation on closed file" that masks the real cause (e.g.
        # NoSuchBucket). Once the spool is closed there's nothing to retry from,
        # so stop and surface the original error rather than sleeping in vain.
        tar_size = spool.tell()
        # always report the finished bundle: on a fast disk a whole tar packs
        # inside one throttle interval and would otherwise never show progress
        ping.emit("bundling", packed_bytes, nbytes_in, force=True,
                  files=len(rows) + failed)
        first_err = None
        for attempt in range(5):
            if spool.closed:
                break
            try:
                spool.seek(0)
                ping.upload_start(tar_size)
                s3.upload_fileobj(
                    spool, task["bucket"], task["tar_key"].lstrip("/"),
                    ExtraArgs={"StorageClass": sclass}, Config=tconf,
                    Callback=ping.upload_cb(tar_size) if ping._q is not None else None)
                first_err = None
                break
            except Exception as e:
                if first_err is None:
                    first_err = e
                if spool.closed or attempt == 4:
                    break
                time.sleep(2 ** attempt)
        if first_err is not None:
            ping.emit("failed", 0, tar_size, force=True)
            raise first_err
        ping.emit("uploaded", tar_size, tar_size, force=True)
    finally:
        spool.close()

    total = sum(r["filesize"] for r in rows)
    return {"tar_key": task["tar_key"], "rows": rows, "failed_files": failed_files,
            "failed": failed, "total_bytes": total, "count": len(rows)}


# --------------------------------------------------------------------------- #
# Manifest writer: single thread, batched Iceberg append into S3 Tables.
# --------------------------------------------------------------------------- #
MANIFEST_ARROW_SCHEMA = pa.schema([
    pa.field("tar_key",    pa.string(),                  nullable=False),
    pa.field("source_key", pa.string(),                  nullable=False),
    pa.field("byte_start", pa.int64(),                   nullable=False),
    pa.field("byte_end",   pa.int64(),                   nullable=False),
    pa.field("md5",        pa.string(),                  nullable=True),
    pa.field("filesize",   pa.int64(),                   nullable=False),
    pa.field("archive_ts", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("parent_dir", pa.string(),                  nullable=True),
])

ICEBERG_SCHEMA = Schema(
    NestedField(1, "tar_key",    StringType(),      required=True),
    NestedField(2, "source_key", StringType(),      required=True),
    NestedField(3, "byte_start", LongType(),        required=True),
    NestedField(4, "byte_end",   LongType(),        required=True),
    NestedField(5, "md5",        StringType(),      required=False),
    NestedField(6, "filesize",   LongType(),        required=True),
    NestedField(7, "archive_ts", TimestamptzType(), required=True),
    # Directory of source_key ('' at the root). Lets a browser list one folder
    # with an equality filter instead of a prefix scan over every descendant.
    # Optional so tables created before it existed evolve by adding it.
    NestedField(8, "parent_dir", StringType(),      required=False),
)

# Directory index (<table>_dirs): per-commit RECURSIVE file/byte totals for
# every directory touched, plus all its ancestors. A browser lists folder X
# with parent_dir == X and sums rows per dir_path. Row count is per commit x
# directories touched -- orders of magnitude below the file count -- so it stays
# cheap to query even when the manifest holds billions of rows. It is a
# browsing aid, committed after (not atomically with) the manifest batch.
DIRS_ARROW_SCHEMA = pa.schema([
    pa.field("parent_dir", pa.string(),                  nullable=False),
    pa.field("dir_path",   pa.string(),                  nullable=False),
    pa.field("files",      pa.int64(),                   nullable=False),
    pa.field("bytes",      pa.int64(),                   nullable=False),
    pa.field("archive_ts", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("run_id",     pa.string(),                  nullable=True),
])

DIRS_ICEBERG_SCHEMA = Schema(
    NestedField(1, "parent_dir", StringType(),      required=True),
    NestedField(2, "dir_path",   StringType(),      required=True),
    NestedField(3, "files",      LongType(),        required=True),
    NestedField(4, "bytes",      LongType(),        required=True),
    NestedField(5, "archive_ts", TimestamptzType(), required=True),
    NestedField(6, "run_id",     StringType(),      required=False),
)


def dir_index_rows(rows, run_id):
    """Aggregate manifest rows into recursive per-directory totals.

    Counts by direct parent first, then propagates to ancestors once per unique
    parent, so the cost is per directory rather than per file x depth."""
    direct = {}
    for r in rows:
        d = r["parent_dir"]
        f, b = direct.get(d, (0, 0))
        direct[d] = (f + 1, b + r["filesize"])
    total = {}
    for d, (f, b) in direct.items():
        while d:
            tf, tb = total.get(d, (0, 0))
            total[d] = (tf + f, tb + b)
            d = posixpath.dirname(d)
    now = datetime.now(timezone.utc)
    return [{"parent_dir": posixpath.dirname(d), "dir_path": d, "files": f,
             "bytes": b, "archive_ts": now, "run_id": run_id}
            for d, (f, b) in sorted(total.items())]


class ManifestWriter(threading.Thread):
    POISON = None

    def __init__(self, catalog_props, namespace, table_name, row_queue, logger,
                 batch_size=50_000, flush_interval=15.0, commit_retries=8,
                 retry_cap=30.0, dir_index=True, run_id=None):
        super().__init__(name="manifest-writer", daemon=False)
        self._props = catalog_props
        self._ns = namespace
        self._tbl_name = table_name
        self._q = row_queue
        self._log = logger
        self._batch = batch_size
        self._flush = flush_interval
        self._retries = commit_retries
        self._retry_cap = retry_cap
        self._dir_index = dir_index
        self._run_id = run_id
        self._dirs_tbl = None
        self._has_parent_col = False
        self.rows_committed = 0
        self.commit_count = 0
        self.conflict_count = 0
        self.dir_rows_committed = 0
        self.dir_index_failures = 0
        self._error = None

    def _ensure_table(self):
        """Create the namespace + table if missing, then load the table.

        Both creates are idempotent by design: concurrent runs race here, and the
        loser must carry on. But only the already-exists outcome is benign, so it
        is the only one treated as success. Anything else (a denied
        s3tables:CreateNamespace, a wrong table-bucket ARN, an endpoint the
        credentials can't reach) is logged with its real type and message, and if
        the subsequent load_table then fails, that original error -- not the
        downstream 'no_such_table' it causes -- is what the run aborts with."""
        catalog = load_catalog("s3tables", **self._props)
        ident = f"{self._ns}.{self._tbl_name}"
        setup_err = None

        try:
            catalog.create_namespace(self._ns)
            self._log.info(f"Created namespace {self._ns}")
        except NamespaceAlreadyExistsError:
            self._log.info(f"Namespace {self._ns} already exists, reusing")
        except Exception as e:
            setup_err = e
            self._log.warning(f"create_namespace({self._ns}) failed, trying to "
                              f"use the table anyway -- {type(e).__name__}: {e}")

        try:
            catalog.create_table(
                identifier=ident, schema=ICEBERG_SCHEMA,
                partition_spec=PartitionSpec(
                    PartitionField(source_id=7, field_id=1000,
                                   transform=DayTransform(), name="archive_ts_day")))
            self._log.info(f"Created table {ident}")
        except TableAlreadyExistsError:
            self._log.info(f"Table {ident} already exists, reusing")
        except Exception as e:
            setup_err = setup_err or e
            self._log.warning(f"create_table({ident}) failed, trying to load it "
                              f"anyway -- {type(e).__name__}: {e}")

        try:
            tbl = catalog.load_table(ident)
        except Exception as e:
            if setup_err is not None:
                raise RuntimeError(
                    f"cannot use manifest table {ident}: setup failed earlier with "
                    f"{type(setup_err).__name__}: {setup_err}") from setup_err
            raise

        if self._dir_index:
            self._ensure_parent_col(tbl, ident)
            self._dirs_tbl = self._ensure_dirs_table(catalog)
        self._has_parent_col = "parent_dir" in tbl.schema().column_names
        return tbl

    def _ensure_parent_col(self, tbl, ident):
        """Additive schema evolution for tables created before parent_dir.

        Concurrent runs may race to add it; a loser just re-checks."""
        for _ in range(3):
            tbl.refresh()
            if "parent_dir" in tbl.schema().column_names:
                return
            try:
                with tbl.update_schema() as upd:
                    upd.add_column("parent_dir", StringType(),
                                   doc="directory of source_key ('' = root)")
                self._log.info(f"Added optional column parent_dir to {ident}")
                return
            except Exception as e:
                self._log.warning(f"adding parent_dir to {ident} failed, "
                                  f"re-checking -- {type(e).__name__}: {e}")
                time.sleep(1 + random.random())
        tbl.refresh()
        if "parent_dir" not in tbl.schema().column_names:
            self._log.warning(f"{ident} has no parent_dir column; rows are written "
                              f"without it (GUI falls back to prefix scans)")

    def _ensure_dirs_table(self, catalog):
        ident = f"{self._ns}.{self._tbl_name}_dirs"
        try:
            catalog.create_table(identifier=ident, schema=DIRS_ICEBERG_SCHEMA)
            self._log.info(f"Created directory index table {ident}")
        except TableAlreadyExistsError:
            pass
        except Exception as e:
            self._log.warning(f"create_table({ident}) failed, trying to load it "
                              f"anyway -- {type(e).__name__}: {e}")
        try:
            return catalog.load_table(ident)
        except Exception as e:
            self._log.error(f"directory index disabled for this run: cannot load "
                            f"{ident} -- {type(e).__name__}: {e}")
            return None

    def _commit(self, tbl, pending):
        """Append one batch, retrying on commit conflict.

        Iceberg appends are optimistic: the data files are written first, then the
        catalog swaps the metadata pointer conditionally on the base snapshot. When
        another writer wins that race we get a commit conflict; re-reading the
        latest metadata and retrying is always safe here because append-only
        writes never logically conflict (no deletes, no overwrite).

        Caveat: if an append succeeds server-side but the response is lost, the
        retry re-appends the same rows. Those duplicates are harmless for restore
        (identical tar_key and byte range), but reconciliation queries should
        count DISTINCT source_key rather than raw rows."""
        # Sorted so each data file covers a narrow source_key / parent_dir range:
        # Iceberg min/max stats then prune folder listings to a few files.
        pending.sort(key=lambda r: r["source_key"])
        arrow = pa.Table.from_pylist(pending, schema=MANIFEST_ARROW_SCHEMA)
        if not self._has_parent_col:
            arrow = arrow.drop_columns(["parent_dir"])
        self._append_retry(tbl, arrow, "manifest")
        self.rows_committed += len(pending)
        self.commit_count += 1
        self._log.info(
            f"manifest-writer: committed {len(pending):,} rows "
            f"(total {self.rows_committed:,}, commits {self.commit_count})")
        if self._dirs_tbl is not None:
            drows = dir_index_rows(pending, self._run_id)
            if drows:
                try:
                    self._append_retry(
                        self._dirs_tbl,
                        pa.Table.from_pylist(drows, schema=DIRS_ARROW_SCHEMA),
                        "dir-index")
                    self.dir_rows_committed += len(drows)
                except Exception as e:
                    # The manifest (the source of truth) is committed; a missing
                    # dir-index batch only under-counts folder totals in a browser.
                    self.dir_index_failures += 1
                    self._log.error(f"manifest-writer: dir-index commit failed "
                                    f"({len(drows)} rows) -- {type(e).__name__}: {e}")

    def _append_retry(self, tbl, arrow, what):
        for attempt in range(self._retries):
            try:
                tbl.refresh()
                tbl.append(arrow)
                return
            except Exception as e:
                if attempt == self._retries - 1:
                    raise
                self.conflict_count += 1
                # Full-jitter exponential backoff. The jitter is the point: two
                # writers that conflict and then back off by the same fixed
                # amount just collide again in lockstep.
                delay = min(self._retry_cap, 0.5 * (2 ** attempt)) * (0.5 + random.random())
                self._log.warning(
                    f"manifest-writer: {what} append failed "
                    f"(attempt {attempt + 1}/{self._retries}), retrying in "
                    f"{delay:.1f}s -- {type(e).__name__}: {e}")
                time.sleep(delay)

    def run(self):
        try:
            tbl = self._ensure_table()
            pending, last = [], time.monotonic()
            while True:
                try:
                    item = self._q.get(timeout=1.0)
                except queue.Empty:
                    item = "TICK"
                if item is self.POISON:
                    self._q.task_done()
                    break
                if item != "TICK":
                    pending.append(item)
                    self._q.task_done()
                if pending and (len(pending) >= self._batch
                                or time.monotonic() - last >= self._flush):
                    self._commit(tbl, pending)
                    pending, last = [], time.monotonic()
            if pending:
                self._commit(tbl, pending)
        except Exception as e:
            self._error = e
            self._log.exception(f"manifest-writer FATAL: {e}")

    @property
    def error(self):
        return self._error


# --------------------------------------------------------------------------- #
# Parallel scanner support
# --------------------------------------------------------------------------- #
# Sentinel that stops a scanner thread / marks the end of the batch stream.
# A dedicated object (not None) so a legitimately empty batch can't be mistaken
# for end-of-stream.
_SCAN_DONE = object()


class _ScanAborted(Exception):
    """Raised inside a scanner thread when the run is aborting, so it stops
    waiting on a batch queue nobody is draining any more."""


class _BatchEmitter:
    """Accumulates files into one batch and pushes closed batches downstream.

    Each scanner thread owns its own emitter, so batching needs no shared lock.
    The cost is up to (scan_threads - 1) extra partial tars at the end of a run,
    which is harmless -- batch_no, and therefore the tar name, is still assigned
    single-threaded by the main process when it dequeues.
    """

    def __init__(self, out_queue, max_files, max_size, abort, on_flush=None):
        self._on_flush = on_flush
        self._out = out_queue
        self._max_files = max_files
        self._max_size = max_size
        self._abort = abort
        self._batch = []
        self._bytes = 0
        self.unchanged = 0      # --incremental skips, reported with each flush

    def add(self, item):
        self._batch.append(item)
        self._bytes += item[2]
        if (self._max_files and len(self._batch) >= self._max_files) or \
           (self._max_size and self._bytes >= self._max_size):
            self.flush()

    def flush(self):
        if not self._batch:
            if self.unchanged and self._on_flush is not None:
                self._on_flush(0, 0, self.unchanged)
                self.unchanged = 0
            return
        batch, nbytes = self._batch, self._bytes
        self._batch, self._bytes = [], 0
        if self._on_flush is not None:
            self._on_flush(len(batch), nbytes, self.unchanged)
            self.unchanged = 0
        # Bounded put with a timeout rather than a blocking one: on abort the
        # main thread stops draining, and a plain put() would hang this thread
        # forever (and with it the watcher that joins it).
        while True:
            if self._abort.is_set():
                raise _ScanAborted()
            try:
                self._out.put(batch, timeout=0.5)
                return
            except queue.Full:
                continue


# --------------------------------------------------------------------------- #
# Run history (<table-name>_runs): one row per run. Drives --incremental: the
# watermark is the start time of the last successful run of the same source.
# --------------------------------------------------------------------------- #
RUNS_ARROW_SCHEMA = pa.schema([
    pa.field("run_id",       pa.string(),                  nullable=False),
    pa.field("source_id",    pa.string(),                  nullable=False),
    pa.field("src_path",     pa.string(),                  nullable=True),
    pa.field("walk_paths",   pa.string(),                  nullable=True),
    pa.field("input_file",   pa.string(),                  nullable=True),
    pa.field("mode",         pa.string(),                  nullable=False),
    pa.field("status",       pa.string(),                  nullable=False),
    pa.field("start_ts",     pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("end_ts",       pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("watermark_ts", pa.timestamp("us", tz="UTC"), nullable=True),
    pa.field("files",        pa.int64(),                   nullable=False),
    pa.field("bytes",        pa.int64(),                   nullable=False),
    pa.field("failed",       pa.int64(),                   nullable=False),
    pa.field("skipped",      pa.int64(),                   nullable=False),
    pa.field("unchanged",    pa.int64(),                   nullable=False),
    pa.field("tars",         pa.int64(),                   nullable=False),
    pa.field("dst_prefix",   pa.string(),                  nullable=True),
    pa.field("failed_list",  pa.string(),                  nullable=True),
])

RUNS_ICEBERG_SCHEMA = Schema(
    NestedField(1,  "run_id",       StringType(),      required=True),
    NestedField(2,  "source_id",    StringType(),      required=True),
    NestedField(3,  "src_path",     StringType(),      required=False),
    NestedField(4,  "walk_paths",   StringType(),      required=False),
    NestedField(5,  "input_file",   StringType(),      required=False),
    NestedField(6,  "mode",         StringType(),      required=True),
    NestedField(7,  "status",       StringType(),      required=True),
    NestedField(8,  "start_ts",     TimestamptzType(), required=True),
    NestedField(9,  "end_ts",       TimestamptzType(), required=True),
    NestedField(10, "watermark_ts", TimestamptzType(), required=False),
    NestedField(11, "files",        LongType(),        required=True),
    NestedField(12, "bytes",        LongType(),        required=True),
    NestedField(13, "failed",       LongType(),        required=True),
    NestedField(14, "skipped",      LongType(),        required=True),
    NestedField(15, "unchanged",    LongType(),        required=True),
    NestedField(16, "tars",         LongType(),        required=True),
    NestedField(17, "dst_prefix",   StringType(),      required=False),
    NestedField(18, "failed_list",  StringType(),      required=False),
)


def source_id_for(src_path, walk_paths=None, input_file=None):
    """Stable id of 'what is being archived': the base path plus the walked
    subpaths (or the list file). Incremental runs only compare against runs of
    the same source. Shared with the GUI, which shows the last run per source."""
    key = {"src": os.path.abspath(src_path).rstrip(os.sep),
           "walk": sorted(os.path.abspath(w) for w in (walk_paths or []))}
    if input_file:
        key["input"] = os.path.abspath(input_file)
    return hashlib.sha1(json.dumps(key, sort_keys=True).encode()).hexdigest()[:16]


class RunHistory:
    """Run records in <table-name>_runs. Uses its own catalog handle (the
    manifest writer thread owns the other)."""

    def __init__(self, catalog_props, namespace, table_name, logger, retries=8):
        self._log = logger
        self._retries = retries
        catalog = load_catalog("s3tables", **catalog_props)
        ident = f"{namespace}.{table_name}_runs"
        try:
            catalog.create_namespace(namespace)
        except NamespaceAlreadyExistsError:
            pass
        except Exception as e:
            logger.warning(f"create_namespace({namespace}) for run history failed, "
                           f"trying the table anyway -- {type(e).__name__}: {e}")
        try:
            catalog.create_table(identifier=ident, schema=RUNS_ICEBERG_SCHEMA)
            logger.info(f"Created run history table {ident}")
        except TableAlreadyExistsError:
            pass
        except Exception as e:
            logger.warning(f"create_table({ident}) failed, trying to load it anyway "
                           f"-- {type(e).__name__}: {e}")
        self.table = catalog.load_table(ident)

    def last_success(self, source_id):
        """Most recent run of this source that completed (status ok/partial)."""
        from pyiceberg.expressions import And, EqualTo, In
        rows = self.table.scan(
            row_filter=And(EqualTo("source_id", source_id),
                           In("status", ["ok", "partial"])),
            selected_fields=("run_id", "start_ts", "files", "status")).to_arrow().to_pylist()
        return max(rows, key=lambda r: r["start_ts"]) if rows else None

    def record(self, row):
        arrow = pa.Table.from_pylist([row], schema=RUNS_ARROW_SCHEMA)
        for attempt in range(self._retries):
            try:
                self.table.refresh()
                self.table.append(arrow)
                return
            except Exception as e:
                if attempt == self._retries - 1:
                    raise
                delay = min(30.0, 0.5 * (2 ** attempt)) * (0.5 + random.random())
                self._log.warning(f"run history append failed (attempt {attempt + 1}), "
                                  f"retrying in {delay:.1f}s -- {type(e).__name__}: {e}")
                time.sleep(delay)


class FailedList:
    """Files that could not be archived, streamed to disk as they happen.

    failed_<run_id>.txt  -- one absolute path per line: feed it straight back
                            with --input-file (same --src-path) to retry
    failed_<run_id>.tsv  -- path <TAB> reason, for humans
    Streaming, not buffered, so a run with millions of failures stays flat in
    memory. Skipped entries (symlinks, special files) are not failures and are
    not listed: retrying them would only skip them again."""

    def __init__(self, log_dir, run_id):
        self.txt = os.path.abspath(os.path.join(log_dir, f"failed_{run_id}.txt"))
        self.tsv = os.path.abspath(os.path.join(log_dir, f"failed_{run_id}.tsv"))
        self._lock = threading.Lock()
        self._ft = self._fv = None
        self.count = 0

    def add(self, path, reason, retryable=True):
        reason = " ".join(str(reason).split())
        with self._lock:
            if self._fv is None:
                self._ft = open(self.txt, "w", encoding="utf-8")
                self._fv = open(self.tsv, "w", encoding="utf-8")
                self._fv.write("path\treason\n")
            if retryable:
                self._ft.write(path + "\n")
            self._fv.write(f"{path}\t{reason}\n")
            self.count += 1

    def close(self):
        with self._lock:
            for f in (self._ft, self._fv):
                if f is not None:
                    f.close()

    @property
    def written(self):
        return self._fv is not None


# --------------------------------------------------------------------------- #
# Machine-readable progress (--progress), consumed by the GUI
# --------------------------------------------------------------------------- #
PROGRESS_TAG = "@@PROGRESS "


class ProgressReporter(threading.Thread):
    """Folds worker events into a per-tar table and prints one JSON snapshot per
    interval to stdout, prefixed with PROGRESS_TAG. Logs go to stderr, so stdout
    carries only these lines. A snapshot is O(active tars), never O(files), so
    the cost is flat whether the run is 1 GB or 1 PB."""

    def __init__(self, archiver, event_q, interval=0.5):
        super().__init__(name="progress", daemon=True)
        self._a = archiver
        self._q = event_q
        self._interval = interval
        self._active = {}
        self._lock = threading.Lock()   # _active is touched by main + this thread
        self._halt = threading.Event()
        self.writer = None
        self.scan_done = False

    def finish(self, ok, error=None):
        self._halt.set()
        self.join(timeout=5)
        self._emit(final=True, ok=ok, error=error)

    def tar_finished(self, tar_key):
        with self._lock:
            self._active.pop(tar_key, None)

    def _drain(self):
        while True:
            try:
                ev = self._q.get_nowait()
            except Exception:
                return
            with self._lock:
                # only tars still in flight: drops late events for a tar that
                # was already collected (the set is bounded by the window size)
                if ev["tar"] in self._a._inflight_tars:
                    self._active[ev["tar"]] = ev

    def _emit(self, final=False, **extra):
        a, w = self._a, self.writer
        with a._stats_lock:
            snap = {
                "ev": "done" if final else "progress",
                "run_id": a.run_id, "elapsed": round(time.time() - a._t0, 1),
                "scan_done": self.scan_done,
                "scanned_files": a._scanned_files, "scanned_bytes": a._scanned_bytes,
                "dirs_scanned": a._dirs_scanned,
                "files_done": a._files, "bytes_done": a._bytes, "tars_done": a._tars,
                "failed": a._failed, "skipped": a._skipped,
                "unchanged": a._unchanged,
            }
        snap["rows_committed"] = w.rows_committed if w else 0
        with self._lock:
            evs = list(self._active.values())
        # Finished in the worker but not yet collected by the main thread:
        # report their bytes as complete, but don't list them as active.
        active = [e for e in evs if e["phase"] in ("bundling", "uploading")]
        snap["finished_uncollected_bytes"] = sum(
            e["nbytes"] for e in evs if e["phase"] == "uploaded")
        snap["active"] = active[:64]
        snap["bundling"] = sum(1 for e in active if e["phase"] == "bundling")
        snap["uploading"] = sum(1 for e in active if e["phase"] == "uploading")
        # bytes in flight: partially bundled / uploaded tars, for smooth bars
        snap["bundling_bytes"] = sum(e["done"] for e in active if e["phase"] == "bundling")
        snap["uploaded_inflight"] = sum(e["done"] for e in active if e["phase"] == "uploading")
        snap["failed_list"] = a.failed_list.txt if a.failed_list.written else None
        snap["watermark"] = a.watermark_iso
        snap.update(extra)
        try:
            sys.stdout.write(PROGRESS_TAG + json.dumps(snap, default=str) + "\n")
            sys.stdout.flush()
        except Exception:
            pass

    def run(self):
        while not self._halt.wait(self._interval):
            self._drain()
            self._emit()


# --------------------------------------------------------------------------- #
# Archiver (main process orchestration)
# --------------------------------------------------------------------------- #
class FS2S3ArchiverMP:
    def __init__(self, args):
        self.args = args
        self.src = args.src_path
        self.dst_bucket = args.dst_bucket
        self.dst_prefix = args.dst_prefix.rstrip("/")
        self.max_files = args.max_files
        self.max_size = args.max_size
        self.input_file = args.input_file
        self.walk_paths = [os.path.abspath(w) for w in (args.walk_path or [])]
        if self.walk_paths and self.input_file:
            raise ValueError("--walk-path and --input-file are mutually exclusive")
        self.incremental = getattr(args, "incremental", False)
        if self.incremental and self.input_file:
            raise ValueError("--incremental applies to directory walks, not --input-file "
                             "(a list already says exactly what to archive)")
        self.input_format = getattr(args, "input_format", "auto")
        self.path_column = getattr(args, "path_column", None)
        self.watermark = None        # epoch seconds; files older than this are unchanged
        self.watermark_iso = None
        self.source_id = source_id_for(self.src, self.walk_paths, self.input_file)
        self.num_workers = args.num_workers
        self.compress = args.compress
        self.do_md5 = args.md5
        self.spool_max = args.spool_max_mb * 1024 * 1024
        self.batch_size = args.batch_size
        self.scan_threads = args.scan_threads
        self.current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.mid_prefix = self.current_time.split("_")[0]

        # Per-run token that makes tar keys unique across concurrent runs.
        # current_time alone is NOT enough: it has second granularity and is
        # stamped at process start, so runs launched together from a shell loop
        # share it and would generate identical tar keys -- silently overwriting
        # each other on S3 and leaving manifest rows pointing at a tar that no
        # longer holds those files. pid distinguishes local siblings; the random
        # suffix covers runs on different hosts (where pids can coincide).
        self.run_id = args.run_id or (
            f"{self.current_time}_{os.getpid()}_{uuid.uuid4().hex[:4]}")

        # exactly one of the two batching rules, and it must be positive
        # (argparse's mutually-exclusive group already blocks passing both)
        if (self.max_files is None) == (self.max_size is None):
            raise ValueError("Specify exactly one of --max-files or --max-size")
        if self.max_files is not None and self.max_files <= 0:
            raise ValueError("--max-files must be a positive integer")
        if self.max_size is not None and self.max_size <= 0:
            raise ValueError("--max-size must be positive")
        if self.scan_threads <= 0:
            raise ValueError("--scan-threads must be a positive integer")

        os.makedirs(os.path.join("logs", self.dst_prefix), exist_ok=True)
        self.logger = logging.getLogger("fss3-archiver-mp")
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
            # run_id (not current_time) so concurrent runs sharing a --dst-prefix
            # don't write into the same log file.
            fh = logging.FileHandler(
                os.path.join("logs", self.dst_prefix, f"archiver_{self.run_id}.log"))
            ch = logging.StreamHandler()
            for h in (fh, ch):
                h.setFormatter(fmt)
                self.logger.addHandler(h)

        self.catalog_props = {
            "type": "rest",
            "warehouse": args.table_bucket_arn,
            "uri": f"https://s3tables.{args.region}.amazonaws.com/iceberg",
            "rest.sigv4-enabled": "true",
            "rest.signing-name": "s3tables",
            "rest.signing-region": args.region,
        }
        self.manifest_queue = queue.Queue(maxsize=self.batch_size * 4)
        # Bounded so the scanners run ahead of the tar workers without buffering
        # the whole tree in memory; this bound is the scan/pack backpressure.
        self.batch_queue = queue.Queue(maxsize=max(4, self.num_workers * 2))
        # Set when the run is failing, so scanner threads stop instead of
        # blocking on a batch queue nobody drains any more.
        self._abort = threading.Event()
        self._stats_lock = threading.Lock()
        self._files = self._failed = self._tars = self._bytes = 0
        self._skipped = self._unchanged = 0
        self.failed_list = FailedList(os.path.join("logs", self.dst_prefix), self.run_id)
        self._dirs_scanned = 0
        self._scanned_files = self._scanned_bytes = 0
        self._inflight_tars = set()
        self._t0 = time.time()
        self.progress = None

    def _on_scan_flush(self, nfiles, nbytes, unchanged=0):
        with self._stats_lock:
            self._scanned_files += nfiles
            self._scanned_bytes += nbytes
            self._unchanged += unchanged

    def _fail(self, path, reason, retryable=True):
        self.failed_list.add(path, reason, retryable)
        self._update(failed=1)

    def _update(self, files=0, failed=0, tars=0, nbytes=0, dirs=0, skipped=0):
        with self._stats_lock:
            self._skipped += skipped
            self._files += files
            self._failed += failed
            self._tars += tars
            self._bytes += nbytes
            self._dirs_scanned += dirs

    @staticmethod
    def _human(n):
        size = float(n)
        for u in ['B', 'KB', 'MB', 'GB', 'TB', 'PB']:
            if size < 1024 or u == 'PB':
                return f"{size:.2f}{u}"
            size /= 1024

    def _check_regular(self, st_mode, path):
        """True if an already-obtained lstat mode describes a regular file.

        Symlinks are skipped rather than archived: tarfile would store a 0-byte
        symlink member while the manifest recorded the *target's* size, producing
        an empty file that fails byte-range restore/MD5. Non-regular files (fifo,
        device, socket) are skipped too. Every skip is logged and counted as
        skipped -- not failed: retrying one would just skip it again, so skips
        stay out of the failed-file list. Directories are the caller's business
        and must be filtered before calling this."""
        if stat.S_ISLNK(st_mode):
            self.logger.warning(f"symlink skipped (not archived): {path}")
            self._update(skipped=1)
            return False
        if not stat.S_ISREG(st_mode):
            self.logger.warning(f"not a regular file, skipping: {path}")
            self._update(skipped=1)
            return False
        return True

    def _regular_size(self, path):
        """Return the size of `path` if it is a regular file, else None.

        Uses a single lstat (no symlink follow). Used for --input-file, where
        there is no directory entry to reuse."""
        try:
            st = os.lstat(path)
        except OSError as e:
            self.logger.warning(f"stat failed, skipping {path}: {e}")
            self._fail(path, f"stat: {e}")
            return None
        if not self._check_regular(st.st_mode, path):
            return None
        return st.st_size

    def _rel_to_src(self, path):
        """Path relative to --src-path when `path` lies under it (matching the
        directory-walk layout), else the path with any leading separators
        stripped. The prefix test is component-aware — a plain str.startswith
        would turn src=/data/nfs, /data/nfsshare/x into 'share/x'."""
        src = self.src.rstrip(os.sep) if self.src else ""
        if src and (path == src or path.startswith(src + os.sep)):
            return os.path.relpath(path, src)
        return path.lstrip(os.sep)

    def _scan_one_dir(self, full_dir, rel_dir, dir_queue, emitter):
        """List one directory: enqueue subdirectories, emit regular files.

        Exactly one lstat per entry. We classify from st_mode ourselves instead
        of using entry.is_dir()/is_file(), because those consult the dirent
        d_type first and NFS commonly reports DT_UNKNOWN -- which makes them fall
        back to their own stat call, on top of the one we need for st_size. On a
        latency-bound mount that doubles the round-trips per file.

        Directory recursion goes through the shared queue rather than a recursive
        call, so sibling subtrees spread across all scanner threads instead of
        one thread owning a deep branch."""
        wm = self.watermark
        with os.scandir(full_dir) as it:
            for entry in it:
                try:
                    st = entry.stat(follow_symlinks=False)
                except OSError as e:
                    self.logger.warning(f"stat failed, skipping {entry.path}: {e}")
                    self._fail(entry.path, f"stat: {e}")
                    continue
                rel = os.path.join(rel_dir, entry.name) if rel_dir else entry.name
                if stat.S_ISDIR(st.st_mode):
                    dir_queue.put((entry.path, rel))
                elif stat.S_ISREG(st.st_mode):
                    # --incremental: ctime catches renames/moves and metadata
                    # changes that leave mtime old; mtime catches content writes
                    if wm is not None and max(st.st_mtime, st.st_ctime) < wm:
                        emitter.unchanged += 1
                        continue
                    emitter.add((entry.path, rel, st.st_size))
                else:
                    # symlink (including symlinked dirs, which we don't follow --
                    # matching os.walk's followlinks=False) or special file
                    self._check_regular(st.st_mode, entry.path)
        self._update(dirs=1)

    def _walk_worker(self, dir_queue, emitter):
        """Scanner thread: pull directories off the queue until poisoned.

        Never lets an exception kill the thread -- if it died mid-directory the
        queue's unfinished-task count would never reach zero and the watcher's
        join() would hang forever."""
        try:
            while True:
                item = dir_queue.get()
                if item is _SCAN_DONE:
                    dir_queue.task_done()
                    break
                try:
                    if len(item) == 3:      # a --walk-path that is a single file
                        sz = self._regular_size(item[0])
                        if sz is not None:
                            st = os.lstat(item[0])
                            if self.watermark is not None and \
                                    max(st.st_mtime, st.st_ctime) < self.watermark:
                                emitter.unchanged += 1
                            else:
                                emitter.add((item[0], item[1], sz))
                        continue
                    self._scan_one_dir(item[0], item[1], dir_queue, emitter)
                except _ScanAborted:
                    raise  # let the outer handler end the thread; finally still runs
                except Exception as e:
                    self.logger.error(f"scan failed for directory {item[0]}: {e}")
                    # a directory can't be retried through --input-file; it goes
                    # to the .tsv only, for a human to look at
                    self._fail(item[0] + os.sep, f"directory scan: {e}", retryable=False)
                finally:
                    # exactly once per dequeued item, including the abort path --
                    # a double task_done() would drive unfinished_tasks negative
                    # and let the watcher's join() return before the walk is done
                    dir_queue.task_done()
            emitter.flush()
        except _ScanAborted:
            return

    def _list_worker(self, path_queue, emitter):
        """Scanner thread for --input-file: stat paths pulled off the queue.

        The lstat per path is the same latency-bound cost as walking, so this is
        threaded for the same reason."""
        try:
            while True:
                path = path_queue.get()
                if path is _SCAN_DONE:
                    path_queue.task_done()
                    break
                try:
                    sz = self._regular_size(path)
                    if sz is not None:
                        emitter.add((path, self._rel_to_src(path), sz))
                except _ScanAborted:
                    raise  # let the outer handler end the thread; finally still runs
                except Exception as e:
                    self.logger.error(f"scan failed for {path}: {e}")
                finally:
                    path_queue.task_done()  # exactly once per dequeued item
            emitter.flush()
        except _ScanAborted:
            return

    def _iter_input_paths(self):
        """Yield absolute paths from --input-file, streaming.

        Formats (--input-format, 'auto' picks by extension; .gz is transparent):
          lines  one path per line; blank lines and '#' comments skipped
          csv    / tsv: a file-list report (storage-system export, DB dump,
                 find -printf ...). --path-column picks the column by header name
                 or 0-based index (default 0); a header row is detected when
                 --path-column is a name.
        Relative paths are taken relative to --src-path."""
        name = self.input_file
        opener = gzip.open if name.endswith(".gz") else open
        base = name[:-3] if name.endswith(".gz") else name
        fmt = self.input_format
        if fmt == "auto":
            fmt = ("csv" if base.endswith(".csv") else
                   "tsv" if base.endswith((".tsv", ".tab")) else "lines")
        with opener(name, "rt", encoding="utf-8", newline="") as f:
            if fmt == "lines":
                for line in f:
                    path = line.strip()
                    if path and not path.startswith("#"):
                        yield self._abs_input(path)
                return
            reader = csv.reader(f, delimiter="\t" if fmt == "tsv" else ",")
            col = self.path_column if self.path_column is not None else "0"
            if col.isdigit():
                idx = int(col)
            else:
                header = next(reader, None) or []
                norm = [h.strip().lower() for h in header]
                if col.strip().lower() not in norm:
                    raise ValueError(f"--path-column {col!r} not in report header {header}")
                idx = norm.index(col.strip().lower())
            for row in reader:
                if len(row) <= idx:
                    continue
                path = row[idx].strip()
                if path and not path.startswith("#"):
                    yield self._abs_input(path)

    def _abs_input(self, path):
        return path if os.path.isabs(path) else os.path.join(self.src, path)

    def _feed_input_file(self, path_queue):
        """Read --input-file and hand paths to the scanner threads.

        Bounded put keeps a huge list file from being slurped into memory."""
        for path in self._iter_input_paths():
            while True:
                if self._abort.is_set():
                    return
                try:
                    path_queue.put(path, timeout=0.5)
                    break
                except queue.Full:
                    continue

    def _start_scanners(self):
        """Start the scanner threads and return the thread that ends the stream.

        Everything is daemon: on an abort the main thread raises and we must not
        keep the interpreter alive waiting for scanners.
        """
        if self.input_file:
            work_queue = queue.Queue(maxsize=10_000)
            worker = self._list_worker
            feeder = threading.Thread(target=self._feed_input_file,
                                      args=(work_queue,),
                                      name="scan-feeder", daemon=True)
            feeder.start()
        else:
            # UNBOUNDED on purpose: scanner threads both consume from and produce
            # into this queue. With a bound, every thread could block in put() on
            # a full queue with nobody left to drain it -- a self-deadlock. The
            # directory count is orders of magnitude below the file count, so the
            # memory is not a concern.
            work_queue = queue.Queue()
            worker = self._walk_worker
            feeder = None
            if not self.walk_paths:
                work_queue.put((self.src, ""))
            for wp in self.walk_paths:
                rel = self._rel_to_src(wp)
                if os.path.isdir(wp) and not os.path.islink(wp):
                    work_queue.put((wp, "" if rel == "." else rel))
                else:
                    work_queue.put((wp, rel, "file"))

        scanners = []
        for i in range(self.scan_threads):
            emitter = _BatchEmitter(self.batch_queue, self.max_files,
                                    self.max_size, self._abort,
                                    on_flush=self._on_scan_flush)
            t = threading.Thread(target=worker, args=(work_queue, emitter),
                                 name=f"scanner-{i}", daemon=True)
            t.start()
            scanners.append(t)

        def watch():
            """Close the batch stream once the tree is fully enumerated."""
            try:
                if feeder is not None:
                    # Join the feeder BEFORE the queue: an empty work queue means
                    # "nothing queued yet", not "done", so joining the queue first
                    # could return immediately on a slow first read.
                    feeder.join()
                work_queue.join()
                for _ in scanners:
                    work_queue.put(_SCAN_DONE)
                for t in scanners:
                    t.join()
            finally:
                # Always close the stream, even if a scanner blew up, so the main
                # thread's dequeue loop can't hang.
                self.batch_queue.put(_SCAN_DONE)

        watcher = threading.Thread(target=watch, name="scan-watcher", daemon=True)
        watcher.start()
        return watcher

    def start_processing(self):
        """Run history + incremental watermark around the archive pipeline.

        The watermark is the START time of the last ok/partial run of the same
        source (minus --watermark-skew, for clock differences between this host
        and a file server that stamps mtime/ctime). Starting from the previous
        run's start, not its end, means a file written while that run was
        walking is picked up again rather than missed. A failed or cancelled run
        never advances the watermark; a partial run does, and its failed files
        are in the failed list for a retry."""
        start_dt = datetime.now(timezone.utc)
        history = None
        if self.args.run_history:
            try:
                history = RunHistory(self.catalog_props, self.args.namespace,
                                     self.args.table_name, self.logger)
            except Exception as e:
                if self.incremental:
                    raise RuntimeError(f"--incremental needs the run history table: "
                                       f"{type(e).__name__}: {e}") from e
                self.logger.warning(f"run history unavailable, this run won't be "
                                    f"recorded -- {type(e).__name__}: {e}")
        if self.incremental:
            last = history.last_success(self.source_id)
            if last is None:
                self.logger.info(f"incremental: no previous successful run of source "
                                 f"{self.source_id}; archiving everything")
            else:
                wm = last["start_ts"]
                self.watermark = wm.timestamp() - self.args.watermark_skew
                self.watermark_iso = wm.isoformat()
                self.logger.info(
                    f"incremental: source {self.source_id}, last successful run "
                    f"{last['run_id']} started {wm.isoformat()}; archiving files with "
                    f"mtime/ctime after it (minus {self.args.watermark_skew:.0f}s skew)")
        status = "failed"
        try:
            self._run_pipeline()
            # partial (some files failed) still advances the incremental
            # watermark -- its failures are in the failed list; a run that
            # archived nothing but had failures does not
            status = ("ok" if self._failed == 0 else
                      "partial" if self._files > 0 else "failed")
        except KeyboardInterrupt:
            status = "cancelled"
            raise
        finally:
            self.failed_list.close()
            if self.failed_list.written:
                self.logger.info(f"Failed-file list: {self.failed_list.txt} "
                                 f"(retry: --input-file it with the same --src-path); "
                                 f"reasons in {self.failed_list.tsv}")
            if history is not None:
                try:
                    history.record({
                        "run_id": self.run_id, "source_id": self.source_id,
                        "src_path": os.path.abspath(self.src),
                        "walk_paths": json.dumps(self.walk_paths) if self.walk_paths else None,
                        "input_file": os.path.abspath(self.input_file) if self.input_file else None,
                        "mode": "incremental" if self.incremental else "full",
                        "status": status, "start_ts": start_dt,
                        "end_ts": datetime.now(timezone.utc),
                        "watermark_ts": (datetime.fromisoformat(self.watermark_iso)
                                         if self.watermark_iso else None),
                        "files": self._files, "bytes": self._bytes,
                        "failed": self._failed, "skipped": self._skipped,
                        "unchanged": self._unchanged, "tars": self._tars,
                        "dst_prefix": self.dst_prefix,
                        "failed_list": (f"{socket.gethostname()}:{self.failed_list.txt}"
                                        if self.failed_list.written else None),
                    })
                    self.logger.info(f"Run recorded: {self.run_id} status={status}")
                except Exception as e:
                    self.logger.error(f"could not record run in history -- "
                                      f"{type(e).__name__}: {e}")

    def _run_pipeline(self):
        start = self._t0 = time.time()
        ext = ".tar.gz" if self.compress else ".tar"
        mp_ctx = multiprocessing.get_context("spawn")

        writer = ManifestWriter(self.catalog_props, self.args.namespace,
                                self.args.table_name, self.manifest_queue,
                                self.logger, batch_size=self.batch_size,
                                flush_interval=self.args.flush_interval,
                                commit_retries=self.args.commit_retries,
                                dir_index=self.args.dir_index, run_id=self.run_id)
        writer.start()

        progress_q = None
        if self.args.progress:
            # bounded: workers put_nowait and drop events rather than block
            progress_q = mp_ctx.Queue(maxsize=10_000)
            self.progress = ProgressReporter(self, progress_q)
            self.progress.writer = writer
            self.progress.start()

        self.logger.info(
            f"Starting: run_id={self.run_id}, workers={self.num_workers}, "
            f"scan_threads={self.scan_threads}, "
            f"md5={'on' if self.do_md5 else 'off'}, compress={self.compress}")

        class WriterDied(RuntimeError):
            """Manifest writer is gone; the run must abort, not continue."""

        def check_writer():
            """Abort promptly if the manifest writer died (bad table-bucket ARN,
            auth failure, commit retries exhausted). Without this check the run
            would keep uploading tars that get NO manifest rows (all orphans),
            and once the bounded queue filled, put() would block forever."""
            if not writer.is_alive():
                raise WriterDied(
                    f"manifest writer died, aborting before more orphan tars "
                    f"are uploaded: {writer.error}")

        def collect(fut):
            """Collect one finished future's result in the MAIN thread (no callback,
            no cross-thread queue deadlock)."""
            try:
                try:
                    res = fut.result()
                finally:
                    tk = getattr(fut, "_tar_key", None)
                    if tk:
                        self._inflight_tars.discard(tk)
                        if self.progress:
                            self.progress.tar_finished(tk)
                for path, why in res.get("failed_files", []):
                    self.failed_list.add(path, why)
                for r in res["rows"]:
                    check_writer()
                    self.manifest_queue.put(r, timeout=60)
                # count a tar only when one was actually uploaded (rows > 0)
                self._update(files=res["count"], failed=res["failed"],
                             tars=1 if res["count"] else 0, nbytes=res["total_bytes"])
                if res["count"]:
                    self.logger.info(
                        f"tar {res['tar_key'].split('/')[-1]} "
                        f"({res['count']} files, {self._human(res['total_bytes'])})")
            except WriterDied:
                raise  # abort the run, don't swallow as a batch failure
            except queue.Full:
                check_writer()  # most likely cause; raises WriterDied with detail
                raise           # writer alive but stalled >60s: still abort
            except Exception as e:
                # whole batch lost (e.g. upload failed after retries): every file
                # in it failed, so attribute the full batch size, not 1.
                files = getattr(fut, "_files", [])
                nfiles = len(files) or 1
                self.logger.error(f"batch failed ({nfiles} files): {e}")
                why = f"tar upload: {type(e).__name__}: {e}"
                for full, _rel, _sz in files:
                    self.failed_list.add(full, why)
                self._update(failed=nfiles)

        # Sliding window: keep at most `max_inflight` tasks submitted at once so we
        # don't buffer millions of futures.
        max_inflight = self.num_workers * 3
        # Use 'spawn' (not fork): the manifest-writer thread is already running and
        # holds pyiceberg/logging locks. Forking under a held lock deadlocks the
        # child. spawn starts a clean interpreter, so no inherited locks.
        poisoned = False
        run_ok, run_err = False, None
        try:
            watcher = self._start_scanners()
            with ProcessPoolExecutor(
                    max_workers=self.num_workers,
                    mp_context=mp_ctx,
                    initializer=_init_worker,
                    initargs=(self.args.profile_name, self.args.region,
                              self.args.tar_storageclass,
                              16 * 1024 * 1024, 16 * 1024 * 1024,
                              self.args.upload_concurrency, progress_q)) as ex:
                inflight = set()
                batch_no = 0
                while True:
                    batch = self.batch_queue.get()
                    if batch is _SCAN_DONE:
                        if self.progress:
                            self.progress.scan_done = True
                        break
                    check_writer()  # stop submitting once the writer is gone
                    batch_no += 1
                    # run_id keeps this key unique across concurrent runs even
                    # when they share --dst-prefix; batch_no is assigned here in
                    # the main thread, so it stays gap-free and unique per run.
                    tar_key = (f"{self.dst_prefix}/{self.mid_prefix}/"
                               f"archive_{self.run_id}_{batch_no:05d}{ext}")
                    task = {"files": batch, "tar_key": tar_key,
                            "bucket": self.dst_bucket, "compress": self.compress,
                            "do_md5": self.do_md5, "spool_max": self.spool_max}
                    fut = ex.submit(_process_batch, task)
                    fut._files = batch  # failure accounting + the failed-file list
                    fut._tar_key = tar_key
                    self._inflight_tars.add(tar_key)
                    inflight.add(fut)
                    # Drain whatever has FINISHED, not the oldest: a single slow
                    # upload used to block the window head while later batches sat
                    # done. The scanners keep filling batch_queue while we wait
                    # here, so waiting no longer stalls the walk.
                    if len(inflight) >= max_inflight:
                        done, inflight = wait(inflight, return_when=FIRST_COMPLETED)
                        for fut in done:
                            collect(fut)
                # drain remaining
                for fut in inflight:
                    collect(fut)

            watcher.join(timeout=30)
            self.manifest_queue.put(ManifestWriter.POISON)
            poisoned = True
            writer.join()
            if writer.error:
                raise RuntimeError(f"manifest writer failed: {writer.error}")
            run_ok = True
        except BaseException as e:
            run_err = f"{type(e).__name__}: {e}"
            raise
        finally:
            # Release the scanner threads, and make sure the (non-daemon) manifest
            # writer is always told to stop -- otherwise an abort here would leave
            # the process unable to exit.
            self._abort.set()
            if not poisoned:
                try:
                    self.manifest_queue.put_nowait(ManifestWriter.POISON)
                except queue.Full:
                    pass
                if writer.is_alive():
                    writer.join(timeout=60)
            # last, so the final snapshot includes the writer's closing flush
            if self.progress:
                self.progress.finish(run_ok, run_err)

        elapsed = time.time() - start
        rate = self._bytes / elapsed if elapsed else 0
        self.logger.info("#" * 40)
        self.logger.info(f"Completed in {elapsed:.1f}s")
        self.logger.info(f"Run id          : {self.run_id}")
        self.logger.info(f"Files processed : {self._files:,}")
        self.logger.info(f"Files failed    : {self._failed:,}")
        self.logger.info(f"Files skipped   : {self._skipped:,} (symlinks / special files)")
        if self.incremental:
            self.logger.info(f"Unchanged       : {self._unchanged:,} (older than the "
                             f"watermark {self.watermark_iso or '-'})")
        self.logger.info(f"Dirs scanned    : {self._dirs_scanned:,}")
        self.logger.info(f"Tar objects     : {self._tars:,}")
        self.logger.info(f"Manifest rows   : {writer.rows_committed:,} "
                         f"in {writer.commit_count} commits "
                         f"({writer.conflict_count} commit retries)")
        if self.args.dir_index:
            self.logger.info(f"Dir-index rows  : {writer.dir_rows_committed:,} "
                             f"({writer.dir_index_failures} failed commits)")
        self.logger.info(f"MD5             : {'on' if self.do_md5 else 'off'}")
        self.logger.info(f"Workers         : {self.num_workers} "
                         f"(scan threads {self.scan_threads})")
        self.logger.info(f"Bytes           : {self._human(self._bytes)}")
        self.logger.info(f"Avg rate        : {self._human(rate)}/s")
        self.logger.info("#" * 40)


def main():
    p = argparse.ArgumentParser(description="Filesystem -> S3 archiver (multiprocess, S3 Tables manifest)")
    p.add_argument("--src-path", required=True,
                   help="Source directory to walk. Also used to derive tar "
                        "member paths (rel to this) when --input-file is given")
    p.add_argument("--input-file",
                   help="Path to a file listing source files to archive (one "
                        "per line; blank lines and '#' comments skipped). When "
                        "set, --src-path is not walked")
    p.add_argument("--walk-path", action="append",
                   help="walk only this directory (or file) instead of the whole "
                        "--src-path; repeatable. Member paths stay relative to "
                        "--src-path, so --src-path /data --walk-path /data/photos "
                        "stores photos/... (used by the GUI)")
    p.add_argument("--dst-bucket", required=True)
    p.add_argument("--dst-prefix", required=True)
    grp = p.add_mutually_exclusive_group(required=True)
    grp.add_argument("--max-files", type=int)
    grp.add_argument("--max-size", type=parse_size, help="e.g. 512MB")
    p.add_argument("--num-workers", type=int, default=os.cpu_count(),
                   help=f"worker processes (default: os.cpu_count()={os.cpu_count()})")
    p.add_argument("--scan-threads", type=int, default=1,
                   help="threads enumerating the source tree (default 1: serial "
                        "walk). Raise this ONLY for a latency-bound source. Against "
                        "a simulated 1ms-per-entry stat the walk scales near-linearly "
                        "(~30x at 32 threads), but on local NVMe the measured peak "
                        "was only 1.9x at 4 threads and flat past 8, and on a warm "
                        "cache extra threads just add contention. In four cold "
                        "1M-file end-to-end runs the total time was unchanged between "
                        "1 and 8 scan threads -- the device's IOPS budget was the "
                        "wall, not the walk. Measure before raising it: compare "
                        "`find <src> -type f -printf ''` against the same find "
                        "fanned out over subdirectories with xargs -P")
    p.add_argument("--run-id",
                   help="token embedded in tar names and the log filename to keep "
                        "concurrent runs apart (default: <timestamp>_<pid>_<rand>). "
                        "Override only if you need a reproducible name -- two live "
                        "runs sharing a --dst-prefix AND a --run-id will overwrite "
                        "each other's tars")
    p.add_argument("--upload-concurrency", type=int, default=10,
                   help="multipart upload threads per worker (default 10)")
    p.add_argument("--md5", action=argparse.BooleanOptionalAction, default=True,
                   help="compute per-file MD5 for integrity verification (on by "
                        "default; --no-md5 disables it and leaves the md5 column null)")
    p.add_argument("--compress", action="store_true",
                   help="gzip the tar (disables byte-range retrieval offsets)")
    p.add_argument("--spool-max-mb", type=int, default=256)
    p.add_argument("--profile-name", default=None)
    p.add_argument("--tar-storageclass", default="STANDARD",
                   help="storage class for the tar objects, passed straight to "
                        "PutObject (default STANDARD: no minimum storage duration "
                        "and no retrieval fee, so nothing surprises you on a first "
                        "run or a test). For a real cold archive set GLACIER_IR -- "
                        "much cheaper per GB and still millisecond ranged GET, at "
                        "the cost of a 90-day minimum duration and a retrieval fee. "
                        "GLACIER / DEEP_ARCHIVE need a whole-tar restore before any "
                        "individual file can be read")
    p.add_argument("--region", required=True)
    p.add_argument("--table-bucket-arn", required=True)
    p.add_argument("--namespace", default="small_files_archive")
    p.add_argument("--table-name", default="tar_manifest")
    p.add_argument("--batch-size", type=int, default=50_000,
                   help="manifest rows per Iceberg commit (default 50,000)")
    p.add_argument("--flush-interval", type=float, default=15.0,
                   help="seconds before a partial manifest batch is committed "
                        "anyway (default 15). Raise it when many runs write to "
                        "one table, to cut the commit rate and with it the "
                        "conflict and snapshot churn")
    p.add_argument("--commit-retries", type=int, default=8,
                   help="attempts per manifest commit before the run aborts "
                        "(default 8, full-jitter exponential backoff)")
    p.add_argument("--dir-index", action=argparse.BooleanOptionalAction, default=True,
                   help="maintain the <table-name>_dirs directory index and the "
                        "manifest parent_dir column, so a browser (the GUI) can "
                        "list one folder at a time at billion-file scale (on by "
                        "default; adds parent_dir to an existing table via "
                        "additive schema evolution)")
    p.add_argument("--incremental", action="store_true",
                   help="archive only files whose mtime or ctime is newer than the "
                        "start of the last successful run of the same source "
                        "(same --src-path and --walk-path set), read from the "
                        "<table-name>_runs history table. The first run of a source "
                        "archives everything")
    p.add_argument("--watermark-skew", type=float, default=300.0,
                   help="seconds subtracted from the incremental watermark to absorb "
                        "clock differences between hosts / file servers (default 300); "
                        "files in the overlap are re-archived, never missed")
    p.add_argument("--input-format", choices=["auto", "lines", "csv", "tsv"],
                   default="auto",
                   help="--input-file format: lines (one path per line), csv or tsv "
                        "(a file-list report); auto picks by extension; .gz is "
                        "read transparently (default auto)")
    p.add_argument("--path-column",
                   help="csv/tsv report column holding the path: header name "
                        "(e.g. 'path') or 0-based index (default 0)")
    p.add_argument("--run-history", action=argparse.BooleanOptionalAction, default=True,
                   help="record every run in <table-name>_runs (on by default; "
                        "required by --incremental)")
    p.add_argument("--progress", action="store_true",
                   help="print machine-readable JSON progress snapshots to stdout "
                        "(lines prefixed '@@PROGRESS '); used by the GUI")
    args = p.parse_args()
    if args.scan_threads <= 0:
        p.error("--scan-threads must be a positive integer")
    if args.commit_retries <= 0:
        p.error("--commit-retries must be a positive integer")
    if args.flush_interval <= 0:
        p.error("--flush-interval must be positive")

    FS2S3ArchiverMP(args).start_processing()


if __name__ == "__main__":
    main()
