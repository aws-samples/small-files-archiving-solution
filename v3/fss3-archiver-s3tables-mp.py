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


def _init_worker(profile, region, storageclass, mp_threshold, mp_chunksize, upload_conc):
    import boto3
    from boto3.s3.transfer import TransferConfig
    session = boto3.Session(profile_name=profile)
    _WORKER["s3"] = session.client("s3", region_name=region)
    _WORKER["storageclass"] = storageclass
    _WORKER["tconf"] = TransferConfig(
        multipart_threshold=mp_threshold,
        multipart_chunksize=mp_chunksize,
        max_concurrency=upload_conc,
        use_threads=True,
    )


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
    rows, failed = [], 0

    try:
        with tarfile.open(fileobj=spool, mode=mode) as tar:
            for full, rel, size in task["files"]:
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
                    })
                except Exception:
                    failed += 1

        # Nothing packed (every file in the batch failed) -> don't upload an
        # empty tar; just report the per-file failures.
        if not rows:
            return {"tar_key": task["tar_key"], "rows": [],
                    "failed": failed, "total_bytes": 0, "count": 0}

        # upload tar with retry. Keep the FIRST error: boto3 closes the fileobj
        # on a failed transfer, so a later spool.seek(0) would raise a generic
        # "I/O operation on closed file" that masks the real cause (e.g.
        # NoSuchBucket). Once the spool is closed there's nothing to retry from,
        # so stop and surface the original error rather than sleeping in vain.
        first_err = None
        for attempt in range(5):
            if spool.closed:
                break
            try:
                spool.seek(0)
                s3.upload_fileobj(
                    spool, task["bucket"], task["tar_key"].lstrip("/"),
                    ExtraArgs={"StorageClass": sclass}, Config=tconf)
                first_err = None
                break
            except Exception as e:
                if first_err is None:
                    first_err = e
                if spool.closed or attempt == 4:
                    break
                time.sleep(2 ** attempt)
        if first_err is not None:
            raise first_err
    finally:
        spool.close()

    total = sum(r["filesize"] for r in rows)
    return {"tar_key": task["tar_key"], "rows": rows,
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
])

ICEBERG_SCHEMA = Schema(
    NestedField(1, "tar_key",    StringType(),      required=True),
    NestedField(2, "source_key", StringType(),      required=True),
    NestedField(3, "byte_start", LongType(),        required=True),
    NestedField(4, "byte_end",   LongType(),        required=True),
    NestedField(5, "md5",        StringType(),      required=False),
    NestedField(6, "filesize",   LongType(),        required=True),
    NestedField(7, "archive_ts", TimestamptzType(), required=True),
)


class ManifestWriter(threading.Thread):
    POISON = None

    def __init__(self, catalog_props, namespace, table_name, row_queue, logger,
                 batch_size=50_000, flush_interval=15.0, commit_retries=8,
                 retry_cap=30.0):
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
        self.rows_committed = 0
        self.commit_count = 0
        self.conflict_count = 0
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
            return catalog.load_table(ident)
        except Exception as e:
            if setup_err is not None:
                raise RuntimeError(
                    f"cannot use manifest table {ident}: setup failed earlier with "
                    f"{type(setup_err).__name__}: {setup_err}") from setup_err
            raise

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
        arrow = pa.Table.from_pylist(pending, schema=MANIFEST_ARROW_SCHEMA)
        for attempt in range(self._retries):
            try:
                tbl.refresh()
                tbl.append(arrow)
                self.rows_committed += len(pending)
                self.commit_count += 1
                self._log.info(
                    f"manifest-writer: committed {len(pending):,} rows "
                    f"(total {self.rows_committed:,}, commits {self.commit_count})")
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
                    f"manifest-writer: append failed "
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

    def __init__(self, out_queue, max_files, max_size, abort):
        self._out = out_queue
        self._max_files = max_files
        self._max_size = max_size
        self._abort = abort
        self._batch = []
        self._bytes = 0

    def add(self, item):
        self._batch.append(item)
        self._bytes += item[2]
        if (self._max_files and len(self._batch) >= self._max_files) or \
           (self._max_size and self._bytes >= self._max_size):
            self.flush()

    def flush(self):
        if not self._batch:
            return
        batch, self._batch, self._bytes = self._batch, [], 0
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
        self._dirs_scanned = 0

    def _update(self, files=0, failed=0, tars=0, nbytes=0, dirs=0):
        with self._stats_lock:
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
        device, socket) are skipped too. Every skip is logged and counted as a
        failure. Directories are the caller's business and must be filtered
        before calling this."""
        if stat.S_ISLNK(st_mode):
            self.logger.warning(f"symlink skipped (not archived): {path}")
            self._update(failed=1)
            return False
        if not stat.S_ISREG(st_mode):
            self.logger.warning(f"not a regular file, skipping: {path}")
            self._update(failed=1)
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
            self._update(failed=1)
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
        with os.scandir(full_dir) as it:
            for entry in it:
                try:
                    st = entry.stat(follow_symlinks=False)
                except OSError as e:
                    self.logger.warning(f"stat failed, skipping {entry.path}: {e}")
                    self._update(failed=1)
                    continue
                rel = os.path.join(rel_dir, entry.name) if rel_dir else entry.name
                if stat.S_ISDIR(st.st_mode):
                    dir_queue.put((entry.path, rel))
                elif stat.S_ISREG(st.st_mode):
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
                    self._scan_one_dir(item[0], item[1], dir_queue, emitter)
                except _ScanAborted:
                    raise  # let the outer handler end the thread; finally still runs
                except Exception as e:
                    self.logger.error(f"scan failed for directory {item[0]}: {e}")
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

    def _feed_input_file(self, path_queue):
        """Read --input-file and hand paths to the scanner threads.

        Blank lines and '#' comments are skipped. Bounded put keeps a huge list
        file from being slurped into memory."""
        with open(self.input_file, "r", encoding="utf-8") as f:
            for line in f:
                path = line.strip()
                if not path or path.startswith("#"):
                    continue
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
            work_queue.put((self.src, ""))

        scanners = []
        for i in range(self.scan_threads):
            emitter = _BatchEmitter(self.batch_queue, self.max_files,
                                    self.max_size, self._abort)
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
        start = time.time()
        ext = ".tar.gz" if self.compress else ".tar"

        writer = ManifestWriter(self.catalog_props, self.args.namespace,
                                self.args.table_name, self.manifest_queue,
                                self.logger, batch_size=self.batch_size,
                                flush_interval=self.args.flush_interval,
                                commit_retries=self.args.commit_retries)
        writer.start()

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
                res = fut.result()
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
                nfiles = getattr(fut, "_nfiles", 1)
                self.logger.error(f"batch failed ({nfiles} files): {e}")
                self._update(failed=nfiles)

        # Sliding window: keep at most `max_inflight` tasks submitted at once so we
        # don't buffer millions of futures.
        max_inflight = self.num_workers * 3
        # Use 'spawn' (not fork): the manifest-writer thread is already running and
        # holds pyiceberg/logging locks. Forking under a held lock deadlocks the
        # child. spawn starts a clean interpreter, so no inherited locks.
        mp_ctx = multiprocessing.get_context("spawn")
        poisoned = False
        try:
            watcher = self._start_scanners()
            with ProcessPoolExecutor(
                    max_workers=self.num_workers,
                    mp_context=mp_ctx,
                    initializer=_init_worker,
                    initargs=(self.args.profile_name, self.args.region,
                              self.args.tar_storageclass,
                              16 * 1024 * 1024, 16 * 1024 * 1024,
                              self.args.upload_concurrency)) as ex:
                inflight = set()
                batch_no = 0
                while True:
                    batch = self.batch_queue.get()
                    if batch is _SCAN_DONE:
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
                    fut._nfiles = len(batch)  # for accurate failure accounting
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

        elapsed = time.time() - start
        rate = self._bytes / elapsed if elapsed else 0
        self.logger.info("#" * 40)
        self.logger.info(f"Completed in {elapsed:.1f}s")
        self.logger.info(f"Run id          : {self.run_id}")
        self.logger.info(f"Files processed : {self._files:,}")
        self.logger.info(f"Files failed    : {self._failed:,}")
        self.logger.info(f"Dirs scanned    : {self._dirs_scanned:,}")
        self.logger.info(f"Tar objects     : {self._tars:,}")
        self.logger.info(f"Manifest rows   : {writer.rows_committed:,} "
                         f"in {writer.commit_count} commits "
                         f"({writer.conflict_count} commit retries)")
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
    p.add_argument("--namespace", default="archive")
    p.add_argument("--table-name", default="manifest")
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
