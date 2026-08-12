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
    scanner --> batches --> ProcessPoolExecutor (N worker processes)
                               each worker: build tar (+ MD5) + S3 multipart upload
                               returns manifest rows (small) back to main
    done-callback --> manifest queue --> single writer thread --> S3 Tables batch commit

Design notes for multiprocessing correctness:
  * Each worker process creates its OWN boto3 S3 client (via pool initializer);
    boto3 clients are not fork/pickle safe to share.
  * tar filename is derived from a batch index assigned in the main process, so no
    cross-process sequence counter/lock is needed.
  * Only small manifest rows cross the process boundary (never tar bytes).
  * The single manifest writer stays in the main process (I/O bound) so commits
    remain serialized -> no Iceberg OCC conflicts, no small-file/snapshot explosion.

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
import queue
import hashlib
import logging
import argparse
import tarfile
import threading
import tempfile
import multiprocessing
from datetime import datetime, timezone
from concurrent.futures import ProcessPoolExecutor

import pyarrow as pa
from pyiceberg.catalog import load_catalog
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
                 batch_size=50_000, flush_interval=15.0):
        super().__init__(name="manifest-writer", daemon=False)
        self._props = catalog_props
        self._ns = namespace
        self._tbl_name = table_name
        self._q = row_queue
        self._log = logger
        self._batch = batch_size
        self._flush = flush_interval
        self.rows_committed = 0
        self.commit_count = 0
        self._error = None

    def _ensure_table(self):
        catalog = load_catalog("s3tables", **self._props)
        try:
            catalog.create_namespace(self._ns)
            self._log.info(f"Created namespace {self._ns}")
        except Exception:
            pass
        ident = f"{self._ns}.{self._tbl_name}"
        try:
            catalog.create_table(
                identifier=ident, schema=ICEBERG_SCHEMA,
                partition_spec=PartitionSpec(
                    PartitionField(source_id=7, field_id=1000,
                                   transform=DayTransform(), name="archive_ts_day")))
            self._log.info(f"Created table {ident}")
        except Exception:
            self._log.info(f"Table {ident} already exists, reusing")
        return catalog.load_table(ident)

    def _commit(self, tbl, pending):
        arrow = pa.Table.from_pylist(pending, schema=MANIFEST_ARROW_SCHEMA)
        for attempt in range(3):
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
                if attempt == 2:
                    raise
                time.sleep(0.5 * (attempt + 1))
                self._log.warning(f"manifest-writer: retry append ({e})")

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
        self.current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.mid_prefix = self.current_time.split("_")[0]

        # exactly one of the two batching rules, and it must be positive
        # (argparse's mutually-exclusive group already blocks passing both)
        if (self.max_files is None) == (self.max_size is None):
            raise ValueError("Specify exactly one of --max-files or --max-size")
        if self.max_files is not None and self.max_files <= 0:
            raise ValueError("--max-files must be a positive integer")
        if self.max_size is not None and self.max_size <= 0:
            raise ValueError("--max-size must be positive")

        os.makedirs(os.path.join("logs", self.dst_prefix), exist_ok=True)
        self.logger = logging.getLogger("fss3-archiver-mp")
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
            fh = logging.FileHandler(
                os.path.join("logs", self.dst_prefix, f"archiver_{self.current_time}.log"))
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
        self._stats_lock = threading.Lock()
        self._files = self._failed = self._tars = self._bytes = 0

    def _update(self, files=0, failed=0, tars=0, nbytes=0):
        with self._stats_lock:
            self._files += files
            self._failed += failed
            self._tars += tars
            self._bytes += nbytes

    @staticmethod
    def _human(n):
        size = float(n)
        for u in ['B', 'KB', 'MB', 'GB', 'TB', 'PB']:
            if size < 1024 or u == 'PB':
                return f"{size:.2f}{u}"
            size /= 1024

    def _regular_size(self, path):
        """Return the size of `path` if it is a regular file, else None.

        Uses a single lstat (no symlink follow). Symlinks are skipped rather
        than archived: tarfile would store a 0-byte symlink member while the
        manifest recorded the *target's* size, producing an empty file that
        fails byte-range restore/MD5. Missing paths and non-regular files
        (dir, fifo, device, socket) are also skipped. Every skip is logged and
        counted as a failure; callers just check for None."""
        try:
            st = os.lstat(path)
        except OSError as e:
            self.logger.warning(f"stat failed, skipping {path}: {e}")
            self._update(failed=1)
            return None
        if stat.S_ISLNK(st.st_mode):
            self.logger.warning(f"symlink skipped (not archived): {path}")
            self._update(failed=1)
            return None
        if not stat.S_ISREG(st.st_mode):
            self.logger.warning(f"not a regular file, skipping: {path}")
            self._update(failed=1)
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

    def _iter_walk(self):
        """Yield (full, rel, size) by recursively walking --src-path."""
        for root, _, files in os.walk(self.src):
            for name in files:
                full = os.path.join(root, name)
                sz = self._regular_size(full)
                if sz is None:
                    continue
                rel = os.path.relpath(full, self.src)
                yield full, rel, sz

    def _iter_input_file(self):
        """Yield (full, rel, size) from an explicit list file (one path per line).

        Blank lines and lines starting with '#' are skipped. Paths that don't
        resolve to a regular file are logged and counted as failures (see
        _regular_size). The rel path is derived relative to --src-path when the
        file lies under it, so tar member names match the directory-walk layout."""
        with open(self.input_file, "r", encoding="utf-8") as f:
            for line in f:
                path = line.strip()
                if not path or path.startswith("#"):
                    continue
                sz = self._regular_size(path)
                if sz is None:
                    continue
                yield path, self._rel_to_src(path), sz

    def _scan_batches(self):
        """Yield batches of (full, rel, size) by size or count.

        Files come from --input-file when provided, otherwise from walking
        --src-path."""
        source = self._iter_input_file() if self.input_file else self._iter_walk()
        batch, bsize = [], 0
        for full, rel, sz in source:
            batch.append((full, rel, sz))
            bsize += sz
            if (self.max_files and len(batch) >= self.max_files) or \
               (self.max_size and bsize >= self.max_size):
                yield batch
                batch, bsize = [], 0
        if batch:
            yield batch

    def start_processing(self):
        start = time.time()
        ext = ".tar.gz" if self.compress else ".tar"

        writer = ManifestWriter(self.catalog_props, self.args.namespace,
                                self.args.table_name, self.manifest_queue,
                                self.logger, batch_size=self.batch_size)
        writer.start()

        self.logger.info(
            f"Starting: workers={self.num_workers}, md5={'on' if self.do_md5 else 'off'}, "
            f"compress={self.compress}")

        def collect(fut):
            """Collect one finished future's result in the MAIN thread (no callback,
            no cross-thread queue deadlock)."""
            try:
                res = fut.result()
                for r in res["rows"]:
                    self.manifest_queue.put(r)
                # count a tar only when one was actually uploaded (rows > 0)
                self._update(files=res["count"], failed=res["failed"],
                             tars=1 if res["count"] else 0, nbytes=res["total_bytes"])
                if res["count"]:
                    self.logger.info(
                        f"tar {res['tar_key'].split('/')[-1]} "
                        f"({res['count']} files, {self._human(res['total_bytes'])})")
            except Exception as e:
                # whole batch lost (e.g. upload failed after retries): every file
                # in it failed, so attribute the full batch size, not 1.
                nfiles = getattr(fut, "_nfiles", 1)
                self.logger.error(f"batch failed ({nfiles} files): {e}")
                self._update(failed=nfiles)

        # Sliding window: keep at most `max_inflight` tasks submitted at once so we
        # don't buffer millions of futures. Submit and drain are interleaved in the
        # main thread, which keeps the manifest queue moving and avoids deadlock.
        max_inflight = self.num_workers * 3
        # Use 'spawn' (not fork): the manifest-writer thread is already running and
        # holds pyiceberg/logging locks. Forking under a held lock deadlocks the
        # child. spawn starts a clean interpreter, so no inherited locks.
        mp_ctx = multiprocessing.get_context("spawn")
        with ProcessPoolExecutor(
                max_workers=self.num_workers,
                mp_context=mp_ctx,
                initializer=_init_worker,
                initargs=(self.args.profile_name, self.args.region,
                          self.args.tar_storageclass,
                          16 * 1024 * 1024, 16 * 1024 * 1024,
                          self.args.upload_concurrency)) as ex:
            inflight = []
            batch_no = 0
            for batch in self._scan_batches():
                batch_no += 1
                tar_key = (f"{self.dst_prefix}/{self.mid_prefix}/"
                           f"archive_{self.current_time}_{batch_no:05d}{ext}")
                task = {"files": batch, "tar_key": tar_key,
                        "bucket": self.dst_bucket, "compress": self.compress,
                        "do_md5": self.do_md5, "spool_max": self.spool_max}
                fut = ex.submit(_process_batch, task)
                fut._nfiles = len(batch)  # for accurate failure accounting
                inflight.append(fut)
                # drain oldest when window is full (FIFO keeps memory bounded)
                if len(inflight) >= max_inflight:
                    collect(inflight.pop(0))
            # drain remaining
            for fut in inflight:
                collect(fut)

        self.manifest_queue.put(ManifestWriter.POISON)
        writer.join()
        if writer.error:
            raise RuntimeError(f"manifest writer failed: {writer.error}")

        elapsed = time.time() - start
        rate = self._bytes / elapsed if elapsed else 0
        self.logger.info("#" * 40)
        self.logger.info(f"Completed in {elapsed:.1f}s")
        self.logger.info(f"Files processed : {self._files:,}")
        self.logger.info(f"Files failed    : {self._failed:,}")
        self.logger.info(f"Tar objects     : {self._tars:,}")
        self.logger.info(f"Manifest rows   : {writer.rows_committed:,} "
                         f"in {writer.commit_count} commits")
        self.logger.info(f"MD5             : {'on' if self.do_md5 else 'off'}")
        self.logger.info(f"Workers         : {self.num_workers}")
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
    p.add_argument("--upload-concurrency", type=int, default=10,
                   help="multipart upload threads per worker (default 10)")
    p.add_argument("--md5", action=argparse.BooleanOptionalAction, default=True,
                   help="compute per-file MD5 for integrity verification (on by "
                        "default; --no-md5 disables it and leaves the md5 column null)")
    p.add_argument("--compress", action="store_true",
                   help="gzip the tar (disables byte-range retrieval offsets)")
    p.add_argument("--spool-max-mb", type=int, default=256)
    p.add_argument("--profile-name", default=None)
    p.add_argument("--tar-storageclass", default="GLACIER_IR")
    p.add_argument("--region", required=True)
    p.add_argument("--table-bucket-arn", required=True)
    p.add_argument("--namespace", default="archive")
    p.add_argument("--table-name", default="manifest")
    p.add_argument("--batch-size", type=int, default=50_000)
    args = p.parse_args()

    FS2S3ArchiverMP(args).start_processing()


if __name__ == "__main__":
    main()
