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

The pipeline, manifest, checksums (CRC64NVME + optional MD5), run history,
cancel and resume live in archiver_core.py, shared with the S3-source
archiver (s3s3-archiver-s3tables-mp.py).

Requires: pip install "pyiceberg[pyarrow,pyiceberg-core]" "boto3[crt]"
"""
import os
import sys
import csv
import gzip
import stat
import queue
import threading
import argparse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from archiver_core import (ArchiverBase, add_common_args, run, source_id_for,  # noqa: E402
                           _BatchEmitter, _ScanAborted, _SCAN_DONE)


class FS2S3ArchiverMP(ArchiverBase):
    """Local disk / NFS / SMB source: directory walk or a file-list report."""

    def _init_source(self, args):
        self.src = args.src_path
        self.input_file = args.input_file
        self.walk_paths = [os.path.abspath(w) for w in (args.walk_path or [])]
        if self.walk_paths and self.input_file:
            raise ValueError("--walk-path and --input-file are mutually exclusive")
        if self.incremental and self.input_file:
            raise ValueError("--incremental applies to directory walks, not --input-file "
                             "(a list already says exactly what to archive)")
        self.input_format = getattr(args, "input_format", "auto")
        self.allow_outside_src = getattr(args, "allow_outside_src", False)
        self._src_real = os.path.realpath(self.src).rstrip(os.sep) or os.sep
        for wp in self.walk_paths:
            if not (os.path.realpath(wp) == self._src_real or
                    os.path.realpath(wp).startswith(self._src_real + os.sep)):
                raise ValueError(f"--walk-path {wp} is not under --src-path {self.src}")
        self.path_column = getattr(args, "path_column", None)
        self.source_id = source_id_for(self.src, self.walk_paths, self.input_file)
        self.scan_threads = args.scan_threads
        if self.scan_threads <= 0:
            raise ValueError("--scan-threads must be a positive integer")
        self._dirs_scanned = 0

    def _history_fields(self):
        import json
        return {"src_path": os.path.abspath(self.src),
                "walk_paths": json.dumps(self.walk_paths) if self.walk_paths else None,
                "input_file": os.path.abspath(self.input_file) if self.input_file else None}

    def _source_desc(self):
        return f"scan_threads={self.scan_threads}"

    def _summary_source(self):
        return [f"Dirs scanned    : {self._dirs_scanned:,}"]

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

    def _inside_src(self, path):
        """True if `path` really lies under --src-path: symlinks in its parent
        directories and '..' are resolved first, so a list entry can't reach
        outside the tree being archived."""
        try:
            real = os.path.realpath(os.path.dirname(path))
        except OSError:
            return False
        return real == self._src_real or real.startswith(self._src_real + os.sep)

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
        # Open the directory itself with O_NOFOLLOW and list it through that fd:
        # a subdirectory swapped for a symlink after it was queued is refused
        # (ELOOP) instead of taking the walk outside the tree. Roots given on the
        # command line are resolved once up front, so they may be symlinks.
        dfd = os.open(full_dir, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            self._scan_fd(dfd, full_dir, rel_dir, dir_queue, emitter, wm)
        finally:
            os.close(dfd)
        self._update(dirs=1)

    def _scan_fd(self, dfd, full_dir, rel_dir, dir_queue, emitter, wm):
        with os.scandir(dfd) as it:
            for entry in it:
                path = os.path.join(full_dir, entry.name)
                try:
                    st = entry.stat(follow_symlinks=False)
                except OSError as e:
                    self.logger.warning(f"stat failed, skipping {path}: {e}")
                    self._fail(path, f"stat: {e}")
                    continue
                rel = os.path.join(rel_dir, entry.name) if rel_dir else entry.name
                if stat.S_ISDIR(st.st_mode):
                    dir_queue.put((path, rel))
                elif stat.S_ISREG(st.st_mode):
                    if self.skip_keys is not None and rel in self.skip_keys:
                        emitter.unchanged += 1          # already archived (--resume-run)
                        continue
                    # --incremental: ctime catches renames/moves and metadata
                    # changes that leave mtime old; mtime catches content writes
                    if wm is not None and max(st.st_mtime, st.st_ctime) < wm:
                        emitter.unchanged += 1
                        if emitter.unchanged >= 2000:   # keep progress moving
                            emitter.report_unchanged()
                        continue
                    emitter.add((path, rel, st.st_size))
                else:
                    # symlink (including symlinked dirs, which we don't follow --
                    # matching os.walk's followlinks=False) or special file
                    self._check_regular(st.st_mode, path)

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
                    if not self.allow_outside_src and not self._inside_src(path):
                        self.logger.warning(f"outside --src-path, skipped: {path!r}")
                        self._fail(path, "outside --src-path (use --allow-outside-src)",
                                   retryable=False)
                        continue
                    rel = self._rel_to_src(path)
                    if self.skip_keys is not None and rel in self.skip_keys:
                        emitter.unchanged += 1
                        continue
                    sz = self._regular_size(path)
                    if sz is not None:
                        emitter.add((path, rel, sz))
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
        if fmt == "nul":          # find ... -print0: any file name, even with newlines
            with opener(name, "rb") as f:
                buf = b""
                while True:
                    chunk = f.read(1 << 20)
                    if not chunk:
                        break
                    buf += chunk
                    *parts, buf = buf.split(b"\0")
                    for raw in parts:
                        if raw:
                            yield self._abs_input(os.fsdecode(raw))
                if buf:
                    yield self._abs_input(os.fsdecode(buf))
            return
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
        return os.path.normpath(path if os.path.isabs(path) else os.path.join(self.src, path))

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
                # the root may itself be a symlink (e.g. a mount alias): resolve
                # it once; only what lies below it is walked without following
                work_queue.put((os.path.realpath(self.src), ""))
            for wp in self.walk_paths:
                rel = self._rel_to_src(wp)
                if os.path.isdir(wp) and not os.path.islink(wp):
                    work_queue.put((os.path.realpath(wp), "" if rel == "." else rel))
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
    p.add_argument("--allow-outside-src", action="store_true",
                   help="accept --input-file paths that resolve outside --src-path "
                        "(stored with the leading '/' dropped). Off by default, so a "
                        "list can't pull in files from elsewhere on the host")
    p.add_argument("--input-format", choices=["auto", "lines", "csv", "tsv", "nul"],
                   default="auto",
                   help="--input-file format: lines (one path per line), nul "
                        "(NUL-separated, from find -print0: safe for any file name), "
                        "csv or tsv (a file-list report); auto picks by extension; "
                        ".gz is read transparently (default auto)")
    p.add_argument("--path-column",
                   help="csv/tsv report column holding the path: header name "
                        "(e.g. 'path') or 0-based index (default 0)")
    add_common_args(p)
    run(FS2S3ArchiverMP, p)


if __name__ == "__main__":
    main()
