"""
s3s3-archiver-s3tables-mp.py
============================
S3 bucket -> S3 archiver with an Amazon S3 Tables (Apache Iceberg) manifest.

Bundles the objects under an S3 prefix (or listed in a manifest) into tar
objects in a destination bucket, and records every object's location in the tar
(byte range) and its CRC64NVME checksum (optionally also MD5) in the manifest --
the same manifest, browser, restore, incremental, resume and failed-file list as
the local-disk archiver (fss3-archiver-s3tables-mp.py).

Where the time goes: small objects make the job GET-bound (one request per
object, ~10-20 ms each), not bandwidth-bound. Every worker process therefore
fetches its batch with many concurrent GETs and writes the tar in order:
--get-concurrency (default 256) is the total across --num-workers processes.

Run it close to the data: on EC2 or AWS CloudShell in the source bucket's
Region, reads cost no data transfer and latency is lowest. From a laptop the
same run works, but S3 data transfer out to the internet is charged per GB.

Sources:
  --src-s3 s3://bucket/prefix   parallel ListObjectsV2 below the prefix (split
                                on '/', --list-threads)
  --src-manifest FILE           s3tar CSV (bucket,key,size[,etag]), an S3
                                Inventory manifest.json (CSV or Parquet), or
                                one s3://bucket/key per line (the failed list)

Checksums: CRC64NVME is computed while each object streams into the tar. When
the source object carries a full-object CRC64NVME (S3 adds one to new uploads
by default), the two are compared and a mismatch fails that object. Values use
S3's encoding, so they can be cross-checked with an S3 Batch Operations
"compute checksum" report.

Objects in GLACIER / DEEP_ARCHIVE must be restored in S3 first; they are
reported as failed, not fetched. The source is never modified or deleted.

Requires: pip install "pyiceberg[pyarrow,pyiceberg-core]" "boto3[crt]"
"""
import os
import io
import sys
import csv
import gzip
import json
import queue
import threading
import argparse
from urllib.parse import unquote

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from archiver_core import (ArchiverBase, add_common_args, run, source_id_for,  # noqa: E402
                           _BatchEmitter, _ScanAborted, _SCAN_DONE, fast_session)

ARCHIVE_CLASSES = {"GLACIER", "DEEP_ARCHIVE"}


def parse_s3_uri(uri):
    if not uri.startswith("s3://"):
        raise ValueError(f"not an s3:// URI: {uri}")
    bucket, _, key = uri[5:].partition("/")
    if not bucket:
        raise ValueError(f"no bucket in {uri}")
    return bucket, key


class S3S3ArchiverMP(ArchiverBase):
    """S3 source: list a prefix (or read a manifest) and archive the objects."""
    retry_hint = "retry: pass it as --src-manifest (format uris) with the same --src-s3"

    # -- options -------------------------------------------------------------
    def _init_source(self, args):
        from botocore.config import Config as BotoConfig
        if bool(args.src_s3) == bool(args.src_manifest):
            raise ValueError("give exactly one of --src-s3 or --src-manifest")
        self.manifest = args.src_manifest
        self.manifest_format = args.src_manifest_format
        def folder(p):
            p = (p or "").lstrip("/")
            return p + "/" if p and not p.endswith("/") else p
        if args.src_s3:
            self.src_bucket, prefix = parse_s3_uri(args.src_s3)
            self.list_prefix = folder(prefix)
        else:
            self.src_bucket, self.list_prefix = None, ""
        # keys are stored relative to --src-prefix (default: the listed prefix);
        # a parent of the listed prefix keeps the folder's own name, e.g.
        # --src-s3 s3://b/data/fs1/ --src-prefix data/  stores  fs1/...
        # None = not given (use the listed prefix); "" = the bucket root
        self.src_prefix = folder(args.src_prefix) if args.src_prefix is not None else self.list_prefix
        if self.src_bucket and not self.list_prefix.startswith(self.src_prefix):
            raise ValueError("--src-prefix must be the listed prefix or a parent of it")
        self.src_label = (f"s3://{self.src_bucket}/{self.list_prefix}" if self.src_bucket
                          else f"manifest:{self.manifest}")
        if self.incremental and self.manifest and self._manifest_kind() in ("s3tar", "uris"):
            raise ValueError("--incremental needs object timestamps: use --src-s3 or an "
                             "S3 Inventory manifest that includes LastModifiedDate")
        self.list_threads = args.list_threads
        self.get_concurrency = args.get_concurrency
        if self.list_threads <= 0 or self.get_concurrency <= 0:
            raise ValueError("--list-threads and --get-concurrency must be positive")
        self.src_region = args.src_region or args.region
        self.verify_src = args.verify_source_checksum
        self.get_client = args.get_client
        self._get_client_note = ""
        if self.get_client == "crt":
            try:
                import awscrt.s3  # noqa: F401
            except ImportError:      # logged with the run settings (no logger yet)
                self._get_client_note = " (awscrt S3 client not installed: pip install 'boto3[crt]')"
                self.get_client = "boto3"
        self.source_id = source_id_for(self.src_label, [], self.manifest)
        self._s3 = fast_session(args.profile_name).client(
            "s3", region_name=self.src_region,
            config=BotoConfig(max_pool_connections=max(32, self.list_threads * 2),
                              retries={"mode": "adaptive", "max_attempts": 10}))
        self._listed = self._prefixes = 0
        self._count_lock = threading.Lock()

    def _history_fields(self):
        return {"src_path": self.src_label, "walk_paths": None, "input_file": self.manifest}

    def _source_desc(self):
        return (f"list_threads={self.list_threads}, get_concurrency={self.get_concurrency} "
                f"({max(1, self.get_concurrency // self.num_workers)} per worker), "
                f"get_client={self.get_client}{self._get_client_note}")

    def _summary_source(self):
        return [f"Objects listed  : {self._listed:,}",
                f"Prefixes listed : {self._prefixes:,}"]

    def _worker_source(self):
        return self.src_region, max(1, self.get_concurrency // self.num_workers), self.get_client

    # -- per object ------------------------------------------------------------
    def _rel(self, key):
        """source_key for an object: its key relative to the source prefix."""
        if self.src_prefix and key.startswith(self.src_prefix):
            return key[len(self.src_prefix):]
        return key

    @staticmethod
    def _unsafe(rel):
        parts = rel.split("/")
        return (not rel or rel.startswith("/") or any(p in ("", ".", "..") for p in parts))

    def _consider(self, emitter, bucket, key, size, mtime=None, sclass=None):
        """Decide what to do with one listed object and emit it if it's archived."""
        uri = f"s3://{bucket}/{key}"
        with self._count_lock:
            self._listed += 1
        if key.endswith("/") and not size:
            self._update(skipped=1)                     # "folder" marker object
            return
        rel = self._rel(key)
        if self._unsafe(rel):
            self.logger.warning(f"key can't be used as a file path, skipped: {key!r}")
            self._fail(uri, "key is not usable as a relative file path", retryable=False)
            return
        if sclass in ARCHIVE_CLASSES:
            self._fail(uri, f"source object is in {sclass}: restore it in S3 first")
            return
        if self.skip_keys is not None and rel in self.skip_keys:
            emitter.unchanged += 1                      # already archived (--resume-run)
            return
        if self.watermark is not None and mtime is not None and mtime < self.watermark:
            emitter.unchanged += 1
            if emitter.unchanged >= 2000:
                emitter.report_unchanged()
            return
        emitter.add((uri, rel, int(size)))

    # -- listing -------------------------------------------------------------
    def _list_prefix(self, prefix, work_queue, emitter):
        pager = self._s3.get_paginator("list_objects_v2")
        for page in pager.paginate(Bucket=self.src_bucket, Prefix=prefix, Delimiter="/"):
            for cp in page.get("CommonPrefixes", []):
                work_queue.put(cp["Prefix"])
            for o in page.get("Contents", []):
                self._consider(emitter, self.src_bucket, o["Key"], o["Size"],
                               o["LastModified"].timestamp(), o.get("StorageClass"))
        with self._count_lock:
            self._prefixes += 1

    def _lister(self, work_queue, emitter):
        """Lister thread: one prefix level at a time; sub-prefixes go back on the
        shared queue so siblings spread over all threads."""
        try:
            while True:
                prefix = work_queue.get()
                if prefix is _SCAN_DONE:
                    work_queue.task_done()
                    break
                try:
                    self._list_prefix(prefix, work_queue, emitter)
                except _ScanAborted:
                    raise
                except Exception as e:
                    self.logger.error(f"listing s3://{self.src_bucket}/{prefix} failed: {e}")
                    self._fail(f"s3://{self.src_bucket}/{prefix}",
                               f"listing failed: {type(e).__name__}: {e}", retryable=False)
                finally:
                    work_queue.task_done()
            emitter.flush()
        except _ScanAborted:
            return

    # -- manifests -------------------------------------------------------------
    def _manifest_kind(self):
        if self.manifest_format != "auto":
            return self.manifest_format
        name = self.manifest.lower()
        if name.endswith("manifest.json"):
            return "inventory"
        if name.endswith((".csv", ".csv.gz")):
            return "s3tar"
        return "uris"

    def _open_text(self, location):
        """A local path or s3:// URI, transparently gunzipped, as text lines."""
        if location.startswith("s3://"):
            b, k = parse_s3_uri(location)
            raw = self._s3.get_object(Bucket=b, Key=k)["Body"]
            data = io.BytesIO(raw.read())
            stream = gzip.GzipFile(fileobj=data) if k.endswith(".gz") else data
            return io.TextIOWrapper(stream, encoding="utf-8", newline="")
        if location.endswith(".gz"):
            return gzip.open(location, "rt", encoding="utf-8", newline="")
        return open(location, "r", encoding="utf-8", newline="")

    def _feed_s3tar(self, emitter):
        """s3tar manifest: bucket,key,size[,etag] (no header)."""
        with self._open_text(self.manifest) as f:
            for row in csv.reader(f):
                if not row or row[0].startswith("#") or row[0].lower() == "bucket":
                    continue
                if self._abort.is_set():
                    return
                self._consider(emitter, row[0], row[1], int(row[2]))

    def _feed_inventory(self, emitter):
        """S3 Inventory: manifest.json -> CSV (keys URL-encoded) or Parquet files."""
        with self._open_text(self.manifest) as f:
            man = json.load(f)
        fmt = man.get("fileFormat", "CSV").upper()
        dest = man["destinationBucket"].split(":::")[-1]
        cols = [c.strip() for c in man.get("fileSchema", "").split(",")]
        want = self.src_prefix
        for entry in man["files"]:
            loc = f"s3://{dest}/{entry['key']}"
            if fmt == "CSV":
                ix = {c: i for i, c in enumerate(cols)}
                with self._open_text(loc) as f:
                    for row in csv.reader(f):
                        if self._abort.is_set():
                            return
                        key = unquote(row[ix["Key"]])
                        if want and not key.startswith(want):
                            continue
                        if "IsLatest" in ix and row[ix["IsLatest"]].lower() == "false":
                            continue
                        if "IsDeleteMarker" in ix and row[ix["IsDeleteMarker"]].lower() == "true":
                            continue
                        mtime = None
                        if "LastModifiedDate" in ix and row[ix["LastModifiedDate"]]:
                            from datetime import datetime
                            mtime = datetime.fromisoformat(
                                row[ix["LastModifiedDate"]].replace("Z", "+00:00")).timestamp()
                        self._consider(emitter, row[ix["Bucket"]], key, int(row[ix["Size"]] or 0),
                                       mtime, row[ix["StorageClass"]] if "StorageClass" in ix else None)
            elif fmt == "PARQUET":
                import pyarrow.parquet as pq
                b, k = parse_s3_uri(loc)
                tbl = pq.read_table(io.BytesIO(self._s3.get_object(Bucket=b, Key=k)["Body"].read()))
                names = {n.lower(): n for n in tbl.column_names}
                for r in tbl.to_pylist():
                    if self._abort.is_set():
                        return
                    key = r[names["key"]]
                    if want and not key.startswith(want):
                        continue
                    if "is_latest" in names and r[names["is_latest"]] is False:
                        continue
                    if "is_delete_marker" in names and r[names["is_delete_marker"]]:
                        continue
                    lm = r.get(names.get("last_modified_date", ""), None)
                    self._consider(emitter, r[names["bucket"]], key, int(r[names["size"]] or 0),
                                   lm.timestamp() if lm else None,
                                   r.get(names.get("storage_class", ""), None))
            else:
                raise ValueError(f"S3 Inventory format {fmt} is not supported (use CSV or Parquet)")

    def _feed_uris(self, emitter, work_queue):
        """One s3://bucket/key per line (e.g. a failed-file list): sizes come from
        a HeadObject per object, done by the lister threads."""
        with self._open_text(self.manifest) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    while not self._abort.is_set():
                        try:
                            work_queue.put(line, timeout=0.5)
                            break
                        except queue.Full:
                            continue

    def _header(self, work_queue, emitter):
        try:
            while True:
                uri = work_queue.get()
                if uri is _SCAN_DONE:
                    work_queue.task_done()
                    break
                try:
                    b, k = parse_s3_uri(uri)
                    h = self._s3.head_object(Bucket=b, Key=k)
                    self._consider(emitter, b, k, h["ContentLength"],
                                   h["LastModified"].timestamp(), h.get("StorageClass"))
                except _ScanAborted:
                    raise
                except Exception as e:
                    self._fail(uri, f"HeadObject: {type(e).__name__}: {e}")
                finally:
                    work_queue.task_done()
            emitter.flush()
        except _ScanAborted:
            return

    # -- threads ---------------------------------------------------------------
    def _new_emitter(self):
        return _BatchEmitter(self.batch_queue, self.max_files, self.max_size,
                             self._abort, on_flush=self._on_scan_flush)

    def _start_scanners(self):
        """Start the listing / manifest threads; returns the thread that ends the
        batch stream once everything has been enumerated."""
        threads, feeder = [], None
        kind = self._manifest_kind() if self.manifest else "list"
        if kind in ("list", "uris"):
            # listing: unbounded on purpose (threads both consume and produce it)
            work_queue = queue.Queue() if kind == "list" else queue.Queue(maxsize=10_000)
            target = self._lister if kind == "list" else self._header
            if kind == "list":
                work_queue.put(self.list_prefix)
            else:
                feeder = threading.Thread(target=lambda: self._guard(
                    lambda: self._feed_uris(None, work_queue)), name="manifest-feeder", daemon=True)
                feeder.start()
            for i in range(self.list_threads):
                t = threading.Thread(target=target, args=(work_queue, self._new_emitter()),
                                     name=f"lister-{i}", daemon=True)
                t.start()
                threads.append(t)
        else:
            work_queue = None
            emitter = self._new_emitter()
            fn = self._feed_s3tar if kind == "s3tar" else self._feed_inventory

            def feed():
                try:
                    self._guard(lambda: fn(emitter))
                    emitter.flush()
                except _ScanAborted:
                    return
            t = threading.Thread(target=feed, name="manifest-reader", daemon=True)
            t.start()
            threads.append(t)

        def watch():
            try:
                if feeder is not None:
                    feeder.join()
                if work_queue is not None:
                    work_queue.join()
                    for _ in threads:
                        work_queue.put(_SCAN_DONE)
                for t in threads:
                    t.join()
            finally:
                self.batch_queue.put(_SCAN_DONE)

        watcher = threading.Thread(target=watch, name="scan-watcher", daemon=True)
        watcher.start()
        return watcher

    def _guard(self, fn):
        """Run a manifest reader; a broken manifest fails the run loudly instead
        of silently archiving nothing."""
        try:
            fn()
        except _ScanAborted:
            raise
        except Exception as e:
            self.logger.error(f"reading manifest {self.manifest} failed: {type(e).__name__}: {e}")
            self._fail(self.manifest, f"manifest unreadable: {e}", retryable=False)


def main():
    p = argparse.ArgumentParser(
        description="S3 bucket -> S3 archiver (tar bundles + S3 Tables manifest)")
    src = p.add_argument_group("source")
    src.add_argument("--src-s3", metavar="s3://BUCKET/PREFIX",
                     help="archive every object below this prefix (the prefix is a folder: "
                          "object keys are stored relative to it)")
    src.add_argument("--src-manifest", metavar="FILE",
                     help="archive the objects listed here (local path or s3:// URI): an "
                          "s3tar CSV (bucket,key,size[,etag]), an S3 Inventory manifest.json "
                          "(CSV or Parquet), or one s3://bucket/key per line")
    src.add_argument("--src-manifest-format", choices=["auto", "s3tar", "inventory", "uris"],
                     default="auto", help="auto: manifest.json -> inventory, .csv(.gz) -> "
                                          "s3tar, anything else -> uris")
    src.add_argument("--src-prefix", default=None,
                     help="store keys relative to this prefix. With --src-s3 it must be the "
                          "listed prefix (default) or a parent, e.g. --src-s3 s3://b/data/fs1/ "
                          "--src-prefix data/ stores fs1/... With --src-manifest it also limits "
                          "an inventory to keys below it")
    src.add_argument("--src-region", help="Region of the source bucket (default --region)")
    src.add_argument("--list-threads", type=int, default=16,
                     help="parallel ListObjectsV2 / HeadObject threads (default 16)")
    src.add_argument("--get-concurrency", type=int, default=256,
                     help="concurrent source GETs in total, spread over --num-workers "
                          "processes (default 256). Small objects make the run GET-bound; "
                          "S3 serves about 5,500 GETs/s per prefix partition, and retries "
                          "with backoff absorb 503 SlowDown")
    src.add_argument("--get-client", choices=("crt", "boto3"), default="crt",
                     help="client for the source GETs of objects under 32 MiB: crt (default; "
                          "the AWS CRT S3 client, native code, several times less CPU per "
                          "GET) or boto3. Larger objects always stream through boto3")
    src.add_argument("--verify-source-checksum", action=argparse.BooleanOptionalAction,
                     default=True,
                     help="compare the CRC64NVME of the bytes read with the source object's "
                          "stored full-object CRC64NVME when it has one (default on)")
    add_common_args(p)
    run(S3S3ArchiverMP, p)


if __name__ == "__main__":
    main()
