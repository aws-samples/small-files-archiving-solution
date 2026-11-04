"""
archiver_core.py
================
Shared core of the Small Files Archiver: everything that does not depend on
where the files come from. Two front ends use it:

  fss3-archiver-s3tables-mp.py   local disk / NFS / SMB  (walk or file-list report)
  s3s3-archiver-s3tables-mp.py   an S3 bucket            (listing or manifest)

Both bundle files into tar objects in S3 and record every file's location
(tar key + byte range) and checksums (CRC64NVME, optional MD5) in an Amazon S3
Tables (Apache Iceberg) manifest, with the same run history, incremental,
resume, failed-file list, progress and cancel semantics.

Pipeline:
  main process
    scanner threads (front end) --> batch queue
      --> main thread submits --> ProcessPoolExecutor (N worker processes)
            each worker: read the files (local open / concurrent S3 GETs),
            build the tar with checksums, multipart-upload it, return rows
    main thread collects finished tars --> manifest queue
      --> single writer thread --> batched Iceberg commits

Worker-side code (file opening, S3 prefetch, tar building) lives here rather
than in the front ends because worker processes are started with 'spawn' and
can only import functions from an importable module.

Requires: pip install "pyiceberg[pyarrow,pyiceberg-core]" "boto3[crt]"
"""
import io
import os
import sys
import base64
import signal
import socket
import posixpath
import json
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
# TLS trust store for pyarrow's S3 client
# --------------------------------------------------------------------------- #
_CA_CANDIDATES = (
    "/etc/ssl/certs/ca-certificates.crt",        # Debian / Ubuntu (pyarrow's default)
    "/etc/pki/tls/certs/ca-bundle.crt",          # Amazon Linux / RHEL / Fedora
    "/etc/ssl/ca-bundle.pem",                    # SUSE
    "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem",
)


def ensure_ca_bundle():
    """pyiceberg writes the manifest's data files through pyarrow's own S3 client
    (AWS SDK for C++ + curl), which looks for CA certificates at the Debian path.
    On Amazon Linux / RHEL / SUSE that file doesn't exist and every manifest
    write fails with curl error 60 ("SSL peer certificate ... not OK") while
    boto3 -- with its own bundle -- keeps working. Point SSL_CERT_FILE at the
    system bundle that does exist (certifi's as a last resort). No-op when
    SSL_CERT_FILE is already set or the default path exists."""
    if not sys.platform.startswith("linux") or os.environ.get("SSL_CERT_FILE"):
        return os.environ.get("SSL_CERT_FILE")
    if os.path.exists(_CA_CANDIDATES[0]):
        return None
    for path in _CA_CANDIDATES[1:]:
        if os.path.exists(path):
            os.environ["SSL_CERT_FILE"] = path
            return path
    try:
        import certifi
        os.environ["SSL_CERT_FILE"] = certifi.where()
        return os.environ["SSL_CERT_FILE"]
    except Exception:
        return None


# --------------------------------------------------------------------------- #
# Checksums: CRC64NVME (always) + MD5 (optional), in one pass over the bytes
# --------------------------------------------------------------------------- #
CHECKSUM_ALGORITHM = "CRC64NVME"


def crc64nvme_b64(value: int) -> str:
    """S3's encoding of a CRC64NVME value (as in x-amz-checksum-crc64nvme and S3
    Batch Operations checksum reports): base64 of the 8-byte big-endian CRC."""
    return base64.b64encode(value.to_bytes(8, "big")).decode("ascii")


class ChecksumReader:
    """Wraps a readable stream; computes CRC64NVME (and MD5 if asked) over every
    byte read, so the data is hashed while it is copied into the tar -- no extra
    read. MD5 is a corruption check here, not a security control (FIPS-safe)."""

    def __init__(self, fileobj, md5=False, extra=None):
        from awscrt import checksums as _crt   # fast C implementation used by boto3
        self._crc_fn = _crt.crc64nvme
        self._f = fileobj
        self._crc = 0
        self._md5 = hashlib.md5(usedforsecurity=False) if md5 else None
        # a second algorithm, only to compare with the source object's own
        # checksum when it carries something other than CRC64NVME
        self.extra = extra if extra and extra != CHECKSUM_ALGORITHM else None
        self._x = 0
        self._xfn = None
        if self.extra in ("CRC32", "CRC32C"):
            self._xfn = _crt.crc32 if self.extra == "CRC32" else _crt.crc32c
        elif self.extra in ("SHA1", "SHA256"):
            self._xh = hashlib.new(self.extra.lower(), usedforsecurity=False)

    def read(self, size=-1):
        chunk = self._f.read(size)
        if chunk:
            self._crc = self._crc_fn(chunk, self._crc)
            if self._md5 is not None:
                self._md5.update(chunk)
            if self._xfn is not None:
                self._x = self._xfn(chunk, self._x)
            elif self.extra:
                self._xh.update(chunk)
        return chunk

    def value(self, algo):
        """Checksum of everything read, base64 in S3's encoding, for `algo`."""
        if algo == CHECKSUM_ALGORITHM:
            return self.checksum
        if algo != self.extra:
            return None
        if self._xfn is not None:
            return base64.b64encode(self._x.to_bytes(4, "big")).decode("ascii")
        return base64.b64encode(self._xh.digest()).decode("ascii")

    @property
    def checksum(self) -> str:
        return crc64nvme_b64(self._crc)

    @property
    def md5(self):
        return self._md5.hexdigest() if self._md5 is not None else None


# --------------------------------------------------------------------------- #
# Worker process: build one tar + upload it, return manifest rows.
# _WORKER holds per-process boto3 state created by the pool initializer.
# --------------------------------------------------------------------------- #
_WORKER = {}


# --------------------------------------------------------------------------- #
# botocore CPU savers for many small requests
# --------------------------------------------------------------------------- #
def _fast_timestamp(value):
    """botocore parses every Last-Modified / LastModified with dateutil, which
    profiling showed at ~15% of the CPU of a small-object S3 run. S3 only sends
    two formats: ISO 8601 (XML bodies) and RFC 1123 (headers). Same values,
    parsed with the stdlib; anything else falls back to botocore's parser."""
    if isinstance(value, str):
        try:
            if value.endswith("GMT"):
                from email.utils import parsedate_to_datetime
                return parsedate_to_datetime(value).replace(tzinfo=timezone.utc)
            if "T" in value:
                d = datetime.fromisoformat(value.replace("Z", "+00:00"))
                return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            pass
    from botocore.utils import parse_timestamp
    return parse_timestamp(value)


def fast_session(profile=None):
    """boto3 Session whose clients parse timestamps with _fast_timestamp."""
    import boto3
    session = boto3.Session(profile_name=profile)
    try:
        session._session.get_component("response_parser_factory").set_parser_defaults(
            timestamp_parser=_fast_timestamp)
    except Exception:
        pass                                      # older botocore: default parser
    return session


class _CrtS3:
    """Source GETs for small objects (get) and tar uploads (put) through the
    AWS CRT S3 client.

    Profiling a 523k-object run showed the CPU, not the network, as the limit,
    ~60% of it spent in botocore per request (retry limiter, header parsing,
    urllib3, TLS in Python). The CRT client signs, sends and receives in native
    code on its own event loop: Python only builds the request and joins the
    body. One plain GET per object (S3RequestType.DEFAULT, never split into
    ranges), so S3 returns the object's full checksum header exactly as boto3
    did and the CRC64NVME comparison is unchanged. Retries (5xx, SlowDown,
    connection errors) are done by the CRT with exponential backoff + jitter."""

    def __init__(self, session, region, concurrency, part_size=None, mp_threshold=None):
        from awscrt import io as crt_io, auth, s3 as crt_s3
        from urllib.parse import urlparse
        self._crt_s3, self._auth = crt_s3, auth
        self._region = region
        elg = crt_io.EventLoopGroup()
        bootstrap = crt_io.ClientBootstrap(elg, crt_io.DefaultHostResolver(elg))
        self._creds = session.get_credentials()
        if self._creds is None:
            raise RuntimeError("no AWS credentials for the CRT S3 client")
        endpoint = os.environ.get("AWS_ENDPOINT_URL_S3") or os.environ.get("AWS_ENDPOINT_URL")
        self._endpoint = urlparse(endpoint) if endpoint else None
        tls = (crt_s3.S3RequestTlsMode.DISABLED if self._endpoint and self._endpoint.scheme == "http"
               else crt_s3.S3RequestTlsMode.ENABLED)
        self._client = crt_s3.S3Client(
            bootstrap=bootstrap, region=region, tls_mode=tls,
            credential_provider=auth.AwsCredentialsProvider.new_delegate(self._frozen),
            throughput_target_gbps=10.0, max_active_connections_override=max(1, concurrency),
            part_size=part_size, multipart_upload_threshold=mp_threshold,
            retry_config=crt_s3.S3RetryConfig(max_retries=10))

    def _frozen(self):
        c = self._creds.get_frozen_credentials()       # refreshes role creds as needed
        return self._auth.AwsCredentials(c.access_key, c.secret_key, c.token)

    def _target(self, bucket, key):
        from urllib.parse import quote
        k = quote(key, safe="/~")
        if self._endpoint is not None:                 # custom endpoint: path style
            return self._endpoint.netloc, f"/{bucket}/{k}"
        if "." in bucket:                              # dotted names break TLS on virtual hosts
            return f"s3.{self._region}.amazonaws.com", f"/{bucket}/{k}"
        return f"{bucket}.s3.{self._region}.amazonaws.com", f"/{k}"

    def get(self, uri):
        """Returns a concurrent.futures.Future of (bytes, meta)."""
        from concurrent.futures import Future
        from awscrt.http import HttpHeaders, HttpRequest
        bucket, key = _parse_s3_uri(uri)
        host, path = self._target(bucket, key)
        fut, hdrs, chunks = Future(), {}, []

        def on_headers(status_code, headers, **_):
            if 200 <= status_code < 300:
                hdrs.clear()
                hdrs.update((k.lower(), v) for k, v in headers)

        def on_body(chunk, offset, **_):
            chunks.append(bytes(chunk))

        def on_done(error=None, error_headers=None, error_body=None, status_code=None, **_):
            if fut.done():                              # cancelled: the run is stopping
                return
            try:
                if error is not None:
                    fut.set_exception(_CrtGetError(status_code, error_body, error))
                    return
                body = b"".join(chunks)
                src = None
                if hdrs.get("x-amz-checksum-type", "FULL_OBJECT") == "FULL_OBJECT":
                    for algo in ("CRC64NVME", "CRC32C", "CRC32", "SHA256", "SHA1"):
                        v = hdrs.get(f"x-amz-checksum-{algo.lower()}")
                        if v:
                            src = (algo, v)
                            break
                lm = hdrs.get("last-modified")
                meta = {"mtime": _fast_timestamp(lm).timestamp() if lm else time.time(),
                        "src_crc": src, "length": int(hdrs.get("content-length", len(body)))}
                if meta["length"] != len(body):
                    fut.set_exception(IOError(f"short read: got {len(body)} of {meta['length']} bytes"))
                else:
                    fut.set_result((body, meta))
            except Exception as e:                      # never lose a result
                if not fut.done():
                    fut.set_exception(e)

        req = HttpRequest("GET", path, HttpHeaders([("Host", host), ("x-amz-checksum-mode", "ENABLED")]))
        self._client.make_request(type=self._crt_s3.S3RequestType.DEFAULT, request=req,
                                  operation_name="GetObject", on_headers=on_headers,
                                  on_body=on_body, on_done=on_done)
        return fut


    def put(self, bucket, key, fileobj, size, storage_class, on_progress=None):
        """Upload a tar (single PUT up to the part size, multipart above it) with
        a CRC64NVME checksum per request: S3 rejects any part whose bytes don't
        match. Blocks until done; raises _CrtGetError on failure."""
        from awscrt.http import HttpHeaders, HttpRequest
        crt_s3 = self._crt_s3
        host, path = self._target(bucket, key)
        headers = [("Host", host), ("Content-Length", str(size))]
        if storage_class and storage_class != "STANDARD":
            headers.append(("x-amz-storage-class", storage_class))
        req = HttpRequest("PUT", path, HttpHeaders(headers), body_stream=fileobj)
        cfg = crt_s3.S3ChecksumConfig(algorithm=crt_s3.S3ChecksumAlgorithm.CRC64NVME,
                                      location=crt_s3.S3ChecksumLocation.TRAILER)
        err = {}

        def on_done(error=None, error_body=None, status_code=None, **_):
            if error is not None:
                err["e"] = _CrtGetError(status_code, error_body, error)

        r = self._client.make_request(
            type=crt_s3.S3RequestType.PUT_OBJECT, request=req, checksum_config=cfg,
            on_progress=(lambda progress, **_: on_progress(progress)) if on_progress else None,
            on_done=on_done)
        try:
            r.finished_future.result()
        except Exception as e:
            raise err.get("e") or e
        if "e" in err:
            raise err["e"]


_CrtGetter = _CrtS3


def tar_crc64nvme(fileobj, chunk=8 << 20):
    """CRC64NVME of a whole file object (from the start), in S3's encoding."""
    from awscrt.checksums import crc64nvme
    fileobj.seek(0)
    crc = 0
    while True:
        b = fileobj.read(chunk)
        if not b:
            break
        crc = crc64nvme(b, crc)
    return crc64nvme_b64(crc)


def _verify_uploaded_tar(s3, bucket, key, size, want):
    """End-to-end check of one tar: the full-object CRC64NVME S3 stored must be
    the CRC64NVME of the bytes we packed. Returns True (verified), None (no
    s3:GetObject on the tar: can't check) or raises on a mismatch."""
    try:
        h = s3.head_object(Bucket=bucket, Key=key, ChecksumMode="ENABLED")
    except Exception as e:
        code = getattr(e, "response", {}).get("Error", {}).get("Code", "")
        if code in ("403", "AccessDenied", "Forbidden"):
            return None
        raise
    got = h.get("ChecksumCRC64NVME")
    if h.get("ContentLength") != size or (got and got != want):
        raise ValueError(f"uploaded tar doesn't match what was packed: size {h.get('ContentLength')} "
                         f"vs {size}, CRC64NVME {got} vs {want}. Its rows are not recorded; "
                         f"remove s3://{bucket}/{key}")
    return True if got else None


class _CrtGetError(Exception):
    """A failed CRT GET, with the S3 error code so _s3_error words it the same
    way as for boto3 (missing object, archive storage class, ...)."""

    def __init__(self, status, body, error):
        import re
        text = bytes(body or b"").decode("utf-8", "replace")
        m = re.search(r"<Code>([^<]+)</Code>", text)
        self.code = m.group(1) if m else (str(status) if status else "")
        self.response = {"Error": {"Code": self.code}}
        super().__init__(f"GET failed: HTTP {status} {self.code} ({error})")


def _init_worker(profile, region, storageclass, mp_threshold, mp_chunksize, upload_conc,
                 progress_q=None, src_region=None, get_concurrency=0, get_client="boto3",
                 upload_client="boto3", verify_tar=True):
    from botocore.config import Config as BotoConfig
    from boto3.s3.transfer import TransferConfig
    _WORKER["progress_q"] = progress_q
    # Only the main process decides how to cancel: a terminal Ctrl-C reaches the
    # whole process group, and a worker dying mid-upload would orphan its tar.
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    session = fast_session(profile)
    _WORKER["s3"] = session.client("s3", region_name=region)
    _WORKER["storageclass"] = storageclass
    _WORKER["tconf"] = TransferConfig(
        multipart_threshold=mp_threshold,
        multipart_chunksize=mp_chunksize,
        max_concurrency=upload_conc,
        use_threads=True,
    )
    _WORKER["get_concurrency"] = max(1, get_concurrency)
    _WORKER["verify_tar"] = verify_tar
    _WORKER["crt_put"] = (_CrtS3(session, region, upload_conc, part_size=mp_chunksize,
                                 mp_threshold=mp_threshold) if upload_client == "crt" else None)
    if get_concurrency:
        # source reads: a connection per in-flight GET. "standard" retries back
        # off on 503 SlowDown / 5xx with jitter; "adaptive" added a client-side
        # rate limiter whose lock the GET threads fought over (~20% of the CPU).
        _WORKER["src_s3"] = session.client(
            "s3", region_name=src_region or region,
            config=BotoConfig(max_pool_connections=get_concurrency + 8,
                              retries={"mode": "standard", "max_attempts": 10}))
        _WORKER["crt"] = None
        if get_client == "crt":
            _WORKER["crt"] = _CrtGetter(session, src_region or region, get_concurrency)


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


def _open_local(full, rel, tar):
    """Open a local file for archiving without following symlinks, and build the
    tar header from the *opened* file: a path swapped for a symlink (or FIFO /
    device) after the scan is refused instead of archiving whatever it now
    points at. Returns (tarinfo, fileobj)."""
    fd = os.open(full, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    f = os.fdopen(fd, "rb")
    ti = tar.gettarinfo(arcname=rel, fileobj=f)
    if not ti.isreg():
        f.close()
        raise OSError(f"not a regular file when opened: {full}")
    return ti, f, None


def _parse_s3_uri(uri):
    rest = uri[5:]
    bucket, _, key = rest.partition("/")
    return bucket, key


class _S3Prefetcher:
    """Fetch the source objects of one batch with many concurrent GETs and hand
    them back in batch order, so the tar is still written sequentially.

    Small objects are read fully into memory by a thread pool; the window of
    objects fetched ahead is bounded by count (get concurrency x 2) and bytes
    (PREFETCH_BYTES), so memory per worker stays flat whatever the batch size.
    Objects of STREAM_MIN or more are not prefetched: they are GET in order and
    streamed straight into the tar."""
    PREFETCH_BYTES = 128 << 20
    STREAM_MIN = 32 << 20

    def __init__(self, items):
        from concurrent.futures import ThreadPoolExecutor
        self._s3 = _WORKER["src_s3"]
        self._crt = _WORKER.get("crt")
        self._items = items
        # the CRT client runs its own native event loop: no Python GET threads
        self._pool = None if self._crt else ThreadPoolExecutor(
            max_workers=_WORKER["get_concurrency"], thread_name_prefix="get")

    def _get(self, uri, stream=False):
        bucket, key = _parse_s3_uri(uri)
        resp = self._s3.get_object(Bucket=bucket, Key=key, ChecksumMode="ENABLED")
        src = None
        # a whole-object checksum can be compared with the bytes; a COMPOSITE one
        # (checksum of part checksums, multipart uploads) cannot
        if resp.get("ChecksumType", "FULL_OBJECT") == "FULL_OBJECT":
            for algo in ("CRC64NVME", "CRC32C", "CRC32", "SHA256", "SHA1"):
                if resp.get(f"Checksum{algo}"):
                    src = (algo, resp[f"Checksum{algo}"])
                    break
        meta = {"mtime": resp["LastModified"].timestamp(), "src_crc": src,
                "length": resp["ContentLength"]}
        if stream:
            return resp["Body"], meta
        return resp["Body"].read(), meta

    def __iter__(self):
        from collections import deque
        window, inflight_bytes, it = deque(), 0, iter(self._items)
        limit = _WORKER["get_concurrency"] * 2
        exhausted = False
        try:
            while True:
                # top up the window: small objects only, bounded by count + bytes
                while not exhausted and len(window) < limit:
                    try:
                        item = next(it)
                    except StopIteration:
                        exhausted = True
                        break
                    size = item[2]
                    if size >= self.STREAM_MIN:
                        window.append((item, None))          # streamed in order
                        break
                    window.append((item, self._crt.get(item[0]) if self._crt
                                   else self._pool.submit(self._get, item[0])))
                    inflight_bytes += size
                    if inflight_bytes >= self.PREFETCH_BYTES:
                        break
                if not window:
                    return
                item, fut = window.popleft()
                if fut is None:
                    try:
                        yield item, self._get(item[0], stream=True), None
                    except Exception as e:
                        yield item, None, e
                else:
                    inflight_bytes -= item[2]
                    try:
                        yield item, fut.result(), None
                    except Exception as e:
                        yield item, None, e
        finally:
            for _, fut in window:
                if fut is not None:
                    fut.cancel()
            if self._pool is not None:
                self._pool.shutdown(wait=False, cancel_futures=True)


def _s3_error(e):
    code = getattr(e, "response", {}).get("Error", {}).get("Code", "")
    if code == "InvalidObjectState":
        return "source object is in an archive storage class: restore it in S3 first"
    if code in ("NoSuchKey", "404"):
        return "source object no longer exists"
    return f"{type(e).__name__}: {e}"


def _process_batch(task):
    """Runs in a worker process. task keys:
       files [(source, rel, size)...] where source is a local path or an
       s3://bucket/key URI, tar_key, bucket, compress, do_md5, spool_max,
       verify_src (S3 sources: compare with the object's stored CRC64NVME).
       Raises on upload failure so the main process attributes the whole batch as
       failed. Skips the upload entirely if no member packed."""
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
    from_s3 = bool(task["files"]) and task["files"][0][0].startswith("s3://")

    def add_member(tar, ti, fobj, src, rel, size, src_crc):
        start_pos = spool.tell()
        reader = ChecksumReader(fobj, md5=task["do_md5"], extra=src_crc[0] if src_crc else None)
        tar.addfile(ti, reader)
        if task.get("verify_src", True) and src_crc:
            algo, want = src_crc
            got = reader.value(algo)
            if got != want:
                # the bytes we archived differ from what S3 says the object
                # holds: drop the member's row (its tar bytes are never referenced)
                raise ValueError(f"{algo} mismatch with the source object "
                                 f"(source {want}, read {got})")
        rows.append({
            "tar_key": task["tar_key"],
            "source_key": rel,
            "byte_start": start_pos if offsets_valid else -1,
            "byte_end": spool.tell() - 1 if offsets_valid else -1,
            "md5": reader.md5,
            "filesize": ti.size,
            "archive_ts": archive_ts,
            "parent_dir": posixpath.dirname(rel),
            "checksum_algorithm": CHECKSUM_ALGORITHM,
            "checksum": reader.checksum,
        })

    try:
        with tarfile.open(fileobj=spool, mode=mode) as tar:
            if from_s3:
                source = _S3Prefetcher(task["files"])
                for (src, rel, size), got, err in source:
                    packed_bytes += size
                    ping.emit("bundling", packed_bytes, nbytes_in, files=len(rows) + failed + 1)
                    try:
                        if err is not None:
                            raise err
                        payload, meta = got
                        ti = tarfile.TarInfo(rel)
                        ti.size, ti.mtime, ti.mode, ti.type = meta["length"], meta["mtime"], 0o644, tarfile.REGTYPE
                        fobj = io.BytesIO(payload) if isinstance(payload, bytes) else payload
                        try:
                            add_member(tar, ti, fobj, src, rel, size, meta["src_crc"])
                        finally:
                            if not isinstance(payload, bytes):
                                payload.close()
                    except Exception as e:
                        failed += 1
                        failed_files.append((src, _s3_error(e)))
            else:
                for src, rel, size in task["files"]:
                    packed_bytes += size
                    ping.emit("bundling", packed_bytes, nbytes_in, files=len(rows) + failed + 1)
                    try:
                        ti, f, _ = _open_local(src, rel, tar)
                        with f:
                            add_member(tar, ti, f, src, rel, size, None)
                    except Exception as e:
                        failed += 1
                        failed_files.append((src, f"{type(e).__name__}: {e}"))

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
        tar_crc = tar_crc64nvme(spool) if _WORKER.get("verify_tar", True) else None
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
                cb = ping.upload_cb(tar_size) if ping._q is not None else None
                if _WORKER.get("crt_put") is not None:
                    _WORKER["crt_put"].put(task["bucket"], task["tar_key"].lstrip("/"), spool,
                                           tar_size, sclass, on_progress=cb)
                else:
                    s3.upload_fileobj(
                        spool, task["bucket"], task["tar_key"].lstrip("/"),
                        ExtraArgs={"StorageClass": sclass, "ChecksumAlgorithm": "CRC64NVME"},
                        Config=tconf, Callback=cb)
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
        if tar_crc is not None:
            ok = _verify_uploaded_tar(s3, task["bucket"], task["tar_key"].lstrip("/"), tar_size, tar_crc)
            tar_verified = bool(ok)
        else:
            tar_verified = False
        ping.emit("uploaded", tar_size, tar_size, force=True)
    finally:
        spool.close()

    total = sum(r["filesize"] for r in rows)
    part = tconf.multipart_chunksize
    # PUT-class requests for this tar: one PutObject, or Create + N x UploadPart
    # + Complete for a multipart upload (boto3 goes multipart above the threshold)
    nreq = 1 if tar_size <= tconf.multipart_threshold else -(-tar_size // part) + 2
    return {"tar_key": task["tar_key"], "rows": rows, "failed_files": failed_files,
            "requests": nreq, "gets": len(task["files"]) if from_s3 else 0,
            "tar_verified": tar_verified,
            "failed": failed, "total_bytes": total, "count": len(rows)}


# --------------------------------------------------------------------------- #
# Manifest writer: single thread, batched Iceberg append into S3 Tables.
# --------------------------------------------------------------------------- #
# Table properties that make a manifest self-describing: the GUI's "Connect an
# existing archive" reads the tar bucket from here.
TAR_BUCKET_PROPERTY = "sfa.tar_bucket"
FORMAT_PROPERTY = "sfa.format"
MANIFEST_COLUMNS = ("tar_key", "source_key", "byte_start", "byte_end")


def table_properties(tar_bucket=None):
    props = {FORMAT_PROPERTY: "small-files-archive-manifest/1"}
    if tar_bucket:
        props[TAR_BUCKET_PROPERTY] = tar_bucket
    return props


MANIFEST_ARROW_SCHEMA = pa.schema([
    pa.field("tar_key",    pa.string(),                  nullable=False),
    pa.field("source_key", pa.string(),                  nullable=False),
    pa.field("byte_start", pa.int64(),                   nullable=False),
    pa.field("byte_end",   pa.int64(),                   nullable=False),
    pa.field("md5",        pa.string(),                  nullable=True),
    pa.field("filesize",   pa.int64(),                   nullable=False),
    pa.field("archive_ts", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("parent_dir", pa.string(),                  nullable=True),
    pa.field("checksum_algorithm", pa.string(),          nullable=True),
    pa.field("checksum",   pa.string(),                  nullable=True),
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
    # Per-file checksum in S3's own encoding (base64 CRC64NVME, as in
    # x-amz-checksum-crc64nvme and S3 Batch Operations checksum reports).
    NestedField(9, "checksum_algorithm", StringType(), required=False),
    NestedField(10, "checksum",  StringType(),      required=False),
)

# columns added after the first release; older tables gain them by additive
# schema evolution, and rows are written without any a table still lacks
OPTIONAL_COLUMNS = {
    "parent_dir": "directory of source_key ('' = root)",
    "checksum_algorithm": "algorithm of the checksum column (CRC64NVME)",
    "checksum": "per-file checksum, base64, S3 encoding",
}

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
                 retry_cap=30.0, dir_index=True, run_id=None, tar_bucket=None):
        super().__init__(name="manifest-writer", daemon=False)
        self._tar_bucket = tar_bucket
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
        self._missing_cols = []
        # set once the table is loaded AND its storage is reachable through the
        # same client the commits use -- or once that failed (then .error is set)
        self.ready = threading.Event()
        self.rows_committed = 0
        self.commit_count = 0
        self.conflict_count = 0
        self.dir_rows_committed = 0
        self.dir_index_failures = 0
        self._error = None

    def _record_tar_bucket(self, tbl, ident):
        """Record the tar bucket in the table's properties once, so the GUI can
        connect to an existing archive without asking for it. Best effort: a
        failure (e.g. no permission to update table metadata) never stops a run."""
        if not self._tar_bucket:
            return tbl
        have = tbl.properties.get(TAR_BUCKET_PROPERTY)
        if have == self._tar_bucket:
            return tbl
        if have:
            self._log.info(f"{ident} records tar bucket {have}; this run writes to "
                           f"{self._tar_bucket} (rows hold keys only, so keep track of both)")
            return tbl
        try:
            with tbl.transaction() as tx:
                tx.set_properties(**table_properties(self._tar_bucket))
            self._log.info(f"Recorded tar bucket {self._tar_bucket} in {ident}")
            return tbl.refresh()
        except Exception as e:
            self._log.info(f"could not record the tar bucket in {ident} (not needed for "
                           f"archiving): {type(e).__name__}: {e}")
            return tbl

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
                                   transform=DayTransform(), name="archive_ts_day")),
                properties=table_properties(self._tar_bucket))
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

        tbl = self._record_tar_bucket(tbl, ident)
        for col, doc in OPTIONAL_COLUMNS.items():
            if col == "parent_dir" and not self._dir_index:
                continue
            self._ensure_col(tbl, ident, col, doc)
        if self._dir_index:
            self._dirs_tbl = self._ensure_dirs_table(catalog)
        present = set(tbl.schema().column_names)
        self._missing_cols = [c for c in OPTIONAL_COLUMNS if c not in present]
        return tbl

    def _ensure_col(self, tbl, ident, col, doc):
        """Additive schema evolution for tables created before `col` existed.

        Concurrent runs may race to add it; a loser just re-checks."""
        for _ in range(3):
            tbl.refresh()
            if col in tbl.schema().column_names:
                return
            try:
                with tbl.update_schema() as upd:
                    upd.add_column(col, StringType(), doc=doc)
                self._log.info(f"Added optional column {col} to {ident}")
                return
            except Exception as e:
                self._log.warning(f"adding {col} to {ident} failed, "
                                  f"re-checking -- {type(e).__name__}: {e}")
                time.sleep(1 + random.random())
        tbl.refresh()
        if col not in tbl.schema().column_names:
            self._log.warning(f"{ident} has no {col} column; rows are written without it")

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
        if self._missing_cols:
            arrow = arrow.drop_columns(self._missing_cols)
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

    def _preflight(self, tbl):
        """Read the table's current metadata file through the table's own FileIO
        (pyarrow's S3 client) -- the path every commit takes. A broken trust
        store, endpoint or permission then stops the run before the first tar
        is uploaded, instead of after many orphan tars."""
        with tbl.io.new_input(tbl.metadata_location).open() as f:
            f.read(16)

    def run(self):
        try:
            tbl = self._ensure_table()
            try:
                self._preflight(tbl)
            except Exception as e:
                raise RuntimeError(f"manifest storage not reachable through pyarrow's S3 client "
                                   f"({type(e).__name__}: {e}); on Linux check the CA bundle "
                                   f"(SSL_CERT_FILE)") from e
            self.ready.set()
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
        finally:
            self.ready.set()

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

    def report_unchanged(self):
        if self.unchanged and self._on_flush is not None:
            self._on_flush(0, 0, self.unchanged)
            self.unchanged = 0

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
    loc = (lambda p: p.rstrip("/")) if str(src_path).startswith("s3://") else \
        (lambda p: os.path.abspath(p).rstrip(os.sep))
    key = {"src": loc(src_path),
           "walk": sorted(loc(w) for w in (walk_paths or []))}
    if input_file:
        key["input"] = input_file if str(input_file).startswith("s3://") else os.path.abspath(input_file)
    return hashlib.sha1(json.dumps(key, sort_keys=True).encode(),
                        usedforsecurity=False).hexdigest()[:16]


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

    @staticmethod
    def _private(path):
        return open(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600),
                    "w", encoding="utf-8", errors="surrogateescape")

    def add(self, path, reason, retryable=True):
        reason = " ".join(str(reason).split())
        if any(c in path for c in "\n\r\t"):
            # a line break in a file name would split into extra paths when the
            # list is read back -- never put such a name into the retry list
            retryable = False
            reason += " [name contains a line break or tab; not in the retry list]"
            shown = path.encode("unicode_escape", "backslashreplace").decode("ascii")
        else:
            shown = path
        with self._lock:
            if self._fv is None:
                self._ft = self._private(self.txt)
                self._fv = self._private(self.tsv)
                self._fv.write("path\treason\n")
            if retryable:
                self._ft.write(path + "\n")
            self._fv.write(f"{shown}\t{reason}\n")
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


class _LogTail(logging.Handler):
    """Keeps the last log lines, numbered, for the remote status object."""

    def __init__(self, keep=200):
        super().__init__()
        from collections import deque
        self.lines = deque(maxlen=keep)
        self.seq = 0
        self._lk = threading.Lock()

    def emit(self, record):
        try:
            line = self.format(record)
        except Exception:
            return
        with self._lk:
            self.seq += 1
            self.lines.append((self.seq, line))

    def tail(self, n=100):
        with self._lk:
            return list(self.lines)[-n:]


class StatusS3:
    """Writes the progress snapshot to an S3 object (--status-s3) so a GUI on
    another machine can follow a run on EC2: at most every INTERVAL seconds,
    plus a final record with the run's status and the failed list's S3 copy."""
    INTERVAL = 5.0

    def __init__(self, uri, region, profile, logtail):
        import boto3
        self.bucket, _, self.key = uri[5:].partition("/")
        self.prefix = self.key.rsplit("/", 1)[0] if "/" in self.key else ""
        self._s3 = boto3.Session(profile_name=profile).client("s3", region_name=region)
        self._tail = logtail
        self._last_put = 0.0
        self.last = {}
        self.host = socket.gethostname()
        self.pid = os.getpid()

    def update(self, snap, force=False):
        self.last = snap
        now = time.time()
        if not force and now - self._last_put < self.INTERVAL:
            return
        self._last_put = now
        self._put(snap)

    def _put(self, snap):
        body = dict(snap, host=self.host, pid=self.pid, updated=datetime.now(timezone.utc).isoformat(),
                    log=self._tail.tail() if self._tail else [])
        try:
            self._s3.put_object(Bucket=self.bucket, Key=self.key, ContentType="application/json",
                                Body=json.dumps(body, default=str).encode())
        except Exception:
            pass        # status is best effort; the run itself must not fail on it

    def upload_failed(self, failed_list):
        """Copy the failed list next to the status object; returns its s3 URI."""
        if not failed_list.written:
            return None
        out = None
        for local, name in ((failed_list.txt, "failed.txt"), (failed_list.tsv, "failed.tsv")):
            key = f"{self.prefix}/{name}" if self.prefix else name
            try:
                self._s3.upload_file(local, self.bucket, key)
                if name == "failed.txt":
                    out = f"s3://{self.bucket}/{key}"
            except Exception:
                pass
        return out

    def final(self, status, failed_s3):
        snap = dict(self.last or {}, ev="done", status=status, failed_list_s3=failed_s3)
        snap["ok"] = status in ("ok", "partial")
        self._put(snap)


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
        if a.status_s3 is not None:
            a.status_s3.update(snap, force=final)
        if a.args.progress:
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
class ArchiverBase:
    """The archive pipeline, independent of where the files come from.

    A front end (local disk / S3) subclasses it and provides:
      _init_source(args)     validate + store source options, set source_id
      _start_scanners()      threads that push batches of (source, rel, size)
                             onto self.batch_queue and end with _SCAN_DONE
      _history_fields()      src_path / walk_paths / input_file for _runs
      _source_desc()         one line for the start + summary logs
      _summary_source()      extra summary lines (dirs scanned, prefixes listed)
      _worker_source()       (source region, GET concurrency) for the workers
    """
    retry_hint = "retry: --input-file it with the same --src-path"

    def _init_source(self, args):
        raise NotImplementedError

    def _start_scanners(self):
        raise NotImplementedError

    def _history_fields(self):
        raise NotImplementedError

    def _source_desc(self):
        return ""

    def _summary_source(self):
        return []

    def _worker_source(self):
        return None, 0

    def __init__(self, args):
        self.args = args
        self.status_s3 = None
        self.dst_bucket = args.dst_bucket
        self.dst_prefix = args.dst_prefix.rstrip("/")
        self.incremental = getattr(args, "incremental", False)
        self.resume_runs = list(getattr(args, "resume_run", None) or [])
        self.skip_keys = None      # source_keys already archived by the resumed run(s)
        self.watermark = None        # epoch seconds; files older than this are unchanged
        self.watermark_iso = None
        self._init_source(args)
        self.max_files = args.max_files
        self.max_size = args.max_size
        self.num_workers = args.num_workers
        self.compress = args.compress
        self.do_md5 = args.md5
        self.spool_max = args.spool_max_mb * 1024 * 1024
        self.batch_size = args.batch_size
        self.current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.mid_prefix = self.current_time.split("_")[0]
        self.tar_layout = getattr(args, "tar_layout", "run")
        # multipart part size: also the threshold, so a tar no larger than one
        # part goes up as a single PUT
        self.part_size = parse_size(getattr(args, "part_size", "32MB") or "32MB")
        self._requests = 0
        self._gets = 0
        self._tars_verified = self._tars_unverified = 0
        self.upload_client = getattr(args, "upload_client", "boto3")
        self.verify_tar = getattr(args, "verify_tar_upload", True)
        if self.upload_client == "crt":
            try:
                import awscrt.s3  # noqa: F401
            except ImportError:
                self.upload_client = "boto3"

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

        # logs and failed lists hold full source paths: keep them private
        os.makedirs(os.path.join("logs", self.dst_prefix), mode=0o700, exist_ok=True)
        self.logger = logging.getLogger("sfa-archiver")
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
            # run_id (not current_time) so concurrent runs sharing a --dst-prefix
            # don't write into the same log file.
            log_path = os.path.join("logs", self.dst_prefix, f"archiver_{self.run_id}.log")
            os.close(os.open(log_path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600))
            fh = logging.FileHandler(log_path)
            ch = logging.StreamHandler()
            self._logtail = _LogTail()
            self._logtail.setFormatter(fmt)
            self.logger.addHandler(self._logtail)
            for h in (fh, ch):
                h.setFormatter(fmt)
                self.logger.addHandler(h)

        if getattr(args, "status_s3", None):
            self.status_s3 = StatusS3(args.status_s3, args.region, args.profile_name,
                                      getattr(self, "_logtail", None))
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

    def _archived_by(self, run_ids):
        """source_keys already in the manifest from tars of these runs, for any
        --tar-layout (run folder, date folder or flat). Held in memory as a set:
        about 100 bytes per file, so a resumed run of N million files needs
        roughly N x 100 MB -- fine for interrupted runs, not meant for billions."""
        from pyiceberg.expressions import StartsWith, Or
        tbl = load_catalog("s3tables", **self.catalog_props).load_table(
            f"{self.args.namespace}.{self.args.table_name}")
        keys = set()
        for rid in run_ids:
            name = f"archive_{rid}_"
            prefixes = [f"{self.dst_prefix}/{rid}/{name}", f"{self.dst_prefix}/{rid[:8]}/{name}",
                        f"{self.dst_prefix}/{name}"]
            flt = None
            for pre in prefixes + ["/" + x for x in prefixes]:
                e = StartsWith("tar_key", pre)
                flt = e if flt is None else Or(flt, e)
            for b in tbl.scan(row_filter=flt, selected_fields=("source_key",)).to_arrow_batch_reader():
                keys.update(b.column(0).to_pylist())
        return keys

    def _cancel_drain(self, inflight, collect):
        """Cancel without orphans. Tars not started yet are dropped (their files
        go to the failed list as 'not uploaded: run cancelled', so a retry of
        the list resumes them); tars already being built or uploaded are waited
        for and their manifest rows committed, so every tar that reaches S3 is
        findable. A second interrupt during this wait skips it (force stop)."""
        self._abort.set()
        dropped = [f for f in list(inflight) if f.cancel()]
        nfiles = 0
        for f in dropped:
            inflight.discard(f)
            self._inflight_tars.discard(getattr(f, "_tar_key", None))
            for full, _rel, _sz in getattr(f, "_files", []):
                self.failed_list.add(full, "not uploaded: run cancelled")
                nfiles += 1
        self.logger.warning(
            f"cancel: dropped {len(dropped)} queued tar(s) ({nfiles:,} files, listed in the "
            f"failed list for a retry); finishing {len(inflight)} tar(s) already in progress "
            f"so their manifest rows are committed -- interrupt again to force-stop")
        for f in list(inflight):
            collect(f)
            inflight.discard(f)
        self.logger.warning("cancel: in-progress tars finished and recorded")

    def _tar_key(self, batch_no, ext):
        """S3 key of one tar. The file name always carries the run id, so a tar
        stays unique even when copied out of its folder; --tar-layout only
        decides the folder level between the prefix and the file:
          run   <prefix>/<run-id>/archive_<run-id>_00001.tar  (default) one
                folder per run: a run is checked, reconciled or cleaned up as a
                unit, and incremental runs never mix with earlier ones
          date  <prefix>/<YYYYMMDD>/archive_<run-id>_00001.tar  (pre-v3.1)
          flat  <prefix>/archive_<run-id>_00001.tar
        Restore never depends on the layout: the manifest stores full keys."""
        name = f"archive_{self.run_id}_{batch_no:05d}{ext}"
        if self.tar_layout == "flat":
            return f"{self.dst_prefix}/{name}"
        folder = self.run_id if self.tar_layout == "run" else self.mid_prefix
        return f"{self.dst_prefix}/{folder}/{name}"

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
        if self.resume_runs:
            self.skip_keys = self._archived_by(self.resume_runs)
            self.logger.info(f"resume: {len(self.skip_keys):,} files already archived by run(s) "
                             f"{', '.join(self.resume_runs)} will be skipped")
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
            if self.status_s3 is not None:
                self.status_s3.final(status, self.status_s3.upload_failed(self.failed_list))
            if self.failed_list.written:
                self.logger.info(f"Failed-file list: {self.failed_list.txt} "
                                 f"({self.retry_hint}); "
                                 f"reasons in {self.failed_list.tsv}")
            if history is not None:
                try:
                    history.record({
                        "run_id": self.run_id, "source_id": self.source_id,
                        **self._history_fields(),
                        # an incremental run without a baseline archived everything
                        "mode": "resume" if self.resume_runs else
                                ("incremental" if (self.incremental and self.watermark_iso)
                                 else "full"),
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
                                dir_index=self.args.dir_index, run_id=self.run_id,
                                tar_bucket=self.dst_bucket)
        writer.start()
        # nothing is read or uploaded until the manifest is known to be writable
        if not writer.ready.wait(timeout=300) or writer.error is not None:
            self.manifest_queue.put(ManifestWriter.POISON)
            raise RuntimeError(f"manifest not usable, nothing was archived: "
                               f"{writer.error or 'table setup timed out'}")

        progress_q = None
        if self.args.progress or self.status_s3 is not None:
            # bounded: workers put_nowait and drop events rather than block
            progress_q = mp_ctx.Queue(maxsize=10_000)
            self.progress = ProgressReporter(self, progress_q)
            self.progress.writer = writer
            self.progress.start()

        self.logger.info(
            f"Starting: run_id={self.run_id}, workers={self.num_workers}, "
            f"{self._source_desc()}, checksum={CHECKSUM_ALGORITHM}"
            f"{' + MD5' if self.do_md5 else ''}, compress={self.compress}")

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
                self._requests += res.get("requests", 0)
                self._gets += res.get("gets", 0)
                if res["count"]:
                    if res.get("tar_verified"):
                        self._tars_verified += 1
                    else:
                        self._tars_unverified += 1
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
                              self.part_size, self.part_size,
                              self.args.upload_concurrency, progress_q,
                              *(tuple(self._worker_source()) + (None, 0, "boto3"))[:3],
                              self.upload_client, self.verify_tar)) as ex:
                inflight = set()
                batch_no = 0
                try:
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
                        tar_key = self._tar_key(batch_no, ext)
                        task = {"files": batch, "tar_key": tar_key,
                                "bucket": self.dst_bucket, "compress": self.compress,
                                "do_md5": self.do_md5, "spool_max": self.spool_max,
                                "verify_src": getattr(self, "verify_src", True)}
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
                            done, _ = wait(inflight, return_when=FIRST_COMPLETED)
                            for fut in done:
                                collect(fut)
                                inflight.discard(fut)   # only once its rows are queued
                    # drain remaining
                    for fut in list(inflight):
                        collect(fut)
                        inflight.discard(fut)
                except KeyboardInterrupt:
                    self._cancel_drain(inflight, collect)
                    raise

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
        self.logger.info(f"Files skipped   : {self._skipped:,} (symlinks / special files / "
                         f"folder markers)")
        if self.resume_runs:
            self.logger.info(f"Already archived: {self._unchanged:,} (skipped, resume of "
                             f"{', '.join(self.resume_runs)})")
        elif self.incremental:
            self.logger.info(f"Unchanged       : {self._unchanged:,} (older than the "
                             f"watermark {self.watermark_iso or '-'})")
        for line in self._summary_source():
            self.logger.info(line)
        self.logger.info(f"Tar objects     : {self._tars:,}")
        self.logger.info(f"Manifest rows   : {writer.rows_committed:,} "
                         f"in {writer.commit_count} commits "
                         f"({writer.conflict_count} commit retries)")
        if self.args.dir_index:
            self.logger.info(f"Dir-index rows  : {writer.dir_rows_committed:,} "
                             f"({writer.dir_index_failures} failed commits)")
        self.logger.info(f"Checksums       : {CHECKSUM_ALGORITHM}"
                         f"{' + MD5' if self.do_md5 else ''}")
        self.logger.info(f"Workers         : {self.num_workers} ({self._source_desc()})")
        if self._gets:
            self.logger.info(f"Source GETs     : {self._gets:,}")
        self.logger.info(f"Bytes           : {self._human(self._bytes)}")
        self.logger.info(f"Upload requests : {self._requests:,} PUT-class "
                         f"(part size {self._human(self.part_size)})")
        if self.verify_tar:
            self.logger.info(f"Tar check       : {self._tars_verified:,} tars verified end to end "
                             f"(CRC64NVME stored by S3 == packed bytes)"
                             + (f", {self._tars_unverified:,} not checked (no s3:GetObject on the "
                                f"tars, or S3 returned no CRC64NVME)" if self._tars_unverified else ""))
        self.logger.info(f"Upload client   : {self.upload_client}")
        self.logger.info(f"Avg rate        : {self._human(rate)}/s")
        self.logger.info("#" * 40)


# --------------------------------------------------------------------------- #
# CLI shared by both front ends
# --------------------------------------------------------------------------- #
def add_common_args(p):
    """Destination, bundling, upload, manifest and run options."""
    p.add_argument("--dst-bucket", required=True)
    p.add_argument("--dst-prefix", required=True)
    grp = p.add_mutually_exclusive_group(required=True)
    grp.add_argument("--max-files", type=int)
    grp.add_argument("--max-size", type=parse_size, help="e.g. 512MB")
    p.add_argument("--num-workers", type=int, default=os.cpu_count(),
                   help=f"worker processes (default: os.cpu_count()={os.cpu_count()})")
    p.add_argument("--run-id",
                   help="token embedded in tar names and the log filename to keep "
                        "concurrent runs apart (default: <timestamp>_<pid>_<rand>). "
                        "Override only if you need a reproducible name -- two live "
                        "runs sharing a --dst-prefix AND a --run-id will overwrite "
                        "each other's tars")
    p.add_argument("--part-size", choices=["8MB", "16MB", "32MB", "64MB"], default="32MB",
                   help="multipart upload part size, also the single-PUT threshold "
                        "(default 32MB). Every part is a billed PUT-class request, so "
                        "bigger parts mean fewer requests: a 256MB tar takes 34 requests "
                        "at 8MB, 18 at 16MB, 10 at 32MB, 6 at 64MB. Bigger parts use more "
                        "memory (about part size x --upload-concurrency per worker) and "
                        "re-send more data when a part fails")
    p.add_argument("--upload-concurrency", type=int, default=10,
                   help="multipart upload connections per worker (default 10)")
    p.add_argument("--upload-client", choices=("crt", "boto3"), default="crt",
                   help="client for the tar uploads: crt (default; the AWS CRT S3 client, "
                        "native TLS and multipart) or boto3. Both send a CRC64NVME checksum "
                        "that S3 checks on every request")
    p.add_argument("--verify-tar-upload", action=argparse.BooleanOptionalAction, default=True,
                   help="after each upload, compare the full-object CRC64NVME S3 stored for "
                        "the tar with the CRC64NVME of the bytes that were packed (one "
                        "HeadObject per tar; needs s3:GetObject on the tars). A mismatch "
                        "fails the tar and records none of its rows (default on)")
    p.add_argument("--md5", action=argparse.BooleanOptionalAction, default=True,
                   help="also compute per-file MD5 (on by default). CRC64NVME is always "
                        "computed; --no-md5 leaves the md5 column null")
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
    p.add_argument("--tar-layout", choices=["run", "date", "flat"], default="run",
                   help="folder level for the tar objects under --dst-prefix: run "
                        "(<prefix>/<run-id>/..., default: one folder per run), date "
                        "(<prefix>/<YYYYMMDD>/..., the earlier layout) or flat "
                        "(<prefix>/archive_*.tar). Existing archives keep working "
                        "whatever you pick -- the manifest stores full tar keys")
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
    p.add_argument("--resume-run", action="append", metavar="RUN_ID",
                   help="continue an interrupted (cancelled or failed) run: archive the "
                        "same source again but skip every file that run already "
                        "archived, so nothing is duplicated. Repeat for a chain of "
                        "resumes. Use the same --src-path / --walk-path / --dst-prefix")
    p.add_argument("--status-s3", metavar="s3://BUCKET/KEY.json",
                   help="also write the progress snapshot (with host, pid and the last "
                        "log lines) to this S3 object every few seconds, and the failed "
                        "list next to it at the end -- lets the GUI follow a run on EC2")
    p.add_argument("--run-history", action=argparse.BooleanOptionalAction, default=True,
                   help="record every run in <table-name>_runs (on by default; "
                        "required by --incremental)")
    p.add_argument("--progress", action="store_true",
                   help="print machine-readable JSON progress snapshots to stdout "
                        "(lines prefixed '@@PROGRESS '); used by the GUI")


def run(cls, p):
    """Parse, validate and run a front end."""
    # A process started in the background by a non-interactive shell (nohup …
    # &, some schedulers, a GUI launched that way) inherits SIGINT as IGNORED,
    # and Python then never raises KeyboardInterrupt -- cancel would silently do
    # nothing. Re-arm it explicitly.
    signal.signal(signal.SIGINT, signal.default_int_handler)
    ensure_ca_bundle()      # before any pyarrow S3 use (inherited by the workers)
    args = p.parse_args()
    if args.commit_retries <= 0:
        p.error("--commit-retries must be a positive integer")
    if args.flush_interval <= 0:
        p.error("--flush-interval must be positive")
    try:
        archiver = cls(args)
    except ValueError as e:
        p.error(str(e))
    archiver.start_processing()
