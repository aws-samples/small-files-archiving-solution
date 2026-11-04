"""
backend.py -- non-GUI logic for the archive browser: S3 Tables manifest queries
and a coalescing byte-range restore engine. No Qt imports, so it is testable on
its own.

Scale notes (GB .. PB, up to billions of manifest rows):
  * Nothing ever loads the whole manifest. A folder listing is two
    equality-filtered Iceberg scans (dir index: parent_dir == X; manifest:
    parent_dir == X, with a row limit), which min/max stats prune to a few
    data files.
  * Tables written before the dir index existed have no parent_dir column; for
    those the browser falls back to a bounded prefix scan and says so.
  * Restore streams manifest rows in Arrow batches, groups them per tar and
    merges nearby byte ranges into one GET, so a million-file folder restore is
    a few thousand requests, not a million.
"""
import io
import os
import sys
import base64
import stat
import json
import re
import uuid
import time
import importlib.util
import hashlib
import tarfile
import posixpath
import threading
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, FIRST_COMPLETED, wait

import boto3
import pyarrow as pa
import pyarrow.compute as pc


CHECKSUM_FIELDS = ("checksum_algorithm", "checksum")
FILE_FIELDS = ("source_key", "filesize", "archive_ts", "tar_key",
               "byte_start", "byte_end", "md5")


ARCHIVER_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                             "archiver", "fss3-archiver-s3tables-mp.py")
_ARCHIVER = {}


def archiver_module(path=ARCHIVER_PATH):
    """The archiver script as a module: table schemas, partition spec and
    source_id_for() come from there, so the GUI can't drift from what the
    archiver writes."""
    if path not in _ARCHIVER:
        spec = importlib.util.spec_from_file_location("sfa_archiver", path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        _ARCHIVER[path] = mod
    return _ARCHIVER[path]


def archiver_core_module(path=ARCHIVER_PATH):
    """archiver_core.py next to the archiver script: the table schemas live
    there since the archiver was split into a shared core and two front ends.
    Loading the script imports its core (it puts its own folder on sys.path)."""
    mod = archiver_module(path)
    core = sys.modules.get("archiver_core")
    if core is None or not hasattr(core, "ICEBERG_SCHEMA"):
        return mod                      # an older single-file archiver
    return core


_ACCOUNT_RE = re.compile(r"(?<![0-9])[0-9]{12}(?![0-9])")
_ARN_ACCT_RE = re.compile(r"(arn:aws[a-z-]*:[a-z0-9-]*:[a-z0-9-]*:)[0-9]{12}")
_PRINCIPAL_RE = re.compile(r"(arn:aws[a-z-]*:(?:iam|sts)::[^:\s]*:(?:user|role|assumed-role|group|federated-user)/)[^\s,;'\"]+")
_IDENT_RE = re.compile(r"(?i)\b(account|accountid|account_id|user ?id)\b(['\"]?\s*[:=]\s*['\"]?)[0-9A-Z]{12,21}")


_HOME = os.path.expanduser("~").rstrip("/")
_KEY_ID_RE = re.compile(r"\b(?:AKIA|ASIA|AIDA|AROA|AIPA|ANPA|ANVA|APKA)[A-Z0-9]{16,17}\b")


def tilde(path):
    """Show a path under the home folder as ~/... (display only)."""
    p = str(path or "")
    if _HOME and _HOME != "/" and (p == _HOME or p.startswith(_HOME + "/")):
        return "~" + p[len(_HOME):]
    return p


def mask(text):
    """Strip AWS account IDs / user ids from anything shown on screen. ARNs keep
    their shape (arn:aws:s3tables:region:************:bucket/name) so messages
    stay readable. Paths under the home folder are shown as ~/..., which keeps
    the local user name off screen too."""
    if not text:
        return text
    text = str(text)
    if _HOME and _HOME != "/":
        text = re.sub(re.escape(_HOME) + r"(?=/|\b)", "~", text)
    text = _ARN_ACCT_RE.sub(r"\1************", text)
    # access key / user / role ids encode the account: hide them too
    text = _KEY_ID_RE.sub("****************", text)
    # IAM / STS principals: user, role and session names identify the account owner
    text = _PRINCIPAL_RE.sub(r"\1****", text)
    text = _IDENT_RE.sub(r"\1\2************", text)
    return _ACCOUNT_RE.sub("************", text)


@dataclass
class Config:
    profile: str = ""
    region: str = ""
    bucket: str = ""
    table_bucket_arn: str = ""     # derived; never shown on screen
    namespace: str = "small_files_archive"
    table_name: str = "tar_manifest"
    table_bucket: str = ""         # the name users see and pick

    def __post_init__(self):
        if not self.table_bucket and self.table_bucket_arn:
            self.table_bucket = self.table_bucket_arn.rsplit("/", 1)[-1]

    def catalog_props(self):
        return {
            "type": "rest",
            "warehouse": self.table_bucket_arn,
            "uri": f"https://s3tables.{self.region}.amazonaws.com/iceberg",
            "rest.sigv4-enabled": "true",
            "rest.signing-name": "s3tables",
            "rest.signing-region": self.region,
        }

    def complete(self):
        return all([self.region, self.bucket, self.table_bucket or self.table_bucket_arn,
                    self.namespace, self.table_name])


@dataclass
class Listing:
    path: str
    dirs: list = field(default_factory=list)    # dicts: name, path, files, bytes, archive_ts
    files: list = field(default_factory=list)   # dicts: FILE_FIELDS + name
    truncated: bool = False
    mode: str = "index"                          # "index" | "legacy"
    elapsed: float = 0.0


_TAR_NAME_RE = re.compile(r"^archive_(.+)_\d{5}\.tar(?:\.gz)?$")


def newest_per_key(tbl):
    """Keep the newest row (max archive_ts) per source_key. Arrow-native, so it
    stays fast and compact for large folders."""
    if tbl is None or tbl.num_rows <= 1:
        return tbl
    t = tbl.take(pc.sort_indices(tbl, [("source_key", "ascending"),
                                       ("archive_ts", "descending")]))
    keys = t.column("source_key").combine_chunks()
    prev = pa.concat_arrays([pa.array([None], type=keys.type), keys.slice(0, len(keys) - 1)])
    keep = pc.invert(pc.fill_null(pc.equal(keys, prev), False))
    return t.filter(keep)


def parse_size(text):
    """'256MB' / '1GB' / '4096' -> bytes (binary units, like the archiver)."""
    t = str(text).strip().upper().replace(" ", "")
    units = {"TB": 1024 ** 4, "GB": 1024 ** 3, "MB": 1024 ** 2, "KB": 1024, "T": 1024 ** 4,
             "G": 1024 ** 3, "M": 1024 ** 2, "K": 1024, "B": 1}
    for u, m in units.items():
        if t.endswith(u) and t[:-len(u)].replace(".", "", 1).isdigit():
            return int(float(t[:-len(u)]) * m)
    if t.isdigit():
        return int(t)
    raise ValueError(f"bad size {text!r}")


def human(n):
    if n is None:
        return ""
    n = float(n)
    for u in ("B", "KB", "MB", "GB", "TB", "PB"):
        if abs(n) < 1024 or u == "PB":
            return f"{n:.0f} {u}" if u == "B" else f"{n:.1f} {u}"
        n /= 1024


class Manifest:
    """Thin, thread-safe-enough wrapper around the two Iceberg tables.

    pyiceberg catalog/table objects are not documented as thread-safe, so each
    call path that may run concurrently loads its own table handle (cheap: a
    metadata fetch)."""

    def __init__(self, cfg: Config):
        self.cfg = cfg
        if cfg.profile:
            # pyiceberg's SigV4 signer and FileIO use the default boto3 chain
            os.environ["AWS_PROFILE"] = cfg.profile
        from pyiceberg.catalog import load_catalog
        self._catalog = load_catalog("s3tables", **cfg.catalog_props())
        self._lock = threading.Lock()
        self.has_index = None
        self.unindexed_rows = False
        self.file_fields = FILE_FIELDS
        # True until the first archive run creates namespace + table; the
        # browser then shows an empty archive instead of a connection error.
        self.table_missing = False

    # -- tables ------------------------------------------------------------
    def _table(self, suffix=""):
        with self._lock:
            return self._catalog.load_table(
                f"{self.cfg.namespace}.{self.cfg.table_name}{suffix}")

    def probe(self):
        """Return (has_index, manifest snapshot summary dict).

        A missing namespace/table is not an error: it is the normal state
        before the first archive run, which creates both."""
        from pyiceberg.exceptions import NoSuchTableError, NoSuchNamespaceError
        try:
            tbl = self._table()
        except (NoSuchTableError, NoSuchNamespaceError):
            self.table_missing = True
            self.has_index = False
            self.unindexed_rows = False
            return False, {}
        self.table_missing = False
        cols = set(tbl.schema().column_names)
        # tables from before the CRC64NVME columns still work, with MD5 only
        self.file_fields = FILE_FIELDS + tuple(c for c in CHECKSUM_FIELDS if c in cols)
        has_col = "parent_dir" in tbl.schema().column_names
        has_dirs = False
        if has_col:
            try:
                self._table("_dirs")
                has_dirs = True
            except Exception:
                has_dirs = False
        self.has_index = has_col and has_dirs
        self.unindexed_rows = False
        if self.has_index:
            # Rows written before parent_dir existed are invisible to folder
            # listings. Null-count stats make this a metadata-only check.
            from pyiceberg.expressions import IsNull
            self.unindexed_rows = tbl.scan(row_filter=IsNull("parent_dir"),
                                           selected_fields=("source_key",),
                                           limit=1).to_arrow().num_rows > 0
        snap = tbl.current_snapshot()
        summary = dict(snap.summary.additional_properties) if snap and snap.summary else {}
        return self.has_index, summary

    # -- browsing ----------------------------------------------------------
    def list_dir(self, path, limit=5000, legacy_scan_limit=200_000):
        from pyiceberg.expressions import EqualTo, StartsWith
        t0 = time.time()
        if self.has_index is None or self.table_missing:
            self.probe()   # re-check: an archive run may have just created it
        out = Listing(path=path)
        if self.table_missing:
            out.mode = "empty"
            out.elapsed = time.time() - t0
            return out
        if self.has_index:
            dtbl = self._table("_dirs")
            arr = dtbl.scan(row_filter=EqualTo("parent_dir", path),
                            selected_fields=("dir_path", "files", "bytes",
                                             "archive_ts")).to_arrow()
            if arr.num_rows:
                g = arr.group_by("dir_path").aggregate(
                    [("files", "sum"), ("bytes", "sum"), ("archive_ts", "max")])
                for r in g.to_pylist():
                    out.dirs.append({"name": posixpath.basename(r["dir_path"]),
                                     "path": r["dir_path"],
                                     "files": r["files_sum"],
                                     "bytes": r["bytes_sum"],
                                     "archive_ts": r["archive_ts_max"]})
            mtbl = self._table()
            rows = mtbl.scan(row_filter=EqualTo("parent_dir", path),
                             selected_fields=self.file_fields,
                             limit=limit + 1).to_arrow()
            out.files, out.truncated = self._files_from(rows, limit)
        else:
            # Legacy table (no parent_dir): bounded prefix scan, folders are
            # synthesized from the first `legacy_scan_limit` descendants.
            out.mode = "legacy"
            mtbl = self._table()
            flt = StartsWith("source_key", path + "/") if path else None
            kw = {"selected_fields": self.file_fields, "limit": legacy_scan_limit}
            if flt is not None:
                kw["row_filter"] = flt
            rows = mtbl.scan(**kw).to_arrow()
            out.truncated = rows.num_rows >= legacy_scan_limit
            plen = len(path) + 1 if path else 0
            dirs, direct = {}, []
            for r in rows.to_pylist():
                rest = r["source_key"][plen:]
                if "/" in rest:
                    name = rest.split("/", 1)[0]
                    d = dirs.setdefault(name, {"name": name,
                                               "path": (path + "/" + name) if path else name,
                                               "files": 0, "bytes": 0,
                                               "archive_ts": None})
                    d["files"] += 1
                    d["bytes"] += r["filesize"]
                    if d["archive_ts"] is None or r["archive_ts"] > d["archive_ts"]:
                        d["archive_ts"] = r["archive_ts"]
                else:
                    direct.append(r)
            out.dirs = list(dirs.values())
            out.files, trunc = self._files_from(pa.Table.from_pylist(direct) if direct
                                                else None, limit)
            out.truncated = out.truncated or trunc
        out.dirs.sort(key=lambda d: d["name"].lower())
        out.elapsed = time.time() - t0
        return out

    @staticmethod
    def _files_from(rows, limit):
        """Arrow rows -> newest version per source_key, sorted, capped."""
        if rows is None or rows.num_rows == 0:
            return [], False
        newest = {}
        for r in rows.to_pylist():
            k = r["source_key"]
            cur = newest.get(k)
            if cur is None:
                r["versions"] = 1
                newest[k] = r
            else:
                n = cur["versions"] + 1
                if r["archive_ts"] > cur["archive_ts"]:
                    newest[k] = r
                newest[k]["versions"] = n
        files = sorted(newest.values(), key=lambda r: r["source_key"].lower())
        truncated = rows.num_rows > limit
        for f in files:
            f["name"] = posixpath.basename(f["source_key"])
        return files[:limit], truncated

    def search(self, prefix, limit=5000):
        """Files whose source_key starts with `prefix` (pushdown, bounded)."""
        from pyiceberg.expressions import StartsWith
        if self.table_missing:
            return [], False
        rows = self._table().scan(row_filter=StartsWith("source_key", prefix),
                                  selected_fields=self.file_fields,
                                  limit=limit + 1).to_arrow()
        return self._files_from(rows, limit)

    @staticmethod
    def _tar_keys(key):
        """The manifest stores tar_key as built from --dst-prefix, while the S3
        upload strips a leading '/'. Match both spellings."""
        k = key.lstrip("/")
        return [k, "/" + k]

    def tar_members(self, tar_key, limit=5000):
        """Members of one tar, in on-disk order. ([], False) means the tar has
        no manifest rows -- an orphan (uploaded, never committed)."""
        from pyiceberg.expressions import In
        if self.table_missing:
            return [], False
        rows = self._table().scan(row_filter=In("tar_key", self._tar_keys(tar_key)),
                                  selected_fields=self.file_fields,
                                  limit=limit + 1).to_arrow()
        lst = rows.to_pylist()
        lst.sort(key=lambda r: r["byte_start"])
        return lst[:limit], len(lst) > limit

    # -- run history ---------------------------------------------------------
    def last_run(self, source_id):
        """Newest ok/partial run of a source from <table>_runs, or None (also
        when the history table doesn't exist yet)."""
        from pyiceberg.expressions import And, EqualTo, In
        try:
            tbl = self._table("_runs")
        except Exception:
            return None
        rows = tbl.scan(row_filter=And(EqualTo("source_id", source_id),
                                       In("status", ["ok", "partial"])),
                        selected_fields=("run_id", "start_ts", "files", "bytes",
                                         "status", "mode")).to_arrow().to_pylist()
        return max(rows, key=lambda r: r["start_ts"]) if rows else None

    def list_runs(self, limit=500):
        """Completed runs (ok/partial), newest first, for 'restore as of run'."""
        from pyiceberg.expressions import In
        try:
            tbl = self._table("_runs")
        except Exception:
            return []
        rows = tbl.scan(row_filter=In("status", ["ok", "partial"]),
                        selected_fields=("run_id", "mode", "status", "start_ts", "end_ts",
                                         "files", "bytes", "src_path", "walk_paths",
                                         "input_file", "dst_prefix")).to_arrow().to_pylist()
        rows.sort(key=lambda r: r["end_ts"], reverse=True)
        return rows[:limit]

    # -- linking the two views ----------------------------------------------
    @staticmethod
    def _common_dir(paths):
        """Longest common directory of '/'-separated paths ('' if none)."""
        parts = [p.strip("/").split("/") for p in paths if p is not None]
        if not parts:
            return ""
        common = parts[0]
        for p in parts[1:]:
            n = 0
            while n < min(len(common), len(p)) and common[n] == p[n]:
                n += 1
            common = common[:n]
        return "/".join(common)

    def tars_for_folder(self, path, sample=5000):
        """Where the tars holding `path`'s files live in S3.

        Returns (s3_prefix, tar_keys, sampled). Reads at most `sample` rows
        (pushdown StartsWith), so on a huge folder the tar set may be partial --
        `sampled` says so -- but the prefix is still right: one run writes all
        its tars under one <dst-prefix>/<date>/."""
        from pyiceberg.expressions import StartsWith
        if self.table_missing:
            return "", [], False
        kw = {"selected_fields": ("tar_key",), "limit": sample + 1}
        if path:
            kw["row_filter"] = StartsWith("source_key", path.strip("/") + "/")
        col = self._table().scan(**kw).to_arrow().column("tar_key").to_pylist()
        sampled = len(col) > sample
        keys = sorted({k.lstrip("/") for k in col[:sample]})
        if not keys:
            return "", [], False
        prefix = self._common_dir([posixpath.dirname(k) for k in keys])
        return (prefix + "/" if prefix else ""), keys, sampled

    @staticmethod
    def dst_prefix_of(tar_key):
        """Destination prefix of a tar key, for any --tar-layout:
             run   <prefix>/<run-id>/archive_<run-id>_00001.tar
             date  <prefix>/<YYYYMMDD>/archive_<run-id>_00001.tar
             flat  <prefix>/archive_<run-id>_00001.tar
        The run id is read back from the file name; the folder above the file
        is a layout level only when it is that run id or an 8-digit date."""
        parts = tar_key.strip("/").split("/")
        if len(parts) < 2:
            return ""
        m = _TAR_NAME_RE.match(parts[-1])
        folder = parts[-2]
        if len(parts) >= 3 and ((m and folder == m.group(1)) or re.fullmatch(r"\d{8}", folder)):
            return "/".join(parts[:-2])
        return "/".join(parts[:-1])

    def archive_location(self, sample=5000):
        """Where this archive's tars live: (s3_prefix_to_open, dst_prefixes).

        Opens the deepest folder every tar shares: <dst-prefix>/<date>/ when
        all tars are under one date, <dst-prefix>/ when there are several dates,
        their common parent (or the bucket root) for several destination
        prefixes. Inputs: a bounded sample of manifest tar keys (covers runs
        from before the history table) plus <table>_runs, whose dst_prefix and
        start dates keep a sample that happens to see one date from hiding
        others."""
        if self.table_missing:
            return "", []
        keys = self._table().scan(selected_fields=("tar_key",), limit=sample) \
            .to_arrow().column("tar_key").to_pylist()
        tar_dirs = {posixpath.dirname(k.strip("/")) for k in keys if k}
        roots = {self.dst_prefix_of(k) for k in keys if k}
        run_dates = {}
        try:
            for r in self._table("_runs").scan(selected_fields=("dst_prefix", "start_ts")) \
                    .to_arrow().to_pylist():
                if r["dst_prefix"]:
                    p = r["dst_prefix"].strip("/")
                    roots.add(p)
                    run_dates.setdefault(p, set()).add(r["start_ts"].astimezone().date())
        except Exception:
            pass
        cands = set(tar_dirs)
        for p in roots:
            covered = any(d == p or d.startswith(p + "/") for d in tar_dirs)
            # a prefix the sample never saw, or one with runs on several dates,
            # must not be skipped past into a single date folder
            if not covered or len(run_dates.get(p, ())) > 1:
                cands.add(p)
        common = self._common_dir(sorted(cands)) if cands else ""
        return (common + "/" if common else ""), sorted(r for r in roots if r)

    def folder_for_tars(self, tar_keys, sample=5000):
        """Deepest folder that contains every file of the given tars ('' = root)."""
        from pyiceberg.expressions import In
        if self.table_missing or not tar_keys:
            return ""
        keys = [v for k in tar_keys for v in self._tar_keys(k)]
        rows = self._table().scan(row_filter=In("tar_key", keys),
                                  selected_fields=("source_key",),
                                  limit=sample).to_arrow().column("source_key").to_pylist()
        return self._common_dir([posixpath.dirname(r) for r in rows])

    # -- restore source ----------------------------------------------------
    DIR_CHUNK = 500        # folders per manifest query in an indexed folder restore

    def iter_batches(self, selections, as_of=None):
        """Yield (kind, path, Arrow batch of FILE_FIELDS) for each selection.

        selections: list of (kind, path):
          'file'   path = source_key       newest copy of that path
          'dir'    path = folder           newest copy of every file below it
          'tar'    path = S3 key of a tar  every member of that tar
          'member' path = (tar_key, source_key)  that tar's copy specifically
        as_of (datetime, optional): point-in-time restore -- 'newest' becomes
        newest archived at or before as_of. It applies to 'file' and 'dir';
        'tar' and 'member' already name one exact copy.

        Folder restores pick the newest copy of every path, also across
        incremental runs. With the directory index this is exact and bounded:
        folders are read a chunk at a time (parent_dir IN chunk), and every copy
        of a path shares its parent_dir, so de-duplicating within a chunk is
        complete. Legacy tables (no index) fall back to a prefix stream that
        de-duplicates within each batch only."""
        from pyiceberg.expressions import EqualTo, StartsWith, In, And, LessThanOrEqual
        tbl = self._table()

        def cut(expr):
            if as_of is None:
                return expr
            le = LessThanOrEqual("archive_ts", as_of)
            return le if expr is None else And(expr, le)

        for kind, path in selections:
            if kind == "tar":
                for b in tbl.scan(row_filter=In("tar_key", self._tar_keys(path)),
                                  selected_fields=self.file_fields).to_arrow_batch_reader():
                    yield kind, path, b
                continue
            if kind == "member":
                tk, sk = path
                arr = tbl.scan(row_filter=And(In("tar_key", self._tar_keys(tk)),
                                              EqualTo("source_key", sk)),
                               selected_fields=self.file_fields).to_arrow()
                for b in arr.slice(0, 1).to_batches():
                    yield kind, path, b
                continue
            if kind == "file":
                arr = tbl.scan(row_filter=cut(EqualTo("source_key", path)),
                               selected_fields=self.file_fields).to_arrow()
                for b in newest_per_key(arr).to_batches():
                    yield kind, path, b
                continue
            if self.has_index is None:
                self.probe()
            if self.has_index and not self.unindexed_rows:
                for b in self._iter_dir_indexed(tbl, path, cut):
                    yield kind, path, b
                continue
            flt = cut(StartsWith("source_key", path + "/") if path else None)
            kw = {"selected_fields": self.file_fields}
            if flt is not None:
                kw["row_filter"] = flt
            for b in tbl.scan(**kw).to_arrow_batch_reader():
                if b.num_rows:
                    for bb in newest_per_key(pa.Table.from_batches([b])).to_batches():
                        yield kind, path, bb

    def folders_under(self, path):
        """Every folder at or below `path`, from the directory index."""
        from pyiceberg.expressions import EqualTo, Or, StartsWith
        dtbl = self._table("_dirs")
        kw = {"selected_fields": ("dir_path",)}
        if path:
            kw["row_filter"] = Or(EqualTo("dir_path", path), StartsWith("dir_path", path + "/"))
        dirs = set(dtbl.scan(**kw).to_arrow().column("dir_path").to_pylist())
        dirs.add(path)          # '' = files directly at the root
        return sorted(dirs)

    def _iter_dir_indexed(self, tbl, path, cut):
        from pyiceberg.expressions import In
        dirs = self.folders_under(path)
        for i in range(0, len(dirs), self.DIR_CHUNK):
            arr = tbl.scan(row_filter=cut(In("parent_dir", dirs[i:i + self.DIR_CHUNK])),
                           selected_fields=self.file_fields).to_arrow()
            for b in newest_per_key(arr).to_batches(max_chunksize=20_000):
                yield b

    def count_under(self, selections, cached_dirs):
        """Cheap (files, bytes) estimate for a selection using listing data."""
        files = nbytes = 0
        for kind, path in selections:
            info = cached_dirs.get((kind, path))
            if info:
                files += info[0]
                nbytes += info[1]
        return files, nbytes


# --------------------------------------------------------------------------- #
# First-run setup: find or create the table bucket, tables and tar bucket.
# Nothing here returns the account id to the caller; it is only used to build
# ARNs internally.
# --------------------------------------------------------------------------- #
TABLE_BUCKET_RE = re.compile(r"^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$")
S3_BUCKET_RE = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")
ICEBERG_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9_]{0,254}$")


def valid_table_bucket(name):
    return bool(TABLE_BUCKET_RE.match(name or "")) and not name.startswith(("aws", "xn--")) \
        and "--" not in name


def valid_s3_bucket(name):
    return bool(S3_BUCKET_RE.match(name or "")) and ".." not in name \
        and not re.match(r"^\d+\.\d+\.\d+\.\d+$", name) and not name.startswith("xn--")


def valid_iceberg_name(name):
    return bool(ICEBERG_NAME_RE.match(name or "")) and not name.startswith("aws")


class Provisioner:
    def __init__(self, profile, region):
        self.region = region
        self.session = boto3.Session(profile_name=profile or None)

    @staticmethod
    def profiles():
        try:
            return boto3.Session().available_profiles
        except Exception:
            return []

    @staticmethod
    def profile_region(profile):
        try:
            return boto3.Session(profile_name=profile or None).region_name or ""
        except Exception:
            return ""

    def check(self):
        """True if the credentials work. Deliberately returns nothing else."""
        self.session.client("sts", region_name=self.region).get_caller_identity()
        return True

    def _account(self):
        return self.session.client("sts", region_name=self.region).get_caller_identity()["Account"]

    def table_bucket_arn(self, name):
        return f"arn:aws:s3tables:{self.region}:{self._account()}:bucket/{name}"

    def list_table_buckets(self):
        c = self.session.client("s3tables", region_name=self.region)
        out, token = [], None
        while True:
            resp = c.list_table_buckets(**({"continuationToken": token} if token else {}))
            out += [b["name"] for b in resp.get("tableBuckets", [])]
            token = resp.get("continuationToken")
            if not token:
                return sorted(out)

    def create_table_bucket(self, name):
        c = self.session.client("s3tables", region_name=self.region)
        return c.create_table_bucket(name=name)["arn"]

    def list_buckets(self):
        """General purpose buckets in this region (server-side filtered)."""
        c = self.session.client("s3", region_name=self.region)
        out, token = [], None
        while True:
            kw = {"BucketRegion": self.region, "MaxBuckets": 1000}
            if token:
                kw["ContinuationToken"] = token
            resp = c.list_buckets(**kw)
            out += [b["Name"] for b in resp.get("Buckets", [])]
            token = resp.get("ContinuationToken")
            if not token:
                return sorted(out)

    def create_bucket(self, name):
        """Private by default: all four Block Public Access settings on, bucket
        owner enforced (no ACLs), SSE-S3 default encryption, TLS-only bucket
        policy, and a lifecycle rule that aborts incomplete multipart uploads."""
        c = self.session.client("s3", region_name=self.region)
        kw = {"Bucket": name, "ObjectOwnership": "BucketOwnerEnforced"}
        if self.region != "us-east-1":
            kw["CreateBucketConfiguration"] = {"LocationConstraint": self.region}
        c.create_bucket(**kw)
        c.put_public_access_block(Bucket=name, PublicAccessBlockConfiguration={
            "BlockPublicAcls": True, "IgnorePublicAcls": True,
            "BlockPublicPolicy": True, "RestrictPublicBuckets": True})
        c.put_bucket_encryption(Bucket=name, ServerSideEncryptionConfiguration={
            "Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"},
                       "BucketKeyEnabled": True}]})
        # TLS only: deny any request made over plain HTTP (a Deny with a
        # condition -- grants nothing to anyone)
        c.put_bucket_policy(Bucket=name, Policy=json.dumps({
            "Version": "2012-10-17",
            "Statement": [{"Sid": "DenyInsecureTransport", "Effect": "Deny", "Principal": "*",
                           "Action": "s3:*",
                           "Resource": [f"arn:aws:s3:::{name}", f"arn:aws:s3:::{name}/*"],
                           "Condition": {"Bool": {"aws:SecureTransport": "false"}}}]}))
        # parts of an interrupted multipart upload are billed until aborted
        c.put_bucket_lifecycle_configuration(Bucket=name, LifecycleConfiguration={
            "Rules": [{"ID": "abort-incomplete-multipart-uploads", "Status": "Enabled",
                       "Filter": {"Prefix": ""},
                       "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 7}}]})

    def _catalog(self, table_bucket_arn):
        from pyiceberg.catalog import load_catalog
        cfg = Config(region=self.region, table_bucket_arn=table_bucket_arn)
        if self.session.profile_name and self.session.profile_name != "default":
            os.environ["AWS_PROFILE"] = self.session.profile_name
        return load_catalog("s3tables", **cfg.catalog_props())

    def create_tables(self, table_bucket_arn, namespace, table_name, tar_bucket=None,
                      archiver_path=ARCHIVER_PATH):
        """Namespace + manifest, dir-index and run-history tables, with exactly
        the schemas the archiver uses. Existing ones are left alone; an existing
        manifest without a recorded tar bucket gets one.
        Returns [(table, 'created' | 'exists')]."""
        from pyiceberg.exceptions import NamespaceAlreadyExistsError, TableAlreadyExistsError
        from pyiceberg.partitioning import PartitionSpec, PartitionField
        from pyiceberg.transforms import DayTransform
        a = archiver_core_module(archiver_path)
        cat = self._catalog(table_bucket_arn)
        try:
            cat.create_namespace(namespace)
        except NamespaceAlreadyExistsError:
            pass
        out = []
        props = a.table_properties(tar_bucket) if hasattr(a, "table_properties") else {}
        specs = [
            (table_name, a.ICEBERG_SCHEMA,
             PartitionSpec(PartitionField(source_id=7, field_id=1000,
                                          transform=DayTransform(), name="archive_ts_day")), props),
            (table_name + "_dirs", a.DIRS_ICEBERG_SCHEMA, None, {}),
            (table_name + "_runs", a.RUNS_ICEBERG_SCHEMA, None, {}),
        ]
        for name, schema, spec, p in specs:
            try:
                kw = {"identifier": f"{namespace}.{name}", "schema": schema}
                if spec is not None:
                    kw["partition_spec"] = spec
                if p:
                    kw["properties"] = p
                cat.create_table(**kw)
                out.append((name, "created"))
            except TableAlreadyExistsError:
                out.append((name, "exists"))
                if p and tar_bucket:
                    self.record_tar_bucket(table_bucket_arn, namespace, name, tar_bucket, cat=cat)
        return out

    # -- connect an existing archive ------------------------------------------
    def list_manifests(self, table_bucket_arn):
        """Archive manifests in a table bucket: tables that have the manifest
        columns (other tables are ignored). Returns [{namespace, table,
        tar_bucket (recorded or None), dirs, runs}], sorted."""
        cat = self._catalog(table_bucket_arn)
        need = {"tar_key", "source_key", "byte_start", "byte_end"}
        out = []
        for ns in cat.list_namespaces():
            ns_name = ".".join(ns)
            names = {t[-1] for t in cat.list_tables(ns_name)}
            for t in sorted(names):
                if t.endswith("_dirs") or t.endswith("_runs"):
                    continue
                try:
                    tbl = cat.load_table(f"{ns_name}.{t}")
                except Exception:
                    continue
                if not need <= set(tbl.schema().column_names):
                    continue
                out.append({"namespace": ns_name, "table": t,
                            "tar_bucket": tbl.properties.get("sfa.tar_bucket"),
                            "dirs": f"{t}_dirs" in names, "runs": f"{t}_runs" in names})
        return out

    def verify_tar_bucket(self, table_bucket_arn, namespace, table, bucket):
        """Check that this bucket holds the archive's tars: take one tar_key from
        the manifest and HEAD it in the bucket. Returns a short message; raises
        ValueError if the tar is not there."""
        cat = self._catalog(table_bucket_arn)
        tbl = cat.load_table(f"{namespace}.{table}")
        rows = tbl.scan(selected_fields=("tar_key",), limit=1).to_arrow()
        if rows.num_rows == 0:
            return "the archive is empty: nothing to check yet"
        key = rows.column("tar_key")[0].as_py().lstrip("/")
        try:
            self.session.client("s3", region_name=self.region).head_object(Bucket=bucket, Key=key)
        except Exception as e:
            code = getattr(e, "response", {}).get("Error", {}).get("Code", type(e).__name__)
            raise ValueError(f"a tar listed in the manifest is not in s3://{bucket} ({code}). "
                             f"Pick the bucket this archive's tars were written to.") from None
        return f"found {posixpath.basename(key)} from the manifest in this bucket"

    def record_tar_bucket(self, table_bucket_arn, namespace, table, bucket, cat=None):
        """Record the tar bucket in the manifest table's properties (best effort)."""
        try:
            cat = cat or self._catalog(table_bucket_arn)
            tbl = cat.load_table(f"{namespace}.{table}")
            if tbl.properties.get("sfa.tar_bucket"):
                return False
            with tbl.transaction() as tx:
                tx.set_properties(**{"sfa.tar_bucket": bucket})
            return True
        except Exception:
            return False


# --------------------------------------------------------------------------- #
# Tar objects in S3
# --------------------------------------------------------------------------- #
class TarStore:
    """S3 listing of the tar bucket, one prefix level and one page at a time
    (ListObjectsV2 with Delimiter='/'), so millions of tars cost nothing until
    you page through them."""

    TAR_EXT = (".tar", ".tar.gz")

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self._s3 = boto3.Session(profile_name=cfg.profile or None).client(
            "s3", region_name=cfg.region or None)

    def list(self, prefix, token=None, name_prefix="", max_keys=1000):
        kw = {"Bucket": self.cfg.bucket, "Prefix": prefix + name_prefix,
              "Delimiter": "/", "MaxKeys": max_keys}
        if token:
            kw["ContinuationToken"] = token
        resp = self._s3.list_objects_v2(**kw)
        # a key starting with '/' (from a --dst-prefix with a leading slash)
        # lists as an empty-named folder; label it '/'
        dirs = [{"kind": "prefix", "name": p["Prefix"][len(prefix):].rstrip("/") or "/",
                 "path": p["Prefix"]} for p in resp.get("CommonPrefixes", [])]
        objs = [{"kind": "object", "name": o["Key"][len(prefix):], "key": o["Key"],
                 "size": o["Size"], "last_modified": o["LastModified"],
                 "storage_class": o.get("StorageClass", "STANDARD"),
                 "is_tar": o["Key"].endswith(self.TAR_EXT)}
                for o in resp.get("Contents", []) if o["Key"] != prefix]
        return dirs, objs, resp.get("NextContinuationToken")


# --------------------------------------------------------------------------- #
# Restore engine
# --------------------------------------------------------------------------- #
class _Verifier:
    """Checks restored bytes against the manifest: CRC64NVME when the row has
    it (S3's encoding, comparable with S3 Batch Operations checksum reports),
    otherwise MD5. Feed it chunks with update(); check() raises on mismatch."""

    def __init__(self, row, enabled=True):
        self.row = row
        self.algo = (row.get("checksum_algorithm") or "").upper() if enabled else ""
        self.want = row.get("checksum") if self.algo == "CRC64NVME" else None
        self._crc = 0
        if self.want:
            from awscrt import checksums as _crt
            self._fn = _crt.crc64nvme
            self._md5 = None
        else:
            self._fn = None
            self._md5 = hashlib.md5(usedforsecurity=False) if (enabled and row.get("md5")) else None

    def update(self, chunk):
        if self._fn is not None:
            self._crc = self._fn(chunk, self._crc)
        elif self._md5 is not None:
            self._md5.update(chunk)

    def check(self):
        if self.want:
            got = base64.b64encode(self._crc.to_bytes(8, "big")).decode("ascii")
            if got != self.want:
                raise ValueError(f"CRC64NVME mismatch (manifest {self.want}, got {got})")
        elif self._md5 is not None and self._md5.hexdigest() != self.row["md5"]:
            raise ValueError(f"md5 mismatch (manifest {self.row['md5']}, got {self._md5.hexdigest()})")


@dataclass
class RestoreStats:
    files_total: int = 0          # rows seen so far (grows while streaming)
    bytes_total: int = 0
    files_done: int = 0
    bytes_done: int = 0
    skipped: int = 0
    failed: int = 0
    requests: int = 0
    bytes_fetched: int = 0
    streaming_done: bool = False
    errors: list = field(default_factory=list)


class RestoreEngine:
    """Restores manifest rows to a local directory.

    Rows are grouped per tar and nearby ranges are merged (gap <= MERGE_GAP,
    span <= MAX_SPAN) so one ranged GET serves many small members. Each member
    slice is a self-contained single-member tar (header + data + padding), so it
    is parsed with tarfile exactly like fss3-restore.py does."""

    MERGE_GAP = 1 << 20          # 1 MiB of unwanted bytes is cheaper than a GET
    MAX_SPAN = 64 << 20          # cap one GET's memory footprint
    STREAM_MIN = 64 << 20        # members at least this big stream to disk
    HEADER_SLACK = 1 << 20       # tar header blocks allowed on top of the file size
    GROUP_ROWS = 20_000          # rows buffered before dispatching GETs

    def __init__(self, cfg: Config, dest_dir, overwrite=False, verify_md5=True,
                 concurrency=32, on_error=None):
        self.cfg = cfg
        self.dest = os.path.abspath(dest_dir)
        self.overwrite = overwrite
        self.verify_md5 = verify_md5
        self.concurrency = concurrency
        self.stats = RestoreStats()
        self.cancel = threading.Event()
        self._lock = threading.Lock()
        self._on_error = on_error
        # the chosen destination itself may be (or sit under) a symlink, e.g.
        # /tmp on macOS; only what gets created *below* it is checked
        os.makedirs(self.dest, exist_ok=True)
        self._dest_real = os.path.realpath(self.dest)
        old = os.umask(0); os.umask(old)
        self._file_mode = 0o666 & ~old           # what a plain open() would give
        session = boto3.Session(profile_name=cfg.profile or None)
        from botocore.config import Config as BotoConfig
        self._s3 = session.client("s3", region_name=cfg.region or None,
                                  config=BotoConfig(max_pool_connections=concurrency + 4,
                                                    retries={"mode": "adaptive",
                                                             "max_attempts": 8}))

    # -- helpers -------------------------------------------------------------
    def _err(self, msg, n=1):
        with self._lock:
            self.stats.failed += n
            if len(self.stats.errors) < 500:
                self.stats.errors.append(msg)
        if self._on_error:
            self._on_error(msg)

    @staticmethod
    def rel_for(kind, sel_path, source_key):
        """Destination path relative to dest: a selected folder keeps its own
        name (photos/2020/a.jpg when 'photos' is selected); a file lands flat;
        a whole tar restores its members at their full archived paths."""
        if kind in ("file", "member"):
            return posixpath.basename(source_key)
        if kind == "tar":
            return source_key
        if not sel_path:
            return source_key
        parent = posixpath.dirname(sel_path)
        return source_key[len(parent) + 1:] if parent else source_key

    def _dest_path(self, rel):
        safe = os.path.normpath(rel).lstrip(os.sep)
        if safe == ".." or safe.startswith(".." + os.sep) or os.path.isabs(safe):
            raise ValueError(f"unsafe path {rel!r}")
        return os.path.join(self.dest, safe)

    # -- planning ------------------------------------------------------------
    def _plan(self, items):
        """items: [(row, rel)] -> [(tar_key, span_start, span_end, [(row, rel)])]"""
        by_tar = {}
        for row, rel in items:
            by_tar.setdefault(row["tar_key"], []).append((row, rel))
        spans = []
        for tar_key, lst in by_tar.items():
            lst.sort(key=lambda x: x[0]["byte_start"])
            cur, cs, ce = [], None, None
            for row, rel in lst:
                s, e = row["byte_start"], row["byte_end"]
                big = e - s + 1 >= self.STREAM_MIN
                if cur and not big and (s - ce - 1 <= self.MERGE_GAP) and \
                        (max(ce, e) - cs + 1 <= self.MAX_SPAN):
                    cur.append((row, rel))
                    ce = max(ce, e)
                else:
                    if cur:
                        spans.append((tar_key, cs, ce, cur))
                    cur, cs, ce = [(row, rel)], s, e
            if cur:
                spans.append((tar_key, cs, ce, cur))
        return spans

    # -- safe writing ----------------------------------------------------------
    def _check_range(self, row):
        """A member's byte range is its header plus the data padded to 512 B;
        anything far larger means a damaged or tampered manifest row -- refuse it
        rather than downloading (and buffering) an arbitrary range."""
        n = row["byte_end"] - row["byte_start"] + 1
        if n <= 0 or n > row["filesize"] + self.HEADER_SLACK:
            raise ValueError(f"byte range {row['byte_start']}-{row['byte_end']} does not fit a "
                             f"{row['filesize']}-byte file; manifest row looks wrong")

    def _open_dest(self, rel):
        """Create the temp file for `rel` so that nothing can redirect the write
        outside the destination: the parent must really resolve inside it (a
        symlinked folder planted there is refused) and the temp file is created
        fresh (O_EXCL, O_NOFOLLOW, random name). Returns (dest, tmp, file)."""
        dest = self._dest_path(rel)
        parent = os.path.dirname(dest)
        self._make_dirs(parent)
        tmp = os.path.join(parent, f".{os.path.basename(dest)}.restoring-{uuid.uuid4().hex[:8]}")
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        return dest, tmp, os.fdopen(fd, "wb")

    def _make_dirs(self, parent):
        """Create `parent` below the destination one component at a time and
        refuse any component that is a symlink (or not a directory). os.makedirs
        would follow a symlinked folder planted in the destination and create
        directories outside it before any check could run."""
        rel = os.path.relpath(parent, self.dest)
        if rel == ".":
            return
        cur = self.dest
        for part in rel.split(os.sep):
            cur = os.path.join(cur, part)
            try:
                st = os.lstat(cur)
            except FileNotFoundError:
                try:
                    os.mkdir(cur)
                except FileExistsError:      # another restore thread made it
                    pass
                st = os.lstat(cur)
            if stat.S_ISLNK(st.st_mode) or not stat.S_ISDIR(st.st_mode):
                raise ValueError(f"destination folder {cur} is a symlink or not a folder; "
                                 f"refusing to write through it")

    def _finish(self, dest, tmp, mtime, nbytes):
        os.chmod(tmp, self._file_mode)
        os.replace(tmp, dest)          # replaces a symlink at dest, never follows it
        try:
            os.utime(dest, (mtime, mtime), follow_symlinks=False)   # original mtime
        except (OSError, NotImplementedError):
            pass
        with self._lock:
            self.stats.files_done += 1
            self.stats.bytes_done += nbytes

    def _stream_member(self, tar_key, row, rel):
        """One large member: stream the ranged GET through tarfile's streaming
        mode straight to disk, hashing as it goes -- memory stays bounded."""
        resp = self._s3.get_object(Bucket=self.cfg.bucket, Key=tar_key.lstrip("/"),
                                   Range=f"bytes={row['byte_start']}-{row['byte_end']}")
        with self._lock:
            self.stats.requests += 1
            self.stats.bytes_fetched += row["byte_end"] - row["byte_start"] + 1
        ver = _Verifier(row, self.verify_md5)
        dest, tmp, out = self._open_dest(rel)
        try:
            with out, tarfile.open(fileobj=resp["Body"], mode="r|") as tf:
                m = tf.next()
                if m is None or not m.isfile():
                    raise ValueError("no regular-file tar member at recorded offset")
                src = tf.extractfile(m)
                n = 0
                while True:
                    buf = src.read(8 << 20)
                    if not buf:
                        break
                    ver.update(buf); out.write(buf); n += len(buf)
            ver.check()
            self._finish(dest, tmp, m.mtime, n)
        except BaseException:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise

    # -- execution -----------------------------------------------------------
    def _fetch_span(self, span):
        tar_key, cs, ce, members = span
        if self.cancel.is_set():
            return
        try:
            for row, _ in members:
                self._check_range(row)
        except ValueError as e:
            self._err(f"{members[0][0]['source_key']}: {e}", n=len(members))
            return
        if len(members) == 1 and ce - cs + 1 >= self.STREAM_MIN:
            row, rel = members[0]
            try:
                self._stream_member(tar_key, row, rel)
            except Exception as e:
                self._err(f"{row['source_key']}: {type(e).__name__}: {e}")
            return
        try:
            resp = self._s3.get_object(Bucket=self.cfg.bucket, Key=tar_key.lstrip("/"),
                                       Range=f"bytes={cs}-{ce}")
            chunk = resp["Body"].read()
        except Exception as e:
            code = getattr(e, "response", {}).get("Error", {}).get("Code", "")
            hint = (" (archive storage class: restore the tar from Glacier first)"
                    if code == "InvalidObjectState" else "")
            self._err(f"GET s3://{self.cfg.bucket}/{tar_key} bytes {cs}-{ce} failed: "
                      f"{type(e).__name__}: {e}{hint}", n=len(members))
            return
        with self._lock:
            self.stats.requests += 1
            self.stats.bytes_fetched += len(chunk)
        for row, rel in members:
            if self.cancel.is_set():
                return
            try:
                off = row["byte_start"] - cs
                piece = chunk[off: off + (row["byte_end"] - row["byte_start"] + 1)]
                with tarfile.open(fileobj=io.BytesIO(piece), mode="r:") as tf:
                    m = tf.next()
                    if m is None or not m.isfile():
                        raise ValueError("no regular-file tar member at recorded offset")
                    data = tf.extractfile(m).read()
                ver = _Verifier(row, self.verify_md5)
                ver.update(data)
                ver.check()
                dest, tmp, out = self._open_dest(rel)
                try:
                    with out:
                        out.write(data)
                    self._finish(dest, tmp, m.mtime, len(data))
                except BaseException:
                    try:
                        os.unlink(tmp)
                    except OSError:
                        pass
                    raise
            except Exception as e:
                self._err(f"{row['source_key']}: {type(e).__name__}: {e}")

    def run(self, manifest: Manifest, selections, as_of=None):
        """Blocking. Streams rows, dispatches GETs with bounded parallelism.
        as_of: restore the newest copies archived at or before this time."""
        pool = ThreadPoolExecutor(max_workers=self.concurrency,
                                  thread_name_prefix="restore")
        inflight = set()
        buf = []

        def dispatch():
            nonlocal buf, inflight
            for span in self._plan(buf):
                while len(inflight) >= self.concurrency * 2:
                    done, inflight = wait(inflight, return_when=FIRST_COMPLETED)
                    for f in done:
                        f.result()
                if self.cancel.is_set():
                    break
                inflight.add(pool.submit(self._fetch_span, span))
            buf = []

        try:
            for kind, sel, batch in manifest.iter_batches(selections, as_of=as_of):
                if self.cancel.is_set():
                    break
                # newest-last within a batch, so a re-archived path ends on its
                # newest copy when both versions fall in one batch
                rows = sorted(batch.to_pylist(), key=lambda r: r["archive_ts"])
                for r in rows:
                    with self._lock:
                        self.stats.files_total += 1
                        self.stats.bytes_total += r["filesize"]
                    if r["byte_start"] < 0:
                        self._err(f"{r['source_key']}: archived with --compress; "
                                  f"byte-range restore impossible")
                        continue
                    rel = self.rel_for(kind, sel, r["source_key"])
                    try:
                        dest = self._dest_path(rel)
                    except ValueError as e:
                        self._err(f"{r['source_key']}: {e}")
                        continue
                    if not self.overwrite and os.path.exists(dest):
                        with self._lock:
                            self.stats.skipped += 1
                        continue
                    buf.append((r, rel))
                if len(buf) >= self.GROUP_ROWS:
                    dispatch()
            if buf and not self.cancel.is_set():
                dispatch()
            self.stats.streaming_done = True
            for f in inflight:
                f.result()
        finally:
            pool.shutdown(wait=True, cancel_futures=True)
            self.stats.streaming_done = True
        return self.stats
