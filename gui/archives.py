"""
archives.py -- the named list of archives the GUI knows about.

An archive is a tar bucket + an S3 Tables manifest (table bucket, namespace,
table) in a Region, reached with an AWS profile. Entries are stored in the
app settings as JSON and shown by name only, so no account id ever reaches
the screen (the table bucket ARN is kept for the archiver, never displayed).

No Qt here: the settings object only needs get(key, default) / set(key, value).
"""
import json

KEYS = ("profile", "region", "bucket", "table_bucket", "table_bucket_arn",
        "namespace", "table_name")
_IDENT = ("profile", "region", "bucket", "table_bucket", "namespace", "table_name")
SETTING = "archives"
LEGACY_SETTING = "archive_history"     # the old automatic "recent archives" list


def same_target(a, b):
    return all((a.get(k) or "") == (b.get(k) or "") for k in _IDENT)


def default_name(e):
    tb = e.get("table_bucket") or (e.get("table_bucket_arn") or "").rsplit("/", 1)[-1]
    name = f"{tb}/{e.get('table_name') or 'tar_manifest'}" if tb else (e.get("bucket") or "archive")
    if e.get("namespace") and e["namespace"] != "small_files_archive":
        name = f"{tb}/{e['namespace']}.{e.get('table_name')}"
    return name


class ArchiveRegistry:
    def __init__(self, settings):
        self.s = settings
        self._migrate()

    # -- storage ---------------------------------------------------------------
    def _load(self):
        try:
            v = json.loads(self.s.get(SETTING, "") or "[]")
            return [e for e in v if isinstance(e, dict) and e.get("name") and e.get("bucket")]
        except ValueError:
            return []

    def _save(self, entries):
        self.s.set(SETTING, json.dumps(entries))

    def _migrate(self):
        """First start with this version: turn the old recent-archives history
        and the current connection into named entries (once)."""
        if self.s.get(SETTING, None) not in (None, ""):
            return
        old = []
        try:
            old = [e for e in json.loads(self.s.get(LEGACY_SETTING, "[]") or "[]")
                   if isinstance(e, dict) and e.get("bucket")]
        except ValueError:
            pass
        cur = self.current_config()
        if cur.get("bucket") and not any(same_target(cur, e) for e in old):
            old.insert(0, cur)
        entries = []
        for e in old:
            e = {k: e.get(k, "") or "" for k in KEYS}
            if not e["table_bucket"] and e["table_bucket_arn"]:
                e["table_bucket"] = e["table_bucket_arn"].rsplit("/", 1)[-1]
            if any(same_target(e, x) for x in entries):
                continue
            e["name"] = self._unique(default_name(e), entries)
            entries.append(e)
        self._save(entries)

    @staticmethod
    def _unique(name, entries, ignore=None):
        taken = {e["name"] for e in entries if e is not ignore}
        if name not in taken:
            return name
        i = 2
        while f"{name} ({i})" in taken:
            i += 1
        return f"{name} ({i})"

    # -- queries ---------------------------------------------------------------
    def list(self):
        return self._load()

    def names(self):
        return [e["name"] for e in self._load()]

    def get(self, name):
        return next((e for e in self._load() if e["name"] == name), None)

    def current_config(self):
        e = {k: self.s.get(k, "") or "" for k in KEYS}
        if not e["table_bucket"] and e["table_bucket_arn"]:
            e["table_bucket"] = e["table_bucket_arn"].rsplit("/", 1)[-1]
        return e

    def current_name(self):
        cur = self.current_config()
        return next((e["name"] for e in self._load() if same_target(e, cur)), None)

    # -- changes ---------------------------------------------------------------
    def add(self, entry, name=None):
        """Add (or update) an archive; an entry for the same target keeps one
        row. Returns the name it is listed under."""
        entries = self._load()
        e = {k: entry.get(k, "") or "" for k in KEYS}
        old = next((x for x in entries if same_target(x, e)), None)
        wanted = (name or "").strip() or (old["name"] if old else default_name(e))
        if old is not None:
            entries.remove(old)
        e["name"] = self._unique(wanted, entries)
        entries.insert(0, e)
        self._save(entries)
        return e["name"]

    def rename(self, name, new_name):
        new_name = (new_name or "").strip()
        if not new_name:
            raise ValueError("The name can't be empty.")
        entries = self._load()
        e = next((x for x in entries if x["name"] == name), None)
        if e is None:
            raise KeyError(name)
        if new_name != name and new_name in {x["name"] for x in entries}:
            raise ValueError(f"'{new_name}' is already in the list.")
        e["name"] = new_name
        self._save(entries)
        return new_name

    def remove(self, name):
        """Forget an archive (its data in AWS is not touched)."""
        self._save([e for e in self._load() if e["name"] != name])

    def activate(self, name):
        """Make this archive the current connection (settings the rest of the app
        reads). Returns the entry."""
        e = self.get(name)
        if e is None:
            raise KeyError(name)
        for k in KEYS:
            self.s.set(k, e.get(k, "") or "")
        return e
