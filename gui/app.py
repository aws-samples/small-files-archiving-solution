"""
Small Files Archiver -- desktop GUI (PySide6).

    Left  : local disk browser
    Right : archive browser -- the *untarred* file tree reconstructed from the
            S3 Tables manifest (tar objects themselves stay hidden)
    Bottom: monitoring pane -- per-job progress, per-tar bundling / uploading
            bars, log; overall progress in the status bar

Archive (left -> right) runs ../archiver/fss3-archiver-s3tables-mp.py as a subprocess
with --progress and renders its JSON snapshots. Restore (right -> left) runs
backend.RestoreEngine in a thread: coalesced byte-range GETs, MD5 verified.

Run:  ./run-gui.sh      (or .venv/bin/python app.py)
"""
import os
import sys
import json
import time
import signal
import posixpath
import traceback
from datetime import datetime
from pathlib import Path

from PySide6.QtCore import (Qt, QObject, QRunnable, QThreadPool, Signal, QTimer,
                            QProcess, QSettings, QAbstractTableModel, QModelIndex,
                            QDir, QSize, QProcessEnvironment, QItemSelectionModel)
from PySide6.QtGui import QAction, QIcon, QKeySequence, QFont
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QSplitter, QTreeView, QTableView,
    QFileSystemModel, QVBoxLayout, QHBoxLayout, QLineEdit, QPushButton, QLabel,
    QToolBar, QStatusBar, QProgressBar, QTabWidget, QPlainTextEdit, QTreeWidget,
    QTreeWidgetItem, QHeaderView, QDialog, QFormLayout, QDialogButtonBox,
    QSpinBox, QComboBox, QCheckBox, QMessageBox, QAbstractItemView, QStyle,
    QFileDialog, QGroupBox, QRadioButton, QButtonGroup, QToolButton, QSizePolicy,
    QFileIconProvider, QInputDialog)

import backend
from backend import Config, Manifest, RestoreEngine, human, mask, tilde

HERE = Path(__file__).resolve().parent
DEFAULT_ARCHIVER = HERE.parent / "archiver" / "fss3-archiver-s3tables-mp.py"
PROGRESS_TAG = "@@PROGRESS "
STORAGE_CLASSES = ["STANDARD", "GLACIER_IR", "STANDARD_IA", "INTELLIGENT_TIERING",
                   "GLACIER", "DEEP_ARCHIVE"]


def fmt_ts(ts):
    if ts is None:
        return ""
    try:
        return ts.astimezone().strftime("%Y-%m-%d %H:%M")
    except Exception:
        return str(ts)


def fmt_dur(sec):
    if sec is None or sec < 0 or sec == float("inf"):
        return "--"
    sec = int(sec)
    h, r = divmod(sec, 3600)
    m, s = divmod(r, 60)
    return f"{h}h{m:02d}m" if h else (f"{m}m{s:02d}s" if m else f"{s}s")


# --------------------------------------------------------------------------- #
# Background calls
# --------------------------------------------------------------------------- #
class _Signals(QObject):
    done = Signal(object)
    failed = Signal(str)


class Task(QRunnable):
    def __init__(self, fn, *a, **kw):
        super().__init__()
        self.fn, self.a, self.kw = fn, a, kw
        self.signals = _Signals()

    def run(self):
        try:
            res = self.fn(*self.a, **self.kw)
        except Exception as e:
            self.signals.failed.emit(f"{type(e).__name__}: {e}\n\n{traceback.format_exc(limit=4)}")
        else:
            self.signals.done.emit(res)


def run_async(owner, fn, on_done, on_fail, *a, **kw):
    t = Task(fn, *a, **kw)
    t.signals.done.connect(on_done)
    t.signals.failed.connect(on_fail)
    owner._tasks = getattr(owner, "_tasks", set())
    owner._tasks.add(t.signals)   # keep signals alive until delivered
    t.signals.done.connect(lambda *_: owner._tasks.discard(t.signals))
    t.signals.failed.connect(lambda *_: owner._tasks.discard(t.signals))
    QThreadPool.globalInstance().start(t)


# --------------------------------------------------------------------------- #
# Settings
# --------------------------------------------------------------------------- #
class Settings:
    KEYS = ["profile", "region", "bucket", "table_bucket", "table_bucket_arn", "namespace",
            "table_name", "archiver", "workdir", "page_size", "restore_concurrency"]

    def __init__(self):
        self.q = QSettings("aws-storage-field", "SmallFilesArchiverGUI")

    def get(self, k, default=""):
        v = self.q.value(k, default)
        return v if v is not None else default

    def set(self, k, v):
        self.q.setValue(k, v)

    def config(self):
        name = self.get("table_bucket")
        arn = self.get("table_bucket_arn")
        # the stored ARN is only trusted while it still names the chosen bucket
        # and Region; otherwise it is re-derived on connect
        if arn and ((name and not arn.endswith("/" + name))
                    or f":{self.get('region')}:" not in arn):
            arn = ""
        return Config(profile=self.get("profile"), region=self.get("region"),
                      bucket=self.get("bucket"), table_bucket=name,
                      table_bucket_arn=arn,
                      namespace=self.get("namespace", "small_files_archive") or "small_files_archive",
                      table_name=self.get("table_name", "tar_manifest") or "tar_manifest")

    def archiver(self):
        # a saved path that no longer exists (e.g. the repo was reorganized)
        # falls back to the archiver shipped next to this GUI
        p = self.get("archiver", "") or ""
        return p if p and os.path.isfile(p) else str(DEFAULT_ARCHIVER)

    def workdir(self):
        p = self.get("workdir", "") or ""
        return p if p and os.path.isdir(p) else str(HERE.parent)

    def page_size(self):
        return int(self.get("page_size", 5000) or 5000)

    def restore_concurrency(self):
        return int(self.get("restore_concurrency", 32) or 32)


class SettingsDialog(QDialog):
    def __init__(self, settings: Settings, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Connection settings")
        self.s = settings
        f = QFormLayout(self)
        self.w = {}

        def line(k, label, ph="", default=""):
            v = self.s.get(k, default)
            # real values (a bucket name may contain 12 digits); paths only are
            # shown as ~/… and expanded again on Save
            e = QLineEdit(tilde(v) if k in ("archiver", "workdir") else v)
            e.setPlaceholderText(ph)
            e.setMinimumWidth(460)
            f.addRow(label, e)
            self.w[k] = e

        line("profile", "AWS profile", "default credential chain if empty")
        line("region", "Region", "ap-northeast-2")
        line("bucket", "Tar bucket", "bucket that stores the tar objects")
        line("table_bucket", "Table bucket", "S3 Tables table bucket name",
             self.s.config().table_bucket)
        line("namespace", "Namespace", "small_files_archive", "small_files_archive")
        line("table_name", "Manifest table", "tar_manifest", "tar_manifest")
        f.addRow(QLabel("<b>Advanced</b>"))
        line("archiver", "Archiver script", tilde(str(DEFAULT_ARCHIVER)), str(DEFAULT_ARCHIVER))
        line("workdir", "Working dir (logs/)", tilde(str(HERE.parent)), str(HERE.parent))
        self.page = QSpinBox(); self.page.setRange(100, 1_000_000); self.page.setSingleStep(1000)
        self.page.setValue(self.s.page_size())
        f.addRow("Files per folder page", self.page)
        self.conc = QSpinBox(); self.conc.setRange(1, 256)
        self.conc.setValue(self.s.restore_concurrency())
        f.addRow("Restore parallel GETs", self.conc)
        bb = QDialogButtonBox(QDialogButtonBox.Save | QDialogButtonBox.Cancel)
        wiz = bb.addButton("Setup wizard…", QDialogButtonBox.ResetRole)
        wiz.setToolTip("Pick or create the table bucket, tables and tar bucket by name")
        wiz.clicked.connect(self._wizard)
        bb.accepted.connect(self._save)
        bb.rejected.connect(self.reject)
        f.addRow(bb)

    def _wizard(self):
        from setup_wizard import SetupWizard
        if SetupWizard(self.s, self).exec():
            self.accept()

    def _save(self):
        for k, e in self.w.items():
            v = e.text().strip()
            self.s.set(k, os.path.expanduser(v) if k in ("archiver", "workdir") else v)
        self.s.set("page_size", self.page.value())
        self.s.set("restore_concurrency", self.conc.value())
        self.accept()


# --------------------------------------------------------------------------- #
# Left pane: local disk
# --------------------------------------------------------------------------- #
class PathBar(QWidget):
    navigate = Signal(str)
    up = Signal()

    def __init__(self, title, parent=None):
        super().__init__(parent)
        h = QHBoxLayout(self); h.setContentsMargins(0, 0, 0, 0)
        lbl = QLabel(f"<b>{title}</b>")
        self.btn_up = QToolButton(); self.btn_up.setText("↑"); self.btn_up.setToolTip("Up one folder")
        self.edit = QLineEdit()
        self.btn_up.clicked.connect(self.up.emit)
        self.edit.returnPressed.connect(lambda: self.navigate.emit(self.edit.text().strip()))
        h.addWidget(lbl); h.addWidget(self.btn_up); h.addWidget(self.edit, 1)

    def set_path(self, p):
        self.edit.setText(p)


class LocalPane(QWidget):
    # Name, Size, Kind, Date Modified -- Name gets the rest of the pane
    DEFAULT_WIDTHS = [None, 90, 110, 150]

    def __init__(self, settings=None, parent=None):
        super().__init__(parent)
        self.s = settings
        self._applying_widths = True
        v = QVBoxLayout(self); v.setContentsMargins(4, 4, 4, 4)
        self.bar = PathBar("Local disk")
        self.model = QFileSystemModel(self)
        self.model.setRootPath("/")
        self.model.setFilter(QDir.AllEntries | QDir.NoDotAndDotDot | QDir.Hidden)
        self.view = QTreeView()
        self.view.setModel(self.model)
        self.view.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.view.setSortingEnabled(True)
        self.view.sortByColumn(0, Qt.AscendingOrder)
        self.view.setItemsExpandable(False)
        self.view.setRootIsDecorated(False)
        self.view.setUniformRowHeights(True)
        # every column user-resizable, like the archive pane: drag a divider,
        # double-click to fit, right-click for fit-all / reset; widths remembered
        hh = self.view.header()
        hh.setSectionResizeMode(QHeaderView.Interactive)
        hh.setStretchLastSection(True)
        hh.setMinimumSectionSize(40)
        hh.setSectionsMovable(False)
        hh.sectionResized.connect(self._save_widths)
        hh.sectionHandleDoubleClicked.connect(self.view.resizeColumnToContents)
        hh.setToolTip("Drag a divider to resize a column; double-click it to fit the "
                      "contents; right-click for more")
        hh.setContextMenuPolicy(Qt.CustomContextMenu)
        hh.customContextMenuRequested.connect(self._header_menu)
        self.view.setHorizontalScrollMode(QAbstractItemView.ScrollPerPixel)
        self.view.doubleClicked.connect(self._open)
        self.bar.navigate.connect(self.cd)
        self.bar.up.connect(lambda: self.cd(str(Path(self.cwd).parent)))
        v.addWidget(self.bar); v.addWidget(self.view, 1)
        self.cwd = str(Path.home())
        self.cd(self.cwd)
        self._apply_widths()
        QTimer.singleShot(0, self._apply_widths)   # again once the pane is laid out

    def _saved_widths(self):
        raw = self.s.get("colw_local", "") if self.s else ""
        w = [int(x) for x in str(raw).split(",") if x.strip().isdigit()]
        return w if len(w) == 4 else None

    def _apply_widths(self, force_default=False):
        self._applying_widths = True
        try:
            widths = None if force_default else self._saved_widths()
            if widths is None:
                widths = list(self.DEFAULT_WIDTHS)
                widths[0] = max(200, self.view.viewport().width() - sum(widths[1:]) - 4)
            hh = self.view.header()
            for c, w in enumerate(widths[:3]):       # the last column stretches
                hh.resizeSection(c, w)
        finally:
            self._applying_widths = False

    def _save_widths(self, *_):
        if self._applying_widths or not self.s:
            return
        hh = self.view.header()
        self.s.set("colw_local", ",".join(str(hh.sectionSize(c)) for c in range(4)))

    def _header_menu(self, pos):
        from PySide6.QtWidgets import QMenu
        m = QMenu(self)
        fit = m.addAction("Fit columns to contents")
        reset = m.addAction("Reset column widths")
        act = m.exec(self.view.header().mapToGlobal(pos))
        if act is fit:
            for c in range(3):
                self.view.resizeColumnToContents(c)
        elif act is reset and self.s:
            self.s.q.remove("colw_local")
            self._apply_widths(force_default=True)

    def cd(self, path):
        path = os.path.abspath(os.path.expanduser(path or "/"))
        if not os.path.isdir(path):
            return
        # a selection made in the previous folder must not follow us: it is no
        # longer visible, and Archive / Restore would silently act on it
        self.view.clearSelection()
        self.cwd = path
        self.view.setRootIndex(self.model.index(path))
        self.bar.set_path(path)

    def _open(self, idx):
        p = self.model.filePath(idx)
        if os.path.isdir(p):
            self.cd(p)

    def selected_paths(self):
        """Selected items in the folder being shown (never stale ones)."""
        rows = {self.model.filePath(i) for i in self.view.selectionModel().selectedRows(0)}
        return sorted(p for p in rows if os.path.dirname(p.rstrip("/")) == self.cwd.rstrip("/")
                      or (self.cwd == "/" and os.path.dirname(p) == "/"))

    def restore_target(self):
        """Selected single folder, else the current folder."""
        sel = self.selected_paths()
        if len(sel) == 1 and os.path.isdir(sel[0]):
            return sel[0]
        return self.cwd


# --------------------------------------------------------------------------- #
# Right pane: archive (untarred view of the manifest)
# --------------------------------------------------------------------------- #
class ArchiveModel(QAbstractTableModel):
    """One table model, three column layouts:
       files   -- untarred tree from the manifest (dirs + files)
       tars    -- tar objects in S3 (prefixes + objects)
       members -- contents of one opened tar, in on-disk order"""
    COLS = {
        "files":   ["Name", "Size", "Files", "Archived", "Tar object", "Byte range"],
        "tars":    ["Name", "Size", "Storage class", "Last modified"],
        "members": ["Path in archive", "Size", "Archived", "Byte range", "Checksum"],
    }

    def __init__(self, parent=None):
        super().__init__(parent)
        self.mode = "files"
        self.items = []
        prov = QFileIconProvider()
        self.icon_dir = prov.icon(QFileIconProvider.Folder)
        self.icon_file = prov.icon(QFileIconProvider.File)
        self.icon_tar = QApplication.style().standardIcon(QStyle.SP_DriveHDIcon)

    def set_items(self, items, mode=None):
        self.beginResetModel()
        if mode:
            self.mode = mode
        self.items = items
        self.endResetModel()

    def append(self, items):
        if not items:
            return
        n = len(self.items)
        self.beginInsertRows(QModelIndex(), n, n + len(items) - 1)
        self.items.extend(items)
        self.endInsertRows()

    def rowCount(self, parent=QModelIndex()):
        return 0 if parent.isValid() else len(self.items)

    def columnCount(self, parent=QModelIndex()):
        return len(self.COLS[self.mode])

    def headerData(self, s, o, role=Qt.DisplayRole):
        if o == Qt.Horizontal and role == Qt.DisplayRole:
            return self.COLS[self.mode][s]

    @staticmethod
    def _range(it):
        return ("compressed" if it["byte_start"] < 0
                else f"{it['byte_start']:,}–{it['byte_end']:,}")

    def data(self, idx, role=Qt.DisplayRole):
        it = self.items[idx.row()]
        c = idx.column()
        kind = it["kind"]
        if role == Qt.TextAlignmentRole and c in (1, 2) and self.mode == "files":
            return int(Qt.AlignRight | Qt.AlignVCenter)
        if role == Qt.TextAlignmentRole and c == 1:
            return int(Qt.AlignRight | Qt.AlignVCenter)
        if self.mode == "tars":
            if role == Qt.DisplayRole:
                if c == 0:
                    return it["name"]
                if kind == "prefix":
                    return ""
                return [None, human(it["size"]), it["storage_class"],
                        fmt_ts(it["last_modified"])][c]
            if role == Qt.DecorationRole and c == 0:
                return (self.icon_dir if kind == "prefix"
                        else self.icon_tar if it["is_tar"] else self.icon_file)
            if role == Qt.ToolTipRole:
                if kind == "prefix":
                    return it["path"]
                tip = it["key"]
                if it["storage_class"] in ("GLACIER", "DEEP_ARCHIVE"):
                    tip += "\nArchive storage class: restore the object in S3 before reading files."
                return tip + ("\nDouble-click to list its files" if it["is_tar"] else "")
            return None
        if self.mode == "members":
            if role == Qt.DisplayRole:
                return [it["source_key"], human(it["filesize"]), fmt_ts(it["archive_ts"]),
                        self._range(it), it.get("checksum") or it.get("md5") or ""][c]
            if role == Qt.DecorationRole and c == 0:
                return self.icon_file
            if role == Qt.ToolTipRole:
                return (f"{it['source_key']}\n"
                        + (f"CRC64NVME: {it['checksum']}\n" if it.get("checksum") else "")
                        + f"MD5: {it.get('md5') or '-'}")
            return None
        # files mode
        isdir = kind == "dir"
        if role == Qt.DisplayRole:
            if c == 0:
                # short badge so eliding a long path never hides the file name
                return it["name"] + ("" if isdir or it.get("versions", 1) == 1
                                     else f"  [{it['versions']}×]")
            if c == 1:
                return human(it.get("bytes") if isdir else it.get("filesize"))
            if c == 2:
                return f"{it['files']:,}" if isdir else ""
            if c == 3:
                return fmt_ts(it.get("archive_ts"))
            if c == 4:
                return "" if isdir else posixpath.basename(it["tar_key"])
            if c == 5:
                return "" if isdir else self._range(it)
        if role == Qt.DecorationRole and c == 0:
            return self.icon_dir if isdir else self.icon_file
        if role == Qt.ToolTipRole:
            if isdir:
                return f"{it['path']}\n{it['files']:,} files, {human(it['bytes'])} (recursive)"
            ver = it.get("versions", 1)
            return (f"{it['source_key']}\ntar: s3://…/{it['tar_key']}\n"
                    f"md5: {it.get('md5') or '-'}"
                    + (f"\n{ver} archived versions — showing the newest" if ver > 1 else ""))


def selection_label(kind, path):
    if kind == "dir":
        return "📁 /" + path
    if kind == "tar":
        return "📦 " + path
    if kind == "member":
        return f"{posixpath.basename(path[0])} ▸ {path[1]}"
    return "/" + path


class S3SourcePane(QWidget):
    """Browse a source S3 bucket (one prefix level at a time, paged)."""

    def __init__(self, settings, parent=None):
        super().__init__(parent)
        self.s = settings
        self.bucket = ""
        self.cwd = ""
        self._token = None
        self._req = 0
        v = QVBoxLayout(self); v.setContentsMargins(0, 0, 0, 0)
        top = QHBoxLayout()
        top.addWidget(QLabel("Bucket:"))
        self.buckets = QComboBox(); self.buckets.setEditable(True); self.buckets.setMinimumContentsLength(18)
        self.buckets.setToolTip("Buckets in the Region from Settings; you can also type a name")
        self.btn_reload = QToolButton(); self.btn_reload.setText("⟳"); self.btn_reload.setToolTip("Reload bucket list")
        top.addWidget(self.buckets, 1); top.addWidget(self.btn_reload)
        self.bar = PathBar("S3")
        self.model = ArchiveModel(self)
        self.model.set_items([], mode="tars")
        self.view = QTableView(); self.view.setModel(self.model)
        self.view.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.view.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.view.verticalHeader().setVisible(False); self.view.verticalHeader().setDefaultSectionSize(22)
        self.view.setShowGrid(False); self.view.setWordWrap(False)
        self.view.setTextElideMode(Qt.ElideMiddle)
        hh = self.view.horizontalHeader()
        hh.setSectionResizeMode(QHeaderView.Interactive); hh.setStretchLastSection(True)
        self.model.modelReset.connect(lambda: (hh.resizeSection(0, max(220, self.view.viewport().width() - 340)),
                                               hh.resizeSection(1, 85), hh.resizeSection(2, 110)))
        foot = QHBoxLayout()
        self.info = QLabel(""); self.info.setWordWrap(True)
        self.btn_more = QPushButton("Show more"); self.btn_more.setVisible(False)
        foot.addWidget(self.info, 1); foot.addWidget(self.btn_more)
        v.addLayout(top); v.addWidget(self.bar); v.addWidget(self.view, 1); v.addLayout(foot)
        self.buckets.activated.connect(lambda *_: self.open_bucket(self.buckets.currentText().strip()))
        self.buckets.lineEdit().returnPressed.connect(lambda: self.open_bucket(self.buckets.currentText().strip()))
        self.btn_reload.clicked.connect(self.load_buckets)
        self.bar.navigate.connect(self._navigate_text)
        self.bar.up.connect(self._up)
        self.view.doubleClicked.connect(self._open)
        self.btn_more.clicked.connect(lambda: self._list(append=True))
        self._loaded = False

    def showEvent(self, ev):
        super().showEvent(ev)
        if not self._loaded:
            self._loaded = True
            self.load_buckets()

    def _store(self, bucket):
        cfg = self.s.config()
        return backend.TarStore(backend.Config(profile=cfg.profile, region=cfg.region, bucket=bucket))

    def load_buckets(self):
        cfg = self.s.config()
        self.info.setText("Listing buckets…")

        def ok(names):
            cur = self.s.get("last_src_bucket", "")
            self.buckets.blockSignals(True)
            self.buckets.clear(); self.buckets.addItems(names)
            if cur in names:
                self.buckets.setCurrentText(cur)
            self.buckets.blockSignals(False)
            self.info.setText(f"{len(names)} buckets in {cfg.region}. Pick the source bucket.")
            if cur in names:
                self.open_bucket(cur, self.s.get("last_src_prefix", ""))

        run_async(self, lambda: backend.Provisioner(cfg.profile, cfg.region).list_buckets(), ok,
                  lambda m: self.info.setText(mask(f"Could not list buckets: {m.splitlines()[0]}")))

    def open_bucket(self, bucket, prefix=""):
        if not bucket:
            return
        self.bucket = bucket
        self.s.set("last_src_bucket", bucket)
        self.cd(prefix)

    def cd(self, prefix):
        prefix = prefix.lstrip("/")
        if prefix and not prefix.endswith("/"):
            prefix += "/"
        self.cwd = prefix
        self.s.set("last_src_prefix", prefix)
        self.bar.set_path(f"s3://{self.bucket}/{prefix}")
        self._token = None
        self._list(append=False)

    def _list(self, append):
        if not self.bucket:
            return
        self._req += 1
        req, prefix = self._req, self.cwd
        self.info.setText("Listing…")

        def ok(res):
            if req != self._req:
                return
            dirs, objs, token = res
            for o in objs:
                o["is_tar"] = False        # source objects: plain file icon
            self._token = token
            if append:
                self.model.append(dirs + objs)
            else:
                self.model.set_items(dirs + objs, mode="tars")
            items = self.model.items
            nobj = sum(1 for i in items if i["kind"] == "object")
            size = sum(i["size"] for i in items if i["kind"] == "object")
            self.info.setText(f"{sum(1 for i in items if i['kind'] == 'prefix'):,} folders, {nobj:,} objects"
                              f" · {human(size)} listed" + (" · more available" if token else ""))
            self.btn_more.setVisible(bool(token))

        run_async(self, self._store(self.bucket).list, ok,
                  lambda m: self.info.setText(mask(f"Listing failed: {m.splitlines()[0]}")),
                  prefix, token=self._token if append else None)

    def _navigate_text(self, text):
        t = text.strip()
        if t.startswith("s3://"):
            b, _, p = t[5:].partition("/")
            if b and b != self.bucket:
                self.buckets.setCurrentText(b)
                return self.open_bucket(b, p)
            t = p
        self.cd(t)

    def _up(self):
        parent = posixpath.dirname(self.cwd.rstrip("/"))
        self.cd(parent + "/" if parent else "")

    def _open(self, idx):
        it = self.model.items[idx.row()]
        if it["kind"] == "prefix":
            self.cd(it["path"])

    def selected(self):
        return [self.model.items[i.row()] for i in self.view.selectionModel().selectedRows(0)]


class SourcePane(QWidget):
    """Left pane: Local disk | S3 bucket."""

    def __init__(self, settings, parent=None):
        super().__init__(parent)
        v = QVBoxLayout(self); v.setContentsMargins(4, 4, 4, 4)
        sw = QHBoxLayout(); sw.setContentsMargins(0, 0, 0, 0)
        self.btn_local = QToolButton(); self.btn_local.setText("Local disk")
        self.btn_s3 = QToolButton(); self.btn_s3.setText("S3 bucket")
        grp = QButtonGroup(self); grp.setExclusive(True)
        sw.addWidget(QLabel("Source:"))
        for b in (self.btn_local, self.btn_s3):
            b.setCheckable(True); grp.addButton(b); sw.addWidget(b)
        sw.addStretch(1)
        self.local = LocalPane(settings)
        self.local.layout().setContentsMargins(0, 0, 0, 0)
        self.s3 = S3SourcePane(settings)
        from PySide6.QtWidgets import QStackedWidget
        self.stack = QStackedWidget(); self.stack.addWidget(self.local); self.stack.addWidget(self.s3)
        v.addLayout(sw); v.addWidget(self.stack, 1)
        self.btn_local.clicked.connect(lambda: self.set_mode("local"))
        self.btn_s3.clicked.connect(lambda: self.set_mode("s3"))
        self.set_mode(settings.get("last_source", "local") or "local")
        self.s = settings

    def set_mode(self, mode):
        self.mode = mode
        self.btn_local.setChecked(mode == "local"); self.btn_s3.setChecked(mode == "s3")
        self.stack.setCurrentWidget(self.s3 if mode == "s3" else self.local)
        if hasattr(self, "s"):
            self.s.set("last_source", mode)


class S3ArchiveDialog(QDialog):
    """Archive from an S3 bucket: a selected folder (kept by name), the current
    folder, or selected objects."""

    def __init__(self, bucket, cwd, items, settings, parent=None, manifest=None):
        super().__init__(parent)
        self.setWindowTitle("Archive from S3")
        self.s, self.manifest, self.bucket, self.cwd = settings, manifest, bucket, cwd
        folders = [i for i in items if i["kind"] == "prefix"]
        objects = [i for i in items if i["kind"] == "object"]
        self.err0 = None
        if folders and (objects or len(folders) > 1):
            self.err0 = "Select one folder, or objects only (several folders: archive them one by one)."
        if folders:
            self.mode, self.list_prefix = "folder", folders[0]["path"]
            parent_ = posixpath.dirname(self.list_prefix.rstrip("/"))
            base = parent_ + "/" if parent_ else ""
            label = f"folder s3://{bucket}/{self.list_prefix}"
        elif objects:
            self.mode, self.list_prefix, base = "objects", cwd, cwd
            self.objects = objects
            label = f"{len(objects):,} selected object(s) in s3://{bucket}/{cwd}"
        else:
            self.mode, self.list_prefix = "folder", cwd
            parent_ = posixpath.dirname(cwd.rstrip("/"))
            base = parent_ + "/" if parent_ else ""
            label = f"everything in s3://{bucket}/{cwd}"
        f = QFormLayout(self)
        f.addRow("Source", QLabel(label))
        self.base = QLineEdit(base)
        self.base.setToolTip("Object keys are stored relative to this prefix. The default keeps the "
                             "selected folder's own name.")
        f.addRow("Store keys relative to", self.base)
        self.prefix = QLineEdit(self.s.get("last_prefix", "") or datetime.now().strftime("%Y%m%d"))
        f.addRow("Destination prefix", self.prefix)
        self.layout_cb = QComboBox()
        self.layout_cb.addItem("One folder per run  (<prefix>/<run-id>/)", "run")
        self.layout_cb.addItem("One folder per day  (<prefix>/<YYYYMMDD>/)", "date")
        self.layout_cb.addItem("No subfolder  (<prefix>/)", "flat")
        self.layout_cb.setCurrentIndex(max(0, self.layout_cb.findData(self.s.get("last_layout", "run") or "run")))
        f.addRow("Tar folders", self.layout_cb)
        self.incr = QCheckBox("Incremental — only objects modified since the last successful run of this source")
        self.incr.setEnabled(self.mode == "folder")
        self.incr_lbl = QLabel(""); self.incr_lbl.setWordWrap(True); self.incr_lbl.setStyleSheet("color: gray")
        f.addRow("", self.incr); f.addRow("", self.incr_lbl)
        g = QHBoxLayout()
        self.by_size = QRadioButton("Max tar size"); self.by_count = QRadioButton("Max files per tar")
        self.by_size.setChecked(True)
        self.size = QComboBox(); self.size.setEditable(True)
        self.size.addItems(["20MB", "64MB", "256MB", "512MB", "1GB", "4GB"])
        self.size.setCurrentText(self.s.get("last_size", "256MB") or "256MB")
        self.count = QSpinBox(); self.count.setRange(1, 10_000_000); self.count.setValue(1000)
        g.addWidget(self.by_size); g.addWidget(self.size); g.addSpacing(16); g.addWidget(self.by_count); g.addWidget(self.count)
        f.addRow("Bundling", g)
        self.part = QComboBox(); self.part.addItems(["8MB", "16MB", "32MB", "64MB"])
        self.part.setCurrentText(self.s.get("last_part", "32MB") or "32MB")
        f.addRow("Upload part size", self.part)
        hw = QHBoxLayout()
        self.workers = QSpinBox(); self.workers.setRange(1, 512); self.workers.setValue(os.cpu_count() or 4)
        self.gets = QSpinBox(); self.gets.setRange(1, 4096); self.gets.setValue(256)
        self.gets.setToolTip("Concurrent source GETs in total, spread over the worker processes. Small "
                             "objects make the run GET-bound.")
        self.lists = QSpinBox(); self.lists.setRange(1, 256); self.lists.setValue(16)
        hw.addWidget(self.workers); hw.addWidget(QLabel("GET concurrency")); hw.addWidget(self.gets)
        hw.addWidget(QLabel("list threads")); hw.addWidget(self.lists)
        f.addRow("Worker processes", hw)
        self.sclass = QComboBox(); self.sclass.addItems(STORAGE_CLASSES)
        self.sclass.setCurrentText(self.s.get("last_sclass", "STANDARD") or "STANDARD")
        f.addRow("Tar storage class", self.sclass)
        self.verify = QCheckBox("Compare each object's checksum with the one S3 stored for it")
        self.verify.setChecked(True)
        self.md5 = QCheckBox("Also store MD5 (CRC64NVME is always stored)"); self.md5.setChecked(True)
        self.dirindex = QCheckBox("Directory index (fast folder browsing at scale)"); self.dirindex.setChecked(True)
        for w in (self.verify, self.md5, self.dirindex):
            f.addRow("", w)
        ro = QHBoxLayout()
        self.run_on = QComboBox(); self.run_on.addItem("This computer", None)
        self.run_on.setToolTip("Run the archiver here, or on an EC2 instance through AWS Systems Manager "
                               "(the data then never leaves AWS)")
        self.btn_prep = QPushButton("Prepare instance"); self.btn_prep.setEnabled(False)
        self.btn_prep.setToolTip("Install this archiver version on the instance (otherwise done at start)")
        ro.addWidget(self.run_on, 1); ro.addWidget(self.btn_prep)
        f.addRow("Run on", ro)
        self.warn = QLabel(""); self.warn.setWordWrap(True)
        f.addRow("", self.warn)
        self.run_on.currentIndexChanged.connect(self._sync_run_on)
        self.btn_prep.clicked.connect(self._prepare)
        self._sync_run_on()
        run_async(self, lambda: __import__("remote").Remote(self.s.config()).list_instances(),
                  self._fill_instances, lambda m: None)
        self.preview = QPlainTextEdit(); self.preview.setReadOnly(True); self.preview.setMaximumHeight(110)
        self.preview.setFont(QFont("Menlo", 10))
        f.addRow("Command", self.preview)
        bb = QDialogButtonBox()
        self.btn_run = bb.addButton("Start archiving", QDialogButtonBox.AcceptRole)
        self.btn_copy = bb.addButton("Copy script for EC2", QDialogButtonBox.ActionRole)
        self.btn_copy.setToolTip("Copy a shell script with these settings: paste it into a terminal on "
                                 "an EC2 instance to run the archive there yourself")
        self.btn_copy.clicked.connect(self._copy_script)
        bb.addButton(QDialogButtonBox.Cancel)
        bb.accepted.connect(self._accept); bb.rejected.connect(self.reject)
        f.addRow(bb)
        for w in (self.base, self.prefix):
            w.textChanged.connect(self._refresh)
        for w in (self.size, self.part, self.sclass):
            w.currentTextChanged.connect(self._refresh)
        self.layout_cb.currentIndexChanged.connect(self._refresh)
        for w in (self.count, self.workers, self.gets, self.lists):
            w.valueChanged.connect(self._refresh)
        for w in (self.by_size, self.by_count, self.verify, self.md5, self.dirindex, self.incr):
            w.toggled.connect(self._refresh)
        self._refresh()
        self._lookup_last_run()

    def _fill_instances(self, items):
        last = self.s.get("last_instance", "")
        for it in items:
            cpu = f" · {it['vcpus']} vCPU" if it.get("vcpus") else ""
            self.run_on.addItem(f"EC2 {it['name'] or '(no name)'}  ·  {it['id']}  ·  {it['type']}{cpu}", it)
            if it["id"] == last:
                self.run_on.setCurrentIndex(self.run_on.count() - 1)

    def run_on_instance(self):
        return self.run_on.currentData()

    def _sync_run_on(self):
        inst = self.run_on_instance()
        self.btn_prep.setEnabled(inst is not None)
        # size the run for the machine it runs on (still editable)
        if inst is not None and inst.get("vcpus"):
            w, g = __import__("remote").Remote.suggested(inst["vcpus"])
            self.workers.setValue(w); self.gets.setValue(g)
            self.workers.setToolTip(f"Suggested for {inst['type']} ({inst['vcpus']} vCPU): vCPU + 16, at most 2 per vCPU")
        elif inst is None:
            self.workers.setValue(os.cpu_count() or 4); self.gets.setValue(256)
            self.workers.setToolTip("")
        if inst is None:
            self.warn.setText("<small>Running here reads every object over the internet: S3 data transfer "
                              "out is charged per GB. Pick an EC2 instance in the bucket's Region to keep "
                              "the reads inside AWS — free and much faster.</small>")
            self.warn.setStyleSheet("color: #a05a00")
        else:
            self.warn.setText("<small>Runs on the instance through AWS Systems Manager: reads stay inside "
                              "AWS (no data transfer out), progress shows here, and the run continues if "
                              "this computer sleeps. Use a public subnet or an S3 Gateway VPC Endpoint — "
                              "not a NAT Gateway, which bills every GB.</small>")
            self.warn.setStyleSheet("color: #2d6a2d")

    def _prepare(self):
        inst = self.run_on_instance()
        if inst is None:
            return
        self.btn_prep.setEnabled(False); self.btn_prep.setText("Preparing…")

        def done(msg):
            self.btn_prep.setText("Prepared ✓"); self.warn.setText(f"<small>{msg}</small>")

        def fail(msg):
            self.btn_prep.setText("Prepare instance"); self.btn_prep.setEnabled(True)
            self.warn.setText(f"<small>Prepare failed: {mask(msg.splitlines()[0])}</small>")

        run_async(self, lambda: __import__("remote").Remote(self.s.config()).prepare(inst["id"]),
                  done, fail)

    def _lookup_last_run(self):
        if self.mode != "folder" or self.manifest is None:
            return
        try:
            sid = backend.archiver_module(self.s.archiver()).source_id_for(
                f"s3://{self.bucket}/{self.list_prefix}", [], None)
        except Exception:
            return
        self.incr_lbl.setText("Looking up the last run of this source…")

        def ok(r):
            self.incr_lbl.setText(
                "No previous successful run of this exact source: the first incremental run archives "
                "everything." if r is None else
                f"Last successful run: {fmt_ts(r['start_ts'])} ({r['mode']}, {r['files']:,} objects). "
                f"Incremental archives objects modified after it.")

        run_async(self, self.manifest.last_run, ok, lambda m: self.incr_lbl.setText(""), sid)

    def _script(self):
        return str(Path(self.s.archiver()).with_name("s3s3-archiver-s3tables-mp.py"))

    # -- manual run on EC2 ------------------------------------------------------
    def kit_args(self):
        """The archiver arguments of this dialog for a run on EC2: no local
        progress pipe, no local AWS profile (the instance role is used)."""
        cmd, err = self.command()
        if err:
            return None, err
        out, skip = [], False
        for a in cmd[3:]:
            if skip:
                skip = False
                continue
            if a == "--progress":
                continue
            if a == "--profile-name":
                skip = True
                continue
            out.append(a)
        return out, None

    def ec2_script(self):
        """Upload this archiver version (and a selection list) to the tar bucket,
        then return a paste-ready shell script that runs these settings on an
        EC2 instance. Blocking: call off the UI thread."""
        import shlex
        import remote as R
        args, err = self.kit_args()
        if err:
            raise ValueError(err)
        cfg = self.s.config()
        r = R.Remote(cfg)
        ver = r.upload_runner()
        if "--src-manifest" in args:            # selected objects: the list goes to S3 too
            i = args.index("--src-manifest") + 1
            key = f"_sfa/jobs/manual-{datetime.now():%Y%m%d_%H%M%S}/selection.txt"
            r.s3.upload_file(args[i], cfg.bucket, key)
            args[i] = f"s3://{cfg.bucket}/{key}"
        q = shlex.quote
        pairs, i = [], 0                       # one "--option value" per line
        while i < len(args):
            if args[i].startswith("--") and i + 1 < len(args) and not args[i + 1].startswith("--"):
                pairs.append(f"{args[i]} {q(args[i + 1])}"); i += 2
            else:
                pairs.append(q(args[i])); i += 1
        run = " \\\n    ".join(pairs)
        return f"""bash <<'SFA_EOF'
# Small Files Archiver: S3-source run, copied from the GUI on {datetime.now():%Y-%m-%d %H:%M}.
# Paste into a shell on an EC2 instance in {cfg.region}, the source bucket's Region (no data transfer out).
set -eu
RESUME_OF=""     # to continue a stopped run, paste this again with its run id here
D="$HOME/sfa-runner/{ver}"; W="$HOME/sfa-work"
if [ ! -f "$D/.ready" ]; then       # first time only: this archiver version + a private Python 3.12
  mkdir -p "$D"
  aws s3 cp --recursive --only-show-errors {q(f"s3://{cfg.bucket}/_sfa/runner/{ver}/")} "$D/" --region {q(cfg.region)}
  UV=$(command -v uv || echo "$HOME/.local/bin/uv")
  [ -x "$UV" ] || {{ curl -LsSf https://astral.sh/uv/install.sh | sh >/dev/null; UV="$HOME/.local/bin/uv"; }}
  "$UV" venv --quiet --python 3.12 "$D/.venv"
  "$UV" pip install --quiet --only-binary :all: --python "$D/.venv/bin/python" -r "$D/requirements.txt"
  touch "$D/.ready"
fi
mkdir -p "$W" && cd "$W"
OUT="$W/archiver-$(date +%Y%m%d_%H%M%S).out"
EXTRA=(); [ -n "$RESUME_OF" ] && EXTRA=(--resume-run "$RESUME_OF")
setsid nohup "$D/.venv/bin/python" -u "$D/s3s3-archiver-s3tables-mp.py" \\
    {run} ${{EXTRA[@]+"${{EXTRA[@]}}"}} > "$OUT" 2>&1 < /dev/null &
PID=$!; sleep 5
echo "Started (pid $PID).  Follow:  tail -f $OUT"
grep -m1 -o "run_id=[^,]*" "$OUT" || true
echo "Stop cleanly: kill -INT $PID.  Continue a stopped run: paste again with RESUME_OF=<run id>."
SFA_EOF
"""

    def _copy_script(self):
        self.btn_copy.setEnabled(False); self.btn_copy.setText("Preparing…")

        def ok(text):
            QApplication.clipboard().setText(text)
            self.btn_copy.setEnabled(True); self.btn_copy.setText("Copied ✓")
            QTimer.singleShot(4000, lambda: self.btn_copy.setText("Copy script for EC2"))
            self.warn.setText("<small>Script copied. Paste it into a terminal on an EC2 instance in the "
                              "bucket's Region (SSH, Session Manager or EC2 Instance Connect). The "
                              "instance role needs the archiver permissions and s3:GetObject on "
                              "_sfa/runner/* in the tar bucket.</small>")

        def fail(m):
            self.btn_copy.setEnabled(True); self.btn_copy.setText("Copy script for EC2")
            self.warn.setText(mask(f"<small>Could not prepare the script: {m.splitlines()[0]}</small>"))

        run_async(self, self.ec2_script, ok, fail)

    def command(self):
        cfg = self.s.config()
        if self.err0:
            return None, self.err0
        base = self.base.text().strip().lstrip("/")
        if base and not base.endswith("/"):
            base += "/"
        if not self.list_prefix.startswith(base):
            return None, "“Store keys relative to” must be the source folder or one of its parents."
        if not self.prefix.text().strip().strip("/"):
            return None, "Destination prefix is required."
        cmd = [sys.executable, "-u", self._script(), "--progress"]
        if self.mode == "folder":
            cmd += ["--src-s3", f"s3://{self.bucket}/{self.list_prefix}"]
            if self.incr.isChecked():
                cmd.append("--incremental")
        else:
            cmd += ["--src-manifest", self._write_uri_list(), "--src-manifest-format", "uris"]
        cmd += ["--src-prefix", base]          # always explicit: "" means the bucket root
        cmd += ["--dst-bucket", cfg.bucket, "--dst-prefix", self.prefix.text().strip().strip("/"),
                "--tar-layout", self.layout_cb.currentData(), "--part-size", self.part.currentText()]
        cmd += ["--max-size", self.size.currentText().strip()] if self.by_size.isChecked() else \
               ["--max-files", str(self.count.value())]
        cmd += ["--num-workers", str(self.workers.value()), "--get-concurrency", str(self.gets.value()),
                "--list-threads", str(self.lists.value()), "--tar-storageclass", self.sclass.currentText(),
                "--region", cfg.region, "--table-bucket-arn", cfg.table_bucket_arn,
                "--namespace", cfg.namespace, "--table-name", cfg.table_name,
                "--md5" if self.md5.isChecked() else "--no-md5",
                "--dir-index" if self.dirindex.isChecked() else "--no-dir-index",
                "--verify-source-checksum" if self.verify.isChecked() else "--no-verify-source-checksum"]
        if cfg.profile:
            cmd += ["--profile-name", cfg.profile]
        return cmd, None

    def _write_uri_list(self):
        if getattr(self, "_uri_file", None):
            return self._uri_file
        d = os.path.join(self.s.workdir(), "logs", "s3-selections")
        os.makedirs(d, mode=0o700, exist_ok=True)
        path = os.path.join(d, f"selection-{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")
        with open(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600), "w") as fh:
            for o in self.objects:
                fh.write(f"s3://{self.bucket}/{o['key']}\n")
        self._uri_file = path
        return path

    def _refresh(self):
        if self.mode == "objects":
            self.preview.setPlainText(f"{len(self.objects):,} objects will be passed as a list (--src-manifest)")
            self.btn_run.setEnabled(self.err0 is None)
            return
        cmd, err = self.command()
        self.btn_run.setEnabled(err is None)
        if err:
            self.preview.setPlainText("⚠ " + err)
        else:
            import shlex
            cfg = self.s.config()
            shown = [f"<table bucket {cfg.table_bucket}>" if c == cfg.table_bucket_arn else c for c in cmd[1:]]
            self.preview.setPlainText(mask(" ".join(shlex.quote(c) for c in shown)))

    def _accept(self):
        self.s.set("last_prefix", self.prefix.text().strip())
        self.s.set("last_size", self.size.currentText())
        self.s.set("last_sclass", self.sclass.currentText())
        self.s.set("last_layout", self.layout_cb.currentData())
        self.s.set("last_part", self.part.currentText())
        inst = self.run_on_instance()
        self.s.set("last_instance", inst["id"] if inst else "")
        self.accept()

    def title(self):
        name = posixpath.basename(self.list_prefix.rstrip("/")) or self.bucket
        what = f"s3 {name}" if self.mode == "folder" else f"s3 {len(self.objects):,} objects"
        return what + (" (incremental)" if self.incr.isChecked() else "")


class ArchivePane(QWidget):
    status = Signal(str)

    def __init__(self, settings: Settings, parent=None):
        super().__init__(parent)
        self.s = settings
        self.manifest = None
        self.tars = None
        self.mode = "files"          # files | tars   (members is a sub-view of tars)
        self.cwd = ""                # files: manifest folder
        self.tar_prefix = ""         # tars: S3 prefix ('' or 'a/b/')
        self.tar_open = None         # tars: key of the tar whose members are shown
        self.tar_token = None
        self.limit = settings.page_size()
        self.search_mode = None
        self._req = 0
        v = QVBoxLayout(self); v.setContentsMargins(4, 4, 4, 4)
        self.bar = PathBar("Archive (S3)")
        self.btn_refresh = QToolButton(); self.btn_refresh.setText("⟳"); self.btn_refresh.setToolTip("Refresh")
        self.bar.layout().addWidget(self.btn_refresh)

        # recent archives: switch between tar bucket / table bucket / table sets
        hr = QHBoxLayout(); hr.setContentsMargins(0, 0, 0, 0)
        self.hist = QComboBox()
        self.hist.setSizeAdjustPolicy(QComboBox.AdjustToMinimumContentsLengthWithIcon)
        self.hist.setMinimumContentsLength(16)
        self.hist.setToolTip("Your archives, by name. Pick one to switch to it.")
        self.hist.activated.connect(self._switch_archive)
        self.btn_hist_add = QToolButton(); self.btn_hist_add.setText("+")
        self.btn_hist_add.setToolTip("Add an archive: create a new one or connect an existing one")
        self.btn_hist_rename = QToolButton(); self.btn_hist_rename.setText("Rename")
        self.btn_hist_remove = QToolButton(); self.btn_hist_remove.setText("Remove")
        self.btn_hist_remove.setToolTip("Remove from this list (nothing in AWS is deleted)")
        self.btn_hist_add.clicked.connect(self._hist_add)
        self.btn_hist_rename.clicked.connect(self._hist_rename)
        self.btn_hist_remove.clicked.connect(self._hist_remove)
        hr.addWidget(QLabel("Archive:")); hr.addWidget(self.hist, 1)
        for b_ in (self.btn_hist_add, self.btn_hist_rename, self.btn_hist_remove):
            hr.addWidget(b_)

        # view switch
        sw = QHBoxLayout(); sw.setContentsMargins(0, 0, 0, 0)
        self.btn_files = QToolButton(); self.btn_files.setText("Files (untarred)")
        self.btn_tars = QToolButton(); self.btn_tars.setText("Tar objects")
        self.btn_files.setToolTip("Original files, rebuilt from the S3 Tables manifest")
        self.btn_tars.setToolTip("The tar objects stored in the S3 bucket")
        grp = QButtonGroup(self); grp.setExclusive(True)
        sw.addWidget(QLabel("View:"))
        for b in (self.btn_files, self.btn_tars):
            b.setCheckable(True); grp.addButton(b); sw.addWidget(b)
        self.btn_files.setChecked(True)
        sw.addStretch(1)
        self.btn_files.clicked.connect(lambda: self.set_mode("files"))
        self.btn_tars.clicked.connect(lambda: self.set_mode("tars"))

        srow = QHBoxLayout()
        self.search = QLineEdit()
        self.search.setClearButtonEnabled(True)
        srow.addWidget(self.search, 1)
        self.who = QLabel("")
        self.who.setStyleSheet("color: gray")
        self.who.setWordWrap(True)
        self.model = ArchiveModel(self)
        self.view = QTableView()
        self.view.setModel(self.model)
        self.view.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.view.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.view.verticalHeader().setVisible(False)
        self.view.verticalHeader().setDefaultSectionSize(22)
        self.view.setShowGrid(False)
        self.view.setWordWrap(False)
        self.view.setTextElideMode(Qt.ElideMiddle)   # long paths keep their file name
        # while the model swaps views the header reports the old view's widths;
        # ignore resizes from then until _fit_columns has applied the new view's
        self.model.modelAboutToBeReset.connect(lambda: setattr(self, "_applying_widths", True))
        self.model.modelReset.connect(self._fit_columns)
        hh = self.view.horizontalHeader()
        hh.setSectionsMovable(False)
        hh.sectionResized.connect(self._save_widths)
        hh.sectionHandleDoubleClicked.connect(self._fit_to_contents)
        hh.setToolTip("Drag a divider to resize a column; double-click it to fit the "
                      "contents; right-click for more")
        hh.setContextMenuPolicy(Qt.CustomContextMenu)
        hh.customContextMenuRequested.connect(self._header_menu)
        self.view.setHorizontalScrollMode(QAbstractItemView.ScrollPerPixel)
        self.view.doubleClicked.connect(self._open)
        foot = QHBoxLayout()
        self.info = QLabel("")
        self.info.setWordWrap(True)
        self.btn_more = QPushButton("Show more")
        self.btn_more.setVisible(False)
        self.btn_more.clicked.connect(self._more)
        foot.addWidget(self.info, 1); foot.addWidget(self.btn_more)
        v.addWidget(self.bar); v.addLayout(hr); v.addLayout(sw); v.addLayout(srow); v.addWidget(self.who)
        v.addWidget(self.view, 1); v.addLayout(foot)
        self.bar.navigate.connect(self._navigate_text)
        self.bar.up.connect(self._up)
        self.btn_refresh.clicked.connect(self.refresh)
        self.search.returnPressed.connect(self._search)
        self._update_chrome()
        self._fill_history()
        self.connect_backend()

    # -- archive list ---------------------------------------------------------
    def _registry(self):
        from archives import ArchiveRegistry
        return ArchiveRegistry(self.s)

    def _fill_history(self):
        reg = self._registry()
        entries = reg.list()
        cur = reg.current_name()
        self.hist.blockSignals(True)
        self.hist.clear()
        for e in entries:
            self.hist.addItem(e["name"])
            tip = (f"tar bucket s3://{e['bucket']}\ntable bucket {e.get('table_bucket') or '?'}\n"
                   f"table {e.get('namespace')}.{e.get('table_name')}\nRegion {e.get('region')}"
                   + (f"\nprofile {e['profile']}" if e.get("profile") else ""))
            self.hist.setItemData(self.hist.count() - 1, mask(tip), Qt.ToolTipRole)
        self.hist.setCurrentIndex(self.hist.findText(cur) if cur else -1)
        self.hist.blockSignals(False)
        self.btn_hist_rename.setEnabled(cur is not None)
        self.btn_hist_remove.setEnabled(cur is not None)

    def _remember(self, cfg):
        """The archive just connected to stays in (or joins) the list."""
        self._registry().add({"profile": cfg.profile, "region": cfg.region, "bucket": cfg.bucket,
                              "table_bucket": cfg.table_bucket, "table_bucket_arn": cfg.table_bucket_arn,
                              "namespace": cfg.namespace, "table_name": cfg.table_name})
        self._fill_history()

    def _switch_archive(self, index):
        name = self.hist.itemText(index)
        if not name:
            return
        self._registry().activate(name)
        self.cwd, self.tar_prefix, self.tar_open = "", "", None
        self.search.clear()
        self.connect_backend()          # checks the tables and the bucket; errors show below

    def _hist_add(self):
        w = self.window()
        if hasattr(w, "open_wizard"):
            w.open_wizard()

    def _hist_rename(self):
        reg = self._registry()
        cur = reg.current_name()
        if cur is None:
            return
        new, ok = QInputDialog.getText(self, "Rename archive", "Name in the archive list:", text=cur)
        if not ok:
            return
        try:
            reg.rename(cur, new)
        except (ValueError, KeyError) as e:
            QMessageBox.information(self, "Rename archive", str(e))
        self._fill_history()

    def _hist_remove(self):
        reg = self._registry()
        cur = reg.current_name()
        if cur is None:
            return
        if QMessageBox.question(self, "Remove from list",
                                f"Remove “{cur}” from the archive list?\n\nOnly the entry is removed; "
                                f"the tars and the manifest in AWS are not touched.") != QMessageBox.Yes:
            return
        reg.remove(cur)
        rest = reg.names()
        if rest:
            self._switch_archive_by_name(rest[0])
        else:
            self._fill_history()

    def _switch_archive_by_name(self, name):
        self._registry().activate(name)
        self.cwd, self.tar_prefix, self.tar_open = "", "", None
        self.search.clear()
        self.connect_backend()

    # Initial widths per view, used until the user resizes a column. Name is
    # sized to take what's left of the pane.
    DEFAULT_WIDTHS = {
        "files":   [None, 80, 60, 130, 170, 160],
        "tars":    [None, 85, 110, 130],
        "members": [None, 80, 130, 160, 110],
    }

    def _fit_columns(self):
        """Every column is user-resizable (drag the header divider; double-click
        it to fit the contents). Widths are remembered per view -- across
        folders and restarts -- and the last column fills leftover space."""
        hh = self.view.horizontalHeader()
        mode, n = self.model.mode, self.model.columnCount()
        self._applying_widths = True
        try:
            hh.setSectionResizeMode(QHeaderView.Interactive)
            hh.setStretchLastSection(True)
            hh.setMinimumSectionSize(40)
            saved = self.s.get(f"colw_{mode}", "")
            widths = [int(x) for x in str(saved).split(",") if x.strip().isdigit()] if saved else []
            if len(widths) != n:
                widths = self._default_widths(mode)
                # first show: the pane may not be laid out yet; refit once it is
                QTimer.singleShot(0, lambda m=mode: self._refit_defaults(m))
            for c, w in enumerate(widths[:n]):
                if c < n - 1:            # the last column stretches
                    hh.resizeSection(c, w)
        finally:
            self._applying_widths = False

    def _default_widths(self, mode):
        """Name gets what's left after the fixed columns; the last column
        stretches anyway, so it doesn't count -- otherwise the defaults overflow
        the pane and add a needless horizontal scrollbar."""
        widths = list(self.DEFAULT_WIDTHS[mode])
        fixed = sum(w for w in widths[1:-1] if w)
        widths[0] = max(180, self.view.viewport().width() - fixed - 140)
        return widths

    def _refit_defaults(self, mode):
        if self.model.mode != mode or self.s.get(f"colw_{mode}", ""):
            return               # user has sized this view already
        self._applying_widths = True
        try:
            for c, w in enumerate(self._default_widths(mode)[:self.model.columnCount() - 1]):
                self.view.horizontalHeader().resizeSection(c, w)
        finally:
            self._applying_widths = False

    def _save_widths(self, *_):
        if getattr(self, "_applying_widths", False):
            return
        hh = self.view.horizontalHeader()
        n = self.model.columnCount()
        self.s.set(f"colw_{self.model.mode}",
                   ",".join(str(hh.sectionSize(c)) for c in range(n)))

    def _fit_to_contents(self, col):
        self.view.resizeColumnToContents(col)

    def _header_menu(self, pos):
        from PySide6.QtWidgets import QMenu
        m = QMenu(self)
        fit = m.addAction("Fit columns to contents")
        reset = m.addAction("Reset column widths")
        act = m.exec(self.view.horizontalHeader().mapToGlobal(pos))
        if act is fit:
            for c in range(self.model.columnCount() - 1):
                self.view.resizeColumnToContents(c)
        elif act is reset:
            self.s.q.remove(f"colw_{self.model.mode}")
            self._applying_widths = True
            self._fit_columns()
            self._refit_defaults(self.model.mode)

    def _update_chrome(self):
        if self.mode == "files":
            self.bar.edit.setPlaceholderText("/  (root of the manifest)")
            self.search.setPlaceholderText("Path prefix search, e.g. photos/2020/IMG_ (Enter)")
        elif self.tar_open:
            self.search.setPlaceholderText("Filter files in this tar (Enter)")
        else:
            self.bar.edit.setPlaceholderText("s3://bucket/  (prefix)")
            self.search.setPlaceholderText("Object name prefix in this folder, e.g. archive_2026 (Enter)")

    def set_mode(self, mode):
        """Switch views, keeping the location in sync through the manifest:
        Files -> Tars opens the S3 prefix holding the current folder's tars and
        selects them; Tars -> Files opens the folder holding the open / selected
        / listed tars' files."""
        if mode == self.mode:
            return
        prev = self.mode
        back_to = None
        if prev == "tars":
            keys = self._tars_in_view()
            # Came here from a Files folder and haven't moved since (same prefix,
            # selection untouched or cleared, or opened one of that folder's own
            # tars)? Then return to that folder -- the tars' common folder is
            # usually much higher, because one tar mixes files from many folders.
            link = getattr(self, "_link", None)
            if link and self.tar_prefix == link["prefix"]:
                if self.tar_open:
                    if self.tar_open.lstrip("/") in link.get("tars", set()):
                        back_to = link["folder"]
                else:
                    cur = {self.model.items[i.row()].get("key")
                           for i in self.view.selectionModel().selectedRows(0)} \
                        if self.model.mode == "tars" else set()
                    if not cur or cur == link["auto"]:
                        back_to = link["folder"]
        self.mode = mode
        self.btn_files.setChecked(mode == "files"); self.btn_tars.setChecked(mode == "tars")
        self.search.clear()
        self.tar_open = None
        self._update_chrome()
        if self.manifest is None:
            return
        req = self._next_req()
        if mode == "tars":
            self._link = None
            if not self.cwd:
                self.goto_archive_root(req)   # whole archive -> its destination prefix
                return
            self.info.setText(f"Finding the tars that hold /{self.cwd}…")

            def ok(res):
                if req != self._req or self.mode != "tars":
                    return
                prefix, keys, sampled = res
                if not keys:
                    self.cd_tars(self.tar_prefix)
                    return
                self._pending_select = (set(keys), self.cwd, sampled)
                self._link = {"folder": self.cwd, "prefix": prefix, "auto": set(),
                              "tars": {k.lstrip("/") for k in keys}}
                self.cd_tars(prefix)

            run_async(self, self.manifest.tars_for_folder, ok,
                      lambda m: (self.status.emit(f"Locating tars failed: {m}"),
                                 self.cd_tars(self.tar_prefix)), self.cwd)
        else:
            if back_to is not None:
                self.cd(back_to)
                return
            if not keys:
                self.cd(self.cwd)
                return
            self.info.setText("Finding the folder these tars hold…")

            def ok(folder):
                if req != self._req or self.mode != "files":
                    return
                self.cd(folder)

            run_async(self, self.manifest.folder_for_tars, ok,
                      lambda m: (self.status.emit(f"Locating folder failed: {m}"),
                                 self.cd(self.cwd)), keys)

    def goto_archive_root(self, req):
        """Open the tar view where this archive's tars are, not at the bucket
        root: 'projects_archive/20261103/' when every tar shares that folder,
        'projects_archive/' across several dates, the common parent (or bucket
        root) across several destination prefixes -- listed in the status line."""
        self.info.setText("Finding the archive's destination prefix…")

        def ok(res):
            if req != self._req or self.mode != "tars":
                return
            prefix, roots = res
            self._link = {"folder": "", "prefix": prefix, "auto": set()}
            self._root_note = ("" if len(roots) <= 1 else
                               "Destination prefixes in this archive: "
                               + ", ".join(r + "/" for r in roots[:8])
                               + (f" … +{len(roots) - 8}" if len(roots) > 8 else ""))
            self.cd_tars(prefix)

        def fail(msg):
            self.status.emit(f"Finding destination prefix failed: {msg}")
            self._link = {"folder": "", "prefix": "", "auto": set()}
            self.cd_tars("")

        run_async(self, self.manifest.archive_location, ok, fail)

    def _tars_in_view(self):
        """Tar keys that define 'where we are' in the tar view."""
        if self.tar_open:
            return [self.tar_open]
        items = self.model.items if self.model.mode == "tars" else []
        sel = [items[i.row()]["key"] for i in self.view.selectionModel().selectedRows(0)
               if items and items[i.row()]["kind"] == "object" and items[i.row()]["is_tar"]]
        if sel:
            return sel
        return [i["key"] for i in items if i["kind"] == "object" and i["is_tar"]][:50]

    # -- connection ----------------------------------------------------------
    def connect_backend(self, keep_path=False):
        self._fill_history()
        cfg = self.s.config()
        target = self.cwd if keep_path else ""
        self.manifest = None
        self.tars = None
        self.model.set_items([])
        if not cfg.complete():
            self.info.setText("Not configured — open Settings (⌘,) and fill in "
                              "region, tar bucket and S3 Tables coordinates.")
            return
        self.info.setText("Connecting…")

        def connect():
            prov = backend.Provisioner(cfg.profile, cfg.region)
            prov.check()
            if not cfg.table_bucket_arn:
                # picked by name (Settings / wizard): derive the ARN, never shown
                cfg.table_bucket_arn = prov.table_bucket_arn(cfg.table_bucket)
            m = Manifest(cfg)
            has_index, _ = m.probe()
            return m, backend.TarStore(cfg), cfg.table_bucket_arn, has_index

        def ok(res):
            m, tars, arn, has_index = res
            self.s.set("table_bucket_arn", arn)
            self.manifest, self.tars = m, tars
            self._remember(cfg)
            mode = ("directory index" if has_index else
                    "legacy table (no dir index: folder view samples a bounded prefix scan)")
            if m.table_missing:
                mode = (f"table not created yet — the first Archive run creates "
                        f"{cfg.namespace}.{cfg.table_name}")
            if m.unindexed_rows:
                mode += (" ⚠ some rows predate the dir index and only appear in "
                         "search — see README 'Backfill'")
            self.who.setText(f"{cfg.profile or 'default profile'} · {cfg.region}  ·  "
                             f"s3://{cfg.bucket}  ·  table bucket {cfg.table_bucket}  ·  "
                             f"{cfg.namespace}.{cfg.table_name}  ·  {mode}")
            if self.mode == "files":
                self.cd(target)
            elif self.tar_open and keep_path:
                self.open_tar(self.tar_open)
            elif keep_path:
                self.cd_tars(self.tar_prefix)
            else:
                self.goto_archive_root(self._next_req())

        def fail(msg):
            self.info.setText("Connection failed — see Log tab.")
            self.status.emit(f"Archive connection failed: {msg}")

        run_async(self, connect, ok, fail)

    def refresh(self):
        """Reload the current view; reconnect first when there is no live
        connection (an earlier failure) or the table did not exist yet --
        otherwise a refresh after the first archive run would do nothing."""
        if self.manifest is None or self.manifest.table_missing:
            self.connect_backend(keep_path=True)
        elif self.mode == "files":
            self.cd(self.cwd, keep_limit=True)
        elif self.tar_open:
            self.open_tar(self.tar_open, keep_limit=True)
        else:
            self.cd_tars(self.tar_prefix)

    def _navigate_text(self, text):
        t = text.strip()
        if self.mode == "files":
            self.cd(t.strip("/"))
            return
        bucket = self.s.config().bucket
        for pre in (f"s3://{bucket}/", f"s3://{bucket}"):
            if t.startswith(pre):
                t = t[len(pre):]
                break
        t = t.lstrip("/")
        if t.endswith(backend.TarStore.TAR_EXT):
            self.open_tar(t)
        else:
            self.cd_tars(t + "/" if t and not t.endswith("/") else t)

    def _up(self):
        if self.mode == "files":
            self.cd(posixpath.dirname(self.cwd))
        elif self.tar_open:
            self.cd_tars(self.tar_prefix)
        else:
            parent = posixpath.dirname(self.tar_prefix.rstrip("/"))
            self.cd_tars(parent + "/" if parent else "")

    def _next_req(self):
        self._req += 1
        return self._req

    # -- files view ----------------------------------------------------------
    def cd(self, path, keep_limit=False):
        if self.manifest is None or self.mode != "files":
            return
        path = path.strip("/")
        if not keep_limit or path != self.cwd:
            self.limit = self.s.page_size()
        self.cwd = path
        self.search_mode = None
        self.bar.set_path("/" + path)
        self.info.setText("Loading…")
        req = self._next_req()

        def ok(lst):
            if req != self._req:
                return  # stale response from an earlier navigation
            items = [dict(d, kind="dir") for d in lst.dirs] + \
                    [dict(f, kind="file") for f in lst.files]
            self.model.set_items(items, mode="files")
            nd, nf = len(lst.dirs), len(lst.files)
            tot_f = sum(d["files"] for d in lst.dirs) + nf
            tot_b = sum(d["bytes"] for d in lst.dirs) + sum(f["filesize"] for f in lst.files)
            if lst.mode == "empty":
                self.info.setText(f"Archive is empty: {self.s.config().namespace}."
                                  f"{self.s.config().table_name} does not exist yet. "
                                  f"Archive something to create it.")
                self.btn_more.setVisible(False)
                return
            extra = " (legacy scan)" if lst.mode == "legacy" else ""
            more = f" — showing first {nf:,} files" if lst.truncated else ""
            self.info.setText(f"{nd:,} folders, {nf:,} files here{more} · "
                              f"{tot_f:,} files / {human(tot_b)} below · "
                              f"{lst.elapsed:.1f}s{extra}")
            self.btn_more.setVisible(lst.truncated)

        def fail(msg):
            if req == self._req:
                self.info.setText("Listing failed — see Log tab.")
                self.status.emit(f"Listing /{path} failed: {msg}")

        run_async(self, self.manifest.list_dir, ok, fail, path, limit=self.limit)

    # -- tar objects view ----------------------------------------------------
    def cd_tars(self, prefix, name_prefix=""):
        if self.tars is None or self.mode != "tars":
            return
        self.tar_prefix, self.tar_open, self.tar_token = prefix, None, None
        self._update_chrome()
        self.bar.set_path(f"s3://{self.s.config().bucket}/{prefix}")
        self.info.setText("Listing objects…")
        self._list_tars_page(name_prefix, append=False)

    def _list_tars_page(self, name_prefix, append):
        req = self._next_req()
        prefix, t0 = self.tar_prefix, time.time()

        def ok(res):
            if req != self._req:
                return
            dirs, objs, token = res
            self.tar_token = token
            self._tar_name_prefix = name_prefix
            if append:
                self.model.append(dirs + objs)
            else:
                self.model.set_items(dirs + objs, mode="tars")
            pending = getattr(self, "_pending_select", None)
            self._pending_select = None
            if pending and not append:
                want, folder, sampled = pending
                sm = self.view.selectionModel()
                # tars listed here, or -- when the files span several runs --
                # the run folders that contain them
                want_dirs = {posixpath.dirname(k) + "/" for k in want}
                rows = [n for n, i in enumerate(self.model.items)
                        if (i["kind"] == "object" and i["key"] in want) or
                        (i["kind"] == "prefix" and any(d.startswith(i["path"]) for d in want_dirs))]
                nfold = sum(1 for n in rows if self.model.items[n]["kind"] == "prefix")
                for n in rows:
                    sm.select(self.model.index(n, 0),
                              QItemSelectionModel.SelectionFlag.Select
                              | QItemSelectionModel.SelectionFlag.Rows)
                if rows:
                    self.view.scrollTo(self.model.index(rows[0], 0))
                link = getattr(self, "_link", None)
                if link:
                    link["auto"] = {self.model.items[n].get("key") for n in rows}
                what = (f"{nfold:,} run folder(s)" if nfold and nfold == len(rows) else
                        f"{len(rows):,} tar(s)" if not nfold else f"{len(rows):,} items")
                self._linked_note = (f"Selected {what} holding files of /{folder}"
                                     + (" (sampled)" if sampled else "")
                                     + (f"; {len(want) - len(rows):,} more on later pages"
                                        if not nfold and len(want) > len(rows) else ""))
            else:
                self._linked_note = getattr(self, "_root_note", "") \
                    if self.tar_prefix == (getattr(self, "_link", None) or {}).get("prefix") else ""
                self._root_note = ""
            items = self.model.items
            ntar = sum(1 for i in items if i["kind"] == "object" and i["is_tar"])
            nobj = sum(1 for i in items if i["kind"] == "object")
            ndir = sum(1 for i in items if i["kind"] == "prefix")
            size = sum(i["size"] for i in items if i["kind"] == "object")
            classes = sorted({i["storage_class"] for i in items if i["kind"] == "object"})
            filt = f" matching '{name_prefix}*'" if name_prefix else ""
            self.info.setText(f"{ndir:,} folders, {ntar:,} tars"
                              + (f" (+{nobj - ntar:,} other objects)" if nobj > ntar else "")
                              + f"{filt} · {human(size)} listed"
                              + (f" · {', '.join(classes)}" if classes else "")
                              + (" · more available" if token else "")
                              + f" · {time.time() - t0:.1f}s"
                              + (f"\n{self._linked_note}" if self._linked_note else ""))
            self.btn_more.setVisible(bool(token))

        def fail(msg):
            if req == self._req:
                self.info.setText("S3 listing failed — see Log tab.")
                self.status.emit(f"Listing s3://{self.s.config().bucket}/{prefix} failed: {msg}")

        run_async(self, self.tars.list, ok, fail, prefix,
                  token=self.tar_token if append else None, name_prefix=name_prefix)

    def open_tar(self, key, keep_limit=False):
        if self.manifest is None:
            return
        if not keep_limit:
            self.limit = self.s.page_size()
        self.tar_open = key
        self.tar_prefix = posixpath.dirname(key) + "/" if "/" in key else ""
        self._update_chrome()
        self.bar.set_path(f"s3://{self.s.config().bucket}/{key}")
        self.info.setText("Reading manifest rows for this tar…")
        req = self._next_req()
        t0 = time.time()

        def ok(res):
            if req != self._req:
                return
            rows, truncated = res
            items = [dict(r, kind="member") for r in rows]
            self._members_all = items
            self.model.set_items(items, mode="members")
            if not rows:
                self.info.setText("⚠ No manifest rows reference this tar — it is an orphan "
                                  "(uploaded, but its manifest commit never happened). "
                                  "Its files cannot be found from the Files view; see README §6.")
            else:
                tot = sum(r["filesize"] for r in rows)
                self.info.setText(f"{len(rows):,} files in this tar"
                                  f"{' (first page)' if truncated else ''} · {human(tot)} of "
                                  f"file data · {time.time() - t0:.1f}s · Up (↑) returns to the "
                                  f"tar list")
            self.btn_more.setVisible(truncated)

        def fail(msg):
            if req == self._req:
                self.info.setText("Reading tar contents failed — see Log tab.")
                self.status.emit(f"Tar contents for {key} failed: {msg}")

        run_async(self, self.manifest.tar_members, ok, fail, key, limit=self.limit)

    # -- shared --------------------------------------------------------------
    def _more(self):
        if self.mode == "tars" and not self.tar_open:
            if self.tar_token:
                self._list_tars_page(getattr(self, "_tar_name_prefix", ""), append=True)
            return
        self.limit = min(self.limit * 4, 1_000_000)
        if self.mode == "tars":
            self.open_tar(self.tar_open, keep_limit=True)
        elif self.search_mode:
            self._search(keep_limit=True)
        else:
            self.cd(self.cwd, keep_limit=True)

    def _open(self, idx):
        it = self.model.items[idx.row()]
        if it["kind"] == "dir":
            self.cd(it["path"])
        elif it["kind"] == "prefix":
            self.cd_tars(it["path"])
        elif it["kind"] == "object" and it["is_tar"]:
            self.open_tar(it["key"])

    def _search(self, keep_limit=False):
        q = self.search.text().strip()
        if self.mode == "tars":
            if self.tar_open:
                ql = q.lower()
                allm = getattr(self, "_members_all", [])
                items = [m for m in allm if ql in m["source_key"].lower()] if q else allm
                self.model.set_items(items, mode="members")
                self.info.setText(f"{len(items):,} of {len(allm):,} loaded files match '{q}'"
                                  if q else f"{len(allm):,} files in this tar")
            else:
                self.cd_tars(self.tar_prefix, name_prefix=q)
            return
        q = q.lstrip("/")
        if not q or self.manifest is None:
            self.cd(self.cwd)
            return
        if not keep_limit:
            self.limit = self.s.page_size()
        self.search_mode = q
        self.info.setText(f"Searching '{q}*'…")
        req = self._next_req()
        t0 = time.time()

        def ok(res):
            if req != self._req:
                return
            files, truncated = res
            items = [dict(f, kind="file", name=f["source_key"]) for f in files]
            self.model.set_items(items, mode="files")
            self.info.setText(f"{len(files):,} matches for '{q}*'"
                              f"{' (first page)' if truncated else ''} · {time.time() - t0:.1f}s")
            self.btn_more.setVisible(truncated)

        def fail(msg):
            if req == self._req:
                self.info.setText("Search failed — see Log tab.")
                self.status.emit(f"Search failed: {msg}")

        run_async(self, self.manifest.search, ok, fail, q, limit=self.limit)

    def selections(self):
        """([(kind, path)], est_files, est_bytes, skipped_folders)."""
        out, files, nbytes, skipped = [], 0, 0, 0
        for idx in self.view.selectionModel().selectedRows(0):
            it = self.model.items[idx.row()]
            k = it["kind"]
            if k == "dir":
                out.append(("dir", it["path"])); files += it["files"]; nbytes += it["bytes"]
            elif k == "file":
                out.append(("file", it["source_key"])); files += 1; nbytes += it["filesize"]
            elif k == "member":
                out.append(("member", (it["tar_key"], it["source_key"])))
                files += 1; nbytes += it["filesize"]
            elif k == "object" and it["is_tar"]:
                out.append(("tar", it["key"])); nbytes += it["size"]
            else:
                skipped += 1     # S3 folder prefixes / non-tar objects
        return out, files, nbytes, skipped


# --------------------------------------------------------------------------- #
# Archive dialog
# --------------------------------------------------------------------------- #
class ArchiveDialog(QDialog):
    """Archive options. Two sources:
       * the items selected in the local pane (directory walk), optionally
         --incremental against the last successful run of the same source
       * a file-list report (txt / csv / tsv, optionally .gz) -> --input-file
    Either way the archiver runs its full parallel pipeline (scan threads ->
    worker processes)."""

    def __init__(self, paths, settings: Settings, parent=None, manifest=None,
                 report_dir=None, retry_from=None):
        super().__init__(parent)
        self.setWindowTitle("Archive to S3")
        self.paths = paths
        self.s = settings
        self.manifest = manifest
        self._arch = None
        f = QFormLayout(self)

        # -- source
        self.src_sel = QRadioButton("Selected items in the local pane")
        self.src_rep = QRadioButton("File-list report (txt / csv / tsv, .gz ok)")
        sg = QButtonGroup(self); sg.addButton(self.src_sel); sg.addButton(self.src_rep)
        src = "\n".join(paths[:6]) + (f"\n… and {len(paths) - 6} more" if len(paths) > 6 else "")
        self.sel_lbl = QLabel(mask(src) or "(nothing selected)")
        self.sel_lbl.setStyleSheet("color: gray")
        f.addRow("Source", self.src_sel)
        f.addRow("", self.sel_lbl)
        f.addRow("", self.src_rep)
        rw = QWidget(); rf = QFormLayout(rw); rf.setContentsMargins(16, 0, 0, 0)
        self.report = QLineEdit(retry_from or "")
        self.report.setPlaceholderText("report file path")
        rb = QToolButton(); rb.setText("…")
        rb.clicked.connect(lambda: self._pick_report(report_dir))
        rh = QHBoxLayout(); rh.addWidget(self.report, 1); rh.addWidget(rb)
        rf.addRow("Report", rh)
        self.rfmt = QComboBox(); self.rfmt.addItems(["auto", "lines", "csv", "tsv"])
        self.rcol = QLineEdit("0")
        self.rcol.setToolTip("Column with the file path: header name (e.g. path) or 0-based index")
        rh2 = QHBoxLayout(); rh2.addWidget(self.rfmt); rh2.addWidget(QLabel("path column")); rh2.addWidget(self.rcol)
        rf.addRow("Format", rh2)
        self.rep_box = rw
        f.addRow("", rw)

        # -- paths / destination
        base_default = self._default_base(paths, report_dir)
        self.base = QLineEdit(base_default)
        self.base.setToolTip("Archived paths are stored relative to this folder (and relative "
                             "report entries are resolved against it). Keep it the same across "
                             "runs. Default keeps a selected folder's own name.")
        f.addRow("Store paths relative to", self.base)
        self.prefix = QLineEdit(self.s.get("last_prefix", "") or datetime.now().strftime("%Y%m%d"))
        f.addRow("Destination prefix", self.prefix)
        self.layout_cb = QComboBox()
        self.layout_cb.addItem("One folder per run  (<prefix>/<run-id>/)", "run")
        self.layout_cb.addItem("One folder per day  (<prefix>/<YYYYMMDD>/)", "date")
        self.layout_cb.addItem("No subfolder  (<prefix>/)", "flat")
        i = self.layout_cb.findData(self.s.get("last_layout", "run") or "run")
        self.layout_cb.setCurrentIndex(max(0, i))
        self.layout_cb.setToolTip("Where the tar objects go under the destination prefix. "
                                  "Per run keeps each run (including every incremental run) "
                                  "together for checking and cleanup. Restore works the same "
                                  "for any layout.")
        f.addRow("Tar folders", self.layout_cb)
        self.dest_lbl = QLabel()
        f.addRow("", self.dest_lbl)

        # -- incremental
        self.incr = QCheckBox("Incremental — only files changed since the last successful "
                              "run of this source")
        self.incr_lbl = QLabel(""); self.incr_lbl.setWordWrap(True)
        self.incr_lbl.setStyleSheet("color: gray")
        f.addRow("", self.incr)
        f.addRow("", self.incr_lbl)

        g = QHBoxLayout()
        self.by_size = QRadioButton("Max tar size"); self.by_count = QRadioButton("Max files per tar")
        self.by_size.setChecked(True)
        self.size = QComboBox(); self.size.setEditable(True)
        self.size.addItems(["20MB", "64MB", "256MB", "512MB", "1GB", "4GB"])
        self.size.setCurrentText(self.s.get("last_size", "256MB") or "256MB")
        self.count = QSpinBox(); self.count.setRange(1, 10_000_000); self.count.setValue(1000)
        g.addWidget(self.by_size); g.addWidget(self.size); g.addSpacing(16)
        g.addWidget(self.by_count); g.addWidget(self.count)
        f.addRow("Bundling", g)

        self.part = QComboBox()
        self.part.addItems(["8MB", "16MB", "32MB", "64MB"])
        self.part.setCurrentText(self.s.get("last_part", "32MB") or "32MB")
        self.part.setToolTip("Multipart upload part size. Every part is a billed PUT-class "
                             "request, so bigger parts mean fewer requests. Bigger parts use "
                             "more memory (about part size × 10 upload threads per worker) and "
                             "re-send more data if a part fails.")
        self.part_lbl = QLabel(""); self.part_lbl.setStyleSheet("color: gray")
        hp = QHBoxLayout(); hp.addWidget(self.part); hp.addWidget(self.part_lbl, 1)
        f.addRow("Upload part size", hp)

        self.workers = QSpinBox(); self.workers.setRange(1, 512); self.workers.setValue(os.cpu_count() or 4)
        self.scan = QSpinBox(); self.scan.setRange(1, 256); self.scan.setValue(1)
        self.scan.setToolTip("Raise for a latency-bound network filesystem (NFS/SMB) or a "
                             "large report: each scan thread stats files in parallel.")
        hw = QHBoxLayout(); hw.addWidget(self.workers); hw.addWidget(QLabel("scan threads")); hw.addWidget(self.scan)
        f.addRow("Worker processes", hw)
        self.sclass = QComboBox(); self.sclass.addItems(STORAGE_CLASSES)
        self.sclass.setCurrentText(self.s.get("last_sclass", "STANDARD") or "STANDARD")
        f.addRow("Tar storage class", self.sclass)
        self.md5 = QCheckBox("Per-file MD5 (integrity)"); self.md5.setChecked(True)
        self.compress = QCheckBox("gzip tars (disables single-file restore)")
        self.dirindex = QCheckBox("Directory index (fast folder browsing at scale)"); self.dirindex.setChecked(True)
        for w in (self.md5, self.compress, self.dirindex):
            f.addRow("", w)
        self.preview = QPlainTextEdit(); self.preview.setReadOnly(True)
        self.preview.setMaximumHeight(110)
        self.preview.setFont(QFont("Menlo", 10))
        f.addRow("Command", self.preview)
        bb = QDialogButtonBox()
        self.btn_run = bb.addButton("Start archiving", QDialogButtonBox.AcceptRole)
        bb.addButton(QDialogButtonBox.Cancel)
        bb.accepted.connect(self._accept); bb.rejected.connect(self.reject)
        f.addRow(bb)

        (self.src_rep if (retry_from or not paths) else self.src_sel).setChecked(True)
        self.src_sel.setEnabled(bool(paths))
        for w in (self.base, self.prefix, self.report, self.rcol):
            w.textChanged.connect(self._refresh)
        self.size.currentTextChanged.connect(self._refresh)
        self.rfmt.currentTextChanged.connect(self._refresh)
        for w in (self.count, self.workers, self.scan):
            w.valueChanged.connect(self._refresh)
        for w in (self.by_size, self.by_count, self.md5, self.compress, self.dirindex,
                  self.src_sel, self.src_rep, self.incr):
            w.toggled.connect(self._refresh)
        self.sclass.currentTextChanged.connect(self._refresh)
        self.layout_cb.currentIndexChanged.connect(self._refresh)
        self.part.currentTextChanged.connect(self._refresh)
        self.base.editingFinished.connect(self._lookup_last_run)
        self.src_sel.toggled.connect(self._lookup_last_run)
        self._refresh()
        self._lookup_last_run()

    @staticmethod
    def _default_base(paths, report_dir):
        if not paths:
            return report_dir or str(Path.home())
        common = os.path.commonpath(paths)
        if len(paths) == 1 or os.path.isfile(common):
            return os.path.dirname(common)
        return common

    def _pick_report(self, start):
        fn, _ = QFileDialog.getOpenFileName(self, "File-list report", start or str(Path.home()),
                                            "Reports (*.txt *.csv *.tsv *.lst *.gz);;All files (*)")
        if fn:
            self.report.setText(fn)
            self.src_rep.setChecked(True)

    def _walk_spec(self, base):
        """(src_path, walk_paths) exactly as passed to the archiver."""
        if len(self.paths) == 1 and self.paths[0] == base:
            return base, []
        return base, list(self.paths)

    def _lookup_last_run(self):
        """Show what --incremental will compare against (async, read-only)."""
        if not self.src_sel.isChecked() or self.manifest is None:
            self.incr_lbl.setText("")
            return
        base, err = self._validate()
        if err:
            return
        try:
            if self._arch is None:
                self._arch = backend.archiver_module(self.s.archiver())
            sid = self._arch.source_id_for(*self._walk_spec(base))
        except Exception as e:
            self.incr_lbl.setText(mask(f"Can't compute the source id: {e}"))
            return
        self.incr_lbl.setText("Looking up the last run of this source…")

        def ok(r):
            if r is None:
                self.incr_lbl.setText("No previous successful run of this exact source (same base "
                                      "folder and selection): the first incremental run archives "
                                      "everything and sets the watermark.")
            else:
                self.incr_lbl.setText(
                    f"Last successful run: {fmt_ts(r['start_ts'])} ({r['mode']}, {r['files']:,} "
                    f"files, {r['status']}). Incremental archives files modified, created, "
                    f"renamed or moved after it.")

        run_async(self, self.manifest.last_run, ok,
                  lambda m: self.incr_lbl.setText("Run history not available yet."), sid)

    def _validate(self):
        base = os.path.abspath(os.path.expanduser(self.base.text().strip() or "/"))
        if not os.path.isdir(base):
            return None, "Base folder does not exist."
        if self.src_sel.isChecked():
            if not self.paths:
                return None, "Select files or folders in the local pane, or use a report."
            for p in self.paths:
                if not (p == base or p.startswith(base.rstrip("/") + "/")):
                    return None, f"{p} is not under the base folder."
        else:
            rep = os.path.expanduser(self.report.text().strip())
            if not rep or not os.path.isfile(rep):
                return None, "Choose an existing report file."
            if not self.rcol.text().strip():
                return None, "Path column is required (name or 0-based index)."
        if not self.prefix.text().strip().strip("/"):
            return None, "Destination prefix is required."
        return base, None

    def command(self):
        cfg = self.s.config()
        base, err = self._validate()
        if err:
            return None, err
        cmd = [sys.executable, "-u", self.s.archiver(), "--progress",
               "--src-path", base]
        if self.src_sel.isChecked():
            for p in self._walk_spec(base)[1]:
                cmd += ["--walk-path", p]
            if self.incr.isChecked():
                cmd.append("--incremental")
        else:
            cmd += ["--input-file", os.path.abspath(os.path.expanduser(self.report.text().strip())),
                    "--input-format", self.rfmt.currentText()]
            if self.rfmt.currentText() != "lines":
                cmd += ["--path-column", self.rcol.text().strip()]
        cmd += ["--dst-bucket", cfg.bucket, "--dst-prefix", self.prefix.text().strip().strip("/"),
                "--tar-layout", self.layout_cb.currentData(),
                "--part-size", self.part.currentText()]
        if self.by_size.isChecked():
            cmd += ["--max-size", self.size.currentText().strip()]
        else:
            cmd += ["--max-files", str(self.count.value())]
        cmd += ["--num-workers", str(self.workers.value()),
                "--scan-threads", str(self.scan.value()),
                "--tar-storageclass", self.sclass.currentText(),
                "--region", cfg.region, "--table-bucket-arn", cfg.table_bucket_arn,
                "--namespace", cfg.namespace, "--table-name", cfg.table_name]
        cmd.append("--md5" if self.md5.isChecked() else "--no-md5")
        cmd.append("--dir-index" if self.dirindex.isChecked() else "--no-dir-index")
        if self.compress.isChecked():
            cmd.append("--compress")
        if cfg.profile:
            cmd += ["--profile-name", cfg.profile]
        return cmd, None

    def _refresh(self):
        cfg = self.s.config()
        rep = self.src_rep.isChecked()
        self.rep_box.setEnabled(rep)
        self.sel_lbl.setEnabled(not rep)
        self.incr.setEnabled(not rep)
        if rep and self.incr.isChecked():
            self.incr.setChecked(False)
        self.incr_lbl.setVisible(not rep)
        self.part_lbl.setText(self._requests_hint())
        folder = {"run": "<run-id>/", "date": "<YYYYMMDD>/", "flat": ""}[self.layout_cb.currentData()]
        self.dest_lbl.setText(f"→ s3://{cfg.bucket}/{self.prefix.text().strip().strip('/')}/"
                              f"{folder}archive_<run-id>_00001.tar …")
        cmd, err = self.command()
        self.btn_run.setEnabled(err is None)
        if err:
            self.preview.setPlainText("⚠ " + err)
        else:
            import shlex
            shown = [f"<table bucket {cfg.table_bucket}>" if c == cfg.table_bucket_arn else c
                     for c in cmd[1:]]
            self.preview.setPlainText(mask(" ".join(shlex.quote(c) for c in shown)))

    def _requests_hint(self):
        """'≈ N upload requests per tar' for the chosen tar size and part size."""
        part = int(self.part.currentText()[:-2]) * 1024 * 1024
        if not self.by_size.isChecked():
            return "requests per tar depend on the tar size (bundling by file count)"
        try:
            size = backend.parse_size(self.size.currentText())
        except ValueError:
            return ""
        n = 1 if size <= part else -(-size // part) + 2
        return (f"≈ {n} upload request{'s' if n > 1 else ''} per {human(size)} tar"
                + ("" if n == 1 else f" (start + {n - 2} parts + finish)"))

    def _accept(self):
        if self.compress.isChecked() and QMessageBox.question(
                self, "Compression",
                "gzip tars cannot be restored file-by-file (byte offsets are not "
                "recorded). Continue?") != QMessageBox.Yes:
            return
        self.s.set("last_prefix", self.prefix.text().strip())
        self.s.set("last_size", self.size.currentText())
        self.s.set("last_sclass", self.sclass.currentText())
        self.s.set("last_layout", self.layout_cb.currentData())
        self.s.set("last_part", self.part.currentText())
        self.accept()

    def title(self):
        if self.src_rep.isChecked():
            return "report " + os.path.basename(self.report.text().strip())
        t = os.path.basename(self.paths[0]) + (f" +{len(self.paths) - 1}" if len(self.paths) > 1 else "")
        return t + (" (incremental)" if self.incr.isChecked() else "")


class RestoreDialog(QDialog):
    def __init__(self, selections, files, nbytes, dest, parent=None, manifest=None):
        super().__init__(parent)
        self.setWindowTitle("Restore to local disk")
        self.manifest = manifest
        f = QFormLayout(self)
        names = [selection_label(k, p) for k, p in selections[:8]]
        if len(selections) > 8:
            names.append(f"… and {len(selections) - 8} more")
        f.addRow("Selected", QLabel("\n".join(names)))
        est = (f"{files:,} files, {human(nbytes)}" if files else
               f"{human(nbytes)} of tar objects (file count read from the manifest)")
        f.addRow("Estimated", QLabel(est))
        self.dest = QLineEdit(dest)
        btn = QToolButton(); btn.setText("…")
        btn.clicked.connect(self._pick)
        h = QHBoxLayout(); h.addWidget(self.dest, 1); h.addWidget(btn)
        f.addRow("Destination", h)
        # -- which version: newest, or as the archive was at a past run / time
        from PySide6.QtWidgets import QDateTimeEdit
        from PySide6.QtCore import QDateTime
        self.v_new = QRadioButton("Newest archived copy of every file")
        self.v_run = QRadioButton("As of a past run")
        self.v_time = QRadioButton("As of a date and time")
        vg = QButtonGroup(self)
        for b in (self.v_new, self.v_run, self.v_time):
            vg.addButton(b)
        self.v_new.setChecked(True)
        self.runs = QComboBox(); self.runs.setMinimumWidth(420)
        self.runs.addItem("Loading run history…")
        self.when = QDateTimeEdit(QDateTime.currentDateTime())
        self.when.setCalendarPopup(True)
        self.when.setDisplayFormat("yyyy-MM-dd HH:mm:ss")
        f.addRow("Version", self.v_new)
        f.addRow("", self.v_run); f.addRow("", self.runs)
        f.addRow("", self.v_time); f.addRow("", self.when)
        self.v_note = QLabel(""); self.v_note.setWordWrap(True)
        self.v_note.setStyleSheet("color: gray")
        f.addRow("", self.v_note)
        only_exact = all(k in ("tar", "member") for k, _ in selections)
        for w in (self.v_run, self.v_time):
            w.setEnabled(not only_exact)
        for w in (self.v_new, self.v_run, self.v_time):
            w.toggled.connect(self._sync_version)
        if only_exact:
            self.v_note.setText("Tar objects and files picked inside a tar are restored "
                                "exactly as stored, so there is no version to choose.")
        self._sync_version()
        self._run_rows = []
        if manifest is not None and not only_exact:
            run_async(self, manifest.list_runs, self._fill_runs,
                      lambda m: (self.runs.clear(), self.runs.addItem("Run history unavailable")))
        self.overwrite = QCheckBox("Overwrite existing files (otherwise skip)")
        self.verify = QCheckBox("Verify MD5"); self.verify.setChecked(True)
        f.addRow("", self.overwrite); f.addRow("", self.verify)
        f.addRow("", QLabel("<small>Folders keep their own name under the destination; "
                            "a whole tar restores its files at their archived paths. "
                            "Tars in GLACIER / DEEP_ARCHIVE must be restored in S3 first.</small>"))
        bb = QDialogButtonBox()
        bb.addButton("Restore", QDialogButtonBox.AcceptRole)
        bb.addButton(QDialogButtonBox.Cancel)
        bb.accepted.connect(self.accept); bb.rejected.connect(self.reject)
        f.addRow(bb)

    def _pick(self):
        d = QFileDialog.getExistingDirectory(self, "Restore into", self.dest.text())
        if d:
            self.dest.setText(d)

    @staticmethod
    def _run_label(r):
        src = posixpath.basename((r.get("src_path") or "").rstrip("/")) or "/"
        if r.get("walk_paths"):
            try:
                walks = [posixpath.basename(w.rstrip("/")) for w in json.loads(r["walk_paths"])]
                src = ", ".join(walks[:3]) + (" …" if len(walks) > 3 else "")
            except ValueError:
                pass
        elif r.get("input_file"):
            src = "report " + posixpath.basename(r["input_file"])
        return mask(f"{fmt_ts(r['end_ts'])}  ·  {r['mode']}  ·  {r['files']:,} files  ·  {src}"
                    + ("" if r["status"] == "ok" else "  (partial)"))

    def _fill_runs(self, rows):
        self._run_rows = rows
        self.runs.clear()
        if not rows:
            self.runs.addItem("No completed runs recorded yet")
            self.v_run.setEnabled(False)
            return
        for r in rows:
            self.runs.addItem(self._run_label(r))
        self._sync_version()

    def _sync_version(self):
        self.runs.setEnabled(self.v_run.isChecked() and bool(self._run_rows)
                             if hasattr(self, "_run_rows") else False)
        self.when.setEnabled(self.v_time.isChecked())
        if self.v_run.isChecked() or self.v_time.isChecked():
            self.v_note.setText("Restores each file as it was at that point: the newest copy "
                                "archived up to then. Files archived later are left out. The "
                                "archive is append-only, so files deleted from the source "
                                "before then are still included.")
        elif self.v_new.isEnabled() and self.v_run.isEnabled():
            self.v_note.setText("")
        # the note wraps to a different height per choice: re-lay the dialog
        self.v_note.setMinimumHeight(self.v_note.heightForWidth(max(300, self.v_note.width()))
                                     if self.v_note.text() else 0)
        self.adjustSize()

    def as_of(self):
        """UTC cutoff for point-in-time restore, or None for 'newest'."""
        from datetime import timezone
        if self.v_run.isChecked() and self._run_rows:
            return self._run_rows[self.runs.currentIndex()]["end_ts"]
        if self.v_time.isChecked():
            return self.when.dateTime().toPython().astimezone(timezone.utc)
        return None


# --------------------------------------------------------------------------- #
# Jobs
# --------------------------------------------------------------------------- #
class Job(QObject):
    changed = Signal(object)
    log = Signal(str)
    finished = Signal(object)
    _seq = 0

    def __init__(self, kind, title):
        super().__init__()
        Job._seq += 1
        self.id = Job._seq
        self.kind, self.title = kind, title
        self.state = "starting"      # starting|running|cancelling|done|failed|cancelled
        self.phase = ""
        self.fraction = 0.0
        self.determinate = False
        self.detail = ""
        self.rate = 0.0
        self.eta = None
        self.active = []             # [(label, phase, done, total)]
        self.t0 = time.time()
        self._rate_hist = []

    def _final_rate(self, snap=None):
        """Once a job ends, show the run's average speed instead of the last
        10-second window (which mostly covers the final manifest commit)."""
        snap = snap or self.last or {}
        b, e = snap.get("bytes_done") or 0, snap.get("elapsed") or (time.time() - self.t0)
        if b and e > 0:
            self.rate = b / e

    def _rate(self, nbytes):
        now = time.time()
        self._rate_hist.append((now, nbytes))
        self._rate_hist = [x for x in self._rate_hist if now - x[0] <= 10]
        if len(self._rate_hist) >= 2:
            (t1, b1), (t2, b2) = self._rate_hist[0], self._rate_hist[-1]
            self.rate = (b2 - b1) / (t2 - t1) if t2 > t1 else 0.0
        return self.rate


class ArchiveJob(Job):
    def __init__(self, cmd, workdir, cfg: Config, title):
        super().__init__("archive", title)
        self.cmd = cmd
        self.proc = QProcess(self)
        self.proc.setWorkingDirectory(workdir)
        env = QProcessEnvironment.systemEnvironment()
        env.insert("PYTHONUNBUFFERED", "1")
        if cfg.profile:
            env.insert("AWS_PROFILE", cfg.profile)
        self.proc.setProcessEnvironment(env)
        self.proc.readyReadStandardOutput.connect(self._stdout)
        self.proc.readyReadStandardError.connect(self._stderr)
        self.proc.finished.connect(self._done)
        self.proc.errorOccurred.connect(lambda e: self.log.emit(f"[job {self.id}] process error: {e}"))
        self._outbuf = b""
        self._errbuf = b""
        self.last = {}
        self.summary = None
        self.failed_list = None
        self.incremental = "--incremental" in cmd
        self.run_id = None

    def retry_command(self):
        """Same run settings, but the source is this run's failed-file list
        (one absolute path per line). Same --src-path, so archived paths line up
        with the original run; same workers / scan threads, so it is just as
        parallel. Incremental is dropped: the list says exactly what to send."""
        if not self.failed_list or not os.path.isfile(self.failed_list):
            return None
        if any(a in ("--src-s3", "--src-manifest") for a in self.cmd):
            return self._retry_s3()
        out, skip = [], 0
        drop_with_value = {"--walk-path", "--input-file", "--input-format", "--path-column"}
        for i, a in enumerate(self.cmd):
            if skip:
                skip -= 1
                continue
            if a in drop_with_value:
                skip = 1
                continue
            if a == "--incremental":
                continue
            out.append(a)
        return out + ["--input-file", self.failed_list, "--input-format", "lines"]

    def _retry_s3(self):
        out, skip, base, listed = [], 0, None, None
        for i, a in enumerate(self.cmd):
            if skip:
                skip -= 1
                continue
            if a in ("--src-s3", "--src-manifest", "--src-manifest-format", "--src-prefix"):
                if a == "--src-prefix":
                    base = self.cmd[i + 1]
                if a == "--src-s3":
                    listed = self.cmd[i + 1].split("/", 3)[3] if self.cmd[i + 1].count("/") >= 3 else ""
                skip = 1
                continue
            if a == "--incremental":
                continue
            out.append(a)
        base = base if base is not None else (listed or "")
        return out + ["--src-manifest", self.failed_list, "--src-manifest-format", "uris",
                      "--src-prefix", base]

    def resume_command(self):
        """Same job again, skipping every file this run (and the runs it was
        itself resuming) already archived: an interrupted run completes with no
        gaps and no duplicates."""
        if not self.run_id:
            return None
        return list(self.cmd) + ["--resume-run", self.run_id]

    def failed_count(self):
        if not self.failed_list or not os.path.isfile(self.failed_list):
            return 0
        with open(self.failed_list, "rb") as f:
            return sum(1 for _ in f)

    def start(self):
        self.log.emit(f"[job {self.id}] $ {' '.join(self.cmd[1:])}")
        self.proc.start(self.cmd[0], self.cmd[1:])
        self.state = "running"
        self.phase = "scanning"
        self.changed.emit(self)

    def cancel(self):
        if self.proc.state() == QProcess.NotRunning:
            return
        if self.state != "cancelling":
            # SIGINT: the archiver drops queued tars (their files go to the
            # failed list for a retry), finishes the tars already in progress
            # and commits their manifest rows -- so nothing becomes an orphan.
            # No timer: on a slow link that can take minutes, and killing it
            # midway is what creates orphans.
            self.state = "cancelling"
            self.phase = "cancelling — finishing the tars in progress"
            self.log.emit(f"[job {self.id}] cancelling: queued tars dropped, tars in progress "
                          f"finish and are recorded (Cancel again to force-stop)")
            try:
                os.kill(self.proc.processId(), signal.SIGINT)
            except Exception:
                self.proc.terminate()
        else:
            if QMessageBox.question(
                    None, "Force stop",
                    "Stop now, without waiting for the tars in progress?\n\nTars that are "
                    "mid-upload may end up in S3 without manifest rows (orphans, flagged in "
                    "the Tar objects view).") == QMessageBox.Yes:
                self._force()
        self.changed.emit(self)

    def _force(self):
        if self.proc.state() != QProcess.NotRunning:
            self.log.emit(f"[job {self.id}] force-stopping")
            try:   # a second SIGINT makes the archiver skip the wait and unwind
                os.kill(self.proc.processId(), signal.SIGINT)
            except Exception:
                pass
            QTimer.singleShot(5_000, lambda: self.proc.state() != QProcess.NotRunning
                              and self.proc.terminate())
            QTimer.singleShot(10_000, lambda: self.proc.state() != QProcess.NotRunning and self.proc.kill())

    def _stderr(self):
        self._errbuf += bytes(self.proc.readAllStandardError())
        *lines, self._errbuf = self._errbuf.split(b"\n")
        for ln in lines:
            self.log.emit(f"[job {self.id}] {ln.decode('utf-8', 'replace')}")

    def _stdout(self):
        self._outbuf += bytes(self.proc.readAllStandardOutput())
        *lines, self._outbuf = self._outbuf.split(b"\n")
        snap = None
        for ln in lines:
            s = ln.decode("utf-8", "replace")
            if s.startswith(PROGRESS_TAG):
                try:
                    snap = json.loads(s[len(PROGRESS_TAG):])
                except ValueError:
                    pass
            elif s.strip():
                self.log.emit(f"[job {self.id}] {s}")
        if snap:   # only the newest snapshot matters
            self._apply(snap)

    def _apply(self, p):
        self.last = p
        scanned = p["scanned_bytes"]
        weighted = p["bytes_done"] + p.get("finished_uncollected_bytes", 0)
        for e in p.get("active", []):
            nb = e.get("nbytes", 0)
            frac = (e["done"] / e["total"]) if e.get("total") else 0
            weighted += nb * (0.5 * frac if e["phase"] == "bundling" else 0.5 + 0.5 * frac)
        self.fraction = min(1.0, weighted / scanned) if scanned else 0.0
        self.determinate = p["scan_done"]
        rate = self._rate(weighted)
        remaining = max(0, scanned - weighted)
        self.eta = remaining / rate if (p["scan_done"] and rate > 0) else None
        if not p["scan_done"]:
            self.phase = "scanning + bundling" if p["active"] else "scanning"
        elif p["active"]:
            self.phase = "bundling / uploading"
        else:
            self.phase = "committing manifest"
        found = (f"{p['scanned_files']:,} files / {human(scanned)} found"
                 + ("" if p["scan_done"] else " so far"))
        if self.incremental:
            checked = p["scanned_files"] + p.get("unchanged", 0)
            found = (f"{checked:,} checked · {p['scanned_files']:,} changed / {human(scanned)}"
                     + ("" if p["scan_done"] else " so far"))
            if not p["scan_done"] and not p["active"]:
                self.phase = "checking for changes"
        self.detail = (f"{p['files_done']:,} archived · {p['tars_done']:,} tars · "
                       f"{p['rows_committed']:,} manifest rows · {p['failed']:,} failed · {found}")
        self.active = [(posixpath.basename(e["tar"]), e["phase"], e["done"], e["total"],
                        e.get("nfiles", 0)) for e in p.get("active", [])]
        self.counts = (p.get("bundling", 0), p.get("uploading", 0))
        if p.get("failed_list"):
            self.failed_list = p["failed_list"]
        if p.get("run_id"):
            self.run_id = p["run_id"]
        if p.get("unchanged") and not self.incremental:
            self.detail += f" · {p['unchanged']:,} unchanged (incremental)"
        if p.get("skipped"):
            self.detail += f" · {p['skipped']:,} skipped (symlinks/special)"
        if p.get("ev") == "done":
            self.summary = p
        self.changed.emit(self)

    def _done(self, code, status):
        if self._outbuf:
            self._stdout()
        if self._errbuf:
            self.log.emit(f"[job {self.id}] {self._errbuf.decode('utf-8', 'replace')}")
            self._errbuf = b""
        if self.state == "cancelling":
            self.state = "cancelled"
        else:
            self.state = "done" if code == 0 and status == QProcess.NormalExit else "failed"
        if self.state == "done":
            self.fraction, self.determinate = 1.0, True
        self.active = []
        self.phase = {"done": "completed", "failed": f"failed (exit {code})",
                      "cancelled": "cancelled — Resume continues where it stopped"}[self.state]
        nfail = (self.summary or self.last or {}).get("failed", 0)
        if nfail and self.state == "done":
            self.phase = f"completed — {nfail:,} files failed (Retry failed files)"
        self.eta = None
        self._final_rate()
        self.changed.emit(self)
        self.finished.emit(self)


class RemoteJob(ArchiveJob):
    """An S3-source archive running on an EC2 instance (started through SSM).
    Same interface as ArchiveJob for the monitor, Cancel, Retry and Resume; the
    progress comes from the status object the archiver writes to S3."""
    POLL_MS = 3000

    def __init__(self, cmd, cfg, title, instance, run_id=None, pid=None, status_uri=None):
        super().__init__(cmd, ".", cfg, f"{title}  @ {instance.get('name') or instance['id']}")
        import remote as R
        self.remote = R.Remote(cfg)
        self.instance = instance
        self.run_id, self.pid, self.status_uri = run_id, pid, status_uri
        self.settings = None
        self._seq = 0
        self._last_update = time.time()
        self._checking = False
        self._polling = False
        self.timer = QTimer(self)
        self.timer.timeout.connect(self._poll)
        self._sig = _Signals()

    # -- lifecycle ----------------------------------------------------------
    def start(self):
        self.state, self.phase = "running", "starting on EC2 (preparing runner…)"
        self.changed.emit(self)
        rid = self.run_id or self.remote.new_run_id()
        args = self.cmd[3:]       # [python, -u, script, ...args]

        def ok(info):
            self.run_id, self.pid, self.status_uri = info["run_id"], info["pid"], info["status_uri"]
            self.log.emit(f"[job {self.id}] started on {self.instance['id']} as pid {self.pid}; "
                          f"progress: {self.status_uri}")
            self.phase = "running on EC2"
            self._persist()
            self.timer.start(self.POLL_MS)
            self.changed.emit(self)

        def fail(msg):
            self.state, self.phase = "failed", "could not start on EC2"
            self.log.emit(f"[job {self.id}] {msg}")
            self.changed.emit(self); self.finished.emit(self)

        self.log.emit(f"[job {self.id}] starting on EC2 {self.instance['id']} via SSM")
        run_async(self, self.remote.start, ok, fail, self.instance["id"], args, rid)

    def attach(self):
        """Reconnect to a run started by an earlier GUI session."""
        self.state, self.phase = "running", "running on EC2"
        self.log.emit(f"[job {self.id}] reattached to {self.run_id} on {self.instance['id']}")
        self.timer.start(self.POLL_MS)
        self.changed.emit(self)

    def _persist(self, remove=False):
        if self.settings is None:
            return
        jobs = [j for j in json.loads(self.settings.get("remote_jobs", "[]") or "[]")
                if j.get("run_id") != self.run_id]
        if not remove:
            jobs.append({"run_id": self.run_id, "pid": self.pid, "status_uri": self.status_uri,
                         "instance": self.instance, "cmd": self.cmd, "title": self.title})
        self.settings.set("remote_jobs", json.dumps(jobs))

    def _poll(self):
        if self._polling or not self.status_uri:
            return
        self._polling = True

        def ok(snap):
            self._polling = False
            if snap:
                self._take(snap)
            elif time.time() - self._last_update > 180:
                self._check_alive()

        def fail(msg):
            self._polling = False
            self.log.emit(f"[job {self.id}] status read failed: {msg.splitlines()[0]}")

        run_async(self, self.remote.read_status, ok, fail, self.status_uri)

    def _take(self, snap):
        if snap.get("updated") != getattr(self, "_last_seen", None):
            self._last_seen = snap.get("updated")
            self._last_update = time.time()
        for seq, line in snap.get("log", []):
            if seq > self._seq:
                self._seq = seq
                self.log.emit(f"[job {self.id}] {line}")
        state = self.state
        self._apply(snap)
        self.failed_list = snap.get("failed_list_s3") or None
        self.state = state
        if snap.get("ev") == "done" and snap.get("status"):
            self._finish(snap)
        elif time.time() - self._last_update > 90:
            self._check_alive()

    def _check_alive(self):
        if self._checking or not self.pid:
            return
        self._checking = True

        def ok(alive):
            self._checking = False
            if not alive:
                self.timer.stop()
                self.state, self.phase = "failed", "the archiver stopped on EC2 without a final status"
                self.log.emit(f"[job {self.id}] pid {self.pid} is gone; see {REMOTE_LOG_HINT}")
                self._persist(remove=True)
                self.changed.emit(self); self.finished.emit(self)
            else:
                self._last_update = time.time()

        run_async(self, self.remote.alive, ok, lambda m: setattr(self, "_checking", False),
                  self.instance["id"], self.pid)

    def _finish(self, snap):
        self.timer.stop()
        st = snap["status"]
        self.state = {"ok": "done", "partial": "done", "cancelled": "cancelled"}.get(st, "failed")
        if self.state == "done":
            self.fraction, self.determinate = 1.0, True
        self.active = []
        self.phase = {"done": "completed on EC2", "failed": "failed on EC2",
                      "cancelled": "cancelled — Resume continues where it stopped"}[self.state]
        if snap.get("failed") and self.state == "done":
            self.phase = f"completed on EC2 — {snap['failed']:,} files failed (Retry failed files)"
        self.eta = None
        self._final_rate(snap)
        self._persist(remove=True)
        self.changed.emit(self); self.finished.emit(self)

    # -- actions --------------------------------------------------------------
    def cancel(self):
        if self.state not in ("running", "cancelling") or not self.pid:
            return
        force = self.state == "cancelling"
        if force and QMessageBox.question(
                None, "Force stop", "Stop the EC2 run now, without waiting for the tars in "
                "progress?\n\nTars that are mid-upload may end up without manifest rows "
                "(orphans, flagged in the Tar objects view).") != QMessageBox.Yes:
            return
        self.state = "cancelling"
        self.phase = "cancelling on EC2 — finishing the tars in progress"
        self.log.emit(f"[job {self.id}] {'force-stopping' if force else 'cancelling'} pid {self.pid} via SSM")
        run_async(self, self.remote.cancel, lambda *_: None,
                  lambda m: self.log.emit(f"[job {self.id}] cancel failed: {m.splitlines()[0]}"),
                  self.instance["id"], self.pid, force)
        self.changed.emit(self)

    def retry_command(self):
        if not self.failed_list:
            return None
        return self._retry_s3()

    def failed_count(self):
        if not self.failed_list:
            return 0
        if getattr(self, "_failed_n", None) is None:
            try:
                self._failed_n = sum(1 for ln in self.remote.fetch_text(self.failed_list).splitlines() if ln.strip())
            except Exception:
                return 0
        return self._failed_n


REMOTE_LOG_HINT = "/var/lib/sfa/<run id>.log on the instance"


class RestoreJob(Job):
    def __init__(self, cfg, manifest, selections, dest, overwrite, verify, concurrency,
                 est_files, est_bytes, title, as_of=None):
        super().__init__("restore", title)
        self.as_of = as_of
        self.engine = RestoreEngine(cfg, dest, overwrite=overwrite, verify_md5=verify,
                                    concurrency=concurrency)
        self.manifest = manifest
        self.selections = selections
        self.est = (est_files, est_bytes)
        self.timer = QTimer(self)
        self.timer.timeout.connect(self._poll)
        self._sig = _Signals()
        self._sig.done.connect(self._finish)
        self._sig.failed.connect(self._fail)
        self._logged = 0

    def start(self):
        self.state, self.phase = "running", "querying manifest"
        self.log.emit(f"[job {self.id}] restore {len(self.selections)} item(s) -> {self.engine.dest}")
        if self.as_of is not None:
            self.log.emit(f"[job {self.id}] point-in-time: newest copies archived at or "
                          f"before {fmt_ts(self.as_of)}")
        t = Task(self.engine.run, self.manifest, self.selections, as_of=self.as_of)
        t.signals = self._sig
        QThreadPool.globalInstance().start(t)
        self.timer.start(500)
        self.changed.emit(self)

    def cancel(self):
        if self.state == "running":
            self.state = "cancelling"
            self.engine.cancel.set()
            self.changed.emit(self)

    def _poll(self):
        st = self.engine.stats
        total_b = st.bytes_total if st.streaming_done else max(st.bytes_total, self.est[1])
        total_f = st.files_total if st.streaming_done else max(st.files_total, self.est[0])
        done_b = st.bytes_done
        target = max(1, total_b - 0)
        self.fraction = min(1.0, (done_b + 0) / target) if total_b else 0.0
        self.determinate = st.streaming_done or self.est[1] > 0
        rate = self._rate(done_b)
        rem = max(0, total_b - done_b)
        self.eta = rem / rate if rate > 0 and self.determinate else None
        if self.state == "running":
            self.phase = "downloading" if st.streaming_done else "querying + downloading"
        self.detail = (f"{st.files_done:,} / {total_f:,} files · {human(done_b)} / {human(total_b)} · "
                       f"{st.skipped:,} skipped · {st.failed:,} failed · "
                       f"{st.requests:,} GETs ({human(st.bytes_fetched)} fetched)")
        for msg in st.errors[self._logged:]:
            self.log.emit(f"[job {self.id}] ERROR {msg}")
        self._logged = len(st.errors)
        self.changed.emit(self)

    def _finish(self, _):
        self.timer.stop()
        self._poll()
        st = self.engine.stats
        if self.state == "cancelling":
            self.state, self.phase = "cancelled", "cancelled"
        elif st.failed:
            self.state, self.phase = "failed", f"completed with {st.failed:,} failures"
        else:
            self.state, self.phase = "done", "completed"
            self.fraction, self.determinate = 1.0, True
        self.eta = None
        self.log.emit(f"[job {self.id}] restore {self.phase}: {self.detail}")
        self.changed.emit(self)
        self.finished.emit(self)

    def _fail(self, msg):
        self.timer.stop()
        self.state, self.phase = "failed", "failed"
        self.log.emit(f"[job {self.id}] restore failed: {msg}")
        self.changed.emit(self)
        self.finished.emit(self)


# --------------------------------------------------------------------------- #
# Monitoring pane
# --------------------------------------------------------------------------- #
class MonitorPane(QTabWidget):
    COLS = ["Job", "Status", "Progress", "Rate", "ETA", "Details"]

    def __init__(self, parent=None):
        super().__init__(parent)
        self.tree = QTreeWidget()
        self.tree.setHeaderLabels(self.COLS)
        self.tree.setRootIsDecorated(True)
        self.tree.setUniformRowHeights(True)
        hh = self.tree.header()
        hh.setSectionResizeMode(0, QHeaderView.Interactive); hh.resizeSection(0, 300)
        hh.setSectionResizeMode(1, QHeaderView.Interactive); hh.resizeSection(1, 280)
        hh.setSectionResizeMode(2, QHeaderView.Interactive); hh.resizeSection(2, 220)
        hh.setSectionResizeMode(3, QHeaderView.ResizeToContents)
        hh.setSectionResizeMode(4, QHeaderView.ResizeToContents)
        hh.setStretchLastSection(True)
        self.log = QPlainTextEdit(); self.log.setReadOnly(True)
        self.log.setMaximumBlockCount(5000)     # PB runs log millions of lines
        self.log.setFont(QFont("Menlo", 10))
        w = QWidget(); v = QVBoxLayout(w); v.setContentsMargins(0, 0, 0, 0)
        bar = QHBoxLayout()
        self.btn_cancel = QPushButton("Cancel selected job")
        self.btn_clear = QPushButton("Clear finished")
        self.btn_retry = QPushButton("Retry failed files")
        self.btn_retry.setToolTip("Re-run the selected archive job's failed files "
                                  "(its failed-file list), with the same settings and parallelism")
        self.btn_retry.setEnabled(False)
        self.btn_open_failed = QPushButton("Show failed list")
        self.btn_open_failed.setEnabled(False)
        self.btn_resume = QPushButton("Resume")
        self.btn_resume.setToolTip("Continue a cancelled or failed archive job: the same source "
                                   "again, skipping files this run already archived")
        self.btn_resume.setEnabled(False)
        bar.addStretch(1)
        for b in (self.btn_open_failed, self.btn_resume, self.btn_retry, self.btn_clear, self.btn_cancel):
            bar.addWidget(b)
        v.addWidget(self.tree, 1); v.addLayout(bar)
        self.addTab(w, "Monitoring")
        self.addTab(self.log, "Log")
        self.items = {}
        self.jobs = {}
        self.btn_cancel.clicked.connect(self._cancel)
        self.btn_clear.clicked.connect(self._clear)
        self.tree.currentItemChanged.connect(lambda *_: self._sync_buttons())
        self.btn_open_failed.clicked.connect(self._open_failed)

    def add(self, job):
        it = QTreeWidgetItem([mask(f"#{job.id} {job.kind}: {job.title}"), "", "", "", "", ""])
        self.tree.insertTopLevelItem(0, it)
        bar = QProgressBar(); bar.setMaximumHeight(16); bar.setTextVisible(True)
        self.tree.setItemWidget(it, 2, bar)
        self.items[job.id] = (it, bar)
        self.jobs[job.id] = job
        job.changed.connect(self.update_job)
        job.log.connect(self.append_log)
        it.setExpanded(True)
        self.tree.setCurrentItem(it)

    def append_log(self, s):
        self.log.appendPlainText(mask(s))

    def update_job(self, job):
        it, bar = self.items[job.id]
        if self.selected_job() is job:
            self._sync_buttons()
        # running: current phase; finished: the outcome ("completed — 3 files
        # failed (Retry failed files)", "cancelled — …"), not just the state
        it.setText(1, job.phase or job.state)
        it.setToolTip(1, job.phase or job.state)
        if job.determinate:
            bar.setRange(0, 1000); bar.setValue(int(job.fraction * 1000))
            bar.setFormat(f"{job.fraction * 100:.1f}%")
        else:
            # total still growing (scan in progress): show progress against
            # what has been found so far, flagged with '~'
            bar.setRange(0, 1000); bar.setValue(int(job.fraction * 1000))
            bar.setFormat(f"~{job.fraction * 100:.0f}% of found")
        it.setText(3, f"{human(job.rate)}/s" if job.rate else "")
        it.setText(4, fmt_dur(job.eta) if job.eta is not None else "")
        it.setText(5, job.detail)
        it.setToolTip(5, job.detail)
        # child rows: one per active tar (bounded by worker count)
        want = job.active
        while it.childCount() > len(want):
            it.removeChild(it.child(it.childCount() - 1))
        for i, (name, phase, done, total, nfiles) in enumerate(want):
            if i < it.childCount():
                c = it.child(i)
            else:
                c = QTreeWidgetItem(["", "", "", "", "", ""])
                it.addChild(c)
                cb = QProgressBar(); cb.setMaximumHeight(14)
                self.tree.setItemWidget(c, 2, cb)
            c.setText(0, f"   {name}")
            c.setText(1, phase)
            cb = self.tree.itemWidget(c, 2)
            cb.setRange(0, 1000)
            cb.setValue(int(1000 * done / total) if total else 0)
            cb.setFormat(f"{human(done)} / {human(total)}")
            c.setText(5, f"{nfiles:,} files")

    def selected_job(self):
        it = self.tree.currentItem()
        if it is None:
            return None
        while it.parent():
            it = it.parent()
        for jid, (i, _) in self.items.items():
            if i is it:
                return self.jobs[jid]
        return None

    def _sync_buttons(self):
        j = self.selected_job()
        n = j.failed_count() if isinstance(j, ArchiveJob) and j.state in (
            "done", "failed", "cancelled") else 0
        self.btn_retry.setEnabled(n > 0)
        self.btn_retry.setText(f"Retry failed files ({n:,})" if n else "Retry failed files")
        self.btn_open_failed.setEnabled(bool(isinstance(j, ArchiveJob) and j.failed_list))
        self.btn_resume.setEnabled(bool(isinstance(j, ArchiveJob) and j.run_id
                                        and j.state in ("cancelled", "failed")))

    def _open_failed(self):
        j = self.selected_job()
        if isinstance(j, RemoteJob) and j.failed_list:
            import tempfile
            tsv = j.failed_list[:-4] + ".tsv"
            path = os.path.join(tempfile.mkdtemp(prefix="sfa-failed-"), "failed.tsv")
            with open(os.open(path, os.O_WRONLY | os.O_CREAT, 0o600), "w") as fh:
                fh.write(j.remote.fetch_text(tsv))
            QProcess.startDetached("open", ["-R", path])
            return
        if j and j.failed_list:
            tsv = j.failed_list[:-4] + ".tsv"
            QProcess.startDetached("open", ["-R", tsv if os.path.isfile(tsv) else j.failed_list])

    def _cancel(self):
        it = self.tree.currentItem()
        if it is None:
            return
        while it.parent():
            it = it.parent()
        for jid, (i, _) in self.items.items():
            if i is it:
                self.jobs[jid].cancel()

    def _clear(self):
        for jid in list(self.items):
            job = self.jobs[jid]
            if job.state in ("done", "failed", "cancelled"):
                it, _ = self.items.pop(jid)
                self.tree.takeTopLevelItem(self.tree.indexOfTopLevelItem(it))
                self.jobs.pop(jid)

    def running(self):
        return [j for j in self.jobs.values() if j.state in ("starting", "running", "cancelling")]


# --------------------------------------------------------------------------- #
# Main window
# --------------------------------------------------------------------------- #
class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Small Files Archiver")
        self.resize(1500, 950)
        self.s = Settings()

        self.source = SourcePane(self.s)
        self.local = self.source.local          # restore target + local archive source
        self.archive = ArchivePane(self.s)
        mid = QWidget(); mv = QVBoxLayout(mid); mv.addStretch(1)
        self.btn_arch = QPushButton("Archive →"); self.btn_arch.setToolTip("Bundle the selected local files/folders into tars on S3")
        self.btn_rest = QPushButton("← Restore"); self.btn_rest.setToolTip("Restore the selected archived files/folders into the local folder")
        for b in (self.btn_arch, self.btn_rest):
            b.setMinimumWidth(110); mv.addWidget(b)
        mv.addStretch(1)

        top = QSplitter(Qt.Horizontal)
        top.addWidget(self.source); top.addWidget(mid); top.addWidget(self.archive)
        top.setStretchFactor(0, 5); top.setStretchFactor(1, 0); top.setStretchFactor(2, 6)
        self.monitor = MonitorPane()
        vs = QSplitter(Qt.Vertical)
        vs.addWidget(top); vs.addWidget(self.monitor)
        vs.setStretchFactor(0, 3); vs.setStretchFactor(1, 2)
        self.setCentralWidget(vs)

        tb = QToolBar("Main"); tb.setMovable(False); self.addToolBar(tb)
        a_set = QAction("Settings", self); a_set.setShortcut(QKeySequence.Preferences)
        a_set.triggered.connect(self.open_settings)
        a_wiz = QAction("Setup wizard", self)
        a_wiz.triggered.connect(self.open_wizard)
        a_arch = QAction("Archive →", self); a_arch.triggered.connect(self.do_archive)
        a_rest = QAction("← Restore", self); a_rest.triggered.connect(self.do_restore)
        a_ref = QAction("Refresh archive", self); a_ref.setShortcut(QKeySequence.Refresh)
        a_ref.triggered.connect(self.archive.refresh)
        for a in (a_set, a_wiz, a_ref):
            tb.addAction(a)
        tb.addSeparator(); tb.addAction(a_arch); tb.addAction(a_rest)
        self.btn_arch.clicked.connect(self.do_archive)
        self.btn_rest.clicked.connect(self.do_restore)

        sb = QStatusBar(); self.setStatusBar(sb)
        self.sb_text = QLabel("Idle")
        self.sb_bar = QProgressBar(); self.sb_bar.setMaximumWidth(320); self.sb_bar.setMaximumHeight(16)
        self.sb_bar.setVisible(False)
        sb.addWidget(self.sb_text, 1); sb.addPermanentWidget(self.sb_bar)
        self.archive.status.connect(self._status_msg)
        self.monitor.btn_retry.clicked.connect(self.retry_failed)
        self.monitor.btn_resume.clicked.connect(self.resume_job)

        self.sb_timer = QTimer(self); self.sb_timer.timeout.connect(self._update_statusbar)
        self.sb_timer.start(500)

        if not self.s.config().complete():
            QTimer.singleShot(300, self.open_wizard)
        else:
            QTimer.singleShot(500, self._reattach_remote)

    def _status_msg(self, msg):
        self.monitor.append_log(msg)
        self.statusBar().showMessage(mask(msg.splitlines()[0]), 10_000)

    def open_settings(self):
        if SettingsDialog(self.s, self).exec():
            self.archive.connect_backend()

    def open_wizard(self):
        from setup_wizard import SetupWizard
        if SetupWizard(self.s, self).exec():
            self.archive.connect_backend()

    # -- actions -------------------------------------------------------------
    def do_archive(self):
        cfg = self.s.config()
        if not cfg.complete():
            return self.open_wizard()
        if not cfg.table_bucket_arn:
            QMessageBox.information(self, "Archive", "Still connecting to the archive — try again "
                                    "in a moment (see the Log tab if it doesn't connect).")
            return
        if self.source.mode == "s3":
            src = self.source.s3
            if not src.bucket:
                QMessageBox.information(self, "Archive", "Pick a source bucket in the left pane first.")
                return
            dlg = S3ArchiveDialog(src.bucket, src.cwd, src.selected(), self.s, self,
                                  manifest=self.archive.manifest)
        else:
            paths = self.local.selected_paths()
            dlg = ArchiveDialog(paths, self.s, self, manifest=self.archive.manifest,
                                report_dir=self.local.cwd)
        if not dlg.exec():
            return
        cmd, err = dlg.command()
        if err:
            QMessageBox.warning(self, "Archive", mask(err))
            return
        inst = dlg.run_on_instance() if isinstance(dlg, S3ArchiveDialog) else None
        if inst:
            self.start_remote(cmd, dlg.title(), inst)
        else:
            self.start_archive(cmd, dlg.title())

    def start_remote(self, cmd, title, instance, run_id=None, pid=None, status_uri=None):
        job = RemoteJob(cmd, self.s.config(), title, instance, run_id, pid, status_uri)
        job.settings = self.s
        self.monitor.add(job)
        job.finished.connect(self._job_finished)
        if status_uri:
            job.attach()
        else:
            job.start()
        return job

    def _reattach_remote(self):
        try:
            jobs = json.loads(self.s.get("remote_jobs", "[]") or "[]")
        except ValueError:
            jobs = []
        for j in jobs:
            self.start_remote(j["cmd"], j["title"].split("  @ ")[0], j["instance"],
                              j["run_id"], j.get("pid"), j["status_uri"])

    def start_archive(self, cmd, title):
        cfg = self.s.config()
        job = ArchiveJob(cmd, self.s.workdir(), cfg, title)
        self.monitor.add(job)
        job.finished.connect(self._job_finished)
        job.start()

    def do_restore(self):
        if self.archive.manifest is None:
            QMessageBox.information(self, "Restore", "Archive is not connected.")
            return
        sel, files, nbytes, skipped = self.archive.selections()
        if not sel:
            msg = ("Select files or folders in the archive pane." if self.archive.mode == "files"
                   else "Select tar objects (or open a tar and select files). "
                        "S3 folders can't be restored as a whole — open them first.")
            QMessageBox.information(self, "Restore", msg)
            return
        if skipped:
            self._status_msg(f"Restore: {skipped} selected S3 folder(s)/non-tar object(s) ignored")
        dlg = RestoreDialog(sel, files, nbytes, self.local.restore_target(), self,
                            manifest=self.archive.manifest)
        if not dlg.exec():
            return
        dest = dlg.dest.text().strip()
        if not os.path.isdir(dest):
            QMessageBox.warning(self, "Restore", mask(f"{dest} is not a folder."))
            return
        first = selection_label(*sel[0])
        title = first + (f" +{len(sel) - 1}" if len(sel) > 1 else "")
        as_of = dlg.as_of()
        if as_of is not None:
            title += f" (as of {fmt_ts(as_of)})"
        job = RestoreJob(self.s.config(), self.archive.manifest, sel, dest,
                         dlg.overwrite.isChecked(), dlg.verify.isChecked(),
                         self.s.restore_concurrency(), files, nbytes, f"{title} → {dest}",
                         as_of=as_of)
        self.monitor.add(job)
        job.finished.connect(self._job_finished)
        job.start()

    def resume_job(self):
        j = self.monitor.selected_job()
        if not isinstance(j, ArchiveJob):
            return
        cmd = j.resume_command()
        if cmd is None:
            QMessageBox.information(self, "Resume", "This job has no run to resume.")
            return
        if QMessageBox.question(
                self, "Resume archive job",
                f"Continue job #{j.id}? The same source is archived again, skipping every "
                f"file this run already archived, so nothing is duplicated.") != QMessageBox.Yes:
            return
        if isinstance(j, RemoteJob):
            self.start_remote(cmd, f"resume of #{j.id}", j.instance)
        else:
            self.start_archive(cmd, f"resume of #{j.id}")

    def retry_failed(self):
        j = self.monitor.selected_job()
        if not isinstance(j, ArchiveJob):
            return
        cmd = j.retry_command()
        if cmd is None:
            QMessageBox.information(self, "Retry", "This job has no failed-file list.")
            return
        n = j.failed_count()
        if QMessageBox.question(
                self, "Retry failed files",
                f"Archive the {n:,} files that failed in job #{j.id} again, with the same "
                f"destination and settings?\n\nFiles that fail again go to a new failed "
                f"list.") != QMessageBox.Yes:
            return
        if isinstance(j, RemoteJob):
            self.start_remote(cmd, f"retry of #{j.id} ({n:,} files)", j.instance)
        else:
            self.start_archive(cmd, f"retry of #{j.id} ({n:,} files)")

    def _job_finished(self, job):
        self._status_msg(f"Job #{job.id} {job.kind} {job.state}: {job.detail}")
        # refresh even after a failed/cancelled run: rows committed before the
        # failure are real and browsable
        if job.kind == "archive":
            self.archive.refresh()

    def _update_statusbar(self):
        run = self.monitor.running()
        if not run:
            self.sb_text.setText("Idle")
            self.sb_bar.setVisible(False)
            return
        bundling = uploading = 0
        restoring = 0
        rate = 0.0
        frac = []
        for j in run:
            rate += j.rate
            frac.append(j.fraction)
            if j.kind == "archive":
                b, u = getattr(j, "counts", (0, 0))
                bundling += b; uploading += u
            else:
                restoring += 1
        parts = [f"{len(run)} job(s) running"]
        if bundling:
            parts.append(f"bundling {bundling} tar(s)")
        if uploading:
            parts.append(f"uploading {uploading} tar(s)")
        if restoring:
            parts.append(f"restoring {restoring}")
        parts.append(f"{human(rate)}/s")
        self.sb_text.setText("  ·  ".join(parts))
        self.sb_bar.setVisible(True)
        self.sb_bar.setRange(0, 1000)
        self.sb_bar.setValue(int(1000 * sum(frac) / len(frac)))

    def closeEvent(self, ev):
        run = self.monitor.running()
        # EC2 runs are detached on the instance: quitting only stops watching them
        # (they reattach on the next start). Only local jobs are cancelled.
        remote = [j for j in run if isinstance(j, RemoteJob)]
        local = [j for j in run if not isinstance(j, RemoteJob)]
        if local:
            msg = f"{len(local)} local job(s) still running. Cancel them and quit?"
            if remote:
                msg += (f"\n\n{len(remote)} EC2 job(s) keep running on the instance and "
                        f"reattach the next time you open the app.")
            if QMessageBox.question(self, "Quit", msg) != QMessageBox.Yes:
                ev.ignore()
                return
        for j in remote:
            j.timer.stop()
        for j in local:
            j.cancel()
        for j in local:
            if isinstance(j, ArchiveJob) and getattr(j, "proc", None) is not None:
                j.proc.waitForFinished(15_000)
        ev.accept()


def main():
    app = QApplication(sys.argv)
    app.setApplicationName("Small Files Archiver")
    w = MainWindow()
    w.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
