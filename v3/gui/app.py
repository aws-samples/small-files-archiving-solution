"""
Small Files Archiver -- desktop GUI (PySide6).

    Left  : local disk browser
    Right : archive browser -- the *untarred* file tree reconstructed from the
            S3 Tables manifest (tar objects themselves stay hidden)
    Bottom: monitoring pane -- per-job progress, per-tar bundling / uploading
            bars, log; overall progress in the status bar

Archive (left -> right) runs ../fss3-archiver-s3tables-mp.py as a subprocess
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
    QFileIconProvider)

import backend
from backend import Config, Manifest, RestoreEngine, human, mask

HERE = Path(__file__).resolve().parent
DEFAULT_ARCHIVER = HERE.parent / "fss3-archiver-s3tables-mp.py"
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
        return self.get("archiver", str(DEFAULT_ARCHIVER)) or str(DEFAULT_ARCHIVER)

    def workdir(self):
        return self.get("workdir", str(HERE.parent)) or str(HERE.parent)

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
            e = QLineEdit(mask(self.s.get(k, default)))   # ~/… instead of the home path
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
        line("archiver", "Archiver script", mask(str(DEFAULT_ARCHIVER)), str(DEFAULT_ARCHIVER))
        line("workdir", "Working dir (logs/)", mask(str(HERE.parent)), str(HERE.parent))
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
        self.cwd = path
        self.view.setRootIndex(self.model.index(path))
        self.bar.set_path(path)

    def _open(self, idx):
        p = self.model.filePath(idx)
        if os.path.isdir(p):
            self.cd(p)

    def selected_paths(self):
        rows = {self.model.filePath(i) for i in self.view.selectionModel().selectedRows(0)}
        return sorted(rows)

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
        "members": ["Path in archive", "Size", "Archived", "Byte range", "MD5"],
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
                        self._range(it), it.get("md5") or ""][c]
            if role == Qt.DecorationRole and c == 0:
                return self.icon_file
            if role == Qt.ToolTipRole:
                return f"{it['source_key']}\nmd5: {it.get('md5') or '-'}"
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
        v.addWidget(self.bar); v.addLayout(sw); v.addLayout(srow); v.addWidget(self.who)
        v.addWidget(self.view, 1); v.addLayout(foot)
        self.bar.navigate.connect(self._navigate_text)
        self.bar.up.connect(self._up)
        self.btn_refresh.clicked.connect(self.refresh)
        self.search.returnPressed.connect(self._search)
        self._update_chrome()
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
        widths = list(self.DEFAULT_WIDTHS[mode])
        rest = sum(w for w in widths[1:] if w)
        widths[0] = max(220, self.view.viewport().width() - rest - 4)
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
            # no tar opened, selection untouched or cleared)? Then return to that
            # folder -- the tars' common folder is usually much higher, because
            # one tar mixes files from many folders.
            link = getattr(self, "_link", None)
            if link and not self.tar_open and self.tar_prefix == link["prefix"]:
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
                self._link = {"folder": self.cwd, "prefix": prefix, "auto": set()}
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
                rows = [n for n, i in enumerate(self.model.items)
                        if i["kind"] == "object" and i["key"] in want]
                for n in rows:
                    sm.select(self.model.index(n, 0),
                              QItemSelectionModel.SelectionFlag.Select
                              | QItemSelectionModel.SelectionFlag.Rows)
                if rows:
                    self.view.scrollTo(self.model.index(rows[0], 0))
                link = getattr(self, "_link", None)
                if link:
                    link["auto"] = {self.model.items[n]["key"] for n in rows}
                self._linked_note = (f"Selected {len(rows):,} tar(s) holding files of "
                                     f"/{folder}" + (" (sampled)" if sampled else "")
                                     + (f"; {len(want) - len(rows):,} more on later pages"
                                        if len(want) > len(rows) else ""))
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
        self.sel_lbl = QLabel(src or "(nothing selected)")
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
        cmd += ["--dst-bucket", cfg.bucket, "--dst-prefix", self.prefix.text().strip().strip("/")]
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
        self.dest_lbl.setText(f"→ s3://{cfg.bucket}/{self.prefix.text().strip().strip('/')}/<date>/archive_*.tar")
        cmd, err = self.command()
        self.btn_run.setEnabled(err is None)
        if err:
            self.preview.setPlainText("⚠ " + err)
        else:
            import shlex
            shown = [f"<table bucket {cfg.table_bucket}>" if c == cfg.table_bucket_arn else c
                     for c in cmd[1:]]
            self.preview.setPlainText(mask(" ".join(shlex.quote(c) for c in shown)))

    def _accept(self):
        if self.compress.isChecked() and QMessageBox.question(
                self, "Compression",
                "gzip tars cannot be restored file-by-file (byte offsets are not "
                "recorded). Continue?") != QMessageBox.Yes:
            return
        self.s.set("last_prefix", self.prefix.text().strip())
        self.s.set("last_size", self.size.currentText())
        self.s.set("last_sclass", self.sclass.currentText())
        self.accept()

    def title(self):
        if self.src_rep.isChecked():
            return "report " + os.path.basename(self.report.text().strip())
        t = os.path.basename(self.paths[0]) + (f" +{len(self.paths) - 1}" if len(self.paths) > 1 else "")
        return t + (" (incremental)" if self.incr.isChecked() else "")


class RestoreDialog(QDialog):
    def __init__(self, selections, files, nbytes, dest, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Restore to local disk")
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

    def retry_command(self):
        """Same run settings, but the source is this run's failed-file list
        (one absolute path per line). Same --src-path, so archived paths line up
        with the original run; same workers / scan threads, so it is just as
        parallel. Incremental is dropped: the list says exactly what to send."""
        if not self.failed_list or not os.path.isfile(self.failed_list):
            return None
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
            # SIGINT: the archiver unwinds -- flushes pending manifest rows and
            # stops the writer -- instead of dying mid-commit.
            self.state = "cancelling"
            self.log.emit(f"[job {self.id}] cancelling (SIGINT); in-flight tars finish first")
            try:
                os.kill(self.proc.processId(), signal.SIGINT)
            except Exception:
                self.proc.terminate()
            QTimer.singleShot(60_000, self._force)
        else:
            self._force()
        self.changed.emit(self)

    def _force(self):
        if self.proc.state() != QProcess.NotRunning:
            self.log.emit(f"[job {self.id}] still running; terminating")
            self.proc.terminate()
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
        self.detail = (f"{p['files_done']:,} archived · {p['tars_done']:,} tars · "
                       f"{p['rows_committed']:,} manifest rows · {p['failed']:,} failed · {found}")
        self.active = [(posixpath.basename(e["tar"]), e["phase"], e["done"], e["total"],
                        e.get("nfiles", 0)) for e in p.get("active", [])]
        self.counts = (p.get("bundling", 0), p.get("uploading", 0))
        if p.get("failed_list"):
            self.failed_list = p["failed_list"]
        if p.get("unchanged"):
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
                      "cancelled": "cancelled — orphan tars possible, see README"}[self.state]
        nfail = (self.summary or self.last or {}).get("failed", 0)
        if nfail and self.state == "done":
            self.phase = f"completed — {nfail:,} files failed (Retry failed files)"
        self.eta = None
        self.changed.emit(self)
        self.finished.emit(self)


class RestoreJob(Job):
    def __init__(self, cfg, manifest, selections, dest, overwrite, verify, concurrency,
                 est_files, est_bytes, title):
        super().__init__("restore", title)
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
        t = Task(self.engine.run, self.manifest, self.selections)
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
        bar.addStretch(1)
        for b in (self.btn_open_failed, self.btn_retry, self.btn_clear, self.btn_cancel):
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
        it = QTreeWidgetItem([f"#{job.id} {job.kind}: {job.title}", "", "", "", "", ""])
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

    def _open_failed(self):
        j = self.selected_job()
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

        self.local = LocalPane(self.s)
        self.archive = ArchivePane(self.s)
        mid = QWidget(); mv = QVBoxLayout(mid); mv.addStretch(1)
        self.btn_arch = QPushButton("Archive →"); self.btn_arch.setToolTip("Bundle the selected local files/folders into tars on S3")
        self.btn_rest = QPushButton("← Restore"); self.btn_rest.setToolTip("Restore the selected archived files/folders into the local folder")
        for b in (self.btn_arch, self.btn_rest):
            b.setMinimumWidth(110); mv.addWidget(b)
        mv.addStretch(1)

        top = QSplitter(Qt.Horizontal)
        top.addWidget(self.local); top.addWidget(mid); top.addWidget(self.archive)
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

        self.sb_timer = QTimer(self); self.sb_timer.timeout.connect(self._update_statusbar)
        self.sb_timer.start(500)

        if not self.s.config().complete():
            QTimer.singleShot(300, self.open_wizard)

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
        paths = self.local.selected_paths()
        dlg = ArchiveDialog(paths, self.s, self, manifest=self.archive.manifest,
                            report_dir=self.local.cwd)
        if not dlg.exec():
            return
        cmd, err = dlg.command()
        if err:
            QMessageBox.warning(self, "Archive", err)
            return
        self.start_archive(cmd, dlg.title())

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
        dlg = RestoreDialog(sel, files, nbytes, self.local.restore_target(), self)
        if not dlg.exec():
            return
        dest = dlg.dest.text().strip()
        if not os.path.isdir(dest):
            QMessageBox.warning(self, "Restore", f"{dest} is not a folder.")
            return
        first = selection_label(*sel[0])
        title = first + (f" +{len(sel) - 1}" if len(sel) > 1 else "")
        job = RestoreJob(self.s.config(), self.archive.manifest, sel, dest,
                         dlg.overwrite.isChecked(), dlg.verify.isChecked(),
                         self.s.restore_concurrency(), files, nbytes, f"{title} → {dest}")
        self.monitor.add(job)
        job.finished.connect(self._job_finished)
        job.start()

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
        if run and QMessageBox.question(
                self, "Quit", f"{len(run)} job(s) still running. Cancel them and quit?") != QMessageBox.Yes:
            ev.ignore()
            return
        for j in run:
            j.cancel()
        for j in run:
            if isinstance(j, ArchiveJob):
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
