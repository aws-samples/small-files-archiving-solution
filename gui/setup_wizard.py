"""
Setup wizard: pick an AWS profile and Region, then either
  * create a new archive -- choose or create the S3 Tables table bucket, the
    manifest tables and the tar bucket, or
  * connect an existing archive -- pick a table bucket, pick one of the archive
    manifests found in it, and confirm its tar bucket (read from the table, or
    picked and verified with a sample tar).
Either way the archive is added to the named archive list. Users never type or
see an ARN or account id -- buckets are picked by name, ARNs derived internally.
"""
import uuid

from PySide6.QtCore import Qt, QEventLoop, QThreadPool
from PySide6.QtWidgets import (
    QWizard, QWizardPage, QVBoxLayout, QHBoxLayout, QFormLayout, QLabel, QComboBox,
    QLineEdit, QPushButton, QRadioButton, QButtonGroup, QCheckBox, QPlainTextEdit,
    QMessageBox)

import backend
from backend import Provisioner, mask
from archives import ArchiveRegistry, default_name

P_CRED, P_MODE, P_TB, P_BK, P_TABLES, P_CONNECT, P_SUM = range(7)

REGIONS = ["ap-northeast-2", "ap-northeast-1", "ap-northeast-3", "ap-southeast-1",
           "ap-southeast-2", "ap-south-1", "us-east-1", "us-east-2", "us-west-2",
           "ca-central-1", "eu-west-1", "eu-west-2", "eu-central-1", "eu-north-1",
           "sa-east-1"]


def _call(fn, *a, **kw):
    """Run a blocking AWS call off the UI thread and wait for it without
    freezing the window. Returns (result, error_message)."""
    from app import Task          # same QRunnable helper the main window uses
    loop, box = QEventLoop(), {}
    t = Task(fn, *a, **kw)
    t.signals.done.connect(lambda r: (box.update(r=r), loop.quit()))
    t.signals.failed.connect(lambda m: (box.update(e=m.split("\n\n")[0]), loop.quit()))
    QThreadPool.globalInstance().start(t)
    loop.exec()
    return box.get("r"), box.get("e")


def _hint(text):
    lbl = QLabel(text)
    lbl.setWordWrap(True)
    lbl.setStyleSheet("color: gray")
    return lbl


class CredentialsPage(QWizardPage):
    def __init__(self, s):
        super().__init__()
        self.s = s
        self.setTitle("AWS credentials")
        self.setSubTitle("Choose the AWS CLI profile and the Region to work in.")
        f = QFormLayout(self)
        self.profile = QComboBox(); self.profile.setEditable(True)
        self.profile.addItems(["(default)"] + Provisioner.profiles())
        cur = s.get("profile")
        if cur:
            self.profile.setCurrentText(cur)
        self.region = QComboBox(); self.region.setEditable(True)
        self.region.addItems(REGIONS)
        self.region.setCurrentText(s.get("region") or
                                   Provisioner.profile_region(self.prof()) or "ap-northeast-2")
        self.profile.currentTextChanged.connect(self._profile_changed)
        self.btn = QPushButton("Check credentials")
        self.status = QLabel("")
        self.status.setWordWrap(True)
        self.btn.clicked.connect(self.check)
        f.addRow("Profile", self.profile)
        f.addRow("Region", self.region)
        f.addRow("", self.btn)
        f.addRow("", self.status)
        f.addRow("", _hint("Profiles come from ~/.aws/config. If you have none, run "
                           "'aws configure' (or 'aws configure sso') in a terminal first."))
        self.ok = False

    def prof(self):
        t = self.profile.currentText().strip()
        return "" if t in ("", "(default)") else t

    def _profile_changed(self):
        self.ok = False
        self.status.setText("")
        r = Provisioner.profile_region(self.prof())
        if r:
            self.region.setCurrentText(r)
        self.completeChanged.emit()

    def check(self):
        self.status.setText("Checking…")
        self.btn.setEnabled(False)
        _, err = _call(lambda: Provisioner(self.prof(), self.region.currentText().strip()).check())
        self.btn.setEnabled(True)
        self.ok = err is None
        self.status.setText("✓ Credentials work." if self.ok else f"✗ {mask(err)}")
        self.completeChanged.emit()

    def isComplete(self):
        return self.ok

    def initializePage(self):
        if not self.ok:
            self.check()

    def nextId(self):
        return P_MODE


class ModePage(QWizardPage):
    def __init__(self, s):
        super().__init__()
        self.setTitle("New or existing archive")
        self.setSubTitle("Create a new archive, or connect to one that already exists "
                         "(for example, one a colleague or an earlier setup created).")
        v = QVBoxLayout(self)
        self.create = QRadioButton("Create a new archive")
        self.connect = QRadioButton("Connect an existing archive")
        g = QButtonGroup(self); g.addButton(self.create); g.addButton(self.connect)
        v.addWidget(self.create)
        v.addWidget(_hint("Pick or create the table bucket, the tar bucket and the manifest tables."))
        v.addSpacing(10)
        v.addWidget(self.connect)
        v.addWidget(_hint("Pick a table bucket; the wizard lists the archive manifests in it and "
                          "finds or checks the tar bucket. Nothing is created or changed in your "
                          "archive, except recording its tar bucket in the table if it isn't yet."))
        v.addStretch(1)
        (self.connect if ArchiveRegistry(s).names() else self.create).setChecked(True)

    def nextId(self):
        return P_TB if self.create.isChecked() else P_CONNECT


class PickOrCreatePage(QWizardPage):
    """Shared layout: use an existing resource (listed by name) or create one."""
    def __init__(self, title, subtitle, what, validator, default_name, saved_name, hint):
        super().__init__()
        self.setTitle(title)
        self.setSubTitle(subtitle)
        self.what, self.validator, self.saved = what, validator, saved_name
        v = QVBoxLayout(self)
        self.use = QRadioButton(f"Use an existing {what}")
        self.new = QRadioButton(f"Create a new {what}")
        g = QButtonGroup(self); g.addButton(self.use); g.addButton(self.new)
        self.existing = QComboBox()
        self.name = QLineEdit(default_name)
        self.err = QLabel(""); self.err.setStyleSheet("color: #c0392b")
        v.addWidget(self.use); v.addWidget(self.existing)
        v.addSpacing(8)
        v.addWidget(self.new); v.addWidget(self.name); v.addWidget(self.err)
        v.addWidget(_hint(hint))
        v.addStretch(1)
        for w in (self.use, self.new):
            w.toggled.connect(self._sync)
        self.name.textChanged.connect(self._sync)
        self.existing.currentTextChanged.connect(lambda *_: self.completeChanged.emit())

    def lister(self):
        raise NotImplementedError

    def initializePage(self):
        self.existing.clear()
        self.existing.addItem("Loading…")
        names, err = _call(self.lister)
        self.existing.clear()
        if err:
            self.existing.addItem("")
            self.err.setText(f"Could not list: {mask(err)}")
            names = []
        self.existing.addItems(names or [])
        if self.saved and self.saved in (names or []):
            self.existing.setCurrentText(self.saved)
        (self.use if names else self.new).setChecked(True)
        self.use.setEnabled(bool(names))
        self._sync()

    def _sync(self):
        self.existing.setEnabled(self.use.isChecked())
        self.name.setEnabled(self.new.isChecked())
        if self.new.isChecked():
            n = self.name.text().strip()
            exists = n in [self.existing.itemText(i) for i in range(self.existing.count())]
            self.err.setText("" if self.validator(n) and not exists else
                             f"'{n}' already exists — choose it above." if exists else
                             f"Not a valid {self.what} name.")
        else:
            self.err.setText("")
        self.completeChanged.emit()

    def choice(self):
        """(name, create?)"""
        if self.use.isChecked():
            return self.existing.currentText().strip(), False
        return self.name.text().strip(), True

    def isComplete(self):
        n, create = self.choice()
        return bool(n) and (not create or (self.validator(n) and not self.err.text()))


class TableBucketPage(PickOrCreatePage):
    def __init__(self, wiz, s):
        super().__init__(
            "S3 Tables table bucket",
            "Holds the manifest that maps every archived file to its tar and byte range.",
            "table bucket", backend.valid_table_bucket,
            f"small-files-archive-{uuid.uuid4().hex[:6]}", s.config().table_bucket,
            "Names: 3–63 lowercase letters, numbers and hyphens; unique within your "
            "account and Region.")
        self.wiz = wiz

    def lister(self):
        return self.wiz.prov().list_table_buckets()


class TarBucketPage(PickOrCreatePage):
    def __init__(self, wiz, s):
        super().__init__(
            "S3 bucket for the tar files",
            "The general purpose bucket where the tar objects are stored.",
            "bucket", backend.valid_s3_bucket,
            f"small-files-archive-tars-{uuid.uuid4().hex[:8]}", s.get("bucket"),
            "Bucket names are global across all AWS accounts, so the suggestion has a "
            "random suffix. A new bucket is created private: all Block Public Access "
            "settings on, ACLs disabled, SSE-S3 encryption, HTTPS-only access, and a rule that "
            "cleans up interrupted multipart uploads after 7 days. Only buckets in the chosen "
            "Region are listed.")
        self.wiz = wiz

    def lister(self):
        return self.wiz.prov().list_buckets()


class TablesPage(QWizardPage):
    def __init__(self, s):
        super().__init__()
        self.setTitle("Manifest tables")
        self.setSubTitle("Namespace and table names inside the table bucket.")
        f = QFormLayout(self)
        self.ns = QLineEdit(s.config().namespace)
        self.tbl = QLineEdit(s.config().table_name)
        self.create = QCheckBox("Create the tables now (otherwise the first archive run creates them)")
        self.create.setChecked(True)
        self.err = QLabel(""); self.err.setStyleSheet("color: #c0392b")
        self.names = QLabel("")
        f.addRow("Namespace", self.ns)
        f.addRow("Manifest table", self.tbl)
        f.addRow("", self.names)
        f.addRow("", self.create)
        f.addRow("", self.err)
        f.addRow("", _hint("Lowercase letters, numbers and underscores. Existing tables "
                           "with these names are reused, never overwritten."))
        for w in (self.ns, self.tbl):
            w.textChanged.connect(self._sync)
        self._sync()

    def _sync(self):
        ns, t = self.ns.text().strip(), self.tbl.text().strip()
        ok = backend.valid_iceberg_name(ns) and backend.valid_iceberg_name(t)
        self.err.setText("" if ok else "Use lowercase letters, numbers and underscores.")
        self.names.setText(f"Creates: {ns}.{t}, {ns}.{t}_dirs (folder index), "
                           f"{ns}.{t}_runs (run history)")
        self.completeChanged.emit()

    def isComplete(self):
        return not self.err.text()

    def nextId(self):
        return P_SUM


class ConnectPage(QWizardPage):
    """Connect an existing archive: table bucket -> manifest -> tar bucket."""

    def __init__(self, wiz):
        super().__init__()
        self.wiz = wiz
        self.setTitle("Connect an existing archive")
        self.setSubTitle("Pick the table bucket and the archive manifest. The tar bucket is "
                         "filled in when the archive recorded it; otherwise pick it and it is checked.")
        f = QFormLayout(self)
        self.tb = QComboBox()
        self.manifest = QComboBox()
        self.bucket = QComboBox(); self.bucket.setEditable(True)
        self.status = QLabel(""); self.status.setWordWrap(True)
        self.btn_check = QPushButton("Check tar bucket")
        f.addRow("Table bucket", self.tb)
        f.addRow("Archive manifest", self.manifest)
        f.addRow("Tar bucket", self.bucket)
        f.addRow("", self.btn_check)
        f.addRow("", self.status)
        f.addRow("", _hint("Only tables with the archive manifest columns are listed. The check "
                           "looks up one tar from the manifest in the bucket (read-only)."))
        self._manifests = []
        self.verified = None          # (table bucket, ns, table, bucket) that passed the check
        self.tb.currentTextChanged.connect(self._load_manifests)
        self.manifest.currentIndexChanged.connect(self._manifest_changed)
        self.bucket.currentTextChanged.connect(self._bucket_changed)
        self.btn_check.clicked.connect(self.check)

    def initializePage(self):
        prov = self.wiz.prov()
        self.tb.blockSignals(True); self.tb.clear(); self.tb.addItem("Loading…")
        names, err = _call(prov.list_table_buckets)
        self.tb.clear()
        if err:
            self.status.setText(f"✗ Could not list table buckets: {mask(err)}")
            names = []
        self.tb.addItems(names or [])
        saved = self.wiz.s.config().table_bucket
        if saved in (names or []):
            self.tb.setCurrentText(saved)
        self.tb.blockSignals(False)
        buckets, _ = _call(prov.list_buckets)
        self.bucket.blockSignals(True); self.bucket.clear(); self.bucket.addItems(buckets or [])
        self.bucket.blockSignals(False)
        self._load_manifests()

    def _arn(self):
        return self.wiz.prov().table_bucket_arn(self.tb.currentText().strip())

    def _load_manifests(self, *_):
        self.manifest.blockSignals(True); self.manifest.clear()
        self.verified = None
        name = self.tb.currentText().strip()
        if not name or name == "Loading…":
            self.manifest.blockSignals(False); self.completeChanged.emit()
            return
        self.status.setText("Looking for archive manifests…")
        prov = self.wiz.prov()
        found, err = _call(lambda: prov.list_manifests(prov.table_bucket_arn(name)))
        self._manifests = found or []
        for m in self._manifests:
            extra = "" if m["dirs"] and m["runs"] else "  (no folder index / run history yet)"
            self.manifest.addItem(f"{m['namespace']}.{m['table']}{extra}")
        self.manifest.blockSignals(False)
        if err:
            self.status.setText(f"✗ Could not read the table bucket: {mask(err)}")
        elif not self._manifests:
            self.status.setText("No archive manifest in this table bucket. Pick another one, or go "
                                "Back and choose “Create a new archive”.")
        self._manifest_changed()

    def selected(self):
        i = self.manifest.currentIndex()
        return self._manifests[i] if 0 <= i < len(self._manifests) else None

    def _manifest_changed(self, *_):
        m = self.selected()
        self.verified = None
        if m and m.get("tar_bucket"):
            self.bucket.setCurrentText(m["tar_bucket"])
            self.status.setText(f"The archive records its tar bucket: {m['tar_bucket']}. "
                                f"Checking it…")
            self.check()
        elif m:
            self.status.setText("This archive doesn't record its tar bucket (older archiver). "
                                "Pick the bucket and click “Check tar bucket”.")
        self.completeChanged.emit()

    def _bucket_changed(self, *_):
        self.verified = None
        self.completeChanged.emit()

    def check(self):
        m = self.selected()
        b = self.bucket.currentText().strip()
        if not (m and b):
            return
        self.btn_check.setEnabled(False)
        prov = self.wiz.prov()
        tbn = self.tb.currentText().strip()
        msg, err = _call(lambda: prov.verify_tar_bucket(prov.table_bucket_arn(tbn), m["namespace"],
                                                         m["table"], b))
        self.btn_check.setEnabled(True)
        if err:
            self.status.setText(f"✗ {mask(err)}")
            self.verified = None
        else:
            self.status.setText(f"✓ s3://{b}: {msg}")
            self.verified = (tbn, m["namespace"], m["table"], b)
        self.completeChanged.emit()

    def isComplete(self):
        m = self.selected()
        return bool(m and self.verified == (self.tb.currentText().strip(), m["namespace"],
                                            m["table"], self.bucket.currentText().strip()))

    def nextId(self):
        return P_SUM


class SummaryPage(QWizardPage):
    def __init__(self, wiz):
        super().__init__()
        self.wiz = wiz
        self.setTitle("Review and apply")
        v = QVBoxLayout(self)
        self.plan = QLabel(""); self.plan.setWordWrap(True); self.plan.setTextFormat(Qt.RichText)
        nr = QHBoxLayout()
        self.name = QLineEdit()
        self.name.setToolTip("How this archive is shown in the archive list")
        nr.addWidget(QLabel("Name in the archive list:")); nr.addWidget(self.name, 1)
        self.log = QPlainTextEdit(); self.log.setReadOnly(True)
        v.addWidget(self.plan); v.addLayout(nr); v.addWidget(self.log, 1)
        self.done = False

    def _connect_mode(self):
        return self.wiz.p_mode.connect.isChecked()

    def _target(self):
        """(table bucket, tar bucket, namespace, table, create table bucket?, create bucket?)"""
        w = self.wiz
        if self._connect_mode():
            m = w.p_connect.selected()
            return (w.p_connect.tb.currentText().strip(), w.p_connect.bucket.currentText().strip(),
                    m["namespace"], m["table"], False, False)
        tb, tb_new = w.p_tb.choice()
        bk, bk_new = w.p_bk.choice()
        return tb, bk, w.p_tables.ns.text().strip(), w.p_tables.tbl.text().strip(), tb_new, bk_new

    def initializePage(self):
        w = self.wiz
        tb, bk, ns, t, tb_new, bk_new = self._target()
        if self._connect_mode():
            self.setSubTitle("Nothing is changed yet. Click Finish to add the archive to your list.")
            rows = [f"<b>Profile / Region:</b> {w.p_cred.prof() or '(default)'} / {w.region()}",
                    f"<b>Table bucket:</b> {tb} — existing",
                    f"<b>Archive manifest:</b> {ns}.{t} — existing",
                    f"<b>Tar bucket:</b> {bk} — existing, checked"]
        else:
            self.setSubTitle("Nothing has been created yet. Click Finish to apply.")
            rows = [
                f"<b>Profile / Region:</b> {w.p_cred.prof() or '(default)'} / {w.region()}",
                f"<b>Table bucket:</b> {tb} — {'<b>create</b>' if tb_new else 'existing'}",
                f"<b>Tar bucket:</b> {bk} — {'<b>create</b> (private, SSE-S3, HTTPS only)' if bk_new else 'existing'}",
                f"<b>Tables:</b> {ns}.{t}, {t}_dirs, {t}_runs — "
                f"{'create if missing' if w.p_tables.create.isChecked() else 'created by the first archive run'}",
            ]
        self.plan.setText("<br>".join(rows))
        if not self.name.text().strip() or not self.done:
            self.name.setText(default_name({"table_bucket": tb, "namespace": ns, "table_name": t,
                                            "bucket": bk}))
        self.log.clear()
        self.done = False

    def validatePage(self):
        if self.done:
            return True       # second click on "Close"
        w = self.wiz
        prov = w.prov()
        tb, bk, ns, t, tb_new, bk_new = self._target()
        connect = self._connect_mode()

        def step(label, fn, *a):
            self.log.appendPlainText(f"… {label}")
            res, err = _call(fn, *a)
            if err:
                self.log.appendPlainText(f"✗ {label}: {mask(err)}")
                QMessageBox.warning(self, "Setup", f"{label} failed:\n\n{mask(err)}")
                return False, None
            self.log.appendPlainText(f"✓ {label}")
            return True, res

        if tb_new:
            ok, _ = step(f"Create table bucket {tb}", prov.create_table_bucket, tb)
            if not ok:
                return False
        ok, arn = step("Resolve table bucket", prov.table_bucket_arn, tb)
        if not ok:
            return False
        if bk_new:
            ok, _ = step(f"Create bucket {bk} (private, SSE-S3, HTTPS only)", prov.create_bucket, bk)
            if not ok:
                return False
        if connect:
            ok, msg = step(f"Check tar bucket {bk}", prov.verify_tar_bucket, arn, ns, t, bk)
            if not ok:
                return False
            self.log.appendPlainText(f"   {msg}")
            recorded, _ = _call(prov.record_tar_bucket, arn, ns, t, bk)
            if recorded:
                self.log.appendPlainText(f"✓ Recorded the tar bucket in {ns}.{t}")
        elif w.p_tables.create.isChecked():
            ok, res = step(f"Create tables in {ns}", prov.create_tables, arn, ns, t, bk)
            if not ok:
                return False
            for name, state in res:
                self.log.appendPlainText(f"   {ns}.{name}: {state}")
        s = w.s
        s.set("profile", w.p_cred.prof())
        s.set("region", w.region())
        s.set("table_bucket", tb)
        s.set("table_bucket_arn", arn)      # stored for the archiver, never displayed
        s.set("bucket", bk)
        s.set("namespace", ns)
        s.set("table_name", t)
        s.set("setup_done", 1)
        listed = ArchiveRegistry(s).add(
            {"profile": w.p_cred.prof(), "region": w.region(), "bucket": bk, "table_bucket": tb,
             "table_bucket_arn": arn, "namespace": ns, "table_name": t}, name=self.name.text())
        self.done = True
        self.log.appendPlainText(f"Saved as “{listed}” in the archive list. Add more archives any "
                                 f"time with + next to the archive list.")
        # stay on the page so the result is visible; Finish becomes Close
        self.setSubTitle("Done — everything above is ready and the settings are saved.")
        self.wiz.setButtonText(QWizard.FinishButton, "Close")
        self.wiz.button(QWizard.BackButton).setEnabled(False)
        self.wiz.button(QWizard.CancelButton).setEnabled(False)
        return False


class SetupWizard(QWizard):
    def __init__(self, settings, parent=None):
        super().__init__(parent)
        self.s = settings
        self.setWindowTitle("Small Files Archiver — setup")
        self.setWizardStyle(QWizard.ModernStyle)
        self.setMinimumSize(720, 520)
        self.p_cred = CredentialsPage(settings)
        self.p_mode = ModePage(settings)
        self.p_tb = TableBucketPage(self, settings)
        self.p_bk = TarBucketPage(self, settings)
        self.p_tables = TablesPage(settings)
        self.p_connect = ConnectPage(self)
        self.p_sum = SummaryPage(self)
        for pid, p in ((P_CRED, self.p_cred), (P_MODE, self.p_mode), (P_TB, self.p_tb),
                       (P_BK, self.p_bk), (P_TABLES, self.p_tables), (P_CONNECT, self.p_connect),
                       (P_SUM, self.p_sum)):
            self.setPage(pid, p)
        self.setStartId(P_CRED)

    def region(self):
        return self.p_cred.region.currentText().strip()

    def prov(self):
        return Provisioner(self.p_cred.prof(), self.region())
