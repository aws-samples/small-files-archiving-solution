"""
First-run setup wizard: pick an AWS profile and region, then choose or create
the S3 Tables table bucket, the manifest tables and the tar bucket. Users never
type or see an ARN or account id -- buckets are picked by name and ARNs are
derived internally.
"""
import uuid

from PySide6.QtCore import Qt, QEventLoop, QThreadPool
from PySide6.QtWidgets import (
    QWizard, QWizardPage, QVBoxLayout, QHBoxLayout, QFormLayout, QLabel, QComboBox,
    QLineEdit, QPushButton, QRadioButton, QButtonGroup, QCheckBox, QPlainTextEdit,
    QMessageBox)

import backend
from backend import Provisioner, mask

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
        self.setTitle("1. AWS credentials")
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
            "2. S3 Tables table bucket",
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
            "3. S3 bucket for the tar files",
            "The general purpose bucket where the tar objects are stored.",
            "bucket", backend.valid_s3_bucket,
            f"small-files-archive-tars-{uuid.uuid4().hex[:8]}", s.get("bucket"),
            "Bucket names are global across all AWS accounts, so the suggestion has a "
            "random suffix. A new bucket is created private: all Block Public Access "
            "settings on, ACLs disabled, SSE-S3 encryption. Only buckets in the chosen "
            "Region are listed.")
        self.wiz = wiz

    def lister(self):
        return self.wiz.prov().list_buckets()


class TablesPage(QWizardPage):
    def __init__(self, s):
        super().__init__()
        self.setTitle("4. Manifest tables")
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


class SummaryPage(QWizardPage):
    def __init__(self, wiz):
        super().__init__()
        self.wiz = wiz
        self.setTitle("5. Review and apply")
        self.setSubTitle("Nothing has been created yet. Click Finish to apply.")
        v = QVBoxLayout(self)
        self.plan = QLabel(""); self.plan.setWordWrap(True); self.plan.setTextFormat(Qt.RichText)
        self.log = QPlainTextEdit(); self.log.setReadOnly(True)
        v.addWidget(self.plan); v.addWidget(self.log, 1)
        self.done = False

    def initializePage(self):
        w = self.wiz
        tb, tb_new = w.p_tb.choice()
        bk, bk_new = w.p_bk.choice()
        ns, t = w.p_tables.ns.text().strip(), w.p_tables.tbl.text().strip()
        rows = [
            f"<b>Profile / Region:</b> {w.p_cred.prof() or '(default)'} / {w.region()}",
            f"<b>Table bucket:</b> {tb} — {'<b>create</b>' if tb_new else 'existing'}",
            f"<b>Tar bucket:</b> {bk} — {'<b>create</b> (private, SSE-S3)' if bk_new else 'existing'}",
            f"<b>Tables:</b> {ns}.{t}, {t}_dirs, {t}_runs — "
            f"{'create if missing' if w.p_tables.create.isChecked() else 'created by the first archive run'}",
        ]
        self.plan.setText("<br>".join(rows))
        self.log.clear()
        self.done = False

    def validatePage(self):
        if self.done:
            return True       # second click on "Close"
        w = self.wiz
        prov = w.prov()
        tb, tb_new = w.p_tb.choice()
        bk, bk_new = w.p_bk.choice()
        ns, t = w.p_tables.ns.text().strip(), w.p_tables.tbl.text().strip()

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
            ok, _ = step(f"Create bucket {bk} (private, SSE-S3)", prov.create_bucket, bk)
            if not ok:
                return False
        if w.p_tables.create.isChecked():
            ok, res = step(f"Create tables in {ns}", prov.create_tables, arn, ns, t)
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
        self.done = True
        self.log.appendPlainText("Saved. You can rerun this wizard any time from the toolbar.")
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
        self.p_tb = TableBucketPage(self, settings)
        self.p_bk = TarBucketPage(self, settings)
        self.p_tables = TablesPage(settings)
        self.p_sum = SummaryPage(self)
        for p in (self.p_cred, self.p_tb, self.p_bk, self.p_tables, self.p_sum):
            self.addPage(p)

    def region(self):
        return self.p_cred.region.currentText().strip()

    def prov(self):
        return Provisioner(self.p_cred.prof(), self.region())
