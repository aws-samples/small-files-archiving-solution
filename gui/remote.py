"""
remote.py -- run an S3-source archive on an EC2 instance through AWS Systems
Manager, so the data never leaves AWS (no data transfer out to this computer).

The GUI stays the controller:
  1. upload_runner()  copies the archiver files this GUI runs to
                      s3://<tar bucket>/_sfa/runner/<content hash>/
  2. start()          one SSM Run Command: fetch those files once, build a
                      private venv (uv, Python 3.12) once, then launch the
                      archiver detached (setsid nohup) and return its PID
  3. read_status()    the archiver writes its progress snapshot (+ log tail)
                      to s3://<tar bucket>/_sfa/jobs/<run id>/status.json
  4. cancel()         SIGINT through SSM (the same no-orphan cancel), or a
                      force-stop

No Qt here. Every value that reaches the remote shell goes through
shlex.quote, so bucket names, prefixes or keys can't inject commands.
"""
import hashlib
import json
import os
import shlex
import time
import uuid
from datetime import datetime

import boto3

ARCHIVER_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "archiver")
RUNNER_FILES = ("archiver_core.py", "s3s3-archiver-s3tables-mp.py")
try:                     # the same dependency list as archiver/requirements.txt
    with open(os.path.join(ARCHIVER_DIR, "requirements.txt")) as _f:
        REQUIREMENTS = _f.read()
except OSError:
    REQUIREMENTS = "pyiceberg[pyarrow,pyiceberg-core]>=0.11\nboto3[crt]>=1.34\n"
REMOTE_ROOT = "/opt/sfa-runner"
REMOTE_WORK = "/var/lib/sfa"


class Remote:
    def __init__(self, cfg):
        self.cfg = cfg
        sess = boto3.Session(profile_name=cfg.profile or None)
        self.ssm = sess.client("ssm", region_name=cfg.region)
        self.ec2 = sess.client("ec2", region_name=cfg.region)
        self.s3 = sess.client("s3", region_name=cfg.region)

    # -- instances --------------------------------------------------------
    def list_instances(self):
        """Online, SSM-managed Linux instances: [{id, name, type, state}]."""
        infos, token = [], None
        while True:
            kw = {"Filters": [{"Key": "PingStatus", "Values": ["Online"]},
                              {"Key": "PlatformTypes", "Values": ["Linux"]}], "MaxResults": 50}
            if token:
                kw["NextToken"] = token
            r = self.ssm.describe_instance_information(**kw)
            infos += [i for i in r["InstanceInformationList"] if i["InstanceId"].startswith("i-")]
            token = r.get("NextToken")
            if not token:
                break
        if not infos:
            return []
        ids = [i["InstanceId"] for i in infos]
        meta = {}
        for i in range(0, len(ids), 100):
            for res in self.ec2.describe_instances(InstanceIds=ids[i:i + 100])["Reservations"]:
                for inst in res["Instances"]:
                    name = next((t["Value"] for t in inst.get("Tags", []) if t["Key"] == "Name"), "")
                    meta[inst["InstanceId"]] = (name, inst["InstanceType"])
        vcpus = {}
        types = sorted({t for _, t in meta.values() if t})
        try:
            for i in range(0, len(types), 100):
                for t in self.ec2.describe_instance_types(InstanceTypes=types[i:i + 100])["InstanceTypes"]:
                    vcpus[t["InstanceType"]] = t["VCpuInfo"]["DefaultVCpus"]
        except Exception:
            pass                       # sizing hint only; the list still works without it
        return sorted(({"id": i, "name": meta.get(i, ("", ""))[0], "type": meta.get(i, ("", "?"))[1],
                        "vcpus": vcpus.get(meta.get(i, ("", ""))[1])}
                       for i in ids), key=lambda x: (x["name"] or "~", x["id"]))

    @staticmethod
    def suggested(vcpus):
        """Worker processes and total GET concurrency for an instance. Measured:
        small-object runs are CPU-bound; vCPU + 16 workers (at most 2 per vCPU)
        with 32 GETs each gave the best throughput on both sizes tested
        (m5.2xlarge 16/512, c6in.8xlarge 48/1536)."""
        v = int(vcpus)
        workers = max(2, min(512, 2 * v, v + 16))
        return workers, min(4096, 32 * workers)

    # -- runner files ------------------------------------------------------
    def runner_version(self):
        h = hashlib.sha256(REQUIREMENTS.encode())
        for f in RUNNER_FILES:
            h.update(open(os.path.join(ARCHIVER_DIR, f), "rb").read())
        return h.hexdigest()[:12]

    def upload_runner(self):
        """Upload the archiver files (once per version). Returns the version."""
        ver = self.runner_version()
        base = f"_sfa/runner/{ver}/"
        try:
            self.s3.head_object(Bucket=self.cfg.bucket, Key=base + "requirements.txt")
            return ver
        except Exception:
            pass
        for f in RUNNER_FILES:
            self.s3.upload_file(os.path.join(ARCHIVER_DIR, f), self.cfg.bucket, base + f)
        self.s3.put_object(Bucket=self.cfg.bucket, Key=base + "requirements.txt", Body=REQUIREMENTS.encode())
        return ver

    # -- SSM ---------------------------------------------------------------
    def _send(self, instance_id, commands, comment, timeout=1800):
        r = self.ssm.send_command(
            InstanceIds=[instance_id], DocumentName="AWS-RunShellScript", Comment=comment[:100],
            TimeoutSeconds=60, Parameters={"commands": commands, "executionTimeout": [str(timeout)]})
        return r["Command"]["CommandId"]

    def wait(self, command_id, instance_id, timeout=1800):
        t0 = time.time()
        while time.time() - t0 < timeout:
            time.sleep(2)
            try:
                inv = self.ssm.get_command_invocation(CommandId=command_id, InstanceId=instance_id)
            except self.ssm.exceptions.InvocationDoesNotExist:
                continue
            if inv["Status"] not in ("Pending", "InProgress", "Delayed"):
                return inv
        raise TimeoutError(f"SSM command {command_id} still running after {timeout}s")

    def _setup_lines(self, ver):
        r = shlex.quote(self.cfg.region)
        d = f"{REMOTE_ROOT}/{ver}"
        return [
            "set -eu",
            f"D={shlex.quote(d)}",
            f"mkdir -p {REMOTE_ROOT} {REMOTE_WORK} && chmod 700 {REMOTE_ROOT} {REMOTE_WORK}",
            # .ready is written only after a complete install, so a half-finished
            # attempt is repaired instead of being mistaken for a working runner
            'if [ ! -f "$D/.ready" ]; then',
            '  rm -rf "$D/.venv"',
            f'  mkdir -p "$D" && aws s3 cp --recursive --only-show-errors '
            f's3://{self.cfg.bucket}/_sfa/runner/{ver}/ "$D/" --region {r}',
            '  UV=$(command -v uv || true); [ -n "$UV" ] || UV=/root/.local/bin/uv',
            '  if [ ! -x "$UV" ]; then curl -LsSf https://astral.sh/uv/install.sh | sh >/dev/null; UV=/root/.local/bin/uv; fi',
            '  "$UV" venv --quiet --python 3.12 "$D/.venv"',
            # wheels only: never compile on the instance. On older distros (e.g.
            # Amazon Linux 2, glibc 2.26) this picks the newest compatible wheels
            '  "$UV" pip install --quiet --only-binary :all: --python "$D/.venv/bin/python" -r "$D/requirements.txt"',
            '  touch "$D/.ready"',
            "fi",
        ]

    def prepare(self, instance_id):
        """Install the runner on the instance (no-op if this version is there).
        Returns a short report string."""
        ver = self.upload_runner()
        probe = (f"import sys; sys.path.insert(0, '{REMOTE_ROOT}/{ver}'); import archiver_core as c; "
                 f"ca = c.ensure_ca_bundle(); import pyarrow.fs as f; "
                 f"f.S3FileSystem(region='{self.cfg.region}').open_input_stream("
                 f"'{self.cfg.bucket}/_sfa/runner/{ver}/requirements.txt').read(); "
                 f"print('runner ready, python', sys.version.split()[0], "
                 f"'| pyarrow S3 TLS ok' + (' (CA ' + ca + ')' if ca else ''))")
        lines = self._setup_lines(ver) + [f'"$D/.venv/bin/python" -c {shlex.quote(probe)}']
        inv = self.wait(self._send(instance_id, lines, "sfa: prepare runner"), instance_id)
        if inv["Status"] != "Success":
            raise RuntimeError((inv["StandardErrorContent"] or inv["StandardOutputContent"])[-800:])
        return inv["StandardOutputContent"].strip()

    def new_run_id(self):
        return f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_r{uuid.uuid4().hex[:6]}"

    def status_uri(self, run_id):
        return f"s3://{self.cfg.bucket}/_sfa/jobs/{run_id}/status.json"

    def start(self, instance_id, archiver_args, run_id):
        """Launch the S3 archiver detached on the instance. archiver_args are the
        arguments after the script name (as the local command would get them).
        Returns {run_id, pid, status_uri}."""
        ver = self.upload_runner()
        args = list(archiver_args) + ["--run-id", run_id, "--status-s3", self.status_uri(run_id)]
        quoted = " ".join(shlex.quote(a) for a in args)
        log = f"{REMOTE_WORK}/{run_id}.log"
        lines = self._setup_lines(ver) + [
            f"cd {REMOTE_WORK}",
            # setsid + nohup + no inherited stdin/stdout: the archiver outlives
            # this SSM command, the agent and any SSM session
            f'setsid nohup "$D/.venv/bin/python" -u "$D/s3s3-archiver-s3tables-mp.py" {quoted} '
            f'> {shlex.quote(log)} 2>&1 < /dev/null &',
            'echo "PID $!"',
        ]
        inv = self.wait(self._send(instance_id, lines, f"sfa: start {run_id}"), instance_id)
        out = inv["StandardOutputContent"]
        if inv["Status"] != "Success" or "PID " not in out:
            raise RuntimeError((inv["StandardErrorContent"] or out)[-800:])
        pid = int(out.rsplit("PID ", 1)[1].split()[0])
        return {"run_id": run_id, "pid": pid, "status_uri": self.status_uri(run_id), "log": log}

    def read_status(self, status_uri):
        b, _, k = status_uri[5:].partition("/")
        try:
            return json.loads(self.s3.get_object(Bucket=b, Key=k)["Body"].read())
        except self.s3.exceptions.NoSuchKey:
            return None

    def alive(self, instance_id, pid):
        inv = self.wait(self._send(instance_id, [f"kill -0 {int(pid)} 2>/dev/null && echo ALIVE || echo GONE"],
                                   "sfa: check"), instance_id, timeout=120)
        return "ALIVE" in inv["StandardOutputContent"]

    def cancel(self, instance_id, pid, force=False):
        """SIGINT = clean cancel (in-progress tars finish and are recorded);
        force = a second SIGINT, then SIGTERM after a grace period."""
        pid = int(pid)
        lines = [f"kill -INT {pid} 2>/dev/null || true"]
        if force:
            lines += ["sleep 1", f"kill -INT {pid} 2>/dev/null || true", "sleep 10",
                      f"kill -TERM {pid} 2>/dev/null || true"]
        return self._send(instance_id, lines, "sfa: cancel" + (" (force)" if force else ""))

    def fetch_text(self, uri, limit=5_000_000):
        b, _, k = uri[5:].partition("/")
        return self.s3.get_object(Bucket=b, Key=k, Range=f"bytes=0-{limit}")["Body"].read().decode(
            "utf-8", "replace")
