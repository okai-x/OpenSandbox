"""Unit tests for the orchestrator; no cluster, SDK or network needed.

python3 -m unittest discover -s deploy/gke/orchestrator -p '*_test.py'
"""

import base64
import hashlib
import io
import itertools
import json
import os
import re
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
import unittest
from unittest import mock

import attempt
import gcs
import source
from orchestrator import Orchestrator, Settings
from records import (
    MAX_RECORD,
    Task,
    expand_tasks,
    summary,
    validate_outputs,
    validate_result,
)

SECRET = "sk-very-secret-value"
V2 = {
    "version": 2,
    "outcome": "accepted",
    "class": "quality",
    "detail": "ok",
    "metrics": {"rounds": 3},
    "usage": {"costUsd": 0.33, "tokens": 1089377, "complete": False},
    "record": {"item_name": "Chair", "triangles": 2204},
    "files": [
        {"path": "asset/mesh/chair.glb", "role": "mesh"},
        {"path": "views/0.png", "role": "view"},
    ],
    "checks": [
        {"id": "geometry.closed", "pass": True},
        {"id": "views.count", "pass": True, "value": 14, "detail": "14 views"},
    ],
}
# What V2's task leaves in /tmp/op/output, besides result.json.
OUTPUTS = {"asset/mesh/chair.glb": "glb-bytes", "views/0.png": "png"}


def github_tarball(files: dict[str, str]) -> bytes:
    """A tarball shaped like GitHub's: one <owner>-<repo>-<sha>/ top directory."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        dirs = {"top"}
        for name in files:
            parts = name.split("/")[:-1]
            dirs |= {"top/" + "/".join(parts[: i + 1]) for i in range(len(parts))}
        for d in sorted(dirs):
            info = tarfile.TarInfo(d)
            info.type = tarfile.DIRTYPE
            tar.addfile(info)
        for name, text in files.items():
            data = text.encode()
            info = tarfile.TarInfo("top/" + name)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    return buf.getvalue()


TARBALL = github_tarball(
    {"README.md": "root", "tasks/run_task.sh": "echo hi", "tasks/lib/a.py": "x = 1"}
)


class FakeGCS:
    bucket = "artifacts"

    def __init__(self):
        self.objects: dict[str, bytes] = {}
        self.generations: dict[str, int] = {}
        self.counter = itertools.count(1793000000000000)
        self.listed: list[str] = []
        self.list_error = ""
        self.lock = threading.Lock()

    def put(self, name, data, content_type="application/octet-stream"):
        with self.lock:
            self.objects[name] = data
            self.generations[name] = next(self.counter)

    def put_json(self, name, doc):
        self.put(name, json.dumps(doc).encode())

    def list(self, prefix):
        """Like the JSON API: size and generation as strings, md5Hash in base64."""
        self.listed.append(prefix)
        if self.list_error:
            raise RuntimeError(self.list_error)
        with self.lock:
            return [
                {"name": n, **self.metadata(n)}
                for n in self.objects
                if n.startswith(prefix)
            ]

    def metadata(self, name):
        data = self.objects[name]
        return {
            "size": str(len(data)),
            "generation": str(self.generations[name]),
            "md5Hash": base64.b64encode(hashlib.md5(data).digest()).decode(),
        }

    def json(self, name):
        return json.loads(self.objects[name])


class FakeSandbox:
    def __init__(self, n):
        self.id = f"sb-{n}"
        self.behavior: dict = {}
        self.deleted = False
        self.polls = 0
        self.interrupted = False
        self.envs: dict = {}


class FakeSandboxes:
    """Fake of sandboxes.Sandboxes. Each item's "behavior" field scripts its
    sandbox: polls (status calls while running), exit, result (dict, None for
    missing, or raw text), hang, lost, setup_exit, collect_exit, collect_raises,
    outputs ({path: text} uploaded under output/ and reported as declared),
    not_uploaded (paths of outputs left out of the bucket) and report (entries
    that replace the sandbox's report of declared files)."""

    def __init__(self, fail_creates=0):
        self.fail_creates = fail_creates
        self.lock = threading.Lock()
        self.created: list[FakeSandbox] = []
        self.live = 0
        self.max_live = 0
        self.delete_all_calls = 0

    def create(self, image, cpu, memory_gib, lifetime_s):
        with self.lock:
            if self.fail_creates:
                self.fail_creates -= 1
                raise RuntimeError("no capacity")
            sb = FakeSandbox(len(self.created))
            self.created.append(sb)
            self.live += 1
            self.max_live = max(self.max_live, self.live)
            return sb

    def write(self, sb, path, data):
        if path == "/tmp/op/input.json":
            sb.behavior = json.loads(data).get("behavior", {})

    def exec(self, sb, command):
        if sb.deleted:
            raise RuntimeError("sandbox gone")
        b = sb.behavior
        if not command.startswith("python3 /tmp/op/collect.py"):
            return b.get("setup_exit", 0), ""
        if b.get("collect_raises"):
            raise RuntimeError("sandbox gone")
        result = b.get(
            "result", {"version": 1, "outcome": "accepted", "class": "quality"}
        )
        text = (
            result if isinstance(result, str) or result is None else json.dumps(result)
        )
        out = {
            "size": -1 if text is None else len(text),
            "text": text,
            "declared": self.upload_outputs(command.split()[-1], b),
        }
        return b.get("collect_exit", 0), "uploaded\n" + json.dumps(out) + "\n"

    def upload_outputs(self, dest, b):
        prefix = dest.removeprefix(f"gs://{self.gcs.bucket}/")
        declared = {}
        for path, text in b.get("outputs", {}).items():
            data = text.encode()
            declared[path] = {
                "size": len(data),
                "sha256": hashlib.sha256(data).hexdigest(),
            }
            if path not in b.get("not_uploaded", []):
                self.gcs.put(f"{prefix}/output/{path}", data)
        return {**declared, **b.get("report", {})}

    def start(self, sb, command, envs):
        sb.envs = envs
        return "exec-1"

    def status(self, sb, execution_id):
        b = sb.behavior
        if sb.deleted or b.get("lost"):
            raise RuntimeError("sandbox gone")
        if sb.interrupted:
            return False, 137
        sb.polls += 1
        if sb.polls > 500:  # a "hang" the orchestrator failed to time out
            return False, 99
        if b.get("hang") or sb.polls < b.get("polls", 1):
            return True, None
        return False, b.get("exit", 0)

    def interrupt(self, sb, execution_id):
        sb.interrupted = True

    def delete(self, sb, wait_s):
        with self.lock:
            if not sb.deleted:
                sb.deleted = True
                self.live -= 1

    def delete_all(self):
        self.delete_all_calls += 1
        with self.lock:
            alive = [sb for sb in self.created if not sb.deleted]
        for sb in alive:
            self.delete(sb, 0)
        return len(alive)


def make_spec(**over):
    spec = {
        "version": 1,
        "job": "abcdefgh12345678",
        "region": "asia-southeast1",
        "source": {
            "repo": "PranaLabs/PranaSkills",
            "commit": "c" * 40,
            "path": "tasks",
        },
        "image": {"ref": "registry/runtime@sha256:" + "0" * 64},
        "sandbox": {"cpu": 8, "memoryGiB": 12, "timeoutMinutes": 1},
        "command": ["bash", "run_task.sh", "--flag", "a b"],
        "variables": {"WORKER_MODEL": "m1"},
        "secrets": {"CODEX_API_KEY": "task-codex-gateway-key"},
        "inputs": {"idField": "item_name"},
        "samplesPerItem": 1,
        "concurrency": 2,
        "budgetUsd": 0,
        "specHash": "sha256:abc",
    }
    spec.update(over)
    return spec


def run_collect(root, files, links=None):
    """Run COLLECT with /tmp/op mapped to <root>/op and an uploader that copies
    into <root>/gcs. files maps paths under op to contents (empty logs are
    added), links maps paths under op to symlink targets. Returns the report
    COLLECT prints last and the store directory."""
    op, store = os.path.join(root, "op"), os.path.join(root, "gcs")
    uploader = (
        "import os, shutil, sys\n"
        f"dst = os.path.join({store!r}, sys.argv[2].removeprefix('gs://'))\n"
        "os.makedirs(os.path.dirname(dst), exist_ok=True)\n"
        "shutil.copy(sys.argv[1], dst)\n"
    )
    logs = {"logs/stdout.log": "", "logs/stderr.log": ""}
    for name, data in {**logs, **files, "upload.py": uploader}.items():
        path = os.path.join(op, name)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            f.write(data.encode() if isinstance(data, str) else data)
    for name, target in (links or {}).items():
        os.symlink(target, os.path.join(op, name))
    script = attempt.COLLECT.replace("/tmp/op/", op + "/")
    out = subprocess.run(
        [sys.executable, "-c", script, "gs://b/jobs/j/tasks/0/0/attempt-1"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    return attempt.last_line(out), store


def jsonl(items):
    return "".join(json.dumps(i) + "\n" for i in items).encode()


class Harness(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.termlog = os.path.join(self.tmp.name, "termination-log")
        patcher = mock.patch.object(source, "fetch_tarball", return_value=TARBALL)
        patcher.start()
        self.addCleanup(patcher.stop)

    def orchestrator(self, items, sandboxes=None, env=None, **spec):
        self.gcs = FakeGCS()
        self.sandboxes = sandboxes or FakeSandboxes()
        settings = Settings(
            poll_s=0.01,
            summary_s=0.02,
            gone_wait_s=0,
            stop_wait_s=2,
            termination_log=self.termlog,
        )
        env = (
            {"OP_GITHUB_TOKEN": "ghp-token", "OP_SECRET_CODEX_API_KEY": SECRET}
            if env is None
            else env
        )
        self.sandboxes.gcs = self.gcs
        self.spec_raw = json.dumps(make_spec(**spec)).encode()
        return Orchestrator(
            self.spec_raw, jsonl(items), env, self.gcs, self.sandboxes, settings
        )

    def summary(self):
        return self.gcs.json("jobs/abcdefgh12345678/summary.json")

    def task_json(self, index, sample, attempt_n):
        return self.gcs.json(
            f"jobs/abcdefgh12345678/tasks/{index}/{sample}/attempt-{attempt_n}/task.json"
        )

    def spy_task_json(self) -> list:
        """Collects every task.json the orchestrator writes, in order."""
        writes = []
        put = self.gcs.put

        def spy(name, data, content_type="application/octet-stream"):
            if name.endswith("/task.json"):
                writes.append(json.loads(data))
            put(name, data, content_type)

        self.gcs.put = spy
        return writes

    def termination_message(self):
        with open(self.termlog) as f:
            return f.read().strip()

    def assert_no_secret(self):
        for name, data in self.gcs.objects.items():
            self.assertNotIn(SECRET.encode(), data, name)


class TasksTest(unittest.TestCase):
    def test_expand_tasks_orders_by_index_then_sample(self):
        tasks = expand_tasks([{"id": "a"}, {"id": 7}], "id", 3)
        self.assertEqual(
            [(t.index, t.sample, t.item_id) for t in tasks],
            [
                (0, 0, "a"),
                (0, 1, "a"),
                (0, 2, "a"),
                (1, 0, "7"),
                (1, 1, "7"),
                (1, 2, "7"),
            ],
        )

    def test_validate_result(self):
        ok = {"version": 1, "outcome": "accepted", "class": "quality"}
        cases = [
            ("minimal", ok, None),
            (
                "full",
                {
                    **ok,
                    "detail": "d",
                    "metrics": {"rounds": 10, "score": 0.5},
                    "usage": {"costUsd": 1.84, "tokens": 412000, "complete": False},
                },
                None,
            ),
            (
                "rejected upstream",
                {**ok, "outcome": "rejected", "class": "upstream"},
                None,
            ),
            ("not an object", [ok], "not a JSON object"),
            ("wrong version", {**ok, "version": 3}, "version"),
            ("bad outcome", {**ok, "outcome": "ok"}, "outcome"),
            ("bad class", {**ok, "class": "other"}, "class must be"),
            ("accepted not quality", {**ok, "class": "timeout"}, "accepted result"),
            ("long detail", {**ok, "detail": "x" * 501}, "detail"),
            ("metric not number", {**ok, "metrics": {"a": "1"}}, "metrics"),
            ("metric bool", {**ok, "metrics": {"a": True}}, "metrics"),
            ("negative cost", {**ok, "usage": {"costUsd": -1}}, "costUsd"),
            ("complete not bool", {**ok, "usage": {"complete": "yes"}}, "complete"),
            ("fractional tokens", {**ok, "usage": {"tokens": 1.5}}, "tokens"),
        ]
        for name, doc, error in cases:
            with self.subTest(name):
                if error is None:
                    result, _ = validate_result(doc)
                    self.assertEqual(result["outcome"], doc["outcome"])
                else:
                    with self.assertRaisesRegex((TypeError, ValueError), error):
                        validate_result(doc)

    def test_summary_counts_follow_the_class_table(self):
        def task(status, outcome="", cls="", complete=None):
            t = Task(index=0, sample=0, item={}, item_id="x", status=status)
            if outcome:
                t.result = {"outcome": outcome, "class": cls, "detail": ""}
            if complete is not None:
                t.usage = {"costUsd": 1.5, "complete": complete}
            return t

        tasks = [
            task("queued"),
            task("running"),
            task("finished", "accepted", "quality", True),
            task("finished", "rejected", "quality", True),
            task("finished", "rejected", "timeout", True),
            *[
                task("finished", "rejected", c, True)
                for c in ("infra", "upstream", "budget", "task_error")
            ],
        ]
        doc = summary("j", "running", tasks, 10.5, time.time())
        self.assertEqual(
            doc["counts"],
            {
                "total": 9,
                "queued": 1,
                "running": 1,
                "accepted": 1,
                "rejected": 2,
                "notCounted": 4,
            },
        )
        self.assertTrue(doc["usageComplete"])
        tasks.append(task("finished", "accepted", "quality"))
        self.assertFalse(
            summary("j", "running", tasks, 0, time.time())["usageComplete"]
        )


class SourceTest(unittest.TestCase):
    def names(self, archive):
        with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as tar:
            return sorted(m.name for m in tar if m.isfile())

    def test_extract_path_keeps_only_the_directory(self):
        self.assertEqual(
            self.names(source.extract_path(TARBALL, "tasks/")),
            ["lib/a.py", "run_task.sh"],
        )

    def test_extract_root(self):
        self.assertEqual(
            self.names(source.extract_path(TARBALL, "")),
            ["README.md", "tasks/lib/a.py", "tasks/run_task.sh"],
        )

    def test_missing_path_fails(self):
        for path in ("nope", "README.md", "task"):
            with (
                self.subTest(path),
                self.assertRaisesRegex(RuntimeError, "not a directory"),
            ):
                source.extract_path(TARBALL, path)

    def test_fetch_follows_redirect_without_token(self):
        import http.server

        seen = {}

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                seen[self.path] = self.headers.get("Authorization")
                if self.path.startswith("/repos/"):
                    self.send_response(302)
                    self.send_header("Location", f"http://127.0.0.1:{port}/codeload")
                    self.end_headers()
                    return
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"tarball-bytes")

            def log_message(self, *args):
                pass

        server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
        port = server.server_address[1]
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        api = f"http://127.0.0.1:{port}/repos/{{repo}}/tarball/{{commit}}"
        with mock.patch.object(source, "API", api):
            self.assertEqual(
                source.fetch_tarball("o/r", "abc", "tok"), b"tarball-bytes"
            )
        self.assertEqual(
            seen, {"/repos/o/r/tarball/abc": "Bearer tok", "/codeload": None}
        )


class RunTest(Harness):
    def test_runs_every_task_and_writes_records(self):
        items = [{"item_name": "Chair"}, {"item_name": "Table"}]
        orch = self.orchestrator(items, samplesPerItem=2)
        self.assertEqual(orch.run(), 0)

        prefix = "jobs/abcdefgh12345678/"
        self.assertEqual(self.gcs.objects[prefix + "spec.json"], self.spec_raw)
        self.assertEqual(self.gcs.objects[prefix + "inputs.jsonl"], jsonl(items))
        with tarfile.open(
            fileobj=io.BytesIO(self.gcs.objects[prefix + "source.tar.gz"])
        ) as tar:
            self.assertIn("run_task.sh", tar.getnames())
        doc = self.summary()
        self.assertEqual(doc["state"], "finished")
        self.assertEqual(doc["counts"]["accepted"], 4)
        self.assertEqual(
            [(t["index"], t["sample"]) for t in doc["tasks"]],
            [(0, 0), (0, 1), (1, 0), (1, 1)],
        )

        rec = self.task_json(1, 1, 1)
        self.assertEqual(rec["task"], "abcdefgh12345678/1/1")
        self.assertEqual(rec["item"], {"id": "Table", "index": 1})
        self.assertEqual(rec["status"], "finished")
        self.assertEqual(rec["sandbox"]["region"], "asia-southeast1")
        self.assertEqual(rec["result"]["class"], "quality")
        self.assertEqual(rec["exitCode"], 0)

        envs = self.sandboxes.created[0].envs
        self.assertEqual(envs["CODEX_API_KEY"], SECRET)
        self.assertEqual(envs["WORKER_MODEL"], "m1")
        self.assertEqual(envs["OP_INPUT"], "/tmp/op/input.json")
        self.assertEqual(envs["OP_OUTPUT_DIR"], "/tmp/op/output")
        task_envs = sorted(
            (
                sb.envs["OP_TASK_ID"],
                sb.envs["OP_ITEM_ID"],
                sb.envs["OP_ITEM_INDEX"],
                sb.envs["OP_SAMPLE_INDEX"],
            )
            for sb in self.sandboxes.created
        )
        self.assertEqual(task_envs[3], ("abcdefgh12345678/1/1", "Table", "1", "1"))
        self.assertEqual(self.sandboxes.live, 0)
        self.assert_no_secret()

    def test_null_variables_and_secrets(self):
        """Go marshals an empty map as null."""
        orch = self.orchestrator([{"item_name": "a"}], variables=None, secrets=None)
        self.assertEqual(orch.run(), 0)
        self.assertEqual(self.sandboxes.created[0].envs["OP_ITEM_ID"], "a")

    def test_task_json_types_match_one_platform(self):
        """null for values not known yet; tokens as an integer."""
        result = {
            "version": 1,
            "outcome": "accepted",
            "class": "quality",
            "usage": {"costUsd": 1.5, "tokens": 412000.0},
        }
        orch = self.orchestrator([{"item_name": "a", "behavior": {"result": result}}])
        writes = self.spy_task_json()
        self.assertEqual(orch.run(), 0)
        running, finished = writes
        self.assertEqual(running["status"], "running")
        self.assertIsNone(running["finishedAt"])
        self.assertIsNone(running["result"])
        self.assertIsNone(running["usage"])
        self.assertRegex(running["startedAt"], r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ$")
        self.assertEqual(finished["usage"], {"costUsd": 1.5, "tokens": 412000})
        self.assertIsInstance(finished["usage"]["tokens"], int)

    def test_command_line_quotes_arguments(self):
        line = attempt.command_line(["bash", "run_task.sh", "a b", "$HOME"])
        self.assertTrue(
            line.endswith("cd /tmp/op/src && exec bash run_task.sh 'a b' '$HOME'"), line
        )

    def test_collect_script_uploads_logs_and_output(self):
        """Runs the in-sandbox script with /tmp/op remapped and a copying uploader."""
        files = {
            "logs/stdout.log": "out",
            "logs/stderr.log": "warming up\n\nTraceback (most recent call last):\nValueError: no item\n",
            "output/meshes/a.glb": "mesh",
            "output/result.json": '{"version": 1}',
        }
        report, store = run_collect(self.tmp.name, files)
        self.assertEqual(
            report,
            {"size": 14, "text": '{"version": 1}', "declared": {},
             "stderrTail": "warming up | Traceback (most recent call last): | ValueError: no item"},
        )
        uploaded = []
        for root, _, names in os.walk(store):
            uploaded += [os.path.relpath(os.path.join(root, n), store) for n in names]
        prefix = "b/jobs/j/tasks/0/0/attempt-1/"
        self.assertEqual(
            sorted(uploaded),
            [
                prefix + n
                for n in (
                    "output/meshes/a.glb",
                    "output/result.json",
                    "stderr.log",
                    "stdout.log",
                )
            ],
        )

    def test_missing_result_quotes_exit_code_and_stderr(self):
        tail = "python3: can't open file '/tmp/op/src/one-platform/run_task.py': [Errno 2] No such file or directory"
        judged = attempt.judge({"size": -1, "text": None, "stderrTail": tail}, 2)
        result, usage = judged["result"], judged["usage"]
        self.assertEqual(result["class"], "task_error")
        self.assertEqual(result["detail"], "result.json is missing; exit 2; stderr: " + tail)
        self.assertEqual(usage, {})
        judged = attempt.judge({"size": -1, "text": None, "stderrTail": "x" * 900}, 1)
        self.assertEqual(len(judged["result"]["detail"]), 500)
        judged = attempt.judge({"size": -1, "text": None, "stderrTail": ""}, None)
        self.assertEqual(judged["result"]["detail"], "result.json is missing")

    def test_concurrency_limits_live_sandboxes(self):
        items = [{"item_name": f"i{n}", "behavior": {"polls": 4}} for n in range(10)]
        orch = self.orchestrator(items, concurrency=3)
        self.assertEqual(orch.run(), 0)
        self.assertEqual(self.sandboxes.max_live, 3)
        self.assertEqual(len(self.sandboxes.created), 10)

    def test_create_failures_retry_in_new_attempt_dirs(self):
        orch = self.orchestrator(
            [{"item_name": "a"}], sandboxes=FakeSandboxes(fail_creates=2), concurrency=1
        )
        self.assertEqual(orch.run(), 0)
        for n in (1, 2):
            rec = self.task_json(0, 0, n)
            self.assertEqual(
                (rec["status"], rec["result"]["class"]), ("finished", "infra")
            )
            self.assertIn("create sandbox", rec["result"]["detail"])
        self.assertEqual(self.task_json(0, 0, 3)["result"]["class"], "quality")
        row = self.summary()["tasks"][0]
        self.assertEqual((row["attempt"], row["outcome"]), (3, "accepted"))

    def test_lost_sandbox_gives_up_after_three_attempts(self):
        orch = self.orchestrator([{"item_name": "a", "behavior": {"lost": True}}])
        self.assertEqual(orch.run(), 0)
        self.assertEqual(len(self.sandboxes.created), 3)
        self.assertNotIn(
            "jobs/abcdefgh12345678/tasks/0/0/attempt-4/task.json", self.gcs.objects
        )
        self.assertEqual(self.task_json(0, 0, 3)["result"]["class"], "infra")
        self.assertEqual(self.summary()["counts"]["notCounted"], 1)

    def test_task_verdicts(self):
        cases = [
            ("missing result", {"result": None}, "task_error", "missing"),
            ("invalid json", {"result": "{"}, "task_error", "invalid"),
            (
                "invalid result",
                {"result": {"version": 1, "outcome": "accepted", "class": "infra"}},
                "task_error",
                "invalid",
            ),
            ("timeout", {"hang": True}, "timeout", "longer than"),
            ("upload failed", {"collect_exit": 1}, "infra", "upload output"),
            (
                "setup failed",
                {"setup_exit": 127},
                "task_error",
                "unpack source: exit 127",
            ),
            (
                "timeout then lost",
                {"hang": True, "collect_raises": True},
                "timeout",
                "longer than",
            ),
            (
                "rejected",
                {
                    "exit": 3,
                    "result": {
                        "version": 1,
                        "outcome": "rejected",
                        "class": "quality",
                        "detail": "BAD",
                    },
                },
                "quality",
                "BAD",
            ),
        ]
        for name, behavior, cls, detail in cases:
            with self.subTest(name):
                orch = self.orchestrator(
                    [{"item_name": "a", "behavior": behavior}],
                    sandboxes=FakeSandboxes(),
                )
                orch.spec["sandbox"]["timeoutMinutes"] = 0.002
                self.assertEqual(orch.run(), 0)
                rec = self.task_json(0, 0, 1)
                self.assertEqual(rec["result"]["class"], cls)
                self.assertIn(detail, rec["result"]["detail"])
                self.assertEqual(len(self.sandboxes.created), 1, "not retried")

    def test_budget_stops_new_tasks(self):
        result = {
            "version": 1,
            "outcome": "accepted",
            "class": "quality",
            "usage": {"costUsd": 5},
        }
        items = [
            {"item_name": f"i{n}", "behavior": {"result": result}} for n in range(5)
        ]
        orch = self.orchestrator(items, concurrency=1, budgetUsd=10)
        self.assertEqual(orch.run(), 0)
        doc = self.summary()
        self.assertEqual(doc["state"], "stoppedBudget")
        self.assertEqual(doc["spentUsd"], 10)
        self.assertEqual((doc["counts"]["accepted"], doc["counts"]["queued"]), (2, 3))

    def test_sigterm_deletes_sandboxes_and_writes_final_summary(self):
        items = [{"item_name": f"i{n}", "behavior": {"hang": True}} for n in range(5)]
        orch = self.orchestrator(
            items,
            concurrency=2,
            sandbox={"cpu": 8, "memoryGiB": 12, "timeoutMinutes": 60},
        )
        code = []
        runner = threading.Thread(target=lambda: code.append(orch.run()))
        runner.start()
        deadline = time.monotonic() + 5
        while self.sandboxes.live < 2 and time.monotonic() < deadline:
            time.sleep(0.01)
        orch.terminate()
        runner.join(5)
        self.assertEqual(code, [143])
        self.assertEqual(self.sandboxes.delete_all_calls, 1)
        self.assertEqual(self.sandboxes.live, 0)
        self.assertEqual(
            len(self.sandboxes.created), 2, "no task started after SIGTERM"
        )
        doc = self.summary()
        self.assertEqual(doc["state"], "stopped")
        self.assertEqual(doc["counts"]["queued"], 3)
        self.assertEqual(doc["counts"]["notCounted"], 2)
        self.assertIn("stopped", self.task_json(0, 0, 1)["result"]["detail"])
        self.assertEqual(self.termination_message(), "terminated by SIGTERM")

    def test_missing_secret_fails_before_any_sandbox(self):
        orch = self.orchestrator([{"item_name": "a"}], env={"OP_GITHUB_TOKEN": "t"})
        self.assertEqual(orch.run(), 1)
        self.assertEqual(self.sandboxes.created, [])
        self.assertEqual(
            self.termination_message(),
            "secret CODEX_API_KEY is not set (Secret task-codex-gateway-key, key value)",
        )

    def test_download_failure_fails_the_job(self):
        orch = self.orchestrator([{"item_name": "a"}])
        with mock.patch.object(
            source, "fetch_tarball", side_effect=RuntimeError("HTTP 404")
        ):
            self.assertEqual(orch.run(), 1)
        self.assertIn(
            "download PranaLabs/PranaSkills@cccccccccccc", self.termination_message()
        )
        self.assertEqual(self.summary()["state"], "stopped")
        self.assertEqual(self.sandboxes.created, [])

    def test_record_write_failure_stops_the_job(self):
        orch = self.orchestrator(
            [{"item_name": f"i{n}"} for n in range(3)], concurrency=1
        )
        put = self.gcs.put

        def flaky(name, data, content_type="application/octet-stream"):
            if "/tasks/" in name:
                raise RuntimeError("HTTP 403")
            put(name, data, content_type)

        self.gcs.put = flaky
        self.assertEqual(orch.run(), 1)
        self.assertIn("HTTP 403", self.termination_message())
        self.assertEqual(self.sandboxes.live, 0)
        self.assertEqual(len(self.sandboxes.created), 1)



def validate(doc):
    """Both steps of judging a result.json document."""
    return validate_result(doc), validate_outputs(doc)


def with_file(path="a.glb", role="mesh"):
    return {**V2, "files": [{"path": path, "role": role}]}


def with_check(**check):
    return {**V2, "checks": [{"id": "c", "pass": True, **check}]}


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class ResultV2Test(unittest.TestCase):
    def test_version_1_is_accepted_unchanged(self):
        v1 = {
            "version": 1,
            "outcome": "accepted",
            "class": "quality",
            "record": [],
            "files": "not read in version 1",
            "checks": 3,
        }
        (result, _), outputs = validate(v1)
        self.assertEqual(result["checks"], [])
        self.assertEqual(outputs, ({}, []))

    def test_version_2(self):
        (result, usage), (record, files) = validate(V2)
        self.assertEqual(result["checks"], V2["checks"])
        self.assertEqual(usage, V2["usage"])
        self.assertEqual(record, V2["record"])
        self.assertEqual(files, V2["files"])
        bare = {"version": 2, "outcome": "rejected", "class": "quality"}
        (result, _), outputs = validate(bare)
        self.assertEqual((result["checks"], outputs), ([], ({}, [])))

    def test_version_2_limits_are_inclusive(self):
        doc = {
            **V2,
            # '{"a":""}' is 8 bytes.
            "record": {"a": "x" * (MAX_RECORD - 8)},
            "files": [{"path": f"v/{i}.png", "role": "r" * 64} for i in range(999)]
            + [{"path": "é" * 512, "role": "mesh"}],
            "checks": [{"id": f"c{i}", "pass": False} for i in range(199)]
            + [{"id": "A.z_0:-" + "c" * 121, "pass": True, "value": 0.5, "detail": "x" * 500}],
        }
        (result, _), (_, files) = validate(doc)
        self.assertEqual((len(result["checks"]), len(files)), (200, 1000))

    def test_version_2_rules(self):
        cases = [
            ("version 3", {**V2, "version": 3}, "version must be 1 or 2"),
            ("record not object", {**V2, "record": [1]}, "record must be an object"),
            (
                "record too large",
                {**V2, "record": {"a": "x" * (MAX_RECORD - 7)}},
                "record is larger than 65536 bytes as compact JSON",
            ),
            (
                "record counted in UTF-8 bytes",
                {**V2, "record": {"a": "é" * 32765}},
                "record is larger than 65536 bytes",
            ),
            ("record NaN", {**V2, "record": {"a": float("nan")}}, "not JSON compliant"),
            ("files not list", {**V2, "files": {}}, "files must be a list"),
            (
                "too many files",
                {**V2, "files": [{"path": f"{i}", "role": "r"} for i in range(1001)]},
                "files has more than 1000 entries",
            ),
            ("file not object", {**V2, "files": ["a.glb"]}, "files[0] must be an object"),
            ("path missing", {**V2, "files": [{"role": "mesh"}]}, "files[0].path must be a non-empty string"),
            ("path empty", with_file(""), "files[0].path must be a non-empty string"),
            ("path not string", with_file(7), "files[0].path must be a non-empty string"),
            ("path too long", with_file("é" * 513), "files[0].path is longer than 1024 bytes"),
            ("path not UTF-8", with_file("\ud800"), "files[0].path is not valid UTF-8"),
            ("path absolute", with_file("/tmp/op/output/a.glb"), "must be relative to /tmp/op/output"),
            ("path backslash", with_file("mesh\\a.glb"), "must not contain a backslash"),
            ("path ..", with_file("a/../../etc/passwd"), "must not contain a .. segment"),
            ("path leading ..", with_file("../a.glb"), "must not contain a .. segment"),
            ("path empty segment", with_file("a//b.glb"), "must not contain an empty segment"),
            ("path trailing slash", with_file("a/"), "must not contain an empty segment"),
            ("path . segment", with_file("a/./b.glb"), "must be normalized"),
            ("path leading .", with_file("./a.glb"), "must be normalized"),
            ("path .", with_file("."), "must be normalized"),
            ("path result.json", with_file("result.json"), "files[0].path must not be result.json"),
            (
                "path twice",
                {**V2, "files": [*V2["files"], {"path": "views/0.png", "role": "mesh"}]},
                "files[2].path views/0.png is listed twice",
            ),
            ("role missing", {**V2, "files": [{"path": "a"}]}, "files[0].role must be a string of 1 to 64"),
            ("role empty", with_file(role=""), "files[0].role must be a string of 1 to 64"),
            ("role too long", with_file(role="r" * 65), "files[0].role must be a string of 1 to 64"),
            ("role not string", with_file(role=3), "files[0].role must be a string of 1 to 64"),
            ("checks not list", {**V2, "checks": {"c": True}}, "checks must be a list"),
            (
                "too many checks",
                {**V2, "checks": [{"id": f"c{i}", "pass": True} for i in range(201)]},
                "checks has more than 200 entries",
            ),
            ("check not object", {**V2, "checks": ["c"]}, "checks[0] must be an object"),
            ("id missing", {**V2, "checks": [{"pass": True}]}, "checks[0].id must match"),
            ("id empty", with_check(id=""), "checks[0].id must match"),
            ("id with space", with_check(id="views count"), "checks[0].id must match"),
            ("id with newline", with_check(id="views\n"), "checks[0].id must match"),
            ("id too long", with_check(id="c" * 129), "checks[0].id must match"),
            (
                "id twice",
                {**V2, "checks": [*V2["checks"], {"id": "views.count", "pass": False}]},
                "checks[2].id views.count is listed twice",
            ),
            ("pass missing", {**V2, "checks": [{"id": "c"}]}, "checks[0].pass must be a boolean"),
            ("pass not bool", with_check(**{"pass": 1}), "checks[0].pass must be a boolean"),
            ("value bool", with_check(value=True), "checks[0].value must be a finite number"),
            ("value string", with_check(value="14"), "checks[0].value must be a finite number"),
            ("value null", with_check(value=None), "checks[0].value must be a finite number"),
            ("value infinite", with_check(value=float("inf")), "checks[0].value must be a finite number"),
            ("detail too long", with_check(detail="x" * 501), "checks[0].detail must be a string of at most 500"),
            ("detail not string", with_check(detail=5), "checks[0].detail must be a string of at most 500"),
        ]
        for name, doc, error in cases:
            with (
                self.subTest(name),
                self.assertRaisesRegex((TypeError, ValueError), re.escape(error)),
            ):
                validate(doc)

    def test_invalid_version_2_result_is_a_task_error(self):
        out = {"size": 1, "text": json.dumps(with_file("../a.glb")), "stderrTail": ""}
        judged = attempt.judge(out, 0)
        self.assertEqual(judged["result"]["class"], "task_error")
        self.assertEqual(
            judged["result"]["detail"],
            "result.json is invalid: files[0].path must not contain a .. segment; exit 0",
        )
        self.assertEqual((judged["usage"], judged["record"], judged["files"]), ({}, {}, []))

    def test_deeply_nested_result_is_a_task_error(self):
        judged = attempt.judge({"size": 1, "text": "[" * 100000, "stderrTail": ""})
        self.assertEqual(judged["result"]["class"], "task_error")
        self.assertIn("result.json is invalid", judged["result"]["detail"])

    def test_composite_object_has_no_md5(self):
        obj = {"name": "p/output/a.bin", "size": "4", "generation": "17"}
        report = {"size": 4, "sha256": "0" * 64}
        entry = attempt.located("b", {"path": "a.bin", "role": "r"}, report, obj)
        self.assertEqual(
            entry,
            {
                "path": "a.bin",
                "role": "r",
                "size": 4,
                "sha256": "0" * 64,
                "uri": "gs://b/p/output/a.bin",
                "generation": "17",
                "md5": None,
            },
        )


class CollectTest(unittest.TestCase):
    """Runs COLLECT itself against a temporary /tmp/op."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def dir(self, name):
        return os.path.join(self.tmp.name, name)

    def test_reports_declared_files(self):
        big = bytes(range(256)) * 12289  # just over 3 MiB: several read blocks
        declared = [
            "mesh/a.glb",
            "empty.txt",
            "big.bin",
            "mesh",
            "link.glb",
            "linked/a.glb",
            "missing.glb",
            "../output/mesh/a.glb",
            "/etc/hostname",
        ]
        result = {
            "version": 2,
            "files": [{"path": p, "role": "r"} for p in declared]
            + [1, None, {"path": 2}, {"path": ["x"]}, {"role": "no path"}],
        }
        files = {
            "output/mesh/a.glb": "mesh",
            "output/empty.txt": "",
            "output/big.bin": big,
            "output/result.json": json.dumps(result),
        }
        links = {"output/link.glb": "mesh/a.glb", "output/linked": "mesh"}
        report, _ = run_collect(self.tmp.name, files, links)
        outside = {"error": "is outside the output directory"}
        self.assertEqual(
            report["declared"],
            {
                "mesh/a.glb": {"size": 4, "sha256": sha256(b"mesh")},
                "empty.txt": {"size": 0, "sha256": sha256(b"")},
                "big.bin": {"size": len(big), "sha256": sha256(big)},
                "mesh": {"error": "is not a regular file"},
                "link.glb": {"error": "is a symlink or under one"},
                "linked/a.glb": {"error": "is a symlink or under one"},
                "missing.glb": {"error": "does not exist"},
                "../output/mesh/a.glb": outside,
                "/etc/hostname": outside,
            },
        )

    def test_malformed_result_gives_no_report(self):
        too_many = {"files": [{"path": "a", "role": "r"}] * 1001}
        texts = [
            "{",
            "[1]",
            '"files"',
            '{"files": 3}',
            '{"files": {"a": 1}}',
            json.dumps(too_many),
            "[" * 100000,
        ]
        for n, text in enumerate(texts):
            with self.subTest(text[:20]):
                files = {"output/result.json": text}
                report, _ = run_collect(self.dir(str(n)), files)
                self.assertEqual((report["text"], report["declared"]), (text, {}))

    def test_reads_result_json_up_to_1_mib(self):
        self.assertEqual(attempt.MAX_RESULT, 1 << 20)
        doc = '{"version": 1, "outcome": "accepted", "class": "quality"}'
        fits = doc + " " * (attempt.MAX_RESULT - len(doc))
        report, _ = run_collect(self.dir("fits"), {"output/result.json": fits})
        self.assertEqual((report["size"], report["text"]), (1 << 20, fits))
        self.assertEqual(attempt.judge(report)["result"]["outcome"], "accepted")

        report, _ = run_collect(self.dir("over"), {"output/result.json": fits + " "})
        self.assertEqual((report["size"], report["text"]), ((1 << 20) + 1, None))
        self.assertEqual(
            attempt.judge(report)["result"]["detail"],
            "result.json is larger than 1048576 bytes",
        )


class DeclaredFilesTest(Harness):
    """result.json version 2 through the orchestrator, with fake GCS and sandboxes."""

    OUTPUT = "jobs/abcdefgh12345678/tasks/0/0/attempt-1/output/"

    def run_one(self, behavior, list_error="", **spec):
        orch = self.orchestrator([{"item_name": "Chair", "behavior": behavior}], **spec)
        self.gcs.list_error = list_error
        writes = self.spy_task_json()
        self.assertEqual(orch.run(), 0)
        self.assertEqual(len(self.sandboxes.created), 1, "not retried")
        return writes

    def expected_files(self):
        files = []
        for f in V2["files"]:
            name = self.OUTPUT + f["path"]
            data = OUTPUTS[f["path"]].encode()
            meta = self.gcs.metadata(name)
            files.append(
                {
                    **f,
                    "size": len(data),
                    "sha256": sha256(data),
                    "uri": f"gs://artifacts/{name}",
                    "generation": meta["generation"],
                    "md5": meta["md5Hash"],
                }
            )
        return files

    def test_task_json_v2_for_an_accepted_attempt(self):
        running, finished = self.run_one({"result": V2, "outputs": OUTPUTS})
        self.assertEqual(running["version"], 2)
        self.assertEqual((running["result"], running["record"], running["files"]), (None, {}, []))
        self.assertEqual(finished["version"], 2)
        self.assertEqual(
            finished["result"],
            {
                "outcome": "accepted",
                "class": "quality",
                "detail": "ok",
                "metrics": {"rounds": 3},
                "checks": V2["checks"],
            },
        )
        self.assertEqual(finished["usage"], V2["usage"])
        self.assertEqual(finished["record"], V2["record"])
        self.assertEqual(finished["files"], self.expected_files())
        self.assertIsInstance(finished["files"][0]["generation"], str)
        self.assertEqual(self.gcs.listed, [self.OUTPUT])

    def test_rejected_attempt_keeps_its_record_and_files(self):
        result = {**V2, "outcome": "rejected", "detail": "BAD"}
        _, finished = self.run_one({"result": result, "outputs": OUTPUTS})
        self.assertEqual(
            (finished["result"]["outcome"], finished["result"]["class"]),
            ("rejected", "quality"),
        )
        self.assertEqual(finished["result"]["checks"], V2["checks"])
        self.assertEqual(finished["record"], V2["record"])
        self.assertEqual(finished["files"], self.expected_files())

    def test_task_error_attempt_has_no_record_or_files(self):
        _, finished = self.run_one({"result": with_file("/abs.glb"), "outputs": OUTPUTS})
        self.assertEqual(finished["version"], 2)
        self.assertEqual(finished["result"]["class"], "task_error")
        self.assertIn("files[0].path must be relative", finished["result"]["detail"])
        self.assertEqual(finished["result"]["checks"], [])
        self.assertEqual((finished["record"], finished["files"]), ({}, []))
        self.assertEqual(self.gcs.listed, [])

    def test_version_1_result_lists_nothing(self):
        _, finished = self.run_one({})
        self.assertEqual(finished["result"]["checks"], [])
        self.assertEqual((finished["record"], finished["files"]), ({}, []))
        self.assertEqual(self.gcs.listed, [])

    def test_declared_files_must_check_out(self):
        bad_sha = {"size": 3, "sha256": "x"}
        cases = [
            ("not uploaded", {"not_uploaded": ["views/0.png"]}, "views/0.png: was not uploaded"),
            (
                "size mismatch",
                {"report": {"views/0.png": {"size": 5, "sha256": "0" * 64}}},
                "views/0.png: the sandbox read 5 bytes but the bucket holds 3",
            ),
            (
                "sandbox error",
                {"report": {"asset/mesh/chair.glb": {"error": "is a symlink or under one"}}},
                "asset/mesh/chair.glb: is a symlink or under one",
            ),
            (
                "not reported",
                {"report": {"asset/mesh/chair.glb": None}},
                "asset/mesh/chair.glb: the sandbox did not report it",
            ),
            (
                "malformed report",
                {"report": {"views/0.png": bad_sha}},
                "views/0.png: the sandbox's report is malformed",
            ),
        ]
        for name, behavior, problem in cases:
            with self.subTest(name):
                behavior = {"result": V2, "outputs": OUTPUTS, **behavior}
                _, finished = self.run_one(behavior)
                self.assertEqual(finished["result"]["class"], "task_error")
                self.assertEqual(finished["result"]["detail"], "declared file " + problem)
                self.assertEqual((finished["record"], finished["files"]), ({}, []))
                # The task ran and reported its cost, so the cost still counts.
                self.assertEqual(finished["usage"], V2["usage"])
                self.assertEqual(self.summary()["spentUsd"], 0.33)

    def test_list_failure_is_infra(self):
        behavior = {"result": V2, "outputs": OUTPUTS}
        _, finished = self.run_one(behavior, list_error="HTTP 503")
        self.assertEqual(finished["result"]["class"], "infra")
        self.assertIn("list output: RuntimeError: HTTP 503", finished["result"]["detail"])
        self.assertEqual((finished["record"], finished["files"]), ({}, []))

    def test_timeout_has_no_record_or_files(self):
        behavior = {"hang": True, "result": V2, "outputs": OUTPUTS}
        sandbox = {"cpu": 8, "memoryGiB": 12, "timeoutMinutes": 0.002}
        _, finished = self.run_one(behavior, sandbox=sandbox)
        self.assertEqual(finished["result"]["class"], "timeout")
        self.assertEqual(finished["result"]["checks"], [])
        self.assertEqual((finished["record"], finished["files"]), ({}, []))
        self.assertEqual(self.gcs.listed, [])


class GCSTest(unittest.TestCase):
    def serve(self, responses):
        """A local JSON API answering GETs with responses in order; returns the
        requests it saw as (path, query, Authorization)."""
        import http.server
        import urllib.parse

        seen = []

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                url = urllib.parse.urlsplit(self.path)
                query = urllib.parse.parse_qs(url.query)
                seen.append((url.path, query, self.headers.get("Authorization")))
                status, body = responses[len(seen) - 1]
                self.send_response(status)
                self.end_headers()
                self.wfile.write(json.dumps(body).encode())

            def log_message(self, *args):
                pass

        server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        url = f"http://127.0.0.1:{server.server_address[1]}/b/{{bucket}}/o?"
        for patcher in (
            mock.patch.object(gcs, "LIST", url),
            mock.patch.object(gcs.time, "sleep"),
            mock.patch.object(gcs.GCS, "token", return_value="tok"),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)
        return seen

    def test_list_follows_pages_and_retries_server_errors(self):
        a = {"name": "p/a", "size": "1", "generation": "7", "md5Hash": "x=="}
        seen = self.serve(
            [
                (503, {}),
                (200, {"items": [a], "nextPageToken": "t2"}),
                (200, {"items": [{"name": "p/b"}]}),
            ]
        )
        self.assertEqual(gcs.GCS("bkt").list("p/"), [a, {"name": "p/b"}])
        query = {"prefix": ["p/"], "fields": [gcs.LIST_FIELDS]}
        self.assertEqual(
            seen,
            [
                ("/b/bkt/o", query, "Bearer tok"),
                ("/b/bkt/o", query, "Bearer tok"),
                ("/b/bkt/o", {**query, "pageToken": ["t2"]}, "Bearer tok"),
            ],
        )

    def test_list_does_not_retry_client_errors(self):
        seen = self.serve([(403, {})])
        with self.assertRaisesRegex(RuntimeError, re.escape("list gs://bkt/p/: HTTP 403")):
            gcs.GCS("bkt").list("p/")
        self.assertEqual(len(seen), 1)


if __name__ == "__main__":
    unittest.main()
