"""Unit tests for the orchestrator; no cluster, SDK or network needed.

python3 -m unittest discover -s deploy/gke/orchestrator -p '*_test.py'
"""

import io
import json
import os
import tarfile
import tempfile
import threading
import time
import unittest
from unittest import mock

import attempt
import source
from orchestrator import Orchestrator, Settings
from records import Task, expand_tasks, summary, validate_result

SECRET = "sk-very-secret-value"


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
        self.lock = threading.Lock()

    def put(self, name, data, content_type="application/octet-stream"):
        with self.lock:
            self.objects[name] = data

    def put_json(self, name, doc):
        self.put(name, json.dumps(doc).encode())

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
    missing, or raw text), hang, lost, setup_exit, collect_exit, collect_raises."""

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
        out = {"size": -1 if text is None else len(text), "text": text}
        return b.get("collect_exit", 0), "uploaded\n" + json.dumps(out) + "\n"

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
            ("wrong version", {**ok, "version": 2}, "version"),
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
        writes = []
        put = self.gcs.put

        def spy(name, data, content_type="application/octet-stream"):
            if name.endswith("/task.json"):
                writes.append(json.loads(data))
            put(name, data, content_type)

        self.gcs.put = spy
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
        import subprocess
        import sys

        op = os.path.join(self.tmp.name, "op")
        store = os.path.join(self.tmp.name, "gcs")
        for d in ("logs", "output/meshes"):
            os.makedirs(os.path.join(op, d))
        files = {
            "logs/stdout.log": "out",
            "logs/stderr.log": "warming up\n\nTraceback (most recent call last):\nValueError: no item\n",
            "output/meshes/a.glb": "mesh",
            "output/result.json": '{"version": 1}',
        }
        for name, text in files.items():
            with open(os.path.join(op, name), "w") as f:
                f.write(text)
        with open(os.path.join(op, "upload.py"), "w") as f:
            f.write(
                "import os, shutil, sys\n"
                f"dst = os.path.join({store!r}, sys.argv[2].removeprefix('gs://'))\n"
                "os.makedirs(os.path.dirname(dst), exist_ok=True)\n"
                "shutil.copy(sys.argv[1], dst)\n"
            )
        script = attempt.COLLECT.replace("/tmp/op/", op + "/")
        out = subprocess.run(
            [sys.executable, "-c", script, "gs://b/jobs/j/tasks/0/0/attempt-1"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout
        self.assertEqual(
            attempt.last_line(out),
            {"size": 14, "text": '{"version": 1}',
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
        result, usage = attempt.judge({"size": -1, "text": None, "stderrTail": tail}, 2)
        self.assertEqual(result["class"], "task_error")
        self.assertEqual(result["detail"], "result.json is missing; exit 2; stderr: " + tail)
        self.assertEqual(usage, {})
        result, _ = attempt.judge({"size": -1, "text": None, "stderrTail": "x" * 900}, 1)
        self.assertEqual(len(result["detail"]), 500)
        result, _ = attempt.judge({"size": -1, "text": None, "stderrTail": ""}, None)
        self.assertEqual(result["detail"], "result.json is missing")

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


if __name__ == "__main__":
    unittest.main()
