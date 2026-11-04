"""Standard orchestrator of a One Platform sandbox Git job.

One Platform runs it as Job opjob-<job> in namespace opensandbox-jobs with the
job's ConfigMap mounted at $OP_JOB_DIR (spec.json, inputs.jsonl). It runs one
sandbox per (item index, sample) task, at most `concurrency` at a time, and
keeps the job's records in gs://$OP_ARTIFACTS_BUCKET/jobs/<job>/. The contract
is docs/sandbox-git-jobs-design.md in One Platform, sections 3-7; version 2 of
result.json and task.json (a task's record, output files and checks) is
section 4 of its docs/data-lineage-design.md.

Exit 0 when every task ended or the budget stopped new tasks; otherwise a
one-line reason goes to /dev/termination-log and the exit code is non-zero.
"""

import json
import logging
import os
import signal
import sys
import threading
import time
from dataclasses import dataclass

import attempt
import source
from gcs import GCS
from records import Task, cost, expand_tasks, now, summary, task_record, verdict

log = logging.getLogger("orchestrator")
UPLOADER = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "sandbox_gcs_upload.py"
)


class Fatal(Exception):
    """The orchestrator itself cannot continue."""


@dataclass
class Settings:
    attempts: int = 3
    poll_s: float = 10
    summary_s: float = 10
    gone_wait_s: float = 60  # how long a deleted sandbox may take to disappear
    stop_wait_s: float = 10  # how long SIGTERM waits for workers to record their tasks
    lifetime_margin_s: float = (
        1800  # sandbox lifetime beyond timeoutMinutes, for setup and upload
    )
    termination_log: str = "/dev/termination-log"


class Orchestrator:
    def __init__(
        self,
        spec: bytes,
        inputs: bytes,
        env,
        gcs,
        sandboxes,
        settings: Settings | None = None,
    ):
        # spec.json is parsed here and written to the bucket byte for byte.
        self.spec_raw, self.spec = spec, json.loads(spec)
        self.inputs, self.env = inputs, env
        self.gcs, self.sandboxes = gcs, sandboxes
        self.settings = settings or Settings()
        self.job = self.spec["job"]
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.tasks: list[Task] = []
        self.pending: list[Task] = []
        self.live: dict = {}  # sandbox id -> sandbox
        self.spent = 0.0
        self.expanded = False
        self.budget_hit = False
        self.terminated = False
        self.fatal = ""

    # Startup -------------------------------------------------------------

    def prepare(self) -> attempt.Context:
        items = parse_inputs(self.inputs, self.spec["inputs"].get("count"))
        self.envs = self.task_envs()
        token = self.env.get("OP_GITHUB_TOKEN")
        if not token:
            raise Fatal(
                "OP_GITHUB_TOKEN is not set (Secret github-read-token, key value)"
            )
        self.tasks = expand_tasks(
            items, self.spec["inputs"]["idField"], self.spec["samplesPerItem"]
        )
        self.pending = list(self.tasks)
        self.expanded = True
        self.put("spec.json", self.spec_raw, "application/json")
        self.put("inputs.jsonl", self.inputs, "application/x-ndjson")
        self.write_summary()
        src = self.spec["source"]
        try:
            tarball = source.fetch_tarball(src["repo"], src["commit"], token)
            archive = source.extract_path(tarball, src["path"])
        except Exception as e:  # noqa: BLE001 - any download or archive error stops the job
            raise Fatal(
                f"download {src['repo']}@{src['commit'][:12]}: {attempt.short(e)}"
            ) from None
        self.put("source.tar.gz", archive, "application/gzip")
        with open(UPLOADER) as f:
            uploader = f.read()
        return attempt.Context(
            bucket=self.gcs.bucket,
            command=self.spec["command"],
            timeout_s=self.spec["sandbox"]["timeoutMinutes"] * 60,
            source=archive,
            uploader=uploader,
            gcs=self.gcs,
            poll_s=self.settings.poll_s,
        )

    def task_envs(self) -> dict:
        """variables, then each Secret under its own name; OP_* are added per task."""
        # Go writes an empty map as null.
        envs = dict(self.spec.get("variables") or {})
        for name, secret in (self.spec.get("secrets") or {}).items():
            value = self.env.get(f"OP_SECRET_{name}")
            if value is None:
                raise Fatal(f"secret {name} is not set (Secret {secret}, key value)")
            envs[name] = value
        return envs

    # Running -------------------------------------------------------------

    def run(self) -> int:
        try:
            self.ctx = self.prepare()
        except Fatal as e:
            self.fatal = str(e)
            return self.finish()
        writer = threading.Thread(target=self.summary_loop, daemon=True)
        writer.start()
        workers = [
            threading.Thread(target=self.worker, daemon=True)
            for _ in range(self.spec["concurrency"])
        ]
        for w in workers:
            w.start()
        # Short sleeps rather than join(), so the SIGTERM handler runs promptly.
        while any(w.is_alive() for w in workers) and not self.stop.is_set():
            time.sleep(0.2)
        if self.stop.is_set():
            self.shutdown(workers)
        self.stop.set()
        writer.join(self.settings.stop_wait_s)
        return self.finish()

    def worker(self) -> None:
        while not self.stop.is_set():
            task = self.next_task()
            if task is None:
                return
            try:
                self.run_task(task)
            except Exception as e:  # noqa: BLE001 - e.g. records cannot be written
                self.fatal = self.fatal or attempt.short(e)
                self.stop.set()
                return

    def next_task(self) -> Task | None:
        with self.lock:
            if not self.pending:
                return None
            budget = self.spec.get("budgetUsd") or 0
            if budget > 0 and self.spent >= budget:
                self.budget_hit = True
                return None
            return self.pending.pop(0)

    def run_task(self, task: Task) -> None:
        for n in range(1, self.settings.attempts + 1):
            if self.stop.is_set() or not self.run_attempt(task, n):
                return

    def run_attempt(self, task: Task, n: int) -> bool:
        """Run attempt n of a task; return True when it should be retried."""
        prefix = f"jobs/{self.job}/tasks/{task.index}/{task.sample}/attempt-{n}"
        with self.lock:
            task.status, task.attempt, task.result, task.usage = "running", n, {}, {}
            task.record, task.files = {}, []
            task.started, task.finished = time.time(), 0.0
        run = {"startedAt": now()}
        try:
            sb = self.create_sandbox()
        except Exception as e:  # noqa: BLE001
            out = {
                "result": verdict("infra", f"create sandbox: {attempt.short(e)}"),
                "retry": True,
            }
            return self.record(task, prefix, run, out)
        self.live[sb.id] = sb
        run["sandbox"] = sb.id
        try:
            self.gcs.put_json(f"{prefix}/task.json", self.task_doc(task, run))
            out = attempt.run(
                self.sandboxes,
                sb,
                self.ctx,
                task.item,
                self.op_envs(task),
                prefix,
                self.stop,
            )
        except attempt.SandboxLost as e:
            out = {"result": verdict("infra", str(e)), "retry": True}
        except attempt.Stopped as e:
            out = {"result": verdict("infra", f"stopped: {e}")}
        finally:
            self.delete(sb)
        return self.record(task, prefix, run, out)

    def create_sandbox(self):
        sandbox = self.spec["sandbox"]
        lifetime = self.ctx.timeout_s + self.settings.lifetime_margin_s
        return self.sandboxes.create(
            self.spec["image"]["ref"], sandbox["cpu"], sandbox["memoryGiB"], lifetime
        )

    def record(self, task: Task, prefix: str, run: dict, out: dict) -> bool:
        run["finishedAt"], run["exitCode"] = now(), out.get("exitCode")
        with self.lock:
            task.status, task.result, task.usage = (
                "finished",
                out["result"],
                out.get("usage", {}),
            )
            task.record, task.files = out.get("record", {}), out.get("files", [])
            task.finished = time.time()
            self.spent += cost(task.usage)
            doc = self.task_doc(task, run)
        self.gcs.put_json(f"{prefix}/task.json", doc)
        r = task.result
        log.info(
            f"task {task.index}/{task.sample} attempt {task.attempt}: {r['outcome']} {r['class']} {r['detail'][:120]}"
        )
        return out.get("retry", False)

    def task_doc(self, task: Task, run: dict) -> dict:
        return task_record(
            self.job,
            self.spec.get("specHash", ""),
            self.spec.get("region", ""),
            task,
            run,
        )

    def delete(self, sb) -> None:
        try:
            self.sandboxes.delete(sb, self.settings.gone_wait_s)
        except Exception as e:  # noqa: BLE001 - the server deletes leftovers by label after the job
            log.warning(f"delete sandbox {sb.id}: {attempt.short(e)}")
        finally:
            self.live.pop(sb.id, None)

    def op_envs(self, task: Task) -> dict:
        return {
            **self.envs,
            "OP_JOB_ID": self.job,
            "OP_TASK_ID": f"{self.job}/{task.index}/{task.sample}",
            "OP_ITEM_ID": task.item_id,
            "OP_ITEM_INDEX": str(task.index),
            "OP_SAMPLE_INDEX": str(task.sample),
            "OP_INPUT": "/tmp/op/input.json",
            "OP_OUTPUT_DIR": "/tmp/op/output",
        }

    # Stopping and summary -------------------------------------------------

    def terminate(self, *_) -> None:
        """SIGTERM: start no task, delete the job's sandboxes, write the summary, exit."""
        self.terminated = True
        self.stop.set()

    def shutdown(self, workers: list[threading.Thread]) -> None:
        try:
            log.info(f"stopping: deleted {self.sandboxes.delete_all()} sandboxes")
        except Exception as e:  # noqa: BLE001 - fall back to the sandboxes this process knows
            log.warning(f"delete sandboxes by label: {attempt.short(e)}")
            for sb in list(self.live.values()):
                try:
                    self.sandboxes.delete(sb, 0)
                except Exception as e2:  # noqa: BLE001
                    log.warning(f"delete sandbox {sb.id}: {attempt.short(e2)}")
        deadline = time.monotonic() + self.settings.stop_wait_s
        for w in workers:
            w.join(max(0.0, deadline - time.monotonic()))

    def state(self) -> str:
        if self.terminated or self.fatal:
            return "stopped"
        if any(t.status == "running" for t in self.tasks):
            return "running"
        if self.budget_hit:
            return "stoppedBudget"
        if any(t.status == "queued" for t in self.tasks):
            return "running"
        return "finished"

    def write_summary(self) -> None:
        with self.lock:
            doc = summary(self.job, self.state(), self.tasks, self.spent, time.time())
        self.gcs.put_json(f"jobs/{self.job}/summary.json", doc)

    def summary_loop(self) -> None:
        while not self.stop.wait(self.settings.summary_s):
            try:
                self.write_summary()
            except Exception as e:  # noqa: BLE001 - retried on the next tick
                log.warning(f"write summary: {attempt.short(e)}")

    def finish(self) -> int:
        try:
            if self.expanded:
                self.write_summary()
        except Exception as e:  # noqa: BLE001
            self.fatal = self.fatal or f"write summary: {attempt.short(e)}"
        if self.fatal:
            return fail(self.settings.termination_log, 1, self.fatal)
        if self.terminated:
            # Non-zero, so a Job whose Pod is evicted does not read as Complete.
            return fail(self.settings.termination_log, 143, "terminated by SIGTERM")
        return 0

    def put(self, name: str, data: bytes, content_type: str) -> None:
        try:
            self.gcs.put(f"jobs/{self.job}/{name}", data, content_type)
        except Exception as e:  # noqa: BLE001
            raise Fatal(f"write {name}: {attempt.short(e)}") from None


def parse_inputs(raw: bytes, count) -> list[dict]:
    items = []
    for n, line in enumerate(raw.decode().splitlines(), 1):
        if not line.strip():
            continue
        try:
            item = json.loads(line)
        except ValueError:
            raise Fatal(f"inputs.jsonl line {n} is not JSON") from None
        if not isinstance(item, dict):
            raise Fatal(f"inputs.jsonl line {n} is not a JSON object")
        items.append(item)
    if count is not None and count != len(items):
        raise Fatal(f"inputs.jsonl has {len(items)} items, spec.json says {count}")
    return items


def fail(termination_log: str, code: int, reason: str) -> int:
    """Log the reason and leave it as the Pod's termination message."""
    log.error(reason)
    try:
        with open(termination_log, "w") as f:
            f.write(reason[:1000] + "\n")
    except OSError as e:
        log.warning(f"write termination log: {e}")
    return code


def main() -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    job_dir = os.environ.get("OP_JOB_DIR", "/op/job")
    try:
        with open(os.path.join(job_dir, "spec.json"), "rb") as f:
            spec = f.read()
        with open(os.path.join(job_dir, "inputs.jsonl"), "rb") as f:
            inputs = f.read()
        gcs = GCS(os.environ["OP_ARTIFACTS_BUCKET"])
        orch = Orchestrator(spec, inputs, os.environ, gcs, None)
    except (OSError, ValueError, KeyError) as e:
        return fail(Settings.termination_log, 1, f"read job: {e!r}")

    # Imported here: the SDK is only needed to run, not to test.
    from sandboxes import Sandboxes

    orch.sandboxes = Sandboxes(orch.job)
    signal.signal(signal.SIGTERM, orch.terminate)
    return orch.run()


if __name__ == "__main__":
    sys.exit(main())
