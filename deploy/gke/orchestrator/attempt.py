"""One attempt of a task inside a created sandbox.

The command runs detached and is polled, so a multi-hour task does not depend
on one long-lived stream. Logs and /tmp/op/output are uploaded from inside the
sandbox with sandbox_gcs_upload.py (the sandbox KSA can write the artifacts
bucket); the orchestrator itself reads back result.json and, when it declares
output files, lists the uploaded output to check them.
"""

import json
import re
import shlex
import threading
import time
from dataclasses import dataclass

from records import MAX_FILES, validate_outputs, validate_result, verdict

MAX_RESULT = 1 << 20
SHA256 = re.compile(r"[0-9a-f]{64}")
# How much of stderr's end a missing or invalid result's detail quotes; the
# detail itself is cut at 500 characters by validate_result's contract.
STDERR_TAIL = 300

# Uploads the attempt's logs and output, then prints result.json (if any) as
# the last stdout line, with the size and sha256 of each file it declares
# ("declared"). Runs with the image's python3. Each upload prints a line, which
# keeps the command's output stream from hitting the SDK's read timeout while a
# large output directory uploads.
COLLECT = f"""\
import hashlib, json, os, stat, subprocess, sys
dest, output = sys.argv[1], "/tmp/op/output"
def up(path, name):
    subprocess.run([sys.executable, "/tmp/op/upload.py", path, dest + "/" + name], check=True)
def describe(path):
    # Size and sha256 in one read; a symlink is never followed.
    full = os.path.join(output, path)
    if path.startswith("/") or ".." in path.split("/"):
        return {{"error": "is outside the output directory"}}
    try:
        if os.path.realpath(full) != os.path.join(os.path.realpath(output), path):
            return {{"error": "is a symlink or under one"}}
        if not stat.S_ISREG(os.lstat(full).st_mode):
            return {{"error": "is not a regular file"}}
        digest, size = hashlib.sha256(), 0
        with open(full, "rb") as f:
            for block in iter(lambda: f.read(1 << 20), b""):
                digest.update(block)
                size += len(block)
        return {{"size": size, "sha256": digest.hexdigest()}}
    except FileNotFoundError:
        return {{"error": "does not exist"}}
    except (OSError, ValueError) as e:
        return {{"error": type(e).__name__ + ": " + str(e)[:200]}}
def declared(text):
    # The orchestrator validates result.json; here a bad one only means no report.
    try:
        files = json.loads(text).get("files")
    except (ValueError, RecursionError, AttributeError):
        return {{}}
    if not isinstance(files, list) or len(files) > {MAX_FILES}:
        return {{}}
    entries = [f for f in files if isinstance(f, dict)]
    paths = {{f["path"] for f in entries if isinstance(f.get("path"), str)}}
    return {{p: describe(p) for p in sorted(paths)}}
for log in ("stdout.log", "stderr.log"):
    up("/tmp/op/logs/" + log, log)
for root, _, files in os.walk("/tmp/op/output"):
    for f in sorted(files):
        path = os.path.join(root, f)
        if os.path.isfile(path):
            up(path, "output/" + os.path.relpath(path, "/tmp/op/output"))
res = "/tmp/op/output/result.json"
out = {{"size": -1, "text": None, "stderrTail": "", "declared": {{}}}}
with open("/tmp/op/logs/stderr.log", "rb") as f:
    f.seek(max(os.path.getsize("/tmp/op/logs/stderr.log") - 4096, 0))
    lines = [l for l in f.read().decode("utf-8", "replace").splitlines() if l.strip()]
out["stderrTail"] = " | ".join(lines[-3:])[-{STDERR_TAIL}:]
if os.path.isfile(res):
    out["size"] = os.path.getsize(res)
    if out["size"] <= {MAX_RESULT}:
        with open(res, encoding="utf-8", errors="replace") as f:
            out["text"] = f.read()
        out["declared"] = declared(out["text"])
print(json.dumps(out))
"""


class SandboxLost(Exception):
    """The sandbox stopped answering: an infra failure worth a new attempt."""


class Stopped(Exception):
    """The job is being terminated."""


class SetupFailed(Exception):
    """A setup step exited non-zero: the image lacks what the task needs."""


@dataclass
class Context:
    bucket: str
    command: list[str]
    timeout_s: float
    source: bytes
    uploader: str
    gcs: object  # lists the uploaded output; gcs.GCS
    poll_s: float = 10
    lost_after: int = 6  # consecutive failed status polls


def run(
    sandboxes,
    sb,
    ctx: Context,
    item: dict,
    envs: dict,
    prefix: str,
    stop: threading.Event,
) -> dict:
    """Return {"exitCode", "result", "usage"} and, when the result is the task's
    own, its "record" and verified "files"; raise SandboxLost or Stopped."""
    try:
        prepare(sandboxes, sb, ctx, item)
        execution = sandboxes.start(sb, command_line(ctx.command), envs)
    except SetupFailed as e:
        return {"exitCode": None, "result": verdict("task_error", str(e)), "usage": {}}
    except Exception as e:  # noqa: BLE001 - any SDK or transport error
        raise SandboxLost(f"prepare sandbox: {short(e)}") from None
    exit_code, timed_out = wait(sandboxes, sb, ctx, execution, stop)
    out = {"exitCode": exit_code, "usage": {}}
    if timed_out:
        out["result"] = verdict(
            "timeout", f"command ran longer than {ctx.timeout_s / 60:g} minutes"
        )
    try:
        code, stdout = sandboxes.exec(
            sb, f"python3 /tmp/op/collect.py gs://{ctx.bucket}/{prefix}"
        )
    except Exception as e:  # noqa: BLE001
        if timed_out:  # never run a timed-out task again
            return out
        raise SandboxLost(f"collect output: {short(e)}") from None
    if code != 0:
        # Not retried: the command already ran, and running it again costs as much.
        out.setdefault("result", verdict("infra", f"upload output: exit {code}"))
        return out
    report = last_line(stdout)
    judged = judge(report, exit_code)
    if timed_out:
        out["usage"] = judged["usage"]
        return out
    verified = verify(ctx.gcs, prefix, judged, report.get("declared"))
    return {"exitCode": exit_code, **verified}


def prepare(sandboxes, sb, ctx: Context, item: dict) -> None:
    check(sandboxes.exec(sb, "mkdir -p /tmp/op/src /tmp/op/output /tmp/op/logs"), "create /tmp/op")
    sandboxes.write(sb, "/tmp/op/src.tar.gz", ctx.source)
    sandboxes.write(sb, "/tmp/op/input.json", json.dumps(item))
    sandboxes.write(sb, "/tmp/op/upload.py", ctx.uploader)
    sandboxes.write(sb, "/tmp/op/collect.py", COLLECT)
    check(
        sandboxes.exec(sb, "tar -xzf /tmp/op/src.tar.gz -C /tmp/op/src && rm /tmp/op/src.tar.gz"),
        "unpack source",
    )


def check(res: tuple, step: str) -> None:
    if res[0] != 0:
        raise SetupFailed(f"{step}: exit {res[0]}")


def command_line(command: list[str]) -> str:
    return f"exec >/tmp/op/logs/stdout.log 2>/tmp/op/logs/stderr.log </dev/null; cd /tmp/op/src && exec {shlex.join(command)}"


def wait(sandboxes, sb, ctx: Context, execution: str, stop: threading.Event) -> tuple:
    """Poll until the command exits or times out; return (exit code, timed out)."""
    deadline = time.monotonic() + ctx.timeout_s
    failures = 0
    while True:
        if stop.wait(min(ctx.poll_s, max(0.0, deadline - time.monotonic()))):
            raise Stopped("the job was terminated")
        try:
            running, code = sandboxes.status(sb, execution)
            failures = 0
        except Exception as e:  # noqa: BLE001
            failures += 1
            if failures >= ctx.lost_after:
                raise SandboxLost(f"command status: {short(e)}") from None
            continue
        if not running:
            return code, False
        if time.monotonic() >= deadline:
            return interrupt(sandboxes, sb, execution), True


def interrupt(sandboxes, sb, execution: str):
    try:
        sandboxes.interrupt(sb, execution)
        return sandboxes.status(sb, execution)[1]
    except Exception:  # noqa: BLE001 - the sandbox is deleted next anyway
        return None


def judge(out: dict, exit_code=None) -> dict:
    """The task's {"result", "usage", "record", "files"}; when the task gave none,
    why, with its exit code and the end of its stderr, so the cause shows
    without opening the logs."""
    if out.get("size", -1) < 0:
        return task_error(why("result.json is missing", out, exit_code))
    if out.get("text") is None:
        reason = f"result.json is larger than {MAX_RESULT} bytes"
        return task_error(why(reason, out, exit_code))
    try:
        doc = json.loads(out["text"])
        result, usage = validate_result(doc)
        record, files = validate_outputs(doc)
    # json.JSONDecodeError is a ValueError; deep nesting raises RecursionError.
    except (TypeError, ValueError, RecursionError) as e:
        return task_error(why(f"result.json is invalid: {e}", out, exit_code))
    return {"result": result, "usage": usage, "record": record, "files": files}


def task_error(detail: str, usage: dict | None = None) -> dict:
    result = verdict("task_error", detail)
    return {"result": result, "usage": usage or {}, "record": {}, "files": []}


def verify(gcs, prefix: str, judged: dict, declared) -> dict:
    """judged, with each declared file checked against the sandbox's report and
    the uploaded object, and its location added. A file that does not check
    out makes the attempt a task_error; its usage still counts."""
    if not judged["files"]:
        return judged
    try:
        listed = {o["name"]: o for o in gcs.list(f"{prefix}/output/")}
    except Exception as e:  # noqa: BLE001 - not retried: the command already ran
        result = verdict("infra", f"list output: {short(e)}")
        return {**judged, "result": result, "record": {}, "files": []}
    declared = declared if isinstance(declared, dict) else {}
    try:
        files = [
            located(
                gcs.bucket,
                f,
                declared.get(f["path"]),
                listed.get(f"{prefix}/output/{f['path']}"),
            )
            for f in judged["files"]
        ]
    except ValueError as e:
        return task_error(str(e), judged["usage"])
    return {**judged, "files": files}


def located(bucket: str, f: dict, report, obj) -> dict:
    """The task.json entry of declared file f; raise ValueError when the
    sandbox's report or the uploaded object does not match it."""
    problem = file_problem(report, obj)
    if problem:
        raise ValueError(f"declared file {f['path']}: {problem}")
    return {
        **f,
        "size": report["size"],
        "sha256": report["sha256"],
        "uri": f"gs://{bucket}/{obj['name']}",
        "generation": obj["generation"],
        "md5": obj.get("md5Hash"),  # a composite object (a large upload) has none
    }


def file_problem(report, obj) -> str:
    if not isinstance(report, dict):
        return "the sandbox did not report it"
    if "error" in report:
        return str(report["error"])[:200]
    size, sha256 = report.get("size"), report.get("sha256")
    if not isinstance(size, int) or not SHA256.fullmatch(str(sha256)):
        return "the sandbox's report is malformed"
    if obj is None:
        return "was not uploaded"
    if int(obj["size"]) != size:
        return f"the sandbox read {size} bytes but the bucket holds {obj['size']}"
    return ""


def why(reason: str, out: dict, exit_code) -> str:
    detail = reason if exit_code is None else f"{reason}; exit {exit_code}"
    tail = out.get("stderrTail") or ""
    return f"{detail}; stderr: {tail}"[:500] if tail else detail


def last_line(stdout: str) -> dict:
    """COLLECT's report: its last line as a JSON object, or {} when it is not one."""
    lines = stdout.strip().splitlines()
    try:
        report = json.loads(lines[-1]) if lines else {}
    except (ValueError, RecursionError):
        return {}
    return report if isinstance(report, dict) else {}


def short(e: Exception) -> str:
    return f"{type(e).__name__}: {str(e)[:300]}"
