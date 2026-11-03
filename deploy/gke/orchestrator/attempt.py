"""One attempt of a task inside a created sandbox.

The command runs detached and is polled, so a multi-hour task does not depend
on one long-lived stream. Logs and /tmp/op/output are uploaded from inside the
sandbox with sandbox_gcs_upload.py (the sandbox KSA can write the artifacts
bucket); the orchestrator itself only reads back result.json.
"""

import json
import shlex
import threading
import time
from dataclasses import dataclass

from records import validate_result, verdict

MAX_RESULT = 64 << 10
# How much of stderr's end a missing or invalid result's detail quotes; the
# detail itself is cut at 500 characters by validate_result's contract.
STDERR_TAIL = 300

# Uploads the attempt's logs and output, then prints result.json (if any) as
# the last stdout line. Runs with the image's python3. Each upload prints a
# line, which keeps the command's output stream from hitting the SDK's read
# timeout while a large output directory uploads.
COLLECT = f"""\
import json, os, subprocess, sys
dest = sys.argv[1]
def up(path, name):
    subprocess.run([sys.executable, "/tmp/op/upload.py", path, dest + "/" + name], check=True)
for log in ("stdout.log", "stderr.log"):
    up("/tmp/op/logs/" + log, log)
for root, _, files in os.walk("/tmp/op/output"):
    for f in sorted(files):
        path = os.path.join(root, f)
        if os.path.isfile(path):
            up(path, "output/" + os.path.relpath(path, "/tmp/op/output"))
res, out = "/tmp/op/output/result.json", {{"size": -1, "text": None, "stderrTail": ""}}
with open("/tmp/op/logs/stderr.log", "rb") as f:
    f.seek(max(os.path.getsize("/tmp/op/logs/stderr.log") - 4096, 0))
    lines = [l for l in f.read().decode("utf-8", "replace").splitlines() if l.strip()]
out["stderrTail"] = " | ".join(lines[-3:])[-{STDERR_TAIL}:]
if os.path.isfile(res):
    out["size"] = os.path.getsize(res)
    if out["size"] <= {MAX_RESULT}:
        with open(res, encoding="utf-8", errors="replace") as f:
            out["text"] = f.read()
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
    """Return {"exitCode", "result", "usage"}; raise SandboxLost or Stopped."""
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
    result, out["usage"] = judge(last_line(stdout), exit_code)
    out.setdefault("result", result)
    return out


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


def judge(out: dict, exit_code=None) -> tuple[dict, dict]:
    """The task's result; when the task gave none, why, with its exit code
    and the end of its stderr, so the cause shows without opening the logs."""
    if out.get("size", -1) < 0:
        return verdict("task_error", why("result.json is missing", out, exit_code)), {}
    if out.get("text") is None:
        reason = f"result.json is larger than {MAX_RESULT} bytes"
        return verdict("task_error", why(reason, out, exit_code)), {}
    try:
        return validate_result(json.loads(out["text"]))
    except (TypeError, ValueError) as e:  # json.JSONDecodeError is a ValueError
        return verdict("task_error", why(f"result.json is invalid: {e}", out, exit_code)), {}


def why(reason: str, out: dict, exit_code) -> str:
    detail = reason if exit_code is None else f"{reason}; exit {exit_code}"
    tail = out.get("stderrTail") or ""
    return f"{detail}; stderr: {tail}"[:500] if tail else detail


def last_line(stdout: str) -> dict:
    lines = stdout.strip().splitlines()
    try:
        return json.loads(lines[-1]) if lines else {}
    except ValueError:
        return {}


def short(e: Exception) -> str:
    return f"{type(e).__name__}: {str(e)[:300]}"
