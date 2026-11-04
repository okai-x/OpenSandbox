"""Records of a sandbox Git job: tasks, result.json checks, task.json and summary.json.

Pure functions over plain dicts; the formats are sections 5-7 of One Platform's
docs/sandbox-git-jobs-design.md. Version 2 of result.json and task.json, which
add a task's record, output files and checks, are section 4 of its
docs/data-lineage-design.md.
"""

import json
import math
import posixpath
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone

OUTCOMES = {"accepted", "rejected"}
CLASSES = {"quality", "timeout", "infra", "upstream", "budget", "task_error"}
# Classes that count toward the success rate; the others are "notCounted".
COUNTED = {"quality", "timeout"}
MAX_DETAIL = 500
MAX_RECORD = 64 << 10  # bytes of the record's compact JSON
MAX_FILES = 1000
MAX_PATH = 1024  # bytes of a file path in UTF-8
MAX_ROLE = 64
MAX_CHECKS = 200
CHECK_ID = re.compile(r"[A-Za-z0-9._:-]{1,128}")


def now() -> str:
    return (
        datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    )


@dataclass
class Task:
    index: int
    sample: int
    item: dict
    item_id: str
    # Latest attempt; status is queued until the first attempt starts.
    status: str = "queued"
    attempt: int = 0
    result: dict = field(default_factory=dict)
    usage: dict = field(default_factory=dict)
    record: dict = field(default_factory=dict)
    files: list = field(default_factory=list)
    started: float = 0.0
    finished: float = 0.0


def expand_tasks(items: list[dict], id_field: str, samples: int) -> list[Task]:
    """One task per (index, sample), in inputs.jsonl order then sample order."""
    return [
        Task(index=i, sample=s, item=item, item_id=str(item.get(id_field, "")))
        for i, item in enumerate(items)
        for s in range(samples)
    ]


def is_number(v) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def validate_result(doc) -> tuple[dict, dict]:
    """Return (result, usage) of a result.json document, or raise ValueError/TypeError.
    The result holds the task's checks; version 1 has none."""
    if not isinstance(doc, dict):
        raise TypeError("not a JSON object")
    if doc.get("version") not in (1, 2):
        raise ValueError("version must be 1 or 2")
    outcome, cls = doc.get("outcome"), doc.get("class")
    if outcome not in OUTCOMES:
        raise ValueError(f"outcome must be one of {sorted(OUTCOMES)}")
    if cls not in CLASSES:
        raise ValueError(f"class must be one of {sorted(CLASSES)}")
    if outcome == "accepted" and cls != "quality":
        raise ValueError("an accepted result must have class quality")
    detail = doc.get("detail", "")
    if not isinstance(detail, str) or len(detail) > MAX_DETAIL:
        raise ValueError(f"detail must be a string of at most {MAX_DETAIL} characters")
    metrics = doc.get("metrics", {})
    if not isinstance(metrics, dict) or not all(
        isinstance(k, str) and is_number(v) for k, v in metrics.items()
    ):
        raise ValueError("metrics must map strings to numbers")
    usage = validate_usage(doc.get("usage", {}))
    checks = validate_checks(doc.get("checks", [])) if doc["version"] == 2 else []
    return {
        "outcome": outcome,
        "class": cls,
        "detail": detail,
        "metrics": metrics,
        "checks": checks,
    }, usage


def validate_outputs(doc: dict) -> tuple[dict, list]:
    """Return (record, files) a valid result.json declares; version 1 declares none."""
    if doc["version"] == 1:
        return {}, []
    return validate_record(doc.get("record", {})), validate_files(doc.get("files", []))


def validate_record(record) -> dict:
    if not isinstance(record, dict):
        raise TypeError("record must be an object")
    # allow_nan=False: NaN and Infinity are not JSON; One Platform cannot read them.
    encoded = json.dumps(
        record, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    )
    if len(encoded.encode()) > MAX_RECORD:
        raise ValueError(f"record is larger than {MAX_RECORD} bytes as compact JSON")
    return record


def validate_files(files) -> list:
    """[{path, role}] of the declared output files, paths relative to /tmp/op/output."""
    if not isinstance(files, list):
        raise TypeError("files must be a list")
    if len(files) > MAX_FILES:
        raise ValueError(f"files has more than {MAX_FILES} entries")
    out, seen = [], set()
    for i, f in enumerate(files):
        if not isinstance(f, dict):
            raise TypeError(f"files[{i}] must be an object")
        path, role = f.get("path"), f.get("role")
        problem = path_problem(path)
        if problem:
            raise ValueError(f"files[{i}].path {problem}")
        if path in seen:
            raise ValueError(f"files[{i}].path {path} is listed twice")
        if not (isinstance(role, str) and 1 <= len(role) <= MAX_ROLE):
            raise ValueError(
                f"files[{i}].role must be a string of 1 to {MAX_ROLE} characters"
            )
        seen.add(path)
        out.append({"path": path, "role": role})
    return out


def path_problem(path) -> str:
    """Why path is not a normalized path inside /tmp/op/output, or "" when it is."""
    if not isinstance(path, str) or not path:
        return "must be a non-empty string"
    try:
        size = len(path.encode())
    except UnicodeEncodeError:
        return "is not valid UTF-8"
    if size > MAX_PATH:
        return f"is longer than {MAX_PATH} bytes"
    if path.startswith("/"):
        return "must be relative to /tmp/op/output"
    if "\\" in path:
        return "must not contain a backslash"
    segments = path.split("/")
    if ".." in segments:
        return "must not contain a .. segment"
    if "" in segments:
        return "must not contain an empty segment"
    # normpath keeps a lone "." as it is.
    if "." in segments or posixpath.normpath(path) != path:
        return "must be normalized"
    if path == "result.json":
        return "must not be result.json"
    return ""


def validate_checks(checks) -> list:
    if not isinstance(checks, list):
        raise TypeError("checks must be a list")
    if len(checks) > MAX_CHECKS:
        raise ValueError(f"checks has more than {MAX_CHECKS} entries")
    out, seen = [], set()
    for i, c in enumerate(checks):
        entry = validate_check(i, c)
        if entry["id"] in seen:
            raise ValueError(f"checks[{i}].id {entry['id']} is listed twice")
        seen.add(entry["id"])
        out.append(entry)
    return out


def validate_check(i: int, c) -> dict:
    if not isinstance(c, dict):
        raise TypeError(f"checks[{i}] must be an object")
    cid = c.get("id")
    if not (isinstance(cid, str) and CHECK_ID.fullmatch(cid)):
        raise ValueError(f"checks[{i}].id must match {CHECK_ID.pattern}")
    if not isinstance(c.get("pass"), bool):
        raise ValueError(f"checks[{i}].pass must be a boolean")
    entry = {"id": cid, "pass": c["pass"]}
    if "value" in c:
        if not (is_number(c["value"]) and math.isfinite(c["value"])):
            raise ValueError(f"checks[{i}].value must be a finite number")
        entry["value"] = c["value"]
    if "detail" in c:
        if not (isinstance(c["detail"], str) and len(c["detail"]) <= MAX_DETAIL):
            raise ValueError(
                f"checks[{i}].detail must be a string of at most {MAX_DETAIL} characters"
            )
        entry["detail"] = c["detail"]
    return entry


def validate_usage(usage) -> dict:
    if not isinstance(usage, dict):
        raise TypeError("usage must be an object")
    out = {}
    if "costUsd" in usage:
        if not (is_number(usage["costUsd"]) and usage["costUsd"] >= 0):
            raise ValueError("usage.costUsd must be a non-negative number")
        out["costUsd"] = usage["costUsd"]
    if "tokens" in usage:
        tokens = usage["tokens"]
        if not (is_number(tokens) and tokens >= 0 and float(tokens).is_integer()):
            raise ValueError("usage.tokens must be a non-negative whole number")
        out["tokens"] = int(tokens)  # One Platform reads it as an integer
    if "complete" in usage:
        if not isinstance(usage["complete"], bool):
            raise ValueError("usage.complete must be a boolean")
        out["complete"] = usage["complete"]
    return out


def verdict(cls: str, detail: str) -> dict:
    """A result the orchestrator decides itself (timeout, task_error, infra)."""
    return {
        "outcome": "rejected",
        "class": cls,
        "detail": detail[:MAX_DETAIL],
        "metrics": {},
        "checks": [],
    }


def cost(usage: dict) -> float:
    return float(usage.get("costUsd", 0))


def task_record(job: str, spec_hash: str, region: str, task: Task, run: dict) -> dict:
    """task.json (version 2) of one attempt; run holds sandbox, timestamps and
    exit code. Values not known yet are null, which One Platform reads as absent.
    record and files are what the task declared and the orchestrator verified,
    and stay empty unless the result is the task's own."""
    return {
        "version": 2,
        "task": f"{job}/{task.index}/{task.sample}",
        "attempt": task.attempt,
        "specHash": spec_hash,
        "item": {"id": task.item_id, "index": task.index},
        "sample": task.sample,
        "sandbox": {"id": run.get("sandbox", ""), "region": region},
        "status": task.status,
        "startedAt": run.get("startedAt"),
        "finishedAt": run.get("finishedAt"),
        "exitCode": run.get("exitCode"),
        "result": task.result or None,
        "usage": task.usage or None,
        "record": task.record,
        "files": task.files,
    }


def summary(
    job: str, state: str, tasks: list[Task], spent: float, clock: float
) -> dict:
    counts = {
        "total": len(tasks),
        "queued": 0,
        "running": 0,
        "accepted": 0,
        "rejected": 0,
        "notCounted": 0,
    }
    for t in tasks:
        counts[bucket(t)] += 1
    finished = [t for t in tasks if t.status == "finished"]
    return {
        "version": 1,
        "job": job,
        "updatedAt": now(),
        "state": state,
        "counts": counts,
        "spentUsd": round(spent, 6),
        # True only when every finished task reported complete usage.
        "usageComplete": all(t.usage.get("complete") is True for t in finished),
        "tasks": [row(t, clock) for t in tasks],
    }


def bucket(t: Task) -> str:
    if t.status != "finished":
        return t.status
    if t.result.get("class") not in COUNTED:
        return "notCounted"
    return t.result["outcome"]


def row(t: Task, clock: float) -> dict:
    end = t.finished if t.status == "finished" else clock
    return {
        "index": t.index,
        "sample": t.sample,
        "id": t.item_id,
        "status": t.status,
        "attempt": t.attempt,
        "outcome": t.result.get("outcome", ""),
        "class": t.result.get("class", ""),
        "detail": t.result.get("detail", ""),
        "costUsd": cost(t.usage),
        "seconds": round(end - t.started) if t.started else 0,
    }
