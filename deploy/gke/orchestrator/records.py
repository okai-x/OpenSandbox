"""Records of a sandbox Git job: tasks, result.json checks, task.json and summary.json.

Pure functions over plain dicts; the formats are sections 5-7 of One Platform's
docs/sandbox-git-jobs-design.md.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone

OUTCOMES = {"accepted", "rejected"}
CLASSES = {"quality", "timeout", "infra", "upstream", "budget", "task_error"}
# Classes that count toward the success rate; the others are "notCounted".
COUNTED = {"quality", "timeout"}
MAX_DETAIL = 500


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
    """Return (result, usage) of a result.json document, or raise ValueError/TypeError."""
    if not isinstance(doc, dict):
        raise TypeError("not a JSON object")
    if doc.get("version") != 1:
        raise ValueError("version must be 1")
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
    return {
        "outcome": outcome,
        "class": cls,
        "detail": detail,
        "metrics": metrics,
    }, usage


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
    }


def cost(usage: dict) -> float:
    return float(usage.get("costUsd", 0))


def task_record(job: str, spec_hash: str, region: str, task: Task, run: dict) -> dict:
    """task.json of one attempt; run holds sandbox, timestamps and exit code.
    Values not known yet are null, which One Platform reads as absent."""
    return {
        "version": 1,
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
