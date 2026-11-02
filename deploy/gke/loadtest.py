"""Load test for the self-hosted OpenSandbox deployment on GKE.

Each task mimics one data-synthesis job: create a sandbox of the production
spec, keep all its cores busy for a while, write an artifact, upload it to the
artifacts bucket with sandbox_gcs_upload.py, delete the object and destroy the
sandbox. --concurrency tasks run at once until --total have finished.

Runs in-cluster through the gateway (loadtest-job.yaml, same path as the
orchestrator) or from a workstation through the server proxy
(REGRESS_SERVER_PROXY=true with a port-forward to the server).

Environment: OPENSANDBOX_TEST_DOMAIN, OPENSANDBOX_TEST_API_KEY,
OPENSANDBOX_SANDBOX_DEFAULT_IMAGE, REGRESS_SERVER_PROXY, REGRESS_ARTIFACT_BUCKET.
Prints one JSON line per task and a summary; exits 1 when the failure rate
exceeds --max-failure-rate.
"""

import argparse
import json
import os
import statistics
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

from opensandbox import SandboxSync
from opensandbox.config import ConnectionConfigSync

UPLOADER = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "sandbox_gcs_upload.py"
)
RUN_ID = f"loadtest-{uuid.uuid4().hex[:8]}"

BURN = (
    "import multiprocessing as m, time, sys\n"
    "def f(secs):\n"
    "    t = time.time(); n = 0\n"
    "    while time.time() - t < secs:\n"
    "        n += 1\n"
    "    return n\n"
    "procs, secs = int(sys.argv[1]), float(sys.argv[2])\n"
    "with m.Pool(procs) as p:\n"
    "    print(sum(p.map(f, [secs] * procs)))\n"
)
DELETE = (
    "import json, sys, urllib.parse, urllib.request\n"
    "md = {'Metadata-Flavor': 'Google'}\n"
    "tok = json.load(urllib.request.urlopen(urllib.request.Request(\n"
    "    'http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token', headers=md)))['access_token']\n"
    "bucket, name = sys.argv[1], sys.argv[2]\n"
    "url = f'https://storage.googleapis.com/storage/v1/b/{bucket}/o/' + urllib.parse.quote(name, safe='')\n"
    "req = urllib.request.Request(url, method='DELETE', headers={'Authorization': 'Bearer ' + tok})\n"
    "print(urllib.request.urlopen(req).status)\n"
)


def stdout(execution) -> str:
    return "".join(m.text for m in execution.logs.stdout).strip()


def run_task(i: int, args, config, uploader: str, start_gate: float) -> dict:
    rec = {"i": i, "ok": False, "start_s": round(time.monotonic() - start_gate, 1)}
    t = time.monotonic()
    try:
        sb = SandboxSync.create(
            args.image,
            connection_config=config,
            resource={"cpu": args.cpu, "memory": args.memory},
            metadata={"loadtest-run": RUN_ID},
            timeout=timedelta(minutes=30),
            ready_timeout=timedelta(minutes=6),
        )
    except Exception as e:  # noqa: BLE001 - every failure is a data point
        rec.update(stage="create", error=f"{type(e).__name__}: {str(e)[:200]}")
        rec["create_s"] = round(time.monotonic() - t, 1)
        return rec
    rec["create_s"] = round(time.monotonic() - t, 1)
    rec["sandbox"] = sb.id
    stage = "burn"
    try:
        sb.files.write_file("/tmp/burn.py", BURN)
        sb.files.write_file("/tmp/up.py", uploader)
        sb.files.write_file("/tmp/del.py", DELETE)
        t = time.monotonic()
        ops = int(
            stdout(
                sb.commands.run(
                    f"python3 /tmp/burn.py {args.procs} {args.burn_seconds}"
                )
            )
        )
        rec["burn_s"] = round(time.monotonic() - t, 1)
        rec["mops_per_s"] = round(ops / args.burn_seconds / 1e6, 1)

        stage = "upload"
        name = f"loadtest/{RUN_ID}/{i}.bin"
        t = time.monotonic()
        out = stdout(
            sb.commands.run(
                f"head -c {args.artifact_mb}M /dev/urandom > /tmp/a.bin && "
                f"python3 /tmp/up.py /tmp/a.bin gs://{args.bucket}/{name}"
            )
        )
        up = json.loads(out.splitlines()[-1])
        assert up["size"] == args.artifact_mb << 20, up
        rec["upload_s"] = up["seconds"]
        rec["artifact_s"] = round(time.monotonic() - t, 1)
        status = stdout(sb.commands.run(f"python3 /tmp/del.py {args.bucket} {name}"))
        assert status == "204", f"delete -> {status}"
        rec["ok"] = True
    except Exception as e:  # noqa: BLE001
        rec.update(stage=stage, error=f"{type(e).__name__}: {str(e)[:200]}")
    finally:
        t = time.monotonic()
        try:
            sb.destroy()
        except Exception as e:  # noqa: BLE001
            rec["destroy_error"] = f"{type(e).__name__}: {str(e)[:120]}"
        rec["destroy_s"] = round(time.monotonic() - t, 1)
    return rec


def pct(values: list[float], q: float) -> float | None:
    if not values:
        return None
    values = sorted(values)
    return values[min(len(values) - 1, round(q * (len(values) - 1)))]


def summarize(recs: list[dict], wall: float, args) -> dict:
    ok = [r for r in recs if r["ok"]]
    created = [r["create_s"] for r in recs if "sandbox" in r]
    errors: dict[str, int] = {}
    for r in recs:
        if not r["ok"]:
            key = f"{r.get('stage')}: {r.get('error', '')[:80]}"
            errors[key] = errors.get(key, 0) + 1

    def stats(key):
        vals = [r[key] for r in ok if key in r]
        if not vals:
            return None
        return {
            "p50": pct(vals, 0.5),
            "p95": pct(vals, 0.95),
            "max": max(vals),
            "mean": round(statistics.mean(vals), 1),
        }

    return {
        "run_id": RUN_ID,
        "spec": f"{args.cpu} CPU / {args.memory}",
        "concurrency": args.concurrency,
        "total": len(recs),
        "succeeded": len(ok),
        "failure_rate": round(1 - len(ok) / len(recs), 3) if recs else None,
        "wall_s": round(wall, 1),
        "tasks_per_min": round(len(ok) / wall * 60, 1) if wall else None,
        "create_s": {
            "p50": pct(created, 0.5),
            "p95": pct(created, 0.95),
            "max": max(created) if created else None,
        },
        "cpu_mops_per_s": stats("mops_per_s"),
        "upload_s": stats("upload_s"),
        "errors": errors,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--concurrency", type=int, default=7)
    ap.add_argument("--total", type=int, help="tasks to run (default: concurrency)")
    ap.add_argument("--cpu", default="8")
    ap.add_argument("--memory", default="12Gi")
    ap.add_argument("--procs", type=int, default=8, help="busy processes per sandbox")
    ap.add_argument("--burn-seconds", type=float, default=60)
    ap.add_argument("--artifact-mb", type=int, default=100)
    ap.add_argument("--max-failure-rate", type=float, default=0.0)
    ap.add_argument(
        "--image",
        default=os.environ.get(
            "OPENSANDBOX_SANDBOX_DEFAULT_IMAGE",
            "asia-southeast1-docker.pkg.dev/pranalab-infra/opensandbox/code-interpreter:v1.1.0",
        ),
    )
    ap.add_argument(
        "--bucket",
        default=os.getenv(
            "REGRESS_ARTIFACT_BUCKET", "pranalab-infra-opensandbox-artifacts"
        ),
    )
    args = ap.parse_args()
    total = args.total or args.concurrency

    config = ConnectionConfigSync(
        domain=os.environ["OPENSANDBOX_TEST_DOMAIN"],
        protocol="http",
        api_key=os.environ["OPENSANDBOX_TEST_API_KEY"],
        use_server_proxy=os.getenv("REGRESS_SERVER_PROXY", "false").lower() == "true",
        # Create blocks server-side up to sandbox_create_timeout_seconds (300).
        request_timeout=timedelta(seconds=360),
    )
    with open(UPLOADER) as f:
        uploader = f.read()

    print(
        f"run {RUN_ID}: {total} tasks, concurrency {args.concurrency}, "
        f"{args.cpu} CPU / {args.memory}, burn {args.burn_seconds}s, "
        f"artifact {args.artifact_mb}MiB",
        flush=True,
    )
    lock = threading.Lock()
    recs: list[dict] = []
    start = time.monotonic()

    def one(i):
        rec = run_task(i, args, config, uploader, start)
        with lock:
            recs.append(rec)
            print(json.dumps(rec), flush=True)

    with ThreadPoolExecutor(args.concurrency) as pool:
        list(pool.map(one, range(total)))

    summary = summarize(recs, time.monotonic() - start, args)
    print("SUMMARY " + json.dumps(summary, indent=2), flush=True)
    return 0 if (summary["failure_rate"] or 0) <= args.max_failure_rate else 1


if __name__ == "__main__":
    sys.exit(main())
