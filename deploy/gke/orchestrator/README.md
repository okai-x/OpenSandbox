# Sandbox Git job orchestrator

The standard orchestrator of One Platform's sandbox Git jobs. One Platform runs it as Job `opjob-<job>` in namespace `opensandbox-jobs` of a region's cluster; it runs one sandbox per task and keeps the job's records in the region's artifacts bucket. The contract (job spec, `result.json`, `task.json`, `summary.json`) is One Platform's `docs/sandbox-git-jobs-design.md`, sections 3–7.

## What it does

1. Reads `$OP_JOB_DIR/spec.json` and `inputs.jsonl` and writes them to `gs://$OP_ARTIFACTS_BUCKET/jobs/<job>/`.
2. Downloads `source.repo` at `source.commit` from GitHub with `OP_GITHUB_TOKEN`, keeps only `source.path` and writes it as `jobs/<job>/source.tar.gz`.
3. Runs one sandbox per task `(index, sample)`, at most `concurrency` at a time, labeled `one-platform.io/job=<job>`. A slot is reused only after the previous sandbox is gone.
4. In each sandbox: unpacks the source into `/tmp/op/src`, writes the item to `/tmp/op/input.json`, and runs `command` detached in `/tmp/op/src` with `variables`, the job's Secrets and the `OP_*` variables in its environment. After `timeoutMinutes` the command is interrupted and the task is a `timeout`.
5. From inside the sandbox, uploads `stdout.log`, `stderr.log` and `/tmp/op/output/` to `jobs/<job>/tasks/<index>/<sample>/attempt-<n>/` with `sandbox_gcs_upload.py`, then checks `result.json`; a missing or invalid one makes the task a `task_error`.
6. A failed sandbox create or a sandbox that stops answering is an `infra` failure and is tried again in a new `attempt-<n>` directory, up to 3 attempts.
7. Stops starting tasks once the reported `usage.costUsd` reaches `budgetUsd`; running tasks finish.
8. Rewrites `jobs/<job>/summary.json` every 10 s and at the end.

It exits 0 when every task ended or the budget stopped new tasks. When it cannot continue (a missing Secret, a failed download, a bucket it cannot write) it writes one line to `/dev/termination-log` and exits 1. On SIGTERM it starts no task, deletes the job's sandboxes, writes a final summary with state `stopped` and exits 143, so an evicted Pod never reads as a completed Job.

Secret values only reach the orchestrator's environment and the command's environment; they are never written to a record or a log.

## Environment

| Variable | Source |
| --- | --- |
| `OP_JOB_DIR` | `/op/job`, the job's ConfigMap |
| `OP_ARTIFACTS_BUCKET` | the region's artifacts bucket |
| `OPEN_SANDBOX_DOMAIN` | the OpenSandbox server, e.g. `opensandbox-server.opensandbox-system.svc.cluster.local:80` |
| `OPEN_SANDBOX_API_KEY` | Secret `opensandbox-api-key`, key `value` |
| `OP_GITHUB_TOKEN` | Secret `github-read-token`, key `value` |
| `OP_SECRET_<NAME>` | for each entry `<NAME>: <secret>` of the spec's `secrets`, Secret `<secret>`, key `value`; the command sees it as `<NAME>`. One Platform accepts only Secrets named `task-…`, so a job cannot hand the orchestrator's own Secrets to its tasks |

The Pod runs as KSA `sandbox-orchestrator` in `opensandbox-jobs`, which Workload Identity maps to a principal with `roles/storage.objectAdmin` on the artifacts bucket (`prepare-jobs.sh`). Sandboxes upload with their own KSA, `opensandbox/default`.

## Task images

The orchestrator runs `mkdir`, `tar` and `python3` (3.10 or later) in the task image, and everything a task gets is under `/tmp/op`, which any user can create.

## Tests

```bash
python3 -m unittest discover -s deploy/gke/orchestrator -p '*_test.py'
```

The tests use fakes of the SDK adapter (`sandboxes.py`) and of GCS, and need neither a cluster nor the SDK.

## Image

```bash
deploy/gke/orchestrator/build.sh            # pushes .../opensandbox/sandbox-orchestrator:<commit>
deploy/gke/sync-images.sh deploy/gke/regions/<region>.env   # for each region
```

Then set One Platform's `ONE_PLATFORM_SANDBOX_ORCHESTRATOR_IMAGE` to `sandbox-orchestrator:<commit>`.
