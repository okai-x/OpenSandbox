# Sandbox Git job orchestrator

The standard orchestrator of One Platform's sandbox Git jobs. One Platform runs it as Job `opjob-<job>` in namespace `opensandbox-jobs` of a region's cluster; it runs one sandbox per task and keeps the job's records in the region's artifacts bucket. The contract (job spec, `result.json`, `task.json`, `summary.json`) is One Platform's `docs/sandbox-git-jobs-design.md`, sections 3–7; version 2 of `result.json` and `task.json`, below, is section 4 of its `docs/data-lineage-design.md`, which also describes how One Platform builds dataset versions from them.

## What it does

1. Reads `$OP_JOB_DIR/spec.json` and `inputs.jsonl` and writes them to `gs://$OP_ARTIFACTS_BUCKET/jobs/<job>/`.
2. Downloads `source.repo` at `source.commit` from GitHub with `OP_GITHUB_TOKEN`, keeps only `source.path` and writes it as `jobs/<job>/source.tar.gz`.
3. Runs one sandbox per task `(index, sample)`, at most `concurrency` at a time, labeled `one-platform.io/job=<job>`. A slot is reused only after the previous sandbox is gone. Each sandbox gets `sandbox.cpu` and `sandbox.memoryGiB` as its `resourceLimits` and `sandbox.cpuRequest` and `sandbox.memoryRequestGiB` as its `resourceRequests` (in whole millicores and MiB), so it runs Burstable; a spec without requests, or with 0, requests its limits.
4. In each sandbox: unpacks the source into `/tmp/op/src`, writes the item to `/tmp/op/input.json`, and runs `command` detached in `/tmp/op/src` with `variables`, the job's Secrets and the `OP_*` variables in its environment. After `timeoutMinutes` the command is interrupted and the task is a `timeout`.
5. From inside the sandbox, uploads `stdout.log`, `stderr.log` and `/tmp/op/output/` to `jobs/<job>/tasks/<index>/<sample>/attempt-<n>/` with `sandbox_gcs_upload.py`, then checks `result.json`; a missing or invalid one makes the task a `task_error`. Files a version 2 result declares are checked against the uploaded objects (see below). The attempt's `task.json` (version 2) is written when it starts and when it ends.
6. A failed sandbox create or a sandbox that stops answering is an `infra` failure and is tried again in a new `attempt-<n>` directory, up to 3 attempts.
7. Stops starting tasks once the reported `usage.costUsd` reaches `budgetUsd`; running tasks finish.
8. Rewrites `jobs/<job>/summary.json` every 10 s and at the end.

It exits 0 when every task ended or the budget stopped new tasks. When it cannot continue (a missing Secret, a failed download, a bucket it cannot write) it writes one line to `/dev/termination-log` and exits 1. On SIGTERM it starts no task, deletes the job's sandboxes, writes a final summary with state `stopped` and exits 143, so an evicted Pod never reads as a completed Job.

## result.json and task.json version 2

A task writes `/tmp/op/output/result.json`, at most 1 MiB. Version 1 (`version`, `outcome`, `class`, `detail`, `metrics`, `usage`) is still accepted and declares nothing more. Version 2 adds three optional fields:

| Field | Rules |
| --- | --- |
| `record` | a JSON object, default `{}`, at most 64 KiB as compact JSON |
| `files` | at most 1000 `{path, role}`; `path` is relative to `/tmp/op/output`, at most 1024 bytes of UTF-8, normalized (no `..`, `.` or empty segment, no leading `/`, no backslash), unique, and not `result.json`; `role` is 1–64 characters |
| `checks` | at most 200 `{id, pass, value?, detail?}`; `id` matches `^[A-Za-z0-9._:-]{1,128}$` and is unique, `pass` is a boolean, `value` a finite number, `detail` at most 500 characters |

Breaking a rule makes the attempt a `task_error` whose detail names the first problem. Only declared files count as the task's output; anything else in `/tmp/op/output` is uploaded but is not part of the record.

Each declared file must be a regular file and no symlink or under one: the sandbox reads it once for its size and sha256. It must then be in the bucket at `<attempt>/output/<path>` with the same size, which the orchestrator checks by listing `<attempt>/output/`. Otherwise the attempt is a `task_error` (`declared file <path>: …`) and its usage still counts.

`task.json` version 2 is version 1 plus `result.checks`, a top-level `record` and a top-level `files`, whose entries are `{path, role, size, sha256, uri, generation, md5}`: `sha256` comes from the sandbox, `uri`, `generation` and `md5` (GCS's base64 `md5Hash`) from the listing. A file large enough for `sandbox_gcs_upload.py` to compose (128 MiB or more) is a composite object without an MD5, so its `md5` is `null`. `record` and `files` are empty, and `checks` is `[]`, unless the result is the task's own: a `timeout`, `infra` or orchestrator `task_error` declares nothing. `summary.json` is unchanged.

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
