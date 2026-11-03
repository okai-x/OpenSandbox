#!/usr/bin/env bash
# Build the sandbox-job orchestrator image and push it to the source repository:
#
#   deploy/gke/orchestrator/build.sh
#
# The image is tagged with the commit it was built from, so a job's
# orchestrator always names the code it runs. Regions pull their own copy:
# run deploy/gke/sync-images.sh for each region afterwards, then set One
# Platform's ONE_PLATFORM_SANDBOX_ORCHESTRATOR_IMAGE to sandbox-orchestrator:<tag>.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
TAG="$(git -C "$ROOT" rev-parse --short HEAD)"
IMAGE="asia-southeast1-docker.pkg.dev/pranalab-infra/opensandbox/sandbox-orchestrator:$TAG"

[[ -z "$(git -C "$ROOT" status --porcelain -- deploy/gke sdks/sandbox/python)" ]] \
  || { echo "commit deploy/gke and sdks/sandbox/python first: the tag must name what the image holds" >&2; exit 1; }

CTX="$(mktemp -d)"
trap 'rm -rf "$CTX"' EXIT
git -C "$ROOT" archive HEAD deploy/gke/orchestrator deploy/gke/sandbox_gcs_upload.py | tar -x -C "$CTX"
# hatch-vcs needs git metadata, so the SDK wheel is built here, not inside docker.
uv build -q --wheel --project "$ROOT/sdks/sandbox/python" -o "$CTX/dist"

docker build -q --platform linux/amd64 -f "$CTX/deploy/gke/orchestrator/Dockerfile" -t "$IMAGE" "$CTX" >/dev/null
docker push -q "$IMAGE" >/dev/null
echo "pushed $IMAGE"
