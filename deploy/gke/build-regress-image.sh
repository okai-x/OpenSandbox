#!/usr/bin/env bash
# Build the in-cluster regression runner image and push it to Artifact Registry.
#   deploy/gke/build-regress-image.sh
# Rebuild after changing regress.py, loadtest.py, sandbox_gcs_upload.py or the Python SDK.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
IMAGE="${REGRESS_IMAGE:-asia-southeast1-docker.pkg.dev/pranalab-infra/opensandbox/regress:latest}"
CTX_DIR="$(mktemp -d)"
trap 'rm -rf "$CTX_DIR"' EXIT

# hatch-vcs needs git metadata, so the wheel is built here, not inside docker.
uv build -q --wheel --project "$ROOT/sdks/sandbox/python" -o "$CTX_DIR/dist"
cp "$ROOT/deploy/gke/regress.py" "$ROOT/deploy/gke/loadtest.py" "$ROOT/deploy/gke/sandbox_gcs_upload.py" "$CTX_DIR/"

docker build -q --platform linux/amd64 -f "$ROOT/deploy/gke/regress.Dockerfile" -t "$IMAGE" "$CTX_DIR"
docker push -q "$IMAGE"
