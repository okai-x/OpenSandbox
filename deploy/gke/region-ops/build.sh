#!/usr/bin/env bash
# Build the sandbox-region-ops image, push it and point the Cloud Run Job at it:
#
#   deploy/gke/region-ops/build.sh
#
# The image holds the committed deploy/gke scripts and the OpenSandbox charts,
# and is tagged with the commit it was built from, so the Job's image names
# exactly what it runs. Changing what the Job does always goes through this
# repository: One Platform can run the Job, never change it.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
PROJECT=pranalab-infra
REGION=asia-southeast1
TAG="$(git -C "$ROOT" rev-parse --short HEAD)"
IMAGE="$REGION-docker.pkg.dev/$PROJECT/opensandbox/region-ops:$TAG"

[[ -z "$(git -C "$ROOT" status --porcelain -- deploy/gke manifests/charts)" ]] \
  || { echo "commit deploy/gke and manifests/charts first: the tag must name what the image holds" >&2; exit 1; }

# Only the committed files the image needs; the charts' built archives are not.
CTX="$(mktemp -d)"
trap 'rm -rf "$CTX"' EXIT
git -C "$ROOT" archive HEAD deploy/gke manifests/charts | tar -x -C "$CTX"

docker build -q --platform linux/amd64 -f "$CTX/deploy/gke/region-ops/Dockerfile" -t "$IMAGE" "$CTX" >/dev/null
docker push -q "$IMAGE" >/dev/null
gcloud run jobs update sandbox-region-ops --region "$REGION" --project "$PROJECT" --image "$IMAGE" \
  --task-timeout 3600s --quiet >/dev/null 2>&1
echo "sandbox-region-ops runs $IMAGE"
