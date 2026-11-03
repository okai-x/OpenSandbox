#!/usr/bin/env bash
# Build the sandbox-region-ops image, push it and point the Cloud Run Job at it:
#
#   deploy/gke/region-ops/build.sh
#
# The image is tagged with the commit it was built from, so the Job's image
# names the exact script it runs. Changing what the Job does always goes
# through this repository: One Platform can run the Job, never change it.
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT=pranalab-infra
REGION=asia-southeast1
TAG="$(git -C "$DIR" describe --always --dirty)"
IMAGE="$REGION-docker.pkg.dev/$PROJECT/opensandbox/region-ops:$TAG"

[[ $TAG != *-dirty ]] || { echo "commit deploy/gke/region-ops first: the tag must name the script" >&2; exit 1; }
docker build -q --platform linux/amd64 -t "$IMAGE" "$DIR" >/dev/null
docker push -q "$IMAGE" >/dev/null
gcloud run jobs update sandbox-region-ops --region "$REGION" --project "$PROJECT" --image "$IMAGE" --quiet >/dev/null
echo "sandbox-region-ops runs $IMAGE"
