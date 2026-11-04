#!/usr/bin/env bash
# Install OpenSandbox on a cluster, from a parameter file in deploy/gke/regions/
# (or one region-ops.sh writes):
#
#   deploy/gke/install-region.sh deploy/gke/regions/asia-south1.env
#
# The cluster must already exist. One Platform's cluster-ops Job creates it
# (one-platform deploy/cluster-ops/cluster-ops.sh create-cluster opensandbox
# <region> <machineType> <minNodes> <maxNodes>): a private cluster labeled
# app=opensandbox (Workload Identity, Managed Prometheus, DNS endpoint) with
# one sandbox node pool labeled and tainted opensandbox.io/pool=sandbox
# (NoSchedule), which the sandbox template selects and tolerates.
#
# Creates what is missing and leaves what exists: the region's artifacts
# bucket and its IAM binding, a region-local copy of the images
# (sync-images.sh), then installs the Helm release, the metrics collector and
# the sandbox template, and ends with prepare-jobs.sh: One Platform's access
# (one-platform-rbac.yaml) and everything its sandbox Git job orchestrators
# (orchestrator/) need. Re-running it is safe; it upgrades the Helm release
# and re-applies the manifests.
#
# values-gke-private.yaml and observability.yaml name the asia-southeast1
# repository; they are applied with it replaced by the region's REGISTRY.
set -euo pipefail

[[ $# -eq 1 && -f $1 ]] || { echo "usage: $0 deploy/gke/regions/<region>.env" >&2; exit 2; }
# shellcheck source=/dev/null
source "$1"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CTX="gke_${PROJECT}_${LOCATION}_${CLUSTER}"
SYS_NS=opensandbox-system
SB_NS=opensandbox
PROJECT_NUMBER=$(gcloud projects describe "$PROJECT" --format='value(projectNumber)')
# Sandboxes run as the default KSA of the sandbox namespace.
SANDBOX_PRINCIPAL="principal://iam.googleapis.com/projects/$PROJECT_NUMBER/locations/global/workloadIdentityPools/$PROJECT.svc.id.goog/subject/ns/$SB_NS/sa/default"
G=(--project "$PROJECT")
KC=(kubectl --context "$CTX")
SOURCE_REGISTRY=asia-southeast1-docker.pkg.dev/$PROJECT/opensandbox

log() { printf '\n== %s\n' "$*"; }
# Print a deploy/gke file with image references pointed at the region's registry.
regional() { sed "s#$SOURCE_REGISTRY#$REGISTRY#g" "$ROOT/deploy/gke/$1"; }

log "artifacts bucket gs://$ARTIFACTS_BUCKET"
if gcloud storage buckets describe "gs://$ARTIFACTS_BUCKET" "${G[@]}" >/dev/null 2>&1; then
  echo "exists"
else
  gcloud storage buckets create "gs://$ARTIFACTS_BUCKET" "${G[@]}" --location "$REGION" \
    --uniform-bucket-level-access --public-access-prevention --soft-delete-duration=7d
fi
gcloud storage buckets add-iam-policy-binding "gs://$ARTIFACTS_BUCKET" \
  --member "$SANDBOX_PRINCIPAL" --role roles/storage.objectAdmin >/dev/null
echo "objectAdmin granted to $SB_NS/default"

log "credentials ($CTX)"
gcloud container clusters get-credentials "$CLUSTER" --location "$LOCATION" "${G[@]}" --dns-endpoint >/dev/null 2>&1

log "namespaces and API key"
# The chart owns the system namespace, so it carries Helm's ownership marks
# before the install; the API key Secret must exist before the server starts.
"${KC[@]}" create namespace "$SYS_NS" --dry-run=client -o yaml \
  | "${KC[@]}" label --local -f - app.kubernetes.io/managed-by=Helm -o yaml \
  | "${KC[@]}" annotate --local -f - meta.helm.sh/release-name=opensandbox \
      meta.helm.sh/release-namespace="$SYS_NS" -o yaml \
  | "${KC[@]}" apply -f - >/dev/null
"${KC[@]}" create namespace "$SB_NS" --dry-run=client -o yaml | "${KC[@]}" apply -f - >/dev/null
if "${KC[@]}" -n "$SYS_NS" get secret opensandbox-api-key >/dev/null 2>&1; then
  echo "API key exists"
else
  "${KC[@]}" -n "$SYS_NS" create secret generic opensandbox-api-key \
    --from-literal=api-key="$(openssl rand -hex 24)" >/dev/null
  echo "API key created"
fi

log "images ($REGISTRY)"
"$ROOT/deploy/gke/sync-images.sh" "$1"

log "sandbox template and metrics collector"
"${KC[@]}" apply -f "$ROOT/deploy/gke/sandbox-template.yaml" >/dev/null
regional observability.yaml | "${KC[@]}" apply -f - >/dev/null
echo "applied"

log "Helm release opensandbox"
regional values-gke-private.yaml \
  | helm --kube-context "$CTX" upgrade --install opensandbox "$ROOT/manifests/charts/opensandbox" \
      -n "$SYS_NS" -f - --wait --timeout 10m \
  | grep -E '^(STATUS|REVISION):'

log "sandbox Git jobs"
"$ROOT/deploy/gke/prepare-jobs.sh" "$1"

log "done: KUBE_CONTEXT=$CTX REGISTRY=$REGISTRY REGRESS_ARTIFACT_BUCKET=$ARTIFACTS_BUCKET deploy/gke/regress.sh --in-cluster"
