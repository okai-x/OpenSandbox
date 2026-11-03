#!/usr/bin/env bash
# Build an OpenSandbox region from a parameter file in deploy/gke/regions/:
#
#   deploy/gke/bootstrap-region.sh deploy/gke/regions/asia-south1.env
#
# Creates what is missing and leaves what exists: private GKE cluster (Workload
# Identity, Managed Prometheus, DNS endpoint), one tainted sandbox node pool,
# the region's artifacts bucket and its IAM binding, a region-local copy of the
# images (sync-images.sh), then installs the Helm release, the metrics
# collector, the sandbox template and One Platform's access
# (one-platform-rbac.yaml), including namespace opensandbox-jobs where One
# Platform runs sandbox Git job orchestrators (orchestrator/). Re-running it is
# safe; it upgrades the Helm release and re-applies the manifests.
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
# Sandbox Git job orchestrators run as KSA sandbox-orchestrator in their own namespace
# and write the job's records to the artifacts bucket.
JOBS_NS=opensandbox-jobs
ORCHESTRATOR_KSA=sandbox-orchestrator
ORCHESTRATOR_PRINCIPAL="principal://iam.googleapis.com/projects/$PROJECT_NUMBER/locations/global/workloadIdentityPools/$PROJECT.svc.id.goog/subject/ns/$JOBS_NS/sa/$ORCHESTRATOR_KSA"
G=(--project "$PROJECT")
KC=(kubectl --context "$CTX")
SOURCE_REGISTRY=asia-southeast1-docker.pkg.dev/$PROJECT/opensandbox

log() { printf '\n== %s\n' "$*"; }
# Print a deploy/gke file with image references pointed at the region's registry.
regional() { sed "s#$SOURCE_REGISTRY#$REGISTRY#g" "$ROOT/deploy/gke/$1"; }

log "network: Private Google Access on subnet default/$REGION"
# Nodes have no external IP and no NAT; Google APIs (Artifact Registry, GCS,
# Monitoring) are reached through Private Google Access.
gcloud compute networks subnets update default --region "$REGION" "${G[@]}" \
  --enable-private-ip-google-access --quiet

log "cluster $CLUSTER ($LOCATION)"
if gcloud container clusters describe "$CLUSTER" --location "$LOCATION" "${G[@]}" >/dev/null 2>&1; then
  echo "exists"
else
  # --num-nodes is per zone of --node-locations.
  gcloud container clusters create "$CLUSTER" --location "$LOCATION" "${G[@]}" \
    --node-locations "$SYSTEM_NODE_LOCATIONS" --num-nodes 1 \
    --machine-type "$SYSTEM_MACHINE" --disk-size 100 \
    --network default --subnetwork default --enable-ip-alias \
    --enable-private-nodes --enable-master-authorized-networks \
    --master-authorized-networks "$MASTER_AUTHORIZED_NETWORKS" \
    --enable-dns-access --release-channel regular \
    --workload-pool "$PROJECT.svc.id.goog" --enable-managed-prometheus \
    --autoscaling-profile optimize-utilization \
    --labels app=opensandbox --quiet
fi

log "sandbox node pool $SANDBOX_POOL ($SANDBOX_MACHINE)"
if gcloud container node-pools describe "$SANDBOX_POOL" --cluster "$CLUSTER" --location "$LOCATION" "${G[@]}" >/dev/null 2>&1; then
  echo "exists"
else
  # The taint keeps system pods off autoscaled sandbox nodes; the sandbox
  # template (sandbox-template.yaml) selects the label and tolerates the taint.
  # ANY lets the autoscaler use whichever zone has the machine type in stock.
  gcloud container node-pools create "$SANDBOX_POOL" --cluster "$CLUSTER" --location "$LOCATION" "${G[@]}" \
    --node-locations "$SANDBOX_NODE_LOCATIONS" --machine-type "$SANDBOX_MACHINE" \
    --disk-type "$SANDBOX_DISK_TYPE" --disk-size "$SANDBOX_DISK_GB" \
    --num-nodes 0 --enable-autoscaling --location-policy ANY \
    --total-min-nodes "$SANDBOX_MIN_NODES" --total-max-nodes "$SANDBOX_MAX_NODES" \
    --enable-image-streaming --workload-metadata GKE_METADATA \
    --node-labels opensandbox.io/pool=sandbox \
    --node-taints opensandbox.io/pool=sandbox:NoSchedule --quiet
fi

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
gcloud storage buckets add-iam-policy-binding "gs://$ARTIFACTS_BUCKET" \
  --member "$ORCHESTRATOR_PRINCIPAL" --role roles/storage.objectAdmin >/dev/null
echo "objectAdmin granted to $JOBS_NS/$ORCHESTRATOR_KSA"

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
# One Platform creates orchestrator Jobs here; one-platform-rbac.yaml grants it
# access to this namespace, so it must exist before that file is applied.
"${KC[@]}" create namespace "$JOBS_NS" --dry-run=client -o yaml | "${KC[@]}" apply -f - >/dev/null
"${KC[@]}" apply -f - >/dev/null <<EOF
apiVersion: v1
kind: ServiceAccount
metadata:
  name: $ORCHESTRATOR_KSA
  namespace: $JOBS_NS
automountServiceAccountToken: false
EOF
# Orchestrators read these Secrets (key value); they are created by hand, never
# by this script, and a missing one fails the job at start:
#   opensandbox-api-key  the server API key (same value as opensandbox-system/opensandbox-api-key, key api-key)
#   github-read-token    read-only GitHub token for the PranaLabs repositories
#   task-<model key>     each Secret a job's spec.json names under "secrets"; One Platform accepts only task- names, so no job can name the two above
# e.g. kubectl -n opensandbox-jobs create secret generic github-read-token --from-file=value=/dev/stdin
for secret in opensandbox-api-key github-read-token; do
  "${KC[@]}" -n "$JOBS_NS" get secret "$secret" >/dev/null 2>&1 \
    || echo "missing Secret $JOBS_NS/$secret (key value): sandbox Git jobs fail until it is created"
done

log "images ($REGISTRY)"
"$ROOT/deploy/gke/sync-images.sh" "$1"

log "sandbox template and metrics collector"
"${KC[@]}" apply -f "$ROOT/deploy/gke/sandbox-template.yaml" >/dev/null
regional observability.yaml | "${KC[@]}" apply -f - >/dev/null
"${KC[@]}" apply -f "$ROOT/deploy/gke/one-platform-rbac.yaml" >/dev/null
echo "applied"

log "Helm release opensandbox"
regional values-gke-private.yaml \
  | helm --kube-context "$CTX" upgrade --install opensandbox "$ROOT/manifests/charts/opensandbox" \
      -n "$SYS_NS" -f - --wait --timeout 10m \
  | grep -E '^(STATUS|REVISION):'

log "done: KUBE_CONTEXT=$CTX REGISTRY=$REGISTRY REGRESS_ARTIFACT_BUCKET=$ARTIFACTS_BUCKET deploy/gke/regress.sh --in-cluster"
