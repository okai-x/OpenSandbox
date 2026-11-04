#!/usr/bin/env bash
# Make an OpenSandbox region able to run One Platform's Harbor jobs, from a
# parameter file in deploy/gke/regions/ (or one region-ops.sh writes):
#
#   deploy/gke/prepare-jobs.sh deploy/gke/regions/asia-south1.env
#
# One Platform lists a region for jobs only once this has succeeded there. It
# sets up namespace opensandbox-jobs and the Harbor runner's KSA, its read of
# its limits ConfigMap, One Platform's RBAC (one-platform-rbac.yaml), the
# runner's write and One Platform's read access to the artifacts bucket, and
# the Secrets runners read; then it checks all of them and that the region's
# image copy holds the runner. Copy the images first (sync-images.sh). The
# cluster's credentials must be in the kubeconfig. Re-running it is safe.
#
# opensandbox-api-key is the region's own server key. github-read-token and
# the task- Secrets are kept in asia-southeast1 and copied from there: change
# one there, then run this for every other region. A task- Secret removed
# there is removed here. Secret values pass only through pipes, never through
# files, arguments or the log.
set -euo pipefail

[[ $# -eq 1 && -f $1 ]] || { echo "usage: $0 deploy/gke/regions/<region>.env" >&2; exit 2; }
# shellcheck source=/dev/null
source "$1"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CTX="gke_${PROJECT}_${LOCATION}_${CLUSTER}"
SYS_NS=opensandbox-system
JOBS_NS=opensandbox-jobs
HARBOR_KSA=harbor-runner
# One Platform's account and the Secrets every other region copies are in the
# primary project, whatever the cluster's PROJECT.
PRIMARY_PROJECT=pranalab-infra
PLATFORM_SA=one-platform-sandbox@$PRIMARY_PROJECT.iam.gserviceaccount.com
# The region that holds the Secrets every other region copies.
SOURCE_LOCATION=asia-southeast1-b
SOURCE_CLUSTER=opensandbox
PROJECT_NUMBER=$(gcloud projects describe "$PROJECT" --format='value(projectNumber)')
KSA_PRINCIPAL="principal://iam.googleapis.com/projects/$PROJECT_NUMBER/locations/global/workloadIdentityPools/$PROJECT.svc.id.goog/subject/ns/$JOBS_NS/sa"
HARBOR_PRINCIPAL="$KSA_PRINCIPAL/$HARBOR_KSA"
KC=(kubectl --context "$CTX")

log() { printf '\n== %s\n' "$*"; }

# as_secret reads a Secret and prints Secret $1 of $JOBS_NS holding its key $2
# under key value. Nothing but that key is kept.
as_secret() {
  python3 -c '
import json, sys
name, key = sys.argv[1:]
data = json.load(sys.stdin).get("data", {})
if key not in data:
    sys.exit(f"Secret {name}: the source has no key {key}")
print(json.dumps({"apiVersion": "v1", "kind": "Secret", "type": "Opaque",
                  "metadata": {"name": name, "namespace": "opensandbox-jobs"}, "data": {"value": data[key]}}))
' "$1" "$2"
}

# put_secret applies a Secret from stdin. Server-side apply keeps the value
# out of the last-applied annotation that client-side apply would write.
put_secret() {
  "${KC[@]}" apply --server-side --force-conflicts --field-manager prepare-jobs -f - >/dev/null
}

# copied_secrets names the Secrets of a namespace that are copied from the source.
copied_secrets() {
  "$@" -n "$JOBS_NS" get secrets -o name | sed -n -E 's#^secret/(github-read-token|task-[a-z0-9.-]+)$#\1#p'
}

log "namespace $JOBS_NS and KSA $HARBOR_KSA"
"${KC[@]}" create namespace "$JOBS_NS" --dry-run=client -o yaml | "${KC[@]}" apply -f - >/dev/null
# The token is not mounted by default. The Harbor runner reaches its sandboxes
# through OpenSandbox and calls the API only to read its job's limits
# ConfigMap (its pod opts in to the token).
"${KC[@]}" apply -f - >/dev/null <<EOF
apiVersion: v1
kind: ServiceAccount
metadata:
  name: $HARBOR_KSA
  namespace: $JOBS_NS
automountServiceAccountToken: false
EOF
# A mounted ConfigMap changes only on kubelet's sync, about a minute later; the
# API has a paused or resized job's limits within the runner's 5 s poll. The
# Role covers every job's ConfigMaps (names are per job): rendered configs and
# limits. Credentials reach jobs only as Secrets, which it does not cover.
"${KC[@]}" apply -f - >/dev/null <<EOF_HARBOR_RBAC
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: $HARBOR_KSA
  namespace: $JOBS_NS
rules:
  - apiGroups: [""]
    resources: [configmaps]
    verbs: [get]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: $HARBOR_KSA
  namespace: $JOBS_NS
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: $HARBOR_KSA
subjects:
  - kind: ServiceAccount
    name: $HARBOR_KSA
    namespace: $JOBS_NS
EOF_HARBOR_RBAC
echo "applied"

log "One Platform's access (one-platform-rbac.yaml)"
"${KC[@]}" apply -f "$ROOT/deploy/gke/one-platform-rbac.yaml" >/dev/null
echo "applied"

log "artifacts bucket gs://$ARTIFACTS_BUCKET"
# The Harbor runner writes its job's results and mints the upload sessions its sandboxes use.
gcloud storage buckets add-iam-policy-binding "gs://$ARTIFACTS_BUCKET" \
  --member "$HARBOR_PRINCIPAL" --role roles/storage.objectAdmin >/dev/null
echo "objectAdmin granted to $JOBS_NS/$HARBOR_KSA"
gcloud storage buckets add-iam-policy-binding "gs://$ARTIFACTS_BUCKET" \
  --member "serviceAccount:$PLATFORM_SA" --role roles/storage.objectViewer >/dev/null
echo "objectViewer granted to $PLATFORM_SA"

log "Secrets of $JOBS_NS"
"${KC[@]}" -n "$SYS_NS" get secret opensandbox-api-key -o json | as_secret opensandbox-api-key api-key | put_secret
echo "opensandbox-api-key set from $SYS_NS"
if [[ $PROJECT == "$PRIMARY_PROJECT" && $LOCATION == "$SOURCE_LOCATION" && $CLUSTER == "$SOURCE_CLUSTER" ]]; then
  echo "this region holds the copied Secrets"
else
  SOURCE_KC=$(mktemp)
  trap 'rm -f "$SOURCE_KC"' EXIT
  KUBECONFIG=$SOURCE_KC gcloud container clusters get-credentials "$SOURCE_CLUSTER" --location "$SOURCE_LOCATION" \
    --project "$PRIMARY_PROJECT" --dns-endpoint >/dev/null 2>&1 || { echo "cannot reach $SOURCE_CLUSTER to copy its Secrets" >&2; exit 1; }
  SRC=(kubectl --kubeconfig "$SOURCE_KC" --context "gke_${PRIMARY_PROJECT}_${SOURCE_LOCATION}_${SOURCE_CLUSTER}")
  wanted=$(copied_secrets "${SRC[@]}")
  grep -qx github-read-token <<<"$wanted" || { echo "$SOURCE_CLUSTER has no Secret $JOBS_NS/github-read-token" >&2; exit 1; }
  for secret in $wanted; do
    "${SRC[@]}" -n "$JOBS_NS" get secret "$secret" -o json | as_secret "$secret" value | put_secret
    echo "$secret copied"
  done
  # Listed before the loop: a failed list must stop the script, not skip the loop.
  present=$(copied_secrets "${KC[@]}")
  for secret in $present; do
    if ! grep -qxF "$secret" <<<"$wanted"; then
      "${KC[@]}" -n "$JOBS_NS" delete secret "$secret" >/dev/null
      echo "$secret removed: $SOURCE_CLUSTER no longer has it"
    fi
  done
fi

log "check"
"${KC[@]}" -n "$JOBS_NS" get serviceaccount "$HARBOR_KSA" >/dev/null
"${KC[@]}" -n "$JOBS_NS" get role,rolebinding "$HARBOR_KSA" >/dev/null
"${KC[@]}" get -f "$ROOT/deploy/gke/one-platform-rbac.yaml" >/dev/null
"${KC[@]}" -n "$JOBS_NS" get secret opensandbox-api-key github-read-token >/dev/null
# A failed list stops here with gcloud's own error; an empty one means the copy lacks the image.
harbor_runner=$(gcloud artifacts docker images list "$REGISTRY/harbor-runner" --limit 1 --format='value(version)')
[[ -n $harbor_runner ]] || { echo "$REGISTRY has no harbor-runner image: run sync-images.sh first" >&2; exit 1; }
echo "$CLUSTER can run Harbor jobs"
