#!/usr/bin/env bash
# Entrypoint of the sandbox-region-ops Cloud Run Job, which installs, prepares
# and removes OpenSandbox on the clusters One Platform manages. One Platform
# can only run this image with arguments, so this script is the boundary: it
# accepts a fixed set of actions, checks every argument against a strict
# pattern or allow list, and touches only clusters labeled app=opensandbox in
# this project.
#
#   region-ops.sh install-opensandbox <location> <cluster>
#   region-ops.sh prepare-jobs <location> <cluster> <artifactsBucket>
#   region-ops.sh remove-opensandbox <location> <cluster>
#
# The clusters and their node pools are One Platform's cluster-ops Job's
# (one-platform deploy/cluster-ops/cluster-ops.sh): it creates, resizes and
# deletes them. install-opensandbox ends with what prepare-jobs does
# (install-region.sh runs prepare-jobs.sh), so a cluster it installs can run
# sandbox Git jobs; prepare-jobs brings an existing installation to that
# point, or copies changed Secrets to it.
#
# Every action can run again after a failure: what already exists is kept.
# OPERATION_ID names the One Platform operation, so its logs and execution can
# be found again. Exit 2 means the request was rejected before any change.
set -euo pipefail

# Fixed here, not taken from the environment, which a caller can override.
readonly PROJECT=pranalab-infra
readonly SOURCE_REGION=asia-southeast1 # holds the image repository every region copies
# The cluster that holds the Secrets and image repository other regions copy.
readonly SOURCE_LOCATION=asia-southeast1-b
readonly SOURCE_CLUSTER=opensandbox
readonly DEPLOY=/opt/opensandbox/deploy/gke

reject() { echo "rejected: $*" >&2; exit 2; }
step() { printf '== step: %s\n' "$*"; }

name() { [[ $2 =~ ^[a-z][a-z0-9-]{0,62}$ ]] || reject "$1 '$2' is not a valid name"; }
region_of() { [[ $1 =~ ^([a-z]+-[a-z]+[0-9]+)(-[a-z])?$ ]] && echo "${BASH_REMATCH[1]}"; }
# artifacts_bucket prints the artifacts bucket of a region.
artifacts_bucket() {
  if [[ $1 == "$SOURCE_REGION" ]]; then echo "$PROJECT-opensandbox-artifacts"; else echo "$PROJECT-opensandbox-artifacts-$1"; fi
}

opensandbox_cluster() {
  local label
  label=$(gcloud container clusters describe "$2" --location "$1" --project "$PROJECT" \
    --format='value(resourceLabels.app)') || reject "cluster $2 in $1 does not exist"
  [[ $label == opensandbox ]] || reject "cluster $2 is not labeled app=opensandbox"
}

credentials() {
  gcloud container clusters get-credentials "$2" --location "$1" --project "$PROJECT" --dns-endpoint >/dev/null 2>&1
}

# write_env writes the parameter file the deploy/gke scripts read for a cluster
# and prints its path.
write_env() {
  local region=$1 location=$2 cluster=$3 env=/tmp/$3.env
  cat >"$env" <<EOF
PROJECT=$PROJECT
REGION=$region
LOCATION=$location
CLUSTER=$cluster
ARTIFACTS_BUCKET=$(artifacts_bucket "$region")
REGISTRY=$region-docker.pkg.dev/$PROJECT/opensandbox
EOF
  echo "$env"
}

# install_opensandbox installs OpenSandbox on a cluster cluster-ops built for
# it (install-region.sh), ready to run sandbox Git jobs.
install_opensandbox() {
  (($# == 2)) || reject "install-opensandbox takes location cluster"
  local location=$1 cluster=$2
  name location "$location"; name cluster "$cluster"
  local region
  region=$(region_of "$location") || reject "location '$location' is not a region or zone"

  step "check that $cluster is an OpenSandbox cluster"
  opensandbox_cluster "$location" "$cluster"

  local env
  env=$(write_env "$region" "$location" "$cluster")
  step "install OpenSandbox on $cluster"
  "$DEPLOY/install-region.sh" "$env"
}

# prepare_jobs copies the images to a region and makes it able to run sandbox
# Git jobs (prepare-jobs.sh). The bucket must be the region's own.
prepare_jobs() {
  (($# == 3)) || reject "prepare-jobs takes location cluster artifactsBucket"
  local location=$1 cluster=$2 bucket=$3
  name location "$location"; name cluster "$cluster"
  local region
  region=$(region_of "$location") || reject "location '$location' is not a region or zone"
  [[ $bucket == "$(artifacts_bucket "$region")" ]] || reject "bucket '$bucket' is not the artifacts bucket of $region"

  step "check that $cluster is an OpenSandbox cluster"
  opensandbox_cluster "$location" "$cluster"
  credentials "$location" "$cluster" || reject "cannot reach $cluster"

  local env
  env=$(write_env "$region" "$location" "$cluster")
  step "copy images to $region"
  "$DEPLOY/sync-images.sh" "$env"
  step "prepare $cluster for sandbox Git jobs"
  "$DEPLOY/prepare-jobs.sh" "$env"
}

# count_listed counts the objects kubectl lists. A cluster that never got as
# far as installing OpenSandbox has no such resource type, which counts as
# none; any other failure is an error, never a zero.
count_listed() {
  local out
  if out=$(kubectl get "$@" -o name 2>&1); then
    grep -c . <<<"$out" || true
  elif [[ $out == *"the server doesn't have a resource type"* ]]; then
    echo 0
  else
    echo "$out" >&2
    return 1
  fi
}

# remove_opensandbox uninstalls OpenSandbox from a cluster that runs no
# sandboxes or jobs, and deletes the region's image copy. The cluster and the
# artifacts bucket are kept.
remove_opensandbox() {
  (($# == 2)) || reject "remove-opensandbox takes location cluster"
  local location=$1 cluster=$2
  name location "$location"; name cluster "$cluster"
  local region
  region=$(region_of "$location") || reject "location '$location' is not a region or zone"
  if [[ $location == "$SOURCE_LOCATION" && $cluster == "$SOURCE_CLUSTER" ]]; then
    reject "$cluster holds the Secrets and images every other region copies"
  fi

  step "check that $cluster is an OpenSandbox cluster that runs no sandboxes or jobs"
  opensandbox_cluster "$location" "$cluster"
  credentials "$location" "$cluster" || reject "cannot reach $cluster to check it is empty"
  local sandboxes jobs
  sandboxes=$(count_listed batchsandboxes -A) || reject "cannot list the sandboxes of $cluster"
  jobs=$(count_listed jobs -n opensandbox-jobs) || reject "cannot list the jobs of $cluster"
  ((sandboxes == 0 && jobs == 0)) || reject "$cluster still has $sandboxes sandboxes and $jobs jobs"

  step "uninstall Helm release opensandbox"
  local out
  if ! out=$(helm uninstall opensandbox -n opensandbox-system 2>&1); then
    [[ $out == *"not found"* ]] || { echo "$out" >&2; exit 1; }
    echo "not installed"
  fi
  step "delete namespaces opensandbox and opensandbox-jobs"
  kubectl delete namespace opensandbox opensandbox-jobs --ignore-not-found --wait --timeout 10m

  if [[ $region == "$SOURCE_REGION" ]]; then
    echo "keeping the $SOURCE_REGION image repository: every region copies from it"
  elif gcloud artifacts repositories describe opensandbox --location "$region" --project "$PROJECT" >/dev/null 2>&1; then
    step "delete the $region image repository"
    gcloud artifacts repositories delete opensandbox --location "$region" --project "$PROJECT" --quiet
  fi
  echo "the artifacts bucket and its files are kept"
}

[[ ${OPERATION_ID:-} =~ ^[a-z0-9-]{1,64}$ ]] || reject "OPERATION_ID is missing or malformed"
echo "operation $OPERATION_ID: $*"
action=${1:-}
(($# > 0)) && shift
case $action in
  install-opensandbox) install_opensandbox "$@" ;;
  prepare-jobs) prepare_jobs "$@" ;;
  remove-opensandbox) remove_opensandbox "$@" ;;
  *) reject "unknown action '$action'" ;;
esac
step "done"
