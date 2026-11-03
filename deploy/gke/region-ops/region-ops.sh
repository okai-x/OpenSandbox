#!/usr/bin/env bash
# Entrypoint of the sandbox-region-ops Cloud Run Job, which makes every change
# One Platform asks of an OpenSandbox region. One Platform can only run this
# image with arguments, so this script is the boundary: it accepts a fixed set
# of actions, checks every argument against a strict pattern, and touches only
# clusters labeled app=opensandbox in this project and their sandbox node pool.
#
#   region-ops.sh update-limits <location> <cluster> <nodePool> <minNodes> <maxNodes>
#
# SANDBOX_OP_ID names the One Platform operation, so its logs and execution can
# be found again. Exit 2 means the request was rejected before any change.
set -euo pipefail

# Fixed here, not taken from the environment, which a caller can override.
readonly PROJECT=pranalab-infra
readonly MAX_NODES=64

reject() { echo "rejected: $*" >&2; exit 2; }
step() { printf '== step: %s\n' "$*"; }

name() { [[ $2 =~ ^[a-z][a-z0-9-]{0,62}$ ]] || reject "$1 '$2' is not a valid name"; }
# No leading zeros: bash would read 08 as an invalid octal number.
count() {
  if [[ ! $2 =~ ^(0|[1-9][0-9]{0,2})$ ]] || (($2 > MAX_NODES)); then
    reject "$1 '$2' is not a node count from 0 to $MAX_NODES"
  fi
}

# sandbox_pool checks that the cluster belongs to OpenSandbox and that the
# pool is its sandbox pool, then prints the pool's autoscaling as JSON.
sandbox_pool() {
  local location=$1 cluster=$2 pool=$3
  local label
  label=$(gcloud container clusters describe "$cluster" --location "$location" --project "$PROJECT" \
    --format='value(resourceLabels.app)')
  [[ $label == opensandbox ]] || reject "cluster $cluster is not labeled app=opensandbox"
  gcloud container node-pools describe "$pool" --cluster "$cluster" --location "$location" --project "$PROJECT" \
    --format=json | python3 -c '
import json, sys
pool = json.load(sys.stdin)
if pool.get("config", {}).get("labels", {}).get("opensandbox.io/pool") != "sandbox":
    sys.exit("node pool is not labeled opensandbox.io/pool=sandbox")
print(json.dumps({"autoscaling": pool.get("autoscaling", {}), "zones": len(pool.get("locations", []))}))'
}

update_limits() {
  (($# == 5)) || reject "update-limits takes location cluster nodePool minNodes maxNodes"
  local location=$1 cluster=$2 pool=$3 min=$4 max=$5
  name location "$location"; name cluster "$cluster"; name nodePool "$pool"
  count minNodes "$min"; count maxNodes "$max"
  ((min <= max)) || reject "minNodes $min is above maxNodes $max"

  step "check that $cluster/$pool is a sandbox node pool"
  local shape
  shape=$(sandbox_pool "$location" "$cluster" "$pool") || reject "$cluster/$pool is not a sandbox node pool"
  echo "$shape"

  # A pool created with total limits keeps them; a single-zone pool's per-zone
  # limits are its totals. A multi-zone pool with per-zone limits is refused,
  # since its totals would be a multiple of what was asked.
  local flags
  if python3 -c 'import json,sys; sys.exit(0 if json.loads(sys.argv[1])["autoscaling"].get("totalMaxNodeCount") else 1)' "$shape"; then
    flags=(--total-min-nodes "$min" --total-max-nodes "$max")
  elif python3 -c 'import json,sys; sys.exit(0 if json.loads(sys.argv[1])["zones"] == 1 else 1)' "$shape"; then
    flags=(--min-nodes "$min" --max-nodes "$max")
  else
    reject "$pool sets per-zone limits across several zones"
  fi

  step "update $pool autoscaling to $min-$max nodes"
  gcloud container node-pools update "$pool" --cluster "$cluster" --location "$location" --project "$PROJECT" \
    --enable-autoscaling "${flags[@]}" --quiet

  step "verify the new limits"
  gcloud container node-pools describe "$pool" --cluster "$cluster" --location "$location" --project "$PROJECT" \
    --format='value(autoscaling)'
}

[[ ${SANDBOX_OP_ID:-} =~ ^[a-z0-9-]{1,64}$ ]] || reject "SANDBOX_OP_ID is missing or malformed"
echo "operation $SANDBOX_OP_ID: $*"
action=${1:-}
(($# > 0)) && shift
case $action in
  update-limits) update_limits "$@" ;;
  *) reject "unknown action '$action'" ;;
esac
step "done"
