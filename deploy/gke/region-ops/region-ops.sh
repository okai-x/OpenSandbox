#!/usr/bin/env bash
# Entrypoint of the sandbox-region-ops Cloud Run Job, which makes every change
# One Platform asks of an OpenSandbox region. One Platform can only run this
# image with arguments, so this script is the boundary: it accepts a fixed set
# of actions, checks every argument against a strict pattern or allow list, and
# touches only clusters labeled app=opensandbox in this project and their
# sandbox node pools.
#
#   region-ops.sh update-limits <location> <cluster> <nodePool> <minNodes> <maxNodes>
#   region-ops.sh create-region <region> <machineType> <minNodes> <maxNodes>
#   region-ops.sh delete-region <location> <cluster>
#   region-ops.sh change-machine-type <location> <cluster> <nodePool> <machineType> <minNodes> <maxNodes>
#
# Every action can run again after a failure: what already exists is kept.
# SANDBOX_OP_ID names the One Platform operation, so its logs and execution can
# be found again. Exit 2 means the request was rejected before any change.
set -euo pipefail

# Fixed here, not taken from the environment, which a caller can override.
readonly PROJECT=pranalab-infra
readonly MAX_NODES=64
readonly SOURCE_REGION=asia-southeast1 # holds the image repository every region copies
readonly MASTER_AUTHORIZED_NETWORKS=103.54.154.119/32
readonly DEPLOY=/opt/opensandbox/deploy/gke
readonly DRAIN_MINUTES=45 # how long change-machine-type waits for the old pool to empty

reject() { echo "rejected: $*" >&2; exit 2; }
step() { printf '== step: %s\n' "$*"; }

name() { [[ $2 =~ ^[a-z][a-z0-9-]{0,62}$ ]] || reject "$1 '$2' is not a valid name"; }
# No leading zeros: bash would read 08 as an invalid octal number.
count() {
  if [[ ! $2 =~ ^(0|[1-9][0-9]{0,2})$ ]] || (($2 > MAX_NODES)); then
    reject "$1 '$2' is not a node count from 0 to $MAX_NODES"
  fi
}
limits() {
  count minNodes "$1"; count maxNodes "$2"
  (($1 <= $2)) || reject "minNodes $1 is above maxNodes $2"
  (($2 >= 1)) || reject "maxNodes must be at least 1"
}

# The machine types sandboxes may run on, and the disk each needs.
disk_type() {
  case $1 in
    c3d-highcpu-60) echo pd-balanced ;;
    n4d-highcpu-96) echo hyperdisk-balanced ;;
    *) reject "machine type '$1' is not one sandboxes run on" ;;
  esac
}
# sandbox-n4d96 for n4d-highcpu-96: the family and the vCPU count.
pool_name() { echo "sandbox-${1%%-*}${1##*-}"; }
region_of() { [[ $1 =~ ^([a-z]+-[a-z]+[0-9]+)(-[a-z])?$ ]] && echo "${BASH_REMATCH[1]}"; }

# zones prints the zones of a region, or the zone itself, that offer a machine type.
zones() {
  local where=$1 type=$2
  gcloud compute machine-types list --project "$PROJECT" --filter="name=$type AND zone~^$where" \
    --format='value(zone)' | sort | paste -sd, -
}

opensandbox_cluster() {
  local label
  label=$(gcloud container clusters describe "$2" --location "$1" --project "$PROJECT" \
    --format='value(resourceLabels.app)') || reject "cluster $2 in $1 does not exist"
  [[ $label == opensandbox ]] || reject "cluster $2 is not labeled app=opensandbox"
}

# pool_shape checks that the pool is a sandbox pool of an OpenSandbox cluster and
# prints how its limits are kept: "total" or "zone", then its zones.
pool_shape() {
  local location=$1 cluster=$2 pool=$3
  opensandbox_cluster "$location" "$cluster"
  gcloud container node-pools describe "$pool" --cluster "$cluster" --location "$location" --project "$PROJECT" \
    --format=json | python3 -c '
import json, sys
pool = json.load(sys.stdin)
if pool.get("config", {}).get("labels", {}).get("opensandbox.io/pool") != "sandbox":
    sys.exit("node pool is not labeled opensandbox.io/pool=sandbox")
mode = "total" if pool.get("autoscaling", {}).get("totalMaxNodeCount") else "zone"
print(mode, ",".join(pool.get("locations", [])))'
}

# limit_flags prints the autoscaling flags for min-max nodes in that shape. A
# single-zone pool's per-zone limits are its totals; a multi-zone pool with
# per-zone limits is refused, since its totals would be a multiple of what was asked.
limit_flags() {
  local mode=$1 zones=$2 min=$3 max=$4
  if [[ $mode == total ]]; then
    echo "--total-min-nodes $min --total-max-nodes $max"
  elif [[ $zones != *,* ]]; then
    echo "--min-nodes $min --max-nodes $max"
  else
    reject "the pool sets per-zone limits across several zones"
  fi
}

credentials() {
  gcloud container clusters get-credentials "$2" --location "$1" --project "$PROJECT" --dns-endpoint >/dev/null 2>&1
}

update_limits() {
  (($# == 5)) || reject "update-limits takes location cluster nodePool minNodes maxNodes"
  local location=$1 cluster=$2 pool=$3 min=$4 max=$5
  name location "$location"; name cluster "$cluster"; name nodePool "$pool"; limits "$min" "$max"

  step "check that $cluster/$pool is a sandbox node pool"
  local shape
  shape=$(pool_shape "$location" "$cluster" "$pool") || reject "$cluster/$pool is not a sandbox node pool"
  echo "limits kept per ${shape%% *}; zones ${shape#* }"
  local flags
  flags=$(limit_flags "${shape%% *}" "${shape#* }" "$min" "$max")

  step "update $pool autoscaling to $min-$max nodes"
  # shellcheck disable=SC2086 # the flags are words without spaces
  gcloud container node-pools update "$pool" --cluster "$cluster" --location "$location" --project "$PROJECT" \
    --enable-autoscaling $flags --quiet

  step "verify the new limits"
  gcloud container node-pools describe "$pool" --cluster "$cluster" --location "$location" --project "$PROJECT" \
    --format='value(autoscaling)'
}

create_region() {
  (($# == 4)) || reject "create-region takes region machineType minNodes maxNodes"
  local region=$1 type=$2 min=$3 max=$4
  [[ $region =~ ^[a-z]+-[a-z]+[0-9]+$ ]] || reject "region '$region' is not a region name"
  local disk
  disk=$(disk_type "$type"); limits "$min" "$max"

  step "find the zones of $region that offer $type"
  local sandbox_zones system_zones
  sandbox_zones=$(zones "$region-" "$type")
  [[ -n $sandbox_zones ]] || reject "no zone of $region offers $type"
  system_zones=$(zones "$region-" e2-standard-4 | cut -d, -f1-2)
  [[ -n $system_zones ]] || reject "no zone of $region offers e2-standard-4 for the system pool"
  echo "sandbox zones $sandbox_zones; system zones $system_zones"

  local env=/tmp/$region.env
  cat >"$env" <<EOF
PROJECT=$PROJECT
REGION=$region
LOCATION=$region
CLUSTER=opensandbox-$region
MASTER_AUTHORIZED_NETWORKS=$MASTER_AUTHORIZED_NETWORKS
SYSTEM_NODE_LOCATIONS=$system_zones
SYSTEM_MACHINE=e2-standard-4
SANDBOX_POOL=$(pool_name "$type")
SANDBOX_MACHINE=$type
SANDBOX_DISK_TYPE=$disk
SANDBOX_DISK_GB=200
SANDBOX_NODE_LOCATIONS=$sandbox_zones
SANDBOX_MIN_NODES=$min
SANDBOX_MAX_NODES=$max
ARTIFACTS_BUCKET=$PROJECT-opensandbox-artifacts-$region
REGISTRY=$region-docker.pkg.dev/$PROJECT/opensandbox
EOF
  step "bootstrap $region"
  "$DEPLOY/bootstrap-region.sh" "$env"
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

delete_region() {
  (($# == 2)) || reject "delete-region takes location cluster"
  local location=$1 cluster=$2
  name location "$location"; name cluster "$cluster"
  local region
  region=$(region_of "$location") || reject "location '$location' is not a region or zone"
  opensandbox_cluster "$location" "$cluster"

  step "check that $cluster runs no sandboxes or jobs"
  credentials "$location" "$cluster" || reject "cannot reach $cluster to check it is empty"
  local sandboxes jobs
  sandboxes=$(count_listed batchsandboxes -A) || reject "cannot list the sandboxes of $cluster"
  jobs=$(count_listed jobs -n opensandbox-jobs) || reject "cannot list the jobs of $cluster"
  ((sandboxes == 0 && jobs == 0)) || reject "$cluster still has $sandboxes sandboxes and $jobs jobs"

  step "delete cluster $cluster"
  gcloud container clusters delete "$cluster" --location "$location" --project "$PROJECT" --quiet

  if [[ $region == "$SOURCE_REGION" ]]; then
    echo "keeping the $SOURCE_REGION image repository: every region copies from it"
  elif gcloud artifacts repositories describe opensandbox --location "$region" --project "$PROJECT" >/dev/null 2>&1; then
    step "delete the $region image repository"
    gcloud artifacts repositories delete opensandbox --location "$region" --project "$PROJECT" --quiet
  fi
  echo "the artifacts bucket and its files are kept"
}

change_machine_type() {
  (($# == 6)) || reject "change-machine-type takes location cluster nodePool machineType minNodes maxNodes"
  local location=$1 cluster=$2 old=$3 type=$4 min=$5 max=$6
  name location "$location"; name cluster "$cluster"; name nodePool "$old"; limits "$min" "$max"
  local disk new
  disk=$(disk_type "$type"); new=$(pool_name "$type")
  [[ $new != "$old" ]] || reject "$old already runs $type"

  step "check that $cluster/$old is a sandbox node pool"
  local shape
  shape=$(pool_shape "$location" "$cluster" "$old") || reject "$cluster/$old is not a sandbox node pool"
  local new_zones
  new_zones=$(zones "$location" "$type")
  [[ -n $new_zones ]] || reject "no zone of $location offers $type"
  local flags
  flags=$(limit_flags "${shape%% *}" "$new_zones" "$min" "$max")

  if gcloud container node-pools describe "$new" --cluster "$cluster" --location "$location" --project "$PROJECT" >/dev/null 2>&1; then
    step "node pool $new exists"
  else
    step "create node pool $new ($type) in $new_zones"
    # shellcheck disable=SC2086 # the flags are words without spaces
    gcloud container node-pools create "$new" --cluster "$cluster" --location "$location" --project "$PROJECT" \
      --node-locations "$new_zones" --machine-type "$type" --disk-type "$disk" --disk-size 200 \
      --num-nodes 0 --enable-autoscaling --location-policy ANY $flags \
      --enable-image-streaming --workload-metadata GKE_METADATA \
      --node-labels opensandbox.io/pool=sandbox --node-taints opensandbox.io/pool=sandbox:NoSchedule --quiet
  fi
  drain_and_delete "$location" "$cluster" "$old"
}

# sandboxes_on counts the sandbox pods that have not finished on a pool's nodes.
sandboxes_on() {
  local nodes node total=0 n
  nodes=$(kubectl get nodes -l "cloud.google.com/gke-nodepool=$1" -o jsonpath='{.items[*].metadata.name}') || return 1
  for node in $nodes; do
    n=$(kubectl -n opensandbox get pods -o name \
      --field-selector "spec.nodeName=$node,status.phase!=Succeeded,status.phase!=Failed") || return 1
    total=$((total + $(grep -c . <<<"$n" || true)))
  done
  echo "$total"
}

# drain_and_delete stops new sandboxes landing on the old pool, waits for those
# on it to end, then deletes it. Out of time, it fails and leaves the old pool
# cordoned: running the action again resumes here.
drain_and_delete() {
  local location=$1 cluster=$2 old=$3
  credentials "$location" "$cluster"
  step "stop scheduling on $old"
  kubectl cordon -l "cloud.google.com/gke-nodepool=$old" >/dev/null 2>&1 || true

  step "wait up to $DRAIN_MINUTES minutes for the sandboxes on $old to end"
  local deadline=$((SECONDS + DRAIN_MINUTES * 60)) left
  while true; do
    left=$(sandboxes_on "$old") || reject "cannot count the sandboxes on $old"
    ((left == 0)) && break
    ((SECONDS < deadline)) || { echo "$old still runs $left sandboxes; run this again later to finish" >&2; exit 1; }
    echo "$left sandboxes left on $old"
    sleep 60
  done

  step "delete node pool $old"
  gcloud container node-pools delete "$old" --cluster "$cluster" --location "$location" --project "$PROJECT" --quiet
}

[[ ${SANDBOX_OP_ID:-} =~ ^[a-z0-9-]{1,64}$ ]] || reject "SANDBOX_OP_ID is missing or malformed"
echo "operation $SANDBOX_OP_ID: $*"
action=${1:-}
(($# > 0)) && shift
case $action in
  update-limits) update_limits "$@" ;;
  create-region) create_region "$@" ;;
  delete-region) delete_region "$@" ;;
  change-machine-type) change_machine_type "$@" ;;
  *) reject "unknown action '$action'" ;;
esac
step "done"
