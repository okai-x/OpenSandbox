#!/usr/bin/env bash
# Regression test for the self-hosted OpenSandbox deployment on GKE.
#
#   deploy/gke/regress.sh               # deployment checks from this workstation
#   deploy/gke/regress.sh --full        # + repo Kubernetes mini e2e suite
#   deploy/gke/regress.sh --in-cluster  # deployment checks as an in-cluster Job
#
# The server announces the gateway's in-cluster address, so workstation runs
# reach sandboxes through the server proxy (port-forward to the server only).
# --in-cluster runs regress-job.yaml, which goes through the gateway like the
# in-cluster orchestrator; build its image with build-regress-image.sh.
#
# Env overrides: KUBE_CONTEXT, SERVER_PORT (18080), SANDBOX_IMAGE,
# REGRESS_CONCURRENCY (2), REGRESS_VERBOSE, and for another region its
# REGISTRY and REGRESS_ARTIFACT_BUCKET (see deploy/gke/regions/).
set -euo pipefail

MODE=local
FULL=0
case "${1:-}" in
  --full) FULL=1 ;;
  --in-cluster) MODE=in-cluster ;;
  "") ;;
  *) echo "usage: $0 [--full|--in-cluster]" >&2; exit 2 ;;
esac

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CTX="${KUBE_CONTEXT:-gke_pranalab-infra_asia-southeast1-b_opensandbox}"
SYS_NS=opensandbox-system
SB_NS=opensandbox
SERVER_PORT="${SERVER_PORT:-18080}"
REGISTRY="${REGISTRY:-asia-southeast1-docker.pkg.dev/pranalab-infra/opensandbox}"
IMAGE="${SANDBOX_IMAGE:-$REGISTRY/code-interpreter:v1.1.0}"
KC=(kubectl --context "$CTX")
PF_PIDS=()

log() { printf '\n== %s\n' "$*"; }
die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }

cleanup() {
  for pid in "${PF_PIDS[@]}"; do kill "$pid" 2>/dev/null || true; done
}
trap cleanup EXIT

listening() { ss -ltnH "sport = :$1" | grep -q '127.0.0.1'; }

# Start a port-forward unless 127.0.0.1:<port> is already served. A foreign
# process on the port is caught by the health check below.
port_forward() {
  local svc=$1 port=$2
  if listening "$port"; then
    echo "127.0.0.1:$port already listening, reusing it"
    return
  fi
  "${KC[@]}" -n "$SYS_NS" port-forward --address 127.0.0.1 "svc/$svc" "$port:80" >/dev/null 2>&1 &
  PF_PIDS+=($!)
  for _ in $(seq 1 20); do listening "$port" && return; sleep 0.5; done
  die "port-forward to $svc on $port did not come up"
}

log "preflight ($CTX)"
"${KC[@]}" version --request-timeout=10s >/dev/null 2>&1 \
  || die "cannot reach cluster; is your egress IP in master authorized networks?"
for d in opensandbox-controller-manager opensandbox-server opensandbox-ingress-gateway; do
  "${KC[@]}" -n "$SYS_NS" rollout status "deploy/$d" --timeout=60s >/dev/null \
    || die "deployment $d is not ready"
  echo "deploy/$d ready"
done
"${KC[@]}" -n "$SYS_NS" get pods -o custom-columns=POD:.metadata.name,RESTARTS:.status.containerStatuses[0].restartCount --no-headers
BEFORE=$("${KC[@]}" -n "$SB_NS" get batchsandboxes --no-headers 2>/dev/null | wc -l)

API_KEY=$("${KC[@]}" -n "$SYS_NS" get secret opensandbox-api-key -o jsonpath='{.data.api-key}' | base64 -d)
[[ -n "$API_KEY" ]] || die "empty API key in secret opensandbox-api-key"

STATUS=0

if [[ $MODE == in-cluster ]]; then
  log "in-cluster Job (via gateway)"
  "${KC[@]}" -n "$SYS_NS" delete job opensandbox-regress --ignore-not-found >/dev/null
  # regress-job.yaml names the asia-southeast1 repository; point it at REGISTRY.
  # REGRESS_ARTIFACT_BUCKET is not in the file, so set env always emits it.
  sed "s#asia-southeast1-docker.pkg.dev/pranalab-infra/opensandbox#$REGISTRY#g" \
      "$ROOT/deploy/gke/regress-job.yaml" \
    | "${KC[@]}" set env --local -f - -o yaml \
        REGRESS_ARTIFACT_BUCKET="${REGRESS_ARTIFACT_BUCKET:-pranalab-infra-opensandbox-artifacts}" \
    | "${KC[@]}" apply -f - >/dev/null
  # Wait for the pod to start, then stream its output until the Job ends.
  "${KC[@]}" -n "$SYS_NS" wait --for=condition=Ready pod -l job-name=opensandbox-regress --timeout=180s >/dev/null 2>&1 || true
  "${KC[@]}" -n "$SYS_NS" logs -f job/opensandbox-regress || true
  "${KC[@]}" -n "$SYS_NS" wait --for=condition=Complete job/opensandbox-regress --timeout=30s >/dev/null 2>&1 \
    || STATUS=1
else
  log "port-forward"
  port_forward opensandbox-server "$SERVER_PORT"
  curl -sf "http://127.0.0.1:$SERVER_PORT/health" | grep -q healthy \
    || die "127.0.0.1:$SERVER_PORT is not the OpenSandbox server"

  export OPENSANDBOX_TEST_DOMAIN="127.0.0.1:$SERVER_PORT"
  export OPENSANDBOX_TEST_PROTOCOL=http
  export OPENSANDBOX_TEST_API_KEY="$API_KEY"
  export OPENSANDBOX_SANDBOX_DEFAULT_IMAGE="$IMAGE"
  export REGRESS_SERVER_PROXY=true

  log "deployment checks (via server proxy)"
  uv run -q --project "$ROOT/sdks/sandbox/python" python "$ROOT/deploy/gke/regress.py" || STATUS=1

  if [[ $FULL -eq 1 ]]; then
    log "repo Kubernetes mini e2e suite"
    (
      cd "$ROOT/tests/python"
      export OPENSANDBOX_E2E_RUNTIME=kubernetes
      export OPENSANDBOX_TEST_USE_SERVER_PROXY=true
      export OPENSANDBOX_E2E_NAMESPACE="$SB_NS"
      export OPENSANDBOX_E2E_SANDBOX_CPU=500m
      export OPENSANDBOX_E2E_SANDBOX_MEMORY=1Gi
      # Deselected by design of this deployment: nodes have no NAT, so the
      # port-scoped egress test can never reach 1.1.1.1; the PVC tests need a
      # pre-seeded opensandbox-e2e-pvc-test PVC that only the kind e2e creates.
      T=tests/test_sandbox_e2e_sync.py::TestSandboxE2ESync
      uv run -q pytest -p no:cacheprovider \
        --deselect "$T::test_01ab_network_policy_port_scoped_rules" \
        --deselect "$T::test_01d_pvc_named_volume_mount" \
        --deselect "$T::test_01e_pvc_named_volume_mount_readonly" \
        --deselect "$T::test_01f_pvc_named_volume_subpath_mount" \
        tests/test_sandbox_e2e_sync.py tests/test_sandbox_manager_e2e_sync.py
    ) || STATUS=1
  fi
fi

log "leak check"
sleep 5
AFTER=$("${KC[@]}" -n "$SB_NS" get batchsandboxes --no-headers 2>/dev/null | wc -l)
echo "BatchSandboxes before=$BEFORE after=$AFTER"
if (( AFTER > BEFORE )); then
  echo "WARN: $((AFTER - BEFORE)) BatchSandbox(es) left behind:"
  "${KC[@]}" -n "$SB_NS" get batchsandboxes
  STATUS=1
fi

log "result: $([[ $STATUS -eq 0 ]] && echo PASS || echo FAIL)"
exit $STATUS
