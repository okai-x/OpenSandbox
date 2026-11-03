#!/usr/bin/env bash
# Run loadtest.py as an in-cluster Job (through the gateway, like the
# orchestrator), stream its output and exit with its result.
#
#   deploy/gke/loadtest.sh --concurrency 14 --total 28 --burn-seconds 120
#
# All arguments go to loadtest.py (see its --help). Sandbox nodes scale up as
# needed: 7 sandboxes of 8 CPU / 12Gi fit on one c3d-highcpu-60 node, 11 on an
# n4d-highcpu-96. Build the image first with build-regress-image.sh. For
# another region set KUBE_CONTEXT, REGISTRY and REGRESS_ARTIFACT_BUCKET
# (deploy/gke/regions/).
set -euo pipefail

CTX="${KUBE_CONTEXT:-gke_pranalab-infra_asia-southeast1-b_opensandbox}"
NS=opensandbox-system
JOB=opensandbox-loadtest
REGISTRY="${REGISTRY:-asia-southeast1-docker.pkg.dev/pranalab-infra/opensandbox}"
IMAGE="${REGRESS_IMAGE:-$REGISTRY/regress:latest}"
KC=(kubectl --context "$CTX" -n "$NS")

ARGS_JSON=$(python3 -c 'import json,sys; print(json.dumps(sys.argv[1:]))' "$@")

"${KC[@]}" delete job "$JOB" --ignore-not-found >/dev/null
"${KC[@]}" apply -f - >/dev/null <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: $JOB
  namespace: $NS
spec:
  backoffLimit: 0
  activeDeadlineSeconds: 7200
  ttlSecondsAfterFinished: 86400
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: loadtest
          image: $IMAGE
          imagePullPolicy: Always
          command: ["python", "/app/loadtest.py"]
          args: $ARGS_JSON
          env:
            - name: OPENSANDBOX_TEST_DOMAIN
              value: opensandbox-server.opensandbox-system.svc.cluster.local:80
            - name: OPENSANDBOX_TEST_API_KEY
              valueFrom:
                secretKeyRef: {name: opensandbox-api-key, key: api-key}
            - name: REGRESS_ARTIFACT_BUCKET
              value: ${REGRESS_ARTIFACT_BUCKET:-pranalab-infra-opensandbox-artifacts}
            - name: OPENSANDBOX_SANDBOX_DEFAULT_IMAGE
              value: $REGISTRY/code-interpreter:v1.1.0
          resources:
            requests: {cpu: 250m, memory: 256Mi}
            limits: {cpu: "1", memory: 1Gi}
EOF

"${KC[@]}" wait --for=condition=Ready pod -l job-name="$JOB" --timeout=180s >/dev/null 2>&1 || true
"${KC[@]}" logs -f "job/$JOB" || true
if "${KC[@]}" wait --for=condition=Complete "job/$JOB" --timeout=30s >/dev/null 2>&1; then
  echo "== loadtest PASS"
else
  echo "== loadtest FAIL (logs: kubectl --context $CTX -n $NS logs job/$JOB)"
  exit 1
fi
