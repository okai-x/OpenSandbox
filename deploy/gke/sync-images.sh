#!/usr/bin/env bash
# Copy every tagged image from the primary project's asia-southeast1 Artifact
# Registry repository (the source of truth) into a region's own repository,
# which is created in the region's PROJECT when missing:
#
#   deploy/gke/sync-images.sh deploy/gke/regions/asia-south1.env
#
# Nodes pull a region-local copy: a cross-region pull of the 2.6GB
# code-interpreter image took 2 minutes on a new asia-south1 node, against
# seconds from the same region. The copy runs as a Job inside the region's
# cluster (gcrane over Private Google Access), so no image data passes through
# this workstation. Re-run it after pushing new images to the source.
#
# Image streaming only serves a copy once GCP has imported it, which starts on
# the first pull and takes a while for the large image; until then nodes fall
# back to a full pull (gcfsd logs "Image import is in progress"). Copy and pull
# once well before a region takes traffic.
set -euo pipefail

[[ $# -eq 1 && -f $1 ]] || { echo "usage: $0 deploy/gke/regions/<region>.env" >&2; exit 2; }
# shellcheck source=/dev/null
source "$1"

SOURCE_PROJECT=pranalab-infra
SOURCE=asia-southeast1-docker.pkg.dev/$SOURCE_PROJECT/opensandbox
if [[ $REGISTRY == "$SOURCE" ]]; then
  echo "$REGION uses the source repository; nothing to copy"
  exit 0
fi

CTX="gke_${PROJECT}_${LOCATION}_${CLUSTER}"
SYS_NS=opensandbox-system
KSA=image-sync
JOB=opensandbox-image-sync
GCRANE=gcr.io/go-containerregistry/gcrane:v0.20.3
PROJECT_NUMBER=$(gcloud projects describe "$PROJECT" --format='value(projectNumber)')
PRINCIPAL="principal://iam.googleapis.com/projects/$PROJECT_NUMBER/locations/global/workloadIdentityPools/$PROJECT.svc.id.goog/subject/ns/$SYS_NS/sa/$KSA"
REPO_LOCATION=${REGISTRY%%-docker.pkg.dev/*}
KC=(kubectl --context "$CTX" -n "$SYS_NS")

if ! gcloud artifacts repositories describe opensandbox --location "$REPO_LOCATION" --project "$PROJECT" >/dev/null 2>&1; then
  gcloud artifacts repositories create opensandbox --repository-format docker \
    --location "$REPO_LOCATION" --project "$PROJECT" \
    --description "OpenSandbox images copied from asia-southeast1"
fi
# Nodes pull as the Compute Engine default service account, as in asia-southeast1.
gcloud artifacts repositories add-iam-policy-binding opensandbox --location "$REPO_LOCATION" --project "$PROJECT" \
  --member "serviceAccount:$PROJECT_NUMBER-compute@developer.gserviceaccount.com" \
  --role roles/artifactregistry.reader >/dev/null
gcloud artifacts repositories add-iam-policy-binding opensandbox --location "$REPO_LOCATION" --project "$PROJECT" \
  --member "$PRINCIPAL" --role roles/artifactregistry.writer >/dev/null
# The copy Job reads the source as the cluster's Workload Identity principal,
# also when the cluster is in another project.
gcloud artifacts repositories add-iam-policy-binding opensandbox --location asia-southeast1 --project "$SOURCE_PROJECT" \
  --member "$PRINCIPAL" --role roles/artifactregistry.reader >/dev/null

"${KC[@]}" create serviceaccount "$KSA" --dry-run=client -o yaml | "${KC[@]}" apply -f - >/dev/null
"${KC[@]}" delete job "$JOB" --ignore-not-found >/dev/null
"${KC[@]}" apply -f - >/dev/null <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: $JOB
spec:
  backoffLimit: 2
  ttlSecondsAfterFinished: 3600
  template:
    spec:
      serviceAccountName: $KSA
      restartPolicy: Never
      containers:
        - name: gcrane
          image: $GCRANE
          args: ["cp", "-r", "$SOURCE", "$REGISTRY"]
          resources:
            requests: {cpu: 500m, memory: 512Mi}
            limits: {cpu: "2", memory: 2Gi}
EOF
echo "copying $SOURCE -> $REGISTRY"
if "${KC[@]}" wait --for=condition=Complete "job/$JOB" --timeout=30m >/dev/null 2>&1; then
  gcloud artifacts docker images list "$REGISTRY" --include-tags --format='value(package.basename(),tags)' \
    | awk '$2 != ""'
else
  "${KC[@]}" logs "job/$JOB" --tail=20 || true
  echo "image copy failed" >&2
  exit 1
fi
