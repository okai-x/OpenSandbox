# Image of the sandbox-region-ops Cloud Run Job (see region-ops.sh). Built and
# pushed by deploy/gke/region-ops/build.sh from the committed deploy/gke and
# manifests/charts; the Job runs it as sandbox-region-ops@pranalab-infra.
FROM gcr.io/google.com/cloudsdktool/google-cloud-cli:582.0.0-slim

# kubectl and the GKE auth plugin from the image's own Google Cloud apt source.
RUN apt-get update \
 && apt-get install -y --no-install-recommends kubectl google-cloud-cli-gke-gcloud-auth-plugin \
 && rm -rf /var/lib/apt/lists/*

# Helm at the version the deployment is installed with, checked against its release checksum.
ARG HELM_VERSION=v4.2.4
ARG HELM_SHA256=c306b46f719b0a4da32d0f78ee21bf90ce8d602f15b22ab753f0674d1670a7f3
RUN curl -sSfL -o /tmp/helm.tgz "https://get.helm.sh/helm-${HELM_VERSION}-linux-amd64.tar.gz" \
 && echo "${HELM_SHA256}  /tmp/helm.tgz" | sha256sum -c - \
 && tar -xzf /tmp/helm.tgz -C /tmp linux-amd64/helm \
 && mv /tmp/linux-amd64/helm /usr/local/bin/helm \
 && rm -rf /tmp/helm.tgz /tmp/linux-amd64

COPY deploy/gke /opt/opensandbox/deploy/gke
COPY manifests/charts /opt/opensandbox/manifests/charts
RUN helm dependency build /opt/opensandbox/manifests/charts/opensandbox >/dev/null

ENTRYPOINT ["/opt/opensandbox/deploy/gke/region-ops/region-ops.sh"]
