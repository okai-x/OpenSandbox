# Image of the sandbox-region-ops Cloud Run Job (see region-ops.sh). Built and
# pushed by deploy/gke/region-ops/build.sh; the Job runs it as
# sandbox-region-ops@pranalab-infra.
FROM gcr.io/google.com/cloudsdktool/google-cloud-cli:582.0.0-slim

COPY region-ops.sh /usr/local/bin/region-ops.sh
ENTRYPOINT ["/usr/local/bin/region-ops.sh"]
