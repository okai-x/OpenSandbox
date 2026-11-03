# Image of the sandbox-job orchestrator, which One Platform runs as Job
# opjob-<job> in namespace opensandbox-jobs. Built by build.sh from the
# committed files, with the Python SDK wheel of the same commit in dist/.
FROM python:3.12.16-slim-bookworm

COPY dist/*.whl /tmp/
RUN pip install --no-cache-dir /tmp/*.whl && rm /tmp/*.whl

# sandbox_gcs_upload.py is copied into every sandbox to upload its logs and output.
COPY deploy/gke/sandbox_gcs_upload.py /app/deploy/gke/
COPY deploy/gke/orchestrator/orchestrator.py deploy/gke/orchestrator/attempt.py \
     deploy/gke/orchestrator/records.py deploy/gke/orchestrator/source.py \
     deploy/gke/orchestrator/gcs.py deploy/gke/orchestrator/sandboxes.py \
     /app/deploy/gke/orchestrator/

USER 65532:65532
ENTRYPOINT ["python", "/app/deploy/gke/orchestrator/orchestrator.py"]
