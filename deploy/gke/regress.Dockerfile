# In-cluster regression runner: the Python SDK plus the deployment checks.
# Built by build-regress-image.sh, which supplies the SDK wheel in dist/.
FROM python:3.12-slim

COPY dist/*.whl /tmp/
RUN pip install --no-cache-dir /tmp/*.whl && rm /tmp/*.whl

COPY regress.py sandbox_gcs_upload.py /app/
ENTRYPOINT ["python", "/app/regress.py"]
