"""Write objects to the region's artifacts bucket as the orchestrator (stdlib only).

The orchestrator Pod runs as its own KSA in namespace opensandbox-jobs, which
Workload Identity maps to a principal with roles/storage.objectAdmin on the
artifacts bucket; the GKE metadata server hands out its access token, as for
sandbox_gcs_upload.py inside sandboxes.
"""

import json
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

METADATA_TOKEN = (
    "http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token"
)
UPLOAD = "https://storage.googleapis.com/upload/storage/v1/b/{bucket}/o?"
ATTEMPTS = 5


class GCS:
    def __init__(self, bucket: str):
        self.bucket = bucket
        self._lock = threading.Lock()
        self._token = ""
        self._expires = 0.0

    def token(self) -> str:
        with self._lock:
            if time.time() > self._expires - 60:
                req = urllib.request.Request(
                    METADATA_TOKEN, headers={"Metadata-Flavor": "Google"}
                )
                with urllib.request.urlopen(req, timeout=10) as r:
                    body = json.load(r)
                self._token = body["access_token"]
                self._expires = time.time() + body["expires_in"]
            return self._token

    def put(
        self, name: str, data: bytes, content_type: str = "application/octet-stream"
    ) -> None:
        """Single-request upload, retried on throttling, server errors and network errors."""
        url = UPLOAD.format(bucket=self.bucket) + urllib.parse.urlencode(
            {"uploadType": "media", "name": name}
        )
        for attempt in range(1, ATTEMPTS + 1):
            req = urllib.request.Request(
                url,
                data=data,
                method="POST",
                headers={
                    "Authorization": f"Bearer {self.token()}",
                    "Content-Type": content_type,
                },
            )
            try:
                with urllib.request.urlopen(req, timeout=300) as r:
                    r.read()
                return
            except urllib.error.HTTPError as e:
                if e.code != 429 and e.code < 500 or attempt == ATTEMPTS:
                    raise RuntimeError(
                        f"upload gs://{self.bucket}/{name}: HTTP {e.code}"
                    ) from None
            except OSError as e:
                if attempt == ATTEMPTS:
                    raise RuntimeError(
                        f"upload gs://{self.bucket}/{name}: {e}"
                    ) from None
            time.sleep(min(2**attempt, 30))

    def put_json(self, name: str, doc) -> None:
        self.put(name, json.dumps(doc, indent=2).encode(), "application/json")
