"""Write and list objects in the artifacts bucket as the orchestrator (stdlib only).

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
LIST = "https://storage.googleapis.com/storage/v1/b/{bucket}/o?"
LIST_FIELDS = "items(name,size,generation,md5Hash),nextPageToken"
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
        """Single-request upload."""
        url = UPLOAD.format(bucket=self.bucket) + urllib.parse.urlencode(
            {"uploadType": "media", "name": name}
        )
        self.call("POST", url, f"upload gs://{self.bucket}/{name}", data, content_type)

    def put_json(self, name: str, doc) -> None:
        self.put(name, json.dumps(doc, indent=2).encode(), "application/json")

    def list(self, prefix: str):
        """Objects under prefix as {name, size, generation, md5Hash}, as the JSON
        API returns them (size and generation are strings; a composite object
        has no md5Hash)."""
        query = {"prefix": prefix, "fields": LIST_FIELDS}
        items = []
        while True:
            url = LIST.format(bucket=self.bucket) + urllib.parse.urlencode(query)
            page = json.loads(
                self.call("GET", url, f"list gs://{self.bucket}/{prefix}")
            )
            items += page.get("items", [])
            if not page.get("nextPageToken"):
                return items
            query["pageToken"] = page["nextPageToken"]

    def call(
        self, method: str, url: str, what: str, data=None, content_type=None
    ) -> bytes:
        """One request, retried on throttling, server errors and network errors."""
        for attempt in range(1, ATTEMPTS + 1):
            headers = {"Authorization": f"Bearer {self.token()}"}
            if content_type:
                headers["Content-Type"] = content_type
            req = urllib.request.Request(url, data=data, method=method, headers=headers)
            try:
                with urllib.request.urlopen(req, timeout=300) as r:
                    return r.read()
            except urllib.error.HTTPError as e:
                if e.code != 429 and e.code < 500 or attempt == ATTEMPTS:
                    raise RuntimeError(f"{what}: HTTP {e.code}") from None
            except OSError as e:
                if attempt == ATTEMPTS:
                    raise RuntimeError(f"{what}: {e}") from None
            time.sleep(min(2**attempt, 30))
