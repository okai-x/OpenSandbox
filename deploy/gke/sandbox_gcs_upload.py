"""Upload a large file from inside a sandbox to GCS (Python stdlib only).

The sandbox authenticates through Workload Identity: the GKE metadata server
returns a token for the sandbox KSA (opensandbox/default), which holds
roles/storage.objectAdmin on the dedicated artifacts bucket only.

The file is split into parts uploaded in parallel, each through its own
resumable session, then composed into the destination object:
  - a broken connection resumes the part from the offset GCS has committed;
  - each part's MD5 is computed while streaming (one disk pass) and compared
    with the md5Hash GCS reports, so corrupted parts are re-sent;
  - the composed object's size is checked, then the parts are deleted.

Usage:
  python3 sandbox_gcs_upload.py <file> gs://<bucket>/<object> [--parts N]
Prints one JSON line with size, crc32c, seconds and MiB/s.
"""

import argparse
import base64
import hashlib
import http.client
import json
import os
import sys
import threading
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor

GCS_HOST = "storage.googleapis.com"
METADATA_TOKEN = (
    "http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token"
)
MAX_COMPOSE = 32  # GCS compose accepts at most 32 source objects per call
MIN_PART = 64 << 20
ATTEMPTS = 5
BLOCK = 1 << 20

_token_lock = threading.Lock()
_token = {"value": "", "expires": 0.0}


def token() -> str:
    with _token_lock:
        if time.time() > _token["expires"] - 60:
            req = urllib.request.Request(
                METADATA_TOKEN, headers={"Metadata-Flavor": "Google"}
            )
            with urllib.request.urlopen(req, timeout=10) as r:
                body = json.load(r)
            _token["value"] = body["access_token"]
            _token["expires"] = time.time() + body["expires_in"]
        return _token["value"]


def request(method, path, body=None, headers=None, timeout=60):
    conn = http.client.HTTPSConnection(GCS_HOST, timeout=timeout, blocksize=BLOCK)
    try:
        h = {"Authorization": f"Bearer {token()}"}
        h.update(headers or {})
        conn.request(method, path, body=body, headers=h)
        resp = conn.getresponse()
        return resp.status, dict(resp.getheaders()), resp.read()
    finally:
        conn.close()


def object_path(bucket: str, name: str) -> str:
    return f"/storage/v1/b/{bucket}/o/{urllib.parse.quote(name, safe='')}"


def start_session(bucket: str, name: str, size: int) -> str:
    q = urllib.parse.urlencode({"uploadType": "resumable", "name": name})
    status, headers, body = request(
        "POST",
        f"/upload/storage/v1/b/{bucket}/o?{q}",
        body=b"",
        headers={"X-Upload-Content-Length": str(size), "Content-Length": "0"},
    )
    if status != 200:
        raise RuntimeError(f"start session {name}: HTTP {status} {body[:200]!r}")
    location = headers.get("Location") or headers.get("location")
    return urllib.parse.urlsplit(location)._replace(scheme="", netloc="").geturl()


def committed(session: str, length: int):
    """Bytes GCS already has for this session, or the final object metadata."""
    status, headers, body = request(
        "PUT",
        session,
        body=b"",
        headers={"Content-Length": "0", "Content-Range": f"bytes */{length}"},
    )
    if status in (200, 201):
        return length, json.loads(body)
    if status == 308:
        rng = headers.get("Range") or headers.get("range")
        return (int(rng.rsplit("-", 1)[1]) + 1 if rng else 0), None
    raise RuntimeError(f"session status: HTTP {status} {body[:200]!r}")


class HashingSlice:
    """File-like view of [start, end) that feeds an MD5 while it is read."""

    def __init__(self, f, start: int, end: int, md5):
        f.seek(start)
        self.f, self.left, self.md5 = f, end - start, md5

    def read(self, size: int = -1) -> bytes:
        size = self.left if size < 0 else min(size, self.left)
        data = self.f.read(size)
        self.left -= len(data)
        self.md5.update(data)
        return data


def upload_part(path: str, bucket: str, name: str, offset: int, length: int) -> dict:
    session = start_session(bucket, name, length)
    sent = 0
    for attempt in range(1, ATTEMPTS + 1):
        try:
            md5 = hashlib.md5()
            with open(path, "rb", buffering=0) as f:
                # Rebuild the digest of bytes GCS already holds after a resume.
                f.seek(offset)
                remaining = sent
                while remaining:
                    chunk = f.read(min(BLOCK, remaining))
                    md5.update(chunk)
                    remaining -= len(chunk)
                headers = {
                    "Content-Length": str(length - sent),
                    "Content-Type": "application/octet-stream",
                }
                if sent:
                    headers["Content-Range"] = f"bytes {sent}-{length - 1}/{length}"
                body = HashingSlice(f, offset + sent, offset + length, md5)
                status, _, resp = request(
                    "PUT", session, body=body, headers=headers, timeout=600
                )
            if status not in (200, 201):
                raise RuntimeError(f"HTTP {status} {resp[:200]!r}")
            meta = json.loads(resp)
            local = base64.b64encode(md5.digest()).decode()
            if meta.get("md5Hash") != local or int(meta["size"]) != length:
                # Bytes on GCS differ from the file: drop the part and resend.
                request("DELETE", object_path(bucket, name))
                session, sent = start_session(bucket, name, length), 0
                raise RuntimeError(f"md5 mismatch on {name}")
            return meta
        except Exception as e:  # noqa: BLE001 - retry any network or server error
            if attempt == ATTEMPTS:
                raise RuntimeError(f"{name} failed after {ATTEMPTS} attempts: {e}")
            time.sleep(min(2**attempt, 30))
            try:
                sent, meta = committed(session, length)
                if meta is not None:
                    # Finished but the response was lost, so the streamed MD5
                    # is incomplete: re-send the part rather than trust it.
                    sent = 0
                    session = start_session(bucket, name, length)
            except Exception:  # noqa: BLE001 - session lost: restart the part
                session, sent = start_session(bucket, name, length), 0
            print(f"retry {name} from byte {sent}: {e}", file=sys.stderr)


def compose(bucket: str, dest: str, parts: list[str]) -> dict:
    status, _, body = request(
        "POST",
        object_path(bucket, dest) + "/compose",
        body=json.dumps({"sourceObjects": [{"name": p} for p in parts]}),
        headers={"Content-Type": "application/json"},
    )
    if status != 200:
        raise RuntimeError(f"compose: HTTP {status} {body[:200]!r}")
    return json.loads(body)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("file")
    ap.add_argument("dest", help="gs://bucket/object")
    ap.add_argument("--parts", type=int, default=8)
    args = ap.parse_args()

    bucket, _, name = args.dest.removeprefix("gs://").partition("/")
    size = os.path.getsize(args.file)
    n = max(1, min(args.parts, MAX_COMPOSE, size // MIN_PART or 1))
    start = time.monotonic()

    if n == 1:
        meta = upload_part(args.file, bucket, name, 0, size)
    else:
        step = -(-size // n)
        ranges = [(i * step, min(step, size - i * step)) for i in range(n)]
        part_names = [f"{name}.part-{i:02d}-of-{n:02d}" for i in range(n)]
        with ThreadPoolExecutor(n) as pool:
            list(
                pool.map(
                    lambda i: upload_part(args.file, bucket, part_names[i], *ranges[i]),
                    range(n),
                )
            )
        meta = compose(bucket, name, part_names)
        if int(meta["size"]) != size:
            raise RuntimeError(f"composed size {meta['size']} != {size}")
        for p in part_names:
            request("DELETE", object_path(bucket, p))

    secs = time.monotonic() - start
    print(
        json.dumps(
            {
                "object": f"gs://{bucket}/{name}",
                "size": size,
                "parts": n,
                "crc32c": meta.get("crc32c"),
                "seconds": round(secs, 1),
                "mib_per_s": round(size / secs / (1 << 20)),
            }
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
