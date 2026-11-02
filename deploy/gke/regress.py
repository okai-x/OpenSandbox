"""Regression checks for the self-hosted OpenSandbox deployment on GKE.

Run through regress.sh, which sets up port-forwards and exports:
  OPENSANDBOX_TEST_DOMAIN   lifecycle server, e.g. 127.0.0.1:18080
  OPENSANDBOX_TEST_API_KEY  server API key
  OPENSANDBOX_SANDBOX_DEFAULT_IMAGE  sandbox image in Artifact Registry
  REGRESS_CONCURRENCY       sandboxes created in parallel (default 2)
  REGRESS_ARTIFACT_BUCKET   bucket the sandbox KSA may write
  REGRESS_OTHER_BUCKET      bucket the sandbox must not reach

Each check prints PASS/FAIL; the process exits non-zero if any check fails.
"""

import json
import os
import sys
import time
import traceback
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

import httpx
from opensandbox import SandboxSync
from opensandbox.config import ConnectionConfigSync
from opensandbox.models.sandboxes import SandboxFilter
from opensandbox.sync.manager import SandboxManagerSync

DOMAIN = os.environ["OPENSANDBOX_TEST_DOMAIN"]
API_KEY = os.environ["OPENSANDBOX_TEST_API_KEY"]
IMAGE = os.environ["OPENSANDBOX_SANDBOX_DEFAULT_IMAGE"]
CONCURRENCY = int(os.getenv("REGRESS_CONCURRENCY", "2"))
RUN_ID = f"regress-{uuid.uuid4().hex[:8]}"
RESOURCE = {"cpu": "500m", "memory": "1Gi"}
ARTIFACT_BUCKET = os.getenv(
    "REGRESS_ARTIFACT_BUCKET", "pranalab-infra-opensandbox-artifacts"
)
# Any bucket the sandbox must NOT reach.
OTHER_BUCKET = os.getenv("REGRESS_OTHER_BUCKET", "pranalab-infra-ops-backup")
UPLOADER = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "sandbox_gcs_upload.py"
)

config = ConnectionConfigSync(
    domain=DOMAIN,
    protocol="http",
    api_key=API_KEY,
    # Create blocks server-side up to sandbox_create_timeout_seconds (300).
    request_timeout=timedelta(seconds=360),
)
results: list[tuple[str, bool, str]] = []


def check(name):
    def wrap(fn):
        def run(*args):
            start = time.monotonic()
            try:
                detail = fn(*args) or ""
                ok = True
            except Exception as e:  # noqa: BLE001 - report every failure and continue
                detail = f"{type(e).__name__}: {e}"
                if os.getenv("REGRESS_VERBOSE"):
                    traceback.print_exc()
                ok = False
            elapsed = time.monotonic() - start
            results.append((name, ok, detail))
            print(
                f"[{'PASS' if ok else 'FAIL'}] {name} ({elapsed:.1f}s) {detail}",
                flush=True,
            )
            return ok

        return run

    return wrap


def stdout(execution) -> str:
    return "".join(m.text for m in execution.logs.stdout)


def create(env=None) -> SandboxSync:
    return SandboxSync.create(
        IMAGE,
        connection_config=config,
        resource=RESOURCE,
        env=env,
        metadata={"regress-run": RUN_ID},
        timeout=timedelta(minutes=10),
        ready_timeout=timedelta(minutes=3),
    )


@check("server health")
def check_health():
    r = httpx.get(f"http://{DOMAIN}/health", timeout=10)
    r.raise_for_status()
    assert r.json().get("status") == "healthy", r.text
    return r.text


@check("API rejects a wrong key")
def check_auth():
    r = httpx.get(
        f"http://{DOMAIN}/v1/sandboxes",
        headers={"OPEN-SANDBOX-API-KEY": "wrong-key"},
        timeout=10,
    )
    assert r.status_code in (401, 403), f"expected 401/403, got {r.status_code}"
    return f"HTTP {r.status_code}"


@check("create sandbox")
def check_create(holder):
    holder["sb"] = create(env={"REGRESS_ENV": RUN_ID})
    return holder["sb"].id


@check("run command through gateway")
def check_command(sb):
    out = stdout(sb.commands.run("echo hello && uname -m"))
    assert out.startswith("hello"), repr(out)


@check("non-zero exit code is reported")
def check_exit_code(sb):
    ex = sb.commands.run("exit 7")
    assert ex.exit_code == 7 or ex.error is not None, f"exit_code={ex.exit_code}"


@check("create-time env is visible")
def check_env(sb):
    out = stdout(sb.commands.run("printenv REGRESS_ENV")).strip()
    assert out == RUN_ID, repr(out)


@check("file write/read round trip")
def check_files(sb):
    text = f"regress {RUN_ID}\n中文\n"
    sb.files.write_file("/tmp/regress.txt", text)
    assert sb.files.read_file("/tmp/regress.txt") == text
    out = stdout(sb.commands.run("wc -l < /tmp/regress.txt")).strip()
    assert out == "2", repr(out)


@check("python runs inside sandbox")
def check_python(sb):
    out = stdout(sb.commands.run("python3 -c 'print(sum(range(101)))'")).strip()
    assert out == "5050", repr(out)


@check("sandbox has no internet egress")
def check_no_internet(sb):
    script = (
        "import urllib.request\n"
        "try:\n"
        "    urllib.request.urlopen('https://pypi.org', timeout=5)\n"
        "    print('reachable')\n"
        "except Exception:\n"
        "    print('blocked')\n"
    )
    sb.files.write_file("/tmp/net.py", script)
    out = stdout(sb.commands.run("python3 /tmp/net.py")).strip()
    assert out == "blocked", f"internet is {out}"


@check("GCP access limited to the artifacts bucket")
def check_gcp_scope(sb):
    # Workload Identity gives the sandbox KSA (opensandbox/default) a federated
    # token, granted objectAdmin on the artifacts bucket only. The node service
    # account must stay hidden, and nothing beyond that bucket may be reachable.
    script = (
        "import json, urllib.request, urllib.error\n"
        "md = {'Metadata-Flavor': 'Google'}\n"
        "base = 'http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/'\n"
        "def get(url, headers):\n"
        "    try:\n"
        "        r = urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=10)\n"
        "        return r.status, r.read().decode()\n"
        "    except urllib.error.HTTPError as e:\n"
        "        return e.code, ''\n"
        "    except Exception as e:\n"
        "        return type(e).__name__, ''\n"
        "_, email = get(base + 'email', md)\n"
        "tok = json.loads(get(base + 'token', md)[1])['access_token']\n"
        "h = {'Authorization': 'Bearer ' + tok}\n"
        "ar = get('https://artifactregistry.googleapis.com/v1/projects/pranalab-infra/'\n"
        "         'locations/asia-southeast1/repositories/opensandbox/packages', h)[0]\n"
        f"other = get('https://storage.googleapis.com/storage/v1/b/{OTHER_BUCKET}/o', h)[0]\n"
        f"own = get('https://storage.googleapis.com/storage/v1/b/{ARTIFACT_BUCKET}/o?maxResults=1', h)[0]\n"
        "print(json.dumps({'email': email, 'ar': ar, 'other': other, 'own': own}))\n"
    )
    sb.files.write_file("/tmp/scope.py", script)
    out = json.loads(stdout(sb.commands.run("python3 /tmp/scope.py")).strip())
    assert not out["email"].endswith("gserviceaccount.com"), (
        f"identity is {out['email']}"
    )
    assert out["ar"] == 403, f"Artifact Registry -> {out['ar']}"
    assert out["other"] == 403, f"{OTHER_BUCKET} -> {out['other']}"
    assert out["own"] == 200, f"{ARTIFACT_BUCKET} -> {out['own']}"
    return f"AR={out['ar']} other-bucket={out['other']} artifacts-bucket={out['own']}"


@check("parallel upload to the artifacts bucket")
def check_gcs_upload(sb):
    with open(UPLOADER) as f:
        sb.files.write_file("/tmp/sandbox_gcs_upload.py", f.read())
    name = f"regress/{RUN_ID}.bin"
    out = stdout(
        sb.commands.run(
            "head -c 300M /dev/urandom > /tmp/up.bin && python3 /tmp/sandbox_gcs_upload.py "
            f"/tmp/up.bin gs://{ARTIFACT_BUCKET}/{name} --parts 4"
        )
    ).strip()
    result = json.loads(out.splitlines()[-1])
    assert result["size"] == 300 << 20 and result["parts"] == 4, result
    # Clean up from inside the sandbox; this also proves delete permission.
    cleanup = (
        "import json, urllib.request, urllib.parse\n"
        "md = {'Metadata-Flavor': 'Google'}\n"
        "tok = json.load(urllib.request.urlopen(urllib.request.Request(\n"
        "    'http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token', headers=md)))['access_token']\n"
        f"url = 'https://storage.googleapis.com/storage/v1/b/{ARTIFACT_BUCKET}/o/' + urllib.parse.quote('{name}', safe='')\n"
        "req = urllib.request.Request(url, method='DELETE', headers={'Authorization': 'Bearer ' + tok})\n"
        "print(urllib.request.urlopen(req).status)\n"
    )
    sb.files.write_file("/tmp/cleanup.py", cleanup)
    status = stdout(sb.commands.run("python3 /tmp/cleanup.py")).strip()
    assert status == "204", f"delete -> {status}"
    return f"{result['mib_per_s']} MiB/s"


@check("list/get via manager")
def check_manager(sb):
    mgr = SandboxManagerSync.create(config)
    page = mgr.list_sandbox_infos(SandboxFilter(metadata={"regress-run": RUN_ID}))
    ids = [i.id for i in page.sandbox_infos]
    assert sb.id in ids, f"{sb.id} not in {ids}"
    state = mgr.get_sandbox_info(sb.id).status.state
    assert state == "Running", state
    return state


@check("renew extends expiry")
def check_renew(sb):
    before = sb.get_info().expires_at
    sb.renew(timedelta(minutes=20))
    after = sb.get_info().expires_at
    assert before and after and after > before, f"{before} -> {after}"


@check("destroy removes sandbox")
def check_destroy(sb):
    sid = sb.id
    sb.destroy()
    mgr = SandboxManagerSync.create(config)
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        page = mgr.list_sandbox_infos(SandboxFilter(metadata={"regress-run": RUN_ID}))
        if sid not in [i.id for i in page.sandbox_infos]:
            return
        time.sleep(2)
    raise AssertionError(f"{sid} still listed 60s after destroy")


@check(f"{CONCURRENCY} sandboxes in parallel")
def check_concurrency():
    def one(i):
        sb = create()
        try:
            out = stdout(sb.commands.run(f"echo worker-{i}")).strip()
            assert out == f"worker-{i}", repr(out)
        finally:
            sb.destroy()

    with ThreadPoolExecutor(CONCURRENCY) as pool:
        list(pool.map(one, range(CONCURRENCY)))


def main() -> int:
    print(f"run id: {RUN_ID}  server: {DOMAIN}  image: {IMAGE}\n", flush=True)
    check_health()
    check_auth()
    holder: dict = {}
    if check_create(holder):
        sb = holder["sb"]
        try:
            for fn in (
                check_command,
                check_exit_code,
                check_env,
                check_files,
                check_python,
                check_no_internet,
                check_gcp_scope,
                check_gcs_upload,
                check_manager,
                check_renew,
            ):
                fn(sb)
        finally:
            check_destroy(sb)
    if CONCURRENCY > 0:
        check_concurrency()

    failed = [name for name, ok, _ in results if not ok]
    print(f"\n{len(results) - len(failed)}/{len(results)} passed  (run id {RUN_ID})")
    for name in failed:
        print(f"  FAILED: {name}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
