"""OpenSandbox SDK adapter: create, delete and find a job's sandboxes.

The only module that imports the SDK, so the rest is tested against a fake.
The connection comes from OPEN_SANDBOX_DOMAIN and OPEN_SANDBOX_API_KEY, which
the SDK reads itself.
"""

import time
from datetime import timedelta

from opensandbox import SandboxSync
from opensandbox.config import ConnectionConfigSync
from opensandbox.exceptions import SandboxApiException
from opensandbox.models.execd import RunCommandOpts
from opensandbox.models.sandboxes import SandboxFilter
from opensandbox.sync.manager import SandboxManagerSync

JOB_LABEL = "one-platform.io/job"
# Create blocks server-side up to sandbox_create_timeout_seconds (300).
REQUEST_TIMEOUT = timedelta(seconds=360)


class Sandboxes:
    def __init__(self, job: str):
        self.job = job

    def config(self) -> ConnectionConfigSync:
        return ConnectionConfigSync(request_timeout=REQUEST_TIMEOUT)

    def create(
        self, image: str, limits: dict, requests: dict, lifetime_s: float
    ) -> SandboxSync:
        """Create a sandbox with Kubernetes resource limits and requests;
        requests below the limits make it Burstable."""
        return SandboxSync.create(
            image,
            connection_config=self.config(),
            resource=limits,
            resource_requests=requests,
            metadata={JOB_LABEL: self.job},
            timeout=timedelta(seconds=lifetime_s),
            ready_timeout=timedelta(minutes=6),
        )

    def write(self, sb: SandboxSync, path: str, data: str | bytes) -> None:
        sb.files.write_file(path, data)

    def exec(self, sb: SandboxSync, command: str) -> tuple[int | None, str]:
        """Run a short shell command; return its exit code and stdout."""
        execution = sb.commands.run(command)
        return execution.exit_code, "".join(m.text for m in execution.logs.stdout)

    def start(self, sb: SandboxSync, command: str, envs: dict[str, str]) -> str:
        """Start a detached shell command and return its execution id. The
        values in envs reach only the command's environment (execd logs the
        command text, not its environment)."""
        execution = sb.commands.run(
            command, opts=RunCommandOpts(background=True, envs=envs)
        )
        if not execution.id:
            raise RuntimeError("execd returned no execution id")
        return execution.id

    def status(self, sb: SandboxSync, execution_id: str) -> tuple[bool, int | None]:
        status = sb.commands.get_command_status(execution_id)
        return bool(status.running), status.exit_code

    def interrupt(self, sb: SandboxSync, execution_id: str) -> None:
        sb.commands.interrupt(execution_id)

    def delete(self, sb: SandboxSync, wait_s: float) -> None:
        """Delete a sandbox and wait until the server no longer knows it, so a
        new sandbox never overlaps it."""
        try:
            sb.kill()
        finally:
            sb.close()
        with SandboxManagerSync.create(self.config()) as manager:
            deadline = time.monotonic() + wait_s
            while time.monotonic() < deadline:
                try:
                    manager.get_sandbox_info(sb.id)
                except SandboxApiException as e:
                    if e.status_code == 404:
                        return
                time.sleep(2)

    def delete_all(self) -> int:
        """Delete every sandbox labeled with the job, including any whose
        create call has not returned yet."""
        ids = []
        with SandboxManagerSync.create(self.config()) as manager:
            page = 1
            while True:
                infos = manager.list_sandbox_infos(
                    SandboxFilter(
                        metadata={JOB_LABEL: self.job}, page=page, page_size=100
                    )
                )
                ids += [info.id for info in infos.sandbox_infos]
                if not infos.pagination.has_next_page:
                    break
                page += 1
            failed = 0
            for sandbox_id in ids:
                try:
                    manager.kill_sandbox(sandbox_id)
                except SandboxApiException as e:
                    failed += e.status_code != 404
        if failed:
            raise RuntimeError(f"{failed} of {len(ids)} sandboxes could not be deleted")
        return len(ids)
