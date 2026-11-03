"""Download a commit from GitHub and keep one directory of it as a .tar.gz."""

import io
import posixpath
import tarfile
import urllib.error
import urllib.request

API = "https://api.github.com/repos/{repo}/tarball/{commit}"


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def fetch_tarball(repo: str, commit: str, token: str) -> bytes:
    """GitHub answers with a redirect to a short-lived codeload URL. It is
    followed without the Authorization header, so the token only goes to
    api.github.com."""
    req = urllib.request.Request(
        API.format(repo=repo, commit=commit),
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    try:
        with urllib.request.build_opener(NoRedirect).open(req, timeout=60) as r:
            return r.read()
    except urllib.error.HTTPError as e:
        location = e.headers.get("Location")
        if e.code not in (301, 302, 303, 307, 308) or not location:
            raise RuntimeError(f"download {repo}@{commit}: HTTP {e.code}") from None
    with urllib.request.urlopen(location, timeout=600) as r:
        return r.read()


def extract_path(tarball: bytes, path: str) -> bytes:
    """Repack the files under path so the archive unpacks to that directory's contents.

    GitHub tarballs hold a single top-level directory <owner>-<repo>-<sha>/.
    """
    path = posixpath.normpath(path.strip("/")) if path.strip("/") else ""
    out = io.BytesIO()
    found = False
    with (
        tarfile.open(fileobj=io.BytesIO(tarball), mode="r:gz") as src,
        tarfile.open(fileobj=out, mode="w:gz") as dst,
    ):
        for member in src:
            rel = member.name.split("/", 1)[1] if "/" in member.name else ""
            rel = rel.rstrip("/")
            if path:
                if rel == path and member.isdir():
                    found = True
                if not rel.startswith(path + "/"):
                    continue
                rel = rel[len(path) + 1 :]
            elif not rel:
                found = True
                continue
            if ".." in rel.split("/"):
                continue
            member.name = rel
            dst.addfile(member, src.extractfile(member) if member.isfile() else None)
    if not found:
        raise RuntimeError(f"path {path!r} is not a directory of the commit")
    return out.getvalue()
