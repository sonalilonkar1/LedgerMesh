"""Offline fakes for the LM-047 image scripts.

The scripts talk to the GHCR registry with ``curl`` and to Docker with
``docker``. These fixtures put fake versions of both first on ``PATH``, so
every response, including failures, is tested without a network or a daemon.
"""

import json
import os
import stat
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "infra" / "scripts"

# Fake curl. Answers the token request and manifest requests from FAKE_*
# variables and logs each call to FAKE_LOG.
#   FAKE_TOKEN_STATUS / FAKE_TOKEN_BODY       token exchange
#   FAKE_MANIFESTS='{"<tag or digest>": {"status": 200, "body": "...", "digest": "..."}}'
#   FAKE_MANIFEST_STATUS / _BODY, FAKE_DIGEST  any other manifest reference
#   FAKE_EXIT_ON=token|manifest, FAKE_EXIT_CODE  make curl itself fail
FAKE_CURL = f"""#!{sys.executable}
import json, os, sys

args = sys.argv[1:]
url = next(arg for arg in args if arg.startswith("https://"))
kind = "token" if "/token?" in url else "manifest"
with open(os.environ["FAKE_LOG"], "a") as log:
    log.write(json.dumps({{"kind": kind, "url": url, "args": args}}) + "\\n")

if os.environ.get("FAKE_EXIT_ON") == kind:
    sys.exit(int(os.environ["FAKE_EXIT_CODE"]))

def value(flag):
    return args[args.index(flag) + 1] if flag in args else None

if kind == "token":
    status = os.environ.get("FAKE_TOKEN_STATUS", "200")
    body = os.environ.get("FAKE_TOKEN_BODY", "")
    digest = ""
else:
    ref = url.rsplit("/manifests/", 1)[1]
    responses = json.loads(os.environ.get("FAKE_MANIFESTS", "{{}}"))
    if ref in responses:
        response = responses[ref]
        status = str(response.get("status", 200))
        body = response.get("body", "")
        digest = response.get("digest", "")
    else:
        status = os.environ.get("FAKE_MANIFEST_STATUS", "200")
        body = os.environ.get("FAKE_MANIFEST_BODY", "")
        digest = os.environ.get("FAKE_DIGEST", "")

if value("-o"):
    with open(value("-o"), "w") as out:
        out.write(body)
if value("-D"):
    with open(value("-D"), "w") as out:
        out.write(f"HTTP/2 {{status}}\\r\\n")
        if digest:
            out.write(f"Docker-Content-Digest: {{digest}}\\r\\n")
        out.write("\\r\\n")
sys.stdout.write(status)
"""


def install(path: Path, source: str) -> None:
    path.write_text(source)
    path.chmod(path.stat().st_mode | stat.S_IEXEC)


def digest(char: str) -> str:
    return "sha256:" + char * 64


def manifest_body(config: str) -> str:
    return json.dumps(
        {
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "config": {"mediaType": "application/vnd.oci.image.config.v1+json", "digest": config},
            "layers": [],
        }
    )


def index_body(entries: list[tuple[str, str, str]]) -> str:
    return json.dumps(
        {
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.index.v1+json",
            "manifests": [
                {"digest": entry, "platform": {"os": os_name, "architecture": arch}}
                for entry, os_name, arch in entries
            ],
        }
    )


def not_found_body(code: str) -> str:
    return json.dumps({"errors": [{"code": code, "message": "not found"}]})


@pytest.fixture
def registry(tmp_path: Path):
    """Run an infra script against the fake registry; returns (result, calls)."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    install(bin_dir / "curl", FAKE_CURL)
    log = tmp_path / "calls.log"
    base_env = {
        **os.environ,
        "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
        "GHCR_USER": "ci-bot",
        "GHCR_TOKEN": "workflow-token",
        "FAKE_LOG": str(log),
        "FAKE_TOKEN_BODY": json.dumps({"token": "registry-bearer"}),
    }

    def run(script: str, *args: str, env: dict[str, str] | None = None):
        result = subprocess.run(
            ["bash", str(SCRIPTS / script), *args],
            env={**base_env, **(env or {})},
            capture_output=True,
            text=True,
            check=False,
        )
        calls = []
        if log.exists():
            calls = [json.loads(line) for line in log.read_text().splitlines()]
        return result, calls

    return run
