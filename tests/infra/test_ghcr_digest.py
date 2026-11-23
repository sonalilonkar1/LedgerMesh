"""infra/scripts/ghcr_digest.sh: tell "not published" apart from "lookup failed" (LM-047).

The publish and release jobs only push or create a tag when this script exits
3, the registry-confirmed "not found". These tests put a fake ``curl`` first on
PATH so every registry response, and a failed request, is checked offline.
"""

import json
import os
import stat
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "infra" / "scripts" / "ghcr_digest.sh"
DIGEST = "sha256:" + "ab" * 32
NOT_FOUND = 3

# A stand-in for curl. It answers the token request and the manifest request
# from FAKE_* environment variables and logs each call to FAKE_LOG.
FAKE_CURL = f"""#!{sys.executable}
import json, os, sys

args = sys.argv[1:]
url = next(arg for arg in args if arg.startswith("https://"))
kind = "token" if "/token?" in url else "manifest"
with open(os.environ["FAKE_LOG"], "a") as log:
    log.write(json.dumps({{"kind": kind, "args": args}}) + "\\n")

if os.environ.get("FAKE_EXIT_ON") == kind:
    sys.exit(int(os.environ["FAKE_EXIT_CODE"]))

def value(flag):
    return args[args.index(flag) + 1] if flag in args else None

prefix = "FAKE_TOKEN_" if kind == "token" else "FAKE_MANIFEST_"
status = os.environ.get(prefix + "STATUS", "200")
body = os.environ.get(prefix + "BODY", "")
if value("-o"):
    with open(value("-o"), "w") as out:
        out.write(body)
if value("-D"):
    with open(value("-D"), "w") as out:
        out.write(f"HTTP/2 {{status}}\\r\\n")
        if os.environ.get("FAKE_DIGEST"):
            out.write(f"Docker-Content-Digest: {{os.environ['FAKE_DIGEST']}}\\r\\n")
        out.write("\\r\\n")
sys.stdout.write(status)
"""


@pytest.fixture
def registry(tmp_path: Path):
    fake = tmp_path / "bin" / "curl"
    fake.parent.mkdir()
    fake.write_text(FAKE_CURL)
    fake.chmod(fake.stat().st_mode | stat.S_IEXEC)
    log = tmp_path / "calls.log"
    env = {
        **os.environ,
        "PATH": f"{fake.parent}{os.pathsep}{os.environ['PATH']}",
        "GHCR_USER": "ci-bot",
        "GHCR_TOKEN": "workflow-token",
        "FAKE_LOG": str(log),
        "FAKE_TOKEN_BODY": json.dumps({"token": "registry-bearer"}),
    }

    def run(extra_env: dict[str, str] | None = None, *args: str):
        result = subprocess.run(
            ["bash", str(SCRIPT), *(args or ("owner/ledgermesh-wallet", "abc123"))],
            env={**env, **(extra_env or {})},
            capture_output=True,
            text=True,
            check=False,
        )
        calls = [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []
        return result, calls

    return run


def not_found_body(code: str) -> str:
    return json.dumps({"errors": [{"code": code, "message": "not found"}]})


def test_existing_tag_prints_its_digest(registry):
    result, calls = registry({"FAKE_MANIFEST_STATUS": "200", "FAKE_DIGEST": DIGEST})

    assert result.returncode == 0
    assert result.stdout.strip() == DIGEST
    manifest = next(call for call in calls if call["kind"] == "manifest")
    assert "Authorization: Bearer registry-bearer" in manifest["args"]
    assert "https://ghcr.io/v2/owner/ledgermesh-wallet/manifests/abc123" in manifest["args"]


@pytest.mark.parametrize("code", ["MANIFEST_UNKNOWN", "NAME_UNKNOWN"])
def test_confirmed_not_found_exits_3(registry, code):
    result, _ = registry(
        {"FAKE_MANIFEST_STATUS": "404", "FAKE_MANIFEST_BODY": not_found_body(code)}
    )

    assert result.returncode == NOT_FOUND
    assert result.stdout == ""


@pytest.mark.parametrize(
    ("status", "body"),
    [
        ("404", "<html>Not Found</html>"),
        ("404", not_found_body("DENIED")),
        ("401", not_found_body("UNAUTHORIZED")),
        ("403", not_found_body("DENIED")),
        ("429", ""),
        ("500", ""),
        ("503", ""),
    ],
)
def test_any_other_manifest_response_is_a_lookup_failure(registry, status, body):
    result, _ = registry({"FAKE_MANIFEST_STATUS": status, "FAKE_MANIFEST_BODY": body})

    assert result.returncode == 1
    assert result.stdout == ""


@pytest.mark.parametrize("curl_exit", [3, 6, 7, 28, 35])
def test_a_failed_manifest_request_is_a_lookup_failure_not_not_found(registry, curl_exit):
    # curl's own exit code 3 must never leak out as "not found".
    result, _ = registry({"FAKE_EXIT_ON": "manifest", "FAKE_EXIT_CODE": str(curl_exit)})

    assert result.returncode == 1


@pytest.mark.parametrize(
    "token_env",
    [
        {"FAKE_TOKEN_STATUS": "401"},
        {"FAKE_TOKEN_STATUS": "500"},
        {"FAKE_TOKEN_BODY": "{}"},
        {"FAKE_TOKEN_BODY": "not json"},
        {"FAKE_TOKEN_BODY": json.dumps({"token": ""})},
        {"FAKE_EXIT_ON": "token", "FAKE_EXIT_CODE": "7"},
    ],
)
def test_a_failed_token_exchange_is_a_lookup_failure(registry, token_env):
    result, calls = registry(token_env)

    assert result.returncode == 1
    assert all(call["kind"] == "token" for call in calls)


def test_ok_without_a_valid_digest_is_a_lookup_failure(registry):
    result, _ = registry({"FAKE_MANIFEST_STATUS": "200", "FAKE_DIGEST": "sha256:short"})

    assert result.returncode == 1


def test_missing_credentials_fail_before_any_request(registry):
    result, calls = registry({"GHCR_TOKEN": ""})

    assert result.returncode == 1
    assert calls == []


def test_wrong_arguments_fail(registry):
    result, calls = registry(None, "only-one-argument")

    assert result.returncode == 1
    assert calls == []
