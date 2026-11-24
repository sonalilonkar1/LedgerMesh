"""infra/scripts/check_image.sh: a scan that did not run must not pass (LM-047).

A fake ``docker`` stands in for the container. For the forbidden-file scan it
runs the script's REAL scan shell code against a temporary directory instead
of the container's ``/``, so the find/grep exit-status handling is exercised,
not mocked. A fake ``curl`` answers ``/health``.
"""

import os
import subprocess
import sys
from pathlib import Path

import pytest

from .conftest import SCRIPTS, install

SERVICE, PORT = "wallet", "8001"

FAKE_DOCKER = f"""#!{sys.executable}
import os, subprocess, sys

args = sys.argv[1:]
command = args[0]
if command == "run":
    print("container-id")
elif command == "port":
    print("127.0.0.1:49152")
elif command == "rm":
    pass
elif command == "logs":
    print(os.environ.get("FAKE_LOGS", "starting wallet revision=abc123 version=unknown"))
elif command == "exec":
    rest = args[1:]
    if rest[:2] == ["-u", "0"]:
        rest = rest[2:]
    inner = rest[1:]
    if inner[:1] == ["id"]:
        print(os.environ.get("FAKE_ID", "10001"))
    elif inner[:1] == ["env"]:
        sys.stdout.write(os.environ.get("FAKE_ENV_OUTPUT", "PATH=/usr/bin\\nPORT=8001\\n"))
        sys.exit(int(os.environ.get("FAKE_ENV_EXIT", "0")))
    elif inner[:2] == ["sh", "-c"]:
        if os.environ.get("FAKE_SCAN_EXEC_EXIT"):
            print("Error response from daemon: container is not running", file=sys.stderr)
            sys.exit(int(os.environ["FAKE_SCAN_EXEC_EXIT"]))
        script, name = inner[2], inner[3]
        scan = subprocess.run(["sh", "-c", script, name, os.environ["FAKE_SCAN_ROOT"]])
        sys.exit(scan.returncode)
    else:
        sys.exit(64)
else:
    sys.exit(64)
"""

FAKE_CURL = f"""#!{sys.executable}
import sys
sys.stdout.write('{{"status":"ok","service":"{SERVICE}"}}')
"""


@pytest.fixture
def image_root(tmp_path: Path) -> Path:
    """A clean image filesystem: app code plus a public CA bundle."""
    root = tmp_path / "image"
    (root / "app" / "services").mkdir(parents=True)
    (root / "app" / "services" / "main.py").write_text("app = None\n")
    certifi = root / "app" / ".venv" / "lib" / "certifi"
    certifi.mkdir(parents=True)
    (certifi / "cacert.pem").write_text("-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n")
    return root


@pytest.fixture
def check_image(tmp_path: Path, image_root: Path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    install(bin_dir / "docker", FAKE_DOCKER)
    install(bin_dir / "curl", FAKE_CURL)

    def run(env: dict[str, str] | None = None):
        return subprocess.run(
            ["bash", str(SCRIPTS / "check_image.sh"), "ledgermesh-wallet:candidate", SERVICE, PORT],
            env={
                **os.environ,
                "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
                "FAKE_SCAN_ROOT": str(image_root),
                **(env or {}),
            },
            capture_output=True,
            text=True,
            check=False,
        )

    return run


def test_a_clean_image_passes(check_image):
    result = check_image()

    assert result.returncode == 0, result.stderr
    assert "ok: no .env files, git metadata, private keys or credentials" in result.stdout
    assert "ok: startup log reports revision=abc123" in result.stdout


@pytest.mark.parametrize(
    "relative",
    [".env", "app/.env.production", "app/.git", "root/.netrc", "root/.git-credentials", "etc/tls.key"],
)
def test_a_forbidden_file_fails(check_image, image_root, relative):
    path = image_root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("secret\n")

    result = check_image()

    assert result.returncode == 1
    assert "FAIL: forbidden files in image" in result.stderr
    assert relative.rsplit("/", 1)[-1] in result.stderr


def test_a_pem_holding_a_private_key_fails(check_image, image_root):
    (image_root / "app" / "deploy.pem").write_text(
        "-----BEGIN PRIVATE KEY-----\nMIIE\n-----END PRIVATE KEY-----\n"
    )

    result = check_image()

    assert result.returncode == 1
    assert "deploy.pem" in result.stderr


@pytest.mark.parametrize("exit_code", ["1", "125", "126", "137"])
def test_a_failed_docker_exec_for_the_scan_fails(check_image, exit_code):
    # e.g. the container exited before the scan: empty output must not mean "clean".
    result = check_image({"FAKE_SCAN_EXEC_EXIT": exit_code})

    assert result.returncode == 1
    assert f"the forbidden-file scan did not complete (exit {exit_code})" in result.stderr
    assert "ok: no .env files" not in result.stdout


def test_a_find_that_fails_fails_the_check(check_image, tmp_path):
    result = check_image({"FAKE_SCAN_ROOT": str(tmp_path / "does-not-exist")})

    assert result.returncode == 1
    assert "the forbidden-file scan did not complete" in result.stderr


@pytest.mark.skipif(os.geteuid() == 0, reason="root can read a mode-000 file")
def test_a_grep_error_fails_the_check_but_no_match_does_not(check_image, image_root):
    unreadable = image_root / "app" / "unreadable.pem"
    unreadable.write_text("-----BEGIN CERTIFICATE-----\n")
    unreadable.chmod(0)
    try:
        result = check_image()
    finally:
        unreadable.chmod(0o600)

    assert result.returncode == 1
    assert "the forbidden-file scan did not complete (exit 12)" in result.stderr


def test_a_failed_env_read_fails_the_check(check_image):
    result = check_image({"FAKE_ENV_EXIT": "1", "FAKE_ENV_OUTPUT": ""})

    assert result.returncode == 1
    assert "could not read the container environment" in result.stderr


@pytest.mark.parametrize("variable", ["DB_PASSWORD=x", "GITHUB_TOKEN=x", "API_SECRET=x"])
def test_a_credential_variable_fails_the_check(check_image, variable):
    result = check_image({"FAKE_ENV_OUTPUT": f"PATH=/usr/bin\n{variable}\n"})

    assert result.returncode == 1
    assert "credentials present in the image environment" in result.stderr


def test_a_root_container_fails_the_check(check_image):
    result = check_image({"FAKE_ID": "0"})

    assert result.returncode == 1
    assert "container runs as uid=0" in result.stderr
