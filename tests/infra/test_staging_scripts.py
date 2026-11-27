"""infra/staging shell scripts, offline (LM-048).

- Every script parses (`bash -n`) and fails closed (`set -euo pipefail`).
- render-env.sh writes a 0600 file, validates values and never prints them.
- stack.sh refuses unsafe inputs, and its log bundle is redacted: a fake
  ``docker`` prints the database passwords into service logs, and the bundle
  must contain none of them.
- stack.sh's port check fails when anything but Caddy publishes a port.
"""

import json
import os
import re
import stat
import subprocess
import sys
import tarfile
from pathlib import Path

import pytest
import yaml

from .conftest import install

ROOT = Path(__file__).resolve().parents[2]
STAGING = ROOT / "infra" / "staging"
SCRIPTS = sorted(STAGING.glob("*.sh"))
PASSWORDS = {
    "COORDINATOR_DB_PASSWORD": "coordinator-test-pw-0123456789",  # gitleaks:allow
    "WALLET_DB_PASSWORD": "wallet-test-pw-0123456789",  # gitleaks:allow
    "CATALOG_DB_PASSWORD": "catalog.test~pw-0123456789",
    "AUDIT_LEDGER_DB_PASSWORD": "audit_ledger-test-pw-0123456789",  # gitleaks:allow
}
SHA = "0123456789abcdef0123456789abcdef01234567"


def write_env(path: Path, **overrides: str) -> Path:
    values = {"LEDGERMESH_SITE_ADDRESS": "localhost", "LEDGERMESH_TLS": "internal", **PASSWORDS}
    values.update(overrides)
    path.write_text("".join(f"{key}={value}\n" for key, value in values.items()))
    path.chmod(0o600)
    return path


def run(script: str, *args: str, env: dict[str, str] | None = None):
    return subprocess.run(
        ["bash", str(STAGING / script), *args],
        env={**os.environ, **(env or {})},
        capture_output=True,
        text=True,
        check=False,
    )


@pytest.mark.parametrize("script", SCRIPTS, ids=lambda path: path.name)
def test_script_parses_and_fails_closed(script):
    assert subprocess.run(["bash", "-n", str(script)], check=False).returncode == 0
    text = script.read_text()
    assert text.startswith("#!/usr/bin/env bash\n")
    assert "set -euo pipefail" in text
    assert "|| true" not in text
    assert os.access(script, os.X_OK), f"{script.name} is not executable"


# ---- render-env.sh ------------------------------------------------------------------


def test_render_from_file_writes_a_private_file_and_prints_no_values(tmp_path):
    source = write_env(tmp_path / "source.env")
    out = tmp_path / "etc" / "staging.env"

    result = run("render-env.sh", "--source", f"file:{source}", "--out", str(out))

    assert result.returncode == 0, result.stderr
    assert stat.S_IMODE(out.stat().st_mode) == 0o600
    rendered = dict(
        line.split("=", 1) for line in out.read_text().splitlines() if not line.startswith("#")
    )
    assert rendered == {
        "LEDGERMESH_SITE_ADDRESS": "localhost",
        "LEDGERMESH_TLS": "internal",
        **PASSWORDS,
    }
    for value in PASSWORDS.values():
        assert value not in result.stdout + result.stderr


def test_the_local_example_renders(tmp_path):
    out = tmp_path / "staging.env"
    result = run(
        "render-env.sh", "--source", f"file:{STAGING / '.env.local.example'}", "--out", str(out)
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"WALLET_DB_PASSWORD": "short"}, "WALLET_DB_PASSWORD"),
        ({"WALLET_DB_PASSWORD": "has@at/and:colon-0123456789"}, "WALLET_DB_PASSWORD"),
        ({"CATALOG_DB_PASSWORD": PASSWORDS["WALLET_DB_PASSWORD"]}, "must differ"),
        ({"LEDGERMESH_TLS": "not-an-email"}, "LEDGERMESH_TLS"),
        ({"LEDGERMESH_SITE_ADDRESS": "bad host"}, "LEDGERMESH_SITE_ADDRESS"),
        ({"UNEXPECTED": "x"}, "unexpected key"),
    ],
)
def test_render_rejects_bad_values_without_writing(tmp_path, overrides, message):
    source = write_env(tmp_path / "source.env", **overrides)
    out = tmp_path / "staging.env"

    result = run("render-env.sh", "--source", f"file:{source}", "--out", str(out))

    assert result.returncode != 0
    assert message in result.stderr
    assert not out.exists()


def test_render_from_gcp_reads_each_secret_and_the_metadata(tmp_path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    log = tmp_path / "calls.log"
    secrets = {f"ledgermesh-staging-{k.lower().replace('_', '-')}": v for k, v in PASSWORDS.items()}
    install(
        bin_dir / "gcloud",
        f"""#!{sys.executable}
import json, os, sys
args = sys.argv[1:]
open({str(log)!r}, "a").write(json.dumps(args) + "\\n")
name = next(a.split("=", 1)[1] for a in args if a.startswith("--secret="))
sys.stdout.write(json.loads({json.dumps(json.dumps(secrets))})[name] + "\\n")
""",
    )
    install(
        bin_dir / "curl",
        f"""#!{sys.executable}
import sys
url = sys.argv[-1]
assert "Metadata-Flavor: Google" in sys.argv
print({{"ledgermesh-site-address": "34-105-1-2.sslip.io",
        "ledgermesh-acme-email": "ops@example.com"}}[url.rsplit("/", 1)[1]], end="")
""",
    )
    out = tmp_path / "staging.env"

    result = run(
        "render-env.sh",
        "--out",
        str(out),
        env={"PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}"},
    )

    assert result.returncode == 0, result.stderr
    text = out.read_text()
    assert "LEDGERMESH_SITE_ADDRESS=34-105-1-2.sslip.io\n" in text
    assert "LEDGERMESH_TLS=ops@example.com\n" in text
    for key, value in PASSWORDS.items():
        assert f"{key}={value}\n" in text  # trailing newline from gcloud stripped
    calls = [json.loads(line) for line in log.read_text().splitlines()]
    assert sorted(a for call in calls for a in call if a.startswith("--secret=")) == sorted(
        f"--secret={name}" for name in secrets
    )


# ---- stack.sh --------------------------------------------------------------------------

# Fake docker. `docker compose ... logs` leaks every password into the output;
# `docker inspect` answers port bindings from FAKE_BINDINGS (container -> list).
FAKE_DOCKER = f"""#!{sys.executable}
import json, os, sys

args = sys.argv[1:]
passwords = json.loads(os.environ["FAKE_PASSWORDS"])
containers = json.loads(os.environ.get("FAKE_BINDINGS", "{{}}"))
if args[0] == "compose":
    rest = args[args.index("--env-file") + 2:]
    if rest[:2] == ["config", "--services"]:
        print("kafka\\nwallet\\ncaddy")
    elif rest[0] == "logs":
        print("2026-11-27T00:00:00Z connecting with " + " ".join(passwords.values()))
    elif rest[0] == "ps":
        print("NAME STATUS")
    sys.exit(0)
if args[0] == "ps":
    for name in containers:
        print(name)
    sys.exit(0)
if args[0] == "inspect":
    template, name = args[2], args[3]
    if "com.docker.compose.service" in template:
        print(name.split("-")[1])
    elif "PortBindings" in template:
        print(" ".join(containers[name]))
    elif "NetworkMode" in template:
        print("lm-backend")
    else:
        print(name + " status=running")
    sys.exit(0)
sys.exit(97)
"""


@pytest.fixture
def stack(tmp_path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    install(bin_dir / "docker", FAKE_DOCKER)
    env_file = write_env(tmp_path / "staging.env")
    data_dir = tmp_path / "data"

    def call(*args: str, bindings: dict[str, list[str]] | None = None, env_path: Path = env_file):
        return run(
            "stack.sh",
            "--env-file",
            str(env_path),
            "--data-dir",
            str(data_dir),
            "--project-name",
            "lmtest",
            "--sha",
            SHA,
            *args,
            env={
                "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
                "FAKE_PASSWORDS": json.dumps(PASSWORDS),
                "FAKE_BINDINGS": json.dumps(bindings or {}),
            },
        )

    return call


def test_collect_logs_redacts_every_password(stack, tmp_path):
    out = tmp_path / "logs.tgz"
    result = stack("collect-logs", str(out))

    assert result.returncode == 0, result.stderr
    with tarfile.open(out) as bundle:
        members = [m for m in bundle.getmembers() if m.isfile()]
        contents = "".join(bundle.extractfile(m).read().decode(errors="replace") for m in members)
    assert {Path(m.name).name for m in members} >= {
        "kafka.log",
        "wallet.log",
        "caddy.log",
        "ps.txt",
    }
    assert "[REDACTED]" in contents
    for value in PASSWORDS.values():
        assert value not in contents


def test_port_check_passes_when_only_caddy_publishes_80_and_443(stack):
    result = stack(
        "verify",
        "ports",
        bindings={"lmtest-caddy-1": ["80/tcp", "443/tcp"], "lmtest-kafka-1": []},
    )
    assert result.returncode == 0, result.stderr
    assert "only caddy publishes" in result.stdout


@pytest.mark.parametrize(
    "bindings",
    [
        {"lmtest-caddy-1": ["80/tcp", "443/tcp"], "lmtest-wallet-1": ["8001/tcp"]},
        {"lmtest-caddy-1": ["80/tcp", "443/tcp", "2019/tcp"]},
        {"lmtest-caddy-1": ["80/tcp", "443/udp"]},
        {},
    ],
)
def test_port_check_fails_on_any_other_published_port(stack, bindings):
    result = stack("verify", "ports", bindings=bindings)
    assert result.returncode != 0


def test_stack_refuses_a_readable_env_file(stack, tmp_path):
    env_file = write_env(tmp_path / "loose.env")
    env_file.chmod(0o644)
    result = stack("ps", env_path=env_file)
    assert result.returncode != 0
    assert "must be 0600" in result.stderr


@pytest.mark.parametrize(
    ("args", "message"),
    [
        (["--data-dir", "relative/data", "ps"], "absolute"),
        (["--sha", "abc123", "ps"], "40-character"),
        (["--project-name", "Bad Name", "ps"], "project-name"),
        (["verify", "everything"], "verify needs one of"),
    ],
)
def test_stack_rejects_unsafe_arguments(stack, args, message):
    result = stack(*args)
    assert result.returncode != 0
    assert message in result.stderr


def test_stack_waits_on_every_health_checked_service_and_starts_the_workers():
    # `compose up --wait` rejects containers whose health check is disabled,
    # so stack.sh waits on the data tier, APIs and Caddy, then starts the workers.
    text = (STAGING / "stack.sh").read_text()

    def array(name: str) -> set[str]:
        match = re.search(rf"^{name}=\(([^)]*)\)", text, re.MULTILINE)
        assert match, name
        return set(match.group(1).split())

    services = yaml.safe_load((STAGING / "compose.yaml").read_text())["services"]
    workers = {n for n, s in services.items() if s.get("healthcheck") == {"disable": True}}
    assert array("WORKERS") == workers
    assert array("DATA_TIER") | array("GATEWAY_TIER") == set(services) - workers


# ---- vm-startup.sh: the data disk -----------------------------------------------------


def disk_section() -> str:
    """vm-startup.sh's probe-and-format step, up to reading the disk's UUID."""
    text = (STAGING / "vm-startup.sh").read_text()
    start = text.index('if [[ ! -e "$DATA_DEVICE" ]]')
    end = text.index('uuid="$(blkid -s UUID')
    return text[start:end]


@pytest.mark.parametrize(
    ("blkid_status", "formats", "ok"),
    [(0, False, True), (2, True, True), (4, False, False), (8, False, False)],
    ids=["has-filesystem", "empty-disk", "probe-error", "ambivalent"],
)
def test_startup_formats_the_data_disk_only_when_blkid_finds_nothing(
    tmp_path, blkid_status, formats, ok
):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    install(
        bin_dir / "blkid",
        f'#!/usr/bin/env bash\necho "probe message" >&2\nexit {blkid_status}\n',
    )
    marker = tmp_path / "formatted"
    install(bin_dir / "mkfs.ext4", f'#!/usr/bin/env bash\ntouch "{marker}"\n')
    device = tmp_path / "disk"
    device.write_text("")
    script = (
        "set -euo pipefail\n"
        'log() { echo "ledgermesh-startup: $*"; }\n'
        f'DATA_DEVICE="{device}"\n' + disk_section() + "echo reached-mount\n"
    )
    result = subprocess.run(
        ["bash", "-c", script],
        env={**os.environ, "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}"},
        capture_output=True,
        text=True,
        check=False,
    )

    assert marker.exists() is formats
    assert (result.returncode == 0) is ok, result.stdout + result.stderr
    assert ("reached-mount" in result.stdout) is ok
    if not ok:
        assert f"exited {blkid_status} (probe message)" in result.stdout
