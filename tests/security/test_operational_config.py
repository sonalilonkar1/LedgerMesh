"""Deployment files keep development conveniences local (LM-042)."""

import os
import subprocess
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
APP_SERVICES = ("coordinator", "wallet", "catalog", "audit-ledger")


def test_compose_marks_every_app_service_as_local():
    """Compose uses the development password, so it must say it is local."""
    compose = yaml.safe_load((ROOT / "infra" / "compose.yaml").read_text())

    for name in APP_SERVICES:
        assert compose["services"][name]["environment"]["LEDGERMESH_ENV"] == "local", name


def test_images_never_reload_or_declare_an_environment():
    """Images fail closed: no --reload, and LEDGERMESH_ENV comes from the deployment."""
    start = (ROOT / "infra" / "docker" / "start-service.sh").read_text()
    dockerfile = (ROOT / "infra" / "Dockerfile.service").read_text()
    compose = (ROOT / "infra" / "compose.yaml").read_text()

    for text in (start, dockerfile, compose):
        assert "--reload" not in text
    assert "LEDGERMESH_ENV" not in dockerfile
    assert "LEDGERMESH_ENV" not in start


def test_secret_scan_limits_pull_requests_to_their_own_commits():
    """A finding on another branch must not fail an unrelated PR."""
    workflow = yaml.safe_load((ROOT / ".github" / "workflows" / "security.yml").read_text())
    steps = workflow["jobs"]["secrets"]["steps"]
    scan = next(step for step in steps if "secret_scan.sh" in step.get("run", ""))
    scan_range = scan["env"]["SCAN_RANGE"]

    assert "github.event_name == 'pull_request'" in scan_range
    assert "github.event.pull_request.base.sha" in scan_range
    assert "github.event.pull_request.head.sha" in scan_range


def test_secret_scan_rejects_a_malformed_range():
    """SCAN_RANGE is validated before any clone or container runs."""
    result = subprocess.run(
        ["bash", "infra/scripts/secret_scan.sh"],
        cwd=ROOT,
        env={**os.environ, "SCAN_RANGE": "--all --exec=evil"},
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert result.returncode == 2
    assert "SCAN_RANGE must be <sha>..<sha>" in result.stderr
