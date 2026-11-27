"""Deployment files keep development conveniences local (LM-042)."""

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
