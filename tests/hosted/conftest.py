"""Fixtures for the hosted smoke suite (LM-050). See tests/hosted/README.md."""

from __future__ import annotations

from collections.abc import Iterator

import httpx
import pytest

from tests.hosted._support import (
    Deadline,
    Diagnostics,
    HostedConfigError,
    Settings,
    load_settings,
    make_client,
)

_DIAGNOSTICS: Diagnostics | None = None


def pytest_report_header(config) -> str | None:
    try:
        settings = load_settings()
    except HostedConfigError as error:
        return f"hosted smoke: INVALID CONFIGURATION: {error}"
    if settings is None:
        return None
    return (
        f"hosted smoke: {settings.base_url} run_id={settings.run_id or '-'} "
        f"timeout={settings.timeout_seconds:.0f}s"
    )


@pytest.fixture(scope="session")
def settings() -> Settings:
    try:
        loaded = load_settings()
    except HostedConfigError as error:
        pytest.exit(f"hosted smoke configuration: {error}", returncode=4)
    if loaded is None:  # the module-level skip normally stops us first
        pytest.skip("LEDGERMESH_HOSTED_BASE_URL is not set")
    return loaded


@pytest.fixture(scope="session")
def deadline(settings: Settings) -> Deadline:
    return Deadline(settings.timeout_seconds)


@pytest.fixture(scope="session")
def diagnostics(settings: Settings) -> Iterator[Diagnostics]:
    global _DIAGNOSTICS
    _DIAGNOSTICS = Diagnostics(settings)
    yield _DIAGNOSTICS
    path = _DIAGNOSTICS.write()
    print(f"\nhosted smoke diagnostics: {path}")


@pytest.fixture(scope="session")
def client(settings: Settings, diagnostics: Diagnostics) -> Iterator[httpx.Client]:
    """A client for the public gateway, after proving it is the right service.

    An unreachable URL or a URL that is not a LedgerMesh gateway fails every
    test with one clear message instead of a pile of unrelated assertions.
    """
    with make_client(settings) as http:
        try:
            response = http.get("/health")
        except httpx.HTTPError as error:
            _stop(
                diagnostics,
                f"cannot reach the gateway {settings.base_url}/health: "
                f"{type(error).__name__}: {error}",
            )
        body = _json_or_none(response)
        if (
            response.status_code != 200
            or not isinstance(body, dict)
            or body.get("service") != "coordinator"
        ):
            _stop(
                diagnostics,
                f"{settings.base_url} is not a LedgerMesh gateway: GET /health returned "
                f"HTTP {response.status_code} {response.text[:200]!r}; expected 200 with "
                '{"status": "ok", "service": "coordinator"}',
            )
        yield http


@pytest.fixture(autouse=True)
def _gateway_checked(client: httpx.Client) -> None:
    """Every hosted test first proves the URL is a reachable LedgerMesh gateway."""


def _stop(diagnostics: Diagnostics, message: str) -> None:
    # One clear message and a failed run, instead of the same error per test.
    diagnostics.note(check="gateway", error=message)
    diagnostics.write()
    pytest.exit(f"hosted smoke FAILED: {message}", returncode=1)


@pytest.fixture
def fixtures_ready(settings: Settings) -> Settings:
    """Tests that need this run's wallets and rewards (deploy.sh fixtures)."""
    if settings.run_id is None:
        message = (
            "LEDGERMESH_HOSTED_RUN_ID is not set: create this run's fixtures with "
            "`deploy.sh fixtures <run_id>` (or `stack.sh fixtures <run_id>` locally) first"
        )
        if settings.require_fixtures:
            pytest.fail(message, pytrace=False)
        pytest.skip(message)
    return settings


def pytest_runtest_logreport(report) -> None:
    if _DIAGNOSTICS is None or "tests/hosted/" not in report.nodeid.replace("\\", "/"):
        return
    if report.when == "call" or report.outcome != "passed":
        _DIAGNOSTICS.outcomes[report.nodeid] = report.outcome


def _json_or_none(response: httpx.Response):
    try:
        return response.json()
    except ValueError:
        return None
