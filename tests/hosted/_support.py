"""Shared helpers for the hosted smoke suite (LM-050).

The suite talks only to the public gateway URL. It is configured entirely from
the environment, so it never picks an environment by itself:

    LEDGERMESH_HOSTED_BASE_URL           https://<host>[:port] to test (required;
                                         without it every hosted test is skipped)
    LEDGERMESH_HOSTED_RUN_ID             run id whose fixtures were created with
                                         `deploy.sh fixtures` / `stack.sh fixtures`
    LEDGERMESH_HOSTED_REQUIRE_FIXTURES   "true": fail, instead of skipping, the
                                         redemption tests when RUN_ID is missing
    LEDGERMESH_HOSTED_CA_BUNDLE          CA file to trust (local Caddy `tls internal`
                                         only); verification is never turned off
    LEDGERMESH_HOSTED_EXPECTED_REVISION  commit the caller expects to be deployed
    LEDGERMESH_HOSTED_TIMEOUT_SECONDS    overall budget for the run (default 300)
    LEDGERMESH_HOSTED_WAIT_SECONDS       budget for one wait (default 120)
    LEDGERMESH_HOSTED_CLOSED_PORTS       comma-separated TCP ports that must not
                                         accept connections, or "none"
    LEDGERMESH_HOSTED_ARTIFACTS          where diagnostics.json goes
                                         (default artifacts/hosted-smoke)
"""

from __future__ import annotations

import json
import os
import re
import ssl
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import httpx
import pytest

RUN_ID_PATTERN = re.compile(r"^[a-z0-9]{6,40}$")
BASE_URL_PATTERN = re.compile(r"^https://[A-Za-z0-9.-]+(:[0-9]{1,5})?/?$")

# Host ports that must stay closed: SSH (IAP only), Caddy admin, the four APIs,
# PostgreSQL and every Kafka listener (infra/staging/compose.yaml, provision.sh).
DEFAULT_CLOSED_PORTS = (22, 2019, 5432, 8000, 8001, 8002, 8003, 9092, 19092, 29092, 29093)

# Must match infra/staging/fixtures.py (tests/infra/test_staging_fixtures.py
# checks it): wallet name -> points, reward name -> (point_cost, inventory).
FIXTURE_WALLETS = {"completed": 1_000, "conflict": 1_000, "insufficient": 0, "compensated": 1_000}
FIXTURE_REWARDS = {
    "completed": (100, 3),
    "conflict": (100, 1),
    "insufficient": (100, 5),
    "sold-out": (100, 0),
}

TERMINAL_STATUSES = frozenset({"COMPLETED", "FAILED", "COMPENSATED", "COMMITTED", "ABORTED"})

# Coordinator audit trail per Saga outcome: one `coordinator.transitioned`
# event per transition (services/coordinator/app/saga.py).
SAGA_AUDIT_TRAILS = {
    "COMPLETED": [
        "REQUESTED",
        "DEDUCTING_POINTS",
        "POINTS_DEDUCTED",
        "RESERVING_INVENTORY",
        "COMPLETED",
    ],
    "FAILED": ["REQUESTED", "DEDUCTING_POINTS", "FAILED"],
    "COMPENSATED": [
        "REQUESTED",
        "DEDUCTING_POINTS",
        "POINTS_DEDUCTED",
        "RESERVING_INVENTORY",
        "COMPENSATING",
        "COMPENSATED",
    ],
}


class HostedConfigError(ValueError):
    """The hosted suite's environment variables are unusable."""


def _flag(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes"}


def _positive(name: str, raw: str | None, default: float) -> float:
    if raw in (None, ""):
        return default
    try:
        value = float(raw)
    except ValueError as error:
        raise HostedConfigError(f"{name} must be a number of seconds") from error
    if value <= 0:
        raise HostedConfigError(f"{name} must be positive")
    return value


def _ports(raw: str | None) -> tuple[int, ...]:
    if raw in (None, ""):
        return DEFAULT_CLOSED_PORTS
    if raw.strip().lower() == "none":
        return ()
    try:
        ports = tuple(int(part) for part in raw.split(",") if part.strip())
    except ValueError as error:
        raise HostedConfigError("LEDGERMESH_HOSTED_CLOSED_PORTS must list port numbers") from error
    if not all(0 < port < 65536 for port in ports):
        raise HostedConfigError("LEDGERMESH_HOSTED_CLOSED_PORTS has an invalid port")
    return ports


@dataclass(frozen=True)
class Settings:
    base_url: str
    run_id: str | None
    require_fixtures: bool
    ca_bundle: str | None
    expected_revision: str | None
    timeout_seconds: float
    wait_seconds: float
    closed_ports: tuple[int, ...]
    artifacts: Path
    # A token for identifiers that need no fixtures (idempotency keys, probes).
    token: str = field(default_factory=lambda: uuid.uuid4().hex[:12])

    @property
    def host(self) -> str:
        return urlsplit(self.base_url).hostname or ""

    @property
    def port(self) -> int:
        return urlsplit(self.base_url).port or 443

    def fixture_id(self, name: str) -> str:
        if self.run_id is None:
            raise HostedConfigError("LEDGERMESH_HOSTED_RUN_ID is not set")
        return f"smoke-{self.run_id}-{name}"

    def idempotency_key(self, scenario: str) -> str:
        # Unique per run and per call; at most 128 characters (coordinator/app/main.py).
        prefix = self.run_id or self.token
        return f"smoke-{prefix}-{scenario}-{uuid.uuid4().hex}"[:128]


def load_settings(environ: dict[str, str] | None = None) -> Settings | None:
    """Settings from the environment, or None when no base URL is set."""
    env = os.environ if environ is None else environ
    base_url = env.get("LEDGERMESH_HOSTED_BASE_URL", "").strip()
    if not base_url:
        return None
    if not BASE_URL_PATTERN.fullmatch(base_url):
        raise HostedConfigError(
            "LEDGERMESH_HOSTED_BASE_URL must be https://<host>[:port] with no path"
        )
    run_id = env.get("LEDGERMESH_HOSTED_RUN_ID", "").strip() or None
    if run_id is not None and not RUN_ID_PATTERN.fullmatch(run_id):
        raise HostedConfigError("LEDGERMESH_HOSTED_RUN_ID must be 6-40 lowercase letters/digits")
    ca_bundle = env.get("LEDGERMESH_HOSTED_CA_BUNDLE", "").strip() or None
    if ca_bundle is not None and not Path(ca_bundle).is_file():
        raise HostedConfigError(f"LEDGERMESH_HOSTED_CA_BUNDLE {ca_bundle} is not a file")
    revision = env.get("LEDGERMESH_HOSTED_EXPECTED_REVISION", "").strip() or None
    if revision is not None and not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise HostedConfigError("LEDGERMESH_HOSTED_EXPECTED_REVISION must be a full commit SHA")
    timeout = _positive(
        "LEDGERMESH_HOSTED_TIMEOUT_SECONDS", env.get("LEDGERMESH_HOSTED_TIMEOUT_SECONDS"), 300
    )
    wait = _positive(
        "LEDGERMESH_HOSTED_WAIT_SECONDS", env.get("LEDGERMESH_HOSTED_WAIT_SECONDS"), 120
    )
    return Settings(
        base_url=base_url.rstrip("/"),
        run_id=run_id,
        require_fixtures=_flag(env.get("LEDGERMESH_HOSTED_REQUIRE_FIXTURES")),
        ca_bundle=ca_bundle,
        expected_revision=revision,
        timeout_seconds=timeout,
        wait_seconds=min(wait, timeout),
        closed_ports=_ports(env.get("LEDGERMESH_HOSTED_CLOSED_PORTS")),
        artifacts=Path(env.get("LEDGERMESH_HOSTED_ARTIFACTS", "") or "artifacts/hosted-smoke"),
    )


# Every hosted test module sets `pytestmark = requires_hosted`, so `make check`
# (no base URL) skips the whole suite without contacting anything.
requires_hosted = pytest.mark.skipif(
    not os.environ.get("LEDGERMESH_HOSTED_BASE_URL", "").strip(),
    reason="hosted smoke: set LEDGERMESH_HOSTED_BASE_URL to the environment to test "
    "(make hosted-smoke)",
)


def make_client(settings: Settings) -> httpx.Client:
    # TLS is always verified: against the system trust store, or against the
    # explicitly given CA file for a local Caddy `tls internal` stack.
    verify: ssl.SSLContext | bool = True
    if settings.ca_bundle:
        verify = ssl.create_default_context(cafile=settings.ca_bundle)
    return httpx.Client(
        base_url=settings.base_url,
        verify=verify,
        timeout=httpx.Timeout(10.0, connect=5.0),
        follow_redirects=False,
        headers={"User-Agent": "ledgermesh-hosted-smoke"},
    )


class Deadline:
    """The run's overall time budget; every wait is bounded by it as well."""

    def __init__(self, seconds: float, clock: Callable[[], float] = time.monotonic) -> None:
        self._clock = clock
        self._end = clock() + seconds

    def remaining(self) -> float:
        return max(0.0, self._end - self._clock())


def wait_for[T](
    fetch: Callable[[], T],
    done: Callable[[T], bool],
    *,
    what: str,
    timeout: float,
    initial_delay: float = 0.25,
    max_delay: float = 5.0,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
    describe: Callable[[T], str] = repr,
) -> T:
    """Poll ``fetch`` until ``done`` with exponential backoff and a hard timeout.

    The delay starts at ``initial_delay``, doubles up to ``max_delay`` and is
    cut short so that the total never exceeds ``timeout``.
    """
    end = clock() + timeout
    delay = initial_delay
    attempts = 0
    while True:
        value = fetch()
        attempts += 1
        if done(value):
            return value
        remaining = end - clock()
        if remaining <= 0:
            raise AssertionError(
                f"timed out after {timeout:.0f}s ({attempts} attempts) waiting for {what}; "
                f"last value: {describe(value)}"
            )
        sleep(min(delay, remaining))
        delay = min(delay * 2, max_delay)


@dataclass
class Diagnostics:
    """Secret-free record of what the suite saw, written as diagnostics.json.

    Only public data from the gateway: transaction and correlation ids, the
    statuses observed while polling, audit event summaries and balances.
    """

    settings: Settings
    redemptions: dict[str, dict[str, Any]] = field(default_factory=dict)
    notes: list[dict[str, Any]] = field(default_factory=list)
    outcomes: dict[str, str] = field(default_factory=dict)

    def redemption(self, scenario: str) -> dict[str, Any]:
        return self.redemptions.setdefault(scenario, {"status_history": []})

    def observe(self, scenario: str, body: dict[str, Any]) -> None:
        entry = self.redemption(scenario)
        for key in ("transaction_id", "correlation_id", "coordination_mode"):
            if key in body:
                entry[key] = body[key]
        history = entry["status_history"]
        status = body.get("status")
        if not history or history[-1]["status"] != status:
            history.append({"status": status, "seen_at": _utc_now()})

    def note(self, **fields: Any) -> None:
        self.notes.append({"at": _utc_now(), **fields})

    def as_dict(self) -> dict[str, Any]:
        return {
            "base_url": self.settings.base_url,
            "run_id": self.settings.run_id,
            "expected_revision": self.settings.expected_revision,
            "redemptions": self.redemptions,
            "notes": self.notes,
            "outcomes": self.outcomes,
            "written_at": _utc_now(),
        }

    def write(self) -> Path:
        self.settings.artifacts.mkdir(parents=True, exist_ok=True)
        path = self.settings.artifacts / "diagnostics.json"
        path.write_text(json.dumps(self.as_dict(), indent=2, sort_keys=True) + "\n")
        return path


def _utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def summarize_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """The audit fields worth keeping: no payload beyond the status change."""
    summary = []
    for event in events:
        envelope = event.get("envelope", {})
        payload = envelope.get("payload") or {}
        summary.append(
            {
                "ledger_sequence": event.get("ledger_sequence"),
                "event_id": envelope.get("event_id"),
                "event_type": envelope.get("event_type"),
                "correlation_id": envelope.get("correlation_id"),
                "from_status": payload.get("from_status"),
                "to_status": payload.get("to_status"),
            }
        )
    return summary
