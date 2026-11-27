"""The hosted smoke suite's own machinery, offline (LM-050).

The hosted tests themselves are skipped in `make check`; these check what
they rely on: configuration from the environment only, bounded exponential
backoff with no fixed sleeps, unique keys, and secret-free diagnostics.
"""

import json
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

from tests.hosted._support import (
    DEFAULT_CLOSED_PORTS,
    Deadline,
    Diagnostics,
    HostedConfigError,
    load_settings,
    summarize_events,
    wait_for,
)

ROOT = Path(__file__).resolve().parents[2]
BASE = {"LEDGERMESH_HOSTED_BASE_URL": "https://34-105-1-2.sslip.io"}


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def test_no_base_url_means_no_hosted_run():
    assert load_settings({}) is None
    assert load_settings({"LEDGERMESH_HOSTED_BASE_URL": "  "}) is None


def test_defaults():
    settings = load_settings(BASE)
    assert settings.base_url == "https://34-105-1-2.sslip.io"
    assert settings.host == "34-105-1-2.sslip.io" and settings.port == 443
    assert settings.run_id is None and settings.require_fixtures is False
    assert settings.timeout_seconds == 300 and settings.wait_seconds == 120
    assert settings.closed_ports == DEFAULT_CLOSED_PORTS
    assert {5432, 9092, 19092, 29092, 2019, 8000, 22} <= set(settings.closed_ports)


@pytest.mark.parametrize(
    ("env", "message"),
    [
        ({"LEDGERMESH_HOSTED_BASE_URL": "http://34-105-1-2.sslip.io"}, "https://"),
        ({"LEDGERMESH_HOSTED_BASE_URL": "https://host/v1"}, "no path"),
        ({"LEDGERMESH_HOSTED_RUN_ID": "Bad-Id"}, "RUN_ID"),
        ({"LEDGERMESH_HOSTED_EXPECTED_REVISION": "abc"}, "full commit SHA"),
        ({"LEDGERMESH_HOSTED_TIMEOUT_SECONDS": "0"}, "positive"),
        ({"LEDGERMESH_HOSTED_WAIT_SECONDS": "soon"}, "number"),
        ({"LEDGERMESH_HOSTED_CLOSED_PORTS": "22,x"}, "port numbers"),
        ({"LEDGERMESH_HOSTED_CLOSED_PORTS": "70000"}, "invalid port"),
        ({"LEDGERMESH_HOSTED_CA_BUNDLE": "/nonexistent/root.crt"}, "not a file"),
    ],
)
def test_invalid_configuration_is_rejected(env, message):
    with pytest.raises(HostedConfigError, match=re.escape(message)):
        load_settings({**BASE, **env})


def test_explicit_local_configuration(tmp_path):
    ca = tmp_path / "root.crt"
    ca.write_text("not checked here")
    settings = load_settings(
        {
            "LEDGERMESH_HOSTED_BASE_URL": "https://localhost:8443/",
            "LEDGERMESH_HOSTED_RUN_ID": "loc1a2b3c",
            "LEDGERMESH_HOSTED_REQUIRE_FIXTURES": "true",
            "LEDGERMESH_HOSTED_CA_BUNDLE": str(ca),
            "LEDGERMESH_HOSTED_TIMEOUT_SECONDS": "60",
            "LEDGERMESH_HOSTED_CLOSED_PORTS": "none",
            "LEDGERMESH_HOSTED_ARTIFACTS": str(tmp_path / "out"),
        }
    )
    assert settings.base_url == "https://localhost:8443"
    assert settings.port == 8443
    assert settings.require_fixtures is True
    assert settings.wait_seconds == 60, "one wait never exceeds the whole budget"
    assert settings.closed_ports == ()
    assert settings.fixture_id("completed") == "smoke-loc1a2b3c-completed"


def test_idempotency_keys_are_unique_per_call_and_fit_the_header():
    settings = load_settings({**BASE, "LEDGERMESH_HOSTED_RUN_ID": "gh99a1" + "0" * 34})
    keys = {settings.idempotency_key("completed") for _ in range(200)}
    assert len(keys) == 200
    for key in keys:
        assert key.startswith(f"smoke-{settings.run_id}-")
        assert 1 <= len(key) <= 128  # Idempotency-Key limit (coordinator/app/main.py)


def test_fixture_ids_need_a_run_id():
    with pytest.raises(HostedConfigError):
        load_settings(BASE).fixture_id("completed")


def test_wait_backs_off_exponentially_up_to_the_cap():
    clock = FakeClock()
    values = iter([1, 2, 3, 4, 5, 6, 7, 8])
    result = wait_for(
        lambda: next(values),
        lambda v: v == 8,
        what="eight",
        timeout=100,
        clock=clock,
        sleep=clock.sleep,
    )
    assert result == 8
    assert clock.sleeps == [0.25, 0.5, 1.0, 2.0, 4.0, 5.0, 5.0]


def test_wait_never_exceeds_its_timeout_and_says_what_it_waited_for():
    clock = FakeClock()
    with pytest.raises(AssertionError) as error:
        wait_for(
            lambda: "REQUESTED",
            lambda v: v == "COMPLETED",
            what="redemption X to finish",
            timeout=12,
            clock=clock,
            sleep=clock.sleep,
            describe=str,
        )
    assert clock.now == pytest.approx(12)
    assert "redemption X to finish" in str(error.value)
    assert "last value: REQUESTED" in str(error.value)


def test_deadline_counts_down_to_zero():
    clock = FakeClock()
    deadline = Deadline(10, clock=clock)
    clock.now = 4
    assert deadline.remaining() == 6
    clock.now = 30
    assert deadline.remaining() == 0


def test_hosted_tests_never_sleep_a_fixed_time():
    for path in (ROOT / "tests" / "hosted").glob("*.py"):
        text = path.read_text()
        assert "time.sleep(" not in text, f"{path.name}: use wait_for, not a fixed sleep"
        assert "asyncio.sleep(" not in text, path.name


def test_diagnostics_hold_ids_and_statuses_only(tmp_path):
    settings = load_settings(
        {
            **BASE,
            "LEDGERMESH_HOSTED_RUN_ID": "gh1a1abc",
            "LEDGERMESH_HOSTED_ARTIFACTS": str(tmp_path),
        }
    )
    diagnostics = Diagnostics(settings)
    for status in ("REQUESTED", "REQUESTED", "DEDUCTING_POINTS", "COMPLETED"):
        diagnostics.observe(
            "completed",
            {
                "transaction_id": "t-1",
                "correlation_id": "c-1",
                "coordination_mode": "saga",
                "status": status,
            },
        )
    diagnostics.redemption("completed")["audit_events"] = summarize_events(
        [
            {
                "ledger_sequence": 7,
                "envelope": {
                    "event_id": "e-1",
                    "event_type": "coordinator.transitioned",
                    "correlation_id": "c-1",
                    "payload": {"from_status": None, "to_status": "REQUESTED", "reason": "x"},
                },
            }
        ]
    )
    path = diagnostics.write()
    data = json.loads(path.read_text())
    entry = data["redemptions"]["completed"]
    assert entry["transaction_id"] == "t-1" and entry["correlation_id"] == "c-1"
    assert [h["status"] for h in entry["status_history"]] == [
        "REQUESTED",
        "DEDUCTING_POINTS",
        "COMPLETED",
    ]
    assert entry["audit_events"][0]["to_status"] == "REQUESTED"
    assert "reason" not in entry["audit_events"][0]
    assert data["run_id"] == "gh1a1abc"


def test_make_check_skips_the_hosted_suite_without_contacting_anything():
    env = {k: v for k, v in os.environ.items() if not k.startswith("LEDGERMESH_HOSTED_")}
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "tests/hosted", "-q", "-p", "no:cacheprovider"],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert re.search(r"\d+ skipped", result.stdout)
    assert "passed" not in result.stdout and "failed" not in result.stdout


def test_make_target_refuses_to_run_without_an_explicit_environment():
    env = {k: v for k, v in os.environ.items() if not k.startswith("LEDGERMESH_HOSTED_")}
    result = subprocess.run(
        ["make", "-s", "hosted-smoke"],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode != 0
    assert "LEDGERMESH_HOSTED_BASE_URL" in result.stderr
