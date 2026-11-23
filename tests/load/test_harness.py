"""Tests for the harness safety and bookkeeping behavior (LM-035)."""

import subprocess
import time

import pytest

from tests.load import faults as fault_module
from tests.load import run as run_module
from tests.load.faults import FaultError, FaultRunner, FaultSpec, validate_schedule


def specs(*texts: str) -> list[FaultSpec]:
    return [FaultSpec.parse(text) for text in texts]


# --- overlapping faults -----------------------------------------------------------


def test_overlapping_faults_on_one_service_are_rejected():
    with pytest.raises(FaultError, match="overlap"):
        validate_schedule(specs("pause:wallet@0s+10s", "pause:wallet@5s+10s"))


def test_mixed_kinds_on_one_service_cannot_overlap_either():
    with pytest.raises(FaultError, match="overlap"):
        validate_schedule(specs("kill:coordinator@0s+10s", "pause:coordinator@5s+1s"))


def test_back_to_back_and_different_service_faults_are_allowed():
    validate_schedule(
        specs("pause:wallet@0s+5s", "pause:wallet@5s+5s", "kill:catalog@2s+5s", "stop:kafka@1s+1s")
    )


# --- fault lifecycle --------------------------------------------------------------


@pytest.fixture
def lifecycle(monkeypatch, tmp_path):
    """Replace the Docker calls with fakes that record what was asked of them."""
    calls: list[str] = []
    state = {"inject_error": None, "healthy": 0.1}

    def fake_inject(kind, target):
        calls.append(f"inject {kind} {target}")
        if state["inject_error"]:
            raise state["inject_error"]

    monkeypatch.setattr(fault_module, "inject", fake_inject)
    monkeypatch.setattr(
        fault_module, "cleanup", lambda kind, target: calls.append(f"cleanup {kind} {target}")
    )
    monkeypatch.setattr(fault_module, "seconds_until_healthy", lambda target: state["healthy"])

    def run(*texts: str) -> FaultRunner:
        runner = FaultRunner(specs(*texts), tmp_path / "faults.jsonl", time.time())
        runner.start()
        for thread in runner._threads:
            thread.join(timeout=5)
        return runner

    return run, calls, state


def test_a_clean_fault_injects_cleans_up_and_is_valid(lifecycle):
    run, calls, _state = lifecycle
    runner = run("pause:wallet@0s+0s")
    assert calls == ["inject pause wallet", "cleanup pause wallet"]
    assert runner.problems == []


def test_an_injection_that_times_out_is_still_cleaned_up_and_reported(lifecycle):
    run, calls, state = lifecycle
    # docker may have paused the service before the command timed out
    state["inject_error"] = subprocess.TimeoutExpired("docker", 120)
    runner = run("pause:wallet@0s+0s")
    assert calls == ["inject pause wallet", "cleanup pause wallet"]
    assert any("could not be injected" in problem for problem in runner.problems)


def test_a_service_that_never_recovers_makes_the_run_invalid(lifecycle):
    run, _calls, state = lifecycle
    state["healthy"] = None
    runner = run("kill:wallet@0s+0s")
    assert any("did not answer /health" in problem for problem in runner.problems)


def test_a_fault_that_never_came_due_is_reported(lifecycle):
    run, calls, _state = lifecycle
    runner = run("pause:wallet@600s+5s")
    runner.finish()  # the load "ends" long before second 600
    assert calls == []
    assert any("never ran" in problem for problem in runner.problems)


# --- reset guard ------------------------------------------------------------------


def test_reset_is_refused_unless_the_environment_is_local_or_test(monkeypatch):
    monkeypatch.delenv("LEDGERMESH_ENV", raising=False)
    with pytest.raises(SystemExit, match="LEDGERMESH_ENV"):
        run_module.require_safe_reset()
    monkeypatch.setenv("LEDGERMESH_ENV", "production")
    with pytest.raises(SystemExit, match="LEDGERMESH_ENV"):
        run_module.require_safe_reset()


def test_reset_is_refused_when_a_database_is_not_on_this_machine(monkeypatch):
    monkeypatch.setenv("LEDGERMESH_ENV", "local")
    monkeypatch.setenv("WALLET_DATABASE_URL", "postgresql+psycopg://u:p@db.shared.example.com/w")
    with pytest.raises(SystemExit, match="not this machine"):
        run_module.require_safe_reset()


def test_reset_is_allowed_for_local_databases(monkeypatch):
    monkeypatch.setenv("LEDGERMESH_ENV", "test")
    for service in ("COORDINATOR", "WALLET", "CATALOG", "AUDIT_LEDGER"):
        monkeypatch.delenv(f"{service}_DATABASE_URL", raising=False)
    run_module.require_safe_reset()


# --- run IDs ----------------------------------------------------------------------


def test_run_ids_differ_even_within_the_same_second():
    ids = {run_module.make_run_id("saga", 5) for _ in range(50)}
    assert len(ids) == 50
