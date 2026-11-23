"""Tests for settlement, lost responses, fault serialization and Kafka readiness (LM-035)."""

import json
import subprocess
import time

from tests.load import faults as fault_module
from tests.load import run as run_module
from tests.load.faults import FaultRunner, FaultSpec

BODY = {"user_id": "user-alice", "reward_id": "reward-coffee", "quantity": 1}


def submission(**overrides) -> dict:
    row = {
        "idempotency_key": "key-1",
        "body": BODY,
        "submitted_at": time.time(),
        "transaction_id": None,
        "creation": "unknown",
        "execute_call": "not_applicable",
    }
    return {**row, **overrides}


def save(tmp_path, rows: list[dict]) -> None:
    (tmp_path / "submissions.jsonl").write_text("".join(json.dumps(row) + "\n" for row in rows))


def saved(tmp_path) -> list[dict]:
    return [json.loads(line) for line in (tmp_path / "submissions.jsonl").read_text().splitlines()]


# --- lost creation responses ------------------------------------------------------


def forbid_posts(monkeypatch):
    """Settlement must never send a request that could create a transaction."""

    def no_post(*args, **kwargs):
        raise AssertionError("settlement sent a request to the Coordinator")

    monkeypatch.setattr(run_module.urllib.request, "Request", no_post)


def test_a_lost_creation_response_is_found_by_key_without_sending_anything(tmp_path, monkeypatch):
    """The Coordinator created the redemption but its answer never arrived."""
    save(tmp_path, [submission(idempotency_key="lost-response")])
    forbid_posts(monkeypatch)
    monkeypatch.setattr(
        run_module, "lookup_by_keys", lambda keys: {"lost-response": ("t-1", "COMMITTED")}
    )
    monkeypatch.setattr(run_module, "fetch_json", lambda url, timeout=3.0: {"status": "COMMITTED"})

    outcomes, unresolved, creations = run_module.resolve_transactions(tmp_path, 5)

    assert dict(creations) == {"recovered_existing": 1}
    assert dict(outcomes) == {"COMMITTED": 1}
    assert dict(unresolved) == {}
    assert saved(tmp_path)[0]["transaction_id"] == "t-1"


def test_a_request_the_coordinator_never_stored_is_never_created(tmp_path, monkeypatch):
    save(tmp_path, [submission()])
    forbid_posts(monkeypatch)
    monkeypatch.setattr(run_module, "lookup_by_keys", lambda keys: {})

    outcomes, _unresolved, creations = run_module.resolve_transactions(tmp_path, 5)

    assert dict(creations) == {"never_created": 1}
    assert dict(outcomes) == {}


def test_an_impossible_lookup_leaves_the_request_unknown(tmp_path, monkeypatch):
    save(tmp_path, [submission()])
    forbid_posts(monkeypatch)
    monkeypatch.setattr(run_module, "lookup_by_keys", lambda keys: None)

    _outcomes, _unresolved, creations = run_module.resolve_transactions(tmp_path, 1)

    assert dict(creations) == {"unknown": 1}


def test_a_refused_request_is_not_looked_up(tmp_path, monkeypatch):
    save(tmp_path, [submission(creation="rejected_http_409")])
    monkeypatch.setattr(
        run_module, "lookup_by_keys", lambda keys: (_ for _ in ()).throw(AssertionError)
    )

    _outcomes, _unresolved, creations = run_module.resolve_transactions(tmp_path, 1)

    assert dict(creations) == {"rejected_http_409": 1}


def test_the_lookup_refuses_a_database_that_is_not_local(monkeypatch):
    monkeypatch.setenv("COORDINATOR_DATABASE_URL", "postgresql+psycopg://u:p@db.example.com/x")
    assert run_module.lookup_by_keys(["k"]) is None


# --- settlement deadline ----------------------------------------------------------


def test_the_settlement_timeout_is_an_overall_deadline(tmp_path, monkeypatch):
    rows = [submission(transaction_id=f"t-{n}", creation="accepted") for n in range(30)]
    save(tmp_path, rows)
    timeouts = []

    def slow_unavailable(url, timeout=3.0):
        timeouts.append(timeout)
        time.sleep(0.3)  # an unavailable Coordinator burns the whole request timeout

    monkeypatch.setattr(run_module, "fetch_json", slow_unavailable)

    begin = time.monotonic()
    outcomes, unresolved, _creations = run_module.resolve_transactions(tmp_path, 0.6)
    elapsed = time.monotonic() - begin

    # Without a deadline per request, 30 lookups would take well over 4 seconds.
    assert elapsed < 1.8
    assert all(timeout <= 0.61 for timeout in timeouts)
    assert dict(outcomes) == {}
    assert dict(unresolved) == {"UNKNOWN": 30}


# --- faults on one service cannot interfere ---------------------------------------


def test_a_slow_fault_blocks_a_later_one_on_the_same_service(tmp_path, monkeypatch):
    """The plan looks fine (second fault starts after the first one's length), but
    the first command is slow, so the first fault is still active at 0.1 s."""
    calls = []

    def slow_inject(kind, target):
        calls.append(f"inject {kind} {target}")
        time.sleep(0.4)

    monkeypatch.setattr(fault_module, "inject", slow_inject)
    monkeypatch.setattr(fault_module, "cleanup", lambda kind, target: calls.append("cleanup"))
    monkeypatch.setattr(fault_module, "seconds_until_healthy", lambda target: 0.1)

    runner = FaultRunner(
        [FaultSpec.parse("stop:wallet@0s+0s"), FaultSpec.parse("pause:wallet@0.1s+0s")],
        tmp_path / "faults.jsonl",
        time.time(),
    )
    runner.start()
    for thread in runner._threads:
        thread.join(timeout=5)

    assert calls == ["inject stop wallet", "cleanup"]  # the second fault never ran
    assert any("earlier fault on wallet" in problem for problem in runner.problems)


# --- Kafka readiness --------------------------------------------------------------


def completed(stdout: str, returncode: int = 0) -> subprocess.CompletedProcess:
    return subprocess.CompletedProcess([], returncode, stdout=stdout, stderr="")


def test_docker_health_reads_a_json_list(monkeypatch):
    monkeypatch.setattr(
        fault_module.subprocess,
        "run",
        lambda *a, **k: completed('[{"Service": "kafka", "Health": "healthy"}]'),
    )
    assert fault_module.docker_health("kafka") == "healthy"


def test_docker_health_reads_one_object_per_line(monkeypatch):
    monkeypatch.setattr(
        fault_module.subprocess,
        "run",
        lambda *a, **k: completed('{"Service": "kafka", "Health": "starting"}\n'),
    )
    assert fault_module.docker_health("kafka") == "starting"


def test_docker_health_is_none_when_the_service_is_not_running(monkeypatch):
    monkeypatch.setattr(fault_module.subprocess, "run", lambda *a, **k: completed(""))
    assert fault_module.docker_health("kafka") is None


def test_kafka_is_not_ready_until_docker_says_healthy(monkeypatch):
    monkeypatch.setattr(fault_module, "docker_health", lambda target: "starting")
    assert fault_module.is_ready("kafka") is False
    monkeypatch.setattr(fault_module, "docker_health", lambda target: "healthy")
    assert fault_module.is_ready("kafka") is True


def test_a_kafka_that_never_becomes_ready_makes_the_run_invalid(tmp_path, monkeypatch):
    monkeypatch.setattr(fault_module, "inject", lambda kind, target: None)
    monkeypatch.setattr(fault_module, "cleanup", lambda kind, target: None)
    monkeypatch.setattr(fault_module, "RECOVERY_TIMEOUT_SECONDS", 0.3)
    monkeypatch.setattr(fault_module, "docker_health", lambda target: None)

    runner = FaultRunner(
        [FaultSpec.parse("stop:kafka@0s+0s")], tmp_path / "faults.jsonl", time.time()
    )
    runner.start()
    for thread in runner._threads:
        thread.join(timeout=5)

    assert any("not healthy again" in problem for problem in runner.problems)
    log = [json.loads(line) for line in (tmp_path / "faults.jsonl").read_text().splitlines()]
    assert log[-1]["recovered"] is False


# --- telling an undelivered request from an ambiguous one -------------------------


def test_a_refused_connection_means_the_request_was_never_delivered():
    from requests.exceptions import ConnectionError as RequestsConnectionError
    from urllib3.exceptions import MaxRetryError, NewConnectionError

    from tests.load.classify import connection_never_established

    refused = RequestsConnectionError(
        MaxRetryError(None, "/v1/redemptions", reason=NewConnectionError(None, "refused"))
    )
    assert connection_never_established(refused) is True


def test_a_connection_dropped_after_sending_is_ambiguous():
    from requests.exceptions import ConnectionError as RequestsConnectionError
    from urllib3.exceptions import ProtocolError

    from tests.load.classify import connection_never_established

    dropped = RequestsConnectionError(ProtocolError("Connection aborted.", ConnectionResetError()))
    assert connection_never_established(dropped) is False
    assert connection_never_established(None) is False
