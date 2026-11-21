"""Behavior tests for the durable 2PC Coordinator workflow."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from services.coordinator.app.errors import InvalidTransition
from services.coordinator.app.main import app, get_two_phase_commit_workflow
from services.coordinator.app.repository import WorkflowRecord
from services.coordinator.app.two_phase_commit import (
    ParticipantUnavailable,
    ParticipantVote,
    RewardQuote,
    TwoPhaseCommitWorkflow,
)


class FakeEngine:
    class Connection:
        def execution_options(self, **_kwargs):
            return self

        def exec_driver_sql(self, *_args, **_kwargs):
            return None

        def close(self):
            return None

    def connect(self):
        return self.Connection()


class FakeRepository:
    def __init__(self):
        now = datetime.now(UTC)
        self.engine = FakeEngine()
        self.record = WorkflowRecord(
            transaction_id=uuid4(),
            correlation_id=uuid4(),
            status="REQUESTED",
            coordination_mode="two_phase_commit",
            created_at=now,
            updated_at=now,
            user_id="user-1",
            reward_id="reward-coffee",
            quantity=2,
        )
        self.log: list[str] = []

    def get_workflow(self, _transaction_id):
        return self.record

    def transition(self, _transaction_id, status, reason=None):
        del reason
        self.log.append(f"persist:{status}")
        self.record = self.record.__class__(**(self.record.__dict__ | {"status": status}))
        return self.record

    def record_quote(self, _transaction_id, point_cost, reward_version):
        self.record = self.record.__class__(
            **(
                self.record.__dict__
                | {
                    "point_cost": point_cost,
                    "reward_version": reward_version,
                    "total_points": point_cost * self.record.quantity,
                }
            )
        )
        return self.record

    def finalize_with_audit(self, transaction_id, terminal, reason):
        del reason
        self.log.append(f"persist:{terminal}+outbox")
        self.record = self.record.__class__(**(self.record.__dict__ | {"status": terminal}))
        return self.record, object()


class FakeWallet:
    def __init__(self, vote="PREPARED", fail_final=False, fail_prepare=False):
        self.vote, self.fail_final, self.fail_prepare = vote, fail_final, fail_prepare
        self.log = None

    def prepare(self, _record):
        if self.fail_prepare:
            raise ParticipantUnavailable("timeout")
        return ParticipantVote(self.vote)

    def commit(self, _transaction_id):
        self.log.append("wallet:commit")
        if self.fail_final:
            raise ParticipantUnavailable("timeout")

    def abort(self, _transaction_id):
        self.log.append("wallet:abort")
        if self.fail_final:
            raise ParticipantUnavailable("timeout")


class FakeCatalog(FakeWallet):
    def quote(self, _reward_id):
        return RewardQuote(point_cost=100, version=0)

    def commit(self, _transaction_id):
        self.log.append("catalog:commit")
        if self.fail_final:
            raise ParticipantUnavailable("timeout")

    def abort(self, _transaction_id):
        self.log.append("catalog:abort")
        if self.fail_final:
            raise ParticipantUnavailable("timeout")


def workflow(wallet=None, catalog=None):
    repository = FakeRepository()
    wallet = wallet or FakeWallet()
    catalog = catalog or FakeCatalog()
    wallet.log = catalog.log = repository.log
    return TwoPhaseCommitWorkflow(repository, wallet, catalog), repository


def test_all_yes_persists_commit_decision_before_final_messages_and_audits():
    service, repository = workflow()
    result = service.execute(repository.record.transaction_id)

    assert result.status == "COMMITTED"
    decision = repository.log.index("persist:COMMIT_DECIDED")
    assert decision < repository.log.index("wallet:commit")
    assert decision < repository.log.index("catalog:commit")
    assert repository.log[-1] == "persist:COMMITTED+outbox"


def test_no_vote_or_prepare_timeout_persists_abort_before_abort_messages():
    for wallet in (FakeWallet(vote="ABORTED"), FakeWallet(fail_prepare=True)):
        service, repository = workflow(wallet=wallet)
        result = service.execute(repository.record.transaction_id)
        assert result.status == "ABORTED"
        decision = repository.log.index("persist:ABORT_DECIDED")
        assert decision < repository.log.index("wallet:abort")
        assert decision < repository.log.index("catalog:abort")


def test_unacknowledged_decision_remains_durable_and_is_retried():
    wallet = FakeWallet(fail_final=True)
    service, repository = workflow(wallet=wallet)

    first = service.execute(repository.record.transaction_id)
    assert first.status == "COMMIT_DECIDED"

    wallet.fail_final = False
    second = service.execute(repository.record.transaction_id)
    assert second.status == "COMMITTED"
    assert repository.log.count("persist:COMMIT_DECIDED") == 1


def test_terminal_outcome_leaves_audit_delivery_to_the_outbox_publisher():
    service, repository = workflow()

    result = service.execute(repository.record.transaction_id)

    assert result.status == "COMMITTED"
    assert repository.log[-1] == "persist:COMMITTED+outbox"


def test_durable_decision_mismatch_raises_explicitly():
    service, repository = workflow()
    original_transition = repository.transition

    def conflicting_transition(transaction_id, status, reason=None):
        if status == "COMMIT_DECIDED":
            status = "ABORT_DECIDED"
        return original_transition(transaction_id, status, reason)

    repository.transition = conflicting_transition

    with pytest.raises(InvalidTransition, match="Durable decision"):
        service.execute(repository.record.transaction_id)


def test_execute_route_is_hidden_outside_local_and_test(monkeypatch):
    service, repository = workflow()
    app.dependency_overrides[get_two_phase_commit_workflow] = lambda: service
    monkeypatch.delenv("LEDGERMESH_ENV", raising=False)
    try:
        response = TestClient(app).post(
            f"/internal/v1/redemptions/{repository.record.transaction_id}/execute-2pc"
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 404
    assert repository.record.status == "REQUESTED"


def test_execute_route_is_available_in_test_environment(monkeypatch):
    service, repository = workflow()
    app.dependency_overrides[get_two_phase_commit_workflow] = lambda: service
    monkeypatch.setenv("LEDGERMESH_ENV", "test")
    try:
        response = TestClient(app).post(
            f"/internal/v1/redemptions/{repository.record.transaction_id}/execute-2pc"
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json()["status"] == "COMMITTED"
