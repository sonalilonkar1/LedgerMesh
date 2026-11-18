"""Behavior tests for the durable 2PC Coordinator workflow."""

from datetime import UTC, datetime
from uuid import uuid4

from ledgermesh_messaging import EventEnvelope

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
        self.pending: EventEnvelope | None = None

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
        self.pending = EventEnvelope(
            event_type=f"redemption.{terminal.lower()}",
            transaction_id=transaction_id,
            correlation_id=self.record.correlation_id,
            aggregate_type="redemption",
            aggregate_id=str(transaction_id),
            payload={"status": terminal},
        )
        return self.record, self.pending

    def pending_audit(self, _transaction_id):
        return self.pending

    def mark_audit_published(self, event_id):
        assert self.pending and self.pending.event_id == event_id
        self.log.append("audit:published")
        self.pending = None


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


class FakeAudit:
    def __init__(self, fail=False):
        self.fail = fail
        self.events = []

    def append(self, envelope):
        if self.fail:
            raise ParticipantUnavailable("audit unavailable")
        self.events.append(envelope)


def workflow(wallet=None, catalog=None, audit=None):
    repository = FakeRepository()
    wallet = wallet or FakeWallet()
    catalog = catalog or FakeCatalog()
    audit = audit or FakeAudit()
    wallet.log = catalog.log = repository.log
    return TwoPhaseCommitWorkflow(repository, wallet, catalog, audit), repository, audit


def test_all_yes_persists_commit_decision_before_final_messages_and_audits():
    service, repository, audit = workflow()
    result = service.execute(repository.record.transaction_id)

    assert result.status == "COMMITTED"
    decision = repository.log.index("persist:COMMIT_DECIDED")
    assert decision < repository.log.index("wallet:commit")
    assert decision < repository.log.index("catalog:commit")
    assert repository.log[-1] == "audit:published"
    assert audit.events[0].event_type == "redemption.committed"


def test_no_vote_or_prepare_timeout_persists_abort_before_abort_messages():
    for wallet in (FakeWallet(vote="ABORTED"), FakeWallet(fail_prepare=True)):
        service, repository, audit = workflow(wallet=wallet)
        result = service.execute(repository.record.transaction_id)
        assert result.status == "ABORTED"
        decision = repository.log.index("persist:ABORT_DECIDED")
        assert decision < repository.log.index("wallet:abort")
        assert decision < repository.log.index("catalog:abort")
        assert audit.events[0].event_type == "redemption.aborted"


def test_unacknowledged_decision_remains_durable_and_is_retried():
    wallet = FakeWallet(fail_final=True)
    service, repository, _audit = workflow(wallet=wallet)

    first = service.execute(repository.record.transaction_id)
    assert first.status == "COMMIT_DECIDED"

    wallet.fail_final = False
    second = service.execute(repository.record.transaction_id)
    assert second.status == "COMMITTED"
    assert repository.log.count("persist:COMMIT_DECIDED") == 1


def test_audit_outage_does_not_reverse_terminal_outcome_and_retry_delivers():
    audit = FakeAudit(fail=True)
    service, repository, _audit = workflow(audit=audit)
    first = service.execute(repository.record.transaction_id)
    assert first.status == "COMMITTED"
    assert repository.pending is not None

    audit.fail = False
    second = service.execute(repository.record.transaction_id)
    assert second.status == "COMMITTED"
    assert repository.pending is None
    assert len(audit.events) == 1


def test_recovery_before_durable_decision_uses_presumed_abort_without_reprepare():
    wallet = FakeWallet()
    catalog = FakeCatalog()
    service, repository, audit = workflow(wallet=wallet, catalog=catalog)
    repository.record = repository.record.__class__(
        **(repository.record.__dict__ | {"status": "PREPARING"})
    )

    result = service.recover(repository.record.transaction_id)

    assert result.status == "ABORTED"
    assert repository.log[:3] == [
        "persist:ABORT_DECIDED",
        "wallet:abort",
        "catalog:abort",
    ]
    assert audit.events[0].event_type == "redemption.aborted"


def test_recovery_after_durable_commit_retransmits_commit_without_prepare():
    wallet = FakeWallet(fail_prepare=True)
    catalog = FakeCatalog(fail_prepare=True)
    service, repository, _audit = workflow(wallet=wallet, catalog=catalog)
    repository.record = repository.record.__class__(
        **(repository.record.__dict__ | {"status": "COMMIT_DECIDED"})
    )

    result = service.recover(repository.record.transaction_id)

    assert result.status == "COMMITTED"
    assert repository.log[:2] == ["wallet:commit", "catalog:commit"]
    assert "persist:ABORT_DECIDED" not in repository.log
