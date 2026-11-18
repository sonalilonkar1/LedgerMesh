"""Tests for bounded 2PC crash-recovery scanning and operator views."""

from datetime import UTC, datetime
from uuid import uuid4

from services.coordinator.app.recovery import TwoPhaseCommitRecovery
from services.coordinator.app.repository import WorkflowRecord


def record(status: str) -> WorkflowRecord:
    now = datetime.now(UTC)
    return WorkflowRecord(
        transaction_id=uuid4(),
        correlation_id=uuid4(),
        status=status,
        coordination_mode="two_phase_commit",
        created_at=now,
        updated_at=now,
        workflow_step=2,
    )


class Repository:
    def __init__(self, records):
        self.records = records

    def list_recoverable_2pc(self, _limit):
        return list(self.records)

    def list_in_doubt_2pc(self, _limit):
        return [item for item in self.records if item.status not in {"COMMITTED", "ABORTED"}]


class Workflow:
    def __init__(self, repository, failing_id=None):
        self.repository = repository
        self.failing_id = failing_id
        self.calls = []

    def recover(self, transaction_id):
        self.calls.append(transaction_id)
        if transaction_id == self.failing_id:
            raise RuntimeError("injected recovery failure")
        old = next(
            item for item in self.repository.records if item.transaction_id == transaction_id
        )
        terminal = old.__class__(**(old.__dict__ | {"status": "COMMITTED"}))
        self.repository.records[self.repository.records.index(old)] = terminal
        return terminal


def test_scan_recovers_each_candidate_and_reports_remaining_failures():
    first = record("COMMIT_DECIDED")
    second = record("ABORT_DECIDED")
    repository = Repository([first, second])
    workflow = Workflow(repository, failing_id=second.transaction_id)

    result = TwoPhaseCommitRecovery(repository, workflow).run_once()

    assert result.scanned == 2
    assert result.recovered == 1
    assert result.failed == 1
    assert result.still_in_doubt == 1
    assert workflow.calls == [first.transaction_id, second.transaction_id]


def test_operator_view_explains_the_safe_recovery_action():
    repository = Repository(
        [record("PREPARING"), record("COMMIT_DECIDED"), record("ABORT_DECIDED")]
    )
    recovery = TwoPhaseCommitRecovery(repository, Workflow(repository))

    views = recovery.in_doubt()

    assert [view.recovery_action for view in views] == [
        "PRESUME_ABORT",
        "RETRANSMIT_COMMIT",
        "RETRANSMIT_ABORT",
    ]
