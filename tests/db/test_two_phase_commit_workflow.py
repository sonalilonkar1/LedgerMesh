"""Durability tests for the 2PC Coordinator workflow (LM-015)."""

from uuid import uuid4

import pytest
from ledgermesh_messaging.schema import outbox
from sqlalchemy import Engine, select

from services.coordinator.app.models import CreateRedemptionRequest
from services.coordinator.app.repository import PostgresCoordinatorRepository
from services.coordinator.app.two_phase_commit import (
    ParticipantUnavailable,
    ParticipantVote,
    RewardQuote,
    TwoPhaseCommitWorkflow,
)

from .conftest import upgrade

pytestmark = pytest.mark.database


class Participant:
    def __init__(self, *, vote="PREPARED", fail_final=False, fail_prepare=False):
        self.vote, self.fail_final, self.fail_prepare = vote, fail_final, fail_prepare
        self.final_calls = []

    def quote(self, _reward_id):
        return RewardQuote(100, 0)

    def prepare(self, _record):
        if self.fail_prepare:
            raise ParticipantUnavailable("prepare timeout")
        return ParticipantVote(self.vote)

    def commit(self, transaction_id):
        self.final_calls.append(("commit", transaction_id))
        if self.fail_final:
            raise ParticipantUnavailable("commit timeout")

    def abort(self, transaction_id):
        self.final_calls.append(("abort", transaction_id))
        if self.fail_final:
            raise ParticipantUnavailable("abort timeout")


class Audit:
    def __init__(self, fail=False):
        self.fail = fail
        self.events = []

    def append(self, envelope):
        if self.fail:
            raise ParticipantUnavailable("audit unavailable")
        self.events.append(envelope)


@pytest.fixture
def repository(database_url: str, engine: Engine):
    upgrade("coordinator", database_url)
    return PostgresCoordinatorRepository(engine)


def redemption(repository):
    return repository.create(
        f"2pc-{uuid4()}",
        CreateRedemptionRequest(
            user_id="user-1",
            reward_id="reward-coffee",
            quantity=2,
            coordination_mode="two_phase_commit",
        ),
    )


def test_all_yes_persists_one_commit_decision_terminal_outcome_and_audit(repository):
    created = redemption(repository)
    wallet, catalog, audit = Participant(), Participant(), Audit()
    result = TwoPhaseCommitWorkflow(repository, wallet, catalog, audit).execute(
        created.transaction_id
    )

    assert result.status == "COMMITTED"
    transitions = repository.list_transitions(created.transaction_id)
    assert [row.to_status for row in transitions] == [
        "REQUESTED",
        "PREPARING",
        "COMMIT_DECIDED",
        "COMMITTED",
    ]
    assert len(audit.events) == 1
    assert audit.events[0].event_type == "redemption.committed"
    with repository.engine.connect() as connection:
        row = connection.execute(select(outbox)).one()
    assert row.published_at is not None


@pytest.mark.parametrize(
    "wallet",
    [Participant(vote="ABORTED"), Participant(fail_prepare=True)],
)
def test_no_vote_or_timeout_persists_one_abort_decision(repository, wallet):
    created = redemption(repository)
    result = TwoPhaseCommitWorkflow(repository, wallet, Participant(), Audit()).execute(
        created.transaction_id
    )

    assert result.status == "ABORTED"
    transitions = repository.list_transitions(created.transaction_id)
    assert [row.to_status for row in transitions].count("ABORT_DECIDED") == 1
    assert "COMMIT_DECIDED" not in [row.to_status for row in transitions]
    assert wallet.final_calls[-1][0] == "abort"


def test_unacknowledged_commit_is_retried_without_changing_decision(repository):
    created = redemption(repository)
    wallet = Participant(fail_final=True)
    workflow = TwoPhaseCommitWorkflow(repository, wallet, Participant(), Audit())

    assert workflow.execute(created.transaction_id).status == "COMMIT_DECIDED"
    wallet.fail_final = False
    assert workflow.execute(created.transaction_id).status == "COMMITTED"
    transitions = repository.list_transitions(created.transaction_id)
    assert [row.to_status for row in transitions].count("COMMIT_DECIDED") == 1


def test_audit_outage_leaves_terminal_outcome_and_unpublished_outbox(repository):
    created = redemption(repository)
    audit = Audit(fail=True)
    result = TwoPhaseCommitWorkflow(repository, Participant(), Participant(), audit).execute(
        created.transaction_id
    )

    assert result.status == "COMMITTED"
    with repository.engine.connect() as connection:
        row = connection.execute(select(outbox)).one()
    assert row.published_at is None

    audit.fail = False
    TwoPhaseCommitWorkflow(repository, Participant(), Participant(), audit).execute(
        created.transaction_id
    )
    assert len(audit.events) == 1
