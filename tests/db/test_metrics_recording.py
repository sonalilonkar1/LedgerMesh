"""The Coordinator and 2PC participants record LM-034 metrics against real databases."""

from uuid import uuid4

import pytest
from ledgermesh_messaging import metrics_on_commit
from prometheus_client import REGISTRY
from sqlalchemy import Engine

from services.coordinator.app.models import CreateRedemptionRequest
from services.coordinator.app.repository import PostgresCoordinatorRepository
from services.wallet.app.db.schema import wallets
from services.wallet.app.participant import PostgresWalletParticipantRepository

from .conftest import upgrade

pytestmark = pytest.mark.database


def value(name: str, **labels) -> float:
    return REGISTRY.get_sample_value(name, labels) or 0.0


def test_a_transition_rolled_back_after_its_write_is_not_counted(database_url: str, engine: Engine):
    upgrade("coordinator", database_url)
    repository = PostgresCoordinatorRepository(engine)
    created = repository.create(
        f"metrics-{uuid4()}",
        CreateRedemptionRequest(
            user_id="user-1", reward_id="reward-1", quantity=1, coordination_mode="two_phase_commit"
        ),
    )
    before = value("ledgermesh_transitions_total", mode="two_phase_commit", state="PREPARING")

    # The transition row is written, then a later step in the same transaction fails.
    with pytest.raises(RuntimeError), metrics_on_commit(), engine.begin() as connection:
        repository.transition(
            created.transaction_id, "PREPARING", "about to fail", connection=connection
        )
        raise RuntimeError("a later statement fails")

    assert repository.get(created.transaction_id).status == "REQUESTED"
    assert value("ledgermesh_transitions_total", mode="two_phase_commit", state="PREPARING") == (
        before
    )

    # Retrying the same transition after the rollback counts it exactly once.
    repository.transition(created.transaction_id, "PREPARING", "retried")
    assert value("ledgermesh_transitions_total", mode="two_phase_commit", state="PREPARING") == (
        before + 1
    )


def test_a_terminal_outcome_whose_audit_write_fails_is_not_counted(
    database_url: str, engine: Engine, monkeypatch
):
    upgrade("coordinator", database_url)
    repository = PostgresCoordinatorRepository(engine)
    created = repository.create(
        f"metrics-{uuid4()}",
        CreateRedemptionRequest(
            user_id="user-1", reward_id="reward-1", quantity=1, coordination_mode="two_phase_commit"
        ),
    )
    for state in ("PREPARING", "ABORT_DECIDED"):
        repository.transition(created.transaction_id, state, "metrics test")
    labels = {"mode": "two_phase_commit", "outcome": "ABORTED"}
    before = value("ledgermesh_redemptions_total", **labels)

    # finalize_with_audit writes ABORTED, then reads the workflow and writes the
    # outbox row; make that later read fail so the whole transaction rolls back.
    def broken(*_args, **_kwargs):
        raise RuntimeError("audit write failed")

    monkeypatch.setattr(repository, "_workflow_row", broken)
    with pytest.raises(RuntimeError):
        repository.finalize_with_audit(created.transaction_id, "ABORTED", "acknowledged")

    assert repository.get(created.transaction_id).status == "ABORT_DECIDED"
    assert value("ledgermesh_redemptions_total", **labels) == before


def test_a_2pc_redemption_records_its_outcome_and_duration(database_url: str, engine: Engine):
    upgrade("coordinator", database_url)
    repository = PostgresCoordinatorRepository(engine)
    labels = {"mode": "two_phase_commit", "outcome": "ABORTED"}
    outcomes = value("ledgermesh_redemptions_total", **labels)
    durations = value("ledgermesh_redemption_duration_seconds_count", **labels)
    requested = value("ledgermesh_transitions_total", mode="two_phase_commit", state="REQUESTED")

    created = repository.create(
        f"metrics-{uuid4()}",
        CreateRedemptionRequest(
            user_id="user-1", reward_id="reward-1", quantity=1, coordination_mode="two_phase_commit"
        ),
    )
    for state in ("PREPARING", "ABORT_DECIDED", "ABORTED"):
        repository.transition(created.transaction_id, state, "metrics test")

    assert value("ledgermesh_transitions_total", mode="two_phase_commit", state="REQUESTED") == (
        requested + 1
    )
    assert value("ledgermesh_redemptions_total", **labels) == outcomes + 1
    assert value("ledgermesh_redemption_duration_seconds_count", **labels) == durations + 1


def test_a_repeated_transition_is_not_counted_twice(database_url: str, engine: Engine):
    upgrade("coordinator", database_url)
    repository = PostgresCoordinatorRepository(engine)
    created = repository.create(
        f"metrics-{uuid4()}",
        CreateRedemptionRequest(
            user_id="user-1", reward_id="reward-1", quantity=1, coordination_mode="two_phase_commit"
        ),
    )
    repository.transition(created.transaction_id, "PREPARING", "first")
    before = value("ledgermesh_transitions_total", mode="two_phase_commit", state="PREPARING")

    repository.transition(created.transaction_id, "PREPARING", "retried after a lost response")

    assert value("ledgermesh_transitions_total", mode="two_phase_commit", state="PREPARING") == (
        before
    )


@pytest.mark.parametrize("decision", ["commit", "abort"])
def test_a_prepared_wallet_transaction_records_how_long_it_blocked(
    database_url: str, engine: Engine, decision: str
):
    upgrade("wallet", database_url)
    with engine.begin() as connection:
        connection.execute(wallets.insert().values(user_id="user-1", available_points=100))
    participant = PostgresWalletParticipantRepository(engine)
    transaction_id = uuid4()
    labels = {"service": "wallet", "decision": decision}
    before = value("ledgermesh_2pc_prepared_hold_seconds_count", **labels)

    assert participant.prepare(transaction_id, uuid4(), "user-1", 10).state == "PREPARED"
    getattr(participant, decision)(transaction_id)

    assert value("ledgermesh_2pc_prepared_hold_seconds_count", **labels) == before + 1
