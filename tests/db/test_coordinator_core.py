"""PostgreSQL integration tests for durable Coordinator state (LM-010)."""

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from uuid import uuid4

import pytest
from sqlalchemy import Engine, func, select

from services.coordinator.app.db.schema import redemption_transitions, redemptions
from services.coordinator.app.errors import IdempotencyConflict, InvalidTransition
from services.coordinator.app.models import CreateRedemptionRequest
from services.coordinator.app.repository import PostgresCoordinatorRepository

from .conftest import upgrade

pytestmark = pytest.mark.database


@pytest.fixture
def repository(database_url: str, engine: Engine) -> PostgresCoordinatorRepository:
    upgrade("coordinator", database_url)
    return PostgresCoordinatorRepository(engine)


def create_request(mode: str = "saga", *, quantity: int = 1) -> CreateRedemptionRequest:
    return CreateRedemptionRequest(
        user_id="user-alice",
        reward_id="reward-coffee",
        quantity=quantity,
        coordination_mode=mode,
    )


def count(engine: Engine, table) -> int:
    with engine.connect() as connection:
        return connection.scalar(select(func.count()).select_from(table))


def test_create_persists_requested_state_and_initial_transition(repository):
    created = repository.create("request-1", create_request())
    transitions = repository.list_transitions(created.transaction_id)

    assert created.status == "REQUESTED"
    assert created.coordination_mode == "saga"
    assert len(transitions) == 1
    assert (transitions[0].from_status, transitions[0].to_status) == (None, "REQUESTED")
    assert transitions[0].workflow_step == 0


def test_identical_idempotent_retry_returns_existing_transaction(repository):
    first = repository.create("same-key", create_request())
    duplicate = repository.create("same-key", create_request())

    assert duplicate.duplicate is True
    assert duplicate.transaction_id == first.transaction_id
    assert duplicate.correlation_id == first.correlation_id
    assert count(repository.engine, redemptions) == 1
    assert count(repository.engine, redemption_transitions) == 1


def test_idempotency_key_reuse_with_different_request_is_rejected(repository):
    first = repository.create("same-key", create_request())

    with pytest.raises(IdempotencyConflict) as error:
        repository.create("same-key", create_request(quantity=2))

    assert error.value.transaction_id == first.transaction_id
    assert count(repository.engine, redemptions) == 1
    assert count(repository.engine, redemption_transitions) == 1


def test_concurrent_identical_creates_return_one_transaction(repository):
    barrier = Barrier(10)

    def create_once(_):
        barrier.wait()
        return repository.create("concurrent-key", create_request())

    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(create_once, range(10)))

    assert len({result.transaction_id for result in results}) == 1
    assert sum(not result.duplicate for result in results) == 1
    assert sum(result.duplicate for result in results) == 9
    assert count(repository.engine, redemptions) == 1
    assert count(repository.engine, redemption_transitions) == 1


@pytest.mark.parametrize(
    ("mode", "states"),
    [
        ("two_phase_commit", ["PREPARING", "COMMIT_DECIDED", "COMMITTED"]),
        ("two_phase_commit", ["PREPARING", "ABORT_DECIDED", "ABORTED"]),
        ("saga", ["DEDUCTING_POINTS", "POINTS_DEDUCTED", "RESERVING_INVENTORY", "COMPLETED"]),
        ("saga", ["DEDUCTING_POINTS", "FAILED"]),
        ("saga", ["DEDUCTING_POINTS", "COMPENSATING", "COMPENSATING", "COMPENSATED"]),
        (
            "saga",
            [
                "DEDUCTING_POINTS",
                "POINTS_DEDUCTED",
                "RESERVING_INVENTORY",
                "COMPENSATING",
                "COMPENSATED",
            ],
        ),
    ],
)
def test_valid_state_paths_are_durable(repository, mode, states):
    created = repository.create(f"{mode}-{uuid4()}", create_request(mode))

    for state in states:
        current = repository.transition(created.transaction_id, state, f"enter {state}")

    transitions = repository.list_transitions(created.transaction_id)
    assert current.status == states[-1]
    assert [transition.to_status for transition in transitions] == ["REQUESTED", *states]
    assert [transition.workflow_step for transition in transitions] == list(range(len(states) + 1))


def test_repeated_final_transition_returns_existing_state_without_new_history(repository):
    created = repository.create("repeated-commit", create_request("two_phase_commit"))
    repository.transition(created.transaction_id, "PREPARING")
    repository.transition(created.transaction_id, "COMMIT_DECIDED")
    committed = repository.transition(created.transaction_id, "COMMITTED")
    history_before = repository.list_transitions(created.transaction_id)

    duplicate = repository.transition(created.transaction_id, "COMMITTED")
    history_after = repository.list_transitions(created.transaction_id)

    assert duplicate.duplicate is True
    assert duplicate.status == committed.status == "COMMITTED"
    assert history_after == history_before


def test_competing_commit_and_abort_decisions_only_record_one_winner(repository):
    created = repository.create("competing-decisions", create_request("two_phase_commit"))
    repository.transition(created.transaction_id, "PREPARING")
    barrier = Barrier(2)

    def decide(target: str):
        barrier.wait()
        try:
            return ("accepted", repository.transition(created.transaction_id, target).status)
        except InvalidTransition:
            return ("rejected", target)

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(decide, ["COMMIT_DECIDED", "ABORT_DECIDED"]))

    assert [result[0] for result in results].count("accepted") == 1
    assert [result[0] for result in results].count("rejected") == 1
    current = repository.get(created.transaction_id)
    assert current.status in {"COMMIT_DECIDED", "ABORT_DECIDED"}
    decision_rows = [
        row
        for row in repository.list_transitions(created.transaction_id)
        if row.to_status in {"COMMIT_DECIDED", "ABORT_DECIDED"}
    ]
    assert len(decision_rows) == 1


@pytest.mark.parametrize(
    ("mode", "path", "attempted_status"),
    [
        ("two_phase_commit", ["PREPARING", "COMMIT_DECIDED", "COMMITTED"], "ABORT_DECIDED"),
        ("two_phase_commit", ["PREPARING", "ABORT_DECIDED", "ABORTED"], "COMMIT_DECIDED"),
        (
            "saga",
            ["DEDUCTING_POINTS", "POINTS_DEDUCTED", "RESERVING_INVENTORY", "COMPLETED"],
            "COMPENSATING",
        ),
        ("saga", ["DEDUCTING_POINTS", "FAILED"], "COMPENSATING"),
        ("saga", ["DEDUCTING_POINTS", "COMPENSATING", "COMPENSATED"], "DEDUCTING_POINTS"),
    ],
)
def test_terminal_states_cannot_transition_to_a_different_state(
    repository, mode, path, attempted_status
):
    created = repository.create(f"terminal-{uuid4()}", create_request(mode))
    for state in path:
        repository.transition(created.transaction_id, state)
    history_before = repository.list_transitions(created.transaction_id)

    with pytest.raises(InvalidTransition):
        repository.transition(created.transaction_id, attempted_status)

    current = repository.get(created.transaction_id)
    assert current.status == path[-1]
    assert repository.list_transitions(created.transaction_id) == history_before


@pytest.mark.parametrize(
    ("mode", "invalid_status"),
    [
        ("saga", "COMMITTED"),
        ("saga", "COMPLETED"),
        ("two_phase_commit", "DEDUCTING_POINTS"),
        ("two_phase_commit", "COMMITTED"),
    ],
)
def test_invalid_transition_is_rejected_without_state_change(repository, mode, invalid_status):
    created = repository.create(f"invalid-{uuid4()}", create_request(mode))

    with pytest.raises(InvalidTransition):
        repository.transition(created.transaction_id, invalid_status)

    current = repository.get(created.transaction_id)
    assert current.status == "REQUESTED"
    assert len(repository.list_transitions(created.transaction_id)) == 1


def test_new_repository_instance_reads_state_after_restart(repository):
    created = repository.create("restart-test", create_request("two_phase_commit"))
    repository.transition(created.transaction_id, "PREPARING")

    restarted = PostgresCoordinatorRepository(repository.engine)
    current = restarted.get(created.transaction_id)
    transitions = restarted.list_transitions(created.transaction_id)

    assert current.status == "PREPARING"
    assert [transition.to_status for transition in transitions] == ["REQUESTED", "PREPARING"]


def test_caller_owned_transaction_can_roll_back_creation(repository):
    with repository.engine.connect() as connection:
        transaction = connection.begin()
        repository.create("rolled-back", create_request(), connection=connection)
        transaction.rollback()

    assert count(repository.engine, redemptions) == 0
    assert count(repository.engine, redemption_transitions) == 0
