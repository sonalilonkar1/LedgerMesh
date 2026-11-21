"""PostgreSQL integration tests for Catalog as a 2PC participant (LM-014)."""

from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import pytest
from sqlalchemy import Engine, func, select

from infra.scripts.prepared_transactions import list_prepared
from services.catalog.app.db.schema import participant_transactions, reservations, rewards
from services.catalog.app.errors import ParticipantConflict, ParticipantTransactionNotFound
from services.catalog.app.participant import PostgresCatalogParticipantRepository

from .conftest import upgrade

pytestmark = pytest.mark.database


@pytest.fixture
def repository(database_url: str, engine: Engine) -> PostgresCatalogParticipantRepository:
    upgrade("catalog", database_url)
    with engine.begin() as connection:
        connection.execute(
            rewards.insert().values(
                reward_id="reward-coffee",
                name="Coffee",
                point_cost=100,
                total_inventory=10,
                available_inventory=10,
                version=0,
            )
        )
    return PostgresCatalogParticipantRepository(engine)


def inventory(repository):
    with repository.engine.connect() as connection:
        row = connection.execute(
            select(rewards.c.available_inventory, rewards.c.version).where(
                rewards.c.reward_id == "reward-coffee"
            )
        ).one()
    return row.available_inventory, row.version


def reservation_count(repository):
    with repository.engine.connect() as connection:
        return connection.scalar(select(func.count()).select_from(reservations))


def prepare(repository, transaction_id=None, correlation_id=None, **overrides):
    values = {
        "transaction_id": transaction_id or uuid4(),
        "correlation_id": correlation_id or uuid4(),
        "reward_id": "reward-coffee",
        "quantity": 2,
        "expected_reward_version": 0,
        "expected_point_cost": 100,
    } | overrides
    return repository.prepare(**values), values


def test_prepare_is_durable_and_inventory_is_invisible_until_commit(repository):
    prepared, values = prepare(repository)
    assert prepared.state == "PREPARED"
    assert inventory(repository) == (10, 0)
    assert reservation_count(repository) == 0
    assert len(list_prepared(repository.engine)) == 1

    assert repository.commit(values["transaction_id"]).state == "COMMITTED"
    assert inventory(repository) == (8, 1)
    assert reservation_count(repository) == 1
    assert list_prepared(repository.engine) == []


def test_prepare_and_commit_retries_return_the_same_final_result(repository):
    transaction_id, correlation_id = uuid4(), uuid4()
    first, values = prepare(repository, transaction_id, correlation_id)
    second, _ = prepare(repository, transaction_id, correlation_id)
    committed = repository.commit(transaction_id)
    repeated = repository.commit(transaction_id)
    after_commit, _ = prepare(repository, transaction_id, correlation_id)

    assert first.state == second.state == "PREPARED"
    assert committed.state == repeated.state == after_commit.state == "COMMITTED"
    assert inventory(repository) == (8, 1)


def test_abort_rolls_back_prepared_reservation_and_is_idempotent(repository):
    prepared, values = prepare(repository)
    assert prepared.state == "PREPARED"
    assert repository.abort(values["transaction_id"]).state == "ABORTED"
    assert repository.abort(values["transaction_id"]).state == "ABORTED"
    assert inventory(repository) == (10, 0)
    assert reservation_count(repository) == 0


@pytest.mark.parametrize(
    "overrides",
    [
        {"quantity": 11},
        {"expected_reward_version": 1},
        {"expected_point_cost": 999},
        {"reward_id": "missing"},
    ],
)
def test_unavailable_inventory_or_stale_quote_is_a_durable_no_vote(repository, overrides):
    result, values = prepare(repository, **overrides)
    repeated = repository.prepare(**values)
    assert result.state == repeated.state == "ABORTED"
    assert inventory(repository) == (10, 0)
    assert list_prepared(repository.engine) == []


def test_unknown_commit_and_abort_tombstone_are_defined(repository):
    unknown_commit, unknown_abort = uuid4(), uuid4()
    with pytest.raises(ParticipantTransactionNotFound):
        repository.commit(unknown_commit)
    assert repository.abort(unknown_abort).state == "ABORTED"
    late, _ = prepare(repository, transaction_id=unknown_abort)
    assert late.state == "ABORTED"
    assert inventory(repository) == (10, 0)


def test_conflicting_prepare_and_opposite_decisions_are_rejected(repository):
    transaction_id, correlation_id = uuid4(), uuid4()
    prepare(repository, transaction_id, correlation_id)
    with pytest.raises(ParticipantConflict):
        prepare(repository, transaction_id, correlation_id, quantity=3)
    repository.commit(transaction_id)
    with pytest.raises(ParticipantConflict):
        repository.abort(transaction_id)


def test_concurrent_prepare_calls_create_one_prepared_reservation(repository):
    transaction_id, correlation_id = uuid4(), uuid4()

    def prepare_once(_index):
        result, _ = prepare(repository, transaction_id, correlation_id)
        return result.state

    with ThreadPoolExecutor(max_workers=10) as pool:
        states = list(pool.map(prepare_once, range(10)))

    assert states == ["PREPARED"] * 10
    assert len(list_prepared(repository.engine)) == 1
    with repository.engine.connect() as connection:
        count = connection.scalar(select(func.count()).select_from(participant_transactions))
    assert count == 1
    repository.abort(transaction_id)
