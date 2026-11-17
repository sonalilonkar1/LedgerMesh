"""PostgreSQL integration tests for Catalog core operations (LM-008)."""

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from uuid import uuid4

import pytest
from sqlalchemy import Engine, func, select

from services.catalog.app.db.schema import reservations, rewards
from services.catalog.app.errors import (
    InsufficientInventory,
    InvalidReservationTransition,
    ReservationConflict,
)
from services.catalog.app.repository import PostgresCatalogRepository

from .conftest import upgrade

pytestmark = pytest.mark.database


@pytest.fixture
def repository(database_url: str, engine: Engine) -> PostgresCatalogRepository:
    upgrade("catalog", database_url)
    with engine.begin() as connection:
        connection.execute(
            rewards.insert(),
            [
                {
                    "reward_id": "reward-coffee",
                    "name": "Coffee",
                    "point_cost": 100,
                    "total_inventory": 10,
                    "available_inventory": 10,
                },
                {
                    "reward_id": "reward-last-unit",
                    "name": "Last unit",
                    "point_cost": 500,
                    "total_inventory": 1,
                    "available_inventory": 1,
                },
                {
                    "reward_id": "reward-sold-out",
                    "name": "Sold out",
                    "point_cost": 300,
                    "total_inventory": 0,
                    "available_inventory": 0,
                },
            ],
        )
    return PostgresCatalogRepository(engine)


def reservation_count(engine: Engine) -> int:
    with engine.connect() as connection:
        return connection.scalar(select(func.count()).select_from(reservations))


def test_list_and_query_rewards(repository):
    available = repository.list_rewards()
    all_rewards = repository.list_rewards(available_only=False)
    quote = repository.get_reward("reward-coffee")

    assert [reward.reward_id for reward in available] == ["reward-coffee", "reward-last-unit"]
    assert len(all_rewards) == 3
    assert (quote.point_cost, quote.available_inventory, quote.version) == (100, 10, 0)


def test_reserve_release_and_fulfill_state_transitions(repository):
    released_id = uuid4()
    fulfilled_id = uuid4()

    reserved = repository.reserve("reward-coffee", released_id, 2, None)
    released = repository.release("reward-coffee", released_id, 2, None)
    repository.reserve("reward-coffee", fulfilled_id, 1, None)
    fulfilled = repository.fulfill("reward-coffee", fulfilled_id, None)

    assert (reserved.state, reserved.available_inventory, reserved.version) == ("reserved", 8, 1)
    assert (released.state, released.available_inventory, released.version) == ("released", 10, 2)
    assert (fulfilled.state, fulfilled.available_inventory, fulfilled.version) == (
        "fulfilled",
        9,
        3,
    )
    assert reservation_count(repository.engine) == 2


def test_sold_out_reservation_rolls_back_without_a_row(repository):
    with pytest.raises(InsufficientInventory):
        repository.reserve("reward-sold-out", uuid4(), 1, None)

    assert repository.get_reward("reward-sold-out").available_inventory == 0
    assert reservation_count(repository.engine) == 0


def test_identical_reservation_and_release_retries_are_idempotent(repository):
    transaction_id = uuid4()

    repository.reserve("reward-coffee", transaction_id, 2, None)
    duplicate_reservation = repository.reserve("reward-coffee", transaction_id, 2, None)
    repository.release("reward-coffee", transaction_id, 2, None)
    duplicate_release = repository.release("reward-coffee", transaction_id, 2, None)

    assert duplicate_reservation.duplicate is True
    assert duplicate_release.duplicate is True
    assert repository.get_reward("reward-coffee").available_inventory == 10
    assert repository.get_reward("reward-coffee").version == 2
    assert reservation_count(repository.engine) == 1


def test_release_tombstone_blocks_late_reservation(repository):
    transaction_id = uuid4()

    tombstone = repository.release("reward-coffee", transaction_id, 2, None)
    with pytest.raises(ReservationConflict):
        repository.reserve("reward-coffee", transaction_id, 2, None)

    assert tombstone.tombstone is True
    assert repository.get_reward("reward-coffee").available_inventory == 10
    assert repository.get_reward("reward-coffee").version == 0
    assert reservation_count(repository.engine) == 1


def test_invalid_transitions_do_not_change_inventory(repository):
    transaction_id = uuid4()
    repository.reserve("reward-coffee", transaction_id, 1, None)
    repository.fulfill("reward-coffee", transaction_id, None)

    with pytest.raises(InvalidReservationTransition):
        repository.release("reward-coffee", transaction_id, 1, None)

    assert repository.get_reward("reward-coffee").available_inventory == 9
    assert repository.get_reward("reward-coffee").version == 1


def test_concurrent_identical_reservations_apply_once(repository):
    transaction_id = uuid4()
    barrier = Barrier(10)

    def reserve_once(_):
        barrier.wait()
        return repository.reserve("reward-coffee", transaction_id, 1, None)

    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(reserve_once, range(10)))

    assert sum(not result.duplicate for result in results) == 1
    assert sum(result.duplicate for result in results) == 9
    assert repository.get_reward("reward-coffee").available_inventory == 9
    assert repository.get_reward("reward-coffee").version == 1
    assert reservation_count(repository.engine) == 1


def test_only_one_competing_reservation_gets_the_last_unit(repository):
    barrier = Barrier(10)

    def reserve_once(_):
        barrier.wait()
        try:
            repository.reserve("reward-last-unit", uuid4(), 1, None)
            return "reserved"
        except InsufficientInventory:
            return "sold-out"

    with ThreadPoolExecutor(max_workers=10) as executor:
        outcomes = list(executor.map(reserve_once, range(10)))

    assert outcomes.count("reserved") == 1
    assert outcomes.count("sold-out") == 9
    assert repository.get_reward("reward-last-unit").available_inventory == 0
    assert repository.get_reward("reward-last-unit").version == 1
    assert reservation_count(repository.engine) == 1


def test_caller_owned_transaction_can_roll_back_catalog_mutation(repository):
    with repository.engine.connect() as connection:
        transaction = connection.begin()
        repository.reserve("reward-coffee", uuid4(), 2, None, connection=connection)
        transaction.rollback()

    reward = repository.get_reward("reward-coffee")
    assert (reward.available_inventory, reward.version) == (10, 0)
    assert reservation_count(repository.engine) == 0
