"""Catalog optimistic concurrency (LM-028).

Each test runs against a new, empty database (see conftest.py).

To make a lost race deterministic, ``RacingRepository`` commits a competing
change on another connection right after the repository has read the reward
and before it writes, which is exactly the window optimistic concurrency has
to handle.
"""

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from uuid import uuid4

import pytest
from sqlalchemy import Engine, func, select, update

from services.catalog.app.db.schema import reservations, rewards
from services.catalog.app.errors import (
    ConcurrentUpdateConflict,
    InsufficientInventory,
    InvalidReservationTransition,
    ReservationConflict,
)
from services.catalog.app.repository import PostgresCatalogRepository

from .conftest import upgrade

pytestmark = pytest.mark.database

REWARD = "reward-mug"


@pytest.fixture
def catalog_db(database_url: str, engine: Engine) -> Engine:
    upgrade("catalog", database_url)
    with engine.begin() as connection:
        connection.execute(
            rewards.insert().values(
                reward_id=REWARD,
                name="Mug",
                point_cost=100,
                total_inventory=10,
                available_inventory=10,
            )
        )
    return engine


def set_inventory(engine: Engine, available: int) -> None:
    with engine.begin() as connection:
        connection.execute(
            update(rewards)
            .where(rewards.c.reward_id == REWARD)
            .values(available_inventory=available)
        )


def reward(engine: Engine):
    with engine.connect() as connection:
        return connection.execute(select(rewards).where(rewards.c.reward_id == REWARD)).one()


def reservation_count(engine: Engine, status: str | None = None) -> int:
    statement = select(func.count()).select_from(reservations)
    if status is not None:
        statement = statement.where(reservations.c.status == status)
    with engine.connect() as connection:
        return connection.scalar(statement)


def competing_change(engine: Engine, delta: int) -> Callable[[], None]:
    """A change committed by someone else, bumping the version like any update."""

    def change() -> None:
        with engine.begin() as connection:
            connection.execute(
                update(rewards)
                .where(rewards.c.reward_id == REWARD)
                .values(
                    available_inventory=rewards.c.available_inventory + delta,
                    version=rewards.c.version + 1,
                )
            )

    return change


class RacingRepository(PostgresCatalogRepository):
    """Commits ``change`` after each of the first ``races`` reward reads."""

    def __init__(self, engine: Engine, change: Callable[[], None], races: int, **kwargs) -> None:
        super().__init__(engine, **kwargs)
        self.change = change
        self.races = races
        self.reads = 0

    def _current_reward(self, connection, reward_id):
        row = super()._current_reward(connection, reward_id)
        self.reads += 1
        if self.reads <= self.races:
            self.change()
        return row


# --- Inventory updates include the expected version ------------------------


def test_update_applies_only_at_the_expected_version(catalog_db):
    repository = PostgresCatalogRepository(catalog_db)
    before = reward(catalog_db)

    with catalog_db.begin() as connection:
        stale = repository.compare_and_set_inventory(
            connection, REWARD, expected_version=before.version + 1, delta=-1
        )
        applied = repository.compare_and_set_inventory(
            connection, REWARD, expected_version=before.version, delta=-1
        )

    assert stale is None
    assert applied is not None
    assert (applied.available_inventory, applied.version) == (9, before.version + 1)


def test_only_one_update_succeeds_for_the_same_version(catalog_db):
    repository = PostgresCatalogRepository(catalog_db)
    version = reward(catalog_db).version
    barrier = Barrier(2)

    def attempt(delta: int):
        with catalog_db.begin() as connection:
            barrier.wait()
            return repository.compare_and_set_inventory(
                connection, REWARD, expected_version=version, delta=delta
            )

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(attempt, (-1, -2)))

    winners = [result for result in results if result is not None]
    assert len(winners) == 1
    assert reward(catalog_db).version == version + 1


# --- Only one racing reservation gets the final unit -----------------------


def test_only_one_racing_reservation_gets_the_final_unit(catalog_db):
    set_inventory(catalog_db, 1)
    repository = PostgresCatalogRepository(catalog_db)
    barrier = Barrier(10)

    def reserve(_index: int) -> str:
        barrier.wait()
        try:
            repository.reserve(REWARD, uuid4(), 1, None)
        except InsufficientInventory:
            return "sold_out"
        return "reserved"

    with ThreadPoolExecutor(max_workers=10) as pool:
        outcomes = list(pool.map(reserve, range(10)))

    assert outcomes.count("reserved") == 1
    assert outcomes.count("sold_out") == 9
    assert reward(catalog_db).available_inventory == 0
    assert reservation_count(catalog_db) == 1


def test_concurrent_reservations_never_oversell(catalog_db):
    set_inventory(catalog_db, 5)
    repository = PostgresCatalogRepository(catalog_db)
    barrier = Barrier(20)

    def reserve(_index: int) -> str:
        barrier.wait()
        try:
            repository.reserve(REWARD, uuid4(), 1, None)
        except InsufficientInventory:
            return "sold_out"
        return "reserved"

    with ThreadPoolExecutor(max_workers=20) as pool:
        outcomes = list(pool.map(reserve, range(20)))

    # Every request is answered against the inventory it saw; none is lost to
    # a conflict and none oversells.
    assert outcomes.count("reserved") == 5
    assert outcomes.count("sold_out") == 15
    current = reward(catalog_db)
    assert current.available_inventory == 0
    assert reservation_count(catalog_db, "reserved") == 5


def test_concurrent_duplicate_reservations_apply_once(catalog_db):
    repository = PostgresCatalogRepository(catalog_db)
    transaction_id = uuid4()
    barrier = Barrier(5)

    def reserve(_index: int):
        barrier.wait()
        return repository.reserve(REWARD, transaction_id, 2, None)

    with ThreadPoolExecutor(max_workers=5) as pool:
        results = list(pool.map(reserve, range(5)))

    assert sum(not result.duplicate for result in results) == 1
    assert reward(catalog_db).available_inventory == 8
    assert reservation_count(catalog_db) == 1


def test_a_racing_release_and_fulfilment_cannot_both_apply(catalog_db):
    repository = PostgresCatalogRepository(catalog_db)
    transaction_id = uuid4()
    repository.reserve(REWARD, transaction_id, 3, None)
    barrier = Barrier(2)

    def act(kind: str) -> str:
        barrier.wait()
        try:
            if kind == "release":
                repository.release(REWARD, transaction_id, 3, None)
            else:
                repository.fulfill(REWARD, transaction_id, None)
        except InvalidReservationTransition:
            return "refused"
        return "applied"

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = dict(
            zip(("release", "fulfill"), pool.map(act, ("release", "fulfill")), strict=True)
        )

    assert sorted(outcomes.values()) == ["applied", "refused"]
    with catalog_db.connect() as connection:
        status = connection.scalar(
            select(reservations.c.status).where(reservations.c.transaction_id == transaction_id)
        )
    # Inventory matches whichever one won: released units come back, fulfilled ones don't.
    expected = {"released": 10, "fulfilled": 7}
    assert reward(catalog_db).available_inventory == expected[status]


# --- Inventory cannot become negative --------------------------------------


@pytest.mark.parametrize(("available", "delta"), [(10, -11), (0, -1), (10, 1)])
def test_versioned_update_keeps_inventory_within_bounds(catalog_db, available, delta):
    set_inventory(catalog_db, available)
    repository = PostgresCatalogRepository(catalog_db)
    before = reward(catalog_db)

    with catalog_db.begin() as connection:
        result = repository.compare_and_set_inventory(
            connection, REWARD, expected_version=before.version, delta=delta
        )

    assert result is None
    current = reward(catalog_db)
    assert (current.available_inventory, current.version) == (available, before.version)


# --- Conflict retries reload current state rather than replaying blindly ---


def test_lost_race_reloads_and_reserves_against_the_new_inventory(catalog_db):
    repository = RacingRepository(catalog_db, competing_change(catalog_db, -4), races=1)

    result = repository.reserve(REWARD, uuid4(), 3, None)

    assert repository.reads == 2
    assert result.available_inventory == 10 - 4 - 3
    assert reservation_count(catalog_db) == 1


def test_lost_race_reload_refuses_when_units_are_gone(catalog_db):
    # The competing change takes most of the stock after the first read. A
    # blind replay of the stale decision would oversell; the reload refuses.
    repository = RacingRepository(catalog_db, competing_change(catalog_db, -9), races=1)

    with pytest.raises(InsufficientInventory):
        repository.reserve(REWARD, uuid4(), 3, None)

    assert reward(catalog_db).available_inventory == 1
    assert reservation_count(catalog_db) == 0


def test_lost_race_reload_finds_the_winning_duplicate(catalog_db):
    transaction_id = uuid4()
    plain = PostgresCatalogRepository(catalog_db)

    def same_reservation_wins() -> None:
        plain.reserve(REWARD, transaction_id, 2, None)

    repository = RacingRepository(catalog_db, same_reservation_wins, races=1)

    result = repository.reserve(REWARD, transaction_id, 2, None)

    assert result.duplicate is True
    assert reward(catalog_db).available_inventory == 8
    assert reservation_count(catalog_db) == 1


def test_a_tombstone_landing_mid_reservation_cancels_it(catalog_db):
    # ADR-001 7.1: the release arrives first, between the read and the write.
    transaction_id = uuid4()
    plain = PostgresCatalogRepository(catalog_db)

    def release_first() -> None:
        plain.release(REWARD, transaction_id, 2, None)

    repository = RacingRepository(catalog_db, release_first, races=1)

    with pytest.raises(ReservationConflict, match="cancelled"):
        repository.reserve(REWARD, transaction_id, 2, None)

    assert reward(catalog_db).available_inventory == 10
    assert reservation_count(catalog_db, "tombstone") == 1
    assert reservation_count(catalog_db, "reserved") == 0


def test_release_reloads_after_a_lost_race(catalog_db):
    transaction_id = uuid4()
    PostgresCatalogRepository(catalog_db).reserve(REWARD, transaction_id, 3, None)
    repository = RacingRepository(catalog_db, competing_change(catalog_db, -2), races=1)

    result = repository.release(REWARD, transaction_id, 3, None)

    assert repository.reads == 2
    assert result.state == "released"
    assert result.available_inventory == 10 - 3 - 2 + 3


def test_retries_are_bounded_and_apply_nothing(catalog_db):
    repository = RacingRepository(
        catalog_db, competing_change(catalog_db, -1), races=1_000, max_attempts=3
    )

    with pytest.raises(ConcurrentUpdateConflict):
        repository.reserve(REWARD, uuid4(), 1, None)

    assert repository.reads == 3
    # Only the three competing changes landed; the reservation did not.
    assert reward(catalog_db).available_inventory == 7
    assert reservation_count(catalog_db) == 0
