"""Wallet optimistic concurrency (LM-027).

Each test runs against a new, empty database (see conftest.py).

To make a lost race deterministic, ``RacingRepository`` commits a competing
change on another connection right after the repository has read the wallet
and before it writes, which is exactly the window optimistic concurrency has
to handle.
"""

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from uuid import uuid4

import pytest
from sqlalchemy import Engine, func, select, update

from services.wallet.app.db.schema import wallet_operations, wallets
from services.wallet.app.errors import ConcurrentUpdateConflict, InsufficientPoints
from services.wallet.app.repository import PostgresWalletRepository

from .conftest import upgrade

pytestmark = pytest.mark.database

USER = "user-1"


@pytest.fixture
def wallet_db(database_url: str, engine: Engine) -> Engine:
    upgrade("wallet", database_url)
    with engine.begin() as connection:
        connection.execute(wallets.insert().values(user_id=USER, available_points=100))
    return engine


def wallet(engine: Engine):
    with engine.connect() as connection:
        return connection.execute(select(wallets).where(wallets.c.user_id == USER)).one()


def operation_count(engine: Engine) -> int:
    with engine.connect() as connection:
        return connection.scalar(select(func.count()).select_from(wallet_operations))


def competing_change(engine: Engine, delta: int) -> Callable[[], None]:
    """A change committed by someone else, bumping the version like any update."""

    def change() -> None:
        with engine.begin() as connection:
            connection.execute(
                update(wallets)
                .where(wallets.c.user_id == USER)
                .values(
                    available_points=wallets.c.available_points + delta,
                    version=wallets.c.version + 1,
                )
            )

    return change


class RacingRepository(PostgresWalletRepository):
    """Commits ``change`` after each of the first ``races`` wallet reads."""

    def __init__(self, engine: Engine, change: Callable[[], None], races: int, **kwargs) -> None:
        super().__init__(engine, **kwargs)
        self.change = change
        self.races = races
        self.reads = 0

    def _current_wallet(self, connection, user_id):
        row = super()._current_wallet(connection, user_id)
        self.reads += 1
        if self.reads <= self.races:
            self.change()
        return row


# --- Wallet updates include the expected version ---------------------------


def test_update_applies_only_at_the_expected_version(wallet_db):
    repository = PostgresWalletRepository(wallet_db)
    before = wallet(wallet_db)

    with wallet_db.begin() as connection:
        stale = repository.compare_and_set_balance(
            connection, USER, expected_version=before.version + 1, delta=-10
        )
        applied = repository.compare_and_set_balance(
            connection, USER, expected_version=before.version, delta=-10
        )

    assert stale is None
    assert applied is not None
    assert (applied.available_points, applied.version) == (90, before.version + 1)


# --- Only one racing update succeeds for the same version ------------------


def test_only_one_update_succeeds_for_the_same_version(wallet_db):
    repository = PostgresWalletRepository(wallet_db)
    version = wallet(wallet_db).version
    barrier = Barrier(2)

    def attempt(delta: int):
        with wallet_db.begin() as connection:
            barrier.wait()
            return repository.compare_and_set_balance(
                connection, USER, expected_version=version, delta=delta
            )

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(attempt, (-10, -20)))

    winners = [result for result in results if result is not None]
    assert len(winners) == 1
    current = wallet(wallet_db)
    assert current.version == version + 1
    assert current.available_points == winners[0].available_points


def test_concurrent_deductions_never_overspend(wallet_db):
    repository = PostgresWalletRepository(wallet_db)
    barrier = Barrier(20)

    def deduct(_index: int) -> str:
        barrier.wait()
        try:
            repository.deduct(USER, uuid4(), 15, None)
        except InsufficientPoints:
            return "insufficient"
        return "applied"

    with ThreadPoolExecutor(max_workers=20) as pool:
        outcomes = list(pool.map(deduct, range(20)))

    # 100 points buy exactly six 15-point deductions; every other request is
    # refused against the balance it actually saw, never lost to a conflict.
    assert outcomes.count("applied") == 6
    assert outcomes.count("insufficient") == 14
    current = wallet(wallet_db)
    assert (current.available_points, current.version) == (10, 6)
    assert operation_count(wallet_db) == 6


def test_concurrent_duplicate_deductions_apply_once(wallet_db):
    repository = PostgresWalletRepository(wallet_db)
    transaction_id = uuid4()
    barrier = Barrier(5)

    def deduct(_index: int):
        barrier.wait()
        return repository.deduct(USER, transaction_id, 30, None)

    with ThreadPoolExecutor(max_workers=5) as pool:
        results = list(pool.map(deduct, range(5)))

    assert sum(not result.duplicate for result in results) == 1
    assert wallet(wallet_db).available_points == 70
    assert operation_count(wallet_db) == 1


# --- Balance cannot become negative ----------------------------------------


def test_versioned_update_cannot_make_the_balance_negative(wallet_db):
    repository = PostgresWalletRepository(wallet_db)
    before = wallet(wallet_db)

    with wallet_db.begin() as connection:
        result = repository.compare_and_set_balance(
            connection, USER, expected_version=before.version, delta=-101
        )

    assert result is None
    current = wallet(wallet_db)
    assert (current.available_points, current.version) == (100, before.version)


# --- Conflict retries reload current state rather than replaying blindly ---


def test_lost_race_reloads_and_applies_against_the_new_balance(wallet_db):
    repository = RacingRepository(wallet_db, competing_change(wallet_db, -10), races=1)

    result = repository.deduct(USER, uuid4(), 40, None)

    assert repository.reads == 2
    assert (result.available_points, result.version) == (50, 2)


def test_lost_race_reload_refuses_when_points_are_gone(wallet_db):
    # The competing change spends most of the balance after the first read. A
    # blind replay of the stale decision would overspend; the reload refuses.
    repository = RacingRepository(wallet_db, competing_change(wallet_db, -80), races=1)

    with pytest.raises(InsufficientPoints):
        repository.deduct(USER, uuid4(), 40, None)

    assert wallet(wallet_db).available_points == 20
    assert operation_count(wallet_db) == 0


def test_lost_race_reload_finds_the_winning_duplicate(wallet_db):
    transaction_id = uuid4()
    plain = PostgresWalletRepository(wallet_db)

    def same_deduction_wins() -> None:
        plain.deduct(USER, transaction_id, 30, None)

    repository = RacingRepository(wallet_db, same_deduction_wins, races=1)

    result = repository.deduct(USER, transaction_id, 30, None)

    assert result.duplicate is True
    assert wallet(wallet_db).available_points == 70
    assert operation_count(wallet_db) == 1


def test_refund_reloads_after_a_lost_race(wallet_db):
    transaction_id = uuid4()
    PostgresWalletRepository(wallet_db).deduct(USER, transaction_id, 40, None)
    repository = RacingRepository(wallet_db, competing_change(wallet_db, -5), races=1)

    result = repository.refund(USER, transaction_id, None)

    assert repository.reads == 2
    assert result.available_points == 100 - 40 - 5 + 40


def test_retries_are_bounded_and_apply_nothing(wallet_db):
    repository = RacingRepository(
        wallet_db, competing_change(wallet_db, -1), races=1_000, max_attempts=3
    )

    with pytest.raises(ConcurrentUpdateConflict):
        repository.deduct(USER, uuid4(), 10, None)

    assert repository.reads == 3
    # Only the three competing changes landed; the deduction did not.
    assert wallet(wallet_db).available_points == 97
    assert operation_count(wallet_db) == 0
