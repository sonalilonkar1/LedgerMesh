"""PostgreSQL integration tests for Wallet as a 2PC participant (LM-013)."""

from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import pytest
from sqlalchemy import Engine, func, select

from infra.scripts.prepared_transactions import list_prepared
from services.wallet.app.db.schema import participant_transactions, wallet_operations, wallets
from services.wallet.app.errors import ParticipantConflict, ParticipantTransactionNotFound
from services.wallet.app.participant import PostgresWalletParticipantRepository

from .conftest import upgrade

pytestmark = pytest.mark.database


@pytest.fixture
def repository(database_url: str, engine: Engine) -> PostgresWalletParticipantRepository:
    upgrade("wallet", database_url)
    with engine.begin() as connection:
        connection.execute(
            wallets.insert().values(user_id="user-1", available_points=100, version=0)
        )
    return PostgresWalletParticipantRepository(engine)


def balance(repository: PostgresWalletParticipantRepository) -> tuple[int, int]:
    with repository.engine.connect() as connection:
        row = connection.execute(
            select(wallets.c.available_points, wallets.c.version).where(
                wallets.c.user_id == "user-1"
            )
        ).one()
    return row.available_points, row.version


def operation_count(repository: PostgresWalletParticipantRepository) -> int:
    with repository.engine.connect() as connection:
        return connection.scalar(select(func.count()).select_from(wallet_operations))


def test_prepare_is_durable_but_deduction_is_invisible_until_commit(repository) -> None:
    transaction_id = uuid4()

    prepared = repository.prepare(transaction_id, uuid4(), "user-1", 40)

    assert prepared.state == "PREPARED"
    assert balance(repository) == (100, 0)
    assert operation_count(repository) == 0
    assert len(list_prepared(repository.engine)) == 1

    committed = repository.commit(transaction_id)

    assert committed.state == "COMMITTED"
    assert balance(repository) == (60, 1)
    assert operation_count(repository) == 1
    assert list_prepared(repository.engine) == []


def test_commit_and_prepare_retries_return_same_final_result(repository) -> None:
    transaction_id, correlation_id = uuid4(), uuid4()
    first = repository.prepare(transaction_id, correlation_id, "user-1", 25)
    second = repository.prepare(transaction_id, correlation_id, "user-1", 25)
    committed = repository.commit(transaction_id)
    repeated = repository.commit(transaction_id)
    prepare_after_commit = repository.prepare(transaction_id, correlation_id, "user-1", 25)

    assert first.state == second.state == "PREPARED"
    assert committed.state == repeated.state == prepare_after_commit.state == "COMMITTED"
    assert balance(repository) == (75, 1)
    assert operation_count(repository) == 1


def test_abort_rolls_back_prepared_work_and_is_idempotent(repository) -> None:
    transaction_id = uuid4()
    repository.prepare(transaction_id, uuid4(), "user-1", 40)

    aborted = repository.abort(transaction_id)
    repeated = repository.abort(transaction_id)

    assert aborted.state == repeated.state == "ABORTED"
    assert balance(repository) == (100, 0)
    assert operation_count(repository) == 0
    assert list_prepared(repository.engine) == []


def test_insufficient_balance_is_a_durable_no_vote(repository) -> None:
    transaction_id, correlation_id = uuid4(), uuid4()

    first = repository.prepare(transaction_id, correlation_id, "user-1", 101)
    repeated = repository.prepare(transaction_id, correlation_id, "user-1", 101)

    assert first.state == repeated.state == "ABORTED"
    assert balance(repository) == (100, 0)
    assert list_prepared(repository.engine) == []


def test_unknown_commit_is_rejected_but_unknown_abort_creates_tombstone(repository) -> None:
    unknown_commit, unknown_abort = uuid4(), uuid4()

    with pytest.raises(ParticipantTransactionNotFound):
        repository.commit(unknown_commit)

    aborted = repository.abort(unknown_abort)
    late_prepare = repository.prepare(unknown_abort, uuid4(), "user-1", 20)

    assert aborted.state == late_prepare.state == "ABORTED"
    assert balance(repository) == (100, 0)
    assert list_prepared(repository.engine) == []


def test_conflicting_prepare_and_opposite_final_decisions_are_rejected(repository) -> None:
    prepared_id, aborted_id = uuid4(), uuid4()
    correlation_id = uuid4()
    repository.prepare(prepared_id, correlation_id, "user-1", 20)

    with pytest.raises(ParticipantConflict):
        repository.prepare(prepared_id, correlation_id, "user-1", 21)

    repository.commit(prepared_id)
    with pytest.raises(ParticipantConflict):
        repository.abort(prepared_id)

    repository.abort(aborted_id)
    with pytest.raises(ParticipantConflict):
        repository.commit(aborted_id)


def test_prepare_rejects_transaction_id_already_used_by_core_deduction(repository) -> None:
    transaction_id = uuid4()
    with repository.engine.begin() as connection:
        connection.execute(
            wallet_operations.insert().values(
                transaction_id=transaction_id,
                user_id="user-1",
                operation="deduct",
                points=10,
                status="applied",
            )
        )

    with pytest.raises(ParticipantConflict):
        repository.prepare(transaction_id, uuid4(), "user-1", 10)

    assert repository.get(transaction_id).state == "ABORTED"


def test_concurrent_identical_prepare_calls_create_one_prepared_transaction(repository) -> None:
    transaction_id, correlation_id = uuid4(), uuid4()

    def prepare_once(_index: int):
        return repository.prepare(transaction_id, correlation_id, "user-1", 30).state

    with ThreadPoolExecutor(max_workers=10) as pool:
        states = list(pool.map(prepare_once, range(10)))

    assert states == ["PREPARED"] * 10
    assert len(list_prepared(repository.engine)) == 1
    with repository.engine.connect() as connection:
        rows = connection.scalar(select(func.count()).select_from(participant_transactions))
    assert rows == 1

    repository.abort(transaction_id)
