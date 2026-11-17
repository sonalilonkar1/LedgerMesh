"""PostgreSQL integration tests for Wallet core operations (LM-007)."""

from uuid import uuid4

import pytest
from sqlalchemy import Engine, select

from services.wallet.app.db.schema import wallet_operations, wallets
from services.wallet.app.errors import DeductionNotFound, InsufficientPoints, OperationConflict
from services.wallet.app.repository import PostgresWalletRepository

from .conftest import upgrade

pytestmark = pytest.mark.database


@pytest.fixture
def repository(database_url: str, engine: Engine) -> PostgresWalletRepository:
    upgrade("wallet", database_url)
    with engine.begin() as connection:
        connection.execute(
            wallets.insert().values(user_id="user-1", available_points=100, version=0)
        )
    return PostgresWalletRepository(engine)


def operation_count(engine: Engine) -> int:
    with engine.connect() as connection:
        return len(connection.execute(select(wallet_operations)).all())


def test_deduct_and_refund_update_balance_version_and_operation_log(repository):
    transaction_id = uuid4()

    deduction = repository.deduct("user-1", transaction_id, 40, None)
    refund = repository.refund("user-1", transaction_id, None)

    assert (deduction.available_points, deduction.version) == (60, 1)
    assert (refund.available_points, refund.version) == (100, 2)
    assert refund.points == deduction.points == 40
    assert operation_count(repository.engine) == 2


def test_insufficient_deduction_rolls_back_without_an_operation(repository):
    transaction_id = uuid4()

    with pytest.raises(InsufficientPoints):
        repository.deduct("user-1", transaction_id, 101, None)

    balance = repository.get_balance("user-1")
    assert (balance.available_points, balance.version) == (100, 0)
    assert operation_count(repository.engine) == 0


def test_identical_deduction_and_refund_retries_are_idempotent(repository):
    transaction_id = uuid4()

    repository.deduct("user-1", transaction_id, 25, None)
    duplicate_deduction = repository.deduct("user-1", transaction_id, 25, None)
    repository.refund("user-1", transaction_id, None)
    duplicate_refund = repository.refund("user-1", transaction_id, None)

    assert duplicate_deduction.duplicate is True
    assert duplicate_refund.duplicate is True
    assert repository.get_balance("user-1").available_points == 100
    assert repository.get_balance("user-1").version == 2
    assert operation_count(repository.engine) == 2


def test_transaction_id_cannot_be_reused_for_a_different_deduction(repository):
    transaction_id = uuid4()
    repository.deduct("user-1", transaction_id, 10, None)

    with pytest.raises(OperationConflict):
        repository.deduct("user-1", transaction_id, 20, None)

    assert repository.get_balance("user-1").available_points == 90
    assert operation_count(repository.engine) == 1


def test_refund_without_original_deduction_is_rejected(repository):
    with pytest.raises(DeductionNotFound):
        repository.refund("user-1", uuid4(), None)

    assert repository.get_balance("user-1").available_points == 100
    assert operation_count(repository.engine) == 0
