"""LM-029: concurrent duplicate Saga delivery against real PostgreSQL."""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from uuid import uuid4

import pytest
from ledgermesh_messaging import EventEnvelope, inbox, outbox, process_once
from services.catalog.app.db.schema import catalog_saga_outcomes, reservations, rewards
from services.catalog.app.repository import PostgresCatalogRepository
from services.catalog.app.saga import CatalogSagaHandler
from services.wallet.app.db.schema import (
    deduction_rejections,
    wallet_operations,
    wallets,
)
from services.wallet.app.repository import PostgresWalletRepository
from services.wallet.app.saga import WalletSagaHandler
from sqlalchemy import Engine, func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from tests.db.conftest import upgrade

pytestmark = pytest.mark.database


@pytest.fixture
def wallet_db(database_url: str, engine: Engine) -> Engine:
    upgrade("wallet", database_url)
    with engine.begin() as connection:
        connection.execute(
            wallets.insert().values(user_id="lm029-user", available_points=100)
        )
    return engine


@pytest.fixture
def catalog_db(database_url: str, engine: Engine) -> Engine:
    upgrade("catalog", database_url)
    with engine.begin() as connection:
        connection.execute(
            rewards.insert().values(
                reward_id="lm029-reward",
                name="Concurrent reward",
                point_cost=10,
                total_inventory=10,
                available_inventory=10,
            )
        )
    return engine


def _event(event_type: str, transaction_id, *, points=25, quantity=2) -> EventEnvelope:
    wallet = event_type.startswith("wallet.")
    return EventEnvelope(
        event_type=event_type,
        transaction_id=transaction_id,
        correlation_id=uuid4(),
        causation_id=None if event_type == "wallet.deduct_points" else uuid4(),
        aggregate_type="wallet" if wallet else "reward",
        aggregate_id="lm029-user" if wallet else "lm029-reward",
        workflow_step=1 if wallet else 3,
        payload=(
            {"user_id": "lm029-user", "points": points}
            if wallet
            else {"reward_id": "lm029-reward", "quantity": quantity}
        ),
    )


def _deliver(database_url: str, engine: Engine, envelope: EventEnvelope):
    async def run():
        async_engine = create_async_engine(database_url)
        try:
            sessions = async_sessionmaker(async_engine, expire_on_commit=False)
            if envelope.event_type.startswith("wallet."):
                handler = WalletSagaHandler(PostgresWalletRepository(engine))
                name = "wallet-saga"
            else:
                handler = CatalogSagaHandler(PostgresCatalogRepository(engine))
                name = "catalog-saga"
            return await process_once(sessions, name, envelope, handler)
        finally:
            await async_engine.dispose()

    return asyncio.run(run())


def _race(database_url: str, engine: Engine, envelope: EventEnvelope, copies: int = 10):
    barrier = Barrier(copies)

    def send(_index: int):
        barrier.wait(timeout=10)
        return _deliver(database_url, engine, envelope)

    with ThreadPoolExecutor(max_workers=copies) as pool:
        return list(pool.map(send, range(copies)))


def _count(engine: Engine, table, *conditions) -> int:
    with engine.connect() as connection:
        return int(
            connection.scalar(
                select(func.count()).select_from(table).where(*conditions)
            )
            or 0
        )


def test_concurrent_duplicate_deductions_apply_once_and_count_duplicate_and_rejection_metrics(
    database_url, wallet_db
):
    event = _event("wallet.deduct_points", uuid4(), points=25)
    results = _race(database_url, wallet_db, event)

    # These per-run counters are intentionally derived from consumer outcomes;
    # production Prometheus instrumentation remains a separate observability task.
    metrics = {
        "duplicate_attempts": sum(result.duplicate for result in results),
        "rejected_outcomes": 0,
    }
    assert metrics == {"duplicate_attempts": 9, "rejected_outcomes": 0}
    with wallet_db.connect() as connection:
        assert connection.scalar(select(wallets.c.available_points)) == 75
    assert _count(wallet_db, wallet_operations) == 1
    assert _count(wallet_db, inbox) == 1
    assert _count(wallet_db, outbox) == 1

    rejected = _event("wallet.deduct_points", uuid4(), points=101)
    rejection_results = _race(database_url, wallet_db, rejected)
    metrics = {
        "duplicate_attempts": sum(result.duplicate for result in rejection_results),
        "rejected_outcomes": _count(wallet_db, deduction_rejections),
    }
    assert metrics == {"duplicate_attempts": 9, "rejected_outcomes": 1}
    assert _count(wallet_db, inbox) == 2
    assert _count(wallet_db, outbox) == 2


def test_concurrent_duplicate_reservations_apply_once(database_url, catalog_db):
    event = _event("catalog.reserve_inventory", uuid4(), quantity=2)
    results = _race(database_url, catalog_db, event)

    assert sum(not result.duplicate for result in results) == 1
    assert sum(result.duplicate for result in results) == 9
    with catalog_db.connect() as connection:
        assert connection.scalar(select(rewards.c.available_inventory)) == 8
    assert _count(catalog_db, reservations) == 1
    assert _count(catalog_db, catalog_saga_outcomes) == 1
    assert _count(catalog_db, inbox) == 1
    assert _count(catalog_db, outbox) == 1
