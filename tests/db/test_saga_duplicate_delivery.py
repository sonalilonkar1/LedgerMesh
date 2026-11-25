"""LM-029: concurrent duplicate Saga delivery against real PostgreSQL."""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from urllib.request import urlopen
from uuid import uuid4

import pytest
from ledgermesh_messaging import EventEnvelope, MetricsServer, inbox, outbox, process_once
from sqlalchemy import Engine, func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

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
from tests.db.conftest import upgrade

pytestmark = pytest.mark.database


@pytest.fixture
def wallet_db(database_url: str, engine: Engine) -> Engine:
    upgrade("wallet", database_url)
    with engine.begin() as connection:
        connection.execute(wallets.insert().values(user_id="lm029-user", available_points=100))
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
            connection.scalar(select(func.count()).select_from(table).where(*conditions)) or 0
        )


@pytest.fixture
def metrics_endpoint():
    server = MetricsServer("127.0.0.1", 0).start()
    try:
        yield f"http://{server.address[0]}:{server.address[1]}/metrics"
    finally:
        server.close()


def _scrape(endpoint: str) -> str:
    with urlopen(endpoint, timeout=3) as response:
        assert response.status == 200
        return response.read().decode()


def _counter(metrics: str, name: str, **labels: str) -> int:
    wanted = ",".join(f'{key}="{value}"' for key, value in labels.items())
    prefix = f"{name}{{{wanted}}} "
    for line in metrics.splitlines():
        if line.startswith(prefix):
            return int(float(line.removeprefix(prefix)))
    return 0


def test_concurrent_duplicate_deductions_apply_once_and_persist_rejections(
    database_url, wallet_db, metrics_endpoint
):
    before = _scrape(metrics_endpoint)
    event = _event("wallet.deduct_points", uuid4(), points=25)
    results = _race(database_url, wallet_db, event)

    assert sum(not result.duplicate for result in results) == 1
    assert sum(result.duplicate for result in results) == 9
    with wallet_db.connect() as connection:
        assert connection.scalar(select(wallets.c.available_points)) == 75
    assert _count(wallet_db, wallet_operations) == 1
    assert _count(wallet_db, deduction_rejections) == 0
    assert _count(wallet_db, inbox) == 1
    assert _count(wallet_db, outbox) == 1
    after_duplicates = _scrape(metrics_endpoint)
    assert (
        _counter(
            after_duplicates, "ledgermesh_inbox_duplicate_deliveries_total", consumer="wallet-saga"
        )
        - _counter(before, "ledgermesh_inbox_duplicate_deliveries_total", consumer="wallet-saga")
        == 9
    )

    rejected = _event("wallet.deduct_points", uuid4(), points=101)
    rejection_results = _race(database_url, wallet_db, rejected)
    assert sum(not result.duplicate for result in rejection_results) == 1
    assert sum(result.duplicate for result in rejection_results) == 9
    assert _count(wallet_db, deduction_rejections) == 1
    assert _count(wallet_db, inbox) == 2
    assert _count(wallet_db, outbox) == 2
    after_rejection = _scrape(metrics_endpoint)
    assert (
        _counter(
            after_rejection,
            "ledgermesh_inbox_duplicate_deliveries_total",
            consumer="wallet-saga",
        )
        - _counter(
            after_duplicates,
            "ledgermesh_inbox_duplicate_deliveries_total",
            consumer="wallet-saga",
        )
        == 9
    )
    assert (
        _counter(
            after_rejection,
            "ledgermesh_saga_rejections_total",
            consumer="wallet-saga",
            service="wallet",
            reason="insufficient_points",
        )
        - _counter(
            after_duplicates,
            "ledgermesh_saga_rejections_total",
            consumer="wallet-saga",
            service="wallet",
            reason="insufficient_points",
        )
        == 1
    )


def test_concurrent_duplicate_reservations_apply_once(database_url, catalog_db, metrics_endpoint):
    before = _scrape(metrics_endpoint)
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
    after_duplicates = _scrape(metrics_endpoint)
    assert (
        _counter(
            after_duplicates, "ledgermesh_inbox_duplicate_deliveries_total", consumer="catalog-saga"
        )
        - _counter(before, "ledgermesh_inbox_duplicate_deliveries_total", consumer="catalog-saga")
        == 9
    )

    rejected = _event("catalog.reserve_inventory", uuid4(), quantity=20)
    result = _deliver(database_url, catalog_db, rejected)
    assert not result.duplicate
    after_rejection = _scrape(metrics_endpoint)
    assert (
        _counter(
            after_rejection,
            "ledgermesh_saga_rejections_total",
            consumer="catalog-saga",
            service="catalog",
            reason="insufficient_inventory",
        )
        - _counter(
            after_duplicates,
            "ledgermesh_saga_rejections_total",
            consumer="catalog-saga",
            service="catalog",
            reason="insufficient_inventory",
        )
        == 1
    )
