"""LM-025: cross-service Saga completion, compensation and restart recovery."""

from __future__ import annotations

import asyncio
from uuid import UUID, uuid4

import pytest
from ledgermesh_messaging import EventEnvelope, outbox, process_once
from sqlalchemy import Engine, func, insert, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from services.audit_ledger.app.repository import PostgresAuditRepository
from services.catalog.app.db.schema import reservations, rewards
from services.catalog.app.repository import PostgresCatalogRepository
from services.catalog.app.saga import CatalogSagaHandler
from services.coordinator.app.db.schema import redemption_transitions, redemptions
from services.coordinator.app.models import CreateRedemptionRequest
from services.coordinator.app.repository import PostgresCoordinatorRepository
from services.coordinator.app.saga import (
    CoordinatorSagaMachine,
    CoordinatorSagaResultHandler,
    RewardQuote,
    SagaStarter,
)
from services.wallet.app.db.schema import wallet_operations, wallets
from services.wallet.app.repository import PostgresWalletRepository
from services.wallet.app.saga import WalletSagaHandler
from tests.integration.conftest import ServiceDatabases

pytestmark = [pytest.mark.database, pytest.mark.integration]


class FakeQuote:
    async def get_quote(self, reward_id: str) -> RewardQuote:
        return RewardQuote(reward_id, point_cost=100, available_inventory=10, version=0)


def _event(engine: Engine, topic: str, transaction_id: UUID) -> EventEnvelope:
    with engine.connect() as connection:
        rows = connection.execute(
            select(outbox.c.envelope).where(outbox.c.topic == topic)
        ).scalars()
        for document in reversed(list(rows)):
            envelope = EventEnvelope.model_validate(document, context={"wire": True})
            if envelope.transaction_id == transaction_id:
                return envelope
    raise AssertionError(f"no {topic} event found for {transaction_id}")


def _deliver(database_url: str, engine: Engine, name: str, handler, envelope: EventEnvelope):
    async def run():
        async_engine = create_async_engine(database_url)
        try:
            sessions = async_sessionmaker(async_engine, expire_on_commit=False)
            return await process_once(sessions, name, envelope, handler)
        finally:
            await async_engine.dispose()

    return asyncio.run(run())


def _start(database_url: str, transaction_id: UUID) -> int:
    async def run():
        async_engine = create_async_engine(database_url)
        try:
            sessions = async_sessionmaker(async_engine, expire_on_commit=False)
            return await SagaStarter(sessions, FakeQuote(), CoordinatorSagaMachine()).run_once()
        finally:
            await async_engine.dispose()

    return asyncio.run(run())


def _coordinator_result(databases: ServiceDatabases, result: EventEnvelope):
    engine = databases.engines["coordinator"]
    return _deliver(
        databases.urls["coordinator"],
        engine,
        "coordinator-saga",
        CoordinatorSagaResultHandler(CoordinatorSagaMachine()),
        result,
    )


def _deliver_audit(databases: ServiceDatabases, transaction_id: UUID) -> None:
    coordinator = databases.engines["coordinator"]
    audit = PostgresAuditRepository(databases.engines["audit_ledger"])
    with coordinator.connect() as connection:
        documents = (
            connection.execute(
                select(outbox.c.envelope)
                .where(outbox.c.topic == "redemption.events")
                .order_by(outbox.c.id)
            )
            .scalars()
            .all()
        )
    for document in documents:
        event = EventEnvelope.model_validate(document, context={"wire": True})
        if event.transaction_id == transaction_id:
            audit.append(event)


def _assert_saga_invariants(databases: ServiceDatabases, transaction_id: UUID) -> None:
    wallet = databases.engines["wallet"]
    catalog = databases.engines["catalog"]
    coordinator = databases.engines["coordinator"]
    with wallet.connect() as connection:
        assert (
            connection.scalar(
                select(func.count()).select_from(wallets).where(wallets.c.available_points < 0)
            )
            == 0
        )
        operations = connection.execute(
            select(wallet_operations.c.operation, wallet_operations.c.status).where(
                wallet_operations.c.transaction_id == transaction_id
            )
        ).all()
    with catalog.connect() as connection:
        assert (
            connection.scalar(
                select(func.count())
                .select_from(rewards)
                .where(
                    (rewards.c.available_inventory < 0)
                    | (rewards.c.available_inventory > rewards.c.total_inventory)
                )
            )
            == 0
        )
        reservation = connection.execute(
            select(reservations.c.status).where(reservations.c.transaction_id == transaction_id)
        ).scalar_one_or_none()
    with coordinator.connect() as connection:
        status = connection.execute(
            select(redemptions.c.status).where(redemptions.c.transaction_id == transaction_id)
        ).scalar_one()
        transitions = connection.scalar(
            select(func.count())
            .select_from(redemption_transitions)
            .where(redemption_transitions.c.transaction_id == transaction_id)
        )
    applied = {(operation, state) for operation, state in operations}
    if status == "COMPLETED":
        assert ("deduct", "applied") in applied
        assert ("refund", "applied") not in applied
        assert reservation in {"reserved", "fulfilled"}
    elif status == "COMPENSATED":
        assert ("deduct", "applied") in applied
        assert ("refund", "applied") in applied
        assert reservation in {None, "released", "tombstone"}
    else:
        pytest.fail(f"expected a terminal Saga outcome, got {status}")
    assert transitions >= 4


@pytest.fixture
def saga_databases(service_databases: ServiceDatabases) -> ServiceDatabases:
    with service_databases.engines["wallet"].begin() as connection:
        connection.execute(insert(wallets).values(user_id="saga-user", available_points=1_000))
    with service_databases.engines["catalog"].begin() as connection:
        connection.execute(
            insert(rewards).values(
                reward_id="saga-reward",
                name="Saga reward",
                point_cost=100,
                total_inventory=10,
                available_inventory=10,
            )
        )
    return service_databases


def _create(databases: ServiceDatabases, *, sold_out: bool = False):
    if sold_out:
        with databases.engines["catalog"].begin() as connection:
            connection.execute(
                rewards.update()
                .where(rewards.c.reward_id == "saga-reward")
                .values(total_inventory=0, available_inventory=0)
            )
    return PostgresCoordinatorRepository(databases.engines["coordinator"]).create(
        f"lm025-{uuid4()}",
        CreateRedemptionRequest(
            user_id="saga-user",
            reward_id="saga-reward",
            quantity=1,
            coordination_mode="saga",
        ),
    )


def _run_wallet(databases: ServiceDatabases, command: EventEnvelope):
    return _deliver(
        databases.urls["wallet"],
        databases.engines["wallet"],
        "wallet-saga",
        WalletSagaHandler(PostgresWalletRepository(databases.engines["wallet"])),
        command,
    )


def _run_catalog(databases: ServiceDatabases, command: EventEnvelope):
    return _deliver(
        databases.urls["catalog"],
        databases.engines["catalog"],
        "catalog-saga",
        CatalogSagaHandler(PostgresCatalogRepository(databases.engines["catalog"])),
        command,
    )


def test_saga_happy_path_completes_and_audits_all_transitions(
    saga_databases,
):
    databases = saga_databases
    created = _create(databases)
    assert _start(databases.urls["coordinator"], created.transaction_id) == 1

    _run_wallet(
        databases,
        _event(databases.engines["coordinator"], "wallet.commands", created.transaction_id),
    )
    _coordinator_result(
        databases,
        _event(databases.engines["wallet"], "wallet.results", created.transaction_id),
    )
    _run_catalog(
        databases,
        _event(databases.engines["coordinator"], "catalog.commands", created.transaction_id),
    )
    _coordinator_result(
        databases,
        _event(databases.engines["catalog"], "catalog.results", created.transaction_id),
    )
    _deliver_audit(databases, created.transaction_id)

    with databases.engines["coordinator"].connect() as connection:
        assert (
            connection.scalar(
                select(redemptions.c.status).where(
                    redemptions.c.transaction_id == created.transaction_id
                )
            )
            == "COMPLETED"
        )
    with databases.engines["wallet"].connect() as connection:
        assert (
            connection.scalar(
                select(wallets.c.available_points).where(wallets.c.user_id == "saga-user")
            )
            == 900
        )
    with databases.engines["catalog"].connect() as connection:
        assert (
            connection.scalar(
                select(rewards.c.available_inventory).where(rewards.c.reward_id == "saga-reward")
            )
            == 9
        )
    audit = PostgresAuditRepository(databases.engines["audit_ledger"])
    assert any(
        item.envelope["payload"].get("to_status") == "COMPLETED"
        for item in audit.list_by_transaction(created.transaction_id)
    )
    _assert_saga_invariants(databases, created.transaction_id)


def test_catalog_rejection_refunds_once_and_restart_resumes_to_compensated(
    saga_databases,
):
    databases = saga_databases
    created = _create(databases, sold_out=True)
    assert _start(databases.urls["coordinator"], created.transaction_id) == 1

    # Process the first step, then model a Coordinator process restart by using
    # fresh async engines and fresh handler/machine instances for each delivery.
    _run_wallet(
        databases,
        _event(databases.engines["coordinator"], "wallet.commands", created.transaction_id),
    )
    _coordinator_result(
        databases,
        _event(databases.engines["wallet"], "wallet.results", created.transaction_id),
    )
    _run_catalog(
        databases,
        _event(databases.engines["coordinator"], "catalog.commands", created.transaction_id),
    )
    _coordinator_result(
        databases,
        _event(databases.engines["catalog"], "catalog.results", created.transaction_id),
    )
    _run_wallet(
        databases,
        _event(databases.engines["coordinator"], "wallet.commands", created.transaction_id),
    )
    _coordinator_result(
        databases,
        _event(databases.engines["wallet"], "wallet.results", created.transaction_id),
    )
    _deliver_audit(databases, created.transaction_id)

    with databases.engines["coordinator"].connect() as connection:
        assert (
            connection.scalar(
                select(redemptions.c.status).where(
                    redemptions.c.transaction_id == created.transaction_id
                )
            )
            == "COMPENSATED"
        )
    with databases.engines["wallet"].connect() as connection:
        assert (
            connection.scalar(
                select(wallets.c.available_points).where(wallets.c.user_id == "saga-user")
            )
            == 1_000
        )
        assert (
            connection.scalar(
                select(func.count())
                .select_from(wallet_operations)
                .where(
                    wallet_operations.c.transaction_id == created.transaction_id,
                    wallet_operations.c.operation == "refund",
                )
            )
            == 1
        )
    audit = PostgresAuditRepository(databases.engines["audit_ledger"])
    assert any(
        item.envelope["payload"].get("to_status") == "COMPENSATED"
        for item in audit.list_by_transaction(created.transaction_id)
    )
    _assert_saga_invariants(databases, created.transaction_id)
