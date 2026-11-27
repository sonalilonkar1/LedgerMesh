# ruff: noqa: F811  (the `harness` fixture is imported, then used as a test argument)
"""LM-045: different Saga redemptions racing for the same balance or the last unit.

The Saga normally runs across three worker processes joined by Kafka. These tests
keep the real handlers and databases but replace Kafka with a small in-process
pump: it reads every service's outbox, hands each message to the right handler
and carries the replies back to the Coordinator. All messages of one round are
delivered at the same moment, so the Wallet and Catalog really do see racing
commands. The delivery order is shuffled from a fixed seed so a failing seed can
be replayed.
"""

from __future__ import annotations

import asyncio
import random
import threading
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import pytest
from ledgermesh_messaging import (
    CATALOG_COMMANDS,
    CATALOG_RESULTS,
    WALLET_COMMANDS,
    WALLET_RESULTS,
    EventEnvelope,
    outbox,
    process_once,
)
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from services.catalog.app.db.schema import rewards
from services.catalog.app.repository import PostgresCatalogRepository
from services.catalog.app.saga import CatalogSagaHandler
from services.coordinator.app.db.schema import redemptions
from services.coordinator.app.models import CreateRedemptionRequest
from services.coordinator.app.saga import (
    CoordinatorSagaMachine,
    CoordinatorSagaResultHandler,
    RewardQuote,
    SagaStarter,
)
from services.wallet.app.repository import PostgresWalletRepository
from services.wallet.app.saga import WalletSagaHandler
from tests.integration.test_2pc_atomicity import (  # noqa: F401  (harness is a fixture)
    Harness,
    assert_zero_violations,
    harness,
)
from tests.integration.test_2pc_competing import (
    POINT_COST,
    SEEDS,
    balance,
    inventory,
    set_resources,
)

pytestmark = [pytest.mark.database, pytest.mark.integration]

TERMINAL = {"COMPLETED", "FAILED", "COMPENSATED"}
MAX_ROUNDS = 30


class CatalogQuotes:
    """Quote a reward straight from the Catalog database, as the real client does over HTTP."""

    def __init__(self, harness: Harness) -> None:
        self.engine = harness.engines["catalog"]

    async def get_quote(self, reward_id: str) -> RewardQuote:
        with self.engine.connect() as connection:
            row = connection.execute(
                select(
                    rewards.c.point_cost, rewards.c.available_inventory, rewards.c.version
                ).where(rewards.c.reward_id == reward_id)
            ).one()
        return RewardQuote(reward_id, row.point_cost, row.available_inventory, row.version)


class SagaPump:
    """Move messages between the four service databases without Kafka."""

    # topic -> (database that receives it, consumer name, handler factory)
    def __init__(self, harness: Harness, urls: dict[str, str], seed: int) -> None:
        self.harness = harness
        self.urls = urls
        self.random = random.Random(seed)
        self.routes = {
            WALLET_COMMANDS.name: ("wallet", "wallet-saga"),
            CATALOG_COMMANDS.name: ("catalog", "catalog-saga"),
            WALLET_RESULTS.name: ("coordinator", "coordinator-saga"),
            CATALOG_RESULTS.name: ("coordinator", "coordinator-saga"),
        }

    def handler(self, service: str):
        engines = self.harness.engines
        if service == "wallet":
            return WalletSagaHandler(PostgresWalletRepository(engines["wallet"]))
        if service == "catalog":
            return CatalogSagaHandler(PostgresCatalogRepository(engines["catalog"]))
        return CoordinatorSagaResultHandler(CoordinatorSagaMachine())

    def start_sagas(self) -> None:
        async def go() -> None:
            engine = create_async_engine(self.urls["coordinator"])
            try:
                sessions = async_sessionmaker(engine, expire_on_commit=False)
                starter = SagaStarter(
                    sessions, CatalogQuotes(self.harness), CoordinatorSagaMachine()
                )
                await starter.run_once()
            finally:
                await engine.dispose()

        asyncio.run(go())

    def take_pending(self) -> list[tuple[str, str, EventEnvelope]]:
        """Mark every unpublished outbox message as sent and return those with a route."""
        pending = []
        for service in ("coordinator", "wallet", "catalog"):
            engine = self.harness.engines[service]
            with engine.begin() as connection:
                rows = connection.execute(
                    update(outbox)
                    .where(outbox.c.published_at.is_(None))
                    .values(published_at=func.now())
                    .returning(outbox.c.topic, outbox.c.envelope)
                ).all()
            for topic, document in rows:
                route = self.routes.get(topic)
                if route:  # audit events have no consumer in this test
                    envelope = EventEnvelope.model_validate(document, context={"wire": True})
                    pending.append((*route, envelope))
        return pending

    def deliver(self, service: str, consumer: str, envelope: EventEnvelope) -> None:
        async def go() -> None:
            engine = create_async_engine(self.urls[service])
            try:
                sessions = async_sessionmaker(engine, expire_on_commit=False)
                await process_once(sessions, consumer, envelope, self.handler(service))
            finally:
                await engine.dispose()

        asyncio.run(go())

    def run(self) -> None:
        self.start_sagas()
        for _ in range(MAX_ROUNDS):
            batch = self.take_pending()
            if not batch:
                return
            self.random.shuffle(batch)
            start = threading.Barrier(len(batch))

            def send(item, start=start) -> None:
                start.wait()
                self.deliver(*item)

            with ThreadPoolExecutor(max_workers=len(batch)) as pool:
                list(pool.map(send, batch))
        raise AssertionError(f"the Saga did not settle within {MAX_ROUNDS} rounds")


def create_sagas(harness: Harness, count: int) -> list:
    return [
        harness.coordinator.create(
            f"saga-race-{uuid4()}",
            CreateRedemptionRequest(
                user_id="user-funded",
                reward_id="reward-stocked",
                quantity=1,
                coordination_mode="saga",
            ),
        ).transaction_id
        for _ in range(count)
    ]


def final_statuses(harness: Harness) -> Counter:
    with harness.engines["coordinator"].connect() as connection:
        return Counter(
            connection.scalars(
                select(redemptions.c.status).where(redemptions.c.coordination_mode == "saga")
            ).all()
        )


def race(harness: Harness, urls: dict[str, str], redemptions_count: int, seed: int) -> Counter:
    create_sagas(harness, redemptions_count)
    SagaPump(harness, urls, seed).run()
    return final_statuses(harness)


def assert_saga_safety(harness: Harness, outcomes: Counter, *, points: int, stock: int) -> None:
    """Every Saga ended, and the money and stock add up to the winners alone."""
    assert set(outcomes) <= TERMINAL, f"a Saga did not reach a final state: {outcomes}"
    winners = outcomes["COMPLETED"]
    assert winners * POINT_COST <= points, f"overspent: {outcomes}"
    assert winners <= stock, f"oversold: {outcomes}"
    assert balance(harness) == points - winners * POINT_COST, f"losers left a charge: {outcomes}"
    assert inventory(harness) == stock - winners, f"losers left a reservation: {outcomes}"


@pytest.mark.parametrize("seed", SEEDS)
def test_saga_same_user_race_succeeds_exactly_as_often_as_the_balance_allows(
    harness: Harness, service_databases, lm017_report, seed: int
):
    affordable = 2
    points, stock = affordable * POINT_COST, 10
    set_resources(harness, points=points, inventory=stock)

    outcomes = race(harness, service_databases.urls, 8, seed)

    assert outcomes["COMPLETED"] == affordable, outcomes
    assert_saga_safety(harness, outcomes, points=points, stock=stock)
    assert_zero_violations(harness, lm017_report, f"saga same-user race seed {seed}")


@pytest.mark.parametrize("seed", SEEDS)
def test_saga_last_unit_race_has_exactly_one_winner(
    harness: Harness, service_databases, lm017_report, seed: int
):
    points, stock = 1_000, 1
    set_resources(harness, points=points, inventory=stock)

    outcomes = race(harness, service_databases.urls, 8, seed)

    assert outcomes["COMPLETED"] == 1, outcomes
    assert_saga_safety(harness, outcomes, points=points, stock=stock)
    assert_zero_violations(harness, lm017_report, f"saga last-unit race seed {seed}")
