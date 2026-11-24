"""Catalog Saga consumer integration tests (LM-021)."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from types import SimpleNamespace
from uuid import uuid4

import pytest
from ledgermesh_messaging import EventEnvelope, InboxConsumer, inbox, outbox, process_once
from sqlalchemy import Engine, func, select, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from services.catalog.app.db.schema import (
    catalog_saga_outcomes,
    reservations,
    rewards,
)
from services.catalog.app.repository import PostgresCatalogRepository
from services.catalog.app.saga import (
    INVENTORY_RELEASED,
    INVENTORY_RESERVED,
    RELEASE_INVENTORY,
    RESERVATION_REJECTED,
    RESERVE_INVENTORY,
    CatalogSagaHandler,
    InvalidCommand,
)

from .conftest import upgrade

pytestmark = pytest.mark.database
CONSUMER = "catalog-saga"


@pytest.fixture
def catalog_db(database_url: str, engine: Engine) -> Engine:
    upgrade("catalog", database_url)
    with engine.begin() as connection:
        connection.execute(
            rewards.insert().values(
                reward_id="reward-1",
                name="Coffee",
                point_cost=100,
                total_inventory=10,
                available_inventory=10,
            )
        )
        connection.execute(
            rewards.insert().values(
                reward_id="reward-2",
                name="Tea",
                point_cost=80,
                total_inventory=10,
                available_inventory=10,
            )
        )
        connection.execute(
            rewards.insert().values(
                reward_id="reward-empty",
                name="Sold out",
                point_cost=100,
                total_inventory=0,
                available_inventory=0,
            )
        )
    return engine


def command(
    kind: str,
    transaction_id,
    /,
    *,
    reward_id: str = "reward-1",
    quantity=2,
    step: int | None = 3,
    **overrides,
) -> EventEnvelope:
    fields = {
        "event_type": kind,
        "transaction_id": transaction_id,
        "correlation_id": uuid4(),
        "causation_id": uuid4(),
        "aggregate_type": "reward",
        "aggregate_id": reward_id,
        "workflow_step": step,
        "payload": {"reward_id": reward_id, "quantity": quantity},
    }
    fields.update(overrides)
    return EventEnvelope(**fields)


def deliver(database_url: str, engine: Engine, envelope: EventEnvelope):
    async def go():
        async_engine = create_async_engine(database_url)
        try:
            sessions = async_sessionmaker(async_engine, expire_on_commit=False)
            handler = CatalogSagaHandler(PostgresCatalogRepository(engine))
            return await process_once(sessions, CONSUMER, envelope, handler)
        finally:
            await async_engine.dispose()

    return asyncio.run(go())


def replies(engine: Engine) -> list[EventEnvelope]:
    with engine.connect() as connection:
        rows = connection.execute(select(outbox).order_by(outbox.c.id)).all()
    return [EventEnvelope.model_validate(row.envelope, context={"wire": True}) for row in rows]


def inventory(engine: Engine, reward_id: str = "reward-1") -> tuple[int, int]:
    with engine.connect() as connection:
        row = connection.execute(
            select(rewards.c.available_inventory, rewards.c.version).where(
                rewards.c.reward_id == reward_id
            )
        ).one()
    return tuple(row)


def count(engine: Engine, table) -> int:
    with engine.connect() as connection:
        return connection.scalar(select(func.count()).select_from(table))


def test_reserve_applies_and_replies_with_complete_envelope(database_url, catalog_db):
    transaction_id = uuid4()
    reserve = command(RESERVE_INVENTORY, transaction_id)

    deliver(database_url, catalog_db, reserve)

    assert inventory(catalog_db) == (8, 1)
    [reply] = replies(catalog_db)
    assert reply.event_type == INVENTORY_RESERVED
    assert reply.payload == {
        "reward_id": "reward-1",
        "quantity": 2,
        "available_inventory": 8,
        "outcome": "applied",
    }
    assert reply.transaction_id == reserve.transaction_id
    assert reply.correlation_id == reserve.correlation_id
    assert reply.causation_id == reserve.event_id
    assert reply.workflow_step == reserve.workflow_step
    assert (reply.aggregate_type, reply.aggregate_id, reply.aggregate_version) == (
        "reward",
        "reward-1",
        1,
    )
    with catalog_db.connect() as connection:
        row = connection.execute(select(outbox.c.topic, outbox.c.message_key)).one()
    assert tuple(row) == ("catalog.results", str(transaction_id))


@pytest.mark.parametrize(
    ("reward_id", "reason"),
    [("reward-empty", "insufficient_inventory"), ("reward-missing", "reward_not_found")],
)
def test_reserve_rejection_is_durable_for_new_event_ids(
    database_url, catalog_db, reward_id, reason
):
    transaction_id = uuid4()
    deliver(
        database_url,
        catalog_db,
        command(RESERVE_INVENTORY, transaction_id, reward_id=reward_id),
    )

    if reward_id == "reward-empty":
        with catalog_db.begin() as connection:
            connection.execute(
                update(rewards)
                .where(rewards.c.reward_id == reward_id)
                .values(total_inventory=10, available_inventory=10)
            )

    deliver(
        database_url,
        catalog_db,
        command(RESERVE_INVENTORY, transaction_id, reward_id=reward_id),
    )

    first, second = replies(catalog_db)
    assert first.event_type == second.event_type == RESERVATION_REJECTED
    assert first.payload["reason"] == second.payload["reason"] == reason
    assert count(catalog_db, reservations) == 0
    assert count(catalog_db, catalog_saga_outcomes) == 1


def test_redelivery_of_same_event_is_an_inbox_duplicate(database_url, catalog_db):
    reserve = command(RESERVE_INVENTORY, uuid4())
    first = deliver(database_url, catalog_db, reserve)
    second = deliver(database_url, catalog_db, reserve)

    assert not first.duplicate
    assert second.duplicate
    assert second.result_event_ids == first.result_event_ids
    assert inventory(catalog_db) == (8, 1)
    assert count(catalog_db, outbox) == 1


def test_retry_with_new_event_id_returns_stored_reserve_outcome(database_url, catalog_db):
    transaction_id = uuid4()
    deliver(database_url, catalog_db, command(RESERVE_INVENTORY, transaction_id))
    deliver(database_url, catalog_db, command(RESERVE_INVENTORY, transaction_id))

    assert inventory(catalog_db) == (8, 1)
    first, second = replies(catalog_db)
    assert (first.payload["outcome"], second.payload["outcome"]) == ("applied", "duplicate")
    assert second.aggregate_version is None


def test_transaction_id_reuse_with_different_quantity_returns_conflict(database_url, catalog_db):
    transaction_id = uuid4()
    deliver(database_url, catalog_db, command(RESERVE_INVENTORY, transaction_id, quantity=2))

    deliver(database_url, catalog_db, command(RESERVE_INVENTORY, transaction_id, quantity=3))

    assert inventory(catalog_db) == (8, 1)
    assert replies(catalog_db)[-1].event_type == RESERVATION_REJECTED
    assert replies(catalog_db)[-1].payload["reason"] == "conflict"


def test_release_restores_inventory_once(database_url, catalog_db):
    transaction_id = uuid4()
    deliver(database_url, catalog_db, command(RESERVE_INVENTORY, transaction_id))
    deliver(database_url, catalog_db, command(RELEASE_INVENTORY, transaction_id, step=4))
    deliver(database_url, catalog_db, command(RELEASE_INVENTORY, transaction_id, step=4))

    assert inventory(catalog_db) == (10, 2)
    release, duplicate = replies(catalog_db)[-2:]
    assert release.event_type == duplicate.event_type == INVENTORY_RELEASED
    assert (release.payload["outcome"], duplicate.payload["outcome"]) == (
        "applied",
        "duplicate",
    )


def test_release_retry_with_new_event_id_replays_success_without_releasing_twice(
    database_url, catalog_db
):
    transaction_id = uuid4()
    deliver(database_url, catalog_db, command(RESERVE_INVENTORY, transaction_id))
    first_release = command(RELEASE_INVENTORY, transaction_id, step=4)
    deliver(database_url, catalog_db, first_release)

    # Simulate a retry after the release committed but its result was lost.
    retry = command(RELEASE_INVENTORY, transaction_id, step=5)
    deliver(database_url, catalog_db, retry)

    assert inventory(catalog_db) == (10, 2)
    release, duplicate = replies(catalog_db)[-2:]
    assert release.event_type == duplicate.event_type == INVENTORY_RELEASED
    assert release.payload["outcome"] == "applied"
    assert duplicate.payload["outcome"] == "duplicate"
    assert duplicate.event_id != release.event_id
    assert duplicate.causation_id == retry.event_id
    assert duplicate.workflow_step == retry.workflow_step


def test_early_release_tombstones_and_cancels_late_reserve(database_url, catalog_db):
    transaction_id = uuid4()
    deliver(database_url, catalog_db, command(RELEASE_INVENTORY, transaction_id, step=4))
    deliver(database_url, catalog_db, command(RESERVE_INVENTORY, transaction_id, step=3))

    assert inventory(catalog_db) == (10, 0)
    release, reserve = replies(catalog_db)
    assert release.payload["outcome"] == "cancelled_before_reservation"
    assert reserve.event_type == RESERVATION_REJECTED
    assert reserve.payload["reason"] == "cancelled"
    with catalog_db.connect() as connection:
        assert connection.scalar(select(reservations.c.status)) == "tombstone"


def test_tombstone_does_not_hide_different_command_identity(database_url, catalog_db):
    transaction_id = uuid4()
    deliver(
        database_url,
        catalog_db,
        command(RELEASE_INVENTORY, transaction_id, quantity=2, step=4),
    )

    deliver(
        database_url,
        catalog_db,
        command(RESERVE_INVENTORY, transaction_id, quantity=3, step=3),
    )

    assert replies(catalog_db)[-1].payload["reason"] == "conflict"
    assert inventory(catalog_db) == (10, 0)


@pytest.mark.parametrize(
    "overrides",
    [
        {"event_type": "catalog.fulfill_inventory"},
        {"workflow_step": None},
        {"workflow_step": 0},
        {"aggregate_type": "wallet"},
        {"aggregate_version": 3},
        {"payload": {"reward_id": "reward-other", "quantity": 2}},
        {"payload": {"reward_id": "reward-1", "quantity": 0}},
        {"payload": {"reward_id": "reward-1", "quantity": "2"}},
        {"payload": {"reward_id": "reward-1"}},
        {"workflow_step": 4},
        {"event_type": RELEASE_INVENTORY},
    ],
)
def test_invalid_command_changes_nothing(database_url, catalog_db, overrides):
    invalid = command(RESERVE_INVENTORY, uuid4(), **overrides)
    with pytest.raises(InvalidCommand):
        deliver(database_url, catalog_db, invalid)

    assert inventory(catalog_db) == (10, 0)
    assert count(catalog_db, inbox) == 0
    assert count(catalog_db, outbox) == 0


def test_invalid_command_is_dead_lettered(database_url, catalog_db):
    dead_letters = []

    async def capture(record, error):
        dead_letters.append(error)

    async def go():
        async_engine = create_async_engine(database_url)
        try:
            consumer = InboxConsumer(
                None,
                async_sessionmaker(async_engine, expire_on_commit=False),
                name=CONSUMER,
                handler=CatalogSagaHandler(PostgresCatalogRepository(catalog_db)),
                on_dead_letter=capture,
            )
            invalid = command(RESERVE_INVENTORY, uuid4(), workflow_step=None)
            return await consumer.handle(SimpleNamespace(value=invalid.to_bytes()))
        finally:
            await async_engine.dispose()

    assert asyncio.run(go()) is None
    assert [type(error) for error in dead_letters] == [InvalidCommand]


def test_restart_before_offset_commit_does_not_reserve_twice(database_url, catalog_db):
    reserve = command(RESERVE_INVENTORY, uuid4())
    deliver(database_url, catalog_db, reserve)
    after_restart = deliver(database_url, catalog_db, reserve)

    assert after_restart.duplicate
    assert inventory(catalog_db) == (8, 1)
    assert count(catalog_db, reservations) == 1
    assert count(catalog_db, outbox) == 1


def transaction_replies(engine: Engine, transaction_id) -> list[EventEnvelope]:
    return [reply for reply in replies(engine) if reply.transaction_id == transaction_id]


def test_concurrent_identical_reservations_reply_applied_and_duplicate(database_url, catalog_db):
    transaction_id = uuid4()
    barrier = Barrier(2)

    def send(_):
        envelope = command(RESERVE_INVENTORY, transaction_id, quantity=2, step=3)
        barrier.wait()
        return deliver(database_url, catalog_db, envelope)

    with ThreadPoolExecutor(max_workers=2) as executor:
        list(executor.map(send, range(2)))

    results = transaction_replies(catalog_db, transaction_id)
    assert sorted(reply.payload["outcome"] for reply in results) == ["applied", "duplicate"]
    assert inventory(catalog_db, "reward-1") == (8, 1)
    assert count(catalog_db, reservations) == 1
    assert count(catalog_db, catalog_saga_outcomes) == 1


def test_concurrent_different_rewards_return_one_success_and_one_conflict(database_url, catalog_db):
    transaction_id = uuid4()
    barrier = Barrier(2)

    def send(reward_id):
        envelope = command(
            RESERVE_INVENTORY,
            transaction_id,
            reward_id=reward_id,
            quantity=2,
            step=3,
        )
        barrier.wait()
        return deliver(database_url, catalog_db, envelope)

    with ThreadPoolExecutor(max_workers=2) as executor:
        list(executor.map(send, ["reward-1", "reward-2"]))

    results = transaction_replies(catalog_db, transaction_id)
    assert sorted(reply.event_type for reply in results) == sorted(
        [INVENTORY_RESERVED, RESERVATION_REJECTED]
    )
    rejected = next(reply for reply in results if reply.event_type == RESERVATION_REJECTED)
    assert rejected.payload["reason"] == "conflict"
    assert sum(inventory(catalog_db, reward_id)[0] for reward_id in ("reward-1", "reward-2")) == 18
    assert count(catalog_db, reservations) == 1
    assert count(catalog_db, catalog_saga_outcomes) == 1
