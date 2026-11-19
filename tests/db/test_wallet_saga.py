"""Wallet Saga command consumer (LM-020).

Each test runs against a new, empty database (see conftest.py). Commands go
through ``process_once`` exactly as the worker runs them, so the inbox row, the
Wallet change and the reply in the outbox commit or roll back together.
"""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from types import SimpleNamespace
from uuid import uuid4

import pytest
from ledgermesh_messaging import (
    EventEnvelope,
    InboxConsumer,
    TransientError,
    inbox,
    outbox,
    process_once,
)
from sqlalchemy import Engine, func, select, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from services.wallet.app.db.schema import deduction_rejections, wallet_operations, wallets
from services.wallet.app.repository import PostgresWalletRepository
from services.wallet.app.saga import (
    DEDUCT_POINTS,
    DEDUCTION_REJECTED,
    POINTS_DEDUCTED,
    POINTS_REFUNDED,
    REFUND_POINTS,
    InvalidCommand,
    WalletSagaHandler,
)

from .conftest import upgrade

pytestmark = pytest.mark.database

CONSUMER = "wallet-saga"


@pytest.fixture
def wallet_db(database_url: str, engine: Engine) -> Engine:
    upgrade("wallet", database_url)
    with engine.begin() as connection:
        connection.execute(wallets.insert().values(user_id="user-1", available_points=100))
    return engine


def command(
    kind: str,
    transaction_id,
    /,
    *,
    points=40,
    user_id: str = "user-1",
    step: int | None = 1,
    **overrides,
) -> EventEnvelope:
    # Positional-only, so a test can still override any envelope field by name,
    # including event_type itself.
    fields = {
        "event_type": kind,
        "transaction_id": transaction_id,
        "correlation_id": uuid4(),
        "causation_id": None if step in (None, 1) else uuid4(),
        "aggregate_type": "wallet",
        "aggregate_id": user_id,
        "workflow_step": step,
        "payload": {"user_id": user_id, "points": points},
    }
    fields.update(overrides)
    return EventEnvelope(**fields)


def deliver(database_url: str, engine: Engine, envelope: EventEnvelope):
    """Run one command through process_once, as a fresh worker process would."""

    async def go():
        async_engine = create_async_engine(database_url)
        try:
            sessions = async_sessionmaker(async_engine, expire_on_commit=False)
            handler = WalletSagaHandler(PostgresWalletRepository(engine))
            return await process_once(sessions, CONSUMER, envelope, handler)
        finally:
            await async_engine.dispose()

    return asyncio.run(go())


def replies(engine: Engine) -> list[EventEnvelope]:
    with engine.connect() as connection:
        rows = connection.execute(select(outbox).order_by(outbox.c.id)).all()
    # Criterion 3: every reply is a complete wire envelope.
    return [EventEnvelope.model_validate(row.envelope, context={"wire": True}) for row in rows]


def reply_keys(engine: Engine) -> list[tuple[str, str]]:
    with engine.connect() as connection:
        return [
            tuple(row) for row in connection.execute(select(outbox.c.topic, outbox.c.message_key))
        ]


def balance(engine: Engine) -> tuple[int, int]:
    with engine.connect() as connection:
        row = connection.execute(
            select(wallets.c.available_points, wallets.c.version).where(
                wallets.c.user_id == "user-1"
            )
        ).one()
    return tuple(row)


def count(engine: Engine, table) -> int:
    with engine.connect() as connection:
        return connection.scalar(select(func.count()).select_from(table))


# Deductions


def test_deduction_applies_and_replies_with_a_complete_envelope(database_url, wallet_db):
    transaction_id = uuid4()
    deduct = command(DEDUCT_POINTS, transaction_id, step=1)

    deliver(database_url, wallet_db, deduct)

    assert balance(wallet_db) == (60, 1)
    [reply] = replies(wallet_db)
    assert reply.event_type == POINTS_DEDUCTED
    assert reply.payload == {
        "user_id": "user-1",
        "points": 40,
        "available_points": 60,
        "outcome": "applied",
    }
    # The reply carries the command's workflow identity forward.
    assert reply.transaction_id == deduct.transaction_id
    assert reply.correlation_id == deduct.correlation_id
    assert reply.causation_id == deduct.event_id
    assert reply.workflow_step == deduct.workflow_step
    assert (reply.aggregate_type, reply.aggregate_id, reply.aggregate_version) == (
        "wallet",
        "user-1",
        1,
    )
    assert reply_keys(wallet_db) == [("wallet.results", str(transaction_id))]


@pytest.mark.parametrize(
    ("points", "user_id", "reason"),
    [(101, "user-1", "insufficient_points"), (10, "user-unknown", "wallet_not_found")],
)
def test_rejected_deduction_replies_without_changing_the_balance(
    database_url, wallet_db, points, user_id, reason
):
    deliver(
        database_url, wallet_db, command(DEDUCT_POINTS, uuid4(), points=points, user_id=user_id)
    )

    assert balance(wallet_db) == (100, 0)
    assert count(wallet_db, wallet_operations) == 0
    [reply] = replies(wallet_db)
    assert reply.event_type == DEDUCTION_REJECTED
    assert reply.payload["reason"] == reason
    assert reply.aggregate_version is None
    # The rejection is stored for the transaction, not only for this event id.
    assert count(wallet_db, inbox) == 1
    assert count(wallet_db, deduction_rejections) == 1


def test_rejection_is_kept_after_the_wallet_is_topped_up(database_url, wallet_db):
    # The Coordinator already treated T as failed; a later command for T must not
    # deduct just because the balance has grown since.
    transaction_id = uuid4()
    deliver(database_url, wallet_db, command(DEDUCT_POINTS, transaction_id, points=101))
    with wallet_db.begin() as connection:
        connection.execute(update(wallets).values(available_points=500))

    deliver(database_url, wallet_db, command(DEDUCT_POINTS, transaction_id, points=101))

    assert balance(wallet_db) == (500, 0)
    assert count(wallet_db, wallet_operations) == 0
    first, second = replies(wallet_db)
    assert (first.payload["reason"], second.payload["reason"]) == (
        "insufficient_points",
        "insufficient_points",
    )


def test_wallet_not_found_rejection_is_kept_after_the_wallet_is_created(database_url, wallet_db):
    transaction_id = uuid4()
    late = {"user_id": "user-late", "points": 10}
    deliver(database_url, wallet_db, command(DEDUCT_POINTS, transaction_id, **late))
    with wallet_db.begin() as connection:
        connection.execute(wallets.insert().values(user_id="user-late", available_points=500))

    deliver(database_url, wallet_db, command(DEDUCT_POINTS, transaction_id, **late))

    assert count(wallet_db, wallet_operations) == 0
    assert [reply.payload["reason"] for reply in replies(wallet_db)] == [
        "wallet_not_found",
        "wallet_not_found",
    ]


def test_different_deduction_for_a_rejected_transaction_is_a_conflict(database_url, wallet_db):
    transaction_id = uuid4()
    deliver(database_url, wallet_db, command(DEDUCT_POINTS, transaction_id, points=101))

    deliver(database_url, wallet_db, command(DEDUCT_POINTS, transaction_id, points=50))

    assert balance(wallet_db) == (100, 0)
    assert replies(wallet_db)[-1].payload["reason"] == "conflict"


def test_transaction_id_reused_for_a_different_deduction_is_rejected(database_url, wallet_db):
    transaction_id = uuid4()
    deliver(database_url, wallet_db, command(DEDUCT_POINTS, transaction_id, points=40))

    deliver(database_url, wallet_db, command(DEDUCT_POINTS, transaction_id, points=50))

    assert balance(wallet_db) == (60, 1)
    assert replies(wallet_db)[-1].payload["reason"] == "conflict"


# Criterion 2: duplicates return the stored outcome


def test_redelivered_command_is_a_duplicate_and_applies_once(database_url, wallet_db):
    deduct = command(DEDUCT_POINTS, uuid4())

    first = deliver(database_url, wallet_db, deduct)
    second = deliver(database_url, wallet_db, deduct)

    assert not first.duplicate
    assert second.duplicate
    assert second.result_event_ids == first.result_event_ids
    assert balance(wallet_db) == (60, 1)
    assert count(wallet_db, outbox) == 1


def test_resent_command_with_a_new_event_id_replies_with_the_stored_outcome(
    database_url, wallet_db
):
    # The Coordinator timed out and sent the same deduction again.
    transaction_id = uuid4()
    deliver(database_url, wallet_db, command(DEDUCT_POINTS, transaction_id))

    deliver(database_url, wallet_db, command(DEDUCT_POINTS, transaction_id))

    assert balance(wallet_db) == (60, 1)
    first, second = replies(wallet_db)
    assert (first.payload["outcome"], second.payload["outcome"]) == ("applied", "duplicate")
    # A duplicate describes no new balance change, so it carries no version.
    assert second.aggregate_version is None


# Refunds and ADR-001 section 7.1


def test_refund_after_deduction_returns_the_stored_amount(database_url, wallet_db):
    transaction_id = uuid4()
    deliver(database_url, wallet_db, command(DEDUCT_POINTS, transaction_id, points=40, step=1))

    deliver(database_url, wallet_db, command(REFUND_POINTS, transaction_id, points=40, step=3))

    assert balance(wallet_db) == (100, 2)
    refund = replies(wallet_db)[-1]
    assert refund.event_type == POINTS_REFUNDED
    assert (refund.payload["refunded_points"], refund.payload["outcome"]) == (40, "applied")
    assert refund.aggregate_version == 2


def test_early_refund_cancels_the_late_deduction(database_url, wallet_db):
    transaction_id = uuid4()

    deliver(database_url, wallet_db, command(REFUND_POINTS, transaction_id, points=40, step=3))
    deliver(database_url, wallet_db, command(DEDUCT_POINTS, transaction_id, points=40, step=1))

    # INV-06: no unmatched deduction, and the balance never moved.
    assert balance(wallet_db) == (100, 0)
    refund, late_deduction = replies(wallet_db)
    assert refund.event_type == POINTS_REFUNDED
    assert (refund.payload["refunded_points"], refund.payload["outcome"]) == (
        0,
        "cancelled_before_deduction",
    )
    assert late_deduction.event_type == DEDUCTION_REJECTED
    assert late_deduction.payload["reason"] == "cancelled"
    with wallet_db.connect() as connection:
        statuses = connection.scalars(select(wallet_operations.c.status)).all()
    assert statuses == ["tombstone"]


def test_repeated_early_refund_keeps_one_tombstone(database_url, wallet_db):
    transaction_id = uuid4()

    deliver(database_url, wallet_db, command(REFUND_POINTS, transaction_id, step=3))
    deliver(database_url, wallet_db, command(REFUND_POINTS, transaction_id, step=3))

    assert count(wallet_db, wallet_operations) == 1
    outcomes = [reply.payload["outcome"] for reply in replies(wallet_db)]
    assert outcomes == ["cancelled_before_deduction", "cancelled_before_deduction"]


@pytest.mark.parametrize(
    "late",
    [{"points": 50}, {"user_id": "user-2"}],
    ids=["different-points", "different-user"],
)
def test_deduction_that_differs_from_the_cancelled_one_is_a_conflict(database_url, wallet_db, late):
    # A tombstone cancels one exact deduction; reusing its transaction id for
    # another body is a conflict, not a valid cancellation.
    with wallet_db.begin() as connection:
        connection.execute(wallets.insert().values(user_id="user-2", available_points=100))
    transaction_id = uuid4()
    deliver(database_url, wallet_db, command(REFUND_POINTS, transaction_id, points=40, step=3))

    deliver(database_url, wallet_db, command(DEDUCT_POINTS, transaction_id, **late))

    assert replies(wallet_db)[-1].payload["reason"] == "conflict"
    assert count(wallet_db, wallet_operations) == 1


def test_repeated_early_refund_with_a_different_amount_is_not_a_duplicate(database_url, wallet_db):
    transaction_id = uuid4()
    deliver(database_url, wallet_db, command(REFUND_POINTS, transaction_id, points=40, step=3))

    with pytest.raises(InvalidCommand):
        deliver(database_url, wallet_db, command(REFUND_POINTS, transaction_id, points=50, step=3))

    assert count(wallet_db, wallet_operations) == 1
    assert len(replies(wallet_db)) == 1


def test_concurrent_deduction_and_refund_never_leave_an_unmatched_deduction(
    database_url, wallet_db
):
    # Race a deduction against its refund many times. Each transaction must end
    # either deducted-then-refunded or cancelled with no deduction (INV-06).
    def deliver_with_retries(envelope):
        # InboxConsumer retries a TransientError; mirror that here.
        for _ in range(5):
            try:
                return deliver(database_url, wallet_db, envelope)
            except TransientError:
                continue
        raise AssertionError("refund did not settle after retries")

    for _ in range(10):
        transaction_id = uuid4()
        barrier = Barrier(2)

        def send(envelope, barrier=barrier):
            barrier.wait()
            return deliver_with_retries(envelope)

        with ThreadPoolExecutor(max_workers=2) as executor:
            list(
                executor.map(
                    send,
                    [
                        command(DEDUCT_POINTS, transaction_id, points=40, step=1),
                        command(REFUND_POINTS, transaction_id, points=40, step=3),
                    ],
                )
            )

        with wallet_db.connect() as connection:
            rows = connection.execute(
                select(wallet_operations.c.operation, wallet_operations.c.status).where(
                    wallet_operations.c.transaction_id == transaction_id
                )
            ).all()
        assert sorted(tuple(row) for row in rows) in (
            [("deduct", "applied"), ("refund", "applied")],
            [("deduct", "tombstone")],
        )
        assert balance(wallet_db)[0] == 100


def reasons_for(engine: Engine, transaction_id) -> list[str]:
    return [
        reply.payload["reason"]
        for reply in replies(engine)
        if reply.transaction_id == transaction_id
    ]


@pytest.mark.parametrize(
    ("first", "second", "reason"),
    [
        ({"points": 101}, {"points": 102}, "insufficient_points"),
        (
            {"user_id": "user-ghost-a", "points": 10},
            {"user_id": "user-ghost-b", "points": 10},
            "wallet_not_found",
        ),
    ],
    ids=["same-wallet-different-points", "two-missing-wallets"],
)
def test_concurrent_conflicting_rejections_store_one_identity(
    database_url, wallet_db, first, second, reason
):
    # Two different deductions reuse one transaction id and both fail at the
    # same time. Only one identity may be stored; the other command is a conflict.
    for _ in range(10):
        transaction_id = uuid4()
        bodies = [first, second]
        barrier = Barrier(2)

        def send(body, barrier=barrier, transaction_id=transaction_id):
            barrier.wait()
            return deliver(database_url, wallet_db, command(DEDUCT_POINTS, transaction_id, **body))

        with ThreadPoolExecutor(max_workers=2) as executor:
            list(executor.map(send, bodies))

        with wallet_db.connect() as connection:
            stored = connection.execute(
                select(deduction_rejections).where(
                    deduction_rejections.c.transaction_id == transaction_id
                )
            ).all()
        assert len(stored) == 1
        assert sorted(reasons_for(wallet_db, transaction_id)) == sorted([reason, "conflict"])

        # Later retries (new event ids) reproduce the same outcome per body.
        [winner] = stored
        for body in bodies:
            deliver(database_url, wallet_db, command(DEDUCT_POINTS, transaction_id, **body))
        expected = [
            reason
            if (body.get("user_id", "user-1"), body["points"]) == (winner.user_id, winner.points)
            else "conflict"
            for body in bodies
        ]
        assert reasons_for(wallet_db, transaction_id)[-2:] == expected

    # Nothing was ever deducted.
    assert balance(wallet_db) == (100, 0)
    assert count(wallet_db, wallet_operations) == 0


# Criterion 1: commands are validated before anything is applied


@pytest.mark.parametrize(
    "overrides",
    [
        {"event_type": "wallet.transfer_points"},
        {"workflow_step": None},
        {"workflow_step": 0},
        {"aggregate_type": "reward"},
        {"aggregate_version": 3},
        {"payload": {"user_id": "user-2", "points": 10}},
        {"payload": {"user_id": "user-1", "points": 0}},
        {"payload": {"user_id": "user-1", "points": "10"}},
        {"payload": {"user_id": "user-1"}},
        {"workflow_step": 3},
        {"event_type": REFUND_POINTS},
    ],
    ids=[
        "unknown-command",
        "no-workflow-step",
        "zero-workflow-step",
        "wrong-aggregate",
        "command-with-version",
        "user-mismatch",
        "zero-points",
        "string-points",
        "missing-points",
        "deduction-at-a-compensation-step",
        "refund-at-the-forward-step",
    ],
)
def test_invalid_command_changes_nothing(database_url, wallet_db, overrides):
    invalid = command(DEDUCT_POINTS, uuid4(), **overrides)

    with pytest.raises(InvalidCommand):
        deliver(database_url, wallet_db, invalid)

    assert balance(wallet_db) == (100, 0)
    assert count(wallet_db, inbox) == 0
    assert count(wallet_db, outbox) == 0


def test_invalid_command_is_dead_lettered_by_the_consumer(database_url, wallet_db):
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
                handler=WalletSagaHandler(PostgresWalletRepository(wallet_db)),
                on_dead_letter=capture,
            )
            invalid = command(DEDUCT_POINTS, uuid4(), workflow_step=None)
            return await consumer.handle(SimpleNamespace(value=invalid.to_bytes()))
        finally:
            await async_engine.dispose()

    assert asyncio.run(go()) is None
    assert [type(error) for error in dead_letters] == [InvalidCommand]
    assert balance(wallet_db) == (100, 0)


# Criterion 4: a restart resumes safely


def test_restart_before_offset_commit_does_not_apply_twice(database_url, wallet_db):
    # Process 1 commits the deduction, then dies before committing the Kafka
    # offset. Process 2 (new engine, new handler) receives the same record.
    deduct = command(DEDUCT_POINTS, uuid4())

    deliver(database_url, wallet_db, deduct)
    after_restart = deliver(database_url, wallet_db, deduct)

    assert after_restart.duplicate
    assert balance(wallet_db) == (60, 1)
    assert count(wallet_db, wallet_operations) == 1
    # The reply committed with the deduction, so the publisher still sends it.
    assert count(wallet_db, outbox) == 1
