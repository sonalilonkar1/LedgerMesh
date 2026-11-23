"""PostgreSQL tests for the durable Coordinator Saga state machine (LM-022)."""

import asyncio
from collections import defaultdict
from uuid import uuid4

import httpx
import pytest
from ledgermesh_messaging import EventEnvelope, enqueue, outbox, process_once
from sqlalchemy import Engine, func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from services.coordinator.app.db.schema import redemption_transitions, redemptions
from services.coordinator.app.models import CreateRedemptionRequest
from services.coordinator.app.repository import PostgresCoordinatorRepository
from services.coordinator.app.saga import (
    CoordinatorSagaMachine,
    CoordinatorSagaResultHandler,
    InvalidSagaResult,
    RewardQuote,
    SagaStarter,
)

from .conftest import upgrade

pytestmark = pytest.mark.database
CONSUMER = "coordinator-saga"


class FakeQuotes:
    async def get_quote(self, reward_id: str) -> RewardQuote:
        return RewardQuote(reward_id, point_cost=100, available_inventory=10, version=7)


class MissingRewardQuotes:
    async def get_quote(self, reward_id: str) -> RewardQuote:
        request = httpx.Request("GET", f"http://catalog/v1/rewards/{reward_id}")
        response = httpx.Response(404, request=request)
        raise httpx.HTTPStatusError("not found", request=request, response=response)


class InvalidQuotes:
    async def get_quote(self, reward_id: str) -> RewardQuote:
        return RewardQuote(reward_id, point_cost=0, available_inventory=10, version=7)


class FailingQuotes:
    def __init__(self) -> None:
        self.calls = defaultdict(int)

    async def get_quote(self, reward_id: str) -> RewardQuote:
        self.calls[reward_id] += 1
        if reward_id == "reward-broken":
            raise httpx.ConnectError("Catalog unavailable")
        return RewardQuote(reward_id, point_cost=100, available_inventory=10, version=7)


@pytest.fixture
def saga_db(database_url: str, engine: Engine):
    upgrade("coordinator", database_url)
    return engine


def create(repository: PostgresCoordinatorRepository):
    return repository.create(
        f"saga-{uuid4()}",
        CreateRedemptionRequest(
            user_id="user-alice",
            reward_id="reward-coffee",
            quantity=2,
            coordination_mode="saga",
        ),
    )


def row(engine: Engine, transaction_id):
    with engine.connect() as connection:
        return connection.execute(
            select(redemptions).where(redemptions.c.transaction_id == transaction_id)
        ).one()


def messages(engine: Engine):
    with engine.connect() as connection:
        return connection.execute(select(outbox).order_by(outbox.c.id)).all()


async def start(database_url, transaction_id):
    async_engine = create_async_engine(database_url)
    sessions = async_sessionmaker(async_engine, expire_on_commit=False)
    try:
        count = await SagaStarter(sessions, FakeQuotes(), CoordinatorSagaMachine()).run_once()
        assert count == 1
    finally:
        await async_engine.dispose()


async def run_starter(database_url, quotes, **kwargs):
    async_engine = create_async_engine(database_url)
    sessions = async_sessionmaker(async_engine, expire_on_commit=False)
    try:
        starter = SagaStarter(sessions, quotes, CoordinatorSagaMachine(), **kwargs)
        return await starter.run_once(), starter
    finally:
        await async_engine.dispose()


async def run_starter_twice(database_url, quotes, **kwargs):
    async_engine = create_async_engine(database_url)
    sessions = async_sessionmaker(async_engine, expire_on_commit=False)
    try:
        starter = SagaStarter(sessions, quotes, CoordinatorSagaMachine(), **kwargs)
        return await starter.run_once(), await starter.run_once()
    finally:
        await async_engine.dispose()


def result(current, event_type: str, *, aggregate_type: str, aggregate_id: str, payload=None):
    return EventEnvelope(
        event_type=event_type,
        transaction_id=current.transaction_id,
        correlation_id=current.correlation_id,
        causation_id=current.current_command_id,
        aggregate_type=aggregate_type,
        aggregate_id=aggregate_id,
        aggregate_version=1,
        workflow_step=current.workflow_step,
        payload=payload or {},
    )


async def consume(database_url: str, envelope: EventEnvelope):
    async_engine = create_async_engine(database_url)
    sessions = async_sessionmaker(async_engine, expire_on_commit=False)
    try:
        # A new handler instance models a Coordinator restart.
        return await process_once(
            sessions,
            CONSUMER,
            envelope,
            CoordinatorSagaResultHandler(CoordinatorSagaMachine()),
        )
    finally:
        await async_engine.dispose()


async def begin_compensation(
    database_url: str,
    transaction_id,
    *,
    reservation_may_exist: bool,
):
    async_engine = create_async_engine(database_url)
    sessions = async_sessionmaker(async_engine, expire_on_commit=False)
    try:
        async with sessions() as session, session.begin():
            outgoing = await session.run_sync(
                lambda sync: CoordinatorSagaMachine().begin_compensation(
                    sync.connection(),
                    transaction_id,
                    "participant result timed out",
                    reservation_may_exist=reservation_may_exist,
                )
            )
            for message in outgoing:
                await enqueue(session, message)
            return outgoing
    finally:
        await async_engine.dispose()


async def retry_compensation(database_url: str, transaction_id):
    async_engine = create_async_engine(database_url)
    sessions = async_sessionmaker(async_engine, expire_on_commit=False)
    try:
        async with sessions() as session, session.begin():
            outgoing = await session.run_sync(
                lambda sync: CoordinatorSagaMachine().retry_compensation(
                    sync.connection(), transaction_id
                )
            )
            for message in outgoing:
                await enqueue(session, message)
            return outgoing
    finally:
        await async_engine.dispose()


def event_types(engine: Engine):
    return [item.envelope["event_type"] for item in messages(engine)]


def test_starter_persists_quote_transition_command_and_audits(database_url, saga_db):
    created = create(PostgresCoordinatorRepository(saga_db))

    asyncio.run(start(database_url, created.transaction_id))

    current = row(saga_db, created.transaction_id)
    queued = messages(saga_db)
    assert current.status == "DEDUCTING_POINTS"
    assert (current.point_cost, current.reward_version, current.total_points) == (100, 7, 200)
    assert current.current_command_id is not None
    assert [item.topic for item in queued] == [
        "redemption.events",
        "redemption.events",
        "wallet.commands",
    ]
    command = queued[-1].envelope
    assert command["event_type"] == "wallet.deduct_points"
    assert command["payload"] == {"user_id": "user-alice", "points": 200}


def test_happy_path_resumes_from_durable_state_after_each_restart(database_url, saga_db):
    repository = PostgresCoordinatorRepository(saga_db)
    created = create(repository)
    asyncio.run(start(database_url, created.transaction_id))

    current = row(saga_db, created.transaction_id)
    asyncio.run(
        consume(
            database_url,
            result(
                current,
                "wallet.points_deducted",
                aggregate_type="wallet",
                aggregate_id=current.user_id,
            ),
        )
    )
    current = row(saga_db, created.transaction_id)
    assert current.status == "RESERVING_INVENTORY"

    asyncio.run(
        consume(
            database_url,
            result(
                current,
                "catalog.inventory_reserved",
                aggregate_type="reward",
                aggregate_id=current.reward_id,
            ),
        )
    )
    current = row(saga_db, created.transaction_id)
    assert current.status == "COMPLETED"
    assert current.current_command_id is None

    with saga_db.connect() as connection:
        history = connection.scalars(
            select(redemption_transitions.c.to_status)
            .where(redemption_transitions.c.transaction_id == created.transaction_id)
            .order_by(redemption_transitions.c.id)
        ).all()
        audit_count = connection.scalar(
            select(func.count()).select_from(outbox).where(outbox.c.topic == "redemption.events")
        )
    assert history == [
        "REQUESTED",
        "DEDUCTING_POINTS",
        "POINTS_DEDUCTED",
        "RESERVING_INVENTORY",
        "COMPLETED",
    ]
    assert audit_count == len(history)


def test_wallet_rejection_fails_without_sending_catalog_command(database_url, saga_db):
    created = create(PostgresCoordinatorRepository(saga_db))
    asyncio.run(start(database_url, created.transaction_id))
    current = row(saga_db, created.transaction_id)

    asyncio.run(
        consume(
            database_url,
            result(
                current,
                "wallet.deduction_rejected",
                aggregate_type="wallet",
                aggregate_id=current.user_id,
                payload={"reason": "insufficient_points"},
            ),
        )
    )

    assert row(saga_db, created.transaction_id).status == "FAILED"
    assert all(item.topic != "catalog.commands" for item in messages(saga_db))


def test_catalog_rejection_refunds_then_compensates(database_url, saga_db):
    created = create(PostgresCoordinatorRepository(saga_db))
    asyncio.run(start(database_url, created.transaction_id))
    current = row(saga_db, created.transaction_id)
    asyncio.run(
        consume(
            database_url,
            result(
                current,
                "wallet.points_deducted",
                aggregate_type="wallet",
                aggregate_id=current.user_id,
            ),
        )
    )
    current = row(saga_db, created.transaction_id)
    asyncio.run(
        consume(
            database_url,
            result(
                current,
                "catalog.reservation_rejected",
                aggregate_type="reward",
                aggregate_id=current.reward_id,
            ),
        )
    )
    current = row(saga_db, created.transaction_id)
    assert current.status == "COMPENSATING"
    refund = messages(saga_db)[-1].envelope
    assert refund["event_type"] == "wallet.refund_points"
    assert refund["payload"]["points"] == 200

    # A duplicate rejection with a new event id is stale after the workflow
    # advances; it cannot enqueue a second refund.
    duplicate_rejection = result(
        row(saga_db, created.transaction_id),
        "catalog.reservation_rejected",
        aggregate_type="reward",
        aggregate_id=current.reward_id,
    ).model_copy(
        update={
            "event_id": uuid4(),
            "workflow_step": current.workflow_step - 1,
            "causation_id": uuid4(),
        }
    )
    asyncio.run(consume(database_url, duplicate_rejection))
    assert event_types(saga_db).count("wallet.refund_points") == 1

    asyncio.run(
        consume(
            database_url,
            result(
                current,
                "wallet.points_refunded",
                aggregate_type="wallet",
                aggregate_id=current.user_id,
            ),
        )
    )
    assert row(saga_db, created.transaction_id).status == "COMPENSATED"
    assert event_types(saga_db).count("wallet.refund_points") == 1


def test_unknown_reservation_releases_before_refund_and_survives_restarts(database_url, saga_db):
    created = create(PostgresCoordinatorRepository(saga_db))
    asyncio.run(start(database_url, created.transaction_id))
    deducting = row(saga_db, created.transaction_id)
    asyncio.run(
        consume(
            database_url,
            result(
                deducting,
                "wallet.points_deducted",
                aggregate_type="wallet",
                aggregate_id=deducting.user_id,
            ),
        )
    )

    asyncio.run(
        begin_compensation(
            database_url,
            created.transaction_id,
            reservation_may_exist=True,
        )
    )
    releasing = row(saga_db, created.transaction_id)
    assert releasing.status == "COMPENSATING"
    assert event_types(saga_db)[-1] == "catalog.release_inventory"
    assert event_types(saga_db).count("wallet.refund_points") == 0

    asyncio.run(
        consume(
            database_url,
            result(
                releasing,
                "catalog.inventory_released",
                aggregate_type="reward",
                aggregate_id=releasing.reward_id,
            ),
        )
    )
    refunding = row(saga_db, created.transaction_id)
    assert refunding.status == "COMPENSATING"
    assert event_types(saga_db)[-1] == "wallet.refund_points"
    assert event_types(saga_db).count("catalog.release_inventory") == 1
    assert event_types(saga_db).count("wallet.refund_points") == 1

    asyncio.run(
        consume(
            database_url,
            result(
                refunding,
                "wallet.points_refunded",
                aggregate_type="wallet",
                aggregate_id=refunding.user_id,
            ),
        )
    )
    assert row(saga_db, created.transaction_id).status == "COMPENSATED"

    with saga_db.connect() as connection:
        transition_count = connection.scalar(
            select(func.count())
            .select_from(redemption_transitions)
            .where(redemption_transitions.c.transaction_id == created.transaction_id)
        )
        audit_count = connection.scalar(
            select(func.count())
            .select_from(outbox)
            .where(
                outbox.c.topic == "redemption.events",
                outbox.c.message_key == str(created.transaction_id),
            )
        )
    assert audit_count == transition_count


def test_rejected_release_remains_compensating_and_can_be_retried(database_url, saga_db):
    created = create(PostgresCoordinatorRepository(saga_db))
    asyncio.run(start(database_url, created.transaction_id))
    deducting = row(saga_db, created.transaction_id)
    asyncio.run(
        consume(
            database_url,
            result(
                deducting,
                "wallet.points_deducted",
                aggregate_type="wallet",
                aggregate_id=deducting.user_id,
            ),
        )
    )
    asyncio.run(
        begin_compensation(
            database_url,
            created.transaction_id,
            reservation_may_exist=True,
        )
    )
    releasing = row(saga_db, created.transaction_id)
    asyncio.run(
        consume(
            database_url,
            result(
                releasing,
                "catalog.release_rejected",
                aggregate_type="reward",
                aggregate_id=releasing.reward_id,
                payload={"reason": "temporary_failure"},
            ),
        )
    )

    waiting = row(saga_db, created.transaction_id)
    assert waiting.status == "COMPENSATING"
    assert event_types(saga_db).count("catalog.release_inventory") == 1

    asyncio.run(retry_compensation(database_url, created.transaction_id))
    retried = row(saga_db, created.transaction_id)
    assert retried.status == "COMPENSATING"
    assert retried.current_command_id != waiting.current_command_id
    assert event_types(saga_db).count("catalog.release_inventory") == 2
    assert event_types(saga_db).count("wallet.refund_points") == 0


def test_refund_result_cannot_skip_a_pending_inventory_release(database_url, saga_db):
    created = create(PostgresCoordinatorRepository(saga_db))
    asyncio.run(start(database_url, created.transaction_id))
    deducting = row(saga_db, created.transaction_id)
    asyncio.run(
        consume(
            database_url,
            result(
                deducting,
                "wallet.points_deducted",
                aggregate_type="wallet",
                aggregate_id=deducting.user_id,
            ),
        )
    )
    asyncio.run(
        begin_compensation(
            database_url,
            created.transaction_id,
            reservation_may_exist=True,
        )
    )
    releasing = row(saga_db, created.transaction_id)
    forged_refund = result(
        releasing,
        "wallet.points_refunded",
        aggregate_type="wallet",
        aggregate_id=releasing.user_id,
    )

    with pytest.raises(InvalidSagaResult, match="does not match the current command"):
        asyncio.run(consume(database_url, forged_refund))

    assert row(saga_db, created.transaction_id).status == "COMPENSATING"
    assert event_types(saga_db).count("wallet.refund_points") == 0


def test_unknown_deduction_requests_only_safe_refund(database_url, saga_db):
    created = create(PostgresCoordinatorRepository(saga_db))
    asyncio.run(start(database_url, created.transaction_id))

    asyncio.run(
        begin_compensation(
            database_url,
            created.transaction_id,
            reservation_may_exist=False,
        )
    )
    compensating = row(saga_db, created.transaction_id)
    assert compensating.status == "COMPENSATING"
    assert event_types(saga_db).count("catalog.release_inventory") == 0
    assert event_types(saga_db).count("wallet.refund_points") == 1

    asyncio.run(
        consume(
            database_url,
            result(
                compensating,
                "wallet.points_refunded",
                aggregate_type="wallet",
                aggregate_id=compensating.user_id,
                payload={"outcome": "cancelled_before_deduction"},
            ),
        )
    )
    assert row(saga_db, created.transaction_id).status == "COMPENSATED"
    assert row(saga_db, created.transaction_id).status != "COMPLETED"


@pytest.mark.parametrize("bad_field", ["causation_id", "workflow_step", "correlation_id"])
def test_unexpected_result_is_rejected_without_a_transition(database_url, saga_db, bad_field):
    created = create(PostgresCoordinatorRepository(saga_db))
    asyncio.run(start(database_url, created.transaction_id))
    current = row(saga_db, created.transaction_id)
    envelope = result(
        current,
        "wallet.points_deducted",
        aggregate_type="wallet",
        aggregate_id=current.user_id,
    )
    replacement = {
        "causation_id": uuid4(),
        "workflow_step": current.workflow_step + 1,
        "correlation_id": uuid4(),
    }[bad_field]
    envelope = envelope.model_copy(update={bad_field: replacement})

    with pytest.raises(InvalidSagaResult):
        asyncio.run(consume(database_url, envelope))

    assert row(saga_db, created.transaction_id).status == "DEDUCTING_POINTS"


def test_same_event_redelivery_is_an_inbox_duplicate(database_url, saga_db):
    created = create(PostgresCoordinatorRepository(saga_db))
    asyncio.run(start(database_url, created.transaction_id))
    current = row(saga_db, created.transaction_id)
    envelope = result(
        current,
        "wallet.points_deducted",
        aggregate_type="wallet",
        aggregate_id=current.user_id,
    )

    first = asyncio.run(consume(database_url, envelope))
    second = asyncio.run(consume(database_url, envelope))

    assert first.duplicate is False
    assert second.duplicate is True
    assert row(saga_db, created.transaction_id).status == "RESERVING_INVENTORY"


def test_late_result_with_new_event_id_is_recorded_and_ignored(database_url, saga_db):
    created = create(PostgresCoordinatorRepository(saga_db))
    asyncio.run(start(database_url, created.transaction_id))
    deducting = row(saga_db, created.transaction_id)
    original = result(
        deducting,
        "wallet.points_deducted",
        aggregate_type="wallet",
        aggregate_id=deducting.user_id,
    )

    asyncio.run(consume(database_url, original))
    before = len(messages(saga_db))
    late = original.model_copy(update={"event_id": uuid4()})
    handled = asyncio.run(consume(database_url, late))

    assert handled.duplicate is False
    assert row(saga_db, created.transaction_id).status == "RESERVING_INVENTORY"
    assert len(messages(saga_db)) == before


def test_result_after_terminal_state_is_recorded_and_ignored(database_url, saga_db):
    created = create(PostgresCoordinatorRepository(saga_db))
    asyncio.run(start(database_url, created.transaction_id))
    deducting = row(saga_db, created.transaction_id)
    rejected = result(
        deducting,
        "wallet.deduction_rejected",
        aggregate_type="wallet",
        aggregate_id=deducting.user_id,
    )
    asyncio.run(consume(database_url, rejected))
    before = len(messages(saga_db))

    late = rejected.model_copy(update={"event_id": uuid4()})
    handled = asyncio.run(consume(database_url, late))

    assert handled.duplicate is False
    assert row(saga_db, created.transaction_id).status == "FAILED"
    assert len(messages(saga_db)) == before


@pytest.mark.parametrize("quotes", [MissingRewardQuotes(), InvalidQuotes()])
def test_permanent_quote_failure_marks_only_that_redemption_failed(database_url, saga_db, quotes):
    created = create(PostgresCoordinatorRepository(saga_db))

    count, _ = asyncio.run(run_starter(database_url, quotes))

    assert count == 0
    assert row(saga_db, created.transaction_id).status == "FAILED"
    queued = messages(saga_db)
    assert queued[-1].topic == "redemption.events"
    assert queued[-1].envelope["payload"]["to_status"] == "FAILED"


def test_transient_quote_failure_is_isolated_and_backed_off(database_url, saga_db):
    repository = PostgresCoordinatorRepository(saga_db)
    broken = repository.create(
        f"saga-{uuid4()}",
        CreateRedemptionRequest(
            user_id="user-alice",
            reward_id="reward-broken",
            quantity=1,
            coordination_mode="saga",
        ),
    )
    healthy = create(repository)
    quotes = FailingQuotes()

    first_count, second_count = asyncio.run(
        run_starter_twice(database_url, quotes, poll_interval=10.0, max_backoff=10.0)
    )

    assert first_count == 1
    assert second_count == 0
    assert row(saga_db, broken.transaction_id).status == "REQUESTED"
    assert row(saga_db, healthy.transaction_id).status == "DEDUCTING_POINTS"
    assert quotes.calls["reward-broken"] == 1

    assert quotes.calls["reward-broken"] == 1
