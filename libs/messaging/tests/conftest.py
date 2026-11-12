import os
from uuid import uuid4

import pytest
from ledgermesh_messaging import EventEnvelope, OutgoingMessage, metadata
from sqlalchemy import Column, Integer, MetaData, String, Table, insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

DATABASE_URL = os.environ.get(
    "LM_TEST_DATABASE_URL",
    "postgresql+psycopg://ledgermesh:ledgermesh@localhost:55432/messaging_test",
)
KAFKA_BOOTSTRAP = os.environ.get("LM_TEST_KAFKA_BOOTSTRAP", "localhost:59092")

# Stand-in for a service's business table: every applied event adds 1.
business_metadata = MetaData()
counters = Table(
    "counters",
    business_metadata,
    Column("name", String(64), primary_key=True),
    Column("value", Integer, nullable=False),
)


@pytest.fixture
async def session_factory():
    engine = create_async_engine(DATABASE_URL, pool_size=30, max_overflow=10)
    async with engine.begin() as conn:
        await conn.run_sync(metadata.drop_all)
        await conn.run_sync(business_metadata.drop_all)
        await conn.run_sync(metadata.create_all)
        await conn.run_sync(business_metadata.create_all)
        await conn.execute(insert(counters).values(name="applied", value=0))
    yield async_sessionmaker(engine, expire_on_commit=False)
    await engine.dispose()


async def read_counter(session_factory: async_sessionmaker[AsyncSession]) -> int:
    async with session_factory() as session:
        return await session.scalar(select(counters.c.value).where(counters.c.name == "applied"))


def make_envelope(**overrides) -> EventEnvelope:
    fields = dict(
        event_type="wallet.deduct_requested",
        transaction_id=uuid4(),
        correlation_id=uuid4(),
        aggregate_type="wallet",
        aggregate_id="user-1",
        payload={"points": 10},
    )
    fields.update(overrides)
    return EventEnvelope(**fields)


async def increment_and_reply(
    session: AsyncSession, envelope: EventEnvelope
) -> list[OutgoingMessage]:
    """Handler used by the tests: one business write and one reply event."""
    await session.execute(
        update(counters).where(counters.c.name == "applied").values(value=counters.c.value + 1)
    )
    reply = make_envelope(
        event_type="wallet.deducted",
        transaction_id=envelope.transaction_id,
        correlation_id=envelope.correlation_id,
        causation_id=envelope.event_id,
    )
    return [
        OutgoingMessage(topic="wallet.results", key=str(envelope.transaction_id), envelope=reply)
    ]
