"""One Saga redemption reconstructed from logs across services (LM-032).

Runs the real Coordinator and Wallet code against their own databases (the
LM-017 four-database harness) through ``InboxConsumer``, exactly as the
workers do, then filters the captured JSON logs by ``transaction_id``.
"""

import asyncio
import json
import logging
from types import SimpleNamespace
from uuid import UUID

import pytest
from ledgermesh_messaging import EventEnvelope, InboxConsumer, log_context
from ledgermesh_messaging.logs import BASE_FIELDS, CORRELATION_FIELDS, JsonFormatter
from ledgermesh_messaging.schema import outbox
from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from services.coordinator.app.models import CreateRedemptionRequest
from services.coordinator.app.repository import PostgresCoordinatorRepository
from services.coordinator.app.saga import (
    CoordinatorSagaMachine,
    CoordinatorSagaResultHandler,
    RewardQuote,
)
from services.wallet.app.db.schema import wallets
from services.wallet.app.repository import PostgresWalletRepository
from services.wallet.app.saga import WalletSagaHandler

pytestmark = pytest.mark.database


class Capture(logging.Handler):
    def __init__(self):
        super().__init__()
        self.setFormatter(JsonFormatter("unknown"))
        self.lines = []

    def emit(self, record):
        self.lines.append(json.loads(self.format(record)))


@pytest.fixture
def captured():
    capture = Capture()
    root = logging.getLogger()
    previous = root.level
    root.addHandler(capture)
    root.setLevel(logging.INFO)
    yield capture.lines
    root.removeHandler(capture)
    root.setLevel(previous)


def kafka_record(envelope: EventEnvelope):
    return SimpleNamespace(
        topic="t", partition=0, offset=0, key=b"k", value=envelope.to_bytes(), headers=[]
    )


def consume(url: str, name: str, handler, envelope: EventEnvelope):
    async def go():
        engine = create_async_engine(url)
        try:
            sessions = async_sessionmaker(engine, expire_on_commit=False)
            return await InboxConsumer(None, sessions, name=name, handler=handler).handle(
                kafka_record(envelope)
            )
        finally:
            await engine.dispose()

    return asyncio.run(go())


def test_one_saga_redemption_can_be_followed_across_services(service_databases, captured):
    engines, urls = service_databases.engines, service_databases.urls
    with engines["wallet"].begin() as connection:
        connection.execute(wallets.insert().values(user_id="user-logs", available_points=500))
    machine = CoordinatorSagaMachine()

    # Coordinator: create the redemption and start the Saga.
    with log_context(service="coordinator"):
        created = PostgresCoordinatorRepository(engines["coordinator"]).create(
            "lm032-trail",
            CreateRedemptionRequest(
                user_id="user-logs", reward_id="reward-logs", quantity=2, coordination_mode="saga"
            ),
        )
        with engines["coordinator"].begin() as connection:
            messages = machine.start(
                connection, created.transaction_id, RewardQuote("reward-logs", 100, 10, 3)
            )
            for message in messages:
                connection.execute(
                    insert(outbox).values(
                        event_id=message.envelope.event_id,
                        topic=message.topic,
                        message_key=message.key,
                        envelope=message.envelope.to_json(),
                    )
                )
    command = next(m.envelope for m in messages if m.topic == "wallet.commands")

    # Wallet: apply the deduction, as its worker would.
    with log_context(service="wallet"):
        result = consume(
            urls["wallet"],
            "wallet-saga",
            WalletSagaHandler(PostgresWalletRepository(engines["wallet"])),
            command,
        )
    with engines["wallet"].connect() as connection:
        reply = EventEnvelope.model_validate(
            connection.scalar(
                select(outbox.c.envelope).where(
                    outbox.c.event_id == UUID(str(result.result_event_ids[0]))
                )
            )
        )

    # Coordinator: apply the Wallet's result.
    with log_context(service="coordinator"):
        consume(urls["coordinator"], "coordinator-saga", CoordinatorSagaResultHandler(machine), reply)

    # Every line has the shared field names.
    for line in captured:
        assert set(BASE_FIELDS + CORRELATION_FIELDS) <= set(line)

    # Filtering by one transaction_id gives the whole story, from both services.
    trail = [line for line in captured if line["transaction_id"] == str(created.transaction_id)]
    assert {line["service"] for line in trail} == {"coordinator", "wallet"}
    assert {line["correlation_id"] for line in trail} == {str(created.correlation_id)}

    coordinator_states = [
        line["state"] for line in trail if line["service"] == "coordinator" and line["state"]
    ]
    assert coordinator_states == [
        "REQUESTED",
        "DEDUCTING_POINTS",
        "POINTS_DEDUCTED",
        "RESERVING_INVENTORY",
    ]
    wallet_lines = [line for line in trail if line["service"] == "wallet"]
    assert any(
        line["message"] == "wallet.deduct_points answered with wallet.points_deducted"
        and line["event_id"] == str(command.event_id)
        for line in wallet_lines
    )
    # The order across services is the order things happened.
    services_in_order = [line["service"] for line in trail]
    first_wallet = services_in_order.index("wallet")
    assert "coordinator" in services_in_order[:first_wallet]
    assert "coordinator" in services_in_order[first_wallet:]
