"""One Saga redemption is one OpenTelemetry trace across every service (LM-033).

Runs the real Coordinator, Wallet, Catalog and Audit Ledger code against their
own databases. A test "bus" delivers each outbox row as a Kafka record with the
row's headers, exactly as the publisher sends them, through ``InboxConsumer``.
The API request's context is stored on the redemption and continued by the
``SagaStarter``, so every span shares the request's trace id.
"""

import asyncio
from types import SimpleNamespace
from uuid import UUID

import pytest
from ledgermesh_messaging import EventEnvelope, InboxConsumer, configure_tracing, trace_engine
from ledgermesh_messaging.schema import outbox
from ledgermesh_messaging.tracing import COMPENSATION, MODE, STATE, TRANSACTION_ID, tracer
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from services.audit_ledger.app.event_worker import CONSUMER_GROUP as AUDIT_GROUP
from services.audit_ledger.app.event_worker import ordered_audit_handler
from services.audit_ledger.app.repository import PostgresAuditRepository
from services.catalog.app.db.schema import rewards
from services.catalog.app.repository import PostgresCatalogRepository
from services.catalog.app.saga import CatalogSagaHandler
from services.coordinator.app.db.schema import redemptions
from services.coordinator.app.models import CreateRedemptionRequest
from services.coordinator.app.repository import PostgresCoordinatorRepository
from services.coordinator.app.saga import (
    CoordinatorSagaMachine,
    CoordinatorSagaResultHandler,
    RewardQuote,
    SagaStarter,
)
from services.wallet.app.db.schema import wallets
from services.wallet.app.repository import PostgresWalletRepository
from services.wallet.app.saga import WalletSagaHandler

pytestmark = pytest.mark.database

EXPORTER = InMemorySpanExporter()
configure_tracing("trace-tests", exporter=EXPORTER)


@pytest.fixture(autouse=True)
def spans():
    EXPORTER.clear()
    yield EXPORTER
    EXPORTER.clear()


class FakeQuote:
    async def get_quote(self, reward_id: str) -> RewardQuote:
        return RewardQuote(reward_id, point_cost=100, available_inventory=10, version=0)


def run(url: str, work):
    async def go():
        # Traced like the workers' engines, so their SQL joins the trace.
        engine = trace_engine(create_async_engine(url))
        try:
            return await work(async_sessionmaker(engine, expire_on_commit=False))
        finally:
            await engine.dispose()

    return asyncio.run(go())


def published(engine, topic: str, transaction_id: UUID, seen: set) -> list:
    """Outbox rows not yet delivered, as Kafka records with the row's headers."""
    records = []
    with engine.connect() as connection:
        rows = connection.execute(
            select(outbox.c.id, outbox.c.envelope, outbox.c.headers)
            .where(outbox.c.topic == topic)
            .order_by(outbox.c.id)
        ).all()
    for row_id, document, headers in rows:
        envelope = EventEnvelope.model_validate(document, context={"wire": True})
        if envelope.transaction_id != transaction_id or (topic, row_id) in seen:
            continue
        seen.add((topic, row_id))
        records.append(
            SimpleNamespace(
                topic=topic,
                partition=0,
                offset=row_id,
                key=b"k",
                value=envelope.to_bytes(),
                headers=[(name, value.encode()) for name, value in (headers or {}).items()],
            )
        )
    return records


class Bus:
    """Delivers each service's outbox to the next consumer, like Kafka would."""

    def __init__(self, databases, transaction_id: UUID) -> None:
        engines, urls = databases.engines, databases.urls
        self.transaction_id = transaction_id
        self.seen: set = set()
        machine = CoordinatorSagaMachine()
        self.routes = {
            "wallet.commands": (
                engines["coordinator"],
                urls["wallet"],
                "wallet-saga",
                WalletSagaHandler(PostgresWalletRepository(engines["wallet"])),
            ),
            "catalog.commands": (
                engines["coordinator"],
                urls["catalog"],
                "catalog-saga",
                CatalogSagaHandler(PostgresCatalogRepository(engines["catalog"])),
            ),
            "wallet.results": (
                engines["wallet"],
                urls["coordinator"],
                "coordinator-saga",
                CoordinatorSagaResultHandler(machine),
            ),
            "catalog.results": (
                engines["catalog"],
                urls["coordinator"],
                "coordinator-saga",
                CoordinatorSagaResultHandler(machine),
            ),
            "redemption.events": (
                engines["coordinator"],
                urls["audit_ledger"],
                AUDIT_GROUP,
                ordered_audit_handler(PostgresAuditRepository(engines["audit_ledger"])),
            ),
        }

    def drain(self) -> None:
        while True:
            delivered = 0
            for topic, (source, url, name, handler) in self.routes.items():
                for record in published(source, topic, self.transaction_id, self.seen):

                    async def deliver(sessions, record=record, name=name, handler=handler):
                        return await InboxConsumer(
                            None, sessions, name=name, handler=handler
                        ).handle(record)

                    run(url, deliver)
                    delivered += 1
            if not delivered:
                return


def redeem(databases, *, available_points: int, inventory: int):
    engines = databases.engines
    with engines["wallet"].begin() as connection:
        connection.execute(
            wallets.insert().values(user_id="user-trace", available_points=available_points)
        )
    with engines["catalog"].begin() as connection:
        connection.execute(
            rewards.insert().values(
                reward_id="reward-trace",
                name="Trace reward",
                point_cost=100,
                total_inventory=inventory,
                available_inventory=inventory,
            )
        )

    # The API request: its trace context is stored with the redemption.
    with tracer().start_as_current_span("POST /v1/redemptions", kind=SpanKind.SERVER) as request:
        created = PostgresCoordinatorRepository(engines["coordinator"]).create(
            f"lm033-{inventory}",
            CreateRedemptionRequest(
                user_id="user-trace", reward_id="reward-trace", quantity=1, coordination_mode="saga"
            ),
        )

    # The worker, later and outside any span, picks it up from the database.
    started = run(
        databases.urls["coordinator"],
        lambda sessions: SagaStarter(sessions, FakeQuote(), CoordinatorSagaMachine()).run_once(),
    )
    assert started == 1
    Bus(databases, created.transaction_id).drain()

    with engines["coordinator"].connect() as connection:
        status = connection.scalar(
            select(redemptions.c.status).where(
                redemptions.c.transaction_id == created.transaction_id
            )
        )
    return created, request, status, EXPORTER.get_finished_spans()


def in_trace(spans, request) -> list:
    """The redemption's spans. The only others allowed are the Saga starter's
    polling queries, which run before it picks any redemption."""
    trace_id = request.get_span_context().trace_id
    outside = [span for span in spans if span.context.trace_id != trace_id]
    assert all(
        span.kind == SpanKind.CLIENT and span.parent is None and span.name.startswith("SELECT ")
        for span in outside
    ), [span.name for span in outside]
    return [span for span in spans if span.context.trace_id == trace_id]


def consumer_groups(spans) -> list[str]:
    return [
        span.attributes["messaging.consumer.group.name"]
        for span in spans
        if span.kind == SpanKind.CONSUMER
    ]


def states(spans) -> list[str]:
    return [
        event.attributes[STATE]
        for span in sorted(spans, key=lambda span: span.start_time)
        for event in span.events
        if event.name == "redemption.transition"
    ]


def test_one_saga_redemption_is_one_connected_trace(service_databases):
    created, request, status, spans = redeem(service_databases, available_points=500, inventory=5)

    assert status == "COMPLETED"
    spans = in_trace(spans, request)

    # Every span except the request has a parent, so the trace is one tree.
    ids = {span.context.span_id for span in spans}
    roots = [span.context.span_id for span in spans if span.parent is None]
    assert roots == [request.get_span_context().span_id]
    assert all(span.parent.span_id in ids for span in spans if span.parent)

    # It crosses every service, through Kafka-style consumer spans.
    groups = consumer_groups(spans)
    assert {"wallet-saga", "catalog-saga", "coordinator-saga", AUDIT_GROUP} <= set(groups)
    [start] = [span for span in spans if span.name == "saga start"]
    assert start.parent.span_id == request.get_span_context().span_id

    # Mode and state are on the spans, in the order the redemption moved.
    assert states(spans) == [
        "REQUESTED",
        "DEDUCTING_POINTS",
        "POINTS_DEDUCTED",
        "RESERVING_INVENTORY",
        "COMPLETED",
    ]
    marked = [span for span in spans if STATE in span.attributes]
    assert all(span.attributes[MODE] == "saga" for span in marked)
    assert all(span.attributes[TRANSACTION_ID] == str(created.transaction_id) for span in marked)
    assert not any(span.attributes.get(COMPENSATION) for span in spans)

    # Database work is in the trace too.
    sql = [span for span in spans if span.kind == SpanKind.CLIENT]
    assert any(span.name.startswith("INSERT ") for span in sql)
    assert all(span.parent.span_id in ids for span in sql)


def test_a_compensated_redemption_stays_one_trace_and_marks_compensation(service_databases):
    created, request, status, spans = redeem(service_databases, available_points=500, inventory=0)

    assert status == "COMPENSATED"
    spans = in_trace(spans, request)
    assert "COMPENSATING" in states(spans)
    assert states(spans)[-1] == "COMPENSATED"
    compensating = [span for span in spans if span.attributes.get(COMPENSATION)]
    assert compensating
    assert all(span.attributes[MODE] == "saga" for span in compensating)
