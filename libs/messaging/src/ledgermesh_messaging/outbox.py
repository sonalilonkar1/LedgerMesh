"""Writing outgoing messages in the same transaction as the business update."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncSession

from .envelope import EventEnvelope
from .schema import outbox
from .tracing import with_trace_headers


@dataclass(frozen=True)
class OutgoingMessage:
    topic: str
    key: str
    envelope: EventEnvelope
    headers: Mapping[str, str] = field(default_factory=dict)


async def enqueue(session: AsyncSession, message: OutgoingMessage) -> None:
    """Stage a message for publication.

    Must run inside the caller's open transaction so the message commits or
    rolls back together with the business change that produced it. The
    current trace context is added to the headers (LM-033), so the consumer's
    span joins the producer's trace; headers the caller set explicitly win.
    """
    await session.execute(
        insert(outbox).values(
            event_id=message.envelope.event_id,
            topic=message.topic,
            message_key=message.key,
            envelope=message.envelope.to_json(),
            headers=with_trace_headers(message.headers),
        )
    )
