"""Exactly-once business effects on top of at-least-once delivery."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from uuid import UUID

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from .envelope import EventEnvelope
from .outbox import OutgoingMessage, enqueue
from .schema import inbox, outbox

Handler = Callable[[AsyncSession, EventEnvelope], Awaitable[Sequence[OutgoingMessage]]]


@dataclass(frozen=True)
class ProcessResult:
    duplicate: bool
    result_event_ids: tuple[UUID, ...]


async def process_once(
    session_factory: async_sessionmaker[AsyncSession],
    consumer: str,
    envelope: EventEnvelope,
    handler: Handler,
    *,
    republish_on_duplicate: bool = False,
) -> ProcessResult:
    """Run ``handler`` for ``envelope`` at most once per ``consumer``.

    The inbox claim, the handler's business writes and its outgoing messages
    commit in one database transaction. If the handler raises, nothing is
    recorded and the message can be retried.

    Two concurrent deliveries of the same event serialize on the inbox primary
    key: the second insert waits for the first transaction, then conflicts and
    is reported as a duplicate.

    A duplicate always returns the stored result event ids. Re-queueing those
    events for publication is opt-in through ``republish_on_duplicate``: enable
    it only where a sender retries with the same event id and needs the
    original reply. Every replayed event is itself a duplicate for its
    receiver, so replay enabled throughout a workflow re-sends the whole
    downstream chain for each duplicate it sees.
    """
    async with session_factory() as session, session.begin():
        claimed = await session.scalar(
            pg_insert(inbox)
            .values(
                consumer=consumer,
                event_id=envelope.event_id,
                transaction_id=envelope.transaction_id,
                event_type=envelope.event_type,
            )
            .on_conflict_do_nothing(index_elements=[inbox.c.consumer, inbox.c.event_id])
            .returning(inbox.c.event_id)
        )

        if claimed is None:
            stored = await session.scalar(
                select(inbox.c.result_event_ids).where(
                    inbox.c.consumer == consumer, inbox.c.event_id == envelope.event_id
                )
            )
            result_ids = tuple(UUID(value) for value in stored or [])
            if republish_on_duplicate and result_ids:
                await session.execute(
                    update(outbox)
                    .where(outbox.c.event_id.in_(result_ids))
                    .values(published_at=None)
                )
            return ProcessResult(duplicate=True, result_event_ids=result_ids)

        messages = await handler(session, envelope)
        for message in messages:
            await enqueue(session, message)

        result_ids = tuple(message.envelope.event_id for message in messages)
        if result_ids:
            await session.execute(
                update(inbox)
                .where(inbox.c.consumer == consumer, inbox.c.event_id == envelope.event_id)
                .values(result_event_ids=[str(event_id) for event_id in result_ids])
            )
        return ProcessResult(duplicate=False, result_event_ids=result_ids)
