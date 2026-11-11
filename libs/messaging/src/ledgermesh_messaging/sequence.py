"""Per-aggregate version ordering for consumed events (LM-026).

Partition ordering helps but is not enough (ADR-001 section 8): a retry, a
rebalance or a replay can still deliver one aggregate's events out of order.
Each consumer stores the latest version it applied for every aggregate and
compares each event's ``aggregate_version`` against it:

- at or below the latest: stale, ignored without running the handler;
- exactly the next version: applied, and becomes the latest;
- further ahead: a future event, retried until its predecessor is applied.

Only events emitted by the aggregate's owner carry a version. Commands have
``aggregate_version = None`` and are ordered by workflow step instead.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import StrEnum

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from .consumer import PermanentError, TransientError
from .envelope import EventEnvelope
from .inbox import Handler
from .schema import aggregate_versions

logger = logging.getLogger(__name__)


class SequenceDecision(StrEnum):
    APPLY = "apply"
    STALE = "stale"
    FUTURE = "future"


@dataclass(frozen=True)
class SequenceCheck:
    decision: SequenceDecision
    # The latest applied version before this event was checked.
    latest: int


class MissingVersionError(PermanentError, ValueError):
    """The event has no aggregate_version, so it cannot be ordered.

    Permanent, so ``InboxConsumer`` dead-letters it instead of stopping.
    """


class FutureEventError(TransientError):
    """The event's predecessor has not been applied yet, so it must wait."""

    def __init__(self, envelope: EventEnvelope, expected: int) -> None:
        super().__init__(
            f"{envelope.aggregate_type}/{envelope.aggregate_id} version "
            f"{envelope.aggregate_version} arrived before version {expected}"
        )
        self.expected = expected


async def check_and_advance(
    session: AsyncSession, consumer: str, envelope: EventEnvelope, *, first_version: int = 1
) -> SequenceCheck:
    """Decide whether ``envelope`` is next for its aggregate, and record it if so.

    Must run inside the caller's transaction. The aggregate's row is locked
    until that transaction ends, so two events for one aggregate never both
    pass the check.
    """
    version = envelope.aggregate_version
    if version is None:
        raise MissingVersionError(f"{envelope.event_type} has no aggregate_version to order by")

    key = (
        aggregate_versions.c.consumer == consumer,
        aggregate_versions.c.aggregate_type == envelope.aggregate_type,
        aggregate_versions.c.aggregate_id == envelope.aggregate_id,
    )
    # The first event for an aggregate creates its row. A concurrent insert
    # waits for the other transaction, then finds the row and locks it.
    await session.execute(
        pg_insert(aggregate_versions)
        .values(
            consumer=consumer,
            aggregate_type=envelope.aggregate_type,
            aggregate_id=envelope.aggregate_id,
            version=first_version - 1,
        )
        .on_conflict_do_nothing()
    )
    latest = await session.scalar(
        select(aggregate_versions.c.version).where(*key).with_for_update()
    )

    if version <= latest:
        return SequenceCheck(SequenceDecision.STALE, latest)
    if version > latest + 1:
        return SequenceCheck(SequenceDecision.FUTURE, latest)

    await session.execute(
        update(aggregate_versions)
        .where(*key)
        .values(version=version, event_id=envelope.event_id, updated_at=func.now())
    )
    return SequenceCheck(SequenceDecision.APPLY, latest)


def in_version_order(handler: Handler, *, consumer: str, first_version: int = 1) -> Handler:
    """Wrap ``handler`` so it only sees each aggregate's events in version order.

    A stale event returns no messages, so ``process_once`` still records it in
    the inbox and a redelivery is a plain duplicate. A future event raises
    ``FutureEventError``, which rolls the transaction back and lets
    ``InboxConsumer`` retry it; once retries run out it goes to the dead-letter
    hook (LM-024).
    """

    async def ordered(session: AsyncSession, envelope: EventEnvelope):
        check = await check_and_advance(session, consumer, envelope, first_version=first_version)
        if check.decision is SequenceDecision.STALE:
            logger.info(
                "ignoring stale event %s (transaction %s): %s/%s version %s",
                envelope.event_id,
                envelope.transaction_id,
                envelope.aggregate_type,
                envelope.aggregate_id,
                envelope.aggregate_version,
            )
            return []
        if check.decision is SequenceDecision.FUTURE:
            raise FutureEventError(envelope, expected=check.latest + 1)
        return await handler(session, envelope)

    return ordered
