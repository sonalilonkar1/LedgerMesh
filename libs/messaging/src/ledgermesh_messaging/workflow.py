"""Workflow step and causation checks for consumed messages (LM-026).

ADR-001 section 8: command consumers validate ``transaction_id``,
``workflow_step`` and ``causation_id`` before applying a mutation, and never
use a Coordinator-supplied aggregate version. The two sides of a Saga need
different rules, so there are two wrappers.

**Participants** (Wallet, Catalog) use ``in_workflow_order``. A participant
sees only its own steps of a transaction, so gaps are normal, and a
compensation can legitimately arrive before the forward command it cancels
(ADR-001 section 7.1). Each participant therefore stores the highest step it
has applied per transaction and:

- applies any higher step, gaps included;
- treats a step at or below it as stale: a late forward command after its
  compensation, or the same command resent with a new ``event_id``. Stale
  messages go to ``on_stale``, so the service can return its recorded or
  cancelled result, and are never applied.

**The Coordinator** uses ``matching_workflow_step``. It knows from its own
state which step each transaction is waiting for and which command it sent
for it, so it checks a result exactly:

- an earlier step, or a result caused by a different command, is stale and
  ignored;
- a later step is a future message, retried until the transaction reaches it
  and dead-lettered after bounded retries (ADR-001 section 7);
- a transaction that is finished or unknown ignores the result (the
  terminal-state rule in ADR-001 section 10).

Both reject a non-root message with no ``causation_id``.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from enum import StrEnum
from uuid import UUID

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from .consumer import PermanentError, TransientError
from .envelope import EventEnvelope
from .inbox import Handler
from .outbox import OutgoingMessage
from .schema import workflow_steps

logger = logging.getLogger(__name__)

FIRST_STEP = 1


class StepDecision(StrEnum):
    APPLY = "apply"
    STALE = "stale"
    FUTURE = "future"
    WRONG_CAUSE = "wrong_cause"
    FINISHED = "finished"


@dataclass(frozen=True)
class Expectation:
    """The step a Coordinator transaction is waiting for, and the command it sent."""

    step: int
    causation_id: UUID


ExpectationLookup = Callable[[AsyncSession, EventEnvelope], Awaitable[Expectation | None]]
StaleHandler = Callable[[AsyncSession, EventEnvelope], Awaitable[Sequence[OutgoingMessage]]]


class FutureStepError(TransientError):
    """The transaction has not reached this message's step yet, so it must wait."""

    def __init__(self, envelope: EventEnvelope, expected: int) -> None:
        super().__init__(
            f"transaction {envelope.transaction_id} step {envelope.workflow_step} "
            f"arrived while waiting for step {expected}"
        )
        self.expected = expected


class MissingWorkflowStepError(PermanentError, ValueError):
    """The message has no workflow_step, so it cannot be ordered within its transaction."""


class MissingCausationError(PermanentError):
    """Only the root message of a transaction may have no causation_id."""

    def __init__(self, envelope: EventEnvelope) -> None:
        super().__init__(
            f"{envelope.event_type} {envelope.event_id} is at step {envelope.workflow_step} "
            "but has no causation_id"
        )


def _validate(envelope: EventEnvelope, first_step: int) -> int:
    step = envelope.workflow_step
    if step is None:
        raise MissingWorkflowStepError(f"{envelope.event_type} has no workflow_step to order by")
    if envelope.causation_id is None and step > first_step:
        raise MissingCausationError(envelope)
    return step


async def check_participant_step(
    session: AsyncSession, consumer: str, envelope: EventEnvelope, *, first_step: int = FIRST_STEP
) -> StepDecision:
    """Record ``envelope``'s step if it is newer than any applied for its transaction.

    Must run inside the caller's transaction. The transaction's row stays
    locked until it ends, so two messages for one transaction never both pass.
    """
    step = _validate(envelope, first_step)
    key = (
        workflow_steps.c.consumer == consumer,
        workflow_steps.c.transaction_id == envelope.transaction_id,
    )
    # The first message for a transaction creates its row. A concurrent insert
    # waits for the other transaction, then finds the row and locks it.
    await session.execute(
        pg_insert(workflow_steps)
        .values(consumer=consumer, transaction_id=envelope.transaction_id, step=first_step - 1)
        .on_conflict_do_nothing()
    )
    highest = await session.scalar(select(workflow_steps.c.step).where(*key).with_for_update())
    if step <= highest:
        return StepDecision.STALE

    await session.execute(
        update(workflow_steps)
        .where(*key)
        .values(step=step, event_id=envelope.event_id, updated_at=func.now())
    )
    return StepDecision.APPLY


def check_expected_step(
    envelope: EventEnvelope, expected: Expectation | None, *, first_step: int = FIRST_STEP
) -> StepDecision:
    """Compare a result with the step and command its transaction is waiting for."""
    step = _validate(envelope, first_step)
    if expected is None:
        return StepDecision.FINISHED
    if step < expected.step:
        return StepDecision.STALE
    if step > expected.step:
        return StepDecision.FUTURE
    if envelope.causation_id != expected.causation_id:
        return StepDecision.WRONG_CAUSE
    return StepDecision.APPLY


def _log_ignored(envelope: EventEnvelope, decision: StepDecision) -> None:
    logger.info(
        "ignoring %s message %s (%s, transaction %s, step %s, causation %s)",
        decision,
        envelope.event_id,
        envelope.event_type,
        envelope.transaction_id,
        envelope.workflow_step,
        envelope.causation_id,
    )


async def _no_messages(session: AsyncSession, envelope: EventEnvelope) -> list[OutgoingMessage]:
    return []


def in_workflow_order(
    handler: Handler,
    *,
    consumer: str,
    on_stale: StaleHandler = _no_messages,
    first_step: int = FIRST_STEP,
) -> Handler:
    """Wrap a participant's ``handler`` so no workflow step is applied twice or late.

    ``on_stale`` runs instead of ``handler`` for a stale message. Its messages
    are published and its writes commit with the inbox row, so a service can
    use it to return the result it recorded earlier or the cancelled result of
    a compensation tombstone. By default a stale message is only logged and
    recorded in the inbox.
    """

    async def ordered(session: AsyncSession, envelope: EventEnvelope):
        decision = await check_participant_step(
            session, consumer, envelope, first_step=first_step
        )
        if decision is StepDecision.STALE:
            _log_ignored(envelope, decision)
            return await on_stale(session, envelope)
        return await handler(session, envelope)

    return ordered


def matching_workflow_step(
    handler: Handler, *, expectation: ExpectationLookup, first_step: int = FIRST_STEP
) -> Handler:
    """Wrap the Coordinator's result ``handler`` so only the awaited result is applied.

    ``expectation`` reads the transaction's current step and the ``event_id``
    of the command sent for it from the Coordinator's own state, inside the
    same database transaction, or returns ``None`` when the transaction is
    finished or unknown. It should lock that state row so a concurrent result
    cannot pass the same check.
    """

    async def ordered(session: AsyncSession, envelope: EventEnvelope):
        expected = await expectation(session, envelope)
        decision = check_expected_step(envelope, expected, first_step=first_step)
        if decision is StepDecision.FUTURE:
            raise FutureStepError(envelope, expected=expected.step)
        if decision is not StepDecision.APPLY:
            _log_ignored(envelope, decision)
            return []
        return await handler(session, envelope)

    return ordered
