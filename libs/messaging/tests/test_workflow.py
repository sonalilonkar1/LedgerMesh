import asyncio
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import func, select

from ledgermesh_messaging import InboxConsumer, OutgoingMessage, inbox, process_once
from ledgermesh_messaging.schema import workflow_steps
from ledgermesh_messaging.workflow import (
    Expectation,
    FutureStepError,
    MissingCausationError,
    MissingWorkflowStepError,
    in_workflow_order,
    matching_workflow_step,
)

from .conftest import increment_and_reply, make_envelope, read_counter

pytestmark = pytest.mark.integration

WALLET = "wallet-saga"
COORDINATOR = "coordinator-saga"


def command(transaction_id, step, *, causation_id="auto", event_type="wallet.deduct_points"):
    if causation_id == "auto":
        causation_id = None if step == 1 else uuid4()
    return make_envelope(
        event_type=event_type,
        transaction_id=transaction_id,
        workflow_step=step,
        causation_id=causation_id,
    )


async def deliver(session_factory, envelope, handler, consumer=WALLET):
    return await process_once(session_factory, consumer, envelope, handler)


async def highest_step(session_factory, transaction_id, consumer=WALLET):
    async with session_factory() as session:
        return await session.scalar(
            select(workflow_steps.c.step).where(
                workflow_steps.c.consumer == consumer,
                workflow_steps.c.transaction_id == transaction_id,
            )
        )


async def inbox_rows(session_factory):
    async with session_factory() as session:
        return await session.scalar(select(func.count()).select_from(inbox))


participant = in_workflow_order(increment_and_reply, consumer=WALLET)


# Participants: Wallet and Catalog


async def test_participant_applies_steps_in_order(session_factory):
    transaction_id = uuid4()

    await deliver(session_factory, command(transaction_id, 1), participant)
    await deliver(session_factory, command(transaction_id, 3), participant)

    assert await read_counter(session_factory) == 2
    assert await highest_step(session_factory, transaction_id) == 3


async def test_participant_accepts_gaps_between_its_own_steps(session_factory):
    # Wallet never sees Catalog's steps, so step 4 can follow step 1 directly.
    transaction_id = uuid4()

    await deliver(session_factory, command(transaction_id, 1), participant)
    await deliver(session_factory, command(transaction_id, 4), participant)

    assert await read_counter(session_factory) == 2


async def test_late_forward_command_after_its_compensation_is_not_applied(session_factory):
    # ADR-001 7.1: RefundPoints (step 4) arrives before DeductPoints (step 1).
    transaction_id = uuid4()
    refund = command(transaction_id, 4, event_type="wallet.refund_points")
    late_deduct = command(transaction_id, 1)

    await deliver(session_factory, refund, participant)
    result = await deliver(session_factory, late_deduct, participant)

    assert not result.duplicate
    assert result.result_event_ids == ()
    assert await read_counter(session_factory) == 1
    assert await highest_step(session_factory, transaction_id) == 4


async def test_stale_command_runs_on_stale_and_publishes_its_reply(session_factory):
    transaction_id = uuid4()
    seen = []

    async def cancelled(session, envelope):
        seen.append(envelope.event_id)
        reply = make_envelope(
            event_type="wallet.deduction_cancelled",
            transaction_id=envelope.transaction_id,
            causation_id=envelope.event_id,
        )
        return [OutgoingMessage(topic="wallet.results", key=str(transaction_id), envelope=reply)]

    with_reply = in_workflow_order(increment_and_reply, consumer=WALLET, on_stale=cancelled)
    late_deduct = command(transaction_id, 1)

    await deliver(session_factory, command(transaction_id, 4), with_reply)
    result = await deliver(session_factory, late_deduct, with_reply)

    assert seen == [late_deduct.event_id]
    assert len(result.result_event_ids) == 1
    assert await read_counter(session_factory) == 1


async def test_same_step_resent_with_a_new_event_id_is_not_applied_twice(session_factory):
    transaction_id = uuid4()

    await deliver(session_factory, command(transaction_id, 1), participant)
    await deliver(session_factory, command(transaction_id, 1), participant)

    assert await read_counter(session_factory) == 1
    assert await inbox_rows(session_factory) == 2


async def test_participant_tracks_each_transaction_separately(session_factory):
    first, second = uuid4(), uuid4()

    await deliver(session_factory, command(first, 4), participant)
    await deliver(session_factory, command(second, 1), participant)

    assert await read_counter(session_factory) == 2


async def test_concurrent_commands_for_one_step_apply_once(session_factory):
    transaction_id = uuid4()
    commands = [command(transaction_id, 1) for _ in range(10)]

    await asyncio.gather(*(deliver(session_factory, c, participant) for c in commands))

    assert await read_counter(session_factory) == 1
    assert await inbox_rows(session_factory) == 10


async def test_root_command_may_have_no_causation(session_factory):
    await deliver(session_factory, command(uuid4(), 1, causation_id=None), participant)

    assert await read_counter(session_factory) == 1


async def test_later_command_without_causation_is_rejected(session_factory):
    with pytest.raises(MissingCausationError):
        await deliver(session_factory, command(uuid4(), 2, causation_id=None), participant)

    assert await read_counter(session_factory) == 0
    assert await inbox_rows(session_factory) == 0


async def test_message_without_a_workflow_step_is_dead_lettered(session_factory):
    dead_letters = []

    async def capture(record, exc):
        dead_letters.append(exc)

    consumer = InboxConsumer(
        None, session_factory, name=WALLET, handler=participant, on_dead_letter=capture
    )

    result = await consumer.handle(SimpleNamespace(value=make_envelope().to_bytes()))

    assert result is None
    assert [type(exc) for exc in dead_letters] == [MissingWorkflowStepError]


# Coordinator: results are checked against the step it is waiting for


class Waiting:
    """Stand-in for the Coordinator's saga state: transaction -> Expectation."""

    def __init__(self):
        self.by_transaction = {}

    async def __call__(self, session, envelope):
        return self.by_transaction.get(envelope.transaction_id)


def result_for(transaction_id, step, causation_id):
    return make_envelope(
        event_type="wallet.points_deducted",
        transaction_id=transaction_id,
        workflow_step=step,
        causation_id=causation_id,
    )


async def deliver_result(session_factory, envelope, waiting):
    handler = matching_workflow_step(increment_and_reply, expectation=waiting)
    return await deliver(session_factory, envelope, handler, consumer=COORDINATOR)


async def test_coordinator_applies_the_awaited_result(session_factory):
    transaction_id, sent = uuid4(), uuid4()
    waiting = Waiting()
    waiting.by_transaction[transaction_id] = Expectation(step=1, causation_id=sent)

    await deliver_result(session_factory, result_for(transaction_id, 1, sent), waiting)

    assert await read_counter(session_factory) == 1


async def test_coordinator_ignores_a_result_for_an_earlier_step(session_factory):
    transaction_id = uuid4()
    waiting = Waiting()
    waiting.by_transaction[transaction_id] = Expectation(step=2, causation_id=uuid4())

    result = await deliver_result(session_factory, result_for(transaction_id, 1, uuid4()), waiting)

    assert result.result_event_ids == ()
    assert await read_counter(session_factory) == 0
    assert await inbox_rows(session_factory) == 1


async def test_coordinator_ignores_a_result_caused_by_a_different_command(session_factory):
    transaction_id = uuid4()
    waiting = Waiting()
    waiting.by_transaction[transaction_id] = Expectation(step=1, causation_id=uuid4())

    await deliver_result(session_factory, result_for(transaction_id, 1, uuid4()), waiting)

    assert await read_counter(session_factory) == 0


async def test_coordinator_defers_a_future_result_until_its_step(session_factory):
    transaction_id, step_one, step_two = uuid4(), uuid4(), uuid4()
    waiting = Waiting()
    waiting.by_transaction[transaction_id] = Expectation(step=1, causation_id=step_one)
    early = result_for(transaction_id, 2, step_two)

    with pytest.raises(FutureStepError) as raised:
        await deliver_result(session_factory, early, waiting)
    assert raised.value.expected == 1
    assert await inbox_rows(session_factory) == 0

    waiting.by_transaction[transaction_id] = Expectation(step=2, causation_id=step_two)
    result = await deliver_result(session_factory, early, waiting)

    assert not result.duplicate
    assert await read_counter(session_factory) == 1


async def test_coordinator_ignores_results_for_finished_transactions(session_factory):
    # Terminal-state rule: the lookup returns None once a transaction is finished.
    result = await deliver_result(session_factory, result_for(uuid4(), 1, uuid4()), Waiting())

    assert result.result_event_ids == ()
    assert await read_counter(session_factory) == 0
    assert await inbox_rows(session_factory) == 1


async def test_coordinator_dead_letters_a_result_that_never_becomes_current(session_factory):
    transaction_id = uuid4()
    waiting = Waiting()
    waiting.by_transaction[transaction_id] = Expectation(step=1, causation_id=uuid4())
    dead_letters = []

    async def capture(record, exc):
        dead_letters.append(exc)

    consumer = InboxConsumer(
        None,
        session_factory,
        name=COORDINATOR,
        handler=matching_workflow_step(increment_and_reply, expectation=waiting),
        on_dead_letter=capture,
        max_attempts=3,
        retry_backoff=0,
    )
    early = result_for(transaction_id, 3, uuid4())

    assert await consumer.handle(SimpleNamespace(value=early.to_bytes())) is None
    assert [type(exc) for exc in dead_letters] == [FutureStepError]
    assert await read_counter(session_factory) == 0
