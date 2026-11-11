# ledgermesh-messaging

Transactional inbox and outbox shared by the LedgerMesh services ([LM-019](https://github.com/sonalilonkar1/LedgerMesh/issues/21)).

Kafka delivers messages **at least once**. This library turns that into **exactly-once business effects**:

- **Outbox:** a service writes its outgoing events into an `outbox` table in the same database transaction as its business change. `OutboxPublisher` relays those rows to Kafka afterwards. Nothing is lost if the service crashes after committing, and nothing is published for a change that rolled back.
- **Inbox:** `process_once` records each consumed `event_id` in an `inbox` table in the same transaction as the handler's business change. A redelivered or concurrently delivered duplicate hits the primary key and is reported as a duplicate instead of being applied again.

## Usage

```python
from ledgermesh_messaging import InboxConsumer, OutgoingMessage, create_consumer

async def handle_deduct(session, envelope):
    # Business writes use the same session, so they commit with the inbox row.
    await deduct_points(session, envelope.aggregate_id, envelope.payload["points"])
    return [OutgoingMessage(topic="wallet.results", key=str(envelope.transaction_id), envelope=reply)]

consumer = InboxConsumer(
    create_consumer(bootstrap, "wallet-service", "wallet.commands"),
    session_factory,
    name="wallet-saga",
    handler=handle_deduct,
)
```

Run one `OutboxPublisher(session_factory, create_producer(bootstrap), name="wallet")` per service. An advisory lock keeps a second replica idle.

## Ordering guarantees and limits

What the publisher **does** guarantee:

- Only one publisher per service sends at a time, and a failed send stops the batch, so it never reorders the rows it can see.
- Messages written in the same transaction, or by transactions that commit one after another, are published in that order.
- Kafka keeps that order per partition, because all messages with the same key go to the same partition and the producer is idempotent.

What it does **not** guarantee:

- **Causal order across concurrent transactions.** Outbox ids are assigned when a row is inserted, not when it commits. If transaction A gets id 10 and transaction B gets id 11, and B commits first, id 11 is published before id 10.
- **Order across different keys or topics.** Kafka only orders messages within one partition.

Strict per-aggregate and per-workflow ordering is therefore enforced by the receiver, using `aggregate_version`, `workflow_step` and `causation_id` (LM-026, [#28](https://github.com/sonalilonkar1/LedgerMesh/issues/28)), as described in the next section.

## Out-of-order messages (LM-026)

Three wrappers check a message before its handler runs. Each wraps an ordinary handler and runs inside the same database transaction as the inbox row, so the check, the business change and the replies commit together.

| Wrapper | Used by | Orders by |
|---|---|---|
| `in_version_order` | consumers of owner-emitted events, such as the Audit Ledger | `aggregate_version` per aggregate |
| `in_workflow_order` | participants: Wallet and Catalog command consumers | `workflow_step` per transaction |
| `matching_workflow_step` | the Coordinator's result consumer | the step and command each transaction is waiting for |

Per ADR-001, commands carry `aggregate_version: null` and are never ordered by version. Only events from the aggregate's owner are.

Import `in_version_order` from `ledgermesh_messaging.sequence`, and the other two (with `Expectation`) from `ledgermesh_messaging.workflow`.

### Events: `in_version_order`

```python
handler = in_version_order(record_event, consumer="audit-ledger")
```

Each consumer stores the latest version it applied for each aggregate in `aggregate_versions`.

| Incoming version | Outcome |
|---|---|
| at or below the latest | **stale**: ignored, recorded in the inbox |
| the next version | **applied**, and becomes the latest |
| further ahead | **future**: raises `FutureEventError`, which is retried and dead-lettered after `max_attempts` |

An event with no version raises `MissingVersionError` and is dead-lettered.

### Participants: `in_workflow_order`

```python
handler = in_workflow_order(handle_wallet_command, consumer="wallet-saga", on_stale=recorded_result)
```

A participant sees only its own steps of a transaction, so gaps between steps are normal. A compensation can also arrive before the forward command it cancels (ADR-001 section 7.1), so a participant cannot wait for missing steps. Instead it stores the highest step it has applied per transaction in `workflow_steps`:

- A **higher step** is applied, gaps included.
- A step **at or below** it is **stale** and never applied. Examples: a `wallet.deduct_points` arriving after its `wallet.refund_points`, or the same command resent with a new `event_id`.

A stale command runs `on_stale` instead of the handler. Its writes and replies commit with the inbox row, so the service can return its recorded result or the deterministic cancelled result of a compensation tombstone. The tombstone itself is service logic (LM-020, LM-021). By default a stale command is only logged and recorded.

### Coordinator: `matching_workflow_step`

```python
async def waiting_for(session, envelope):
    saga = await load_saga_for_update(session, envelope.transaction_id)
    if saga is None or saga.is_terminal:
        return None
    return Expectation(step=saga.current_step, causation_id=saga.current_command_id)

handler = matching_workflow_step(apply_result, expectation=waiting_for)
```

The Coordinator knows from its own state which step each transaction is waiting for and which command it sent for it. The lookup reads that state inside the same transaction and should lock the row.

| Incoming result | Outcome |
|---|---|
| the awaited step, caused by the awaited command | **applied** |
| an earlier step | **stale**: ignored, recorded in the inbox |
| the awaited step, but caused by another command | **wrong cause**: ignored, for example a reply to an earlier attempt |
| a later step | **future**: raises `FutureStepError`, which is retried and then dead-lettered (ADR-001 section 7) |
| a finished or unknown transaction (lookup returns `None`) | **ignored** (terminal-state rule, ADR-001 section 10) |

### Causation and malformed messages

`causation_id` may be null only for the root message (step 1). A later message without one raises `MissingCausationError`. A message without `workflow_step` raises `MissingWorkflowStepError`. Both are `PermanentError`s, so `InboxConsumer` dead-letters them instead of retrying.

Ignored messages are logged with their `transaction_id`, `event_id`, step and causation, and recorded in the inbox. A redelivery is then a plain duplicate.

### Tests

| Guarantee | Test |
|---|---|
| Events apply in version order; stale ones are ignored and recorded | `test_in_order_events_are_applied_and_the_latest_version_is_stored`, `test_older_version_is_ignored_without_running_the_handler`, `test_ignored_stale_event_is_recorded_so_redelivery_is_a_duplicate` |
| A future event waits for its predecessor, then is dead-lettered after bounded retries | `test_future_version_succeeds_after_its_predecessor_arrives`, `test_consumer_retries_a_future_event_then_dead_letters_it` |
| Concurrent events with one version apply once | `test_concurrent_events_with_the_same_version_apply_once` |
| A late forward command after its compensation is not applied, and `on_stale` can reply | `test_late_forward_command_after_its_compensation_is_not_applied`, `test_stale_command_runs_on_stale_and_publishes_its_reply` |
| A command resent with a new `event_id`, or delivered concurrently, applies once | `test_same_step_resent_with_a_new_event_id_is_not_applied_twice`, `test_concurrent_commands_for_one_step_apply_once` |
| The Coordinator applies only the awaited result | `test_coordinator_applies_the_awaited_result`, `test_coordinator_ignores_a_result_for_an_earlier_step`, `test_coordinator_ignores_a_result_caused_by_a_different_command` |
| A future result waits for its step, then is dead-lettered | `test_coordinator_defers_a_future_result_until_its_step`, `test_coordinator_dead_letters_a_result_that_never_becomes_current` |
| Results for finished transactions are ignored | `test_coordinator_ignores_results_for_finished_transactions` |
| Only the root message may lack `causation_id`; malformed messages are dead-lettered | `test_later_command_without_causation_is_rejected`, `test_message_without_a_workflow_step_is_dead_lettered`, `test_consumer_dead_letters_an_event_without_a_version` |

## Duplicate replay is opt-in

A duplicate always returns the stored result event ids, and its business change is never applied twice.

`process_once(..., republish_on_duplicate=True)` additionally queues the stored replies for publication again. Enable it only for a consumer whose sender retries with the **same** event id and needs the original reply, for example a participant answering a Coordinator that timed out. It is off by default because every replayed reply is itself a duplicate for its receiver: with replay enabled throughout a workflow, each duplicate re-sends the entire downstream chain. `test_duplicate_storm_across_services_terminates` shows that a workflow reaches a quiet state with exactly one effect per step in both modes.

## Failure backoff

When Kafka is unavailable, `publish_batch()` returns `BatchResult(failed=True)` and `run()` doubles its delay after each failed batch, from `poll_interval` up to `max_backoff`. The first successful batch resets it to `poll_interval`.

## Creating the tables (Alembic)

Adding the metadata only tells Alembic what the `inbox` and `outbox` tables should look like. Each service still has to generate and apply a migration:

```python
# services/<service>/migrations/env.py
from ledgermesh_messaging import metadata as messaging_metadata
from app.models import metadata as service_metadata

target_metadata = [service_metadata, messaging_metadata]
```

```bash
alembic revision --autogenerate -m "Add inbox and outbox tables"
alembic upgrade head
```

Check that the generated migration creates both tables and the partial index `ix_outbox_unpublished`. The metadata also contains the LM-026 tables `aggregate_versions` and `workflow_steps`. They are created by the same migration, and are only written by consumers that use the ordering wrappers.

This PR adds no service migrations because no services exist yet. Each service adds its migration when it starts using the library, using the migration setup from LM-005 ([#7](https://github.com/sonalilonkar1/LedgerMesh/issues/7)): Wallet in LM-020 ([#22](https://github.com/sonalilonkar1/LedgerMesh/issues/22)), Catalog in LM-021 ([#23](https://github.com/sonalilonkar1/LedgerMesh/issues/23)), and the Coordinator with LM-022 ([#24](https://github.com/sonalilonkar1/LedgerMesh/issues/24)).

## Guarantees and their tests

| Guarantee | Test |
|---|---|
| Business change, inbox row and outbox rows commit or roll back together | `test_business_update_inbox_and_outbox_commit_together`, `test_handler_failure_rolls_back_everything` |
| 25 concurrent deliveries of one event apply once | `test_concurrent_duplicates_apply_exactly_once` |
| A duplicate returns the stored outcome without re-queueing it | `test_duplicate_returns_stored_outcome_without_requeueing` |
| Opt-in replay re-queues the stored reply | `test_duplicate_with_replay_requeues_stored_reply` |
| A duplicate storm across three services terminates with one effect per step | `test_duplicate_storm_across_services_terminates` |
| Visible rows publish in id order; a send failure stops the batch | `test_publishes_in_insertion_order_and_marks_rows`, `test_kafka_failure_stops_batch_to_preserve_order` |
| Consecutive Kafka failures back off up to `max_backoff`, then reset | `test_consecutive_kafka_failures_back_off_up_to_the_limit`, `test_backoff_resets_after_successful_batch` |
| Crash after a Kafka send but before marking the row re-sends the same `event_id` | `test_crash_after_send_redelivers_same_event` |
| Only one publisher per service is active | `test_only_one_publisher_runs_at_a_time` |
| Crash after the database commit but before the offset commit is deduplicated on redelivery | `test_redelivery_after_crash_before_offset_commit_is_deduplicated` |
| Per-key order survives the full outbox → Kafka → inbox path | `test_outbox_to_inbox_preserves_per_key_order` |

## Running the tests

Requires Docker and [uv](https://docs.astral.sh/uv/).

```bash
cd libs/messaging
docker compose -f docker-compose.test.yml up -d --wait
uv sync --python 3.12
uv run pytest
docker compose -f docker-compose.test.yml down -v
```

`uv run pytest -m "not integration"` runs only the envelope tests and needs no Docker.

GitHub Actions runs the full suite, including the PostgreSQL and Kafka integration tests, on every pull request that changes `libs/messaging` (`.github/workflows/messaging.yml`).

## Not yet included

- `EventEnvelope` is provisional until the shared contracts from LM-004 exist.
- Dead-letter topics and retry topics come with LM-024. Until then `on_dead_letter` defaults to re-raising, which stops the consumer instead of skipping a message.
- Trace propagation: `headers` are carried through, and LM-033 will fill in `traceparent`.
