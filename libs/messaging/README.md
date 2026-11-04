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

## Kafka topics (LM-018)

All topics are declared in `topics.py`. Services import them instead of typing names, and build messages with `spec.message(envelope)` so the key is always right:

```python
from ledgermesh_messaging import WALLET_COMMANDS, enqueue

await enqueue(session, WALLET_COMMANDS.message(deduct_points_command))
```

| Topic | Key | Producer | Consumer group |
|---|---|---|---|
| `wallet.commands` | user ID (`aggregate_id` of a `wallet` envelope) | Coordinator | `wallet-saga` |
| `wallet.results` | `transaction_id` | Wallet | `coordinator-saga` |
| `catalog.commands` | reward ID (`aggregate_id` of a `reward` envelope) | Coordinator | `catalog-saga` |
| `catalog.results` | `transaction_id` | Catalog | `coordinator-saga` |
| `redemption.events` | `transaction_id` | Coordinator | `audit-ledger` |

- Commands are keyed by the wallet or reward they change, so every command for one user (or one reward) lands on the same partition and is applied in order. Sending a `reward` envelope to `wallet.commands` raises `TopicKeyError`.
- Results and events are keyed by `transaction_id`, so the Coordinator sees each transaction's results in order.
- Every topic above also has `<topic>.retry`, with the same key and partition count and consumed by `<group>.retry`, and `<topic>.dlq`, with 1 partition and no consumer group. LM-024 ([#26](https://github.com/sonalilonkar1/LedgerMesh/issues/26)) adds the retry and dead-letter logic that uses them.
- Main and retry topics: 6 partitions, 7 days retention. Dead-letter topics: 30 days retention. All topics use `cleanup.policy=delete` and `min.insync.replicas` of 2, or 1 when the replication factor is 1.

### Creating the topics

```bash
uv run ledgermesh-topics --bootstrap-servers localhost:9092 --replication-factor 1
```

The command is safe to run any number of times. It creates missing topics, re-applies retention and replica settings to existing ones, and prints `created` or `ok` for each topic. If an existing topic has a different partition count, it stops with `TopicMismatchError` instead of adding partitions, because adding partitions would move keys to other partitions and break per-key ordering for messages already in flight.

Run it before starting any service, and disable `auto.create.topics.enable` on the broker, so that a typo in a topic name fails instead of silently creating a 1-partition topic.

## Ordering guarantees and limits

What the publisher **does** guarantee:

- Only one publisher per service sends at a time, and a failed send stops the batch, so it never reorders the rows it can see.
- Messages written in the same transaction, or by transactions that commit one after another, are published in that order.
- Kafka keeps that order per partition, because all messages with the same key go to the same partition and the producer is idempotent.

What it does **not** guarantee:

- **Causal order across concurrent transactions.** Outbox ids are assigned when a row is inserted, not when it commits. If transaction A gets id 10 and transaction B gets id 11, and B commits first, id 11 is published before id 10.
- **Order across different keys or topics.** Kafka only orders messages within one partition.

Strict per-aggregate and per-workflow ordering is therefore enforced by the receiver, using `aggregate_version`, `workflow_step` and `causation_id` (LM-026, [#28](https://github.com/sonalilonkar1/LedgerMesh/issues/28)).

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

Check that the generated migration creates both tables and the partial index `ix_outbox_unpublished`.

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
| Wallet commands are keyed by user ID, Catalog commands by reward ID, results by transaction | `test_wallet_commands_are_keyed_by_user_id`, `test_catalog_commands_are_keyed_by_reward_id`, `test_results_and_events_are_keyed_by_transaction_id` |
| Each consumer group owns a documented set of topics | `test_consumer_group_ownership` |
| Topic bootstrap is repeatable and fixes setting drift | `test_second_run_is_a_no_op`, `test_rerun_restores_retention_drift` |
| A topic with the wrong partition count is reported, not changed | `test_existing_topic_with_wrong_partition_count_is_reported_not_changed` |

## Running the tests

Requires Docker and [uv](https://docs.astral.sh/uv/).

```bash
cd libs/messaging
docker compose -f docker-compose.test.yml up -d --wait
uv sync --python 3.12
uv run pytest
docker compose -f docker-compose.test.yml down -v
```

`uv run pytest -m "not integration"` runs only the envelope and topic catalog tests and needs no Docker.

GitHub Actions runs the full suite, including the PostgreSQL and Kafka integration tests, on every pull request that changes `libs/messaging` (`.github/workflows/messaging.yml`).

## Not yet included

- `EventEnvelope` is provisional until the shared contracts from LM-004 exist.
- Dead-letter topics and retry topics come with LM-024. Until then `on_dead_letter` defaults to re-raising, which stops the consumer instead of skipping a message.
- Ordering and causation checks (`aggregate_version`, `workflow_step`) come with LM-026.
- Trace propagation: `headers` are carried through, and LM-033 will fill in `traceparent`.
