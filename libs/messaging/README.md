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

- Commands are keyed by the wallet or reward they change, so every command for one user (or one reward) lands on the same partition of that topic and is read in the order it was published there. Sending a `reward` envelope to `wallet.commands` raises `TopicKeyError`.
- Results are keyed by `transaction_id` for stable transaction affinity within each topic. Kafka does not provide ordering across `wallet.results` and `catalog.results`; the Coordinator validates workflow step and causation before applying a result.
- Every topic above also has `<topic>.retry`, with the same key and partition count and consumed by `<group>.retry`, and `<topic>.dlq`, with 1 partition and no consumer group. [Retries, timeouts and dead letters](#retries-timeouts-and-dead-letters-lm-024) describes how LM-024 uses them.
- Retry topics preserve the original message key and partition count, but they are independent Kafka topics and therefore do not preserve ordering relative to the main topic. LM-024 and LM-026 must handle delayed and out-of-order retries explicitly.

**Kafka orders messages only within one partition of one topic.** A key gives stable partition affinity and ordering only within each individual topic partition. There is no order:

- between `wallet.results` and `catalog.results`, even for the same transaction;
- between a main topic and its `.retry` topic: if message A moves to `wallet.commands.retry` while a later message B stays on `wallet.commands`, B can be processed before A;
- between a source topic and its `.dlq` topic.

Consumers therefore never rely on Kafka for order across topics. They use `transaction_id`, `workflow_step`, `causation_id`, inbox idempotency and the LM-026 causal-ordering rules ([#28](https://github.com/sonalilonkar1/LedgerMesh/issues/28)) to defer or reject messages that are not currently applicable.
- Main and retry topics: 6 partitions, 7 days retention. Dead-letter topics: 30 days retention. All topics use `cleanup.policy=delete` and `min.insync.replicas` of 2, or 1 when the replication factor is 1.

### Creating the topics

```bash
uv run ledgermesh-topics --bootstrap-servers localhost:29092 --replication-factor 1
```

`localhost:29092` is the Kafka port of the local stack (`make infra-up`) and is also the default, so `uv run ledgermesh-topics` alone works against it. Set `LM_KAFKA_BOOTSTRAP` or pass `--bootstrap-servers` for any other broker.

The command is safe to run any number of times. It creates missing topics, re-applies mutable topic configurations (`cleanup.policy`, `retention.ms` and `min.insync.replicas`) to existing ones, and prints `created` or `ok` for each topic. Re-applying replaces a topic's whole set of overrides, so a setting changed by hand that is not declared here is reset to the broker default.

It never changes an existing topic's partition count or replication factor. If either differs, it stops with `TopicMismatchError` before applying any configuration:

- Adding partitions would move keys to other partitions and break per-key ordering for messages already in flight.
- `min.insync.replicas` is derived from the replication factor. Applying the value for factor 3 to a topic that has 1 replica would make every `acks=all` producer fail. Changing a replication factor needs a partition reassignment, which this command does not do.

`--replication-factor` must be at least 1.

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

## Retries, timeouts and dead letters (LM-024)

Every service worker passes a `FailureRouter` to its `InboxConsumer`s. A failing message then moves through three tiers:

| Tier | When | What happens |
|---|---|---|
| In-process attempts | `TransientError`, a database `OperationalError`, or a handler running longer than `handler_timeout_seconds` | Retried `attempts` times with exponential `backoff_seconds`. A timed-out attempt is cancelled and its transaction rolled back |
| Retry topic | the in-process attempts ran out | The **original value and key** are republished to `<topic>.retry`, with `lm-retry-count`, `lm-not-before-ms`, `lm-last-error` and the original topic/partition/offset. A second consumer (group `<group>.retry`) reads it once it is due. The main partition is not blocked |
| Dead-letter topic | `redeliveries` trips through the retry topic failed, or at once for an invalid envelope or a `PermanentError` | Published to `<topic>.dlq` with the reason headers below, and logged at error level |

Because the message value is never rewritten, **retries keep the `event_id` and `transaction_id`**. Both consumers use the same inbox name, so a retry of a message that did commit is reported as a duplicate, not applied twice.

```python
policy = RetryPolicy.from_env()          # LM_RETRY_* overrides, validated at startup
router = FailureRouter(producer, consumer="wallet-saga", policy=policy)
main = InboxConsumer(consumer, sessions, name="wallet-saga", handler=handler, router=router)
retries = InboxConsumer(
    create_retry_consumer(bootstrap, WALLET_COMMANDS),
    sessions, name="wallet-saga", handler=handler, router=router,
)
```

### Configuration

| Setting | Variable | Default |
|---|---|---|
| In-process attempts | `LM_RETRY_ATTEMPTS` | 3 |
| Backoff between attempts (doubles) | `LM_RETRY_BACKOFF_SECONDS` | 0.2 |
| Trips through the retry topic | `LM_RETRY_REDELIVERIES` | 5 |
| Delay before a redelivery (doubles) | `LM_RETRY_REDELIVERY_DELAY_SECONDS` | 5 |
| Cap on any wait | `LM_RETRY_MAX_DELAY_SECONDS` | 60 |
| Handler timeout per attempt | `LM_RETRY_HANDLER_TIMEOUT_SECONDS` | 30 |

An invalid value stops the worker at startup. Keep `LM_RETRY_MAX_DELAY_SECONDS` well below Kafka's `max.poll.interval.ms` (300 s), because the retry consumer waits for a message's due time between polls. Messages in one retry partition are processed in order, so a long delay can hold back shorter ones behind it.

### Dead-letter reasons

| Header | Meaning |
|---|---|
| `lm-dlq-reason` | `invalid_envelope`, `permanent_error` or `retries_exhausted` |
| `lm-dlq-error` | the final error (`repr`, at most 1000 characters) |
| `lm-last-error` | the error that sent it to the retry topic, if it went there |
| `lm-dlq-consumer` | the consumer group that gave up |
| `lm-dlq-failed-at` | when, in UTC |
| `lm-retry-count` | how many retry-topic trips it had |
| `lm-original-topic` / `-partition` / `-offset` | where it was first read |

To see them:

```bash
uv run ledgermesh-dlq wallet.commands.dlq
uv run ledgermesh-dlq wallet.commands.dlq --limit 20 --json
```

It reads the topic from the beginning without joining a consumer group, so it never moves anyone's offsets. Each line shows the reason, the consumer, the retry count, the event type, `event_id` and `transaction_id`, the original position and the error.

### Tests

| Guarantee | Test |
|---|---|
| Retry count, backoff and timeout are configurable and validated | `test_policy_reads_every_setting_from_the_environment`, `test_invalid_settings_fail_at_startup`, `test_backoff_and_redelivery_delay_double_up_to_the_cap` |
| Retries keep the original bytes, event and transaction ids, and first position | `test_first_retry_republishes_the_original_bytes_to_the_retry_topic`, `test_later_retries_keep_the_original_position_and_count_up` |
| Exhausted messages reach the source topic's DLQ | `test_exhausted_redeliveries_go_to_the_source_topics_dlq`, `test_every_main_and_retry_topic_routes_to_its_own_retry_and_dlq`, `test_a_message_that_keeps_failing_ends_on_the_dlq_with_its_reason` |
| A retried message applies once on the retry consumer | `test_exhausted_attempts_move_to_the_retry_topic_and_apply_there` |
| A slow handler times out, rolls back and is retried | `test_a_handler_that_exceeds_the_timeout_is_retried_not_applied` |
| Retry consumers wait until a message is due, and stop cleanly | `test_the_retry_consumer_waits_until_a_message_is_due`, `test_stopping_while_waiting_does_not_process_the_message` |
| The reason is recorded and readable | `test_dead_letter_headers_record_reason_error_consumer_and_time`, `test_describe_shows_reason_ids_and_origin_of_a_dead_letter`, `test_describe_handles_an_invalid_envelope` |

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
| Main, retry and dead-letter topics are created with their declared partitions and configs | `test_declared_configs_of_main_retry_and_dead_letter_topics`, `test_created_topics_have_their_declared_layout_and_configs` |
| Topic bootstrap is repeatable and fixes setting drift | `test_second_run_is_a_no_op`, `test_rerun_restores_retention_drift` |
| A topic with the wrong partition count is reported, not changed | `test_existing_topic_with_wrong_partition_count_is_reported_not_changed` |
| A topic with a different replication factor is reported and its configs are left alone | `test_existing_topics_with_a_different_replication_factor_are_reported_not_changed` |
| A replication factor below 1 is rejected | `test_ensure_topics_rejects_a_replication_factor_below_one`, `test_cli_rejects_a_replication_factor_below_one` |

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
- Ordering and causation checks (`aggregate_version`, `workflow_step`) come with LM-026.
- Trace propagation: `headers` are carried through, and LM-033 will fill in `traceparent`.
