# Distributed tracing with OpenTelemetry (LM-033)

**One redemption is one trace.** It covers every REST call, Kafka event and database statement, across the Coordinator, Wallet, Catalog and Audit Ledger, for both 2PC and Saga. Traces go to Jaeger. The setup lives in one place, `ledgermesh_messaging.tracing`, next to the logging (LM-032) it complements.

## Viewing traces

`make infra-up` starts Jaeger with the stack. Open **http://localhost:16686** (`JAEGER_UI_PORT` overrides the port), pick a service such as `coordinator`, and open a trace.

To find one redemption, search by tag, for example `ledgermesh.transaction_id=<id>`. A 2PC redemption shows the create request, then the execute request with the Catalog quote, both prepares and both commits (or aborts) as children. A Saga redemption shows the create request, then `saga start`, then one `<topic> process` span per Kafka message in Wallet, Catalog, the Coordinator and the Audit Ledger, each with its SQL statements.

Traces are kept in memory and reset when the Jaeger container restarts.

## How the context travels

| Hop | Carried by | Code |
|---|---|---|
| Client → service | `traceparent` header | `instrument_fastapi` (each service's `main.py`) |
| Coordinator → Wallet / Catalog (2PC, quotes) | `traceparent` header | `instrument_httpx` |
| Producer → Kafka → consumer | `traceparent` in the outbox row's `headers`, sent as Kafka headers | `enqueue` adds it; `OutboxPublisher` sends it; `InboxConsumer` continues it |
| Retry topic and DLQ | the same headers, kept when republished | `FailureRouter` |
| Coordinator API → Saga worker, and → 2PC recovery | `redemptions.trace_context` (JSONB, migration `0002`) | `create` stores it; `SagaStarter` and `TwoPhaseCommitRecovery` continue it |
| Service → PostgreSQL | a child span per statement | `trace_engine` on every engine |

The last database hop matters because a Saga redemption is written by the API and started later by another process. Without the stored context, the Saga would begin a new trace, disconnected from the request that asked for it. The column is nullable: untraced requests, and rows created before the migration, simply start a new trace.

## Span attributes

| Attribute | On | Meaning |
|---|---|---|
| `ledgermesh.mode` | Coordinator spans, consumer spans | `two_phase_commit` or `saga` |
| `ledgermesh.state` / `ledgermesh.from_state` | the span where a transition happened | the Coordinator status reached, and the one before |
| `ledgermesh.compensation` | Saga spans entering `COMPENSATING` or `COMPENSATED` | `true` while the redemption is undoing work |
| `ledgermesh.retry_count` | consumer spans | how many times the message came back through the retry topic |
| `ledgermesh.attempt` | consumer spans | the in-process attempt that finished (1 = first try) |
| `ledgermesh.transaction_id`, `correlation_id`, `event_id`, `event_type`, `workflow_step` | consumer and Coordinator spans | which redemption and message the span belongs to |

Every state change also adds a `redemption.transition` **span event** with its reason, so a span that covers several transitions keeps all of them in order. Consumer spans add `retry` events for in-process retries. A dead-lettered or abandoned message marks its span as an error, with a `dead_letter` or `gave_up` event.

SQL spans carry the statement text but never its parameters, so balances and user ids don't end up in Jaeger. `/health` and `/metrics` aren't traced.

## Configuration

| Variable | Effect |
|---|---|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | where to send spans (OTLP over HTTP). Set to `http://jaeger:4318` in `infra/compose.yaml`. Unset: spans are still created, so context still propagates, but nothing is exported. |
| `OTEL_SERVICE_NAME` | overrides the service name (default: the service's own name) |
| `OTEL_SDK_DISABLED=true` | turns tracing off completely |

The OTLP port is only reachable on the stack's private network. Only the Jaeger UI is published.

## Tests

- `libs/messaging/tests/test_tracing.py` checks propagation helpers, the outbox headers, consumer spans continuing the producer's trace and passing it on, retry and DLQ headers, retry and failure attributes, and SQL spans that leave values out.
- `tests/test_rest_tracing.py` sends a real HTTP call from the Coordinator's 2PC participant client to an instrumented server, checking that the server joins the caller's trace. It also checks that every service continues a caller's `traceparent` and doesn't trace health checks.
- `tests/integration/test_trace_propagation.py` runs one Saga redemption through the real services and databases, delivering each outbox row with its headers. Every span, from the API request to the Audit Ledger, shares one trace and forms one tree. A second test does the same for a compensated redemption, checking that compensation is marked.
- `tests/infra/test_tracing_config.py` checks that compose runs a pinned Jaeger, publishes only its UI, and points every service at it.
