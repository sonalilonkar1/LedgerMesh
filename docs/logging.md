# Structured logs and transaction correlation (LM-032)

Every LedgerMesh process (the five HTTP services and the four Kafka workers) writes **one JSON object per line** to stdout, with the **same field names** in every service. The setup lives in one place, `ledgermesh_messaging.logs`, so the names can't drift between services.

## Fields

| Field | Meaning |
|---|---|
| `timestamp` | UTC, ISO 8601, milliseconds |
| `level` | `INFO`, `WARNING`, `ERROR`, ... |
| `service` | `coordinator`, `wallet`, `catalog`, `audit-ledger` or `identity` (API and worker alike) |
| `logger` | Python logger name |
| `message` | the log message |
| `transaction_id` | the redemption this line belongs to |
| `event_id` | the Kafka message being handled |
| `correlation_id` | constant across one redemption's commands, results and 2PC calls |
| `causation_id` | the message that caused the one being handled |
| `event_type` | e.g. `wallet.deduct_points` |
| `workflow_step` | the Saga/2PC step of the message |
| `state` | the state reached: a Coordinator status (`DEDUCTING_POINTS`, `COMMIT_DECIDED`, ...), a 2PC participant state (`PREPARED`, `COMMITTED`, ...) or a Saga command outcome (`applied`, `insufficient_points`, ...) |
| `fault_id` | the injected fault this line belongs to, if any |

Every field is always present (`null` when unknown), so queries never have to guess. Some lines add more, for example `from_state` and `coordination_mode` on Coordinator transitions, `result_event_id` on Saga replies, and `exception` with a traceback.

## How the ids get there

Call sites don't pass ids around; they're bound to a context for a whole unit of work:

- **Kafka:** `InboxConsumer` binds the message's `transaction_id`, `event_id`, `correlation_id`, `causation_id`, `event_type` and `workflow_step` while handling it, including its retries and dead-lettering (LM-024). `OutboxPublisher` binds them when a publish fails.
- **HTTP:** `CorrelationMiddleware` takes `transaction_id` from `/transactions/{id}` or `/redemptions/{id}` paths, and `correlation_id` from the `X-Correlation-Id` header.
- **2PC:** the Coordinator binds the transaction while it runs or recovers a workflow, and sends `X-Correlation-Id` on every prepare, commit and abort, so participant logs join the same trail.
- **State changes:** every Coordinator transition (Saga and 2PC), every participant 2PC state change and every Saga command outcome logs one line with `state`.

## Following one transaction

```bash
docker compose -f infra/compose.yaml logs --no-log-prefix \
  | jq -c 'select(.transaction_id == "<transaction id>")
           | {timestamp, service, state, event_type, message}'
```

The result is the redemption's full history across all four services, in order. Filtering on `correlation_id` gives the same trail.

## Sensitive values

- Any field whose name looks like a secret (`password`, `secret`, `token`, `authorization`, `api_key`, `credential`, `cookie`, `database_url`, `dsn`, `private_key`) is written as `[REDACTED]`, including inside nested mappings. `fencing_token` is the one exception, because it isn't a secret.
- Credentials inside URLs (`postgresql://user:password@host`) are masked to `user:***@host` in messages, fields and tracebacks.
- Request and message bodies are never logged.

## Failure injection

The experiments (LM-035/036) mark everything an injected fault causes:

- **Per process:** start a service with `LEDGERMESH_FAULT_ID=<id>`, and every line it writes carries that `fault_id`.
- **At one point in the code:**
  ```python
  from ledgermesh_messaging import fault, log_fault_injected

  log_fault_injected(logger, "kill-wallet-before-commit", "wallet.saga.deduct")
  with fault("kill-wallet-before-commit"):
      ...  # every log in here carries the fault_id
  ```

Then `jq 'select(.fault_id == "<id>")'` shows the fault and its consequences in every service.

## Settings

| Variable | Default | Meaning |
|---|---|---|
| `LEDGERMESH_LOG_LEVEL` | `INFO` | minimum level |
| `LEDGERMESH_FAULT_ID` | unset | tag every line of this process with a fault id |

## Tests

| Guarantee | Test |
|---|---|
| Same field names everywhere, always present | `test_every_line_has_every_shared_field_even_when_unknown` (`libs/messaging/tests/test_logs.py`) |
| Ids reach every log in a unit of work, and don't leak between tasks | `test_bound_context_reaches_every_log_in_the_block`, `test_context_does_not_leak_between_concurrent_tasks`, `test_handler_logs_carry_the_message_ids`, `test_retry_and_dead_letter_logs_carry_the_message_ids` |
| HTTP requests are correlated | `test_middleware_binds_transaction_from_path_and_correlation_from_header` |
| Sensitive values are excluded | `test_secret_like_fields_are_redacted`, `test_credentials_in_urls_are_masked_everywhere`, `test_nested_mappings_are_redacted` |
| Fault ids are recorded | `test_fault_block_tags_every_log_inside_it`, `test_log_fault_injected_records_the_fault_and_where`, `test_fault_id_from_the_environment_tags_the_whole_process` |
| One transaction can be reconstructed across services | `test_one_saga_redemption_can_be_followed_across_services` (`tests/integration/test_log_correlation.py`) |

## Identity logs (LM-052)

The Identity Service logs sign-in steps with `user_id` and `session_id` (the session row id, not the cookie value) only. It never passes an email address, a sign-in token or a cookie value to a logger: the formatter redacts fields named like `token` or `cookie`, but it does not recognise email addresses inside messages. Sign-in links carry the token in the URL fragment, so access logs never see it. See [authentication](authentication.md#audit-and-logging).
