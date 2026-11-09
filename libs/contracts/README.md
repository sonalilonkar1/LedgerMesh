# LedgerMesh Shared Contracts

This directory contains the versioned wire contracts shared by LedgerMesh services. It does
not contain database tables, ORM models, or service-owned persistence types.

## Contents

- `openapi/`: synchronous HTTP contracts.
- `schemas/events/`: asynchronous Kafka envelope contract.
- `examples/events/`: valid and invalid messages used by contract tests.
- `tests/`: OpenAPI, JSON Schema, and runtime-envelope compatibility tests.

## Contract ownership

The files in this directory define what may cross a service boundary. Each service remains
responsible for translating these wire representations into its own domain and persistence
models.

`libs/messaging/src/ledgermesh_messaging/envelope.py` remains the runtime Pydantic
implementation of the event envelope. The compatibility test prevents it from drifting from
`schemas/events/event-envelope.v1.json`.

## Versioning policy

LedgerMesh uses explicit integer major versions:

- HTTP routes include `/v1` and OpenAPI documents use version `1.x.x`.
- Kafka messages carry `schema_version: 1`.
- Backward-compatible additions may stay in the current major version. Examples include a new
  optional response field or a new event type whose payload has its own documented semantics.
- Removing or renaming a field, changing its type or meaning, changing optional data to
  required data, or changing an endpoint incompatibly requires a new major version.
- Published required fields cannot be added to an existing version.
- Consumers must ignore unknown optional fields inside versioned payloads, but the shared
  envelope is strict and rejects unknown envelope metadata.
- Producers validate before publishing and consumers validate before applying business state.
- During a migration, producers and consumers support the old version until all consumers have
  moved to the new version. Removal requires a separately documented deprecation window.
- Schema changes require contract tests and pull-request review.

## Envelope rules

- Commands set `aggregate_version` to `null`.
- An owning service assigns `aggregate_version` to events emitted after a successful aggregate
  mutation.
- `causation_id` may be `null` only for the root message; later messages identify their direct
  cause.
- `fencing_token` is nullable until the etcd leadership extension is enabled.
- `event_id` identifies message delivery/idempotency; `transaction_id` identifies a redemption;
  `correlation_id` connects the full workflow.

## Reward pricing

The client never supplies the authoritative point cost. Before starting either coordination mode,
the Coordinator reads `GET /v1/rewards/{reward_id}` from Catalog and calculates the total using
the returned point cost and requested quantity. Catalog prepare validates the expected reward
version and point cost. A price or version change therefore produces a durable NO vote instead of
allowing Wallet and Catalog to commit different interpretations of the redemption.

## Fencing-token scope

The baseline 2PC HTTP operations do not require a fencing token. If the optional etcd leadership
extension is implemented under LM-038/LM-039, the participant HTTP contracts will be extended
with a Coordinator fencing token, and Wallet and Catalog will reject tokens lower than the highest
accepted term. Saga messages already reserve the nullable `fencing_token` envelope field.

## Validation

From the repository root:

```bash
uv sync --python 3.12
make contracts-check
```

The test suite rejects malformed OpenAPI documents, malformed envelopes, unexpected envelope
fields, invalid identifiers, naive timestamps, negative versions, and schema/runtime drift.
