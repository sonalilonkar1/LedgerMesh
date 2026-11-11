# LedgerMesh Shared Contracts

This directory contains the versioned wire contracts shared by LedgerMesh services. It does
not contain database tables, ORM models, or service-owned persistence types.

## Contents

- `openapi/`: synchronous HTTP contracts.
- `schemas/events/`: asynchronous Kafka envelope contract.
- `examples/events/`: valid and invalid messages used by contract tests.
- `examples/manifest.yaml`: expected schema and result for every checked-in example.
- `src/ledgermesh_contracts/`: offline validation and v1 compatibility checks.
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
git fetch origin main
uv sync --python 3.12 --locked
make contracts-check BASE_REF=origin/main
```

This is the exact validation performed by CI. `contracts-check` formats/lints the contract code,
validates all current contracts and examples, checks the runtime envelope, and compares every
published v1 contract with `BASE_REF`. To run only compatibility validation:

```bash
make contracts-compat BASE_REF=origin/main
```

The current-contract checks reject:

- malformed OpenAPI 3.1 documents and invalid JSON Schemas;
- unresolved or external `$ref` values (validation never fetches a remote schema);
- duplicate `operationId` values across the HTTP contracts;
- valid examples that do not match their schema;
- negative examples that pass or fail for a reason other than their manifest entry;
- missing or unknown envelope metadata, invalid identifiers and timestamps; and
- drift between the canonical JSON Schema and the runtime `EventEnvelope`.

## Compatibility policy

Files whose names contain `.v1.` under `openapi/` and `schemas/` are published contracts. CI
compares them with the pull request's base commit and rejects changes that would require existing
producers or consumers to change, including:

- removing or renaming a published contract file, endpoint, operation, response, field or schema;
- changing `operationId`, field type, `const`, or a validation pattern;
- removing an enum value or supported media type;
- adding a required field or parameter, or making an optional request body required; and
- tightening numeric, string, array, or `additionalProperties` constraints.

Adding a new endpoint, response, schema, enum value, or optional field is allowed in v1. If a
breaking change is intentional, publish a new major version instead of weakening or bypassing the
compatibility check.

CI runs without Kafka, PostgreSQL, deployment credentials, production secrets, or schema-network
access. It publishes JUnit XML and a readable Markdown compatibility report for 14 days.

## Required status check

After the workflow has run at least once, repository administrators must protect `main` and
require the status check **`Contract validation / validate-contracts`**. The workflow intentionally
runs on every pull request to `main`: a path-filtered workflow can leave an unrelated pull request
waiting forever for a required status that was never created.
