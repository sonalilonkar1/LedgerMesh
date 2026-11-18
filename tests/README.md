# Cross-service tests

Tests that exercise more than one LedgerMesh service belong here.

- Unit tests remain next to their owning service.
- Messaging-library tests remain under `libs/messaging/tests`.
- Cross-service integration, failure-injection, and invariant tests will be added here by later issues.

Database-backed component tests live under `tests/db`. The LM-021 suite in
`tests/db/test_catalog_saga.py` covers transactional Catalog commands, durable
results, early-release tombstones, malformed messages, and restart redelivery.
