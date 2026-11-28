# Cross-service integration and invariant checks (LM-017/LM-031)

Run the cross-service suite against local Compose PostgreSQL:

```bash
make infra-up
make test-2pc-integration
```

The suite creates four temporary databases, migrates each with its service-owned
schema, and always rolls back leaked prepared transactions before dropping them.
It never modifies the normal Coordinator, Wallet, Catalog, or Audit Ledger data.

The 2PC scenarios prove:

- a successful 2PC redemption commits Wallet, Catalog, Coordinator, and Audit;
- a Wallet or Catalog NO vote aborts all prepared effects;
- a prepared Wallet transaction blocks a competing update while the Coordinator
  is unavailable;
- restart recovery releases that lock using presumed abort;
- the post-test invariant checker reports zero violations;
- no PostgreSQL prepared transaction remains after a completed experiment.

The blocking test uses a 700 ms PostgreSQL `statement_timeout`. The JSON report
separates the probe timeout from the useful lock-lifetime measurement:

- `prepared_lock_probe_timeout_seconds` proves a competing update was blocked;
- `prepared_lock_duration_seconds` measures from the completed prepare until
  recovery released the lock and the competing update succeeded.

The LM-031 invariant checker runs automatically after every test in
`tests/integration` and checks the four isolated service databases. It detects
negative wallet balances, invalid inventory, duplicate business effects,
multiple terminal transitions, successful redemptions missing either effect,
and failed/aborted/compensated redemptions with unmatched wallet operations or
active reservations. Tests marked `invariant_checker` intentionally inject bad
state and assert that the checker reports the expected violation.

It also reports any redemptions left non-terminal after a test, grouped by
coordination mode and status. This catches workflows stranded in states such
as `PREPARING` or `COMPENSATING`. Because the automatic check runs in the
autouse fixture's teardown, an invariant violation appears in pytest as a
teardown **ERROR** for that test, not a normal test-body **FAILED** assertion.
The error includes the invariant details, which are also written to the JSON
report.

Measurements, explicit scenario checks, and automatic per-test checks are
recorded in `artifacts/2pc-integration.json`. JUnit results are written to
`artifacts/2pc-integration.xml`; CI retains both for 14 days.

To use a different PostgreSQL server:

```bash
LM_TEST_POSTGRES_URL=postgresql+psycopg://user:password@host:5432/database \
  make test-2pc-integration
```

The PostgreSQL role must be allowed to create temporary databases and the
server must have `max_prepared_transactions` greater than zero.

- Unit tests remain next to their owning service.
- Messaging-library tests remain under `libs/messaging/tests`.
- Cross-service integration, failure-injection, and invariant tests live under
  `tests/integration`; LM-017 provides the initial 2PC suite and LM-031 extends
  its shared checker to Saga outcomes.

Database-backed component tests live under `tests/db`. The LM-021 suite in
`tests/db/test_catalog_saga.py` covers transactional Catalog commands, durable
results, early-release tombstones, malformed messages, and restart redelivery.
`tests/db/test_saga_duplicate_delivery.py` (LM-029) races duplicate Wallet and
Catalog deliveries against PostgreSQL and scrapes the workers' real metrics
endpoint to verify duplicate and rejection counter increments.
