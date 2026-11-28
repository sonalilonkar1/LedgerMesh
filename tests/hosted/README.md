# Hosted smoke suite (LM-050)

End-to-end checks of a **deployed** LedgerMesh environment through its public
HTTPS gateway only (the allowlist in `infra/staging/Caddyfile`). It never
talks to a service port, Kafka or a database directly, and it never chooses an
environment by itself: every hosted test is skipped unless
`LEDGERMESH_HOSTED_BASE_URL` is set, so `make check` is unaffected.

## What it checks

| Test | Case from issue #58 |
|---|---|
| `test_gateway.py::test_health_reports_the_right_service` | `/health` and `/health/<service>` answer 200 with the right `service` (health and service identity) |
| `test_gateway.py::test_read_only_routes_reach_their_services` | the read-only routes reach the right service (its problem+json 404, not the gateway's) |
| `test_gateway.py::test_everything_else_is_an_empty_gateway_404` | 40 forbidden requests (`/internal/*`, 2PC and Saga participant writes, audit writes, API docs, wrong methods, `%2F` and `..` tricks) get Caddy's empty 404 |
| `test_gateway.py::test_http_redirects_to_https`, `test_responses_are_hardened`, `test_only_the_gateway_ports_are_reachable` | HTTP redirects, no `Server`/`Alt-Svc` header, HSTS, and no other TCP port answers |
| `test_redemptions.py::test_saga_redemption_completes_exactly_once` | Saga to `COMPLETED`; five concurrent requests and a later replay with one `Idempotency-Key` give one transaction; wallet deducted once and inventory reserved once (public GET routes); audit trail eventually visible with each transition exactly once; the correlation id preserved from the coordinator through Kafka to the audit ledger; the outcome only visible by polling (202 + `REQUESTED`) |
| `test_redemptions.py::test_reusing_a_key_for_a_different_request_is_rejected` | the same key with a different body is 409 and has no effect |
| `test_redemptions.py::test_insufficient_points_fails_without_effects` | insufficient points ends `FAILED`, nothing reserved |
| `test_redemptions.py::test_exhausted_inventory_is_compensated` | exhausted inventory ends `COMPENSATED`, points refunded exactly once |
| `test_redemptions.py::test_two_phase_commit_redemption` | **skipped with the reason**: 2PC cannot be started through the public API (below) |
| `test_redemptions.py::test_running_revision` | **skipped with the reason**: `/health` reports no revision; the workflows check it on the VM |

**Why 2PC is skipped.** `POST /v1/redemptions` only records `REQUESTED`
(`services/coordinator/app/repository.py`); the Saga starter only picks up
`coordination_mode == "saga"` (`services/coordinator/app/saga.py`); a 2PC
redemption runs only through `POST /internal/v1/redemptions/{id}/execute-2pc`,
which the coordinator answers with 404 unless `LEDGERMESH_ENV` is `local` or
`test` (`services/coordinator/app/main.py`) and which the gateway blocks. A
public `two_phase_commit` request would stay `REQUESTED` for ever, so the
suite never sends one. 2PC is covered by `make test-2pc-integration`.

**Not covered: Audit Ledger temporarily unavailable.** Issue #58 lists this
failure case, but testing it means stopping the audit consumer on shared
staging while other runs use it, so the hosted suite does not do it. A
hosted test would have to show that a redemption still reaches its terminal
state and that its audit events appear once the consumer is back. Until such
a test exists, this case is unproven on staging. It should be tested against
the local staging stack (`make staging-local-up`, then stop the
`audit-consumer` container) or as an LM-036 failure experiment.

## Isolation: per-run fixtures

Each run has a run id (6-40 lowercase letters and digits; the workflow uses
`gh<run_id>a<attempt>`). Before the tests, `infra/staging/fixtures.py`
creates that run's own wallets and rewards, `smoke-<run_id>-<name>`, with known
balances and inventory. It runs inside the running `wallet` and `catalog`
containers with each service's own database credentials, inserts only
`smoke-<run_id>-*` rows with a plain `INSERT` (a reused run id fails), and
refuses unless `LEDGERMESH_ENV` is `staging`, `local` or `test`. Every
`Idempotency-Key` also carries the run id plus a random part. Two runs at the
same time therefore never share a balance, an inventory or a key.

Fixture rows are never deleted: wallet operations, reservations and audit
events refer to them. The `smoke-` prefix keeps them apart from real data.

Polling uses exponential backoff from 0.25 s up to 5 s, bounded by
`LEDGERMESH_HOSTED_WAIT_SECONDS` per wait and `LEDGERMESH_HOSTED_TIMEOUT_SECONDS`
for the whole run; there are no fixed sleeps.

## Running it

Always name the environment explicitly.

**Staging, from GitHub:** Actions, **Hosted smoke**, *Run workflow*, with
`base_url` set to the staging URL. For the staging URL the workflow creates
the fixtures on the VM over IAP SSH (`infra/staging/ci/remote.sh fixtures`)
and requires the redemption tests to run. For any other URL it cannot create
fixtures, so only the gateway tests run and the redemption tests are skipped
with that reason. The deploy and rollback workflows call it automatically.

**Staging, from a laptop** (needs IAP SSH access, docs/deployment/staging.md §11):

```bash
RUN_ID=dev$(date +%s)
bash infra/staging/ci/remote.sh fixtures "$RUN_ID"
LEDGERMESH_HOSTED_BASE_URL=https://<ip-with-dashes>.sslip.io \
LEDGERMESH_HOSTED_RUN_ID="$RUN_ID" LEDGERMESH_HOSTED_REQUIRE_FIXTURES=true \
  make hosted-smoke
```

**The local staging stack** (docs/deployment/staging.md §14). Caddy uses its
own local CA there, so pass that CA file; TLS verification is never switched
off. Host ports 5432 and 9092 may be taken by unrelated local programs, so
choose the ports to check:

```bash
RUN_ID=loc$(date +%s)
make staging-local-fixtures RUN_ID="$RUN_ID"
LEDGERMESH_HOSTED_BASE_URL=https://localhost \
LEDGERMESH_HOSTED_RUN_ID="$RUN_ID" LEDGERMESH_HOSTED_REQUIRE_FIXTURES=true \
LEDGERMESH_HOSTED_CA_BUNDLE=artifacts/staging/data/caddy/data/caddy/pki/authorities/local/root.crt \
LEDGERMESH_HOSTED_CLOSED_PORTS=2019,8000,8001,8002,8003,19092,29092,29093 \
  make hosted-smoke
```

`make hosted-smoke` refuses to start without `LEDGERMESH_HOSTED_BASE_URL`. If
the URL cannot be reached, or does not answer `/health` as the LedgerMesh
coordinator, the run stops at once with one message saying so.

## Settings

| Variable | Meaning |
|---|---|
| `LEDGERMESH_HOSTED_BASE_URL` | `https://<host>[:port]` to test. Required. |
| `LEDGERMESH_HOSTED_RUN_ID` | Run id whose fixtures exist. Without it the redemption tests are skipped. |
| `LEDGERMESH_HOSTED_REQUIRE_FIXTURES` | `true`: fail instead of skipping when the run id is missing (the workflow sets it for staging). |
| `LEDGERMESH_HOSTED_CA_BUNDLE` | Extra CA file to trust (local `tls internal` only). |
| `LEDGERMESH_HOSTED_EXPECTED_REVISION` | Recorded in the diagnostics; see `test_running_revision`. |
| `LEDGERMESH_HOSTED_TIMEOUT_SECONDS` | Budget for the whole run (default 300). |
| `LEDGERMESH_HOSTED_WAIT_SECONDS` | Budget for one wait (default 120). |
| `LEDGERMESH_HOSTED_CLOSED_PORTS` | Ports that must refuse TCP connections (default `22,2019,5432,8000-8003,9092,19092,29092,29093`), or `none`. |
| `LEDGERMESH_HOSTED_ARTIFACTS` | Where `junit.xml` and `diagnostics.json` go (default `artifacts/hosted-smoke`). |

## Diagnostics

Every run writes `diagnostics.json`: for each scenario the idempotency key,
transaction and correlation ids, the statuses seen while polling (with UTC
times), the audit events' ids, sequence numbers and status changes, and the
final balances and inventory, plus each test's outcome. It contains only data
the public API returned; no credentials, tokens or connection strings. The
workflow keeps it with `junit.xml` as the `hosted-smoke-<run_id>` artifact
for 14 days.
