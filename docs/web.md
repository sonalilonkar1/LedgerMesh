# Web interface (LM-011)

A minimal page for trying LedgerMesh: pick a user, see their balance and the available rewards, redeem one with **Two-Phase Commit** or a **Saga**, and watch the redemption's status and timeline update until it finishes.

```bash
make infra-up
```

Then open **http://localhost:8080** (`WEB_PORT` changes the port).

- **2PC** redemptions are driven by the Coordinator's recovery loop, which is enabled in Compose.
- **Saga** redemptions also need the Saga workers running: `make run-coordinator-saga`, `make run-wallet-saga` and `make run-catalog-saga`.
- The user list is the LM-006 seed users. For a fresh start, run `LEDGERMESH_ENV=local make db-baseline`.

## How it is built

`services/web` is a small FastAPI app:

| Path | What |
|---|---|
| `/` and `/static/*` | the page: plain HTML, CSS and JavaScript, with no build step |
| `GET /api/users/{user_id}/wallet` | Wallet balance |
| `GET /api/rewards` | Catalog rewards in stock |
| `POST /api/redemptions` | create a redemption (`coordination_mode`: `two_phase_commit` or `saga`); the page sends a new `Idempotency-Key` per click |
| `GET /api/redemptions/{transaction_id}` | the redemption's status and its full transition timeline |

The browser only talks to this app, which calls the services for it, so service addresses come from configuration (`WALLET_URL`, `CATALOG_URL`, `COORDINATOR_URL`) and no cross-origin setup is needed.

The page doesn't hard-code the list of states. Final states are shown as succeeded (`COMMITTED`, `COMPLETED`) or failed (`ABORTED`, `FAILED`, `COMPENSATED`), and anything else as in progress, so a new Coordinator state needs no UI change. Everything from the API is inserted as text, never as HTML.

## Errors

The browser only ever sees a short `{"title", "detail"}`:

| What happened | The user sees |
|---|---|
| a service rejected the request (4xx), e.g. insufficient points or an idempotency conflict | that service's own title and detail, nothing else from its response |
| invalid input (bad mode, quantity < 1, malformed id) | "Invalid request: please check ..." naming the fields |
| a service failed (5xx), timed out or is unreachable | "*Service* is unavailable. Please try again in a moment." |
| anything unexpected in this app | "Something went wrong. Please try again in a moment." |

Stack traces and internal details are written to this app's log, never to the response.

## Tests

`services/web/tests/test_web.py` replaces the services with `httpx.MockTransport`, and covers:
- the page and its assets;
- balance and rewards;
- redemptions in both modes, with idempotency keys;
- status plus timeline;
- input validation;
- forwarded problems, hidden 5xx bodies, unreachable services, and unexpected errors with no trace or secrets in the response.
