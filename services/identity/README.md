# Identity Service

The Identity Service signs people in with their email address and a single-use
link (LM-052). No secret is chosen, stored or reset. It owns users, login link
hashes, server-side sessions, rate-limit counters and an append-only
authentication audit trail in its own `identity` database, and it decides
whether a request comes from a guest, a user or an operator.

It is outside the redemption path: it takes part in neither 2PC nor Saga, and
no other service requires a session yet, so the guest demo is unchanged.

The flow, threat model, policy choices, configuration and email provider setup
are in [docs/authentication.md](../../docs/authentication.md).

## API (v1)

| Method | Path | Who | Purpose |
|---|---|---|---|
| `POST` | `/v1/auth/login-requests` | anyone | Validate and normalize `{email}`, email a 15-minute single-use link; always the same `202` |
| `GET` | `/v1/auth/verify` | anyone | Static confirm page the link opens (token in the URL fragment) |
| `POST` | `/v1/auth/verify` | anyone | Consume `{token}` once, start a session, set the `lm_session` cookie |
| `POST` | `/v1/auth/logout` | anyone | Revoke the session and clear the cookie; `204` |
| `GET` | `/v1/me` | user | The signed-in user, role and session expiry |
| `GET` | `/v1/sessions/introspect` | user | Principal for a forwarded cookie, for trusted callers such as the web BFF |
| `GET` | `/v1/operator/auth-events` | operator | Authentication audit events, newest first |
| `GET` | `/health` | anyone | Liveness |

Contract: [`libs/contracts/openapi/identity.v1.yaml`](../../libs/contracts/openapi/identity.v1.yaml).
`tests/test_contract.py` fails if the app's routes drift from it.

## Code

| Module | Responsibility |
|---|---|
| `app/settings.py` | Environment variables, validation, deployed-environment startup checks |
| `app/email_address.py` | `email-validator` syntax check, normalization, masking, domain denylist |
| `app/tokens.py` | 256-bit random tokens and session ids, SHA-256 storage hashes |
| `app/rate_limit.py` | Persistent fixed-window limits per email and per IP |
| `app/repository.py` | PostgreSQL transactions with conditional updates (atomic consume, session sliding) |
| `app/service.py` | Sign-in flow, sessions, roles, logging without personal data |
| `app/authz.py` | `get_principal`, `require_user`, `require_operator` FastAPI dependencies |
| `app/mailer.py` | SMTP delivery (Mailpit locally, any provider with TLS when deployed) |
| `app/verify_page.py` | The confirm page and its security headers |
| `app/main.py` | Routes, problem responses, cookies |

## Local use

```bash
make infra-up        # identity-db (54325), mailpit (1025/8025) and identity (8005)
make mail-ui         # the Mailpit inbox URL
```

Request a link and open it from <http://localhost:8025>:

```bash
curl -i -X POST http://localhost:8005/v1/auth/login-requests \
  -H 'Content-Type: application/json' -d '{"email":"alice@example.com"}'
```

Or run only this service on the host (Compose `identity-db` and `mailpit` must be up):

```bash
make run-identity
```

## Migrations

```bash
make db-upgrade SERVICES=identity
DATABASE_URL=postgresql+psycopg://identity:ledgermesh_dev@localhost:54325/identity \
  uv run alembic -c services/identity/alembic.ini upgrade head
```

## Tests

```bash
uv run pytest services/identity/tests -q                    # in-memory, runs in make check
make infra-up
uv run pytest tests/db/test_identity_core.py -q             # real PostgreSQL
make identity-e2e                                           # running service + Mailpit API
```
