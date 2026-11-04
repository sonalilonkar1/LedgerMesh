# LedgerMesh

LedgerMesh is a distributed rewards-redemption system for comparing **Two-Phase Commit (2PC)** and the **Saga pattern** under the same workloads and failure conditions.

A redemption spans independently owned Wallet, Rewards Catalog, Coordinator, and Audit Ledger services. The project focuses on transaction correctness during partial failures, duplicate or out-of-order messages, concurrent requests, retries, and service recovery.

> LedgerMesh is under active development. Features are implemented incrementally through the project's LM issues.

## Project Documentation

- [Project Proposal](Project_Proposal.md) — project motivation, business value, proposed architecture, technologies, experiments, and expected deliverables
- [ADR-001: Architecture Decisions and Correctness Invariants](docs/architecture/ADR-001-ledgermesh-architecture-and-correctness.md) — canonical service boundaries, state models, recovery rules, message semantics, and correctness invariants
- [Local Development Guide](docs/development.md) — setup, service commands, and testing instructions
- [Contributing Guide](CONTRIBUTING.md) — branch naming, commits, reviews, and pull-request expectations

## Architecture Overview

LedgerMesh uses strict service ownership:

- **Redemption Coordinator** manages redemption workflow state and executes either 2PC or Saga.
- **Wallet Service** owns customer point balances, deductions, refunds, and Wallet idempotency records.
- **Rewards Catalog Service** owns reward inventory, reservations, releases, and Catalog idempotency records.
- **Audit Ledger Service** stores an append-only transaction history.
- **Messaging Library** provides the shared Kafka envelope, publishing, inbox, and outbox infrastructure.

No service directly reads or modifies another service's database.

In the Saga workflow, commands identify work using `transaction_id` and `workflow_step`. Wallet and Catalog assign `aggregate_version` to events describing their own successful state changes. Database constraints, idempotency records, and optimistic concurrency control protect balances and inventory during concurrent transactions.

In the 2PC workflow, the Coordinator durably records COMMIT or ABORT before notifying participants. Audit events are delivered asynchronously through a transactional outbox, so an Audit Ledger outage cannot block or reverse the transaction decision.

Detailed architecture rules are maintained in ADR-001 rather than duplicated in this README.

## Repository Structure

```text
LedgerMesh/
├── .github/
│   ├── pull_request_template.md
│   └── workflows/
│       ├── ci.yml
│       └── messaging.yml
├── docs/
│   ├── architecture/
│   │   └── ADR-001-ledgermesh-architecture-and-correctness.md
│   └── development.md
├── infra/
├── libs/
│   ├── contracts/
│   └── messaging/
├── services/
│   ├── audit_ledger/
│   ├── catalog/
│   ├── coordinator/
│   └── wallet/
├── tests/
├── CONTRIBUTING.md
├── Makefile
├── Project_Proposal.md
├── pyproject.toml
├── README.md
└── uv.lock
```

The root Python environment owns the core service scaffold and its unit tests. The messaging library maintains a separate `pyproject.toml`, lockfile, and integration-test environment under `libs/messaging`.

## Prerequisites

Install the following tools before starting:

- Git
- Python 3.12
- [uv](https://docs.astral.sh/uv/)
- Docker Desktop with Docker Compose

`uv` creates and manages the repository's `.venv` automatically. Manual virtual-environment creation or activation is not required.

## Initial Setup

Clone the repository and install the core dependencies:

```bash
git clone https://github.com/sonalilonkar1/LedgerMesh.git
cd LedgerMesh
uv sync --python 3.12
```

Run all core formatting, linting, and unit-test checks:

```bash
make check
```

Expected baseline result:

```text
Ruff formatting check passed
Ruff linting passed
4 service health tests passed
```

A dependency deprecation warning from FastAPI/Starlette's test client may be displayed without failing the test suite.

## Running the Core Services

| Service | Command | Port | Health endpoint |
|---|---|---:|---|
| Redemption Coordinator | `make run-coordinator` | 8000 | `http://localhost:8000/health` |
| Wallet | `make run-wallet` | 8001 | `http://localhost:8001/health` |
| Rewards Catalog | `make run-catalog` | 8002 | `http://localhost:8002/health` |
| Audit Ledger | `make run-ledger` | 8003 | `http://localhost:8003/health` |

For example:

```bash
make run-coordinator
```

In another terminal:

```bash
curl http://localhost:8000/health
```

Expected response:

```json
{"status":"ok","service":"coordinator"}
```

The current service applications are initial skeletons. Business APIs, persistence, transaction coordination, and observability will be added through their assigned LM issues.

## Core Development Commands

| Command | Purpose |
|---|---|
| `make install` | Install or synchronize root dependencies |
| `make format` | Format the service code and apply safe lint fixes |
| `make lint` | Check formatting and linting |
| `make test` | Run core service unit tests |
| `make check` | Run linting and unit tests |

Run an individual service's health tests with:

```bash
uv run pytest services/coordinator/tests -q
uv run pytest services/wallet/tests -q
uv run pytest services/catalog/tests -q
uv run pytest services/audit_ledger/tests -q
```

## Messaging Library

The messaging library is isolated under `libs/messaging`. It contains the shared event envelope, Kafka publisher, transactional inbox behavior, and messaging integration tests.

Run its tests separately:

```bash
cd libs/messaging

docker compose -f docker-compose.test.yml up -d --wait
uv sync --python 3.12 --locked
uv run pytest
docker compose -f docker-compose.test.yml down -v
```

Always run the final `docker compose ... down -v` command after testing so the temporary Kafka test environment and volumes are removed.

## Continuous Integration

The repository currently uses two GitHub Actions workflows:

- **`ci.yml`** checks formatting, linting, and core service unit tests.
- **`messaging.yml`** runs messaging-library unit and Kafka integration tests.

Before opening or updating a pull request, run the relevant local checks and include the results in the PR description.

## Development Workflow

1. Assign the GitHub issue to yourself and move it to **In Progress**.
2. Create a focused branch from the latest `main`.
3. Implement only the scope of the assigned issue.
4. Add or update tests for behavioral changes.
5. Run the relevant local checks.
6. Push the branch and open a pull request.
7. Link the issue using `Closes #<issue-number>`.
8. Resolve review comments and obtain the required teammate approval before merging.

Branch naming examples:

```text
feat/lm-007-wallet-core
fix/lm-019-inbox-race
docs/lm-001-architecture
test/lm-026-concurrency
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for the complete workflow.

## Correctness Principles

All implementations must preserve the canonical invariants in ADR-001, including:

- Wallet balances never become negative.
- Reward inventory never becomes negative or oversold.
- A logical operation produces at most one business effect.
- A redemption reaches at most one terminal outcome.
- A successful redemption has both its point deduction and inventory allocation.
- An unsuccessful redemption leaves no uncompensated deduction or ghost allocation.
- Late or duplicate events cannot change an existing terminal outcome.

If this README conflicts with ADR-001, ADR-001 is authoritative.

