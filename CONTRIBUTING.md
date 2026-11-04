# Contributing to LedgerMesh

## Prerequisites

- Python 3.12
- [uv](https://docs.astral.sh/uv/)
- Docker and Docker Compose for integration tests
- GitHub CLI is optional but recommended

## Initial setup

```bash
git clone <repository-url>
cd LedgerMesh
uv sync --python 3.12
uv run pytest services -q
```

The messaging library maintains its own environment and integration suite under
`libs/messaging`.

## Branch naming

Create branches from an up-to-date `main` branch:

```text
feat/lm-###-short-description
fix/lm-###-short-description
docs/lm-###-short-description
test/lm-###-short-description
```

Example:

```text
feat/lm-007-wallet-core
```

## Development workflow

1. Assign the GitHub issue to yourself and move it to **In Progress**.
2. Pull the latest `main` and create a focused branch.
3. Keep service-owned data and logic within that service.
4. Add or update tests for every behavior change.
5. Run `make check` before pushing.
6. Open a pull request and link the issue using `Closes #<issue-number>`.
7. Obtain at least one teammate review before merging.

## Commit messages

Use short, descriptive commit messages:

```text
feat(wallet): add balance query endpoint
fix(catalog): prevent negative inventory
test(coordinator): cover duplicate redemption request
docs: explain Saga compensation flow
```

## Pull-request requirements

- The PR has a clear summary and test evidence.
- Formatting, linting, and unit-test checks pass.
- Changes do not read or modify another service's database directly.
- API or event contract changes are documented.
- New configuration has a safe example and contains no secrets.
- Known limitations and follow-up issues are identified.

## Service ownership

Each service owns its application logic, migrations, and database. Shared code belongs in `libs`
only when at least two services genuinely need the same contract or infrastructure behavior.
