"""Per-run fixture data for the hosted smoke suite (LM-050).

The hosted smoke suite (tests/hosted) checks exact, exactly-once effects, so
every run needs wallets and rewards whose starting values it knows and that
no other run touches. This script creates them inside the running stack:

    stack.sh fixtures <run_id>          # local, or through deploy.sh on the VM
    deploy.sh fixtures <run_id>         # the VM, from the smoke workflow

which pipes this file into ``python -`` inside the running ``wallet`` and
``catalog`` API containers. Each run therefore uses only that service's own
image, schema module and ``DATABASE_URL`` (its own credentials): the Wallet
rows are written with Wallet's credentials and the Catalog rows with
Catalog's. Nothing crosses a service boundary and no password leaves the
container.

Rules, following infra/scripts/seed.py (LM-006):

- Every identifier is ``smoke-<run_id>-<name>``; the script never reads,
  changes or deletes any other row.
- Plain INSERT, no ``ON CONFLICT``: a reused run id fails instead of
  silently starting from someone else's balance or inventory.
- Rows are never deleted. Wallet operations, reservations and the audit
  ledger refer to them, and deleting them would break that history; the
  ``smoke-`` prefix keeps them apart from real data (docs/deployment/staging.md).
- Refuses unless ``LEDGERMESH_ENV`` is staging, local or test.

The names, values and identifiers below are the contract with
tests/hosted/_support.py; tests/infra/test_staging_fixtures.py keeps them in step.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass

RUN_ID_PATTERN = re.compile(r"^[a-z0-9]{6,40}$")
ALLOWED_ENVIRONMENTS = frozenset({"staging", "local", "test"})
SERVICES = ("wallet", "catalog")


@dataclass(frozen=True)
class FixtureWallet:
    name: str
    available_points: int


@dataclass(frozen=True)
class FixtureReward:
    name: str
    point_cost: int
    total_inventory: int


# One wallet and one reward per scenario, so scenarios never share a balance.
WALLETS = (
    # Saga redemption to COMPLETED, with concurrent and later replays.
    FixtureWallet("completed", 1_000),
    # Same Idempotency-Key with a different body is rejected (409).
    FixtureWallet("conflict", 1_000),
    # Insufficient points: the deduction is rejected, the Saga ends FAILED.
    FixtureWallet("insufficient", 0),
    # Exhausted inventory: deducted, reservation rejected, refunded (COMPENSATED).
    FixtureWallet("compensated", 1_000),
)

REWARDS = (
    FixtureReward("completed", point_cost=100, total_inventory=3),
    FixtureReward("conflict", point_cost=100, total_inventory=1),
    FixtureReward("insufficient", point_cost=100, total_inventory=5),
    FixtureReward("sold-out", point_cost=100, total_inventory=0),
)


def validate_run_id(run_id: str) -> str:
    if not RUN_ID_PATTERN.fullmatch(run_id):
        raise ValueError("run id must be 6 to 40 lowercase letters and digits")
    return run_id


def fixture_id(run_id: str, name: str) -> str:
    return f"smoke-{validate_run_id(run_id)}-{name}"


def wallet_rows(run_id: str) -> list[dict]:
    return [
        {"user_id": fixture_id(run_id, w.name), "available_points": w.available_points}
        for w in WALLETS
    ]


def reward_rows(run_id: str) -> list[dict]:
    return [
        {
            "reward_id": fixture_id(run_id, r.name),
            "name": f"Hosted smoke {run_id} {r.name}",
            "point_cost": r.point_cost,
            "total_inventory": r.total_inventory,
            "available_inventory": r.total_inventory,
        }
        for r in REWARDS
    ]


def rows_for(service: str, run_id: str) -> list[dict]:
    if service == "wallet":
        return wallet_rows(run_id)
    if service == "catalog":
        return reward_rows(run_id)
    raise ValueError(f"no fixtures for service {service!r}")


def require_environment() -> str:
    environment = os.environ.get("LEDGERMESH_ENV", "")
    if environment not in ALLOWED_ENVIRONMENTS:
        raise SystemExit(
            f"fixtures: refusing to run with LEDGERMESH_ENV={environment!r}; "
            f"allowed: {', '.join(sorted(ALLOWED_ENVIRONMENTS))}"
        )
    return environment


def _table_and_url(service: str):
    # Imported here so the pure helpers above work without the service code.
    if service == "wallet":
        from services.wallet.app.db.schema import wallets as table
        from services.wallet.app.repository import database_url
    else:
        from services.catalog.app.db.schema import rewards as table
        from services.catalog.app.repository import database_url
    return table, database_url()


def create(service: str, run_id: str, url: str | None = None) -> list[dict]:
    """Insert this run's rows for ``service`` in one transaction; fail if any exists."""
    require_environment()
    rows = rows_for(service, run_id)
    prefix = fixture_id(run_id, "")
    key = "user_id" if service == "wallet" else "reward_id"
    # Defence in depth: never write a row outside this run's prefix.
    if not all(row[key].startswith(prefix) for row in rows):
        raise ValueError(f"every {key} must start with {prefix}")

    from sqlalchemy import create_engine, insert

    table, default_url = _table_and_url(service)
    engine = create_engine(url or default_url)
    try:
        with engine.begin() as connection:
            connection.execute(insert(table), rows)
    finally:
        engine.dispose()
    return rows


def describe_error(service: str, run_id: str, error: Exception) -> str:
    """One line for the log: the driver's first line, never a connection URL."""
    if type(error).__name__ == "IntegrityError":
        return f"{service} rows for run {run_id} already exist; use a new run id"
    cause = getattr(error, "orig", None) or error
    first = (str(cause).strip().splitlines() or [""])[0][:300]
    return f"{service} failed: {type(error).__name__}: {first}"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Create hosted smoke fixtures for one run.")
    parser.add_argument("service", choices=SERVICES)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args(argv)
    try:
        validate_run_id(args.run_id)
    except ValueError as error:
        parser.error(str(error))
    try:
        rows = create(args.service, args.run_id)
    except Exception as error:
        print(f"fixtures: {describe_error(args.service, args.run_id, error)}", file=sys.stderr)
        return 1
    print(json.dumps({"service": args.service, "run_id": args.run_id, "created": rows}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
