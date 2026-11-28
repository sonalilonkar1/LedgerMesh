"""Deterministic seed data and baseline reset (LM-006).

Usage, from the repository root:

    LEDGERMESH_ENV=local uv run python -m infra.scripts.seed seed
    LEDGERMESH_ENV=local uv run python -m infra.scripts.seed baseline

``seed`` is insert-only: it adds any missing seed user or reward and never
changes an existing row. Overwriting a balance or inventory that already has
operations or reservations behind it would leave unmatched deductions, ghost
allocations and a regressed aggregate version, so it is safe to run repeatedly.

``baseline`` is the only way to restore exact values: it wipes every service
database (migrations down and up again) and then seeds it, so demos and
experiments start from the same state with no redemptions, transitions,
operations, reservations, participant rows, audit events, inbox or outbox rows.

Both change data, so the public ``seed()`` and ``baseline()`` functions, not
just the CLI, refuse to run unless ``LEDGERMESH_ENV`` is ``local`` or ``test``.
The identifiers are part of the test contract: automated tests and experiments
refer to them, so change them only together with those tests.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass

from alembic import command
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert

from infra.scripts.db import SERVICES, alembic_config, database_url, require_safe_environment
from services.catalog.app.db.schema import rewards
from services.wallet.app.db.schema import wallets


@dataclass(frozen=True)
class SeedWallet:
    user_id: str
    available_points: int


@dataclass(frozen=True)
class SeedReward:
    reward_id: str
    name: str
    point_cost: int
    total_inventory: int


# Each entry exists for a specific kind of test or experiment.
WALLETS = (
    # Plenty of points: happy paths and throughput experiments.
    SeedWallet("user-alice", 10_000),
    SeedWallet("user-bob", 1_000),
    # Enough for exactly one coffee: competing redemptions for one balance (LM-045).
    SeedWallet("user-carol", 150),
    # No points: insufficient-balance rejection.
    SeedWallet("user-dave", 0),
)

REWARDS = (
    SeedReward("reward-coffee", "Coffee voucher", point_cost=100, total_inventory=100),
    SeedReward("reward-headphones", "Headphones", point_cost=2_500, total_inventory=5),
    # One unit left: competing redemptions for the final inventory unit (LM-045).
    SeedReward("reward-last-unit", "Concert ticket", point_cost=500, total_inventory=1),
    # Nothing left: sold-out rejection.
    SeedReward("reward-sold-out", "Limited poster", point_cost=300, total_inventory=0),
)

# Only Wallet and Catalog own seeded business data; the Coordinator, Audit
# Ledger and Identity start empty in the baseline.
SEEDED_SERVICES = ("wallet", "catalog")


def _seed_wallet(url: str) -> None:
    # Insert-only: an existing wallet keeps its balance and version, because
    # wallet_operations may already account for the difference.
    statement = (
        insert(wallets)
        .values([{"user_id": w.user_id, "available_points": w.available_points} for w in WALLETS])
        .on_conflict_do_nothing(index_elements=[wallets.c.user_id])
    )
    _execute(url, statement)


def _seed_catalog(url: str) -> None:
    # Insert-only: an existing reward keeps its inventory and version, because
    # reservations may already account for the difference.
    statement = (
        insert(rewards)
        .values(
            [
                {
                    "reward_id": r.reward_id,
                    "name": r.name,
                    "point_cost": r.point_cost,
                    "total_inventory": r.total_inventory,
                    "available_inventory": r.total_inventory,
                }
                for r in REWARDS
            ]
        )
        .on_conflict_do_nothing(index_elements=[rewards.c.reward_id])
    )
    _execute(url, statement)


SEEDERS = {"wallet": _seed_wallet, "catalog": _seed_catalog}


def _execute(url: str, statement) -> None:
    engine = create_engine(url)
    try:
        with engine.begin() as connection:
            connection.execute(statement)
    finally:
        engine.dispose()


def _seed_service(service: str, url: str | None) -> None:
    seeder = SEEDERS.get(service)
    if seeder is not None:
        seeder(url or database_url(service))


def _baseline_service(service: str, url: str | None) -> None:
    config = alembic_config(service, url)
    command.downgrade(config, "base")
    command.upgrade(config, "head")
    _seed_service(service, url)


def seed(service: str, url: str | None = None) -> None:
    """Add any missing baseline rows for ``service``; never changes existing rows.

    Refuses unless ``LEDGERMESH_ENV`` is ``local`` or ``test``, even when called
    directly instead of through the CLI.
    """
    require_safe_environment("seed")
    _seed_service(service, url)


def baseline(service: str, url: str | None = None) -> None:
    """Wipe ``service``'s database back to an empty schema, then seed it.

    Refuses unless ``LEDGERMESH_ENV`` is ``local`` or ``test``, even when called
    directly instead of through the CLI.
    """
    require_safe_environment("baseline")
    _baseline_service(service, url)


def run(action: str, services: list[str]) -> None:
    require_safe_environment(action)
    for service in services:
        print(f"{action:<9} {service}")
        if action == "baseline":
            baseline(service)
        else:
            seed(service)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("action", choices=["seed", "baseline"])
    parser.add_argument("services", nargs="*", metavar="service", help=", ".join(SERVICES))
    args = parser.parse_args(argv)
    unknown = sorted(set(args.services) - set(SERVICES))
    if unknown:
        parser.error(f"unknown service(s): {', '.join(unknown)}")
    run(args.action, args.services or list(SERVICES))


if __name__ == "__main__":
    main()
