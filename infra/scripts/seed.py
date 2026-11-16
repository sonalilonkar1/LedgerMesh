"""Deterministic seed data and baseline reset (LM-006).

Usage, from the repository root:

    LEDGERMESH_ENV=local uv run python -m infra.scripts.seed seed
    LEDGERMESH_ENV=local uv run python -m infra.scripts.seed baseline

``seed`` writes the fixed users and rewards below, overwriting any drifted
balance or inventory back to its baseline value. It is safe to run repeatedly.

``baseline`` wipes every service database (migrations down and up again) and
then seeds it, so demos and experiments always start from the same state:
no redemptions, operations, reservations, audit events, inbox or outbox rows.

Both change data, so both refuse to run unless ``LEDGERMESH_ENV`` is ``local``
or ``test``. The identifiers are part of the test contract: automated tests and
experiments refer to them, so change them only together with those tests.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass

from alembic import command
from sqlalchemy import create_engine, func
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

# Only Wallet and Catalog own seeded business data; the Coordinator and Audit
# Ledger start empty in the baseline.
SEEDED_SERVICES = ("wallet", "catalog")


def seed_wallet(url: str) -> None:
    statement = insert(wallets).values(
        [{"user_id": w.user_id, "available_points": w.available_points} for w in WALLETS]
    )
    statement = statement.on_conflict_do_update(
        index_elements=[wallets.c.user_id],
        set_={
            "available_points": statement.excluded.available_points,
            "version": 0,
            "updated_at": func.now(),
        },
    )
    _execute(url, statement)


def seed_catalog(url: str) -> None:
    statement = insert(rewards).values(
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
    statement = statement.on_conflict_do_update(
        index_elements=[rewards.c.reward_id],
        set_={
            "name": statement.excluded.name,
            "point_cost": statement.excluded.point_cost,
            "total_inventory": statement.excluded.total_inventory,
            "available_inventory": statement.excluded.available_inventory,
            "version": 0,
            "updated_at": func.now(),
        },
    )
    _execute(url, statement)


SEEDERS = {"wallet": seed_wallet, "catalog": seed_catalog}


def _execute(url: str, statement) -> None:
    engine = create_engine(url)
    try:
        with engine.begin() as connection:
            connection.execute(statement)
    finally:
        engine.dispose()


def seed(service: str, url: str | None = None) -> None:
    """Write the baseline rows for ``service``; services without seed data are skipped."""
    seeder = SEEDERS.get(service)
    if seeder is not None:
        seeder(url or database_url(service))


def baseline(service: str, url: str | None = None) -> None:
    """Wipe ``service``'s database back to an empty schema, then seed it."""
    config = alembic_config(service, url)
    command.downgrade(config, "base")
    command.upgrade(config, "head")
    seed(service, url)


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
