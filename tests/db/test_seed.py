"""Deterministic seed data and baseline reset (LM-006).

Each database test runs against a new, empty database (see conftest.py).
"""

from uuid import uuid4

import pytest
from sqlalchemy import Engine, func, select, update

from infra.scripts import seed as seeding
from services.audit_ledger.app.db import schema as audit_ledger
from services.catalog.app.db import schema as catalog
from services.coordinator.app.db import schema as coordinator
from services.wallet.app.db import schema as wallet

from .conftest import upgrade
from .test_migrations import audit_event, redemption, run

pytestmark = pytest.mark.database

EXPECTED_BALANCES = {
    "user-alice": 10_000,
    "user-bob": 1_000,
    "user-carol": 150,
    "user-dave": 0,
}
# reward_id -> (point_cost, total_inventory, available_inventory)
EXPECTED_REWARDS = {
    "reward-coffee": (100, 100, 100),
    "reward-headphones": (2_500, 5, 5),
    "reward-last-unit": (500, 1, 1),
    "reward-sold-out": (300, 0, 0),
}


def balances(engine: Engine) -> dict[str, int]:
    with engine.connect() as connection:
        rows = connection.execute(
            select(wallet.wallets.c.user_id, wallet.wallets.c.available_points)
        )
        return dict(rows.all())


def reward_stock(engine: Engine) -> dict[str, tuple[int, int, int]]:
    columns = catalog.rewards.c
    with engine.connect() as connection:
        rows = connection.execute(
            select(
                columns.reward_id,
                columns.point_cost,
                columns.total_inventory,
                columns.available_inventory,
            )
        )
        return {reward_id: tuple(values) for reward_id, *values in rows.all()}


def count(engine: Engine, table) -> int:
    with engine.connect() as connection:
        return connection.scalar(select(func.count()).select_from(table))


# Criterion 3: stable identifiers


def test_seed_identifiers_are_stable():
    # Tests and experiments refer to these ids; renaming one must be deliberate.
    assert [w.user_id for w in seeding.WALLETS] == list(EXPECTED_BALANCES)
    assert [r.reward_id for r in seeding.REWARDS] == list(EXPECTED_REWARDS)


# Criterion 1: known balances and inventory


def test_seed_produces_the_known_wallet_balances(database_url, engine):
    upgrade("wallet", database_url)

    seeding.seed("wallet", database_url)

    assert balances(engine) == EXPECTED_BALANCES


def test_seed_produces_the_known_rewards_and_inventory(database_url, engine):
    upgrade("catalog", database_url)

    seeding.seed("catalog", database_url)

    assert reward_stock(engine) == EXPECTED_REWARDS


def test_seed_is_repeatable_and_restores_drifted_balances(database_url, engine):
    upgrade("wallet", database_url)
    seeding.seed("wallet", database_url)
    run(engine, update(wallet.wallets).values(available_points=7, version=3))

    seeding.seed("wallet", database_url)
    seeding.seed("wallet", database_url)

    assert balances(engine) == EXPECTED_BALANCES


def test_seed_restores_drifted_inventory(database_url, engine):
    upgrade("catalog", database_url)
    seeding.seed("catalog", database_url)
    run(engine, update(catalog.rewards).values(available_inventory=0))

    seeding.seed("catalog", database_url)

    assert reward_stock(engine) == EXPECTED_REWARDS


@pytest.mark.parametrize("service", ["coordinator", "audit_ledger"])
def test_seed_leaves_services_without_seed_data_empty(service, database_url, engine):
    upgrade(service, database_url)

    seeding.seed(service, database_url)

    table = coordinator.redemptions if service == "coordinator" else audit_ledger.audit_events
    assert count(engine, table) == 0


# Criterion 2: reset to a clean baseline


def test_baseline_removes_wallet_activity_and_restores_balances(database_url, engine):
    upgrade("wallet", database_url)
    seeding.seed("wallet", database_url)
    run(engine, wallet.wallets.insert().values(user_id="user-extra", available_points=5))
    run(
        engine,
        wallet.wallet_operations.insert().values(
            transaction_id=uuid4(),
            user_id="user-alice",
            operation="deduct",
            points=10,
            status="applied",
        ),
    )
    run(engine, update(wallet.wallets).values(available_points=1))

    seeding.baseline("wallet", database_url)

    assert balances(engine) == EXPECTED_BALANCES
    assert count(engine, wallet.wallet_operations) == 0


def test_baseline_removes_reservations_and_restores_inventory(database_url, engine):
    upgrade("catalog", database_url)
    seeding.seed("catalog", database_url)
    run(
        engine,
        catalog.reservations.insert().values(
            transaction_id=uuid4(), reward_id="reward-coffee", quantity=1, status="reserved"
        ),
    )
    run(engine, update(catalog.rewards).values(available_inventory=0))

    seeding.baseline("catalog", database_url)

    assert reward_stock(engine) == EXPECTED_REWARDS
    assert count(engine, catalog.reservations) == 0


def test_baseline_empties_the_coordinator(database_url, engine):
    upgrade("coordinator", database_url)
    run(engine, redemption())

    seeding.baseline("coordinator", database_url)

    assert count(engine, coordinator.redemptions) == 0


def test_baseline_empties_the_append_only_audit_ledger(database_url, engine):
    # Rows cannot be deleted one by one (LM-005 trigger); the baseline drops and
    # recreates the schema instead.
    upgrade("audit_ledger", database_url)
    run(engine, audit_event())

    seeding.baseline("audit_ledger", database_url)

    assert count(engine, audit_ledger.audit_events) == 0


# Criterion 4: refuse outside local or test


@pytest.mark.parametrize("action", ["seed", "baseline"])
@pytest.mark.parametrize("environment", [None, "", "staging", "production"])
def test_seed_and_baseline_refuse_outside_local_or_test(action, environment, monkeypatch):
    # Refused before any database connection is opened.
    if environment is None:
        monkeypatch.delenv("LEDGERMESH_ENV", raising=False)
    else:
        monkeypatch.setenv("LEDGERMESH_ENV", environment)

    with pytest.raises(SystemExit, match="LEDGERMESH_ENV"):
        seeding.main([action])


def test_unknown_service_is_rejected(monkeypatch):
    monkeypatch.setenv("LEDGERMESH_ENV", "local")

    with pytest.raises(SystemExit):
        seeding.main(["seed", "payments"])
