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
from .test_migrations import SCHEMAS, audit_event, participant, redemption, run

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
# A database URL that would fail to connect, proving the guard runs first.
UNREACHABLE_URL = "postgresql+psycopg://nobody:nothing@invalid.invalid:1/none"


@pytest.fixture(autouse=True)
def safe_environment(monkeypatch):
    # seed() and baseline() refuse outside local/test; the guard tests override this.
    monkeypatch.setenv("LEDGERMESH_ENV", "test")


def balances(engine: Engine) -> dict[str, int]:
    with engine.connect() as connection:
        rows = connection.execute(
            select(wallet.wallets.c.user_id, wallet.wallets.c.available_points)
        )
        return dict(rows.all())


def versions(engine: Engine, table) -> set[int]:
    with engine.connect() as connection:
        return set(connection.scalars(select(table.c.version)))


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


def add_messaging_rows(engine: Engine, service: str) -> None:
    tables = SCHEMAS[service].metadata.tables
    run(
        engine,
        tables["inbox"]
        .insert()
        .values(
            consumer="test",
            event_id=uuid4(),
            transaction_id=uuid4(),
            event_type="wallet.points_deducted",
        ),
    )
    if "outbox" in tables:
        run(
            engine,
            tables["outbox"]
            .insert()
            .values(event_id=uuid4(), topic="wallet.results", message_key="k", envelope={}),
        )


def assert_messaging_rows_cleared(engine: Engine, service: str) -> None:
    tables = SCHEMAS[service].metadata.tables
    for name in ("inbox", "outbox"):
        if name in tables:
            assert count(engine, tables[name]) == 0, name


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
    assert versions(engine, wallet.wallets) == {0}


def test_seed_produces_the_known_rewards_and_inventory(database_url, engine):
    upgrade("catalog", database_url)

    seeding.seed("catalog", database_url)

    assert reward_stock(engine) == EXPECTED_REWARDS
    assert versions(engine, catalog.rewards) == {0}


@pytest.mark.parametrize("service", ["coordinator", "audit_ledger"])
def test_seed_leaves_services_without_seed_data_empty(service, database_url, engine):
    upgrade(service, database_url)

    seeding.seed(service, database_url)

    table = coordinator.redemptions if service == "coordinator" else audit_ledger.audit_events
    assert count(engine, table) == 0


# db-seed is insert-only, so it can never contradict recorded activity


def test_repeated_seeding_is_idempotent(database_url, engine):
    upgrade("wallet", database_url)

    seeding.seed("wallet", database_url)
    seeding.seed("wallet", database_url)

    assert balances(engine) == EXPECTED_BALANCES
    assert count(engine, wallet.wallets) == len(EXPECTED_BALANCES)


def test_seed_adds_only_the_missing_rows(database_url, engine):
    upgrade("wallet", database_url)
    run(engine, wallet.wallets.insert().values(user_id="user-bob", available_points=5))

    seeding.seed("wallet", database_url)

    assert balances(engine) == EXPECTED_BALANCES | {"user-bob": 5}


def test_seed_does_not_overwrite_a_wallet_after_an_applied_operation(database_url, engine):
    upgrade("wallet", database_url)
    seeding.seed("wallet", database_url)
    # A 100-point deduction is applied: balance 9,900, version 1, and its row recorded.
    run(
        engine,
        wallet.wallet_operations.insert().values(
            transaction_id=uuid4(),
            user_id="user-alice",
            operation="deduct",
            points=100,
            status="applied",
        ),
    )
    run(
        engine,
        update(wallet.wallets)
        .where(wallet.wallets.c.user_id == "user-alice")
        .values(available_points=9_900, version=1),
    )

    seeding.seed("wallet", database_url)

    with engine.connect() as connection:
        alice = connection.execute(
            select(wallet.wallets.c.available_points, wallet.wallets.c.version).where(
                wallet.wallets.c.user_id == "user-alice"
            )
        ).one()
    assert tuple(alice) == (9_900, 1)


def test_seed_does_not_restore_inventory_while_a_reservation_exists(database_url, engine):
    upgrade("catalog", database_url)
    seeding.seed("catalog", database_url)
    run(
        engine,
        catalog.reservations.insert().values(
            transaction_id=uuid4(), reward_id="reward-coffee", quantity=1, status="reserved"
        ),
    )
    run(
        engine,
        update(catalog.rewards)
        .where(catalog.rewards.c.reward_id == "reward-coffee")
        .values(available_inventory=99, version=1),
    )

    seeding.seed("catalog", database_url)

    with engine.connect() as connection:
        coffee = connection.execute(
            select(catalog.rewards.c.available_inventory, catalog.rewards.c.version).where(
                catalog.rewards.c.reward_id == "reward-coffee"
            )
        ).one()
    assert tuple(coffee) == (99, 1)


# Criterion 2: db-baseline returns every service to a clean baseline


def test_baseline_restores_exact_wallet_state_and_clears_all_activity(database_url, engine):
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
    run(engine, participant("wallet", "PREPARED"))
    run(engine, update(wallet.wallets).values(available_points=1, version=4))
    add_messaging_rows(engine, "wallet")

    seeding.baseline("wallet", database_url)

    assert balances(engine) == EXPECTED_BALANCES
    assert versions(engine, wallet.wallets) == {0}
    assert count(engine, wallet.wallet_operations) == 0
    assert count(engine, wallet.participant_transactions) == 0
    assert_messaging_rows_cleared(engine, "wallet")


def test_baseline_restores_exact_catalog_state_and_clears_all_activity(database_url, engine):
    upgrade("catalog", database_url)
    seeding.seed("catalog", database_url)
    run(
        engine,
        catalog.reservations.insert().values(
            transaction_id=uuid4(), reward_id="reward-coffee", quantity=1, status="reserved"
        ),
    )
    run(engine, participant("catalog", "PREPARED"))
    run(
        engine,
        catalog.catalog_saga_outcomes.insert().values(
            transaction_id=uuid4(),
            command_type="reserve",
            reward_id="reward-coffee",
            quantity=1,
            result_event_type="catalog.inventory_reserved",
            result_payload={"outcome": "applied"},
            aggregate_version=1,
        ),
    )
    run(engine, update(catalog.rewards).values(available_inventory=0, version=2))
    add_messaging_rows(engine, "catalog")

    seeding.baseline("catalog", database_url)

    assert reward_stock(engine) == EXPECTED_REWARDS
    assert versions(engine, catalog.rewards) == {0}
    assert count(engine, catalog.reservations) == 0
    assert count(engine, catalog.participant_transactions) == 0
    assert count(engine, catalog.catalog_saga_outcomes) == 0
    assert_messaging_rows_cleared(engine, "catalog")


def test_baseline_empties_the_coordinator(database_url, engine):
    upgrade("coordinator", database_url)
    transaction_id = uuid4()
    run(engine, redemption(transaction_id=transaction_id))
    run(
        engine,
        coordinator.redemption_transitions.insert().values(
            transaction_id=transaction_id,
            from_status=None,
            to_status="REQUESTED",
            workflow_step=0,
        ),
    )
    add_messaging_rows(engine, "coordinator")

    seeding.baseline("coordinator", database_url)

    assert count(engine, coordinator.redemptions) == 0
    assert count(engine, coordinator.redemption_transitions) == 0
    assert_messaging_rows_cleared(engine, "coordinator")


def test_baseline_empties_the_append_only_audit_ledger(database_url, engine):
    # Rows cannot be deleted one by one (LM-005 trigger); the baseline drops and
    # recreates the schema instead.
    upgrade("audit_ledger", database_url)
    run(engine, audit_event())
    add_messaging_rows(engine, "audit_ledger")

    seeding.baseline("audit_ledger", database_url)

    assert count(engine, audit_ledger.audit_events) == 0
    assert_messaging_rows_cleared(engine, "audit_ledger")


# Criterion 4: refuse outside local or test, through every entry point

UNSAFE_ENVIRONMENTS = [None, "", "staging", "production"]


def set_environment(monkeypatch, environment):
    if environment is None:
        monkeypatch.delenv("LEDGERMESH_ENV", raising=False)
    else:
        monkeypatch.setenv("LEDGERMESH_ENV", environment)


@pytest.mark.parametrize("action", ["seed", "baseline"])
@pytest.mark.parametrize("environment", UNSAFE_ENVIRONMENTS)
def test_cli_refuses_outside_local_or_test(action, environment, monkeypatch):
    set_environment(monkeypatch, environment)

    with pytest.raises(SystemExit, match="LEDGERMESH_ENV"):
        seeding.main([action])


@pytest.mark.parametrize("function", [seeding.seed, seeding.baseline], ids=["seed", "baseline"])
@pytest.mark.parametrize("environment", UNSAFE_ENVIRONMENTS)
def test_direct_calls_refuse_outside_local_or_test(function, environment, monkeypatch):
    # The public functions are guarded too, so importing them cannot bypass the
    # check. The unreachable URL proves the guard runs before any connection.
    set_environment(monkeypatch, environment)

    with pytest.raises(SystemExit, match="LEDGERMESH_ENV"):
        function("wallet", UNREACHABLE_URL)


def test_unknown_service_is_rejected():
    with pytest.raises(SystemExit):
        seeding.main(["seed", "payments"])
