"""Migrations and database-enforced safety rules for every service (LM-005).

Each test runs against a new, empty database (see conftest.py).
"""

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from sqlalchemy import Engine, delete, inspect, text, update
from sqlalchemy.exc import IntegrityError

from services.audit_ledger.app.db import schema as audit_ledger
from services.catalog.app.db import schema as catalog
from services.coordinator.app.db import schema as coordinator
from services.wallet.app.db import schema as wallet

from .conftest import SERVICES, downgrade, upgrade

pytestmark = pytest.mark.database

SCHEMAS = {
    "coordinator": coordinator,
    "wallet": wallet,
    "catalog": catalog,
    "audit_ledger": audit_ledger,
}
EXPECTED_TABLES = {
    "coordinator": {"redemptions", "redemption_transitions", "inbox", "outbox"},
    "wallet": {"wallets", "wallet_operations", "participant_transactions", "inbox", "outbox"},
    "catalog": {"rewards", "reservations", "participant_transactions", "inbox", "outbox"},
    "audit_ledger": {"audit_events", "inbox"},
}


def tables(engine: Engine) -> set[str]:
    return set(inspect(engine).get_table_names()) - {"alembic_version"}


def run(engine: Engine, statement) -> None:
    with engine.begin() as connection:
        connection.execute(statement)


def assert_rejected(engine: Engine, statement) -> None:
    with pytest.raises(IntegrityError), engine.begin() as connection:
        connection.execute(statement)


# Every service


@pytest.mark.parametrize("service", SERVICES)
def test_migrations_create_the_service_tables_on_an_empty_database(service, database_url, engine):
    upgrade(service, database_url)

    assert tables(engine) == EXPECTED_TABLES[service]


@pytest.mark.parametrize("service", SERVICES)
def test_schema_module_matches_the_migrations(service, database_url, engine):
    # Fails when a column is added to app/db/schema.py without a migration, or
    # the other way round.
    upgrade(service, database_url)

    with engine.connect() as connection:
        context = MigrationContext.configure(connection, opts={"compare_type": True})
        differences = compare_metadata(context, SCHEMAS[service].metadata)

    assert differences == []


@pytest.mark.parametrize("service", SERVICES)
def test_downgrade_removes_everything_and_upgrade_runs_again(service, database_url, engine):
    upgrade(service, database_url)
    downgrade(service, database_url)

    assert tables(engine) == set()

    upgrade(service, database_url)
    assert tables(engine) == EXPECTED_TABLES[service]


def test_services_do_not_share_tables_except_the_messaging_ones():
    # No service owns another service's business tables (ADR-001 section 2).
    shared = {"inbox", "outbox", "participant_transactions"}
    business = {service: names - shared for service, names in EXPECTED_TABLES.items()}
    for service, owned in business.items():
        for other, other_owned in business.items():
            if other != service:
                assert not owned & other_owned, (service, other)


# Wallet: INV-01 and INV-03


@pytest.fixture
def wallet_db(database_url, engine) -> Engine:
    upgrade("wallet", database_url)
    run(engine, wallet.wallets.insert().values(user_id="user-1", available_points=100))
    return engine


def wallet_operation(**overrides):
    values = dict(
        transaction_id=uuid4(), user_id="user-1", operation="deduct", points=10, status="applied"
    )
    return wallet.wallet_operations.insert().values(**(values | overrides))


def test_wallet_balance_cannot_go_negative(wallet_db):
    balance = wallet.wallets.c.available_points

    assert_rejected(wallet_db, update(wallet.wallets).values(available_points=balance - 101))
    run(wallet_db, update(wallet.wallets).values(available_points=balance - 100))


def test_wallet_allows_one_deduction_and_one_refund_per_transaction(wallet_db):
    transaction_id = uuid4()
    run(wallet_db, wallet_operation(transaction_id=transaction_id, operation="deduct"))
    run(wallet_db, wallet_operation(transaction_id=transaction_id, operation="refund"))

    assert_rejected(wallet_db, wallet_operation(transaction_id=transaction_id))


def test_early_refund_creates_a_deduction_tombstone_that_blocks_the_late_deduction(wallet_db):
    # ADR-001 7.1: the refund arrived first, so it stored a tombstone for the
    # deduction it cancels; the delayed deduction then hits the same unique key.
    transaction_id = uuid4()
    run(
        wallet_db,
        wallet_operation(transaction_id=transaction_id, operation="deduct", status="tombstone"),
    )

    assert_rejected(wallet_db, wallet_operation(transaction_id=transaction_id))


def test_wallet_rejects_a_refund_tombstone_because_it_would_block_nothing(wallet_db):
    assert_rejected(wallet_db, wallet_operation(operation="refund", status="tombstone"))


@pytest.mark.parametrize(
    "overrides",
    [{"points": 0}, {"operation": "transfer"}, {"status": "pending"}],
    ids=["zero-points", "unknown-operation", "unknown-status"],
)
def test_wallet_rejects_invalid_operations(wallet_db, overrides):
    assert_rejected(wallet_db, wallet_operation(**overrides))


# 2PC participants (Wallet and Catalog): stored request identity

PARTICIPANT_REQUEST = {
    "wallet": {"user_id": "user-1", "points": 10},
    "catalog": {"reward_id": "reward-1", "quantity": 1},
}
NO_REQUEST = {"correlation_id": None, "request_hash": None}


def participant(service: str, state: str, **overrides):
    values = {
        "transaction_id": uuid4(),
        "state": state,
        "correlation_id": uuid4(),
        "request_hash": "hash",
        **PARTICIPANT_REQUEST[service],
    }
    table = SCHEMAS[service].participant_transactions
    return table.insert().values(**(values | overrides))


def empty_request(service: str) -> dict:
    return NO_REQUEST | dict.fromkeys(PARTICIPANT_REQUEST[service])


@pytest.mark.parametrize("service", ["wallet", "catalog"])
@pytest.mark.parametrize("state", ["PREPARED", "COMMITTED", "IN_DOUBT", "ABORTED"])
def test_participant_row_with_the_full_request_is_accepted(service, state, database_url, engine):
    upgrade(service, database_url)

    run(engine, participant(service, state))


@pytest.mark.parametrize("service", ["wallet", "catalog"])
def test_empty_abort_tombstone_is_accepted(service, database_url, engine):
    upgrade(service, database_url)

    run(engine, participant(service, "ABORTED", **empty_request(service)))


@pytest.mark.parametrize("service", ["wallet", "catalog"])
@pytest.mark.parametrize(
    ("state", "missing"),
    [
        ("PREPARED", "request_hash"),
        ("PREPARED", "correlation_id"),
        ("PREPARED", "business"),
        ("COMMITTED", "request_hash"),
        ("IN_DOUBT", "correlation_id"),
        ("ABORTED", "request_hash"),
        ("ABORTED", "business"),
    ],
)
def test_participant_row_with_a_partial_request_is_rejected(
    service, state, missing, database_url, engine
):
    # Only the fully empty ABORTED tombstone may omit request fields; anything
    # that was prepared must keep its whole request identity to detect reuse.
    upgrade(service, database_url)
    if missing == "business":
        cleared = dict.fromkeys(PARTICIPANT_REQUEST[service])
    else:
        cleared = {missing: None}

    assert_rejected(engine, participant(service, state, **cleared))


# Catalog: INV-02 and INV-03


@pytest.fixture
def catalog_db(database_url, engine) -> Engine:
    upgrade("catalog", database_url)
    run(
        engine,
        catalog.rewards.insert().values(
            reward_id="reward-1",
            name="Coffee",
            point_cost=500,
            total_inventory=1,
            available_inventory=1,
        ),
    )
    return engine


def test_catalog_inventory_cannot_be_oversold(catalog_db):
    available = catalog.rewards.c.available_inventory

    run(catalog_db, update(catalog.rewards).values(available_inventory=available - 1))
    assert_rejected(catalog_db, update(catalog.rewards).values(available_inventory=available - 1))


def test_catalog_available_inventory_cannot_exceed_total(catalog_db):
    assert_rejected(catalog_db, update(catalog.rewards).values(available_inventory=2))


def test_catalog_reward_must_cost_points(catalog_db):
    assert_rejected(catalog_db, update(catalog.rewards).values(point_cost=0))


def reservation(transaction_id, reward_id="reward-1"):
    return catalog.reservations.insert().values(
        transaction_id=transaction_id, reward_id=reward_id, quantity=1, status="reserved"
    )


def test_catalog_transaction_cannot_reserve_the_same_reward_twice(catalog_db):
    transaction_id = uuid4()
    run(catalog_db, reservation(transaction_id))

    assert_rejected(catalog_db, reservation(transaction_id))


def test_catalog_transaction_cannot_reserve_a_different_reward(catalog_db):
    # A redemption has one reward; a reused transaction id must not allocate a second.
    run(
        catalog_db,
        catalog.rewards.insert().values(
            reward_id="reward-2",
            name="Tea",
            point_cost=300,
            total_inventory=1,
            available_inventory=1,
        ),
    )
    transaction_id = uuid4()
    run(catalog_db, reservation(transaction_id, "reward-1"))

    assert_rejected(catalog_db, reservation(transaction_id, "reward-2"))


def test_catalog_different_transactions_can_reserve_the_same_reward(catalog_db):
    run(catalog_db, reservation(uuid4()))
    run(catalog_db, reservation(uuid4()))


def test_catalog_reservation_needs_a_positive_quantity(catalog_db):
    assert_rejected(
        catalog_db,
        catalog.reservations.insert().values(
            transaction_id=uuid4(), reward_id="reward-1", quantity=0, status="reserved"
        ),
    )


# Coordinator: idempotency and valid states


def redemption(**overrides):
    values = dict(
        transaction_id=uuid4(),
        correlation_id=uuid4(),
        idempotency_key=f"key-{uuid4()}",
        request_hash="hash",
        user_id="user-1",
        reward_id="reward-1",
        quantity=1,
        coordination_mode="saga",
        status="REQUESTED",
    )
    return coordinator.redemptions.insert().values(**(values | overrides))


@pytest.fixture
def coordinator_db(database_url, engine) -> Engine:
    upgrade("coordinator", database_url)
    return engine


def test_coordinator_idempotency_key_creates_one_redemption(coordinator_db):
    run(coordinator_db, redemption(idempotency_key="client-key-1"))

    assert_rejected(coordinator_db, redemption(idempotency_key="client-key-1"))


@pytest.mark.parametrize("status", coordinator.TWO_PHASE_COMMIT_STATES)
def test_coordinator_accepts_every_2pc_state(coordinator_db, status):
    run(coordinator_db, redemption(coordination_mode="two_phase_commit", status=status))


@pytest.mark.parametrize("status", coordinator.SAGA_STATES)
def test_coordinator_accepts_every_saga_state(coordinator_db, status):
    run(coordinator_db, redemption(coordination_mode="saga", status=status))


@pytest.mark.parametrize(
    ("mode", "status"),
    [
        ("two_phase_commit", "COMPLETED"),
        ("saga", "COMMIT_DECIDED"),
        ("saga", "COMPENSATION_FAILED"),
        ("eventual", "REQUESTED"),
    ],
)
def test_coordinator_rejects_a_state_from_the_wrong_mode(coordinator_db, mode, status):
    assert_rejected(coordinator_db, redemption(coordination_mode=mode, status=status))


# Audit Ledger: append-only and idempotent


def audit_event(**overrides):
    values = dict(
        event_id=uuid4(),
        transaction_id=uuid4(),
        correlation_id=uuid4(),
        event_type="wallet.points_deducted",
        aggregate_type="wallet",
        aggregate_id="user-1",
        occurred_at=datetime.now(UTC),
        envelope={},
    )
    return audit_ledger.audit_events.insert().values(**(values | overrides))


@pytest.fixture
def audit_db(database_url, engine) -> Engine:
    upgrade("audit_ledger", database_url)
    run(engine, audit_event())
    return engine


def test_audit_event_is_recorded_once(audit_db):
    event_id = uuid4()
    run(audit_db, audit_event(event_id=event_id))

    assert_rejected(audit_db, audit_event(event_id=event_id))


@pytest.mark.parametrize(
    "statement",
    [
        update(audit_ledger.audit_events).values(event_type="wallet.points_refunded"),
        delete(audit_ledger.audit_events),
        text("TRUNCATE audit_events"),
    ],
    ids=["update", "delete", "truncate"],
)
def test_audit_events_cannot_be_changed(audit_db, statement):
    assert_rejected(audit_db, statement)


def test_downgrade_removes_the_audit_trigger_function(database_url, engine):
    upgrade("audit_ledger", database_url)
    downgrade("audit_ledger", database_url)

    with engine.connect() as connection:
        remaining = connection.scalar(
            text("SELECT count(*) FROM pg_proc WHERE proname = 'audit_events_reject_change'")
        )
    assert remaining == 0
