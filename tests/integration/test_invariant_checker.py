"""LM-031 regression tests for the cross-service invariant checker."""

from __future__ import annotations

from uuid import uuid4

import pytest
from sqlalchemy import insert, text, update

from services.catalog.app.db.schema import reservations, rewards
from services.coordinator.app.db.schema import redemption_transitions, redemptions
from services.coordinator.app.models import CreateRedemptionRequest
from services.coordinator.app.repository import PostgresCoordinatorRepository
from services.wallet.app.db.schema import wallet_operations, wallets
from tests.integration.conftest import ServiceDatabases
from tests.integration.invariants import check_project_invariants

pytestmark = [
    pytest.mark.database,
    pytest.mark.integration,
    pytest.mark.invariant_checker,
]


def make_redemption(databases: ServiceDatabases, mode: str = "saga"):
    return PostgresCoordinatorRepository(databases.engines["coordinator"]).create(
        f"lm031-{uuid4()}",
        CreateRedemptionRequest(
            user_id="user-lm031",
            reward_id="reward-lm031",
            quantity=1,
            coordination_mode=mode,
        ),
    )


def test_checker_detects_negative_wallet_and_out_of_range_inventory(service_databases):
    wallet = service_databases.engines["wallet"]
    catalog = service_databases.engines["catalog"]
    with wallet.begin() as connection:
        connection.execute(
            text("ALTER TABLE wallets DROP CONSTRAINT ck_wallets_available_points_non_negative")
        )
        connection.execute(insert(wallets).values(user_id="bad-balance", available_points=-1))
    with catalog.begin() as connection:
        connection.execute(
            text("ALTER TABLE rewards DROP CONSTRAINT ck_rewards_available_within_total")
        )
        connection.execute(
            insert(rewards).values(
                reward_id="bad-inventory",
                name="Invalid test reward",
                point_cost=100,
                total_inventory=1,
                available_inventory=2,
            )
        )

    report = check_project_invariants(service_databases.engines)

    assert any(item.startswith("INV-01:") for item in report.violations)
    assert any(item.startswith("INV-02:") for item in report.violations)


def test_checker_detects_duplicate_terminal_transitions(service_databases):
    created = make_redemption(service_databases)
    coordinator = service_databases.engines["coordinator"]
    with coordinator.begin() as connection:
        connection.execute(
            insert(redemption_transitions),
            [
                {
                    "transaction_id": created.transaction_id,
                    "from_status": "RESERVING_INVENTORY",
                    "to_status": "COMPLETED",
                    "workflow_step": 4,
                    "reason": "first terminal outcome",
                },
                {
                    "transaction_id": created.transaction_id,
                    "from_status": "RESERVING_INVENTORY",
                    "to_status": "FAILED",
                    "workflow_step": 5,
                    "reason": "conflicting terminal outcome",
                },
            ],
        )

    report = check_project_invariants(service_databases.engines)

    assert any(item.startswith("INV-04:") for item in report.violations)


def test_checker_detects_success_without_deduction_or_reservation(service_databases):
    created = make_redemption(service_databases)
    with service_databases.engines["coordinator"].begin() as connection:
        connection.execute(
            update(redemptions)
            .where(redemptions.c.transaction_id == created.transaction_id)
            .values(status="COMPLETED")
        )

    report = check_project_invariants(service_databases.engines)

    assert any(item.startswith("INV-05:") for item in report.violations)


def test_checker_detects_failed_saga_with_uncompensated_effects(service_databases):
    created = make_redemption(service_databases)
    with service_databases.engines["coordinator"].begin() as connection:
        connection.execute(
            update(redemptions)
            .where(redemptions.c.transaction_id == created.transaction_id)
            .values(status="FAILED")
        )
    with service_databases.engines["wallet"].begin() as connection:
        connection.execute(insert(wallets).values(user_id="user-lm031", available_points=900))
        connection.execute(
            insert(wallet_operations).values(
                transaction_id=created.transaction_id,
                user_id="user-lm031",
                operation="deduct",
                points=100,
                status="applied",
            )
        )
    with service_databases.engines["catalog"].begin() as connection:
        connection.execute(
            insert(rewards).values(
                reward_id="reward-lm031",
                name="Test reward",
                point_cost=100,
                total_inventory=10,
                available_inventory=9,
            )
        )
        connection.execute(
            insert(reservations).values(
                transaction_id=created.transaction_id,
                reward_id="reward-lm031",
                quantity=1,
                status="reserved",
            )
        )

    report = check_project_invariants(service_databases.engines)

    assert any(item.startswith("INV-06:") for item in report.violations)
