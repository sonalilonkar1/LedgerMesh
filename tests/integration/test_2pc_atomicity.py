"""LM-017: real PostgreSQL atomicity, rejection and blocking experiments."""

from __future__ import annotations

import time
from dataclasses import dataclass
from uuid import UUID, uuid4

import pytest
from ledgermesh_messaging import EventEnvelope
from ledgermesh_messaging.schema import outbox
from sqlalchemy import Engine, insert, select, text
from sqlalchemy.exc import DBAPIError

from services.audit_ledger.app.repository import PostgresAuditRepository
from services.catalog.app.db.schema import reservations, rewards
from services.catalog.app.participant import PostgresCatalogParticipantRepository
from services.coordinator.app.models import CreateRedemptionRequest
from services.coordinator.app.repository import PostgresCoordinatorRepository
from services.coordinator.app.two_phase_commit import (
    ParticipantVote,
    RewardQuote,
    TwoPhaseCommitWorkflow,
)
from services.wallet.app.db.schema import wallet_operations, wallets
from services.wallet.app.participant import PostgresWalletParticipantRepository
from tests.integration.invariants import check_2pc_invariants

pytestmark = [pytest.mark.database, pytest.mark.integration]


class WalletAdapter:
    def __init__(self, repository: PostgresWalletParticipantRepository) -> None:
        self.repository = repository

    def prepare(self, record) -> ParticipantVote:
        assert record.total_points is not None
        result = self.repository.prepare(
            record.transaction_id,
            record.correlation_id,
            record.user_id,
            record.total_points,
        )
        return ParticipantVote(result.state)

    def commit(self, transaction_id: UUID) -> None:
        self.repository.commit(transaction_id)

    def abort(self, transaction_id: UUID) -> None:
        self.repository.abort(transaction_id)


class CatalogAdapter:
    def __init__(self, repository: PostgresCatalogParticipantRepository, engine: Engine) -> None:
        self.repository = repository
        self.engine = engine

    def quote(self, reward_id: str) -> RewardQuote:
        with self.engine.connect() as connection:
            row = connection.execute(
                select(rewards.c.point_cost, rewards.c.version).where(
                    rewards.c.reward_id == reward_id
                )
            ).one()
        return RewardQuote(row.point_cost, row.version)

    def prepare(self, record) -> ParticipantVote:
        assert record.reward_version is not None
        assert record.point_cost is not None
        result = self.repository.prepare(
            record.transaction_id,
            record.correlation_id,
            record.reward_id,
            record.quantity,
            record.reward_version,
            record.point_cost,
        )
        return ParticipantVote(result.state)

    def commit(self, transaction_id: UUID) -> None:
        self.repository.commit(transaction_id)

    def abort(self, transaction_id: UUID) -> None:
        self.repository.abort(transaction_id)


@dataclass
class Harness:
    engines: dict[str, Engine]
    coordinator: PostgresCoordinatorRepository
    wallet: PostgresWalletParticipantRepository
    catalog: PostgresCatalogParticipantRepository
    audit: PostgresAuditRepository
    workflow: TwoPhaseCommitWorkflow

    def create(self, *, user_id: str = "user-funded", reward_id: str = "reward-stocked"):
        return self.coordinator.create(
            f"lm017-{uuid4()}",
            CreateRedemptionRequest(
                user_id=user_id,
                reward_id=reward_id,
                quantity=1,
                coordination_mode="two_phase_commit",
            ),
        )


@pytest.fixture
def harness(service_databases) -> Harness:
    engines = service_databases.engines
    with engines["wallet"].begin() as connection:
        connection.execute(
            insert(wallets),
            [
                {"user_id": "user-funded", "available_points": 1_000},
                {"user_id": "user-empty", "available_points": 0},
            ],
        )
    with engines["catalog"].begin() as connection:
        connection.execute(
            insert(rewards),
            [
                {
                    "reward_id": "reward-stocked",
                    "name": "Stocked reward",
                    "point_cost": 100,
                    "total_inventory": 10,
                    "available_inventory": 10,
                },
                {
                    "reward_id": "reward-sold-out",
                    "name": "Sold-out reward",
                    "point_cost": 100,
                    "total_inventory": 0,
                    "available_inventory": 0,
                },
            ],
        )

    coordinator = PostgresCoordinatorRepository(engines["coordinator"])
    wallet = PostgresWalletParticipantRepository(engines["wallet"])
    catalog = PostgresCatalogParticipantRepository(engines["catalog"])
    audit = PostgresAuditRepository(engines["audit_ledger"])
    workflow = TwoPhaseCommitWorkflow(
        coordinator,
        WalletAdapter(wallet),
        CatalogAdapter(catalog, engines["catalog"]),
    )
    return Harness(engines, coordinator, wallet, catalog, audit, workflow)


def assert_zero_violations(harness: Harness, lm017_report, scenario: str) -> None:
    report = check_2pc_invariants(harness.engines)
    lm017_report["scenarios"].append(scenario)
    lm017_report["invariant_violations"].append(
        {"scenario": scenario, "count": report.count, "details": list(report.violations)}
    )
    assert report.count == 0, "\n".join(report.violations)


def deliver_audit_outbox(harness: Harness, transaction_id: UUID) -> None:
    """Model the outbox/Kafka/audit-consumer boundary without a Kafka fixture."""
    with harness.engines["coordinator"].connect() as connection:
        document = connection.scalar(
            select(outbox.c.envelope).where(
                outbox.c.topic == "redemption.events",
                outbox.c.message_key == str(transaction_id),
            )
        )
    assert document is not None
    harness.audit.append(EventEnvelope.model_validate(document))


def test_happy_path_commits_wallet_inventory_coordinator_and_audit(harness: Harness, lm017_report):
    created = harness.create()

    result = harness.workflow.execute(created.transaction_id)
    deliver_audit_outbox(harness, created.transaction_id)

    assert result.status == "COMMITTED"
    with harness.engines["wallet"].connect() as connection:
        assert (
            connection.scalar(
                select(wallets.c.available_points).where(wallets.c.user_id == "user-funded")
            )
            == 900
        )
        assert (
            connection.scalar(
                select(wallet_operations.c.transaction_id).where(
                    wallet_operations.c.transaction_id == created.transaction_id
                )
            )
            == created.transaction_id
        )
    with harness.engines["catalog"].connect() as connection:
        assert (
            connection.scalar(
                select(rewards.c.available_inventory).where(rewards.c.reward_id == "reward-stocked")
            )
            == 9
        )
        assert (
            connection.scalar(
                select(reservations.c.transaction_id).where(
                    reservations.c.transaction_id == created.transaction_id
                )
            )
            == created.transaction_id
        )
    events = harness.audit.list_by_transaction(created.transaction_id)
    assert [event.envelope["event_type"] for event in events] == ["redemption.committed"]
    assert_zero_violations(harness, lm017_report, "happy_path_commit")


@pytest.mark.parametrize(
    ("user_id", "reward_id"),
    [("user-empty", "reward-stocked"), ("user-funded", "reward-sold-out")],
)
def test_participant_rejection_aborts_every_prepared_change(
    harness: Harness, user_id: str, reward_id: str, lm017_report
):
    created = harness.create(user_id=user_id, reward_id=reward_id)

    result = harness.workflow.execute(created.transaction_id)

    assert result.status == "ABORTED"
    with harness.engines["wallet"].connect() as connection:
        assert (
            connection.scalar(
                select(wallet_operations.c.id).where(
                    wallet_operations.c.transaction_id == created.transaction_id
                )
            )
            is None
        )
    with harness.engines["catalog"].connect() as connection:
        assert (
            connection.scalar(
                select(reservations.c.id).where(
                    reservations.c.transaction_id == created.transaction_id
                )
            )
            is None
        )
    assert_zero_violations(harness, lm017_report, f"rejection:{user_id}:{reward_id}")


def test_coordinator_crash_measures_prepared_lock_blocking_and_recovers(
    harness: Harness, lm017_report
):
    created = harness.create()
    harness.coordinator.transition(created.transaction_id, "PREPARING", "inject crash")
    vote = harness.wallet.prepare(
        created.transaction_id,
        created.correlation_id,
        "user-funded",
        100,
    )
    assert vote.state == "PREPARED"
    prepared_at = time.monotonic()

    probe_started = time.monotonic()
    with pytest.raises(DBAPIError), harness.engines["wallet"].begin() as connection:
        connection.execute(text("SET LOCAL statement_timeout = '700ms'"))
        connection.execute(
            text(
                "UPDATE wallets SET available_points = available_points + 1 "
                "WHERE user_id = 'user-funded'"
            )
        )
    probe_timeout_seconds = time.monotonic() - probe_started
    lm017_report["prepared_lock_probe_timeout_seconds"] = round(probe_timeout_seconds, 3)
    assert 0.5 <= probe_timeout_seconds < 3.0

    restarted = PostgresCoordinatorRepository(harness.engines["coordinator"])
    recovered = TwoPhaseCommitWorkflow(
        restarted,
        WalletAdapter(harness.wallet),
        CatalogAdapter(harness.catalog, harness.engines["catalog"]),
    ).recover(created.transaction_id)

    assert recovered.status == "ABORTED"
    with harness.engines["wallet"].begin() as connection:
        connection.execute(text("SET LOCAL statement_timeout = '700ms'"))
        connection.execute(
            text(
                "UPDATE wallets SET available_points = available_points + 1 "
                "WHERE user_id = 'user-funded'"
            )
        )
    lock_duration_seconds = time.monotonic() - prepared_at
    lm017_report["prepared_lock_duration_seconds"] = round(lock_duration_seconds, 3)
    assert lock_duration_seconds >= probe_timeout_seconds
    assert lock_duration_seconds < 10.0
    assert_zero_violations(harness, lm017_report, "coordinator_crash_recovery")
