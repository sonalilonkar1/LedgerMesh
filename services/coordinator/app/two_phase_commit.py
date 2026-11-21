"""Durable Two-Phase Commit orchestration for redemptions (LM-015)."""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Protocol
from uuid import UUID

import httpx
from ledgermesh_messaging import EventEnvelope

from services.coordinator.app.errors import InvalidTransition
from services.coordinator.app.repository import PostgresCoordinatorRepository, WorkflowRecord


@dataclass(frozen=True)
class ParticipantVote:
    state: str


@dataclass(frozen=True)
class RewardQuote:
    point_cost: int
    version: int


class WalletParticipant(Protocol):
    def prepare(self, record: WorkflowRecord) -> ParticipantVote: ...

    def commit(self, transaction_id: UUID) -> None: ...

    def abort(self, transaction_id: UUID) -> None: ...


class CatalogParticipant(Protocol):
    def quote(self, reward_id: str) -> RewardQuote: ...

    def prepare(self, record: WorkflowRecord) -> ParticipantVote: ...

    def commit(self, transaction_id: UUID) -> None: ...

    def abort(self, transaction_id: UUID) -> None: ...


class AuditSink(Protocol):
    def append(self, envelope: EventEnvelope) -> None: ...


class ParticipantUnavailable(RuntimeError):
    pass


class HttpWalletParticipant:
    def __init__(self, base_url: str, timeout: float = 2.0) -> None:
        self.client = httpx.Client(base_url=base_url, timeout=timeout)

    def prepare(self, record: WorkflowRecord) -> ParticipantVote:
        response = self.client.post(
            f"/v1/transactions/{record.transaction_id}/prepare",
            json={
                "correlation_id": str(record.correlation_id),
                "user_id": record.user_id,
                "points": record.total_points,
            },
        )
        return ParticipantVote(_participant_state(response))

    def commit(self, transaction_id: UUID) -> None:
        _final_ack(self.client.post(f"/v1/transactions/{transaction_id}/commit"))

    def abort(self, transaction_id: UUID) -> None:
        _final_ack(self.client.post(f"/v1/transactions/{transaction_id}/abort"))


class HttpCatalogParticipant:
    def __init__(self, base_url: str, timeout: float = 2.0) -> None:
        self.client = httpx.Client(base_url=base_url, timeout=timeout)

    def quote(self, reward_id: str) -> RewardQuote:
        response = self.client.get(f"/v1/rewards/{reward_id}")
        if response.status_code != 200:
            raise ParticipantUnavailable(f"Catalog quote failed with HTTP {response.status_code}")
        body = response.json()
        return RewardQuote(point_cost=body["point_cost"], version=body["version"])

    def prepare(self, record: WorkflowRecord) -> ParticipantVote:
        response = self.client.post(
            f"/v1/transactions/{record.transaction_id}/prepare",
            json={
                "correlation_id": str(record.correlation_id),
                "reward_id": record.reward_id,
                "quantity": record.quantity,
                "expected_reward_version": record.reward_version,
                "expected_point_cost": record.point_cost,
            },
        )
        return ParticipantVote(_participant_state(response))

    def commit(self, transaction_id: UUID) -> None:
        _final_ack(self.client.post(f"/v1/transactions/{transaction_id}/commit"))

    def abort(self, transaction_id: UUID) -> None:
        _final_ack(self.client.post(f"/v1/transactions/{transaction_id}/abort"))


class HttpAuditSink:
    def __init__(self, base_url: str, timeout: float = 2.0) -> None:
        self.client = httpx.Client(base_url=base_url, timeout=timeout)

    def append(self, envelope: EventEnvelope) -> None:
        response = self.client.post("/v1/audit-events", json=envelope.to_json())
        if response.status_code not in {200, 201}:
            raise ParticipantUnavailable(f"Audit append failed with HTTP {response.status_code}")


def _participant_state(response: httpx.Response) -> str:
    if response.status_code != 200:
        return "ABORTED"
    return str(response.json().get("state", "ABORTED"))


def _final_ack(response: httpx.Response) -> None:
    if response.status_code != 200:
        raise ParticipantUnavailable(f"Final decision failed with HTTP {response.status_code}")


class TwoPhaseCommitWorkflow:
    def __init__(
        self,
        repository: PostgresCoordinatorRepository,
        wallet: WalletParticipant,
        catalog: CatalogParticipant,
        audit: AuditSink,
    ) -> None:
        self.repository = repository
        self.wallet = wallet
        self.catalog = catalog
        self.audit = audit

    def execute(self, transaction_id: UUID) -> WorkflowRecord:
        with self._workflow_lock(transaction_id):
            return self._execute_locked(transaction_id)

    @contextmanager
    def _workflow_lock(self, transaction_id: UUID) -> Iterator[None]:
        connection = self.repository.engine.connect().execution_options(
            isolation_level="AUTOCOMMIT"
        )
        key = f"coordinator:{transaction_id}"
        connection.exec_driver_sql("SELECT pg_advisory_lock(hashtextextended(%s, 0))", (key,))
        try:
            yield
        finally:
            connection.exec_driver_sql("SELECT pg_advisory_unlock(hashtextextended(%s, 0))", (key,))
            connection.close()

    def _execute_locked(self, transaction_id: UUID) -> WorkflowRecord:
        record = self.repository.get_workflow(transaction_id)
        if record.coordination_mode != "two_phase_commit":
            raise InvalidTransition("The redemption is not a 2PC transaction.", transaction_id)

        if record.status == "REQUESTED":
            self.repository.transition(transaction_id, "PREPARING", "2PC prepare started")
            record = self.repository.get_workflow(transaction_id)

        if record.status == "PREPARING":
            decision = self._prepare_and_decide(record)
            record = self.repository.get_workflow(transaction_id)
            assert record.status == decision

        if record.status in {"COMMIT_DECIDED", "ABORT_DECIDED"}:
            record = self._deliver_decision(record)

        if record.status in {"COMMITTED", "ABORTED"}:
            self._deliver_audit(record.transaction_id)
        return self.repository.get_workflow(transaction_id)

    def _prepare_and_decide(self, record: WorkflowRecord) -> str:
        votes: list[str] = []
        try:
            quote = self.catalog.quote(record.reward_id)
            record = self.repository.record_quote(
                record.transaction_id, quote.point_cost, quote.version
            )
            votes.append(self.wallet.prepare(record).state)
            votes.append(self.catalog.prepare(record).state)
        except (httpx.TimeoutException, httpx.TransportError, ParticipantUnavailable):
            votes.append("ABORTED")

        if len(votes) == 2 and all(vote == "PREPARED" for vote in votes):
            self.repository.transition(
                record.transaction_id, "COMMIT_DECIDED", "all participants voted YES"
            )
            return "COMMIT_DECIDED"
        self.repository.transition(
            record.transaction_id, "ABORT_DECIDED", "NO vote or prepare timeout"
        )
        return "ABORT_DECIDED"

    def _deliver_decision(self, record: WorkflowRecord) -> WorkflowRecord:
        operation = "commit" if record.status == "COMMIT_DECIDED" else "abort"
        terminal = "COMMITTED" if operation == "commit" else "ABORTED"
        try:
            getattr(self.wallet, operation)(record.transaction_id)
            getattr(self.catalog, operation)(record.transaction_id)
        except (httpx.TimeoutException, httpx.TransportError, ParticipantUnavailable):
            return record
        finalized, _envelope = self.repository.finalize_with_audit(
            record.transaction_id, terminal, "all participants acknowledged final decision"
        )
        return finalized

    def _deliver_audit(self, transaction_id: UUID) -> None:
        envelope = self.repository.pending_audit(transaction_id)
        if envelope is None:
            return
        try:
            self.audit.append(envelope)
        except (httpx.TimeoutException, httpx.TransportError, ParticipantUnavailable):
            return
        self.repository.mark_audit_published(envelope.event_id)


def default_workflow(repository: PostgresCoordinatorRepository) -> TwoPhaseCommitWorkflow:
    timeout = float(os.environ.get("COORDINATOR_HTTP_TIMEOUT_SECONDS", "2"))
    return TwoPhaseCommitWorkflow(
        repository,
        HttpWalletParticipant(os.environ.get("WALLET_URL", "http://wallet:8001"), timeout),
        HttpCatalogParticipant(os.environ.get("CATALOG_URL", "http://catalog:8002"), timeout),
        HttpAuditSink(os.environ.get("AUDIT_LEDGER_URL", "http://audit-ledger:8003"), timeout),
    )
