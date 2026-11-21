"""Crash recovery and operator visibility for durable 2PC transactions (LM-016)."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from threading import Event, Thread

from services.coordinator.app.models import InDoubtTransactionView, RecoveryRunResponse
from services.coordinator.app.repository import PostgresCoordinatorRepository, WorkflowRecord
from services.coordinator.app.two_phase_commit import TwoPhaseCommitWorkflow

LOGGER = logging.getLogger(__name__)


def _recovery_action(status: str) -> str:
    return {
        "PREPARING": "PRESUME_ABORT",
        "COMMIT_DECIDED": "RETRANSMIT_COMMIT",
        "ABORT_DECIDED": "RETRANSMIT_ABORT",
    }[status]


class TwoPhaseCommitRecovery:
    def __init__(
        self,
        repository: PostgresCoordinatorRepository,
        workflow: TwoPhaseCommitWorkflow,
        *,
        batch_size: int = 100,
    ) -> None:
        self.repository = repository
        self.workflow = workflow
        self.batch_size = batch_size

    def in_doubt(self) -> list[InDoubtTransactionView]:
        return [self._view(record) for record in self.repository.list_in_doubt_2pc(self.batch_size)]

    def run_once(self) -> RecoveryRunResponse:
        candidates = self.repository.list_recoverable_2pc(self.batch_size)
        recovered = 0
        failed = 0
        for candidate in candidates:
            try:
                result = self.workflow.recover(candidate.transaction_id)
                if result.status in {"COMMITTED", "ABORTED"}:
                    recovered += 1
            except Exception:  # One poison transaction must not stop the recovery batch.
                failed += 1
                LOGGER.exception("2PC recovery failed for %s", candidate.transaction_id)
        return RecoveryRunResponse(
            scanned=len(candidates),
            recovered=recovered,
            still_in_doubt=len(self.repository.list_in_doubt_2pc(self.batch_size)),
            failed=failed,
        )

    @staticmethod
    def _view(record: WorkflowRecord) -> InDoubtTransactionView:
        return InDoubtTransactionView(
            transaction_id=record.transaction_id,
            status=record.status,
            workflow_step=record.workflow_step,
            updated_at=record.updated_at,
            recovery_action=_recovery_action(record.status),
        )


@dataclass
class RecoveryWorker:
    recovery: TwoPhaseCommitRecovery
    interval_seconds: float

    def __post_init__(self) -> None:
        self._stop = Event()
        self._thread: Thread | None = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = Thread(target=self._run, name="2pc-recovery", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=min(self.interval_seconds + 1, 5))

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self.recovery.run_once()
            except Exception:
                LOGGER.exception("2PC recovery scan failed; it will be retried")
            self._stop.wait(self.interval_seconds)


def recovery_enabled() -> bool:
    return os.environ.get("COORDINATOR_RECOVERY_ENABLED", "false").lower() in {
        "1",
        "true",
        "yes",
    }
