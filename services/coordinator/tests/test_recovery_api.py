"""HTTP operator endpoints for LM-016 recovery."""

from datetime import UTC, datetime
from uuid import uuid4

from fastapi.testclient import TestClient

from services.coordinator.app.main import app, get_two_phase_commit_recovery
from services.coordinator.app.models import InDoubtTransactionView, RecoveryRunResponse


class Recovery:
    def __init__(self):
        self.transaction_id = uuid4()
        self.runs = 0

    def in_doubt(self):
        return [
            InDoubtTransactionView(
                transaction_id=self.transaction_id,
                status="COMMIT_DECIDED",
                workflow_step=2,
                updated_at=datetime.now(UTC),
                recovery_action="RETRANSMIT_COMMIT",
            )
        ]

    def run_once(self):
        self.runs += 1
        return RecoveryRunResponse(scanned=1, recovered=1, still_in_doubt=0, failed=0)


def test_operator_can_list_and_trigger_recovery():
    recovery = Recovery()
    app.dependency_overrides[get_two_phase_commit_recovery] = lambda: recovery
    try:
        with TestClient(app) as client:
            listed = client.get("/internal/v1/2pc/in-doubt")
            run = client.post("/internal/v1/2pc/recovery/run")
    finally:
        app.dependency_overrides.clear()

    assert listed.status_code == 200
    assert listed.json()[0]["transaction_id"] == str(recovery.transaction_id)
    assert listed.json()[0]["recovery_action"] == "RETRANSMIT_COMMIT"
    assert run.status_code == 200
    assert run.json() == {"scanned": 1, "recovered": 1, "still_in_doubt": 0, "failed": 0}
    assert recovery.runs == 1
