from fastapi.testclient import TestClient

from services.coordinator.app.main import app, get_coordinator_leadership, get_coordinator_service
from services.coordinator.app.service import CoordinatorService
from services.coordinator.tests.test_core_api import (
    FakeCoordinatorRepository,
    request_payload,
)


class FakeLeadership:
    is_leader = False


def test_nonleader_cannot_create_a_redemption(monkeypatch) -> None:
    repository = FakeCoordinatorRepository()
    leadership = FakeLeadership()
    app.dependency_overrides[get_coordinator_service] = lambda: CoordinatorService(repository)
    app.dependency_overrides[get_coordinator_leadership] = lambda: leadership
    monkeypatch.setenv("COORDINATOR_LEADERSHIP_ENABLED", "true")
    client = TestClient(app)
    try:
        response = client.post(
            "/v1/redemptions",
            headers={"Idempotency-Key": "nonleader"},
            json=request_payload(),
        )
        assert response.status_code == 503
        assert repository.redemptions == {}
    finally:
        app.dependency_overrides.clear()


def test_leader_can_create_a_redemption(monkeypatch) -> None:
    repository = FakeCoordinatorRepository()
    leadership = FakeLeadership()
    leadership.is_leader = True
    app.dependency_overrides[get_coordinator_service] = lambda: CoordinatorService(repository)
    app.dependency_overrides[get_coordinator_leadership] = lambda: leadership
    monkeypatch.setenv("COORDINATOR_LEADERSHIP_ENABLED", "true")
    client = TestClient(app)
    try:
        response = client.post(
            "/v1/redemptions",
            headers={"Idempotency-Key": "leader"},
            json=request_payload(),
        )
        assert response.status_code == 202
        assert len(repository.redemptions) == 1
    finally:
        app.dependency_overrides.clear()
