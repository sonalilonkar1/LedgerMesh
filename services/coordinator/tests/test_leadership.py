from __future__ import annotations

import threading
import time

import pytest

from services.coordinator.app.leadership import EtcdLeadership, leadership_metrics


class FakeEtcdState:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.value: bytes | None = None
        self.lease_id: int | None = None
        self.next_lease_id = 1
        self.fail_refresh = False
        self.unavailable = False


class FakeLease:
    def __init__(self, client: FakeEtcdClient, lease_id: int, ttl: int) -> None:
        self.client = client
        self.id = lease_id
        self.ttl = ttl

    def refresh(self) -> int:
        if self.client.state.fail_refresh:
            raise ConnectionError("quorum unavailable")
        return self.ttl

    def revoke(self) -> bool:
        with self.client.state.lock:
            if self.client.state.lease_id == self.id:
                self.client.state.value = None
                self.client.state.lease_id = None
        return True


class FakeEtcdClient:
    def __init__(self, state: FakeEtcdState) -> None:
        self.state = state

    def lease(self, ttl: int) -> FakeLease:
        if self.state.unavailable:
            raise ConnectionError("quorum unavailable")
        with self.state.lock:
            lease_id = self.state.next_lease_id
            self.state.next_lease_id += 1
        return FakeLease(self, lease_id, ttl)

    def create(self, key: str, value: str, lease: FakeLease) -> bool:
        if self.state.unavailable:
            raise ConnectionError("quorum unavailable")
        with self.state.lock:
            if self.state.value is not None:
                return False
            self.state.value = value.encode()
            self.state.lease_id = lease.id
            return True

    def get(self, key: str) -> list[bytes]:
        if self.state.unavailable:
            raise ConnectionError("quorum unavailable")
        with self.state.lock:
            return [] if self.state.value is None else [self.state.value]


def client_factory_for(state: FakeEtcdState):
    return lambda _host, _port, _scheme, **_kwargs: FakeEtcdClient(state)


def wait_for(predicate, timeout: float = 1.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return predicate()


@pytest.fixture
def state() -> FakeEtcdState:
    return FakeEtcdState()


def new_manager(state: FakeEtcdState, identity: str) -> EtcdLeadership:
    return EtcdLeadership(
        "http://etcd-1:2379,http://etcd-2:2379,http://etcd-3:2379",
        identity=identity,
        ttl_seconds=3,
        renew_interval_seconds=0.05,
        client_factory=client_factory_for(state),
    )


def test_only_one_coordinator_acquires_the_leadership_lease(state: FakeEtcdState) -> None:
    first = new_manager(state, "coordinator-a")
    second = new_manager(state, "coordinator-b")
    first.start()
    second.start()
    try:
        assert wait_for(lambda: first.is_leader or second.is_leader)
        assert first.is_leader != second.is_leader
    finally:
        first.stop()
        second.stop()


def test_failed_lease_renewal_immediately_steps_down(state: FakeEtcdState) -> None:
    manager = new_manager(state, "coordinator-a")
    manager.start()
    assert wait_for(lambda: manager.is_leader)
    state.fail_refresh = True
    state.unavailable = True

    manager._renew()

    assert not manager.is_leader
    assert manager._lease_id is None
    assert not manager.quorum_available
    manager.stop()


def test_linearizable_check_rejects_a_stale_local_leader_flag(state: FakeEtcdState) -> None:
    manager = new_manager(state, "coordinator-a")
    manager.start()
    assert wait_for(lambda: manager.is_leader)
    with state.lock:
        state.value = b"coordinator-b"

    assert not manager.is_leader
    manager.stop()


def test_prometheus_exposition_contains_leader_and_quorum_gauges(
    state: FakeEtcdState,
) -> None:
    manager = new_manager(state, "coordinator-a")
    manager._set_quorum(True)
    manager._set_leader(True, "test")

    payload, content_type = leadership_metrics()

    assert b"ledgermesh_coordinator_leader 1.0" in payload
    assert b"ledgermesh_etcd_quorum_available 1.0" in payload
    assert "text/plain" in content_type


@pytest.mark.parametrize(
    ("ttl", "renewal"),
    [(0, 0.1), (2, 0.1), (3, 3), (9, 0)],
)
def test_invalid_lease_timing_is_rejected(ttl: int, renewal: float) -> None:
    with pytest.raises(ValueError, match="lease TTL"):
        EtcdLeadership("http://etcd:2379", ttl_seconds=ttl, renew_interval_seconds=renewal)
