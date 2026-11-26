"""Lease-backed single-leader admission for Coordinator writes."""

from __future__ import annotations

import logging
import os
import threading
import uuid
from collections.abc import Callable
from urllib.parse import urlparse

from etcd3gw.client import Etcd3Client
from etcd3gw.lease import Lease
from prometheus_client import CONTENT_TYPE_LATEST, REGISTRY, Gauge, generate_latest

try:
    from ledgermesh_messaging.metrics import REGISTRY as LEDGERMESH_REGISTRY
except ImportError:  # LM-034 may not be merged yet; keep local development functional.
    LEDGERMESH_REGISTRY = REGISTRY

logger = logging.getLogger(__name__)

LEADER = Gauge(
    "ledgermesh_coordinator_leader",
    "Whether this Coordinator holds the leader lease",
    registry=LEDGERMESH_REGISTRY,
)
QUORUM = Gauge(
    "ledgermesh_etcd_quorum_available",
    "Whether a linearizable etcd request succeeded",
    registry=LEDGERMESH_REGISTRY,
)


def leadership_enabled() -> bool:
    return os.environ.get("COORDINATOR_LEADERSHIP_ENABLED", "false").lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def leadership_metrics() -> tuple[bytes, str]:
    return generate_latest(LEDGERMESH_REGISTRY), CONTENT_TYPE_LATEST


class EtcdLeadership:
    """Acquire a single key under an etcd lease and fail closed on uncertainty.

    All reads use etcd3's default linearizable consistency. A successful local
    flag alone never admits a write: each request verifies that this instance
    still owns the key in etcd.
    """

    def __init__(
        self,
        endpoints: str | None = None,
        *,
        identity: str | None = None,
        key: str = "/ledgermesh/coordinator/leader",
        ttl_seconds: int | None = None,
        renew_interval_seconds: float | None = None,
        request_timeout_seconds: float | None = None,
        client_factory: Callable[..., object] = Etcd3Client,
    ) -> None:
        self.identity = identity or str(uuid.uuid4())
        self.key = key
        self.ttl_seconds = (
            ttl_seconds
            if ttl_seconds is not None
            else int(os.environ.get("COORDINATOR_LEADER_TTL_SECONDS", "9"))
        )
        self.renew_interval_seconds = (
            renew_interval_seconds
            if renew_interval_seconds is not None
            else float(os.environ.get("COORDINATOR_LEADER_RENEW_INTERVAL_SECONDS", "2"))
        )
        self.request_timeout_seconds = (
            request_timeout_seconds
            if request_timeout_seconds is not None
            else float(os.environ.get("ETCD_REQUEST_TIMEOUT_SECONDS", "1"))
        )
        if (
            self.ttl_seconds < 3
            or self.renew_interval_seconds <= 0
            or self.renew_interval_seconds >= self.ttl_seconds
            or self.request_timeout_seconds <= 0
        ):
            raise ValueError(
                "lease TTL must be >= 3s and renewal interval must be positive and < TTL"
            )
        configured = endpoints or os.environ.get(
            "ETCD_ENDPOINTS", "http://localhost:23791,http://localhost:23792,http://localhost:23793"
        )
        self.clients = [
            self._make_client(endpoint, client_factory) for endpoint in configured.split(",")
        ]
        if not self.clients:
            raise ValueError("at least one ETCD_ENDPOINTS URL is required")
        self._stop = threading.Event()
        self._leader = threading.Event()
        self._quorum = threading.Event()
        self._lock = threading.RLock()
        self._lease_id: int | None = None
        self._thread: threading.Thread | None = None

    def _make_client(self, endpoint: str, client_factory: Callable) -> object:
        parsed = urlparse(endpoint.strip())
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise ValueError(f"invalid etcd endpoint: {endpoint!r}")
        return client_factory(
            parsed.hostname,
            parsed.port or (443 if parsed.scheme == "https" else 2379),
            parsed.scheme,
            timeout=self.request_timeout_seconds,
        )

    @property
    def is_leader(self) -> bool:
        if not self._leader.is_set():
            return False
        try:
            for client in self.clients:
                try:
                    values = client.get(self.key)
                    self._set_quorum(True)
                    if self.identity.encode() in values:
                        return True
                    self._set_leader(False, "leadership key is owned by another member")
                    return False
                except Exception:
                    continue
        except Exception:
            logger.exception("leadership verification failed")
        self._set_quorum(False)
        self._set_leader(False, "linearizable leadership check failed")
        return False

    @property
    def quorum_available(self) -> bool:
        return self._quorum.is_set()

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run, name="coordinator-leader-lease", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=self.ttl_seconds)
        self._set_leader(False, "shutdown")
        self._release()
        for client in self.clients:
            session = getattr(client, "session", None)
            if session is not None:
                session.close()

    def _run(self) -> None:
        while not self._stop.is_set():
            if self._lease_id is None:
                self._acquire()
            else:
                self._renew()
            self._stop.wait(self.renew_interval_seconds)

    def _acquire(self) -> None:
        for client in self.clients:
            lease = None
            try:
                lease = client.lease(ttl=self.ttl_seconds)
                self._set_quorum(True)
                if client.create(self.key, self.identity, lease=lease):
                    with self._lock:
                        self._lease_id = lease.id
                    self._set_leader(True, "lease acquired")
                    return
                lease.revoke()
                return
            except Exception:
                logger.warning("etcd lease acquisition failed", exc_info=True)
                self._set_quorum(False)
                if lease is not None:
                    try:
                        lease.revoke()
                    except Exception:
                        logger.debug("could not revoke unused etcd lease", exc_info=True)
        self._set_leader(False, "unable to acquire lease")

    def _renew(self) -> None:
        lease_id = self._lease_id
        assert lease_id is not None
        for client in self.clients:
            try:
                ttl = Lease(lease_id, client).refresh()
                if ttl <= 0:
                    raise RuntimeError("etcd lease expired")
                values = client.get(self.key)
                if self.identity.encode() not in values:
                    raise RuntimeError("leadership key ownership changed")
                self._set_quorum(True)
                self._set_leader(True, "lease renewed")
                return
            except Exception:
                logger.warning("etcd lease renewal failed", exc_info=True)
        self._set_quorum(False)
        with self._lock:
            self._lease_id = None
        self._set_leader(False, "lease renewal failed; stepping down")

    def _release(self) -> None:
        with self._lock:
            lease_id, self._lease_id = self._lease_id, None
        if lease_id is None:
            return
        for client in self.clients:
            try:
                Lease(lease_id, client).revoke()
                return
            except Exception:
                continue

    def _set_leader(self, value: bool, reason: str) -> None:
        was_leader = self._leader.is_set()
        if value:
            self._leader.set()
        else:
            self._leader.clear()
        LEADER.set(int(value))
        if value != was_leader:
            logger.info("coordinator leadership changed", extra={"leader": value, "reason": reason})

    def _set_quorum(self, value: bool) -> None:
        changed = self._quorum.is_set() != value
        if value:
            self._quorum.set()
        else:
            self._quorum.clear()
        QUORUM.set(int(value))
        if changed:
            logger.info("etcd quorum health changed", extra={"quorum_available": value})
