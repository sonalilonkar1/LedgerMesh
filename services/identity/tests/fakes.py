"""In-memory stand-ins for the Identity database, mail and clock.

The fake repository mirrors the conditional writes of
``PostgresIdentityRepository``; ``tests/db/test_identity_core.py`` runs the
same behaviour against real PostgreSQL.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

from fastapi.testclient import TestClient

from services.identity.app.main import app, get_identity_service
from services.identity.app.repository import (
    AuthEventRecord,
    LoginOutcome,
    NewAuthEvent,
    NewSession,
    SessionRecord,
    UserRecord,
    classify_token,
)
from services.identity.app.service import IdentityService
from services.identity.app.settings import IdentitySettings

START = datetime(2026, 11, 27, 12, 0, tzinfo=UTC)


class FakeClock:
    def __init__(self, now: datetime = START) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)


@dataclass
class Token:
    id: UUID
    user_id: UUID
    token_hash: str
    created_at: datetime
    expires_at: datetime
    consumed_at: datetime | None = None
    revoked_at: datetime | None = None


@dataclass
class Session:
    id: UUID
    session_hash: str
    user_id: UUID
    created_at: datetime
    last_seen_at: datetime
    idle_expires_at: datetime
    absolute_expires_at: datetime
    revoked_at: datetime | None = None


@dataclass
class FakeIdentityRepository:
    users: dict[str, UserRecord] = field(default_factory=dict)
    tokens: list[Token] = field(default_factory=list)
    sessions: list[Session] = field(default_factory=list)
    counters: dict[tuple[str, str, datetime], int] = field(default_factory=dict)
    events: list[AuthEventRecord] = field(default_factory=list)
    lock: threading.Lock = field(default_factory=threading.Lock)

    def count_attempt(self, *, scope, key_hash, window_start, now) -> int:
        with self.lock:
            key = (scope, key_hash, window_start)
            self.counters[key] = self.counters.get(key, 0) + 1
            return self.counters[key]

    def record_event(self, event: NewAuthEvent) -> None:
        with self.lock:
            self._record(event)

    def issue_login_token(self, *, email, token_hash, now, expires_at, client_ip) -> UUID:
        with self.lock:
            user = self.users.get(email)
            if user is None:
                user = UserRecord(id=uuid4(), email=email, email_verified_at=None)
                self.users[email] = user
            for token in self.tokens:
                if token.user_id == user.id and token.consumed_at is None and not token.revoked_at:
                    token.revoked_at = now
            self.tokens.append(Token(uuid4(), user.id, token_hash, now, expires_at))
            self._record(NewAuthEvent("login_requested", now, user_id=user.id, client_ip=client_ip))
            return user.id

    def consume_login_token(
        self, *, token_hash, now, new_session: NewSession, presented_session_hash, client_ip
    ) -> LoginOutcome:
        with self.lock:
            token = next((t for t in self.tokens if t.token_hash == token_hash), None)
            usable = (
                token is not None
                and token.consumed_at is None
                and token.revoked_at is None
                and token.expires_at > now
            )
            if not usable:
                reason = classify_token(token, now)
                self._record(
                    NewAuthEvent(
                        "login_failed",
                        now,
                        reason=reason,
                        user_id=token.user_id if token else None,
                        client_ip=client_ip,
                    )
                )
                return LoginOutcome(session=None, failure_reason=reason)
            token.consumed_at = now
            email, user = next((e, u) for e, u in self.users.items() if u.id == token.user_id)
            user = replace(user, email_verified_at=user.email_verified_at or now)
            self.users[email] = user
            if presented_session_hash:
                for session in self.sessions:
                    if session.session_hash == presented_session_hash and not session.revoked_at:
                        session.revoked_at = now
            self.sessions.append(
                Session(
                    id=new_session.id,
                    session_hash=new_session.session_hash,
                    user_id=user.id,
                    created_at=now,
                    last_seen_at=now,
                    idle_expires_at=new_session.idle_expires_at,
                    absolute_expires_at=new_session.absolute_expires_at,
                )
            )
            self._record(
                NewAuthEvent(
                    "login_succeeded",
                    now,
                    user_id=user.id,
                    session_id=new_session.id,
                    client_ip=client_ip,
                )
            )
            return LoginOutcome(
                session=SessionRecord(
                    new_session.id,
                    user,
                    new_session.idle_expires_at,
                    new_session.absolute_expires_at,
                )
            )

    def resolve_session(self, *, session_hash, now, idle_seconds) -> SessionRecord | None:
        with self.lock:
            for session in self.sessions:
                if (
                    session.session_hash == session_hash
                    and session.revoked_at is None
                    and session.idle_expires_at > now
                    and session.absolute_expires_at > now
                ):
                    session.last_seen_at = now
                    session.idle_expires_at = min(
                        now + timedelta(seconds=idle_seconds), session.absolute_expires_at
                    )
                    user = next(u for u in self.users.values() if u.id == session.user_id)
                    return SessionRecord(
                        session.id, user, session.idle_expires_at, session.absolute_expires_at
                    )
            return None

    def revoke_session(self, *, session_hash, now, client_ip) -> bool:
        with self.lock:
            for session in self.sessions:
                if session.session_hash == session_hash and session.revoked_at is None:
                    session.revoked_at = now
                    self._record(
                        NewAuthEvent(
                            "logout",
                            now,
                            user_id=session.user_id,
                            session_id=session.id,
                            client_ip=client_ip,
                        )
                    )
                    return True
            return False

    def list_events(self, *, limit, before_id) -> list[AuthEventRecord]:
        events = [e for e in reversed(self.events) if before_id is None or e.id < before_id]
        return events[:limit]

    def _record(self, event: NewAuthEvent) -> None:
        self.events.append(
            AuthEventRecord(
                id=len(self.events) + 1,
                occurred_at=event.occurred_at,
                event_type=event.event_type,
                reason=event.reason,
                user_id=event.user_id,
                session_id=event.session_id,
                email_masked=event.email_masked,
                client_ip=event.client_ip,
            )
        )

    def event_types(self) -> list[str]:
        return [event.event_type for event in self.events]


@dataclass(frozen=True)
class SentLink:
    to: str
    link: str
    expires_in_minutes: int

    @property
    def token(self) -> str:
        return self.link.split("#token=", 1)[1]


class RecordingMailer:
    def __init__(self) -> None:
        self.sent: list[SentLink] = []
        self.fail = False

    def send_login_link(self, *, to: str, link: str, expires_in_minutes: int) -> None:
        if self.fail:
            raise ConnectionRefusedError(f"cannot deliver to {to}")
        self.sent.append(SentLink(to, link, expires_in_minutes))

    def last_token(self) -> str:
        return self.sent[-1].token


def make_settings(**overrides) -> IdentitySettings:
    values = {"environment": "test", "operator_emails": frozenset({"operator@example.com"})}
    return IdentitySettings(**(values | overrides))


@dataclass
class Harness:
    client: TestClient
    repository: FakeIdentityRepository
    mailer: RecordingMailer
    clock: FakeClock
    service: IdentityService

    def request_login(self, email: str, **kwargs):
        return self.client.post("/v1/auth/login-requests", json={"email": email}, **kwargs)

    def verify(self, token: str):
        return self.client.post("/v1/auth/verify", json={"token": token})

    def sign_in(self, email: str = "alice@example.com"):
        assert self.request_login(email).status_code == 202
        response = self.verify(self.mailer.last_token())
        assert response.status_code == 200, response.text
        return response

    def logout(self):
        return self.client.post("/v1/auth/logout", headers={"Content-Type": "application/json"})


def make_harness(**settings_overrides) -> Harness:
    settings = make_settings(**settings_overrides)
    repository = FakeIdentityRepository()
    mailer = RecordingMailer()
    clock = FakeClock()
    service = IdentityService(repository, settings, mailer, clock)
    app.dependency_overrides[get_identity_service] = lambda: service
    scheme = "https" if settings.secure_cookies else "http"
    client = TestClient(app, base_url=f"{scheme}://testserver")
    return Harness(client, repository, mailer, clock, service)
