"""PostgreSQL repository for users, login tokens, sessions and auth events (LM-052).

Correctness comes from conditional writes, not from reading first:

- a login token is consumed by ``UPDATE ... WHERE consumed_at IS NULL AND
  revoked_at IS NULL AND expires_at > :now RETURNING``. Under READ COMMITTED a
  second concurrent update re-checks the row after the first commits and
  matches nothing, so exactly one request wins;
- a session is only extended by an update that re-checks its revocation and
  both expiries;
- rate-limit counters are a single atomic upsert per attempt.

Every method runs in its own transaction. Times come from the caller's clock
(``now``) so tests can move time forward deterministically.
"""

from __future__ import annotations

import ipaddress
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import lru_cache
from typing import Protocol
from uuid import UUID, uuid4

from sqlalchemy import Connection, Engine, create_engine, delete, func, insert, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from services.identity.app.db.schema import (
    auth_events,
    login_tokens,
    rate_limit_counters,
    sessions,
    users,
)

# Counter rows older than this are deleted opportunistically.
RATE_LIMIT_RETENTION = timedelta(days=1)


@dataclass(frozen=True)
class UserRecord:
    id: UUID
    email: str
    email_verified_at: datetime | None


@dataclass(frozen=True)
class SessionRecord:
    id: UUID
    user: UserRecord
    idle_expires_at: datetime
    absolute_expires_at: datetime


@dataclass(frozen=True)
class NewSession:
    id: UUID
    session_hash: str
    idle_expires_at: datetime
    absolute_expires_at: datetime


@dataclass(frozen=True)
class LoginOutcome:
    """Either a new session, or why the token was refused (for the audit only)."""

    session: SessionRecord | None
    failure_reason: str | None = None


@dataclass(frozen=True)
class NewAuthEvent:
    event_type: str
    occurred_at: datetime
    reason: str | None = None
    user_id: UUID | None = None
    session_id: UUID | None = None
    email_masked: str | None = None
    client_ip: str | None = None


@dataclass(frozen=True)
class AuthEventRecord:
    id: int
    occurred_at: datetime
    event_type: str
    reason: str | None
    user_id: UUID | None
    session_id: UUID | None
    email_masked: str | None
    client_ip: str | None


class IdentityRepository(Protocol):
    def count_attempt(
        self, *, scope: str, key_hash: str, window_start: datetime, now: datetime
    ) -> int: ...

    def record_event(self, event: NewAuthEvent) -> None: ...

    def issue_login_token(
        self,
        *,
        email: str,
        token_hash: str,
        now: datetime,
        expires_at: datetime,
        client_ip: str | None,
    ) -> UUID: ...

    def consume_login_token(
        self,
        *,
        token_hash: str,
        now: datetime,
        new_session: NewSession,
        presented_session_hash: str | None,
        client_ip: str | None,
    ) -> LoginOutcome: ...

    def resolve_session(
        self, *, session_hash: str, now: datetime, idle_seconds: int
    ) -> SessionRecord | None: ...

    def revoke_session(
        self, *, session_hash: str, now: datetime, client_ip: str | None
    ) -> bool: ...

    def list_events(self, *, limit: int, before_id: int | None) -> list[AuthEventRecord]: ...


def database_url() -> str:
    if override := os.environ.get("IDENTITY_DATABASE_URL") or os.environ.get("DATABASE_URL"):
        return override
    password = os.environ.get("POSTGRES_PASSWORD", "ledgermesh_dev")
    return f"postgresql+psycopg://identity:{password}@identity-db:5432/identity"


def make_engine(url: str) -> Engine:
    # hide_parameters keeps bound values (a full email address) out of
    # SQLAlchemy exception text, which uvicorn logs with the traceback when a
    # statement fails.
    return create_engine(url, pool_pre_ping=True, hide_parameters=True)


@lru_cache
def get_engine(url: str | None = None) -> Engine:
    return make_engine(url or database_url())


def ip_or_none(value: str | None) -> str | None:
    """The INET column only takes real addresses (TestClient reports 'testclient')."""
    if not value:
        return None
    try:
        return str(ipaddress.ip_address(value))
    except ValueError:
        return None


def classify_token(row, now: datetime) -> str:
    if row is None:
        return "unknown"
    if row.consumed_at is not None:
        return "used"
    if row.revoked_at is not None:
        return "revoked"
    if row.expires_at <= now:
        return "expired"
    # Valid when re-read, but lost the race to a concurrent consume.
    return "used"


class PostgresIdentityRepository:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def count_attempt(
        self, *, scope: str, key_hash: str, window_start: datetime, now: datetime
    ) -> int:
        statement = pg_insert(rate_limit_counters).values(
            scope=scope, key_hash=key_hash, window_start=window_start, count=1
        )
        statement = statement.on_conflict_do_update(
            index_elements=[
                rate_limit_counters.c.scope,
                rate_limit_counters.c.key_hash,
                rate_limit_counters.c.window_start,
            ],
            set_={"count": rate_limit_counters.c.count + 1},
        ).returning(rate_limit_counters.c.count)
        with self.engine.begin() as connection:
            count = connection.execute(statement).scalar_one()
            connection.execute(
                delete(rate_limit_counters).where(
                    rate_limit_counters.c.window_start < now - RATE_LIMIT_RETENTION
                )
            )
        return count

    def record_event(self, event: NewAuthEvent) -> None:
        with self.engine.begin() as connection:
            self._record(connection, event)

    def issue_login_token(
        self,
        *,
        email: str,
        token_hash: str,
        now: datetime,
        expires_at: datetime,
        client_ip: str | None,
    ) -> UUID:
        upsert = pg_insert(users).values(id=uuid4(), email=email, created_at=now)
        # New and existing users take the same path. The no-op update locks the
        # user row, so concurrent requests for one address are serialized.
        upsert = upsert.on_conflict_do_update(
            index_elements=[users.c.email], set_={"email": upsert.excluded.email}
        ).returning(users.c.id)
        with self.engine.begin() as connection:
            user_id = connection.execute(upsert).scalar_one()
            connection.execute(
                update(login_tokens)
                .where(
                    login_tokens.c.user_id == user_id,
                    login_tokens.c.consumed_at.is_(None),
                    login_tokens.c.revoked_at.is_(None),
                )
                .values(revoked_at=now)
            )
            connection.execute(
                insert(login_tokens).values(
                    id=uuid4(),
                    user_id=user_id,
                    token_hash=token_hash,
                    created_at=now,
                    expires_at=expires_at,
                )
            )
            self._record(
                connection,
                NewAuthEvent(
                    "login_requested", occurred_at=now, user_id=user_id, client_ip=client_ip
                ),
            )
        return user_id

    def consume_login_token(
        self,
        *,
        token_hash: str,
        now: datetime,
        new_session: NewSession,
        presented_session_hash: str | None,
        client_ip: str | None,
    ) -> LoginOutcome:
        with self.engine.begin() as connection:
            user_id = connection.execute(
                update(login_tokens)
                .where(
                    login_tokens.c.token_hash == token_hash,
                    login_tokens.c.consumed_at.is_(None),
                    login_tokens.c.revoked_at.is_(None),
                    login_tokens.c.expires_at > now,
                )
                .values(consumed_at=now)
                .returning(login_tokens.c.user_id)
            ).scalar_one_or_none()
            if user_id is None:
                existing = connection.execute(
                    select(login_tokens).where(login_tokens.c.token_hash == token_hash)
                ).one_or_none()
                reason = classify_token(existing, now)
                self._record(
                    connection,
                    NewAuthEvent(
                        "login_failed",
                        occurred_at=now,
                        reason=reason,
                        user_id=existing.user_id if existing is not None else None,
                        client_ip=client_ip,
                    ),
                )
                return LoginOutcome(session=None, failure_reason=reason)

            user = connection.execute(
                update(users)
                .where(users.c.id == user_id)
                .values(
                    email_verified_at=func.coalesce(users.c.email_verified_at, now),
                    last_login_at=now,
                )
                .returning(users.c.id, users.c.email, users.c.email_verified_at)
            ).one()
            if presented_session_hash:
                # A new login never keeps a session id that existed before it.
                connection.execute(
                    update(sessions)
                    .where(
                        sessions.c.session_hash == presented_session_hash,
                        sessions.c.revoked_at.is_(None),
                    )
                    .values(revoked_at=now)
                )
            connection.execute(
                insert(sessions).values(
                    id=new_session.id,
                    session_hash=new_session.session_hash,
                    user_id=user_id,
                    created_at=now,
                    last_seen_at=now,
                    idle_expires_at=new_session.idle_expires_at,
                    absolute_expires_at=new_session.absolute_expires_at,
                )
            )
            self._record(
                connection,
                NewAuthEvent(
                    "login_succeeded",
                    occurred_at=now,
                    user_id=user_id,
                    session_id=new_session.id,
                    client_ip=client_ip,
                ),
            )
        return LoginOutcome(
            session=SessionRecord(
                id=new_session.id,
                user=UserRecord(user.id, user.email, user.email_verified_at),
                idle_expires_at=new_session.idle_expires_at,
                absolute_expires_at=new_session.absolute_expires_at,
            )
        )

    def resolve_session(
        self, *, session_hash: str, now: datetime, idle_seconds: int
    ) -> SessionRecord | None:
        with self.engine.begin() as connection:
            row = connection.execute(
                update(sessions)
                .where(
                    sessions.c.session_hash == session_hash,
                    sessions.c.revoked_at.is_(None),
                    sessions.c.idle_expires_at > now,
                    sessions.c.absolute_expires_at > now,
                )
                .values(
                    last_seen_at=now,
                    idle_expires_at=func.least(
                        now + timedelta(seconds=idle_seconds), sessions.c.absolute_expires_at
                    ),
                )
                .returning(
                    sessions.c.id,
                    sessions.c.user_id,
                    sessions.c.idle_expires_at,
                    sessions.c.absolute_expires_at,
                )
            ).one_or_none()
            if row is None:
                return None
            user = connection.execute(
                select(users.c.id, users.c.email, users.c.email_verified_at).where(
                    users.c.id == row.user_id
                )
            ).one()
        return SessionRecord(
            id=row.id,
            user=UserRecord(user.id, user.email, user.email_verified_at),
            idle_expires_at=row.idle_expires_at,
            absolute_expires_at=row.absolute_expires_at,
        )

    def revoke_session(self, *, session_hash: str, now: datetime, client_ip: str | None) -> bool:
        with self.engine.begin() as connection:
            row = connection.execute(
                update(sessions)
                .where(sessions.c.session_hash == session_hash, sessions.c.revoked_at.is_(None))
                .values(revoked_at=now)
                .returning(sessions.c.id, sessions.c.user_id)
            ).one_or_none()
            if row is None:
                return False
            self._record(
                connection,
                NewAuthEvent(
                    "logout",
                    occurred_at=now,
                    user_id=row.user_id,
                    session_id=row.id,
                    client_ip=client_ip,
                ),
            )
        return True

    def list_events(self, *, limit: int, before_id: int | None) -> list[AuthEventRecord]:
        query = select(auth_events).order_by(auth_events.c.id.desc()).limit(limit)
        if before_id is not None:
            query = query.where(auth_events.c.id < before_id)
        with self.engine.connect() as connection:
            rows = connection.execute(query).all()
        return [
            AuthEventRecord(
                id=row.id,
                occurred_at=row.occurred_at,
                event_type=row.event_type,
                reason=row.reason,
                user_id=row.user_id,
                session_id=row.session_id,
                email_masked=row.email_masked,
                client_ip=str(row.client_ip) if row.client_ip is not None else None,
            )
            for row in rows
        ]

    @staticmethod
    def _record(connection: Connection, event: NewAuthEvent) -> None:
        connection.execute(
            insert(auth_events).values(
                occurred_at=event.occurred_at,
                event_type=event.event_type,
                reason=event.reason,
                user_id=event.user_id,
                session_id=event.session_id,
                email_masked=event.email_masked,
                client_ip=ip_or_none(event.client_ip),
            )
        )
