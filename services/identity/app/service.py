"""Passwordless email sign-in: magic links, sessions and roles (LM-052).

Flow: a login request validates and normalizes the address, creates the user
if needed and stores the hash of a new single-use token; the token goes out by
email. Verifying the token atomically consumes it and creates a server-side
session whose random identifier is the cookie value. Logout revokes the
session row.

Nothing a caller sees depends on whether the account already existed: new
and known addresses take the same path and get the same response.

Logs carry ids only. A token, a session identifier or an email address is
never passed to a logger, because the shared formatter redacts by field name
and does not recognise email addresses.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal
from uuid import UUID, uuid4

from services.identity.app.email_address import (
    InvalidEmailAddress,
    is_blocked_domain,
    mask_email,
    normalize_email,
)
from services.identity.app.errors import InvalidEmail, InvalidOrExpiredLink, RateLimited
from services.identity.app.mailer import Mailer
from services.identity.app.rate_limit import LOGIN_EMAIL, LOGIN_IP, VERIFY_IP, RateLimiter
from services.identity.app.repository import (
    AuthEventRecord,
    IdentityRepository,
    NewAuthEvent,
    NewSession,
    SessionRecord,
)
from services.identity.app.settings import IdentitySettings
from services.identity.app.tokens import hash_secret, new_secret

logger = logging.getLogger(__name__)

Clock = Callable[[], datetime]
Role = Literal["user", "operator"]


def utc_now() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True)
class PendingLoginEmail:
    """What the background task needs to send the link. Never logged."""

    user_id: UUID
    to: str
    link: str


@dataclass(frozen=True)
class Principal:
    kind: Literal["guest", "user", "operator"]
    user_id: UUID | None = None
    email: str | None = None
    session: SessionRecord | None = None

    @property
    def is_authenticated(self) -> bool:
        return self.kind != "guest"


GUEST = Principal(kind="guest")


@dataclass(frozen=True)
class SignedIn:
    session_secret: str
    session: SessionRecord
    role: Role


class IdentityService:
    def __init__(
        self,
        repository: IdentityRepository,
        settings: IdentitySettings,
        mailer: Mailer,
        clock: Clock = utc_now,
    ) -> None:
        self.repository = repository
        self.settings = settings
        self.mailer = mailer
        self.clock = clock
        self.limiter = RateLimiter(
            repository,
            window_seconds=settings.rate_limit_window_seconds,
            limits={
                LOGIN_EMAIL: settings.login_limit_per_email,
                LOGIN_IP: settings.login_limit_per_ip,
                VERIFY_IP: settings.verify_limit_per_ip,
            },
        )

    # --- login request -------------------------------------------------------

    def request_login(self, raw_email: str, client_ip: str) -> PendingLoginEmail:
        now = self.clock()
        # Per IP first, so floods of invalid input are limited too.
        self._enforce(LOGIN_IP, client_ip, now, client_ip=client_ip)
        try:
            email = normalize_email(
                raw_email, check_deliverability=self.settings.check_deliverability
            )
        except InvalidEmailAddress:
            raise InvalidEmail() from None
        if is_blocked_domain(email, self.settings.blocked_email_domains):
            raise InvalidEmail()
        # Before any user lookup, so the outcome is the same for every address.
        self._enforce(LOGIN_EMAIL, email, now, client_ip=client_ip, email_masked=mask_email(email))

        token = new_secret()
        user_id = self.repository.issue_login_token(
            email=email,
            token_hash=hash_secret(token),
            now=now,
            expires_at=now + timedelta(seconds=self.settings.login_token_ttl_seconds),
            client_ip=client_ip,
        )
        logger.info("login link issued", extra={"user_id": str(user_id)})
        return PendingLoginEmail(
            user_id=user_id, to=email, link=f"{self.settings.login_link_url}#token={token}"
        )

    def send_login_email(self, pending: PendingLoginEmail) -> None:
        """Runs after the response is sent; a failure must not change the response."""
        try:
            self.mailer.send_login_link(
                to=pending.to, link=pending.link, expires_in_minutes=self.token_ttl_minutes
            )
        except Exception as exc:
            # No exc_info: SMTP errors can quote the recipient address.
            logger.warning(
                "login email delivery failed",
                extra={"user_id": str(pending.user_id), "error_type": type(exc).__name__},
            )

    @property
    def token_ttl_minutes(self) -> int:
        return max(1, round(self.settings.login_token_ttl_seconds / 60))

    # --- verification and sessions -------------------------------------------

    def verify(self, token: str, client_ip: str, presented_session: str | None) -> SignedIn:
        now = self.clock()
        self._enforce(VERIFY_IP, client_ip, now, client_ip=client_ip)
        session_secret = new_secret()
        absolute = now + timedelta(seconds=self.settings.session_absolute_seconds)
        new_session = NewSession(
            id=uuid4(),
            session_hash=hash_secret(session_secret),
            idle_expires_at=min(
                now + timedelta(seconds=self.settings.session_idle_seconds), absolute
            ),
            absolute_expires_at=absolute,
        )
        outcome = self.repository.consume_login_token(
            token_hash=hash_secret(token),
            now=now,
            new_session=new_session,
            presented_session_hash=hash_secret(presented_session) if presented_session else None,
            client_ip=client_ip,
        )
        if outcome.session is None:
            logger.info("login failed", extra={"reason": outcome.failure_reason})
            raise InvalidOrExpiredLink()
        user = outcome.session.user
        logger.info(
            "login succeeded",
            extra={"user_id": str(user.id), "session_id": str(outcome.session.id)},
        )
        return SignedIn(session_secret, outcome.session, self.role_for(user.email))

    def resolve(self, session_secret: str | None) -> Principal:
        if not session_secret:
            return GUEST
        session = self.repository.resolve_session(
            session_hash=hash_secret(session_secret),
            now=self.clock(),
            idle_seconds=self.settings.session_idle_seconds,
        )
        if session is None:
            return GUEST
        return Principal(
            kind=self.role_for(session.user.email),
            user_id=session.user.id,
            email=session.user.email,
            session=session,
        )

    def logout(self, session_secret: str | None, client_ip: str) -> None:
        if not session_secret:
            return
        if self.repository.revoke_session(
            session_hash=hash_secret(session_secret), now=self.clock(), client_ip=client_ip
        ):
            logger.info("logout")

    def role_for(self, email: str) -> Role:
        # Computed on every request, so removing an operator takes effect at once.
        return "operator" if email in self.settings.operator_emails else "user"

    def list_events(self, *, limit: int, before_id: int | None) -> list[AuthEventRecord]:
        return self.repository.list_events(limit=limit, before_id=before_id)

    # --- helpers --------------------------------------------------------------

    def _enforce(
        self,
        scope: str,
        value: str,
        now: datetime,
        *,
        client_ip: str,
        email_masked: str | None = None,
    ) -> None:
        decision = self.limiter.hit(scope, value, now)
        if decision.allowed:
            return
        # auth_events is append-only, so audit only the first denial per key
        # and window; otherwise every over-limit request would add a row.
        if decision.first_denial:
            self.repository.record_event(
                NewAuthEvent(
                    "rate_limited",
                    occurred_at=now,
                    reason=scope,
                    email_masked=email_masked,
                    client_ip=client_ip,
                )
            )
        logger.warning("rate limited", extra={"scope": scope})
        raise RateLimited(decision.retry_after)
