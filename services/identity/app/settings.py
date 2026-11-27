"""Identity Service configuration from environment variables (LM-052).

The other services read ``os.environ`` where a value is used (for example
``services/audit_ledger/app/repository.py``). Identity has more settings, and
several depend on each other, so they are parsed and checked once here.
``get_settings()`` is cached and can be replaced with
``app.dependency_overrides`` in tests.

Outside ``LEDGERMESH_ENV=local`` or ``test`` the service refuses to start with
a plain-HTTP login link or an unencrypted SMTP connection: both would expose
the one-time login token. An unset ``LEDGERMESH_ENV`` counts as deployed, so a
missing variable fails safe (secure cookies, strict checks).
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from functools import lru_cache
from urllib.parse import urlsplit

from services.identity.app.email_address import InvalidEmailAddress, normalize_email

LOCAL_ENVIRONMENTS = frozenset({"local", "test"})


class SettingsError(ValueError):
    """The configuration is invalid; the service must not start."""


def _bool(environ: Mapping[str, str], name: str, default: bool) -> bool:
    raw = environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    value = raw.strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    raise SettingsError(f"{name} must be true or false")


def _int(environ: Mapping[str, str], name: str, default: int, minimum: int = 1) -> int:
    raw = environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise SettingsError(f"{name} must be an integer") from exc
    if value < minimum:
        raise SettingsError(f"{name} must be at least {minimum}")
    return value


def _list(environ: Mapping[str, str], name: str) -> list[str]:
    return [item.strip() for item in environ.get(name, "").split(",") if item.strip()]


@dataclass(frozen=True)
class IdentitySettings:
    environment: str = ""
    login_link_url: str = "http://localhost:8005/v1/auth/verify"
    login_token_ttl_seconds: int = 900
    session_idle_seconds: int = 1800
    session_absolute_seconds: int = 43200
    session_cookie_name: str = "lm_session"
    operator_emails: frozenset[str] = frozenset()
    check_deliverability: bool = False
    blocked_email_domains: frozenset[str] = frozenset()
    rate_limit_window_seconds: int = 900
    login_limit_per_email: int = 5
    login_limit_per_ip: int = 20
    verify_limit_per_ip: int = 30
    smtp_host: str = "localhost"
    smtp_port: int = 1025
    smtp_starttls: bool = False
    smtp_ssl: bool = False
    smtp_username: str = ""
    # The provider's SMTP API key. Never logged or shown in repr().
    smtp_secret: str = field(default="", repr=False)
    smtp_timeout_seconds: int = 10
    email_from: str = "LedgerMesh <no-reply@example.com>"

    @property
    def is_local(self) -> bool:
        return self.environment in LOCAL_ENVIRONMENTS

    @property
    def secure_cookies(self) -> bool:
        return not self.is_local

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> IdentitySettings:
        environ = os.environ if environ is None else environ
        defaults = cls()
        try:
            operators = frozenset(
                normalize_email(email, check_deliverability=False)
                for email in _list(environ, "IDENTITY_OPERATOR_EMAILS")
            )
        except InvalidEmailAddress as exc:
            raise SettingsError("IDENTITY_OPERATOR_EMAILS contains an invalid address") from exc
        settings = cls(
            environment=environ.get("LEDGERMESH_ENV", "").strip(),
            login_link_url=environ.get("IDENTITY_LOGIN_LINK_URL", defaults.login_link_url),
            login_token_ttl_seconds=_int(
                environ, "IDENTITY_LOGIN_TOKEN_TTL_SECONDS", defaults.login_token_ttl_seconds
            ),
            session_idle_seconds=_int(
                environ, "IDENTITY_SESSION_IDLE_SECONDS", defaults.session_idle_seconds
            ),
            session_absolute_seconds=_int(
                environ, "IDENTITY_SESSION_ABSOLUTE_SECONDS", defaults.session_absolute_seconds
            ),
            session_cookie_name=environ.get(
                "IDENTITY_SESSION_COOKIE_NAME", defaults.session_cookie_name
            ),
            operator_emails=operators,
            check_deliverability=_bool(environ, "IDENTITY_EMAIL_CHECK_DELIVERABILITY", False),
            blocked_email_domains=frozenset(
                domain.lower() for domain in _list(environ, "IDENTITY_BLOCKED_EMAIL_DOMAINS")
            ),
            rate_limit_window_seconds=_int(
                environ, "IDENTITY_RATE_LIMIT_WINDOW_SECONDS", defaults.rate_limit_window_seconds
            ),
            login_limit_per_email=_int(
                environ, "IDENTITY_LOGIN_LIMIT_PER_EMAIL", defaults.login_limit_per_email
            ),
            login_limit_per_ip=_int(
                environ, "IDENTITY_LOGIN_LIMIT_PER_IP", defaults.login_limit_per_ip
            ),
            verify_limit_per_ip=_int(
                environ, "IDENTITY_VERIFY_LIMIT_PER_IP", defaults.verify_limit_per_ip
            ),
            smtp_host=environ.get("IDENTITY_SMTP_HOST", defaults.smtp_host),
            smtp_port=_int(environ, "IDENTITY_SMTP_PORT", defaults.smtp_port),
            smtp_starttls=_bool(environ, "IDENTITY_SMTP_STARTTLS", False),
            smtp_ssl=_bool(environ, "IDENTITY_SMTP_SSL", False),
            smtp_username=environ.get("IDENTITY_SMTP_USERNAME", ""),
            smtp_secret=environ.get("IDENTITY_SMTP_SECRET", ""),
            smtp_timeout_seconds=_int(
                environ, "IDENTITY_SMTP_TIMEOUT_SECONDS", defaults.smtp_timeout_seconds
            ),
            email_from=environ.get("IDENTITY_EMAIL_FROM", defaults.email_from),
        )
        settings.validate()
        return settings

    def validate(self) -> None:
        if not self.session_cookie_name or not self.session_cookie_name.isidentifier():
            raise SettingsError("IDENTITY_SESSION_COOKIE_NAME must be a simple identifier")
        if self.session_idle_seconds > self.session_absolute_seconds:
            raise SettingsError(
                "IDENTITY_SESSION_IDLE_SECONDS must not exceed IDENTITY_SESSION_ABSOLUTE_SECONDS"
            )
        if self.smtp_starttls and self.smtp_ssl:
            raise SettingsError("Set only one of IDENTITY_SMTP_STARTTLS and IDENTITY_SMTP_SSL")
        link = urlsplit(self.login_link_url)
        if link.scheme not in {"http", "https"} or not link.netloc:
            raise SettingsError("IDENTITY_LOGIN_LINK_URL must be an absolute http(s) URL")
        if link.fragment:
            raise SettingsError("IDENTITY_LOGIN_LINK_URL must not contain a fragment")
        if self.is_local:
            return
        # Deployed: the link and the email carry a live login token.
        if link.scheme != "https":
            raise SettingsError("IDENTITY_LOGIN_LINK_URL must use https outside local/test")
        if not (self.smtp_starttls or self.smtp_ssl):
            raise SettingsError(
                "Set IDENTITY_SMTP_STARTTLS or IDENTITY_SMTP_SSL outside local/test"
            )


@lru_cache
def get_settings() -> IdentitySettings:
    return IdentitySettings.from_env()
