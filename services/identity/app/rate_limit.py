"""Persistent fixed-window rate limits (LM-052).

Counters live in the identity database, so limits hold across restarts and
across any number of service replicas. Each attempt is one atomic upsert that
returns the new count; an attempt is allowed while the count is within the
limit. Attempts over the limit are still counted, so hammering stays blocked
until the window ends. Only the first denied attempt in a window is marked
``first_denial``, so the caller audits one ``rate_limited`` event per key and
window instead of one per request.

Keys are SHA-256 digests of ``scope:value`` (a normalized email or a client
IP), so the table holds no plaintext address. An IPv6 client is keyed by its
/64 network, because one subscriber usually controls a whole /64 and could
otherwise rotate addresses to escape the per-IP limits.

Trade-offs (docs/authentication.md): a fixed window allows up to twice the
limit across a window boundary, and anyone can keep an address's per-email
limit exhausted, window after window, for as long as they keep sending.
"""

from __future__ import annotations

import hashlib
import ipaddress
from dataclasses import dataclass
from datetime import UTC, datetime

from services.identity.app.repository import IdentityRepository

LOGIN_EMAIL = "login_email"
LOGIN_IP = "login_ip"
VERIFY_IP = "verify_ip"
IP_SCOPES = frozenset({LOGIN_IP, VERIFY_IP})
IPV6_PREFIX = 64


@dataclass(frozen=True)
class RateLimitDecision:
    allowed: bool
    retry_after: int
    first_denial: bool = False


def key_hash(scope: str, value: str) -> str:
    return hashlib.sha256(f"{scope}:{value}".encode()).hexdigest()


def client_key(value: str) -> str:
    """The per-IP key: an IPv4 address as is, an IPv6 address as its /64."""
    try:
        address = ipaddress.ip_address(value)
    except ValueError:
        return value  # e.g. "testclient" or "unknown"
    if isinstance(address, ipaddress.IPv6Address):
        if address.ipv4_mapped is not None:
            return str(address.ipv4_mapped)
        return str(ipaddress.ip_network(f"{address}/{IPV6_PREFIX}", strict=False))
    return str(address)


def window_start(now: datetime, window_seconds: int) -> datetime:
    epoch = int(now.timestamp())
    return datetime.fromtimestamp(epoch - epoch % window_seconds, UTC)


class RateLimiter:
    def __init__(
        self, repository: IdentityRepository, *, window_seconds: int, limits: dict[str, int]
    ) -> None:
        self.repository = repository
        self.window_seconds = window_seconds
        self.limits = limits

    def hit(self, scope: str, value: str, now: datetime) -> RateLimitDecision:
        start = window_start(now, self.window_seconds)
        if scope in IP_SCOPES:
            value = client_key(value)
        count = self.repository.count_attempt(
            scope=scope, key_hash=key_hash(scope, value), window_start=start, now=now
        )
        remaining = self.window_seconds - int((now - start).total_seconds())
        limit = self.limits[scope]
        return RateLimitDecision(
            allowed=count <= limit,
            retry_after=max(1, remaining),
            first_denial=count == limit + 1,
        )
