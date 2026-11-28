"""Email address validation and normalization policy (LM-052).

Syntax is checked server-side by the maintained ``email-validator`` library.
A valid address proves nothing about who owns the mailbox; ownership is
established only when the emailed single-use link is used.

Policy (see docs/authentication.md, "Policy choices"):

- Surrounding whitespace is stripped, then the whole address is lowercased.
  Mail providers treat the local part case-insensitively in practice, and one
  canonical form is what makes the database unique constraint meaningful.
- No provider-specific folding: dots and ``+tags`` are kept, because merging
  them would join distinct mailboxes at providers that do not ignore them.
- Quoted local parts, IP-literal domains and internationalized (SMTPUTF8) local
  parts are rejected; internationalized domains are stored in ASCII (IDNA) form.
  Special-use domains such as ``.local`` and ``.test`` are rejected by the library.
- DNS deliverability checks are off unless ``check_deliverability`` is set.
- An optional denylist of domains (for example disposable-mail providers) is
  applied by the caller; no list is bundled.
"""

from __future__ import annotations

from email_validator import EmailNotValidError, validate_email

MAX_EMAIL_LENGTH = 254


class InvalidEmailAddress(ValueError):
    """The address is not acceptable. The message is never shown to users."""


def normalize_email(raw: str, *, check_deliverability: bool = False) -> str:
    candidate = raw.strip()
    if not candidate or len(candidate) > MAX_EMAIL_LENGTH:
        raise InvalidEmailAddress("length")
    try:
        validated = validate_email(
            candidate,
            check_deliverability=check_deliverability,
            allow_smtputf8=False,
            allow_quoted_local=False,
            allow_domain_literal=False,
            allow_display_name=False,
        )
    except EmailNotValidError as exc:
        raise InvalidEmailAddress("syntax") from exc
    normalized = validated.ascii_email.lower()
    if len(normalized) > MAX_EMAIL_LENGTH:
        raise InvalidEmailAddress("length")
    return normalized


def email_domain(normalized: str) -> str:
    return normalized.rsplit("@", 1)[1]


def is_blocked_domain(normalized: str, blocked: frozenset[str]) -> bool:
    """True if the domain, or a parent domain, is on the denylist."""
    labels = email_domain(normalized).split(".")
    return any(".".join(labels[index:]) in blocked for index in range(len(labels)))


def mask_email(normalized: str) -> str:
    """``devin@example.com`` -> ``d***@example.com``; for audit rows only."""
    local, _, domain = normalized.rpartition("@")
    return f"{local[:1]}***@{domain}"
