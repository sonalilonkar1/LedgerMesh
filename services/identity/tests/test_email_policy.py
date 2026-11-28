"""Server-side email validation and normalization (LM-052)."""

import pytest

from services.identity.app.email_address import (
    InvalidEmailAddress,
    is_blocked_domain,
    mask_email,
    normalize_email,
)

from .fakes import make_harness

GENERIC_INVALID = {
    "type": "urn:ledgermesh:identity:invalid-email",
    "title": "Invalid email address",
    "status": 422,
    "detail": "Enter a valid email address.",
}

INVALID = [
    "",
    "   ",
    "no-at-sign.example.com",
    "two@@example.com",
    "a@b@example.com",
    "trailing-dot@example.com.",
    "double..dot@example.com",
    '"quoted"@example.com',
    "user@[192.0.2.1]",
    "with space@example.com",
    "ü@example.com",
    "user@localhost",
    "user@ledgermesh.local",
    "Display Name <user@example.com>",
    "a" * 245 + "@example.com",
]


@pytest.mark.parametrize("raw", INVALID)
def test_invalid_addresses_are_rejected_by_the_validator(raw):
    with pytest.raises(InvalidEmailAddress):
        normalize_email(raw)


@pytest.mark.parametrize("raw", INVALID + ["x" * 321])
def test_invalid_address_gets_one_generic_422_that_does_not_echo_input(raw):
    harness = make_harness()

    response = harness.request_login(raw)

    assert response.status_code == 422
    assert response.headers["content-type"].startswith("application/problem+json")
    assert response.json() == GENERIC_INVALID
    assert harness.mailer.sent == []
    assert harness.repository.users == {}


@pytest.mark.parametrize(
    ("raw", "normalized"),
    [
        ("  Alice@Example.COM  ", "alice@example.com"),
        ("ALICE@EXAMPLE.COM", "alice@example.com"),
        # No provider-specific folding: dots and +tags stay distinct.
        ("first.last+ledger@Example.com", "first.last+ledger@example.com"),
        ("firstlast@example.com", "firstlast@example.com"),
        # Internationalized domains are stored in ASCII form.
        ("user@München.de", "user@xn--mnchen-3ya.de"),
    ],
)
def test_normalization(raw, normalized):
    assert normalize_email(raw) == normalized


def test_case_variants_map_to_one_user():
    harness = make_harness()

    harness.request_login("Alice@Example.com")
    harness.request_login("  alice@EXAMPLE.COM ")

    assert list(harness.repository.users) == ["alice@example.com"]
    assert {sent.to for sent in harness.mailer.sent} == {"alice@example.com"}


def test_deliverability_is_not_checked_by_default(monkeypatch):
    import dns.resolver

    def no_dns(*_args, **_kwargs):
        raise AssertionError("DNS must not be queried when deliverability checks are off")

    monkeypatch.setattr(dns.resolver.Resolver, "resolve", no_dns)
    harness = make_harness()

    assert harness.request_login("someone@example.org").status_code == 202


def test_blocked_domains_get_the_same_generic_422():
    harness = make_harness(blocked_email_domains=frozenset({"mailinator.com"}))

    for raw in ("x@mailinator.com", "x@eu.MAILINATOR.com"):
        response = harness.request_login(raw)
        assert response.status_code == 422
        assert response.json() == GENERIC_INVALID
    assert harness.request_login("x@example.com").status_code == 202


def test_blocked_domain_matching():
    blocked = frozenset({"mailinator.com"})
    assert is_blocked_domain("a@mailinator.com", blocked)
    assert is_blocked_domain("a@sub.mailinator.com", blocked)
    assert not is_blocked_domain("a@notmailinator.com", blocked)


def test_mask_email_keeps_only_the_first_character_and_domain():
    assert mask_email("devin@example.com") == "d***@example.com"
