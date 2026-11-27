"""Session cookie attributes and the deployed-environment startup checks (LM-052)."""

import pytest
from fastapi.testclient import TestClient

from services.identity.app.main import app, get_settings
from services.identity.app.settings import IdentitySettings, SettingsError

from .fakes import make_harness

DEPLOYED = {
    "IDENTITY_LOGIN_LINK_URL": "https://ledgermesh.example.com/v1/auth/verify",
    "IDENTITY_SMTP_STARTTLS": "true",
}


def cookie_attributes(header: str) -> set[str]:
    return {part.strip().lower() for part in header.split(";")[1:]}


@pytest.mark.parametrize("environment", ["staging", "production", ""])
def test_deployed_cookies_are_secure_httponly_and_lax(environment):
    harness = make_harness(environment=environment)

    header = harness.sign_in().headers["set-cookie"]

    attributes = cookie_attributes(header)
    assert "secure" in attributes
    assert "httponly" in attributes
    assert "samesite=lax" in attributes
    assert "path=/" in attributes
    assert not any(a.startswith("domain=") for a in attributes)
    assert "max-age=43200" in attributes


@pytest.mark.parametrize("environment", ["local", "test"])
def test_local_cookies_are_not_secure_so_http_localhost_works(environment):
    harness = make_harness(environment=environment)

    attributes = cookie_attributes(harness.sign_in().headers["set-cookie"])

    assert "secure" not in attributes
    assert "httponly" in attributes
    assert "samesite=lax" in attributes


def test_logout_clears_the_cookie_with_the_same_attributes():
    harness = make_harness(environment="staging")
    harness.sign_in()

    attributes = cookie_attributes(harness.logout().headers["set-cookie"])

    assert {"secure", "httponly", "samesite=lax", "path=/", "max-age=0"} <= attributes


def test_unset_environment_counts_as_deployed():
    settings = IdentitySettings.from_env(DEPLOYED)

    assert settings.secure_cookies


@pytest.mark.parametrize(
    "environ",
    [
        {"LEDGERMESH_ENV": "staging", "IDENTITY_SMTP_STARTTLS": "true"},
        {
            "LEDGERMESH_ENV": "staging",
            "IDENTITY_LOGIN_LINK_URL": "http://ledgermesh.example.com/v1/auth/verify",
            "IDENTITY_SMTP_STARTTLS": "true",
        },
        {
            "LEDGERMESH_ENV": "staging",
            "IDENTITY_LOGIN_LINK_URL": DEPLOYED["IDENTITY_LOGIN_LINK_URL"],
        },
        {},
    ],
    ids=["default-http-link", "http-link", "smtp-without-tls", "unset-env-defaults"],
)
def test_deployed_settings_refuse_plain_http_links_and_unencrypted_smtp(environ):
    with pytest.raises(SettingsError):
        IdentitySettings.from_env(environ)


@pytest.mark.parametrize(
    ("environ", "message"),
    [
        ({"IDENTITY_OPERATOR_EMAILS": "not-an-address"}, "OPERATOR"),
        ({"IDENTITY_SESSION_IDLE_SECONDS": "50000"}, "IDLE"),
        ({"IDENTITY_SMTP_STARTTLS": "true", "IDENTITY_SMTP_SSL": "true"}, "only one"),
        ({"IDENTITY_LOGIN_LINK_URL": "http://x.example/verify#frag"}, "fragment"),
        ({"IDENTITY_LOGIN_TOKEN_TTL_SECONDS": "0"}, "TTL"),
        ({"IDENTITY_EMAIL_CHECK_DELIVERABILITY": "maybe"}, "DELIVERABILITY"),
    ],
)
def test_invalid_settings_are_rejected(environ, message):
    with pytest.raises(SettingsError, match=message):
        IdentitySettings.from_env({"LEDGERMESH_ENV": "local", **environ})


def test_local_settings_parse_and_normalize_operators():
    settings = IdentitySettings.from_env(
        {
            "LEDGERMESH_ENV": "local",
            "IDENTITY_OPERATOR_EMAILS": " Ops@Example.com , second@example.org",
            "IDENTITY_BLOCKED_EMAIL_DOMAINS": "Mailinator.com",
            "IDENTITY_SMTP_SECRET": "provider-key-value",
        }
    )

    assert settings.operator_emails == {"ops@example.com", "second@example.org"}
    assert settings.blocked_email_domains == {"mailinator.com"}
    assert "provider-key-value" not in repr(settings)
    assert not settings.secure_cookies


def test_service_does_not_start_with_an_unsafe_configuration():
    def unsafe():
        return IdentitySettings.from_env({"LEDGERMESH_ENV": "staging"})

    app.dependency_overrides[get_settings] = unsafe

    with pytest.raises(SettingsError), TestClient(app):
        pass


def test_service_starts_with_a_local_configuration():
    app.dependency_overrides[get_settings] = lambda: IdentitySettings.from_env(
        {"LEDGERMESH_ENV": "local"}
    )

    with TestClient(app) as client:
        assert client.get("/health").status_code == 200
