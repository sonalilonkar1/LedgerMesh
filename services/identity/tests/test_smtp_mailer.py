"""SmtpMailer: the delivery path deployed environments use (LM-052).

The Mailpit end-to-end test only covers plain SMTP without a login, so these
tests replace smtplib's clients with a recorder and check the TLS and login
order that a provider connection relies on.
"""

import ssl

import pytest

from services.identity.app import mailer as mailer_module
from services.identity.app.mailer import SUBJECT, SmtpMailer

from .fakes import make_settings

LINK = "https://ledgermesh.example/v1/auth/verify#token=abc_DEF-123"
TO = "alice@example.com"
API_KEY = "provider-api-key"


class RecordingSmtp:
    instances: list["RecordingSmtp"] = []

    def __init__(self, host, port, *, timeout, context=None):
        self.host, self.port, self.timeout, self.context = host, port, timeout, context
        self.calls: list[tuple] = []
        self.messages = []
        RecordingSmtp.instances.append(self)

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.calls.append(("quit",))

    def starttls(self, *, context):
        self.calls.append(("starttls", context))

    def login(self, user, secret):
        self.calls.append(("login", user, secret))

    def send_message(self, message):
        self.calls.append(("send_message",))
        self.messages.append(message)

    def names(self) -> list[str]:
        return [call[0] for call in self.calls]


class RecordingSmtpSsl(RecordingSmtp):
    pass


@pytest.fixture
def smtp(monkeypatch):
    RecordingSmtp.instances = []
    monkeypatch.setattr(mailer_module.smtplib, "SMTP", RecordingSmtp)
    monkeypatch.setattr(mailer_module.smtplib, "SMTP_SSL", RecordingSmtpSsl)
    return RecordingSmtp.instances


def send(**settings):
    SmtpMailer(make_settings(**settings)).send_login_link(to=TO, link=LINK, expires_in_minutes=15)


def assert_verifying(context):
    assert isinstance(context, ssl.SSLContext)
    assert context.verify_mode == ssl.CERT_REQUIRED
    assert context.check_hostname is True


def test_starttls_comes_before_login_and_sending(smtp):
    send(
        smtp_host="smtp.provider.example",
        smtp_port=587,
        smtp_starttls=True,
        smtp_username="apikey",
        smtp_secret=API_KEY,
        smtp_timeout_seconds=7,
    )

    [client] = smtp
    assert type(client) is RecordingSmtp
    assert (client.host, client.port, client.timeout) == ("smtp.provider.example", 587, 7)
    assert client.names() == ["starttls", "login", "send_message", "quit"]
    assert_verifying(client.calls[0][1])
    assert client.calls[1] == ("login", "apikey", API_KEY)


def test_implicit_tls_uses_smtp_ssl_with_a_verifying_context(smtp):
    send(smtp_host="smtp.provider.example", smtp_port=465, smtp_ssl=True, smtp_username="apikey")

    [client] = smtp
    assert type(client) is RecordingSmtpSsl
    assert client.port == 465
    assert_verifying(client.context)
    assert client.names() == ["login", "send_message", "quit"]


def test_no_login_without_a_username(smtp):
    send(smtp_host="mailpit", smtp_secret="ignored")

    [client] = smtp
    assert type(client) is RecordingSmtp
    assert client.names() == ["send_message", "quit"]


def test_message_headers_and_link(smtp):
    send(email_from="LedgerMesh <no-reply@ledgermesh.example>")

    message = smtp[0].messages[0]
    assert message["To"] == TO
    assert message["From"] == "LedgerMesh <no-reply@ledgermesh.example>"
    assert message["Subject"] == SUBJECT
    body = message.get_content()
    assert LINK in body
    assert "#token=" in body
    assert "expires in 15 minutes" in body
