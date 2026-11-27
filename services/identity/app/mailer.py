"""Delivery of the sign-in email (LM-052).

``SmtpMailer`` uses the standard library and works with any SMTP provider.
Locally it talks to Mailpit without encryption; deployed environments must use
STARTTLS or implicit TLS (enforced by ``IdentitySettings.validate``), with the
provider's credentials supplied from the deployment secret store.
"""

from __future__ import annotations

import smtplib
import ssl
from email.message import EmailMessage
from typing import Protocol

from services.identity.app.settings import IdentitySettings

SUBJECT = "Your LedgerMesh sign-in link"


class Mailer(Protocol):
    def send_login_link(self, *, to: str, link: str, expires_in_minutes: int) -> None: ...


def login_message(*, sender: str, to: str, link: str, expires_in_minutes: int) -> EmailMessage:
    message = EmailMessage()
    message["From"] = sender
    message["To"] = to
    message["Subject"] = SUBJECT
    message.set_content(
        "Use this link to sign in to LedgerMesh:\n\n"
        f"{link}\n\n"
        f"The link works once and expires in {expires_in_minutes} minutes. Opening it shows "
        "a confirmation page; nothing happens until you press the sign-in button.\n\n"
        "If you did not ask to sign in, you can ignore this email. Nobody can sign in "
        "without access to your mailbox.\n"
    )
    return message


class SmtpMailer:
    def __init__(self, settings: IdentitySettings) -> None:
        self.settings = settings

    def send_login_link(self, *, to: str, link: str, expires_in_minutes: int) -> None:
        settings = self.settings
        message = login_message(
            sender=settings.email_from, to=to, link=link, expires_in_minutes=expires_in_minutes
        )
        context = ssl.create_default_context()
        if settings.smtp_ssl:
            client: smtplib.SMTP = smtplib.SMTP_SSL(
                settings.smtp_host,
                settings.smtp_port,
                timeout=settings.smtp_timeout_seconds,
                context=context,
            )
        else:
            client = smtplib.SMTP(
                settings.smtp_host, settings.smtp_port, timeout=settings.smtp_timeout_seconds
            )
        with client:
            if settings.smtp_starttls:
                client.starttls(context=context)
            if settings.smtp_username:
                client.login(settings.smtp_username, settings.smtp_secret)
            client.send_message(message)
