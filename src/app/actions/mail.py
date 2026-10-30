"""MIME messages and the SMTP relay (SPEC 12).

No Google API credential is used (user decision): mail goes through an SMTP relay whose settings are secrets
(SMTP_HOST, SMTP_PORT, SMTP_USERNAME, SMTP_PASSWORD, SMTP_FROM). Without SMTP_HOST and SMTP_FROM the executor runs
in outbox mode: the same message is rendered, stored and downloadable, and nothing leaves the app.

A meeting is a multipart/mixed message: multipart/alternative (text/plain, and text/calendar with method=REQUEST)
plus invite.ics as an attachment. Google Calendar and Outlook read the calendar part as an invitation.
"""

from __future__ import annotations

import email.policy
import smtplib
import ssl
from dataclasses import dataclass
from datetime import datetime
from email.message import EmailMessage
from email.utils import format_datetime, make_msgid, parseaddr
from typing import Any, Optional

from config import Settings

MESSAGE_ID_DOMAIN = "sidekick.invalid"


@dataclass(frozen=True)
class SmtpConfig:
    host: str
    port: int
    username: Optional[str]
    password: Optional[str]
    timeout_s: float

    @classmethod
    def from_settings(cls, s: Settings) -> Optional["SmtpConfig"]:
        if not s.smtp_configured:
            return None
        return cls(host=s.smtp_host or "", port=s.smtp_port, username=s.smtp_username, password=s.smtp_password,
                   timeout_s=s.smtp_timeout_s)


def new_message_id(action_id: str) -> str:
    return make_msgid(idstring=action_id, domain=MESSAGE_ID_DOMAIN)


def build_message(*, sender: str, to: list[str], cc: list[str], subject: str, body_text: str, message_id: str,
                  date: datetime, invite: Optional[tuple[str, str]] = None) -> EmailMessage:
    """Plain email, or with `invite=(ics_text, method)` a calendar invitation (REQUEST or CANCEL)."""
    msg = EmailMessage()
    msg["From"] = sender
    msg["To"] = ", ".join(to)
    if cc:
        msg["Cc"] = ", ".join(cc)
    msg["Subject"] = subject
    msg["Date"] = format_datetime(date)
    msg["Message-ID"] = message_id
    msg.set_content(body_text)
    if invite is not None:
        ics_text, method = invite
        msg.add_alternative(ics_text, subtype="calendar", cte="base64")
        calendar_part = msg.get_payload()[-1]
        calendar_part.set_param("method", method)
        calendar_part.set_param("charset", "UTF-8")
        msg.add_attachment(ics_text, subtype="calendar", filename="invite.ics", cte="base64")
        msg.get_payload()[-1].set_param("method", method)
    return msg


def to_eml(msg: EmailMessage) -> str:
    """The exact bytes that would go over SMTP (CRLF line endings), as text. Headers use encoded words and every
    body part is base64 or quoted-printable, so the text is ASCII in practice; UTF-8 decoding keeps it lossless."""
    return msg.as_bytes(policy=email.policy.SMTP).decode("utf-8")


def envelope_from(msg: EmailMessage) -> str:
    return parseaddr(str(msg["From"]))[1]


def send_smtp(cfg: SmtpConfig, msg: EmailMessage, recipients: list[str]) -> None:
    """Blocking. Runs in a worker thread (ActionService). STARTTLS is required: a relay that refuses it fails."""
    context = ssl.create_default_context()
    with smtplib.SMTP(cfg.host, cfg.port, timeout=cfg.timeout_s) as smtp:
        smtp.ehlo()
        smtp.starttls(context=context)
        smtp.ehlo()
        if cfg.username:
            smtp.login(cfg.username, cfg.password or "")
        smtp.send_message(msg, from_addr=envelope_from(msg), to_addrs=recipients)


def describe(msg: EmailMessage) -> dict[str, Any]:
    """Headers and part types, for the response and the tests."""
    return {
        "from": str(msg["From"]), "to": str(msg["To"]), "cc": str(msg.get("Cc", "")),
        "subject": str(msg["Subject"]), "message_id": str(msg["Message-ID"]),
        "content_type": msg.get_content_type(),
        "parts": [p.get_content_type() for p in msg.walk()],
    }
