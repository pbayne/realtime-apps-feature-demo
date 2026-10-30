"""Parameter schemas for the four action kinds (SPEC 12). Every action is validated here before it is stored or executed.

Strict on purpose: unknown keys are refused, addresses must look like addresses, subjects are one line, and times
carry a timezone. The model proposes params through the same validator a human-entered form goes through.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Annotated, Any, Literal, Optional

from models import ACTION_KINDS
from pydantic import AfterValidator, BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+'\-]+@[A-Za-z0-9\-]+(\.[A-Za-z0-9\-]+)+$")
PHONE_RE = re.compile(r"^\+?[0-9 ().\-]{5,40}$")
DEFAULT_REMINDERS = [{"method": "email", "minutes": 1440}, {"method": "display", "minutes": 10}]


class ActionValidationError(ValueError):
    """Params do not match the kind's schema. The message is safe to show the user."""


def _check_email(value: str) -> str:
    value = value.strip()
    if len(value) > 254 or not EMAIL_RE.match(value):
        raise ValueError(f"not an email address: {value[:80]!r}")
    return value


def _one_line(value: str) -> str:
    if "\r" in value or "\n" in value:
        raise ValueError("must be a single line")
    return value


Email = Annotated[str, AfterValidator(_check_email)]
OneLine = Annotated[str, AfterValidator(_one_line)]


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class SendEmailParams(_Strict):
    to: list[Email] = Field(min_length=1, max_length=20)
    cc: list[Email] = Field(default_factory=list, max_length=20)
    subject: OneLine = Field(min_length=1, max_length=200)
    body_text: str = Field(min_length=1, max_length=10000)


class Reminder(_Strict):
    method: Literal["email", "display"]
    minutes: int = Field(ge=0, le=40320)


class CreateMeetingParams(_Strict):
    title: OneLine = Field(min_length=1, max_length=200)
    attendees: list[Email] = Field(min_length=1, max_length=50)
    start_iso: str = Field(min_length=10, max_length=40)
    duration_min: int = Field(ge=5, le=1440)
    description: str = Field(default="", max_length=5000)
    location: Optional[OneLine] = Field(default=None, max_length=200)
    reminders: list[Reminder] = Field(default_factory=lambda: [Reminder(**r) for r in DEFAULT_REMINDERS], max_length=5)

    @field_validator("start_iso")
    @classmethod
    def _aware(cls, value: str) -> str:
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("start_iso must be ISO 8601, for example 2026-11-03T14:00:00-05:00") from exc
        if dt.tzinfo is None or dt.utcoffset() is None:
            raise ValueError("start_iso must include a timezone offset")
        return dt.isoformat()


class ContactFields(_Strict):
    first_name: Optional[OneLine] = Field(default=None, max_length=100)
    last_name: Optional[OneLine] = Field(default=None, max_length=100)
    email: Optional[Email] = None
    phone: Optional[str] = None
    company: Optional[OneLine] = Field(default=None, max_length=200)

    @field_validator("phone")
    @classmethod
    def _phone(cls, value: Optional[str]) -> Optional[str]:
        if value is not None and not PHONE_RE.match(value):
            raise ValueError("phone must be 5 to 40 digits, spaces or + ( ) - characters")
        return value

    @model_validator(mode="after")
    def _something_to_change(self) -> "ContactFields":
        if not self.model_dump(exclude_none=True):
            raise ValueError("fields must set at least one of first_name, last_name, email, phone, company")
        return self


class _ContactRef(_Strict):
    contact_id: Optional[str] = Field(default=None, min_length=1, max_length=64)
    email: Optional[Email] = None

    @model_validator(mode="after")
    def _one_ref(self) -> "_ContactRef":
        if bool(self.contact_id) == bool(self.email):
            raise ValueError("give exactly one of contact_id or email")
        return self


class UpdateContactParams(_ContactRef):
    fields: ContactFields


class EnrollParams(_ContactRef):
    program: str = Field(min_length=1, max_length=100)
    note: Optional[str] = Field(default=None, max_length=500)


PARAM_MODELS: dict[str, type[BaseModel]] = {
    "send_email": SendEmailParams,
    "create_meeting": CreateMeetingParams,
    "update_contact": UpdateContactParams,
    "enroll": EnrollParams,
}

assert set(PARAM_MODELS) == set(ACTION_KINDS)


def validate_params(kind: str, raw: Any) -> dict[str, Any]:
    """Normalised params as plain JSON, or ActionValidationError. Enrollment programs are checked by the service,
    which knows the playbook."""
    model = PARAM_MODELS.get(kind)
    if model is None:
        raise ActionValidationError(f"unknown action kind {kind!r}")
    if not isinstance(raw, dict):
        raise ActionValidationError("params must be an object")
    try:
        return model.model_validate(raw).model_dump(mode="json")
    except ValidationError as exc:
        first = exc.errors()[0]
        where = ".".join(str(p) for p in first.get("loc", ()))
        msg = first.get("msg", "invalid")
        msg = msg.removeprefix("Value error, ")
        raise ActionValidationError(f"{kind}: {where + ': ' if where else ''}{msg}") from None


def contact_key(params: dict[str, Any]) -> str:
    """The contact an update or enrollment refers to, as one comparable string."""
    if params.get("contact_id"):
        return f"id:{params['contact_id']}"
    return f"email:{str(params.get('email') or '').lower()}"


def summarize(kind: str, params: dict[str, Any]) -> str:
    """One plain-language line for the card, built from the params (never from model prose)."""
    if kind == "send_email":
        who = _first_name(params["to"][0])
        more = f" and {len(params['to']) - 1} more" if len(params["to"]) > 1 else ""
        return f"Email {who}{more}: {params['subject']}"
    if kind == "create_meeting":
        start = datetime.fromisoformat(params["start_iso"])
        when = start.strftime("%a %d %b %Y, %H:%M ") + _offset_label(start)
        names = ", ".join(_first_name(a) for a in params["attendees"][:3])
        return f"Meeting “{params['title']}” with {names} on {when} ({params['duration_min']} min)"
    if kind == "update_contact":
        who = params["fields"].get("first_name") or params.get("email") or "contact"
        changed = ", ".join(k.replace("_", " ") for k in params["fields"])
        return f"Update {who}: {changed}"
    if kind == "enroll":
        who = params.get("email") or "contact"
        return f"Enroll {_first_name(who) if params.get('email') else who} in {params['program']}"
    return kind


def _first_name(address: str) -> str:
    local = address.split("@", 1)[0]
    word = re.split(r"[._\-]", local)[0] or address
    return word[:1].upper() + word[1:]


def _offset_label(dt: datetime) -> str:
    off = dt.utcoffset()
    if off is None:
        return ""
    minutes = int(off.total_seconds() // 60)
    sign = "+" if minutes >= 0 else "-"
    hh, mm = divmod(abs(minutes), 60)
    return f"UTC{sign}{hh:02d}:{mm:02d}" if mm else f"UTC{sign}{hh:02d}"
