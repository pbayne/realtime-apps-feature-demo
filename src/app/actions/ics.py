"""RFC 5545 invites for create_meeting (SPEC 12).

Output rules that calendars depend on: CRLF line endings, content lines folded at 75 octets (never inside a UTF-8
character), TEXT values escaped (backslash, semicolon, comma, newline), times in UTC with the Z suffix, one VEVENT
with a stable UID so a CANCEL or an update matches the original invite.

Reminders are VALARMs: an EMAIL alarm one day before (-P1D by default) and a DISPLAY alarm ten minutes before.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Optional

CRLF = "\r\n"
PRODID = "-//Databricks//Sidekick Actions 0.7//EN"
MAX_OCTETS = 75


def fmt_utc(dt: datetime) -> str:
    """Aware datetime -> 20261103T190000Z. Naive datetimes are refused: an invite must not guess a zone."""
    if dt.tzinfo is None or dt.utcoffset() is None:
        raise ValueError("calendar times must be timezone-aware")
    return dt.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def escape_text(value: str) -> str:
    """RFC 5545 section 3.3.11 TEXT escaping."""
    return (value.replace("\\", "\\\\").replace(";", "\\;").replace(",", "\\,")
            .replace("\r\n", "\n").replace("\r", "\n").replace("\n", "\\n"))


def fold(line: str) -> list[str]:
    """Split one content line into physical lines of at most 75 octets (RFC 5545 3.1). A continuation line starts
    with one space, so its content is at most 74 octets. A multi-byte character is never split."""
    chunks: list[str] = []
    current = ""
    size = 0
    limit = MAX_OCTETS
    for ch in line:
        width = len(ch.encode("utf-8"))
        if size + width > limit:
            chunks.append(current)
            current, size, limit = ch, width, MAX_OCTETS - 1
        else:
            current += ch
            size += width
    chunks.append(current)
    return [chunks[0]] + [" " + chunk for chunk in chunks[1:]]


def content_line(name: str, value: str) -> list[str]:
    """`name` may carry parameters (ATTENDEE;ROLE=REQ-PARTICIPANT); the value is already escaped."""
    return fold(f"{name}:{value}")


def duration_trigger(minutes: int) -> str:
    """Minutes before the start as an RFC 5545 duration: 1440 -> -P1D, 10 -> -PT10M, 0 -> PT0S."""
    if minutes < 0:
        raise ValueError("reminder minutes must be zero or more")
    if minutes == 0:
        return "PT0S"
    if minutes % 1440 == 0:
        return f"-P{minutes // 1440}D"
    return f"-PT{minutes}M"


def _mailto(address: str) -> str:
    return f"mailto:{address}"


def build_event_lines(*, uid: str, title: str, start: datetime, duration_min: int, attendees: Iterable[str],
                      organizer: str, description: str, location: Optional[str], reminders: list[dict[str, Any]],
                      dtstamp: datetime, sequence: int, status: str, cancel: bool) -> list[str]:
    end = start + timedelta(minutes=duration_min)
    attendee_list = list(attendees)
    lines: list[str] = ["BEGIN:VEVENT"]
    lines += content_line("UID", uid)
    lines += content_line("DTSTAMP", fmt_utc(dtstamp))
    lines += content_line("SEQUENCE", str(sequence))
    lines += content_line("DTSTART", fmt_utc(start))
    lines += content_line("DTEND", fmt_utc(end))
    lines += content_line("SUMMARY", escape_text(title))
    lines += content_line("STATUS", status)
    lines += content_line("ORGANIZER", _mailto(organizer))
    for addr in attendee_list:
        params = "ATTENDEE;ROLE=REQ-PARTICIPANT;PARTSTAT=NEEDS-ACTION;RSVP=" + ("FALSE" if cancel else "TRUE")
        lines += content_line(params, _mailto(addr))
    if description:
        lines += content_line("DESCRIPTION", escape_text(description))
    if location:
        lines += content_line("LOCATION", escape_text(location))
    if not cancel:
        for rem in reminders:
            lines += _alarm_lines(rem, title=title, attendees=attendee_list)
    lines.append("END:VEVENT")
    return lines


def _alarm_lines(rem: dict[str, Any], *, title: str, attendees: list[str]) -> list[str]:
    trigger = duration_trigger(int(rem["minutes"]))
    text = escape_text(f"Reminder: {title}")
    lines = ["BEGIN:VALARM"]
    if rem["method"] == "email":
        lines += content_line("ACTION", "EMAIL")
        lines += content_line("TRIGGER", trigger)
        lines += content_line("SUMMARY", text)
        lines += content_line("DESCRIPTION", text)
        for addr in attendees:
            lines += content_line("ATTENDEE", _mailto(addr))
    else:
        lines += content_line("ACTION", "DISPLAY")
        lines += content_line("TRIGGER", trigger)
        lines += content_line("DESCRIPTION", text)
    lines.append("END:VALARM")
    return lines


def _calendar(method: str, event_lines: list[str]) -> str:
    lines = ["BEGIN:VCALENDAR", "VERSION:2.0", f"PRODID:{PRODID}", "CALSCALE:GREGORIAN", f"METHOD:{method}"]
    lines += event_lines
    lines.append("END:VCALENDAR")
    return CRLF.join(lines) + CRLF


def build_invite(*, uid: str, title: str, start: datetime, duration_min: int, attendees: list[str], organizer: str,
                 reminders: list[dict[str, Any]], dtstamp: datetime, description: str = "",
                 location: Optional[str] = None, sequence: int = 0) -> str:
    """METHOD:REQUEST invite with alarms. Returns the text (CRLF lines)."""
    event = build_event_lines(uid=uid, title=title, start=start, duration_min=duration_min, attendees=attendees,
                              organizer=organizer, description=description, location=location, reminders=reminders,
                              dtstamp=dtstamp, sequence=sequence, status="CONFIRMED", cancel=False)
    return _calendar("REQUEST", event)


def build_cancel(*, uid: str, title: str, start: datetime, duration_min: int, attendees: list[str], organizer: str,
                 dtstamp: datetime, sequence: int = 1) -> str:
    """METHOD:CANCEL for an earlier invite with the same UID (SEQUENCE must be higher than the invite's)."""
    event = build_event_lines(uid=uid, title=title, start=start, duration_min=duration_min, attendees=attendees,
                              organizer=organizer, description="", location=None, reminders=[], dtstamp=dtstamp,
                              sequence=sequence, status="CANCELLED", cancel=True)
    return _calendar("CANCEL", event)


# ---------------------------------------------------------------- strict reader (tests and parity checks)

class IcsError(ValueError):
    pass


def parse_strict(data: str | bytes) -> dict[str, Any]:
    """Checks the RFC 5545 rules this module promises and returns the properties of the first VEVENT:
    CRLF only, lines <= 75 octets, balanced BEGIN/END, required properties, UTC times, unescaped TEXT.
    Raises IcsError. Used by the tests next to the icalendar round trip."""
    text = data.decode("utf-8") if isinstance(data, bytes) else data
    if "\n" in text.replace("\r\n", "") or "\r" in text.replace("\r\n", ""):
        raise IcsError("line endings must be CRLF only")
    if not text.endswith(CRLF):
        raise IcsError("must end with CRLF")
    physical = text.split(CRLF)[:-1]
    for raw in physical:
        if len(raw.encode("utf-8")) > MAX_OCTETS:
            raise IcsError(f"line longer than 75 octets: {raw[:40]!r}")
    logical: list[str] = []
    for raw in physical:
        if raw[:1] in (" ", "\t"):
            if not logical:
                raise IcsError("folded line with nothing to fold")
            logical[-1] += raw[1:]
        else:
            logical.append(raw)
    stack: list[str] = []
    props: dict[str, Any] = {}
    attendees: list[str] = []
    alarms: list[dict[str, str]] = []
    current_alarm: Optional[dict[str, str]] = None
    for line in logical:
        name_params, _, value = line.partition(":")
        name = name_params.split(";", 1)[0].upper()
        if name == "BEGIN":
            stack.append(value)
            if value == "VALARM" and current_alarm is None:
                current_alarm = {}
            continue
        if name == "END":
            if not stack or stack[-1] != value:
                raise IcsError(f"unbalanced END:{value}")
            stack.pop()
            if value == "VALARM" and current_alarm is not None:
                alarms.append(current_alarm)
                current_alarm = None
            continue
        if current_alarm is not None:
            current_alarm[name] = value
        elif stack == ["VCALENDAR", "VEVENT"]:
            if name == "ATTENDEE":
                attendees.append(value)
            props.setdefault(name, value)
    if stack:
        raise IcsError(f"unclosed blocks: {stack}")
    if "UID" not in props and "DTSTART" not in props:
        raise IcsError("no VEVENT found")
    for required in ("UID", "DTSTAMP", "DTSTART", "SUMMARY"):
        if required not in props:
            raise IcsError(f"VEVENT is missing {required}")
    for key in ("DTSTAMP", "DTSTART", "DTEND"):
        if key in props and not props[key].endswith("Z"):
            raise IcsError(f"{key} must be UTC (Z)")
    props["_alarms"] = alarms
    props["_attendees"] = attendees
    return props
