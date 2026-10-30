"""Shared small helpers and the JSON-shaped wire objects (SPEC section 3)."""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import asdict, dataclass, field
from typing import Any, Optional


def now_ms() -> int:
    """Epoch milliseconds. Used for emitted_at_ms / created_at_ms."""
    return int(time.time() * 1000)


def new_id() -> str:
    return str(uuid.uuid4())


def cap_text(text: str, limit: int, what: str = "text") -> str:
    """Typed turns, browser STT results and feedback text are capped in UTF-8 bytes (SPEC 10). Raises ValueError,
    which the socket handlers report back to the client without closing the socket."""
    if len(text.encode("utf-8")) > limit:
        raise ValueError(f"{what} is too long (max {limit} bytes)")
    return text


@dataclass
class Utterance:
    call_id: str
    seq: int
    speaker: str                 # "agent" | "customer"
    text: str
    t_start_ms: int
    t_end_ms: int
    emitted_at_ms: int
    playbook: str
    is_final: bool = True

    def to_wire(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class Card:
    card_id: str
    call_id: str
    seq: int
    kind: str
    title: str
    body: str
    confidence: float
    source_doc: Optional[str]
    source_clause: Optional[str]
    trigger: str
    status: str = "shown"
    created_at_ms: int = field(default_factory=now_ms)

    def without_body(self) -> dict[str, Any]:
        d = asdict(self)
        d.pop("body", None)
        return d

    def to_wire(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class CallRecord:
    call_id: str
    playbook: str
    mode: str                    # "replay" | "mic"
    script: Optional[str]
    started_at_ms: int
    ended_at_ms: Optional[int] = None
    customer_id: Optional[str] = None
    status: str = "active"       # active | ended
    owner: Optional[str] = None  # the user who created the call (X-Forwarded-Email; "local" in MODE=local)

    def to_wire(self) -> dict[str, Any]:
        return asdict(self)


ACTION_KINDS = ("send_email", "create_meeting", "update_contact", "enroll")
ACTION_STATUSES = ("proposed", "executing", "done", "outboxed", "failed", "rejected", "undone")
# Result keys that hold rendered message text. They are served by the message/invite endpoints, not in the socket
# events (SPEC 12).
_RENDERED_KEYS = ("eml", "ics", "cancel_ics", "cancel_eml")


@dataclass
class Action:
    """A proposed or executed action (SPEC 12). Exactly one of session_id (voice) or call_id (call console) is set.
    Never executes on its own: status moves only through ActionService (propose -> approve/reject -> done/outboxed/
    failed; done/outboxed -> undone)."""

    action_id: str
    owner: str
    kind: str
    params: dict[str, Any]
    status: str = "proposed"
    summary: str = ""
    session_id: Optional[str] = None
    call_id: Optional[str] = None
    playbook: Optional[str] = None
    proposed_by: str = "model"                 # model | user
    created_at_ms: int = field(default_factory=now_ms)
    updated_at_ms: int = field(default_factory=now_ms)
    decided_at_ms: Optional[int] = None
    decided_by: Optional[str] = None
    result: Optional[dict[str, Any]] = None
    error: Optional[str] = None

    def to_row(self) -> dict[str, Any]:
        """Full record for the store and the history sink."""
        return asdict(self)

    def to_wire(self) -> dict[str, Any]:
        """The client view: rendered text (eml/ics) is replaced by has_* flags; use the message endpoints to get it."""
        row = asdict(self)
        if self.result is None:
            row["result"] = None
            return row
        result = dict(self.result)
        flags = {f"has_{k}": True for k in _RENDERED_KEYS if result.pop(k, None)}
        row["result"] = {**result, **flags}
        return row

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "Action":
        fields = {k: row.get(k) for k in cls.__dataclass_fields__ if k in row}
        for key in ("params", "result"):
            if isinstance(fields.get(key), str):
                fields[key] = json.loads(fields[key])
        fields["params"] = fields.get("params") or {}
        return cls(**fields)
