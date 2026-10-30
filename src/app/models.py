"""Shared small helpers and the JSON-shaped wire objects (SPEC section 3)."""

from __future__ import annotations

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
