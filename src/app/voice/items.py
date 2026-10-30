"""The `<<ITEMS>>` delimiter (SPEC 9.4): one LLM call streams the spoken reply, then the delimiter line,
then a JSON array of structured items for the turn.

ReplySplitter separates the two streams while tokens arrive. The delimiter can be split across tokens,
so a suffix of the buffer that could still become the delimiter is held back. parse_items is tolerant:
a malformed or truncated array gives no items and never raises, so it cannot break the audio.
"""

from __future__ import annotations

import json
from typing import Any, Optional

DELIM = "<<ITEMS>>"
ITEM_KINDS = ("action_item", "follow_up_question", "follow_up_response", "next_step", "summary", "risk", "note")
MAX_ITEMS = 10
MAX_TEXT_CHARS = 400


class ReplySplitter:
    """Feed model deltas; `feed` returns the spoken text that is safe to speak now. After the delimiter,
    text goes to `items_raw` instead."""

    def __init__(self) -> None:
        self._buf = ""
        self.in_items = False
        self.items_raw = ""

    def feed(self, delta: str) -> str:
        if self.in_items:
            self.items_raw += delta
            return ""
        self._buf += delta
        idx = self._buf.find(DELIM)
        if idx >= 0:
            spoken, self.items_raw = self._buf[:idx], self._buf[idx + len(DELIM):]
            self._buf = ""
            self.in_items = True
            return spoken
        keep = _held_suffix(self._buf)
        out = self._buf[: len(self._buf) - keep]
        self._buf = self._buf[len(self._buf) - keep:]
        return out

    def finish(self) -> str:
        """End of stream: release any held text (it was not the delimiter after all)."""
        if self.in_items:
            return ""
        out, self._buf = self._buf, ""
        return out


def _held_suffix(text: str) -> int:
    """Length of the longest suffix of `text` that is a proper prefix of DELIM."""
    for n in range(min(len(DELIM) - 1, len(text)), 0, -1):
        if DELIM.startswith(text[-n:]):
            return n
    return 0


def parse_items(raw: str, *, turn: int, session_id: str, now_ms: int) -> list[dict[str, Any]]:
    """Tolerant JSON parse. Takes the outermost [...] span; a dict with an `items` list is accepted too.
    Unknown kinds become `note`. Entries without text are dropped. Never raises."""
    if not raw or not raw.strip():
        return []
    start, end = raw.find("["), raw.rfind("]")
    if start == -1 or end <= start:
        obj = _parse_json(raw)
        data: Any = obj.get("items") if isinstance(obj, dict) else None
    else:
        data = _parse_json(raw[start : end + 1])
    if not isinstance(data, list):
        return []
    out: list[dict[str, Any]] = []
    for entry in data:
        if not isinstance(entry, dict):
            continue
        text = _clean(entry.get("text"))
        if not text:
            continue
        kind = entry.get("kind") if entry.get("kind") in ITEM_KINDS else "note"
        out.append({
            "item_id": _new_id(),
            "session_id": session_id,
            "turn": turn,
            "kind": kind,
            "text": text[:MAX_TEXT_CHARS],
            "owner": _clean(entry.get("owner")) or None,
            "due": _clean(entry.get("due")) or None,
            "source_doc": _clean(entry.get("source_doc")) or None,
            "created_at_ms": now_ms,
        })
        if len(out) >= MAX_ITEMS:
            break
    return out


def _parse_json(text: str) -> Optional[Any]:
    try:
        return json.loads(text)
    except (json.JSONDecodeError, ValueError):
        return None


def _clean(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


def _new_id() -> str:
    import uuid

    return str(uuid.uuid4())
