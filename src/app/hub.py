"""Per-call WebSocket fan-out. Every outgoing event carries emitted_at_ms (SPEC section 7)."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional

from models import now_ms

log = logging.getLogger("sidekick.hub")

_QUEUE_MAX = 1000


class Hub:
    def __init__(self) -> None:
        self._subs: dict[str, set[asyncio.Queue[Optional[dict[str, Any]]]]] = {}

    def subscribe(self, call_id: str) -> asyncio.Queue[Optional[dict[str, Any]]]:
        q: asyncio.Queue[Optional[dict[str, Any]]] = asyncio.Queue(maxsize=_QUEUE_MAX)
        self._subs.setdefault(call_id, set()).add(q)
        return q

    def unsubscribe(self, call_id: str, q: asyncio.Queue[Optional[dict[str, Any]]]) -> None:
        subs = self._subs.get(call_id)
        if subs:
            subs.discard(q)
            if not subs:
                self._subs.pop(call_id, None)

    def emit(self, call_id: str, event: dict[str, Any]) -> None:
        event.setdefault("emitted_at_ms", now_ms())
        for q in list(self._subs.get(call_id, ())):
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                log.warning("dropping event %s for slow subscriber on call %s", event.get("type"), call_id)

    def close(self, call_id: str) -> None:
        for q in list(self._subs.get(call_id, ())):
            try:
                q.put_nowait(None)
            except asyncio.QueueFull:
                pass
