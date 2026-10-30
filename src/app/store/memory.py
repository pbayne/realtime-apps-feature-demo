"""In-memory store with asyncio change notification for enrichments."""

from __future__ import annotations

import asyncio
import time
from typing import Any, Optional

from models import CallRecord, Card, Utterance


class MemoryStore:
    def __init__(self) -> None:
        self.calls: dict[str, dict[str, Any]] = {}
        self.utterances: dict[str, dict[int, dict[str, Any]]] = {}
        self.enrichments: dict[tuple[str, int], dict[str, Any]] = {}
        self.cards: dict[str, dict[str, Any]] = {}
        self.feedback: list[dict[str, Any]] = []
        self.voice_turns: dict[str, dict[int, dict[str, Any]]] = {}
        self.voice_items: dict[str, list[dict[str, Any]]] = {}
        self._waiters: dict[tuple[str, int], asyncio.Event] = {}

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def create_call(self, call: CallRecord) -> None:
        self.calls[call.call_id] = call.to_wire()

    async def get_call(self, call_id: str) -> Optional[dict[str, Any]]:
        return self.calls.get(call_id)

    async def end_call(self, call_id: str, ended_at_ms: int) -> None:
        if call_id in self.calls:
            self.calls[call_id].update(ended_at_ms=ended_at_ms, status="ended")

    async def put_utterance(self, utt: Utterance) -> None:
        self.utterances.setdefault(utt.call_id, {})[utt.seq] = utt.to_wire()

    async def put_enrichment(self, call_id: str, seq: int, row: dict[str, Any]) -> None:
        self.enrichments[(call_id, seq)] = dict(row)
        ev = self._waiters.get((call_id, seq))
        if ev is not None:
            ev.set()

    async def get_enrichment(self, call_id: str, seq: int) -> Optional[dict[str, Any]]:
        return self.enrichments.get((call_id, seq))

    async def wait_enrichment(self, call_id: str, seq: int, timeout_s: float) -> Optional[dict[str, Any]]:
        row = self.enrichments.get((call_id, seq))
        if row is not None:
            return row
        ev = self._waiters.setdefault((call_id, seq), asyncio.Event())
        try:
            await asyncio.wait_for(ev.wait(), timeout=max(timeout_s, 0.0))
        except asyncio.TimeoutError:
            return None
        finally:
            if not ev.is_set():
                self._waiters.pop((call_id, seq), None)
        row = self.enrichments.get((call_id, seq))
        self._waiters.pop((call_id, seq), None)
        return row

    async def put_card(self, card: Card, trace: dict[str, Any]) -> None:
        self.cards[card.card_id] = {**card.to_wire(), "trace": trace}

    async def add_feedback(self, card_id: str, call_id: str, action: str, edited_text: Optional[str], at_ms: int) -> None:
        self.feedback.append({"card_id": card_id, "call_id": call_id, "action": action, "edited_text": edited_text, "at_ms": at_ms})
        card = self.cards.get(card_id)
        if card is not None and card["call_id"] == call_id:
            card["status"] = {"accept": "accepted", "reject": "rejected", "edit": "edited"}.get(action, card["status"])

    async def snapshot(self, call_id: str) -> dict[str, Any]:
        cards = [c for c in self.cards.values() if c["call_id"] == call_id]
        return {
            "call": self.calls.get(call_id),
            "utterances": sorted(self.utterances.get(call_id, {}).values(), key=lambda u: u["seq"]),
            "enrichments": sorted(
                [v for (cid, _), v in self.enrichments.items() if cid == call_id], key=lambda e: e["seq"]
            ),
            "cards": sorted(cards, key=lambda c: c["created_at_ms"]),
            "feedback": [f for f in self.feedback if f["call_id"] == call_id],
            "as_of_ms": int(time.time() * 1000),
        }

    async def put_voice_turn(self, session_id: str, row: dict[str, Any]) -> None:
        self.voice_turns.setdefault(session_id, {})[int(row["turn"])] = {**row, "session_id": session_id}

    async def put_voice_item(self, session_id: str, item: dict[str, Any]) -> None:
        items = self.voice_items.setdefault(session_id, [])
        items[:] = [i for i in items if i["item_id"] != item["item_id"]] + [{**item, "session_id": session_id}]

    async def voice_snapshot(self, session_id: str) -> dict[str, Any]:
        return {
            "turns": sorted(self.voice_turns.get(session_id, {}).values(), key=lambda t: t["turn"]),
            "items": list(self.voice_items.get(session_id, [])),
        }

