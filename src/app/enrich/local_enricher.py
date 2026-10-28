"""LOCAL stand-in for the RTM enrichment job (SPEC section 2).

Subscribes to the `utterances` topic exactly like the RTM job would, applies the
pure transform in enrich/rules.py with the playbook's rules, and writes the
enrichment row to the store, which resolves any waiter in CallSession.
"""

from __future__ import annotations

import logging
from typing import Any, Callable

from bus.base import EventBus
from models import now_ms
from store.base import Store

from enrich.rules import Rules, enrich

log = logging.getLogger("copilot.enrich.local")


def enrichment_row(value: dict[str, Any], rules: Rules) -> dict[str, Any]:
    """Build the enrichments-table row for one utterance message."""
    fields = enrich(value.get("text", ""), value.get("speaker", ""), rules)
    return {
        "call_id": value["call_id"],
        "seq": int(value["seq"]),
        "speaker": value.get("speaker"),
        "text": value.get("text", ""),
        **fields,
        "enriched_at_ms": now_ms(),
        "source": "local",
    }


class LocalEnricher:
    def __init__(self, store: Store, rules_for: Callable[[str], Rules]) -> None:
        self._store = store
        self._rules_for = rules_for

    def attach(self, bus: EventBus, topic: str = "utterances") -> None:
        bus.subscribe(topic, self.handle)

    async def handle(self, key: str, value: dict[str, Any]) -> None:
        if not value.get("is_final", True):
            return
        row = enrichment_row(value, self._rules_for(value.get("playbook", "contact_center")))
        await self._store.put_enrichment(row["call_id"], row["seq"], row)
