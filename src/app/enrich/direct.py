"""In-process enrichment (ARCHITECTURE.md, SPEC section 4.1).

direct backbone (default): CallSession calls DirectEnricher.enrich for every final. It is the same pure
transform the RTM job runs (enrich/rules.py mirrors rtm_lib.py), with the playbook's rules. It costs about
1 ms in process, so there is no broker in the path. The enrichments row still goes to Lakebase for history.

kafka backbone, local stand-in: BusEnricher subscribes to the in-memory `utterances` topic the way the RTM job
would, and writes the enrichments row that CallSession waits for (rtm_wait_ms).
"""

from __future__ import annotations

import logging
from typing import Any, Callable

from bus.base import EventBus
from models import now_ms
from store.base import Store

from enrich.rules import Rules, enrich

log = logging.getLogger("sidekick.enrich.direct")


class DirectEnricher:
    def __init__(self, rules_for: Callable[[str], Rules]) -> None:
        self._rules_for = rules_for

    def enrich(self, *, call_id: str, seq: int, speaker: str, text: str, playbook: str,
               emitted_at_ms: int) -> dict[str, Any]:
        fields = enrich(text, speaker, self._rules_for(playbook))
        return {
            "call_id": call_id, "seq": seq, "speaker": speaker, "text": text, **fields,
            "emitted_at_ms": emitted_at_ms, "enriched_at_ms": now_ms(), "source": "direct",
        }


def enrichment_row(value: dict[str, Any], rules: Rules) -> dict[str, Any]:
    """Enrichments-table row for one utterance message (kafka local stand-in)."""
    fields = enrich(value.get("text", ""), value.get("speaker", ""), rules)
    return {
        "call_id": value["call_id"],
        "seq": int(value["seq"]),
        "speaker": value.get("speaker"),
        "text": value.get("text", ""),
        **fields,
        "emitted_at_ms": value.get("emitted_at_ms"),
        "enriched_at_ms": now_ms(),
        "source": "local",
    }


class BusEnricher:
    """Stand-in for the RTM job, local mode only. Subscribes to `utterances` and writes the enrichments row."""

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
