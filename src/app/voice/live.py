"""Live analysis while the user is still talking (SPEC 9.7).

Two passes, both driven by partial and final transcripts of the user (assistant mode) or the rep (practice mode):

1. Rules, always, no network. enrich/rules.py gives sentiment, intent, objection, risk and methodology hits.
   Partials are debounced: at most one analysis per `debounce_s`, and only when the partial has added
   `min_new_words` words since the last analysis. Every final is analysed at once. Each analysis emits a `live`
   event and appends a point to the session's sentiment series. In assistant mode the methodology hits are the
   coverage checklist (over the user's speech). In practice mode coverage stays with the call pipeline.

2. Suggestions, one fast-model call, fail-soft. Returns 0 to 2 provisional items (`suggestion` events, kinds as
   in SPEC 9.3). At most one call per `suggest_interval_s`, and never two in flight. A final cancels a pass still
   running and, when the rate limit is closed, schedules a single pass for the first open slot.

Provisional items are chips. The rep can pin one (it becomes an outcome item, persisted like a <<ITEMS>> entry) or
dismiss it. A new pass supersedes the unpinned chips of the same turn. The end of the turn retires what is left,
because the <<ITEMS>> list from the reply is the authoritative one.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from collections import deque
from typing import Any, Callable, Optional

from enrich.rules import Rules, enrich
from llm import LlmClient, parse_json_object
from models import now_ms
from sidekick.perception import strip_fences

SUGGEST_KINDS = ("next_step", "action_item", "follow_up_question", "follow_up_response", "risk", "note")
MAX_SUGGESTIONS = 2
SUGGEST_TEXT_MAX = 160
SERIES_MAX = 300
SYSTEM_MARKER = "LIVE_SUGGESTIONS"

ROLE_BRIEF = {
    "assistant": ("The speaker is a financial advisor thinking out loud to Sidekick, their assistant, before or during "
                  "a client call. Suggest what they should do or ask next."),
    "practice": ("The speaker is a rep practising a client conversation with an AI customer. Suggest what the rep "
                 "should say, check or do next."),
}


def new_words(text: str, last: str) -> int:
    return max(0, len(text.split()) - len(last.split()))


def suggestion_messages(*, role: str, playbook_title: str, scenario: Optional[str], context: str,
                        speaking: str, signals: list[str]) -> list[dict[str, str]]:
    system = (
        f"{SYSTEM_MARKER}. {ROLE_BRIEF.get(role, ROLE_BRIEF['assistant'])} Playbook: {playbook_title}."
        + (f" Scenario: {scenario}." if scenario else "")
        + " The speech is still in progress, so suggest only what it already supports. Reply with strict JSON "
        "only, no markdown: {\"suggestions\": [{\"kind\": \"next_step|action_item|follow_up_question|"
        "follow_up_response|risk|note\", \"text\": \"one short sentence\"}]}. Return 0 to 2 suggestions, or an "
        "empty list when nothing is useful."
    )
    user = (
        f"Speaking now: {speaking or '(nothing yet)'}\n"
        f"Signals: {', '.join(signals) or 'none'}\n"
        f"Recent context:\n{context or '(none yet)'}"
    )
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


def parse_suggestions(raw: str, *, session_id: str, turn: int, now: int) -> list[dict[str, Any]]:
    """Tolerant parse; never raises. Unknown kinds and empty or duplicate texts are dropped."""
    obj = parse_json_object(strip_fences(raw or ""))
    data = obj.get("suggestions") if isinstance(obj, dict) else None
    if not isinstance(data, list):
        return []
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for entry in data:
        if not isinstance(entry, dict):
            continue
        kind, text = entry.get("kind"), entry.get("text")
        if kind not in SUGGEST_KINDS or not isinstance(text, str):
            continue
        clean = " ".join(text.split())[:SUGGEST_TEXT_MAX]
        if not clean or clean.lower() in seen:
            continue
        seen.add(clean.lower())
        out.append({
            "item_id": str(uuid.uuid4()), "session_id": session_id, "turn": turn, "kind": kind, "text": clean,
            "owner": None, "due": None, "source_doc": None, "created_at_ms": now, "provisional": True,
        })
        if len(out) >= MAX_SUGGESTIONS:
            break
    return out


class LiveAnalyzer:
    def __init__(self, *, session_id: str, role: str, playbook_title: str, methodology: list[tuple[str, str]],
                 rules: Rules, llm: Optional[LlmClient], fast_model: str, emit: Callable[[dict[str, Any]], None],
                 context: Callable[[], str] = lambda: "", scenario: Optional[str] = None,
                 debounce_s: float = 0.6, min_new_words: int = 4, suggest_interval_s: float = 2.5,
                 suggest_timeout_s: float = 1.5, max_tokens: int = 220,
                 clock: Callable[[], float] = time.monotonic) -> None:
        self.session_id = session_id
        self.role = role
        self._title = playbook_title
        self._methodology = methodology            # [(key, label)], the coverage checklist (assistant mode)
        self._rules = rules
        self._llm = llm
        self._model = fast_model
        self._emit = emit
        self._context = context
        self._scenario = scenario
        self.debounce_s = debounce_s
        self.min_new_words = min_new_words
        self.suggest_interval_s = suggest_interval_s
        self.suggest_timeout_s = suggest_timeout_s
        self.max_tokens = max_tokens
        self._clock = clock
        self._t0 = clock()
        self.coverage: dict[str, bool] = {k: False for k, _ in methodology}
        self.series: deque[dict[str, Any]] = deque(maxlen=SERIES_MAX)
        self.provisional: dict[str, dict[str, Any]] = {}
        self.suggest_failures = 0
        self.suggest_calls = 0
        self._pending: Optional[tuple[int, str, str]] = None
        self._last_text = ""
        self._last_turn: Optional[int] = None
        self._timer: Optional[asyncio.Task[None]] = None
        self._inflight: Optional[asyncio.Task[None]] = None
        self._deferred: Optional[tuple[int, str, str]] = None
        self._deferred_timer: Optional[asyncio.Task[None]] = None
        self._next_at = 0.0
        self._closed = False

    # ------------------------------------------------------------- inputs
    def on_partial(self, turn: int, text: str, speaker: str) -> None:
        """A stable partial from streaming STT. Debounced; analysed only with enough new words."""
        if self._closed or not text.strip():
            return
        self._pending = (turn, text, speaker)
        if self._timer is None or self._timer.done():
            self._timer = asyncio.ensure_future(self._fire_after(self.debounce_s))

    def on_final(self, turn: int, text: str, speaker: str, *, suggest: bool = True) -> None:
        """A final utterance (streamed, WAV, typed, or the customer's reply). Analysed at once. `suggest=False`
        runs the rules only (the practice customer's reply: the rep's next turn is what the chips are for)."""
        if self._closed or not text.strip():
            return
        self._cancel_timer()
        self._pending = None
        self._analyze(turn, text, speaker, final=True, suggest=suggest)

    def set_role(self, role: str, scenario: Optional[str]) -> None:
        self.role = role
        self._scenario = scenario

    async def _fire_after(self, delay: float) -> None:
        await asyncio.sleep(delay)
        if self._closed or self._pending is None:
            return
        turn, text, speaker = self._pending
        since = self._last_text if self._last_turn == turn else ""
        if new_words(text, since) < self.min_new_words:
            return                       # keep the pending text; the next partial re-arms the timer
        self._pending = None
        self._analyze(turn, text, speaker, final=False, suggest=True)

    def _cancel_timer(self) -> None:
        if self._timer is not None and not self._timer.done():
            self._timer.cancel()
        self._timer = None

    # ------------------------------------------------------------ analysis
    def _analyze(self, turn: int, text: str, speaker: str, *, final: bool, suggest: bool) -> None:
        if self._last_turn != turn:
            self._last_text = ""
            self._last_turn = turn
        self._last_text = text
        # Objections and methodology read the words of a customer-like speaker; the rep's own lines count for coverage.
        enrich_as = "agent" if speaker == "agent" else "customer"
        fields = enrich(text, enrich_as, self._rules)
        signals: list[str] = []
        if fields["intent"] == "question":
            signals.append("question")
        if fields["objection"]:
            signals.append(f"objection:{fields['objection']}")
        signals += [f"risk:{k}" for k in fields["risk_flags"]]
        signals += [f"method:{k}" for k in fields["methodology_hits"]]

        coverage = None
        if self.role == "assistant":
            for key in fields["methodology_hits"]:
                if key in self.coverage:
                    self.coverage[key] = True
            coverage = [{"key": k, "label": label, "covered": self.coverage.get(k, False)}
                        for k, label in self._methodology]

        sentiment = round(float(fields["sentiment"]), 3)
        self.series.append({"t_ms": round((self._clock() - self._t0) * 1000), "turn": turn, "speaker": speaker,
                            "kind": "final" if final else "partial", "value": sentiment})
        self._emit({"type": "live", "turn": turn, "speaker": speaker, "kind": "final" if final else "partial",
                     "sentiment": sentiment, "intent": fields["intent"], "signals": signals, "coverage": coverage,
                     "emitted_at_ms": now_ms()})
        if suggest and self._llm is not None and self._llm.enabled:
            self._maybe_suggest(turn, text, speaker, signals, final=final)

    # --------------------------------------------------------- suggestions
    def _maybe_suggest(self, turn: int, text: str, speaker: str, signals: list[str], *, final: bool) -> None:
        if self._inflight is not None and not self._inflight.done():
            if not final:
                return
            self._inflight.cancel()
            self._inflight = None
        if self._clock() < self._next_at:
            if final:
                self._defer(turn, text, speaker)
            return
        self._start_pass(turn, text, speaker, signals)

    def _start_pass(self, turn: int, text: str, speaker: str, signals: list[str]) -> None:
        self._next_at = self._clock() + self.suggest_interval_s
        self._inflight = asyncio.ensure_future(self._suggest(turn, text, speaker, signals))

    def _defer(self, turn: int, text: str, speaker: str) -> None:
        self._deferred = (turn, text, speaker)
        if self._deferred_timer is None or self._deferred_timer.done():
            delay = max(0.0, self._next_at - self._clock())
            self._deferred_timer = asyncio.ensure_future(self._run_deferred(delay))

    async def _run_deferred(self, delay: float) -> None:
        await asyncio.sleep(delay)
        if self._closed or self._deferred is None:
            return
        turn, text, speaker = self._deferred
        self._deferred = None
        if self._inflight is not None and not self._inflight.done():
            self._inflight.cancel()
        self._start_pass(turn, text, speaker, [])

    async def _suggest(self, turn: int, text: str, speaker: str, signals: list[str]) -> None:
        try:
            self.suggest_calls += 1
            messages = suggestion_messages(
                role=self.role, playbook_title=self._title, scenario=self._scenario, context=self._context(),
                speaking=text, signals=signals,
            )
            raw = await asyncio.wait_for(
                self._llm.chat(self._model, messages, max_tokens=self.max_tokens, timeout=self.suggest_timeout_s),
                self.suggest_timeout_s,
            )
            items = parse_suggestions(raw, session_id=self.session_id, turn=turn, now=now_ms())
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - fail-soft: a missing suggestion never breaks the call
            self.suggest_failures += 1
            return
        finally:
            if self._inflight is asyncio.current_task():
                self._inflight = None
        if self._closed or not items:
            return
        self._supersede(turn)
        for item in items:
            self.provisional[item["item_id"]] = item
            self._emit({"type": "suggestion", "turn": turn, "item": item})

    def _supersede(self, turn: int) -> None:
        self._retire(turn, "superseded")

    def _retire(self, turn: int, reason: str) -> None:
        ids = [iid for iid, it in self.provisional.items() if it["turn"] == turn]
        for iid in ids:
            del self.provisional[iid]
        if ids:
            self._emit({"type": "suggestion_retire", "turn": turn, "item_ids": ids, "reason": reason})

    # ------------------------------------------------------- pin / dismiss
    def take(self, item_id: str) -> dict[str, Any]:
        """Remove and return a provisional item (for pinning). Raises ValueError when it is not live."""
        item = self.provisional.pop(item_id, None)
        if item is None:
            raise ValueError("unknown or superseded suggestion")
        return item

    def drop(self, item_id: str) -> bool:
        return self.provisional.pop(item_id, None) is not None

    def retire_turn(self, turn: int) -> None:
        """The turn ended: its unpinned chips give way to the authoritative <<ITEMS>> list."""
        self._retire(turn, "turn_done")

    def close(self) -> None:
        self._closed = True
        self._pending = None
        self._deferred = None
        for task in (self._timer, self._inflight, self._deferred_timer):
            if task is not None and not task.done():
                task.cancel()
        self._timer = self._inflight = self._deferred_timer = None
