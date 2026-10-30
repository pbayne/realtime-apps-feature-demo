"""CallSession: one live call. Orchestrates STT events, enrichment, sidekick turns and cards.

Flow per final utterance (SPEC sections 3, 4 and 7):
  final -> store + bus `utterances` (RTM job or local enricher consume it)
        -> customer finals: perception (one haiku call) and KB retrieval START IMMEDIATELY
           (retrieval may already be running from a speculative stable partial)
        -> wait for enrichment (rtm_wait_ms), fall back to the same pure transform
        -> coverage update (customer turns only) and compliance (any speaker, bypasses guardrails)
        -> customer finals enter a debounced serial turn worker:
             retrieval (bounded by retrieval_budget_s, else keyword-only) -> decide
             at once on enrichment + rules + retrieval + coverage. Perception is used only if it
             already finished; otherwise it is LATE: when it arrives, a late candidate may fire only
             if no card fired for that turn, and it still passes the guardrails. Both the decision
             and the late verdict are written to the decision trace.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Optional

from bus.base import EventBus
from config import Settings
from enrich.direct import DirectEnricher
from enrich.rules import enrich
from history import schema as hs
from history.base import HistorySink
from history.memory import NullHistorySink
from hub import Hub
from llm import LlmClient
from models import CallRecord, Card, Utterance, cap_text, new_id, now_ms
from sidekick.compose import compose_body, truncate_words
from sidekick.decide import (
    Candidate,
    GuardConfig,
    GuardState,
    TurnContext,
    compliance_candidates,
    decide,
    turn_candidates,
)
from sidekick.perception import Perception, perceive
from sidekick.playbooks import Playbook, PlaybookRegistry
from sidekick.retrieval import Hit, Retriever
from store.base import Store
from stt.base import TranscriptEvent
from stt.browser import BrowserPassthrough
from stt.parakeet import StreamingSession
from stt.replay import ReplayFrameSource, ReplayScript, ScriptDecoder
from stt.vad import Vad

log = logging.getLogger("sidekick.session")

MAX_TRACES = 200
COMPLIANCE_REPEAT_FINALS = 3
SPECULATIVE_CACHE = 16
SPECULATIVE_MIN_WORDS = 3
SPECULATIVE_MIN_JACCARD = 0.8


@dataclass
class Deps:
    settings: Settings
    bus: EventBus
    store: Store
    llm: LlmClient
    retriever: Retriever
    registry: PlaybookRegistry
    hub: Hub
    vad: Vad
    history: Optional[HistorySink] = None      # SPEC 3.7: Zerobus (direct) or none (kafka)
    enricher: Optional[DirectEnricher] = None  # direct backbone: in-process enrichment


@dataclass
class _Timing:
    stt_first_word_ms: Optional[float]
    stt_commit_ms: Optional[float]
    rtm_ms: float
    source: str
    t_final: float


@dataclass
class _PendingTurn:
    seq: int
    speaker: str
    text: str
    enrichment: dict[str, Any]
    retrieval: "asyncio.Task[list[Hit]]"
    perception: "asyncio.Task[Perception]"
    timing: _Timing
    speculative: bool


@dataclass
class CallSession:
    record: CallRecord
    deps: Deps
    playbook: Playbook = field(init=False)
    guard: GuardState = field(default_factory=GuardState)
    coverage: dict[str, bool] = field(init=False)
    coverage_open: dict[str, int] = field(default_factory=dict)
    decisions: list[dict[str, Any]] = field(default_factory=list)
    ended: bool = False
    ended_mono: Optional[float] = None                                    # set by finish(); drives the grace period
    attached: int = 0                                                     # sockets and voice sessions on this call
    detached_at: float = field(default_factory=time.monotonic)            # armed at creation (SPEC 10, reaper)
    socket: Any = None                                                    # the call socket that owns the events
    _socket_q: Any = None
    _socket_task: Optional[asyncio.Task[Any]] = None

    def __post_init__(self) -> None:
        self.playbook = self.deps.registry.get(self.record.playbook)
        self.coverage = {m.key: False for m in self.playbook.methodology}
        self._history: HistorySink = self.deps.history or NullHistorySink()
        self._enricher = self.deps.enricher or DirectEnricher(self.deps.registry.rules_for)
        self._seq = 0
        self._events: asyncio.Queue[Optional[TranscriptEvent]] = asyncio.Queue()
        self._turn_q: asyncio.Queue[_PendingTurn] = asyncio.Queue()
        self._tasks: set[asyncio.Task[Any]] = set()
        self._stt_tasks: set[asyncio.Task[Any]] = set()
        self._browser: dict[str, BrowserPassthrough] = {}
        self._customer_recent: list[str] = []
        self._cfg = GuardConfig(
            min_confidence=self.deps.settings.card_min_confidence,
            cooldown_turns=self.deps.settings.card_cooldown_turns,
            budget=self.deps.settings.card_budget,
        )
        self._workers: list[asyncio.Task[Any]] = []
        self._final_index = 0
        self._compliance_last: dict[str, int] = {}
        self._repeat_suppressed: list[dict[str, Any]] = []
        self._aux: Optional[asyncio.Task[Any]] = None
        self._finish_lock = asyncio.Lock()
        # Speculative retrieval keyed by normalised customer text (partials, then the final reuses it).
        self._spec_retrieval: dict[str, asyncio.Task[list[Hit]]] = {}
        self._spec_inflight: Optional[asyncio.Task[list[Hit]]] = None
        self._compliance_seqs: set[int] = set()
        self._t0 = time.monotonic()

    # ------------------------------------------------------------- lifecycle
    @property
    def call_id(self) -> str:
        return self.record.call_id

    @property
    def settings(self) -> Settings:
        return self.deps.settings

    @property
    def owner(self) -> Optional[str]:
        return self.record.owner

    def attach(self) -> None:
        """A socket (call or audio) or a voice session is listening to this call."""
        self.attached += 1

    def detach(self) -> None:
        self.attached = max(0, self.attached - 1)
        if self.attached == 0:
            self.detached_at = time.monotonic()

    def claim_socket(self, ws: Any, q: Any, out_task: "asyncio.Task[Any]") -> Any:
        """Same rule as VoiceSession.claim_socket: one call socket reads this call's events. A previous socket is
        evicted (its subscription is removed and its pump cancelled); returns it so the caller can close it (4409)."""
        prev_ws, prev_q, prev_task = self.socket, self._socket_q, self._socket_task
        if prev_q is not None and prev_q is not q:
            self.deps.hub.unsubscribe(self.call_id, prev_q)
        if prev_task is not None and prev_task is not out_task:
            prev_task.cancel()
        if prev_ws is None:
            self.attach()
        self.socket, self._socket_q, self._socket_task = ws, q, out_task
        return prev_ws if prev_ws is not ws else None

    def release_socket(self, ws: Any, q: Any) -> bool:
        """True only if `ws` still owned the call; an evicted socket never tears down its successor."""
        if self.socket is not ws:
            return False
        self.socket, self._socket_q, self._socket_task = None, None, None
        self.deps.hub.unsubscribe(self.call_id, q)
        self.detach()
        return True

    def start(self) -> None:
        self._workers = [
            asyncio.ensure_future(self._event_loop()),
            asyncio.ensure_future(self._turn_worker()),
        ]

    def next_seq(self) -> int:
        self._seq += 1
        return self._seq

    def _spawn(self, coro: Any, *, stt: bool = False) -> asyncio.Task[Any]:
        task = asyncio.ensure_future(coro)
        (self._stt_tasks if stt else self._tasks).add(task)
        task.add_done_callback(lambda t: (self._tasks.discard(t), self._stt_tasks.discard(t)))
        return task

    # ------------------------------------------------------------------ STT
    def start_replay(self, script: ReplayScript, speed: float) -> None:
        for speaker in ("agent", "customer"):
            decoder = ScriptDecoder(script.words_for(speaker), lag_ms=self.settings.stt_decode_lag_ms)
            session = self._stt_session(speaker, decoder)
            src = ReplayFrameSource(script, speaker, speed)
            self._spawn(self._drive_stt(session, src.frames()), stt=True)
        self._aux = asyncio.ensure_future(self._auto_end_after_replay())

    def start_mic(self, speaker: str, frames: Any, decoder: Any) -> asyncio.Task[Any]:
        session = self._stt_session(speaker, decoder)
        return self._spawn(self._drive_stt(session, frames), stt=True)

    def _stt_session(self, speaker: str, decoder: Any) -> StreamingSession:
        s = self.settings
        return StreamingSession(
            speaker, decoder, self.deps.vad, self.next_seq,
            frame_s=s.stt_frame_s, window_s=s.stt_window_s, hold_words=s.stt_hold_words,
            endpoint_silence_ms=s.stt_endpoint_silence_ms,
        )

    async def _drive_stt(self, session: StreamingSession, frames: Any) -> None:
        try:
            async for ev in session.run(frames):
                self._events.put_nowait(ev)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            log.exception("STT channel %s failed call_id=%s", session.speaker, self.call_id)
            self.deps.hub.emit(self.call_id, {"type": "error", "scope": "stt", "speaker": session.speaker,
                                              "message": str(exc)[:200]})

    def inject_final(self, speaker: str, text: str, *, commit_ms: float = 0.0) -> None:
        """Voice practice (SPEC 9): a final utterance from a non-audio source (the rep's typed or spoken turn,
        or the AI customer's reply). It takes the same path as a server STT final."""
        text = (text or "").strip()
        if self.ended or not text:
            return
        if speaker not in ("agent", "customer"):
            raise ValueError("speaker must be agent or customer")
        now_rel = int((time.monotonic() - self._t0) * 1000)
        self._events.put_nowait(TranscriptEvent(
            kind="final", speaker=speaker, seq=self.next_seq(), text=text,
            t_start_ms=now_rel, t_end_ms=now_rel, first_word_ms=0.0, commit_ms=commit_ms,
        ))

    def browser_result(self, speaker: str, msg: dict[str, Any]) -> None:
        """Browser Web Speech result (WS text message `browser_stt` or /ws/audio text frame)."""
        if speaker not in ("agent", "customer"):
            raise ValueError("speaker must be agent or customer")
        if isinstance(msg.get("text"), str):
            cap_text(msg["text"], self.settings.max_text_bytes, "browser result")
        bp = self._browser.get(speaker)
        if bp is None:
            bp = self._browser[speaker] = BrowserPassthrough(speaker, self.next_seq)
        ev = bp.handle({"text": msg.get("text"), "is_final": msg.get("is_final"),
                        "t_start_ms": msg.get("t_start_ms"), "t_end_ms": msg.get("t_end_ms")})
        if ev is not None:
            self._events.put_nowait(ev)

    async def _auto_end_after_replay(self) -> None:
        if self._stt_tasks:
            await asyncio.gather(*list(self._stt_tasks), return_exceptions=True)
        await self.finish()

    # ----------------------------------------------------------- event loop
    async def _event_loop(self) -> None:
        hub = self.deps.hub
        while True:
            ev = await self._events.get()
            if ev is None:
                self._events.task_done()
                break
            if ev.kind == "partial":
                hub.emit(self.call_id, {
                    "type": "transcript.partial", "speaker": ev.speaker, "text": ev.text, "seq": ev.seq,
                    "first_word_ms": ev.first_word_ms,
                })
                if ev.speaker == "customer":
                    self._maybe_speculate(ev.text)
            else:
                hub.emit(self.call_id, {
                    "type": "transcript.final", "speaker": ev.speaker, "text": ev.text, "seq": ev.seq,
                    "latency_ms": round(ev.commit_ms or 0.0),
                })
                self._spawn(self._on_final(ev))
            self._events.task_done()

    async def _drain(self) -> None:
        await self._events.join()
        while True:
            await self._turn_q.join()
            pending = list(self._tasks)
            if not pending:
                return
            await asyncio.gather(*pending, return_exceptions=True)

    async def finish(self) -> None:
        """End the call (POST /end, or replay completion). Stops STT, drains in-flight work,
        emits call.ended. Idempotent."""
        async with self._finish_lock:
            if self.ended:
                return
            for t in list(self._stt_tasks):
                t.cancel()
            for t in list(self._stt_tasks):
                try:
                    await t
                except (asyncio.CancelledError, Exception):  # noqa: BLE001
                    pass
            await self._drain()
            await self._stop_workers()
            self.ended = True
            ended_at = now_ms()
            self.record.ended_at_ms, self.record.status = ended_at, "ended"
            self.ended_mono = time.monotonic()
            self._history.emit("calls", hs.call_row(self.record))
            try:
                await self.deps.store.end_call(self.call_id, ended_at)
            except Exception:  # noqa: BLE001
                log.exception("end_call failed call_id=%s", self.call_id)
            self.deps.hub.emit(self.call_id, {"type": "call.ended"})
            self.deps.hub.close(self.call_id)

    async def _stop_workers(self) -> None:
        event_worker, turn_worker = self._workers
        self._events.put_nowait(None)
        try:
            await asyncio.wait_for(event_worker, timeout=5)
        except (asyncio.TimeoutError, asyncio.CancelledError, Exception):  # noqa: BLE001
            event_worker.cancel()
        turn_worker.cancel()
        await asyncio.gather(turn_worker, return_exceptions=True)

    # ------------------------------------------------------------ retrieval
    async def _retrieve(self, text: str) -> list[Hit]:
        try:
            return await self.deps.retriever.search(self.playbook, text)
        except Exception:  # noqa: BLE001
            log.exception("retrieval failed")
            return []

    def _maybe_speculate(self, text: str) -> None:
        """Start retrieval on a stable-enough customer partial. One speculative search in flight at a time."""
        if len(text.split()) < SPECULATIVE_MIN_WORDS:
            return
        key = _norm(text)
        if key in self._spec_retrieval:
            return
        if self._spec_inflight is not None and not self._spec_inflight.done():
            return
        task = self._spawn(self._retrieve(text))
        self._spec_inflight = task
        self._spec_retrieval[key] = task
        while len(self._spec_retrieval) > SPECULATIVE_CACHE:
            self._spec_retrieval.pop(next(iter(self._spec_retrieval)))

    def _retrieval_for_final(self, text: str) -> tuple["asyncio.Task[list[Hit]]", bool]:
        """Reuse a speculative search when its text is (nearly) the final: the final decode usually adds or
        fixes a word or two after the last partial. Near-identical queries rank the KB the same way."""
        key = _norm(text)
        task = self._spec_retrieval.get(key)
        if task is None:
            words = set(key.split())
            for spec_key in reversed(list(self._spec_retrieval)):
                if _jaccard(words, set(spec_key.split())) >= SPECULATIVE_MIN_JACCARD:
                    task = self._spec_retrieval[spec_key]
                    break
        if task is not None:
            return task, True
        task = self._spawn(self._retrieve(text))
        self._spec_retrieval[key] = task
        return task, False

    # ----------------------------------------------------------- per final
    async def _on_final(self, ev: TranscriptEvent) -> None:
        t_final = time.monotonic()
        call_id, deps, pb = self.call_id, self.deps, self.playbook
        emitted = now_ms()
        direct = self.settings.backbone == "direct"
        utt = Utterance(call_id=call_id, seq=ev.seq, speaker=ev.speaker, text=ev.text,
                        t_start_ms=ev.t_start_ms, t_end_ms=ev.t_end_ms, emitted_at_ms=emitted, playbook=pb.name)
        self._spawn(self._persist_utterance(utt))   # live state; a store outage never reaches the card path
        self._history.emit("utterances", hs.utterance_row(utt))
        if not direct:
            await deps.bus.publish(self.settings.kafka_utterances_topic, call_id, {
                "call_id": call_id, "seq": ev.seq, "speaker": ev.speaker, "text": ev.text, "is_final": True,
                "t_start_ms": ev.t_start_ms, "t_end_ms": ev.t_end_ms, "emitted_at_ms": emitted, "playbook": pb.name,
            })

        # Customer finals start perception and retrieval now, so neither waits for enrichment.
        perc_task: Optional[asyncio.Task[Perception]] = None
        ret_task: Optional[asyncio.Task[list[Hit]]] = None
        speculative = False
        if ev.speaker == "customer":
            recent = list(self._customer_recent[-8:])
            perc_task = self._spawn(perceive(deps.llm, pb, ev.text, recent, self.settings.perception_budget_s))
            ret_task, speculative = self._retrieval_for_final(ev.text)

        t0 = time.monotonic()
        if direct:
            # direct backbone: enrich in process (~1 ms). Nothing waits on a broker or on Lakebase.
            fields = self._enricher.enrich(call_id=call_id, seq=ev.seq, speaker=ev.speaker, text=ev.text,
                                           playbook=pb.name, emitted_at_ms=emitted)
            rtm_ms = (time.monotonic() - t0) * 1000.0
            source = "direct"
            merged = _merge(None, fields)
            self._spawn(self._persist_enrichment(fields))
        else:
            # kafka backbone: RTM (or local stand-in) first, same pure transform as fallback
            row = None
            try:
                row = await deps.store.wait_enrichment(call_id, ev.seq, self.settings.rtm_wait_ms / 1000.0)
            except Exception:  # noqa: BLE001
                log.exception("wait_enrichment failed")
            rtm_ms = (time.monotonic() - t0) * 1000.0
            source = "rtm" if row is not None else "local_fallback"
            local = enrich(ev.text, ev.speaker, self.deps.registry.rules_for(pb.name))
            merged = _merge(row, local)
        deps.hub.emit(call_id, {
            "type": "enrichment", "seq": ev.seq, "speaker": ev.speaker,
            "intent": merged["intent"], "sentiment": merged["sentiment"],
            "risk_flags": merged["risk_flags"], "objection": merged["objection"],
            "source": source,
        })

        if ev.speaker == "customer":
            changed = False
            for key in merged["methodology_hits"]:
                if key in self.coverage and not self.coverage[key]:
                    self.coverage[key] = True
                    changed = True
            if changed:
                self._emit_coverage()

        # compliance: any speaker, always allowed. Exception: the same trigger re-firing within
        # COMPLIANCE_REPEAT_FINALS finals (e.g. customer asks, agent restates) is not shown twice.
        self._final_index += 1
        comp = []
        for c in compliance_candidates(pb, merged["risk_flags"]):
            last = self._compliance_last.get(c.trigger)
            if last is not None and self._final_index - last <= COMPLIANCE_REPEAT_FINALS:
                self._repeat_suppressed.append({"trigger": c.trigger, "seq": ev.seq})
                continue
            self._compliance_last[c.trigger] = self._final_index
            comp.append(c)
        timing = _Timing(ev.first_word_ms, ev.commit_ms, rtm_ms, source, t_final)
        if comp:
            self._compliance_seqs.add(ev.seq)
            trace = {"turn": self.guard.turn, "customer_turn": False, "candidates": [], "cards": [c.kind for c in comp],
                     "silent": False, "reason": "compliance bypass", "speaker": ev.speaker, "seq": ev.seq,
                     "customer_text": ev.text, "enrichment_source": source, "rtm_ms": round(rtm_ms, 1),
                     "compliance_repeat_suppressed": [r for r in self._repeat_suppressed if r["seq"] == ev.seq]}
            self._remember(trace)
            for c in comp:
                await self._emit_card(c, trace, timing, ev.seq, path="compliance")

        if ev.speaker == "customer" and perc_task is not None and ret_task is not None:
            self._customer_recent.append(ev.text)
            await self._turn_q.put(_PendingTurn(
                seq=ev.seq, speaker=ev.speaker, text=ev.text, enrichment=merged,
                retrieval=ret_task, perception=perc_task, timing=timing, speculative=speculative,
            ))
        elif not comp:
            self._emit_metrics(ev.seq, timing, None, None)

    async def _persist_utterance(self, utt: Utterance) -> None:
        try:
            await self.deps.store.put_utterance(utt)
        except Exception:  # noqa: BLE001 - store outage must not stop the sidekick
            log.exception("put_utterance failed call_id=%s", utt.call_id)

    async def _persist_enrichment(self, fields: dict[str, Any]) -> None:
        """direct backbone: the in-process enrichment row goes to Lakebase (live state) and to history."""
        call_id, seq = self.call_id, fields["seq"]
        try:
            await self.deps.store.put_enrichment(call_id, seq, fields)
        except Exception:  # noqa: BLE001
            log.exception("put_enrichment failed call_id=%s", call_id)
        self._history.emit("enrichments", hs.enrichment_row(
            call_id, seq, fields["speaker"], fields["text"], fields, fields["emitted_at_ms"],
            fields["enriched_at_ms"], fields.get("source", "direct")))

    # ----------------------------------------------------------- turn worker
    async def _turn_worker(self) -> None:
        while True:
            first = await self._turn_q.get()
            batch = [first]
            try:
                await asyncio.sleep(self.settings.debounce_ms / 1000.0)
                while not self._turn_q.empty():
                    batch.append(self._turn_q.get_nowait())
                for stale in batch[:-1]:
                    stale.perception.cancel()  # only the last final of a burst is decided
                await self._run_turn(batch)
            except Exception:  # noqa: BLE001 - one bad turn must not kill the worker
                log.exception("turn failed")
            finally:
                for _ in batch:
                    self._turn_q.task_done()

    async def _run_turn(self, batch: list[_PendingTurn]) -> None:
        last = batch[-1]
        pb = self.playbook
        self.guard.turn += 1
        turn_no = self.guard.turn
        customer_text = " ".join(b.text for b in batch)
        for key, done in self.coverage.items():
            if not done:
                self.coverage_open[key] = self.coverage_open.get(key, 0) + 1

        hits, backend, wait_ms, fallback = await self._hits_for(last)
        perc = _perception_if_done(last)
        ctx = self._context(last.enrichment, customer_text, hits, backend, perc)
        decision = decide(turn_candidates(ctx), self.guard, pb.card_weights, self._cfg, customer_turn=True)
        trace = decision.trace
        trace.update({
            "seq": last.seq,
            "speaker": last.speaker,
            "coalesced_seqs": [b.seq for b in batch],
            "customer_text": customer_text,
            "perception": perc.to_trace() if perc is not None else {"state": "pending"},
            "perception_path": "in_time" if perc is not None else "late_eligible",
            "retrieval": {
                "backend": backend, "fallback": fallback, "speculative": last.speculative,
                "wait_ms": round(wait_ms, 1),
                "hits": [{"doc_id": h.doc_id, "section": h.section, "score": h.score} for h in hits],
            },
            "enrichment_source": last.timing.source,
            "rtm_ms": round(last.timing.rtm_ms, 1),
            "decided_after_final_ms": round((time.monotonic() - last.timing.t_final) * 1000.0, 1),
        })
        self._remember(trace)
        if perc is not None:
            self._emit_perception(last.seq, perc)
        for card in decision.cards:
            await self._emit_card(card, trace, last.timing, last.seq, path="in_time")
        if not decision.cards:
            self._emit_metrics(last.seq, last.timing, None, None)
        if perc is None:
            self._spawn(self._late_perception(last, turn_no, trace, hits, backend))

    async def _hits_for(self, last: _PendingTurn) -> tuple[list[Hit], str, float, bool]:
        """Retrieval for this turn, waiting at most retrieval_budget_s. On a miss, keyword-only."""
        t0 = time.monotonic()
        task = last.retrieval
        await asyncio.wait({task}, timeout=self.settings.retrieval_budget_s)
        wait_ms = (time.monotonic() - t0) * 1000.0
        if task.done() and not task.cancelled() and task.exception() is None:
            hits = task.result()
            return hits, _backend_of(hits), wait_ms, False
        try:
            hits = await self.deps.retriever.search(self.playbook, last.text, keyword_only=True)
        except Exception:  # noqa: BLE001
            log.exception("keyword retrieval failed")
            hits = []
        return hits, _backend_of(hits), wait_ms, True

    def _context(self, enrichment: dict[str, Any], customer_text: str, hits: list[Hit], backend: str,
                 perc: Optional[Perception]) -> TurnContext:
        return TurnContext(
            playbook=self.playbook,
            customer_text=customer_text,
            intent=enrichment.get("intent"),
            objection=enrichment.get("objection"),
            risk_flags=list(enrichment.get("risk_flags", [])),
            perception=perc,
            hits=hits,
            coverage=dict(self.coverage),
            coverage_turns_open=dict(self.coverage_open),
            min_retrieval_score=self.deps.retriever.min_score(backend),
        )

    async def _late_perception(self, last: _PendingTurn, turn_no: int, trace: dict[str, Any],
                               hits: list[Hit], backend: str) -> None:
        """Perception finished after the turn was decided. Record it; a late candidate may fire only when
        no card fired for this turn (regular or compliance) and it passes the guardrails (SPEC section 4)."""
        perc = await last.perception  # perceive() is bounded by perception_budget_s and never raises
        self._emit_perception(last.seq, perc)
        arrived_ms = round((time.monotonic() - last.timing.t_final) * 1000.0, 1)
        customer_text = trace.get("customer_text", "")
        ctx = self._context(last.enrichment, customer_text, hits, backend, perc)
        late: dict[str, Any] = {
            "kind": "late_perception", "turn": turn_no, "seq": last.seq, "customer_text": customer_text,
            "perception": perc.to_trace(), "arrived_after_final_ms": arrived_ms,
        }
        winners: list[Candidate] = []
        fired = bool(trace.get("cards")) or last.seq in self._compliance_seqs
        if fired:
            late.update(
                verdict="skipped", reason="a card already fired for this turn", winner=None, cards=[], silent=True,
                candidates=[{"kind": c.kind, "trigger": c.trigger, "confidence": round(c.confidence, 4)}
                            for c in turn_candidates(ctx)],
            )
        else:
            decision = decide(turn_candidates(ctx), self.guard, self.playbook.card_weights, self._cfg,
                              customer_turn=True)
            winners = decision.cards
            late.update(decision.trace)
            late.update(turn=turn_no, kind="late_perception", seq=last.seq,
                        verdict="allowed" if winners else "blocked")
        trace["late_perception"] = {"verdict": late["verdict"], "reason": late.get("reason"),
                                    "cards": late.get("cards", []), "arrived_after_final_ms": arrived_ms}
        self._remember(late)
        for card in winners:
            await self._emit_card(card, late, last.timing, last.seq, path="late")

    def _remember(self, trace: dict[str, Any]) -> None:
        self.decisions.append(trace)
        if len(self.decisions) > MAX_TRACES:
            del self.decisions[: len(self.decisions) - MAX_TRACES]

    # ------------------------------------------------------------ cards
    async def _emit_card(self, cand: Candidate, trace: dict[str, Any], timing: _Timing, seq: int,
                         *, path: str = "in_time") -> None:
        hub = self.deps.hub
        card = Card(
            card_id=new_id(), call_id=self.call_id, seq=seq, kind=cand.kind,
            title=truncate_words(cand.title, self.settings.title_max_words), body="",
            confidence=round(cand.confidence, 4), source_doc=cand.source_doc,
            source_clause=cand.source_clause, trigger=cand.trigger, status="shown",
        )
        hub.emit(self.call_id, {"type": "card.start", "card": card.without_body()})
        start_wall = time.monotonic()
        card_ms = (start_wall - timing.t_final) * 1000.0

        async def emit(text: str) -> None:
            hub.emit(self.call_id, {"type": "card.delta", "card_id": card.card_id, "delta": text})

        result = await compose_body(
            self.deps.llm, self.settings, self.playbook, cand,
            customer_text=trace.get("customer_text", ""), recent=self._customer_recent,
            reference=cand.grounding, emit=emit,
        )
        card.body = result.body
        card_done_ms = (time.monotonic() - timing.t_final) * 1000.0
        trace = {**trace, "compose": {"mode": result.mode, "error": result.error}, "card_path": path}
        hub.emit(self.call_id, {"type": "card.done", "card": card.to_wire()})
        try:
            await self.deps.store.put_card(card, trace)
            self._history.emit("cards", hs.card_row(card, trace))
            await self.deps.bus.publish(self.settings.kafka_cards_topic, self.call_id, card.to_wire())
        except Exception:  # noqa: BLE001 - persistence is best-effort during the call
            log.exception("persisting card failed call_id=%s", self.call_id)
        self._emit_metrics(seq, timing, card_ms, card_done_ms, compose_mode=result.mode, card_path=path)

    def _emit_perception(self, seq: int, perc: Perception) -> None:
        """Perception-derived fields update the chips. Absent fields stay null so the UI keeps rules values."""
        if not perc.has_fields:
            return
        self.deps.hub.emit(self.call_id, {
            "type": "enrichment", "seq": seq, "speaker": "customer", "source": "perception",
            "intent": perc.intent, "sentiment": perc.sentiment, "objection": perc.objection,
        })

    # ---------------------------------------------------------- feedback
    async def feedback(self, card_id: str, action: str, text: Optional[str]) -> None:
        if action not in ("accept", "reject", "edit"):
            raise ValueError("action must be accept|reject|edit")
        if text is not None:
            cap_text(str(text), self.settings.max_text_bytes, "feedback text")
        at_ms = now_ms()
        try:
            await self.deps.store.add_feedback(card_id, self.call_id, action, text, at_ms)
        except Exception:  # noqa: BLE001 - reported to the client; the socket stays up
            log.exception("feedback save failed call_id=%s", self.call_id)
            raise ValueError("feedback could not be saved; try again") from None
        self._history.emit("feedback", hs.feedback_row(card_id, self.call_id, action, text, at_ms))

    # ----------------------------------------------------------- metrics
    def _emit_metrics(self, seq: int, timing: _Timing, card_ms: Optional[float], card_done_ms: Optional[float],
                      compose_mode: Optional[str] = None, card_path: Optional[str] = None) -> None:
        """SPEC section 7 metrics. card_ms = customer final -> card.start, card_done_ms = customer final ->
        card.done. compose_mode and card_path are extra fields."""
        self.deps.hub.emit(self.call_id, {
            "type": "metrics", "seq": seq, "compose_mode": compose_mode, "card_path": card_path,
            "stt_first_word_ms": round(timing.stt_first_word_ms, 1) if timing.stt_first_word_ms is not None else None,
            "stt_commit_ms": round(timing.stt_commit_ms, 1) if timing.stt_commit_ms is not None else None,
            "rtm_ms": round(timing.rtm_ms, 1),
            "card_ms": round(card_ms, 1) if card_ms is not None else None,
            "card_done_ms": round(card_done_ms, 1) if card_done_ms is not None else None,
            "enrichment_source": timing.source,
        })

    def _emit_coverage(self) -> None:
        self.deps.hub.emit(self.call_id, self.coverage_event())

    def coverage_event(self) -> dict[str, Any]:
        return {
            "type": "coverage",
            "items": [{"key": m.key, "label": m.label, "covered": self.coverage.get(m.key, False)}
                      for m in self.playbook.methodology],
        }

    def live_state(self) -> dict[str, Any]:
        return {
            "coverage": self.coverage_event()["items"],
            "decisions": self.decisions[-50:],
            "seq": self._seq,
            "ended": self.ended,
        }


# ------------------------------------------------------------------ helpers

def _norm(text: str) -> str:
    return " ".join((text or "").lower().split())


def _jaccard(a: set[str], b: set[str]) -> float:
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def _backend_of(hits: list[Hit]) -> str:
    return hits[0].backend if hits else "none"


def _perception_if_done(last: _PendingTurn) -> Optional[Perception]:
    task = last.perception
    if task.done() and not task.cancelled() and task.exception() is None:
        return task.result()
    return None


def _merge(rtm_row: Optional[dict[str, Any]], local: dict[str, Any]) -> dict[str, Any]:
    """Union of RTM (shared rules) and app-side (shared + playbook) enrichment."""
    if rtm_row is None:
        return dict(local)

    def union(a: list[str], b: list[str]) -> list[str]:
        out: list[str] = []
        for x in list(a or []) + list(b or []):
            if x not in out:
                out.append(x)
        return out

    return {
        "intent": rtm_row.get("intent") or local["intent"],
        "sentiment": rtm_row.get("sentiment", local["sentiment"]),
        "risk_flags": union(rtm_row.get("risk_flags", []), local["risk_flags"]),
        "objection": rtm_row.get("objection") or local["objection"],
        "entities": rtm_row.get("entities") or local["entities"],
        "methodology_hits": union(rtm_row.get("methodology_hits", []), local["methodology_hits"]),
    }
