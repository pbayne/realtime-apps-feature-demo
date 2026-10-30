"""VoiceSession: one voice conversation (SPEC 9.1 to 9.4).

A turn is one user utterance (a WAV from the client, or typed text). Per turn:
  1. STT (WAV only): `user_text` event with stt_ms.
  2. Practice: the rep's text goes into the call session as an `agent` final, so enrichment, decide,
     cards and coverage run exactly as for a live call. Assistant: context = linked call + KB hits.
  3. One LLM stream. Text before `<<ITEMS>>` is the spoken reply: each `agent_text` delta goes out at once,
     and each complete sentence starts its own Kokoro stream. A single emitter sends the audio in sentence
     order (`audio` events, seq 0..n). Text after the delimiter is parsed into items after the audio.
  4. Practice: the reply text is injected into the call session as a `customer` final.
  5. `turn_done` with timing, and the turn is persisted (voice_turns, voice_items).

Interrupt (`interrupt` message, or a new turn while one runs) cancels the LLM stream and every in-flight
TTS stream, closes them, and ends the turn with interrupted=true. The partial reply is kept.
"""

from __future__ import annotations

import asyncio
import base64
import logging
import re
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Optional

from config import Settings
from history import schema as hs
from llm import LlmClient, LlmUnavailable
from models import now_ms
from sidekick.playbooks import Playbook, PracticeScenario
from sidekick.retrieval import Hit, Retriever
from store.base import Store

from voice.items import ReplySplitter, parse_items
from voice.prompts import assistant_messages, call_text, practice_messages
from voice.sentences import SentenceChunker
from voice.tts import TtsUnavailable

log = logging.getLogger("sidekick.voice")

VOICE_RE = re.compile(r"^[a-z]{2}_[a-z]+$")
MAX_SESSION_TURNS = 500
KB_BUDGET_S = 1.5


class Transcriber:
    """Protocol: WAV bytes in, text out (stt/parakeet.py ParakeetTranscriber)."""

    async def transcribe(self, wav: bytes) -> str: ...


class Speaker:
    """Protocol: sentence in, PCM chunks out (voice/tts.py KokoroTts). Must close its stream on aclose()."""

    def stream(self, text: str, voice: str) -> Any: ...


@dataclass
class VoiceDeps:
    settings: Settings
    llm: LlmClient
    tts: Speaker
    stt: Transcriber
    store: Store
    retriever: Retriever
    calls: dict[str, Any]                      # call_id -> CallSession (services.sessions)
    finish_call: Callable[[str], Awaitable[None]]
    new_practice_call: Callable[[str], Awaitable[str]]
    history: Any = None                        # HistorySink (SPEC 3.7); None in tests means no history


@dataclass
class _Turn:
    turn: int
    opening: bool = False
    timing: dict[str, Any] = field(default_factory=dict)
    marks: dict[str, float] = field(default_factory=dict)
    agent_parts: list[str] = field(default_factory=list)
    user_text: str = ""
    customer_injected: bool = False
    items: list[dict[str, Any]] = field(default_factory=list)
    cancellable: bool = False       # True only while this turn's LLM/TTS/STT work is in flight


def _ms(since: float) -> float:
    return round((time.monotonic() - since) * 1000.0, 1)


class VoiceSession:
    def __init__(self, *, session_id: str, role: str, playbook: Playbook, voice: str, deps: VoiceDeps,
                 call_id: Optional[str] = None, scenario: Optional[PracticeScenario] = None) -> None:
        self.session_id = session_id
        self.role = role
        self.playbook = playbook
        self.voice = voice
        self.deps = deps
        self.call_id = call_id
        self.scenario = scenario
        self.out: asyncio.Queue[Optional[dict[str, Any]]] = asyncio.Queue()
        self.turn = 0
        self.history: list[dict[str, str]] = []
        self.turns: list[dict[str, Any]] = []
        self.items: list[dict[str, Any]] = []
        self.closed = False
        self.opened = False
        self._pending_practice = False
        self._current: Optional[_Turn] = None   # the turn whose work may be interrupted
        self._task: Optional[asyncio.Task[None]] = None
        self._hub_q: Optional[asyncio.Queue[Optional[dict[str, Any]]]] = None
        self._hub: Any = None
        self._hub_task: Optional[asyncio.Task[None]] = None

    # ------------------------------------------------------------ plumbing
    @property
    def settings(self) -> Settings:
        return self.deps.settings

    @property
    def busy(self) -> bool:
        return self._task is not None and not self._task.done()

    def _emit(self, event: dict[str, Any]) -> None:
        event.setdefault("emitted_at_ms", now_ms())
        self.out.put_nowait(event)

    def _error(self, turn: int, stage: str, exc: BaseException | str) -> None:
        self._emit({"type": "error", "turn": turn, "stage": stage, "message": str(exc)[:300]})

    def attach_call_events(self, hub: Any) -> None:
        """Practice (or a linked assistant call): forward the call's events on this session's socket."""
        if not self.call_id:
            return
        call = self.deps.calls.get(self.call_id)
        if call is not None:
            self._emit(call.coverage_event())
        self._hub = hub
        self._hub_q = hub.subscribe(self.call_id)
        self._hub_task = asyncio.ensure_future(self._forward(self._hub_q))

    async def _forward(self, q: asyncio.Queue[Optional[dict[str, Any]]]) -> None:
        while True:
            ev = await q.get()
            if ev is None:
                return
            self.out.put_nowait(ev)

    def detach_call_events(self) -> None:
        if self._hub is not None and self._hub_q is not None:
            self._hub.unsubscribe(self.call_id, self._hub_q)
        if self._hub_task is not None:
            self._hub_task.cancel()
        self._hub_q = None
        self._hub_task = None

    # ------------------------------------------------------------- inputs
    def submit_audio(self, wav: bytes) -> None:
        if not wav:
            raise ValueError("empty audio turn")
        if wav[:4] != b"RIFF" or wav[8:12] != b"WAVE":
            raise ValueError("each binary frame must be one complete WAV turn (16 kHz mono PCM16)")
        if len(wav) > self.settings.voice_max_audio_bytes:
            raise ValueError("audio turn is too long (max 30 s of 16 kHz mono PCM16)")
        self._start_turn(wav=wav)

    def submit_text(self, text: str) -> None:
        text = (text or "").strip()
        if not text:
            raise ValueError("text turn is empty")
        self._start_turn(text=text)

    def interrupt(self) -> None:
        """Barge-in: cancel the running turn while its work is in flight. Its cleanup emits
        turn_done(interrupted=true). A turn that has finished (or is finishing) is never interrupted, so an
        interrupt that arrives after turn_done is ignored."""
        if self.busy and self._current is not None and self._current.cancellable:
            self._task.cancel()

    def start_opening(self) -> None:
        """Practice only: the customer speaks the scenario's opening line once, as turn 0."""
        if self.role != "practice" or self.opened or self.scenario is None:
            return
        self.opened = True
        self._task = asyncio.ensure_future(self._run(0, self._task, opening=self.scenario.opening))

    def configure(self, voice: Optional[str] = None, role: Optional[str] = None,
                  scenario_key: Optional[str] = None) -> None:
        if voice is not None:
            if not VOICE_RE.match(voice):
                raise ValueError("voice must look like af_heart")
            self.voice = voice
        if role is not None and role != self.role:
            if role not in ("assistant", "practice"):
                raise ValueError("role must be assistant or practice")
            self.role = role
            if role == "practice":
                self.scenario = self.playbook.scenario(scenario_key) or self.playbook.scenarios[0]
                self._pending_practice = True
                # The call is created by ensure_practice_call() when the socket applies the config.

    async def ensure_practice_call(self, hub: Any) -> None:
        """After a role switch to practice: create the call the rep's and customer's turns go into."""
        if self._pending_practice and self.call_id is None:
            self.call_id = await self.deps.new_practice_call(self.playbook.name)
            self._pending_practice = False
            self.attach_call_events(hub)

    def _start_turn(self, *, wav: Optional[bytes] = None, text: Optional[str] = None) -> None:
        if self.turn >= MAX_SESSION_TURNS:
            raise ValueError("turn limit reached for this session")
        prev = self._task
        if prev is not None and not prev.done() and self._current is not None and self._current.cancellable:
            prev.cancel()          # a new turn while one is speaking is a barge-in
        self.turn += 1
        self._task = asyncio.ensure_future(self._run(self.turn, prev, wav=wav, text=text))

    async def close(self) -> None:
        """Socket closed: stop the running turn, detach, and end a practice call (it lives with the socket)."""
        if self.closed:
            return
        self.closed = True
        if self.busy:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        self.detach_call_events()
        if self.role == "practice" and self.call_id:
            try:
                await self.deps.finish_call(self.call_id)
            except Exception:  # noqa: BLE001
                log.exception("ending practice call failed")

    # --------------------------------------------------------------- turn
    async def _run(self, turn_no: int, prev: Optional[asyncio.Task[None]], *, wav: Optional[bytes] = None,
                   text: Optional[str] = None, opening: Optional[str] = None) -> None:
        t0 = time.monotonic()
        rec = _Turn(turn=turn_no, opening=opening is not None)
        rec.timing = {"stt_ms": None, "llm_first_token_ms": None, "tts_first_audio_ms": None,
                      "first_audio_ms": None, "total_ms": None}
        interrupted = False
        try:
            if prev is not None:
                await asyncio.gather(prev, return_exceptions=True)   # the previous turn cleans up first
            rec.cancellable = True
            self._current = rec
            if opening is not None:
                await self._reply(rec, t0, messages=None, fixed_text=opening)
            else:
                user_text = await self._user_text(rec, wav, text)
                rec.user_text = user_text
                if user_text and self.role == "practice":
                    self._inject("agent", user_text)
                if user_text:
                    messages = await self._messages(user_text)
                    await self._reply(rec, t0, messages=messages, fixed_text=None)
        except asyncio.CancelledError:
            interrupted = True
        except Exception as exc:  # noqa: BLE001 - a bad turn must not kill the session
            log.exception("voice turn %s failed", turn_no)
            self._error(turn_no, "llm", exc)
        finally:
            rec.cancellable = False
            spoken = "".join(rec.agent_parts).strip()
            if self.role == "practice" and spoken and not rec.customer_injected:
                self._inject("customer", spoken)
                rec.customer_injected = True
            await self._finish_turn(rec, t0, interrupted=interrupted, spoken=spoken)

    async def _user_text(self, rec: _Turn, wav: Optional[bytes], text: Optional[str]) -> str:
        if wav is None:
            rec.timing["stt_ms"] = 0.0
            user_text = (text or "").strip()
        else:
            t = time.monotonic()
            try:
                user_text = (await self.deps.stt.transcribe(wav) or "").strip()
            except Exception as exc:  # noqa: BLE001 - STT failure: no reply this turn, the session goes on
                log.warning("voice STT failed: %s", exc)
                self._error(rec.turn, "stt", exc)
                user_text = ""
            rec.timing["stt_ms"] = _ms(t)
        self._emit({"type": "user_text", "turn": rec.turn, "text": user_text,
                    "stt_ms": round(rec.timing["stt_ms"] or 0.0)})
        return user_text

    async def _messages(self, user_text: str) -> list[dict[str, str]]:
        if self.role == "practice":
            return practice_messages(self.playbook, self.scenario, self.history, user_text)
        call_block = await self._call_block()
        hits = await self._kb_hits(user_text)
        return assistant_messages(self.playbook, user_text, self.history, call_block, hits)

    async def _call_block(self) -> str:
        if not self.call_id:
            return ""
        try:
            snap = await self.deps.store.snapshot(self.call_id)
        except Exception:  # noqa: BLE001 - store outage: answer without the transcript
            log.exception("call snapshot for voice context failed")
            snap = {}
        call = self.deps.calls.get(self.call_id)
        coverage = call.coverage_event()["items"] if call is not None else []
        return call_text(snap, coverage)

    async def _kb_hits(self, user_text: str) -> list[Hit]:
        try:
            return await asyncio.wait_for(self.deps.retriever.search(self.playbook, user_text), KB_BUDGET_S)
        except Exception:  # noqa: BLE001 - retrieval is best effort in voice
            return []

    async def _reply(self, rec: _Turn, t0: float, *, messages: Optional[list[dict[str, str]]],
                     fixed_text: Optional[str]) -> None:
        """Stream the spoken reply, start TTS per sentence, emit audio in order, then the items."""
        sentences: asyncio.Queue[Optional[asyncio.Queue[Any]]] = asyncio.Queue()
        tts_tasks: list[asyncio.Task[None]] = []
        emitter = asyncio.ensure_future(self._emit_audio(rec, t0, sentences))
        chunker = SentenceChunker()
        splitter = ReplySplitter()

        def start_sentence(sentence: str) -> None:
            if "tts_started" not in rec.marks:
                rec.marks["tts_started"] = time.monotonic()
            q: asyncio.Queue[Any] = asyncio.Queue()
            tts_tasks.append(asyncio.ensure_future(self._speak(rec, sentence, q)))
            sentences.put_nowait(q)

        def say(text: str) -> None:
            rec.agent_parts.append(text)
            self._emit({"type": "agent_text", "turn": rec.turn, "delta": text})
            for s in chunker.feed(text):
                start_sentence(s)

        try:
            if fixed_text is not None:
                rec.timing["llm_first_token_ms"] = 0.0
                say(fixed_text)
                for s in chunker.flush():
                    start_sentence(s)
                if self.role == "practice":
                    self._inject("customer", fixed_text)
                    rec.customer_injected = True
                splitter = None
            else:
                stream = self.deps.llm.chat_stream(self.settings.voice_model, messages or [],
                                                   max_tokens=self.settings.voice_max_tokens,
                                                   timeout=self.settings.voice_timeout_s)
                try:
                    async for delta in stream:
                        if rec.timing["llm_first_token_ms"] is None:
                            rec.timing["llm_first_token_ms"] = _ms(t0)
                        spoken = splitter.feed(delta)
                        if spoken:
                            say(spoken)
                except LlmUnavailable as exc:
                    self._error(rec.turn, "llm", exc)
                finally:
                    await stream.aclose()
                tail = splitter.finish()
                if tail:
                    say(tail)
                for s in chunker.flush():
                    start_sentence(s)
                if self.role == "practice":
                    spoken = "".join(rec.agent_parts).strip()
                    if spoken:
                        self._inject("customer", spoken)
                        rec.customer_injected = True
            sentences.put_nowait(None)
            await emitter
        except asyncio.CancelledError:
            for t in tts_tasks:
                t.cancel()
            emitter.cancel()
            await asyncio.gather(*tts_tasks, emitter, return_exceptions=True)
            raise
        if splitter is not None and splitter.in_items:
            rec.items = parse_items(splitter.items_raw, turn=rec.turn, session_id=self.session_id, now_ms=now_ms())

    async def _speak(self, rec: _Turn, sentence: str, q: asyncio.Queue[Any]) -> None:
        """One sentence through Kokoro into its queue. The stream is always closed, even on cancel."""
        gen = self.deps.tts.stream(sentence, self.voice)
        try:
            async for chunk in gen:
                q.put_nowait(chunk)
        except TtsUnavailable as exc:
            log.warning("TTS failed for one sentence: %s", exc)
            self._error(rec.turn, "tts", exc)
        finally:
            await gen.aclose()
            q.put_nowait(None)

    async def _emit_audio(self, rec: _Turn, t0: float,
                          sentences: asyncio.Queue[Optional[asyncio.Queue[Any]]]) -> None:
        """Drain the per-sentence queues in order. Synthesis runs in parallel; playback order is kept."""
        seq = 0
        while True:
            q = await sentences.get()
            if q is None:
                return
            while True:
                chunk = await q.get()
                if chunk is None:
                    break
                if rec.timing["first_audio_ms"] is None:
                    rec.timing["first_audio_ms"] = _ms(t0)
                    started = rec.marks.get("tts_started", time.monotonic())
                    rec.timing["tts_first_audio_ms"] = _ms(started)
                self._emit({"type": "audio", "turn": rec.turn, "seq": seq, "rate": chunk.rate,
                            "pcm_b64": base64.b64encode(chunk.pcm).decode("ascii")})
                seq += 1

    # ------------------------------------------------------------ finish
    async def _finish_turn(self, rec: _Turn, t0: float, *, interrupted: bool, spoken: str) -> None:
        rec.timing["total_ms"] = _ms(t0)
        agent_text = spoken
        if rec.user_text:
            self.history.append({"role": "user", "content": rec.user_text})
        if agent_text:
            self.history.append({"role": "assistant", "content": agent_text})
        for item in rec.items:
            self.items.append(item)
            try:
                await self.deps.store.put_voice_item(self.session_id, item)
            except Exception:  # noqa: BLE001 - persistence is best effort during the session
                log.exception("voice item persist failed")
            if self.deps.history is not None:
                self.deps.history.emit("voice_items", hs.voice_item_row(self.session_id, item))
            self._emit({"type": "item", "turn": rec.turn, "item": _wire_item(item)})
        row = {"turn": rec.turn, "role": self.role, "user_text": rec.user_text, "agent_text": agent_text,
               "interrupted": interrupted, "timing": dict(rec.timing), "created_at_ms": now_ms()}
        if rec.user_text or rec.opening:
            self.turns.append(row)
            try:
                await self.deps.store.put_voice_turn(self.session_id, row)
            except Exception:  # noqa: BLE001
                log.exception("voice turn persist failed")
            if self.deps.history is not None:
                self.deps.history.emit("voice_turns", hs.voice_turn_row(self.session_id, row))
        self._emit({"type": "turn_done", "turn": rec.turn, "interrupted": interrupted, "timing": rec.timing})

    # ------------------------------------------------------------ helpers
    def _inject(self, speaker: str, text: str) -> None:
        call = self.deps.calls.get(self.call_id) if self.call_id else None
        if call is not None:
            call.inject_final(speaker, text)

    def snapshot(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id, "role": self.role, "playbook": self.playbook.name,
            "call_id": self.call_id, "voice": self.voice,
            "scenario": self.scenario.key if self.scenario else None,
            "turns": list(self.turns), "items": [_wire_item(i) for i in self.items],
        }


def _wire_item(item: dict[str, Any]) -> dict[str, Any]:
    keys = ("item_id", "session_id", "turn", "kind", "text", "owner", "due", "source_doc", "created_at_ms")
    return {k: item.get(k) for k in keys}

