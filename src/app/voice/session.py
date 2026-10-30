"""VoiceSession: one voice conversation (SPEC 9.1 to 9.4, 9.6, 9.7).

A turn is one user utterance. It arrives three ways, and all three run the same turn pipeline:
  - streamed PCM (`stream_start`, then binary PCM16 frames): streaming STT emits `user_partial` while the user
    talks and a final at the endpoint (700 ms of silence, or `stream_stop` with flush). The final starts the turn.
  - a whole WAV (the compatibility path, client-side VAD): STT runs once, then the turn starts.
  - typed text.
Per turn:
  1. `user_text` with stt_ms.
  2. Practice: the rep's text goes into the call session as an `agent` final, so enrichment, decide,
     cards and coverage run exactly as for a live call. Assistant: context = linked call + KB hits.
  3. One LLM stream. Text before `<<ITEMS>>` is the spoken reply: each `agent_text` delta goes out at once,
     and each complete sentence starts its own Kokoro stream. A single emitter sends the audio in sentence
     order (`audio` events, seq 0..n). Text after the delimiter is parsed into items after the audio.
  4. Practice: the reply text is injected into the call session as a `customer` final.
  5. `turn_done` with timing, and the turn is persisted (voice_turns, voice_items).

While the user talks (streamed or not), voice/live.py analyses the partials and the finals: `live` events
(sentiment, intent, signals, coverage) and provisional `suggestion` chips. The chips can be pinned (`pin`) or
dismissed (`dismiss`). The end-of-turn <<ITEMS>> list stays authoritative.

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
from functools import lru_cache
from typing import Any, Awaitable, Callable, Optional

from config import Settings
from enrich.rules import Rules, load_rules
from history import schema as hs
from llm import LlmClient, LlmUnavailable
from models import now_ms
from sidekick.playbooks import Playbook, PracticeScenario
from sidekick.retrieval import Hit, Retriever
from store.base import Store
from stt.base import TranscriptEvent
from stt.vad import EnergyVad

from voice.items import ReplySplitter, parse_items
from voice.live import LiveAnalyzer
from voice.logs import vlog
from voice.prompts import assistant_messages, call_text, practice_messages
from voice.resilience import RetryPolicy, WarmState, call_with_warmup
from voice.sentences import SentenceChunker
from voice.stream import MAX_FRAME_BYTES, LiveStt
from voice.tts import PcmChunk, TtsUnavailable

log = logging.getLogger("sidekick.voice")

VOICE_RE = re.compile(r"^[a-z]{2}_[a-z]+$")
MAX_SESSION_TURNS = 500
KB_BUDGET_S = 1.5
RATE = 16000
FRAME_MS = 100
# Events a slow socket may lose: captions and analysis are refreshed constantly, so dropping one is harmless.
LOSSY = frozenset({"user_partial", "live", "suggestion"})


class Transcriber:
    """Protocol: WAV bytes in, text out (stt/parakeet.py ParakeetTranscriber)."""

    async def transcribe(self, wav: bytes) -> str: ...


class Speaker:
    """Protocol: sentence in, PCM chunks out (voice/tts.py KokoroTts). Must close its stream on aclose()."""

    def stream(self, text: str, voice: str) -> Any: ...


@lru_cache(maxsize=1)
def _default_rules() -> Rules:
    return load_rules()


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
    # Streaming input (SPEC 9.2): builds the decoder for one stream from a status emitter. None = streaming off.
    stream_decoder: Optional[Callable[[Callable[[dict[str, Any]], None]], Any]] = None
    vad: Any = None                            # voice-activity gate for streams; None = energy VAD
    rules_for: Optional[Callable[[str], Rules]] = None


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
    warm_tts: Optional[WarmState] = None


def _ms(since: float) -> float:
    return round((time.monotonic() - since) * 1000.0, 1)



# Non-lexical hesitations only. Short answers ("yeah", "okay", "right", "mm-hmm", "no") are real turns.
_FILLER = {"uh", "um", "uhm", "umm", "er", "erm", "ah", "eh", "hm", "hmm", "mm", "huh"}


def is_filler(text: str) -> bool:
    """True for a streamed final that is only noise or hesitation: no word outside the filler set.
    Typed turns are never filtered; this applies to transcribed speech only."""
    words = [w for w in re.sub(r"[^a-z' ]+", " ", text.lower().replace("-", "")).split() if w]
    return not words or all(w.strip("'") in _FILLER for w in words)


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
        self.dropped_events = 0
        self._silenced_turns: set[int] = set()
        self.dropped_frames = 0
        self._pending_practice = False
        self._current: Optional[_Turn] = None   # the turn whose work may be interrupted
        self._task: Optional[asyncio.Task[None]] = None
        self._hub_q: Optional[asyncio.Queue[Optional[dict[str, Any]]]] = None
        self._hub: Any = None
        self._hub_task: Optional[asyncio.Task[None]] = None
        self._stream: Optional[LiveStt] = None
        self._stream_task: Optional[asyncio.Task[None]] = None
        self._flushing: set[asyncio.Task[None]] = set()
        self._stt_seq = 0
        self._deadline: Optional[asyncio.Task[None]] = None
        rules = (deps.rules_for or (lambda _name: _default_rules()))(playbook.name)
        self.live = LiveAnalyzer(
            session_id=session_id, role=role, playbook_title=playbook.title,
            methodology=[(m.key, m.label) for m in playbook.methodology], rules=rules,
            llm=deps.llm, fast_model=deps.settings.live_suggest_model or deps.settings.voice_model, emit=self._emit,
            context=self._live_context, scenario=scenario.title if scenario else None,
            debounce_s=deps.settings.live_debounce_ms / 1000.0,
            min_new_words=deps.settings.live_min_new_words,
            suggest_interval_s=deps.settings.live_suggest_interval_s,
            suggest_timeout_s=deps.settings.live_suggest_timeout_s,
            max_tokens=deps.settings.live_suggest_max_tokens,
        )

    # ------------------------------------------------------------ plumbing
    @property
    def settings(self) -> Settings:
        return self.deps.settings

    @property
    def busy(self) -> bool:
        return self._task is not None and not self._task.done()

    @property
    def streaming(self) -> bool:
        return self._stream is not None

    def _user_label(self) -> str:
        """Speaker label for the user's own speech in live events: the assistant's user, or the practice rep."""
        return "user" if self.role == "assistant" else "agent"

    def _emit(self, event: dict[str, Any]) -> None:
        if event.get("type") == "audio" and event.get("turn") in self._silenced_turns:
            return  # barge-in: this turn's audio is no longer wanted
        event.setdefault("emitted_at_ms", now_ms())
        if event.get("type") in LOSSY and self.out.qsize() >= self.settings.voice_out_queue_max:
            self.dropped_events += 1
            return
        self.out.put_nowait(event)

    def _error(self, turn: int, stage: str, exc: BaseException | str) -> None:
        self._emit({"type": "error", "turn": turn, "stage": stage, "message": str(exc)[:300]})

    def _warm(self, stage: str, turn: int) -> WarmState:
        return WarmState(stage, lambda ev: self._emit({**ev, "turn": turn}))

    def _policy(self) -> RetryPolicy:
        s = self.settings
        return RetryPolicy(budget_s=s.voice_warmup_budget_s, base_s=s.voice_retry_base_s, cap_s=s.voice_retry_cap_s)

    def _max_wav_bytes(self) -> int:
        seconds_cap = int(self.settings.voice_max_turn_audio_s * RATE * 2) + 44
        return min(self.settings.voice_max_audio_bytes, seconds_cap)

    def _live_context(self) -> str:
        return "\n".join(f"{m['role']}: {m['content']}" for m in self.history[-6:])

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

    def arm_deadline(self, seconds: float) -> None:
        """Wall-clock limit for the socket (voice_max_session_minutes). On expiry the session says so and ends."""
        if self._deadline is None:
            self._deadline = asyncio.ensure_future(self._expire_after(seconds))

    async def _expire_after(self, seconds: float) -> None:
        await asyncio.sleep(seconds)
        vlog("limit_hit", session_id=self.session_id, limit="max_session_minutes")
        self._emit({"type": "session_ended", "reason": "max_session_minutes"})
        await self.close()
        self.out.put_nowait(None)

    # ------------------------------------------------------------- inputs
    def submit_audio(self, wav: bytes) -> None:
        if not wav:
            raise ValueError("empty audio turn")
        if wav[:4] != b"RIFF" or wav[8:12] != b"WAVE":
            raise ValueError("each binary frame must be one complete WAV turn (16 kHz mono PCM16)")
        if len(wav) > self._max_wav_bytes():
            raise ValueError(f"audio turn is too long (max {self.settings.voice_max_turn_audio_s:g} s of 16 kHz mono PCM16)")
        self._start_turn(wav=wav)

    def stream_start(self, sample_rate: int = RATE, endpoint_silence_ms: Optional[int] = None) -> None:
        """Open a streaming input (SPEC 9.2). A stream already open is discarded first."""
        if self.closed:
            raise ValueError("voice session is closed")
        if sample_rate != RATE:
            raise ValueError("sample_rate must be 16000 (16 kHz mono PCM16)")
        silence = self.settings.voice_endpoint_silence_ms if endpoint_silence_ms is None else int(endpoint_silence_ms)
        if not 300 <= silence <= 2000:
            raise ValueError("endpoint_silence_ms must be between 300 and 2000")
        if self.deps.stream_decoder is None:
            raise ValueError("streaming input is not available in this process")
        self.abort_stream()
        stream = LiveStt(
            speaker=self._user_label(), decoder=self.deps.stream_decoder(self._status_emitter()),
            vad=self.deps.vad or EnergyVad(self.settings.vad_rms_threshold), next_seq=self._next_stt_seq,
            on_partial=self._on_stream_partial, on_final=self._on_stream_final,
            frame_s=self.settings.stt_frame_s, window_s=self.settings.stt_window_s,
            hold_words=self.settings.stt_hold_words, endpoint_silence_ms=silence,
            max_turn_ms=self.settings.voice_max_turn_audio_s * 1000.0,
            queue_max=self.settings.voice_stream_queue_max,
        )
        self._stream = stream
        self._stream_task = asyncio.ensure_future(self._run_stream(stream))
        vlog("stream_started", session_id=self.session_id, turn=self.turn + 1, endpoint_silence_ms=silence)
        self._emit({"type": "stream_started", "sample_rate": RATE, "endpoint_silence_ms": silence,
                    "frame_ms": FRAME_MS})

    def stream_frame(self, pcm: bytes) -> None:
        """One binary PCM16 frame. Frames are dropped (and counted) when the stream falls behind."""
        live = self._stream
        if live is None:
            raise ValueError("binary frame is not a WAV turn and no stream is open; WAV turns carry a RIFF header, "
                             "and streamed PCM needs stream_start first")
        if not pcm or len(pcm) % 2:
            raise ValueError("stream frame must be whole PCM16 samples")
        if len(pcm) > MAX_FRAME_BYTES:
            raise ValueError("stream frame is too large (max 1 s of 16 kHz mono PCM16)")
        if not live.push(pcm):
            self.dropped_frames += 1
            if self.dropped_frames == 1 or self.dropped_frames % 50 == 0:
                vlog("stream_backpressure", session_id=self.session_id, turn=self.turn + 1,
                     dropped_frames=self.dropped_frames, level=logging.WARNING)

    def stream_stop(self, flush: bool = True) -> None:
        """Close the stream. flush=True finalises the open utterance into a turn (push-to-talk release).
        flush=False discards it (mute)."""
        live, task = self._stream, self._stream_task
        if live is None:
            return
        self._stream = None
        self._stream_task = None
        if flush:
            live.end()
            if task is not None:
                self._flushing.add(task)
                task.add_done_callback(self._flushing.discard)
        elif task is not None and not task.done():
            task.cancel()
        vlog("stream_stopped", session_id=self.session_id, turn=self.turn + 1, flush=flush,
             dropped_frames=live.dropped)
        self._emit({"type": "stream_stopped", "flush": flush, "dropped_frames": live.dropped})

    def abort_stream(self) -> None:
        """Discard the open stream without a final (mute, socket close, a new stream_start)."""
        if self._stream is not None:
            self.stream_stop(flush=False)

    async def _run_stream(self, live: LiveStt) -> None:
        try:
            await live.run()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - a broken stream is reported, the socket stays up
            vlog("stream_failed", session_id=self.session_id, turn=self.turn + 1, level=logging.WARNING,
                 error=type(exc).__name__)
            self._error(self.turn + 1, "stt", exc)

    def _status_emitter(self) -> Callable[[dict[str, Any]], None]:
        return lambda ev: self._emit({**ev, "turn": self.turn + 1})

    def _next_stt_seq(self) -> int:
        self._stt_seq += 1
        return self._stt_seq

    def _on_stream_partial(self, ev: TranscriptEvent) -> None:
        turn = self.turn + 1
        self._emit({"type": "user_partial", "turn": turn, "text": ev.text, "seq": ev.seq,
                    "first_word_ms": ev.first_word_ms})
        self.live.on_partial(turn, ev.text, self._user_label())

    def _on_stream_final(self, ev: TranscriptEvent) -> None:
        text = (ev.text or "").strip()
        if not text:
            return
        if is_filler(text):
            # Noise, a cough or a hesitation ("uh", "mm-hmm") must not start a reply. Clear the caption.
            vlog("turn_skipped", session_id=self.session_id, reason="filler")
            self._emit({"type": "user_partial", "turn": self.turn + 1, "text": "", "first_word_ms": None})
            return
        try:
            self._start_turn(text=text, stt_ms=ev.commit_ms)
        except ValueError as exc:
            self._error(self.turn + 1, "llm", exc)

    def interrupt(self) -> None:
        """Barge-in: cancel the running turn while its work is in flight. Its cleanup emits
        turn_done(interrupted=true). A turn that has finished (or is finishing) is never interrupted, so an
        interrupt that arrives after turn_done is ignored."""
        if self.busy and self._current is not None and self._current.cancellable:
            self._silence(self._current.turn)
            self._task.cancel()

    def _silence(self, turn: int) -> None:
        """Drop the interrupted turn's audio that is still queued for the socket, and any it would still emit."""
        self._silenced_turns.add(turn)
        kept: list[Any] = []
        while not self.out.empty():
            ev = self.out.get_nowait()
            if not (isinstance(ev, dict) and ev.get("type") == "audio" and ev.get("turn") == turn):
                kept.append(ev)
        for ev in kept:
            self.out.put_nowait(ev)

    def submit_text(self, text: str) -> None:
        text = (text or "").strip()
        if not text:
            raise ValueError("text turn is empty")
        self._start_turn(text=text)

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
            self.live.set_role(role, self.scenario.title if (role == "practice" and self.scenario) else None)

    async def ensure_practice_call(self, hub: Any) -> None:
        """After a role switch to practice: create the call the rep's and customer's turns go into."""
        if self._pending_practice and self.call_id is None:
            self.call_id = await self.deps.new_practice_call(self.playbook.name)
            self._pending_practice = False
            self.attach_call_events(hub)

    # --------------------------------------------------------- pin / dismiss
    async def pin(self, item_id: str) -> dict[str, Any]:
        """A provisional suggestion becomes an outcome item (SPEC 9.7). Same id, same persistence as <<ITEMS>>."""
        item = {k: v for k, v in self.live.take(item_id).items() if k != "provisional"}
        item["created_at_ms"] = now_ms()
        await self._persist_item(item)
        vlog("pin", session_id=self.session_id, turn=item["turn"], kind=item["kind"])
        self._emit({"type": "item", "turn": item["turn"], "item": _wire_item(item)})
        return item

    def dismiss(self, item_id: str) -> None:
        self.live.drop(item_id)

    # ----------------------------------------------------------------- turn
    def _start_turn(self, *, wav: Optional[bytes] = None, text: Optional[str] = None,
                    stt_ms: Optional[float] = None) -> None:
        if self.turn >= MAX_SESSION_TURNS:
            raise ValueError("turn limit reached for this session")
        prev = self._task
        if prev is not None and not prev.done() and self._current is not None and self._current.cancellable:
            prev.cancel()          # a new turn while one is speaking is a barge-in
        self.turn += 1
        self._task = asyncio.ensure_future(self._run(self.turn, prev, wav=wav, text=text, stt_ms=stt_ms))

    async def close(self) -> None:
        """Socket closed (or the session limit was hit): stop the running turn, detach, and end a practice call
        (it lives with the socket)."""
        if self.closed:
            return
        self.closed = True
        vlog("session_closed", session_id=self.session_id, role=self.role, turns=self.turn,
             dropped_events=self.dropped_events, dropped_frames=self.dropped_frames)
        self.live.close()
        self.abort_stream()
        if self._deadline is not None and self._deadline is not asyncio.current_task():
            self._deadline.cancel()
        if self.busy:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        self.detach_call_events()
        if self.role == "practice" and self.call_id:
            try:
                await self.deps.finish_call(self.call_id)
            except Exception:  # noqa: BLE001
                log.exception("ending practice call failed")

    async def _run(self, turn_no: int, prev: Optional[asyncio.Task[None]], *, wav: Optional[bytes] = None,
                   text: Optional[str] = None, opening: Optional[str] = None,
                   stt_ms: Optional[float] = None) -> None:
        t0 = time.monotonic()
        rec = _Turn(turn=turn_no, opening=opening is not None)
        rec.timing = {"stt_ms": None, "llm_first_token_ms": None, "tts_first_audio_ms": None,
                      "first_audio_ms": None, "total_ms": None}
        rec.warm_tts = self._warm("tts", turn_no)
        interrupted = False
        try:
            if prev is not None:
                await asyncio.gather(prev, return_exceptions=True)   # the previous turn cleans up first
            rec.cancellable = True
            self._current = rec
            if opening is not None:
                vlog("turn_start", session_id=self.session_id, turn=turn_no, input="opening")
                await self._reply(rec, t0, messages=None, fixed_text=opening)
            else:
                source = "wav" if wav is not None else ("stream" if stt_ms is not None else "text")
                vlog("turn_start", session_id=self.session_id, turn=turn_no, input=source)
                user_text = await self._user_text(rec, wav, text, stt_ms)
                rec.user_text = user_text
                if user_text:
                    if self.role == "practice":
                        self._inject("agent", user_text)
                    self.live.on_final(turn_no, user_text, self._user_label())
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

    async def _user_text(self, rec: _Turn, wav: Optional[bytes], text: Optional[str],
                         stt_ms: Optional[float] = None) -> str:
        if wav is None:
            rec.timing["stt_ms"] = float(stt_ms or 0.0)
            user_text = (text or "").strip()
        else:
            t = time.monotonic()
            try:
                raw = await call_with_warmup(lambda: self.deps.stt.transcribe(wav), self._warm("stt", rec.turn),
                                             self._policy(), sleep=asyncio.sleep)
                user_text = (raw or "").strip()
            except Exception as exc:  # noqa: BLE001 - STT failure: no reply this turn, the session goes on
                log.warning("voice STT failed: %s", type(exc).__name__)
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
        """One sentence through Kokoro into its queue. A cold endpoint is retried before the first audio arrives
        (SPEC 9.6). The stream is always closed, even on cancel."""
        gen: Any = None
        try:
            gen, first = await call_with_warmup(
                lambda: _first_chunk(self.deps.tts, sentence, self.voice),
                rec.warm_tts or self._warm("tts", rec.turn), self._policy(), sleep=asyncio.sleep,
            )
            if first is not None:
                q.put_nowait(first)
                async for chunk in gen:
                    q.put_nowait(chunk)
        except TtsUnavailable as exc:
            log.warning("TTS failed for one sentence: %s", type(exc).__name__)
            self._error(rec.turn, "tts", exc)
        finally:
            if gen is not None:
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
    async def _persist_item(self, item: dict[str, Any]) -> None:
        self.items.append(item)
        try:
            await self.deps.store.put_voice_item(self.session_id, item)
        except Exception:  # noqa: BLE001 - persistence is best effort during the session
            log.exception("voice item persist failed")
        if self.deps.history is not None:
            self.deps.history.emit("voice_items", hs.voice_item_row(self.session_id, item))

    async def _finish_turn(self, rec: _Turn, t0: float, *, interrupted: bool, spoken: str) -> None:
        rec.timing["total_ms"] = _ms(t0)
        agent_text = spoken
        if rec.user_text:
            self.history.append({"role": "user", "content": rec.user_text})
        if agent_text:
            self.history.append({"role": "assistant", "content": agent_text})
        for item in rec.items:
            await self._persist_item(item)
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
        if self.role == "practice" and spoken and not interrupted:
            self.live.on_final(rec.turn, spoken, "customer", suggest=False)
        self.live.retire_turn(rec.turn)
        vlog("turn_done", session_id=self.session_id, turn=rec.turn, interrupted=interrupted,
             items=len(rec.items), total_ms=rec.timing.get("total_ms"), first_audio_ms=rec.timing.get("first_audio_ms"))
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
            "sentiment": list(self.live.series),
            "suggestions": list(self.live.provisional.values()),
            "streaming": self.streaming, "dropped_frames": self.dropped_frames,
        }


async def _first_chunk(tts: Speaker, sentence: str, voice: str) -> tuple[Any, Optional[PcmChunk]]:
    """Open one sentence's TTS stream and read its first chunk. Returns (generator, chunk or None when empty).
    Raises TtsUnavailable (cold or not) before any audio, so the caller may retry. The generator is closed on error."""
    gen = tts.stream(sentence, voice)
    try:
        first = await gen.__anext__()
    except StopAsyncIteration:
        return gen, None
    except BaseException:
        await gen.aclose()
        raise
    return gen, first


def _wire_item(item: dict[str, Any]) -> dict[str, Any]:
    keys = ("item_id", "session_id", "turn", "kind", "text", "owner", "due", "source_doc", "created_at_ms")
    return {k: item.get(k) for k in keys}
