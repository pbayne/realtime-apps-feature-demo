"""Fakes for voice-mode tests: a scripted voice LLM, a fake STT and a fake Kokoro TTS. No network.

FakeTts records every stream it opens and whether it was closed, so tests can assert that cancelled
turns close their streams (SPEC 3.3a: an unread Kokoro stream stalls its worker).
"""

from __future__ import annotations

import asyncio
import json
import re
from typing import Any, AsyncIterator, Callable, Optional

from llm import LlmUnavailable
from voice.live import SYSTEM_MARKER
from voice.tts import PcmChunk, TtsUnavailable


class FakeStream:
    """Async iterator over scripted tokens. `delay` is awaited before each token. Tracks closure."""

    def __init__(self, tokens: list[str], delay: float = 0.0, owner: "FakeVoiceLlm | None" = None) -> None:
        self._tokens = list(tokens)
        self._delay = delay
        self.closed = False
        self.owner = owner

    def __aiter__(self):
        return self

    async def __anext__(self) -> str:
        if not self._tokens:
            raise StopAsyncIteration
        if self._delay:
            await asyncio.sleep(self._delay)
        return self._tokens.pop(0)

    async def aclose(self) -> None:
        self.closed = True
        if self.owner is not None:
            self.owner.closed_streams += 1


class FakeVoiceLlm:
    """Stands in for LlmClient. `voice_script` is the reply stream (tokens) for the voice model; any other
    model streams `card_text` (compose). `chat` answers perception with a fixed JSON object."""

    def __init__(self, voice_script: list[str], *, voice_delay: float = 0.0, card_text: str = "Ask about the fee schedule.",
                 perception: Optional[dict[str, Any]] = None, fail: bool = False,
                 suggestions: Optional[list[dict[str, Any]]] = None, suggest_delay: float = 0.0) -> None:
        self.voice_script = voice_script
        # Live suggestion pass (voice/live.py): the reply to any message that carries SYSTEM_MARKER.
        self.suggestions = suggestions if suggestions is not None else [
            {"kind": "follow_up_question", "text": "Ask what the all-in cost is."}]
        self.suggest_delay = suggest_delay
        self.suggest_calls = 0
        self.suggest_cancelled = 0
        self.suggest_messages: list[list[dict[str, str]]] = []
        self.voice_delay = voice_delay
        self.card_text = card_text
        self.perception = perception or {"intent": "objection", "sentiment": -0.3, "objection": None,
                                         "question": False, "confidence": 0.8}
        self.fail = fail
        self.available = True
        self.enabled = True
        self.last_error = None
        self.embed_model = "fake-embed"
        self.smart_model = "fake-smart"
        self.fast_model = "fake-fast"
        self.streams: list[FakeStream] = []
        self.closed_streams = 0
        self.voice_calls: list[list[dict[str, str]]] = []

    def chat_stream(self, model: str, messages, *, max_tokens: int = 160, timeout: Optional[float] = None):
        if model == "fake-smart":
            tokens = [self.card_text]
            s = FakeStream(tokens)
        else:
            self.voice_calls.append(list(messages))
            if self.fail:
                return _raise_stream(LlmUnavailable("voice model down"))
            s = FakeStream(list(self.voice_script), delay=self.voice_delay, owner=self)
        self.streams.append(s)
        return s

    async def chat(self, model: str, messages, *, max_tokens: int = 300, timeout: Optional[float] = None) -> str:
        if any(SYSTEM_MARKER in (m.get("content") or "") for m in messages):
            self.suggest_calls += 1
            self.suggest_messages.append(list(messages))
            try:
                if self.suggest_delay:
                    await asyncio.sleep(self.suggest_delay)
            except asyncio.CancelledError:
                self.suggest_cancelled += 1
                raise
            return json.dumps({"suggestions": self.suggestions})
        return json.dumps(self.perception)

    async def multipart(self, endpoint, route, *, files, data, timeout=None):
        """Parakeet stand-in for VoiceDecoder tests: scripted bodies, LlmUnavailable entries raise."""
        self.multipart_calls = getattr(self, "multipart_calls", 0) + 1
        script = getattr(self, "stt_script", None)
        if script:
            item = script.pop(0)
            if isinstance(item, Exception):
                raise item
            return item
        return {"text": "", "words": []}

    async def embed(self, model, texts, *, timeout=None):
        raise LlmUnavailable("no embeddings in this fake")

    async def aclose(self) -> None:
        return None

    async def workspace_auth(self) -> tuple[str, dict[str, str]]:
        return "https://workspace.example.com", {"Authorization": "Bearer fake"}

    def status(self) -> dict[str, Any]:
        return {"available": True}


async def _raise_stream_gen(exc: Exception):
    raise exc
    yield  # pragma: no cover


def _raise_stream(exc: Exception):
    return _raise_stream_gen(exc)


class FakeStt:
    """`cold_failures` = how many calls fail as a warming endpoint (cold=True) before one succeeds."""

    def __init__(self, text: str = "what are your fees", fail: bool = False, cold_failures: int = 0) -> None:
        self.text = text
        self.fail = fail
        self.cold_left = cold_failures
        self.calls = 0

    async def transcribe(self, wav: bytes) -> str:
        self.calls += 1
        if self.fail:
            raise LlmUnavailable("stt down")
        if self.cold_left > 0:
            self.cold_left -= 1
            raise LlmUnavailable("stt endpoint starting", cold=True)
        return self.text


class FakeTts:
    """Each sentence becomes PCM whose first byte is the sentence index, so tests can check playback order.
    `delay_for(index, text)` controls how long each sentence takes (out-of-order completion is the point)."""

    def __init__(self, *, delay_for: Optional[Callable[[int, str], float]] = None, chunks: int = 2,
                 fail: bool = False, hang: float = 0.0, cold_failures: int = 0) -> None:
        self.delay_for = delay_for or (lambda i, text: 0.0)
        self.chunks = chunks
        self.fail = fail
        self.hang = hang
        self.cold_left = cold_failures
        self.attempts = 0
        self.sentences: list[str] = []
        self.opened: list[dict[str, Any]] = []   # one record per stream: {"text", "closed"}
        self._index: dict[str, int] = {}

    async def _gen(self, text: str, voice: str, record: dict[str, Any]) -> AsyncIterator[PcmChunk]:

        try:
            self.attempts += 1
            if self.fail:
                raise TtsUnavailable("tts down")
            if self.cold_left > 0:
                self.cold_left -= 1
                raise TtsUnavailable("tts endpoint starting", cold=True)
            idx = self._index[text]
            await asyncio.sleep(self.delay_for(idx, text))
            for c in range(self.chunks):
                if self.hang:
                    await asyncio.sleep(self.hang)
                yield PcmChunk(24000, bytes([idx, c]) * 2)
        finally:
            record["closed"] = True

    def stream(self, text: str, voice: str):

        if text not in self._index:
            self._index[text] = len(self._index)
            self.sentences.append(text)
        record = {"text": text, "closed": False}
        self.opened.append(record)
        return self._gen(text, voice, record)


def events_of(events: list[dict[str, Any]], kind: str) -> list[dict[str, Any]]:
    return [e for e in events if e.get("type") == kind]


def wav_bytes(seconds: float = 0.1) -> bytes:
    import io
    import wave

    buf = io.BytesIO()
    with wave.open(buf, "wb") as f:
        f.setnchannels(1)
        f.setsampwidth(2)
        f.setframerate(16000)
        f.writeframes(b"\x00\x00" * int(16000 * seconds))
    return buf.getvalue()


SENTENCE_SPLIT = re.compile(r"(?<=[.!?])\s+")


# ---------------------------------------------------------------- streaming and session helpers (SPEC 9.2, 9.7)

import struct  # noqa: E402
from pathlib import Path as _Path  # noqa: E402

from config import Settings  # noqa: E402
from enrich.rules import load_rules  # noqa: E402
from sidekick.playbooks import PlaybookRegistry  # noqa: E402
from sidekick.retrieval import Retriever  # noqa: E402
from store.memory import MemoryStore  # noqa: E402
from stt.base import Window, Word  # noqa: E402

_APP = _Path(__file__).resolve().parents[2] / "src" / "app"
_REGISTRY: Optional[PlaybookRegistry] = None


def registry() -> PlaybookRegistry:
    global _REGISTRY
    if _REGISTRY is None:
        _REGISTRY = PlaybookRegistry.load(_APP / "playbooks", load_rules(), _APP / "data" / "replay")
    return _REGISTRY


class ScriptedStreamDecoder:
    """Words appear `lag` ms after they end in partial windows, and all at once in the final window (as replay)."""

    def __init__(self, words: list[Word], lag: int = 250) -> None:
        self.words = list(words)
        self.lag = lag

    async def decode(self, window: Window) -> list[Word]:
        limit = window.end_ms if window.final else window.end_ms - self.lag
        return [w for w in self.words if w.s >= window.start_ms - 1 and w.e <= limit]


def pcm_ms(ms: int, speech: bool) -> bytes:
    """`ms` of 16 kHz mono PCM16. Speech is a loud constant (RMS 3000, above the energy VAD); silence is zeros."""
    n = 16 * ms
    return struct.pack("<%dh" % n, *([3000 if speech else 0] * n))


def make_voice_session(llm, *, tts=None, stt=None, role="assistant", call_id=None, scenario=None, store=None,
                       stream_words=None, stream_lag=250, playbook="advisor_coach", **settings):
    """A VoiceSession wired like services.py: streaming decoder from `stream_words`, fakes for STT and TTS."""
    from voice.session import VoiceDeps, VoiceSession

    s = Settings(mode="local", llm_enabled=True, retrieval_backend="keyword", **settings)
    store = store or MemoryStore()

    async def no_finish(_):
        return None

    async def no_new(*_args):
        raise AssertionError("no practice call in this test")

    words = list(stream_words or [])

    def stream_decoder(emit):
        return ScriptedStreamDecoder(words, lag=stream_lag)

    deps = VoiceDeps(settings=s, llm=llm, tts=tts or FakeTts(), stt=stt or FakeStt(), store=store,
                     retriever=Retriever(llm, s, None, backend="keyword"), calls={},
                     finish_call=no_finish, new_practice_call=no_new, stream_decoder=stream_decoder)
    pb = registry().get(playbook)
    sc = (pb.scenario(scenario) if scenario else pb.scenarios[0]) if role == "practice" else None
    vs = VoiceSession(session_id="s-1", role=role, playbook=pb, voice="af_heart", deps=deps,
                      call_id=call_id, scenario=sc)
    return vs, store
