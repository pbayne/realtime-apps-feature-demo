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
from voice.tts import PcmChunk


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
                 perception: Optional[dict[str, Any]] = None, fail: bool = False) -> None:
        self.voice_script = voice_script
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
        return json.dumps(self.perception)

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
    def __init__(self, text: str = "what are your fees", fail: bool = False) -> None:
        self.text = text
        self.fail = fail
        self.calls = 0

    async def transcribe(self, wav: bytes) -> str:
        self.calls += 1
        if self.fail:
            raise LlmUnavailable("stt down")
        return self.text


class FakeTts:
    """Each sentence becomes PCM whose first byte is the sentence index, so tests can check playback order.
    `delay_for(index, text)` controls how long each sentence takes (out-of-order completion is the point)."""

    def __init__(self, *, delay_for: Optional[Callable[[int, str], float]] = None, chunks: int = 2,
                 fail: bool = False, hang: float = 0.0) -> None:
        self.delay_for = delay_for or (lambda i, text: 0.0)
        self.chunks = chunks
        self.fail = fail
        self.hang = hang
        self.sentences: list[str] = []
        self.opened: list[dict[str, Any]] = []   # one record per stream: {"text", "closed"}
        self._index: dict[str, int] = {}

    async def _gen(self, text: str, voice: str, record: dict[str, Any]) -> AsyncIterator[PcmChunk]:
        from voice.tts import TtsUnavailable

        try:
            if self.fail:
                raise TtsUnavailable("tts down")
            idx = self._index[text]
            await asyncio.sleep(self.delay_for(idx, text))
            for c in range(self.chunks):
                if self.hang:
                    await asyncio.sleep(self.hang)
                yield PcmChunk(24000, bytes([idx, c]) * 2)
        finally:
            record["closed"] = True

    def stream(self, text: str, voice: str):
        from voice.tts import TtsUnavailable  # noqa: F401  (imported for symmetry with _gen)

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
