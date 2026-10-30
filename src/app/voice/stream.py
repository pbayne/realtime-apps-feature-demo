"""Streaming user input for voice mode (SPEC 9.2, streaming input).

The client sends `stream_start`, then binary PCM16 mono 16 kHz frames of ~100 ms while the mic is open. Each stream
runs one StreamingSession (stt/parakeet.py: VAD gate, rolling 12 s window, LocalAgreement-2, endpointing) over a
bounded frame queue:

  LiveStt.push(pcm)  -> queue (dropped and counted when full: backpressure, never blocks the socket reader)
  StreamingSession   -> partial events (user_partial) and final events (one user turn each)

A stream ends in one of two ways. `end(flush=True)` (push-to-talk release, mode switch) flushes the open utterance
into a final. `abort()` (mute, socket close) cancels it and discards the open utterance.

Two more guards live here. A continuous utterance is force-ended after `max_turn_ms` by injecting silence, so one
turn never grows past the limit. And the decoder used for streaming partials never retries (a partial is stale
by the time a retry lands), while a final decode retries cold failures with the warm-up policy.
"""

from __future__ import annotations

import asyncio
import logging
import math
import time
from typing import Any, AsyncIterator, Callable

from llm import LlmClient, LlmUnavailable
from stt.base import Frame, TranscriptEvent, Window, Word
from stt.parakeet import StreamingSession, pcm_to_wav, transcription_request, words_from_response

from voice.resilience import RetryPolicy, WarmState, call_with_warmup

log = logging.getLogger("sidekick.voice.stream")

RATE = 16000
BYTES_PER_MS = RATE * 2 / 1000.0
FRAME_MS = 100
MAX_FRAME_BYTES = RATE * 2          # one second: a frame larger than this is a client bug
_END = object()


class VoiceDecoder:
    """Parakeet decoder for voice streams. Partials: one attempt, failures are reported and skipped.
    Finals: cold failures retry with the warm-up policy, so the endpoint's start-up does not lose the turn."""

    def __init__(self, llm: LlmClient, endpoint: str, *, emit_status: Callable[[dict[str, Any]], None],
                 policy: RetryPolicy, partial_timeout_s: float = 2.0, final_timeout_s: float = 10.0,
                 sleep: Callable[[float], Any] = asyncio.sleep) -> None:
        self._llm = llm
        self._endpoint = endpoint
        self._policy = policy
        self._partial_timeout = partial_timeout_s
        self._final_timeout = final_timeout_s
        self._sleep = sleep
        self.warm = WarmState("stt", emit_status)

    async def decode(self, window: Window) -> list[Word]:
        if not window.pcm:
            return []
        files, data = transcription_request(pcm_to_wav(window.pcm), self._endpoint)

        async def call(timeout: float) -> dict:
            return await self._llm.multipart(self._endpoint, "audio/transcriptions", files=files, data=data,
                                             timeout=timeout)

        if not window.final:
            try:
                body = await call(self._partial_timeout)
            except LlmUnavailable as exc:
                if getattr(exc, "cold", False):
                    self.warm.failed_cold(attempt=1)
                log.debug("partial decode skipped: %s", type(exc).__name__)
                return []
            self.warm.ok()
            return words_from_response(body, window.start_ms, window.end_ms)
        try:
            body = await call_with_warmup(lambda: call(self._final_timeout), self.warm, self._policy,
                                          sleep=self._sleep)
        except LlmUnavailable as exc:
            log.warning("final decode failed (%s); the turn has no words", type(exc).__name__)
            return []
        return words_from_response(body, window.start_ms, window.end_ms)


class LiveStt:
    """One streaming STT channel for a voice session. See the module docstring."""

    def __init__(self, *, speaker: str, decoder: Any, vad: Any, next_seq: Callable[[], int],
                 on_partial: Callable[[TranscriptEvent], None], on_final: Callable[[TranscriptEvent], None],
                 frame_s: float = 0.4, window_s: float = 12.0, hold_words: int = 1,
                 endpoint_silence_ms: int = 700, max_turn_ms: float = 30_000.0, queue_max: int = 300,
                 preroll_ms: int = 300) -> None:
        self.speaker = speaker
        self._decoder = decoder
        self._vad = vad
        self._next_seq = next_seq
        self._on_partial = on_partial
        self._on_final = on_final
        self._frame_s = frame_s
        self._window_s = window_s
        self._hold_words = hold_words
        self.endpoint_silence_ms = endpoint_silence_ms
        self._max_turn_ms = max_turn_ms
        self._queue_max = max(1, queue_max)
        self._preroll_ms = preroll_ms
        self._q: asyncio.Queue[Any] = asyncio.Queue()    # bounded by _size, so sentinels always fit
        self._size = 0
        self.dropped = 0
        self.frames = 0
        self.forced_endpoints = 0
        self._t_ms = 0.0
        self._last_final_ms = 0.0

    # ------------------------------------------------------------ inputs
    def push(self, pcm: bytes) -> bool:
        """Queue one frame. False when the queue is full: the frame is dropped and counted (backpressure)."""
        if self._size >= self._queue_max:
            self.dropped += 1
            return False
        self._size += 1
        self._q.put_nowait(pcm)
        return True

    def end(self) -> None:
        """Flush: the open utterance (if any) is finalised, then run() returns. To discard instead, cancel
        the task that runs this channel (mute and socket close do that)."""
        self._q.put_nowait(_END)

    # ------------------------------------------------------------ run
    async def run(self) -> None:
        session = StreamingSession(
            self.speaker, self._decoder, self._vad, self._next_seq,
            frame_s=self._frame_s, window_s=self._window_s, hold_words=self._hold_words,
            endpoint_silence_ms=self.endpoint_silence_ms, preroll_ms=self._preroll_ms,
        )
        async for ev in session.run(self._frames()):
            if ev.kind == "partial":
                self._on_partial(ev)
            else:
                self._last_final_ms = self._t_ms
                self._on_final(ev)

    async def _frames(self) -> AsyncIterator[Frame]:
        while True:
            item = await self._q.get()
            if item is _END:
                return
            self._size -= 1
            pcm = item
            dur = len(pcm) / BYTES_PER_MS
            self.frames += 1
            yield Frame(t_ms=self._t_ms, wall=time.monotonic(), dur_ms=dur, pcm=pcm)
            self._t_ms += dur
            if self._t_ms - self._last_final_ms >= self._max_turn_ms:
                # One turn may not grow past the limit: end it with silence, as a pause would.
                self.forced_endpoints += 1
                for _ in range(math.ceil((self.endpoint_silence_ms + FRAME_MS) / FRAME_MS)):
                    yield Frame(t_ms=self._t_ms, wall=time.monotonic(), dur_ms=FRAME_MS,
                                pcm=b"\x00" * (RATE * 2 * FRAME_MS // 1000), speech=False)
                    self._t_ms += FRAME_MS
                self._last_final_ms = self._t_ms
