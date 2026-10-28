"""Streaming STT session (SPEC section 1 + section 3 audio contract).

StreamingSession runs one channel. Per utterance:
  - VAD gate decides speech vs silence per frame (Frame.speech overrides it for replay).
  - While speaking, every `frame_s` seconds the rolling window (last `window_s` s,
    clipped to the utterance start) is decoded by a Decoder.
  - LocalAgreement-2 with hold_words commits stable words; partial events carry
    committed + tail text.
  - `endpoint_silence_ms` of silence ends the utterance: one final decode over the
    whole utterance is flushed and a final event is emitted.

ParakeetDecoder calls the Parakeet serving endpoint (STT_ENDPOINT). This is the contract that
src/notebooks/register_parakeet.py implements (MLflow pyfunc, dataframe_records):

  request : {"dataframe_records": [{"audio_b64": "<base64 PCM16 LE mono 16 kHz, <= 30 s>",
                                    "offset_ms": <window start on the call timeline>,
                                    "channel": "agent" | "customer"}]}
  response: {"predictions": [{"text": "...",
                              "words": [{"word": "...", "start_ms": <absolute>, "end_ms": <absolute>}],
                              "audio_ms": 4300, "channel": "...", "model": "..."}]}

Word times in the response are already on the call timeline (the endpoint adds offset_ms), so
they are used as-is. A response with no word timestamps gets its text spread evenly over the window.
The endpoint may also be reached through its route-optimized data-plane URL (STT_ENDPOINT_URL).
"""

from __future__ import annotations

import base64
import logging
import time
from collections import deque
from dataclasses import dataclass
from typing import AsyncIterator, Callable, Optional

from llm import LlmClient, LlmUnavailable

from stt.agreement import LocalAgreement2
from stt.base import Decoder, Frame, TranscriptEvent, Window, Word
from stt.vad import Vad

log = logging.getLogger("sidekick.stt")


@dataclass
class _Utterance:
    seq: int
    start_ms: float
    first_wall: float
    agreement: LocalAgreement2
    end_ms: float
    last_tick_ms: float
    last_speech_wall: float
    first_word_ms: Optional[float] = None
    last_text: str = ""
    first_commit_wall: Optional[float] = None


def _window_pcm(frames: deque[Frame], start_ms: float, end_ms: float) -> Optional[bytes]:
    chunks = [f.pcm for f in frames if f.pcm and start_ms <= f.t_ms < end_ms]
    if not chunks:
        return None
    return b"".join(chunks)


class StreamingSession:
    def __init__(
        self,
        speaker: str,
        decoder: Decoder,
        vad: Vad,
        next_seq: Callable[[], int],
        *,
        frame_s: float = 0.4,
        window_s: float = 12.0,
        hold_words: int = 1,
        endpoint_silence_ms: int = 600,
        preroll_ms: int = 300,
    ) -> None:
        if frame_s < 0.3:
            raise ValueError("frame_s must be >= 0.3")
        self.speaker = speaker
        self.decoder = decoder
        self.vad = vad
        self.next_seq = next_seq
        self.frame_ms = frame_s * 1000.0
        self.window_ms = window_s * 1000.0
        self.hold_words = hold_words
        self.endpoint_silence_ms = endpoint_silence_ms
        # VAD flags speech a frame (or more) after onset; open the window earlier so the first
        # word is not clipped, but never before the previous utterance's end.
        self.preroll_ms = preroll_ms

    async def run(self, frames: AsyncIterator[Frame]) -> AsyncIterator[TranscriptEvent]:
        ring: deque[Frame] = deque()
        utt: Optional[_Utterance] = None
        silence_ms = 0.0
        last_end_ms = 0.0
        async for f in frames:
            speech = f.speech if f.speech is not None else self.vad.is_speech(f.pcm or b"")
            ring.append(f)
            while ring and f.t_ms - ring[0].t_ms > self.window_ms + 1000.0:
                ring.popleft()

            if speech:
                silence_ms = 0.0
                if utt is None:
                    utt = _Utterance(
                        seq=self.next_seq(),
                        start_ms=max(last_end_ms, f.t_ms - self.preroll_ms),
                        first_wall=f.wall,
                        agreement=LocalAgreement2(self.hold_words),
                        end_ms=f.t_ms,
                        last_tick_ms=f.t_ms - self.frame_ms,
                        last_speech_wall=f.wall,
                    )
                utt.end_ms = f.t_ms + f.dur_ms
                utt.last_speech_wall = f.wall
                if utt.end_ms - utt.last_tick_ms >= self.frame_ms:
                    utt.last_tick_ms = utt.end_ms
                    ev = await self._partial(utt, ring)
                    if ev is not None:
                        yield ev
            elif utt is not None:
                silence_ms += f.dur_ms
                if silence_ms >= self.endpoint_silence_ms:
                    ev = await self._final(utt, ring)
                    last_end_ms = utt.end_ms
                    utt = None
                    silence_ms = 0.0
                    if ev is not None:
                        yield ev

        if utt is not None:  # stream ended mid-utterance
            ev = await self._final(utt, ring)
            if ev is not None:
                yield ev

    async def _partial(self, utt: _Utterance, ring: deque[Frame]) -> Optional[TranscriptEvent]:
        win_start = max(utt.start_ms, utt.end_ms - self.window_ms)
        words = await self.decoder.decode(
            Window(int(win_start), int(utt.end_ms), _window_pcm(ring, win_start, utt.end_ms), final=False)
        )
        newly = utt.agreement.update(words)
        if newly and utt.first_commit_wall is None:
            utt.first_commit_wall = time.monotonic()
        text = utt.agreement.text()
        if not text or text == utt.last_text:
            return None
        if utt.first_word_ms is None:
            utt.first_word_ms = (time.monotonic() - utt.first_wall) * 1000.0
        utt.last_text = text
        return TranscriptEvent(
            kind="partial", speaker=self.speaker, seq=utt.seq, text=text,
            t_start_ms=int(utt.start_ms), t_end_ms=int(utt.end_ms),
            first_word_ms=utt.first_word_ms,
        )

    async def _final(self, utt: _Utterance, ring: deque[Frame]) -> Optional[TranscriptEvent]:
        words = await self.decoder.decode(
            Window(int(utt.start_ms), int(utt.end_ms), _window_pcm(ring, utt.start_ms, utt.end_ms), final=True)
        )
        utt.agreement.flush(words)
        text = utt.agreement.text()
        if not text:
            return None
        now = time.monotonic()
        if utt.first_word_ms is None:
            utt.first_word_ms = (now - utt.first_wall) * 1000.0
        return TranscriptEvent(
            kind="final", speaker=self.speaker, seq=utt.seq, text=text,
            t_start_ms=int(utt.start_ms), t_end_ms=int(utt.end_ms),
            first_word_ms=utt.first_word_ms,
            commit_ms=(now - utt.last_speech_wall) * 1000.0,
            words=list(utt.agreement.committed),
        )


class ParakeetDecoder:
    """Decoder over the Parakeet serving endpoint (see module docstring for the contract)."""

    def __init__(self, llm: LlmClient, endpoint: str, *, channel: Optional[str] = None,
                 url: Optional[str] = None, timeout_s: float = 2.0) -> None:
        self._llm = llm
        self._endpoint = endpoint
        self._channel = channel
        self._url = url or None
        self._timeout = timeout_s

    def request_payload(self, window: Window) -> dict:
        return {
            "dataframe_records": [{
                "audio_b64": base64.b64encode(window.pcm or b"").decode("ascii"),
                "offset_ms": int(window.start_ms),
                "channel": self._channel,
            }]
        }

    async def decode(self, window: Window) -> list[Word]:
        if not window.pcm:
            return []
        try:
            body = await self._llm.invoke(self._endpoint, self.request_payload(window),
                                          timeout=self._timeout, url=self._url)
        except LlmUnavailable as exc:
            # Fail-soft: a bad window yields no words this tick, the channel keeps running.
            log.warning("STT decode failed (%s); no words this window", exc)
            return []
        return _parse_words(_prediction(body), window)


def _prediction(body: dict) -> dict:
    """Accept {"predictions": [{...}]} and the MLflow-wrapped {"predictions": {"predictions": [...]}}."""
    preds = body.get("predictions", body)
    if isinstance(preds, dict) and "predictions" in preds:
        preds = preds["predictions"]
    if isinstance(preds, list):
        return preds[0] if preds and isinstance(preds[0], dict) else {}
    return preds if isinstance(preds, dict) else {}


def _parse_words(pred: dict, window: Window) -> list[Word]:
    raw = pred.get("words")
    out: list[Word] = []
    if raw:
        for item in raw:
            text = item.get("word") or item.get("w") or ""
            s = item.get("start_ms", item.get("s"))
            e = item.get("end_ms", item.get("e"))
            if not text or s is None or e is None:
                continue
            # Already absolute: the endpoint added offset_ms.
            out.append(Word(w=text, s=int(float(s)), e=int(float(e))))
        return out
    text = (pred.get("text") or "").split()
    if not text:
        return []
    span = max(window.end_ms - window.start_ms, 1)
    step = span / len(text)
    return [
        Word(w=t, s=int(window.start_ms + i * step), e=int(window.start_ms + (i + 1) * step))
        for i, t in enumerate(text)
    ]
