"""Streaming STT session (SPEC section 1) and the Parakeet client (SPEC section 3.3, contract v0.3).

StreamingSession runs one channel. Per utterance:
  - VAD gate decides speech vs silence per frame (Frame.speech overrides it for replay).
  - While speaking, every `frame_s` seconds the rolling window (last `window_s` s,
    clipped to the utterance start) is decoded by a Decoder.
  - LocalAgreement-2 with hold_words commits stable words; partial events carry
    committed + tail text.
  - `endpoint_silence_ms` of silence ends the utterance: one final decode over the
    whole utterance is flushed and a final event is emitted.

ParakeetDecoder and ParakeetTranscriber call the Parakeet serving endpoint with the field-eng
audio-serving server (src/notebooks/parakeet_serving/asr_server.py):

  request : multipart POST /serving-endpoints/{STT_ENDPOINT}/audio/transcriptions
            file = 16 kHz mono PCM16 WAV (<= 30 s), model = endpoint name, response_format = verbose_json
  response: {"duration", "text", "segments": [{"start","end","text"}], "words": [{"word","start","end"}]}

Times in the response are seconds relative to the clip. The caller converts them to absolute call
time: start_ms = offset_ms + round(start * 1000). The endpoint knows nothing about channels.
Auth is the app's service principal (LlmClient.multipart). Route-optimized URLs are not used.
"""

from __future__ import annotations

import base64
import io
import logging
import time
import wave
from collections import deque
from dataclasses import dataclass
from typing import AsyncIterator, Callable, Optional

from llm import WARMUP, LlmClient, LlmUnavailable

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


STT_RATE = 16000
STT_ROUTE = "audio/transcriptions"


def pcm_to_wav(pcm: bytes, rate: int = STT_RATE) -> bytes:
    """Wrap raw PCM16 mono in a WAV container (what the endpoint's multipart `file` expects)."""
    buf = io.BytesIO()
    with wave.open(buf, "wb") as f:
        f.setnchannels(1)
        f.setsampwidth(2)
        f.setframerate(rate)
        f.writeframes(pcm)
    return buf.getvalue()


def transcription_request(wav: bytes, endpoint: str) -> tuple[dict, dict]:
    """(files, data) for the multipart request. Kept separate so the contract is testable."""
    files = {"file": ("audio.wav", wav, "audio/wav")}
    data = {"model": endpoint, "response_format": "verbose_json"}
    return files, data


WARMING_AFTER = 3   # consecutive failed windows before the call is told transcription is warming up


class ParakeetDecoder:
    """Decoder over the Parakeet serving endpoint. Fail-soft: a failed window yields no words.

    Window decodes run 2-3 times a second, so they never open the LLM circuit breaker (a scale-to-zero
    endpoint fails every window while it starts; a tripped breaker then rejected every window even after it
    was warm). After WARMING_AFTER consecutive failures `on_status` gets a "warming" status event, and a
    "ready" one on the first success after that, so the UI can explain the wait."""

    def __init__(self, llm: LlmClient, endpoint: str, *, timeout_s: float = 2.0,
                 on_status: Optional[Callable[[dict], None]] = None) -> None:
        self._llm = llm
        self._endpoint = endpoint
        self._timeout = timeout_s
        self._on_status = on_status
        self._fails = 0
        self._warming = False
        self._last_log = 0.0

    def _status(self, state: str) -> None:
        if self._on_status is not None:
            self._on_status({"type": "status", "stage": "stt", "state": state,
                             "message": "Warming up transcription (GPU starting)…" if state == "warming" else ""})

    async def decode(self, window: Window) -> list[Word]:
        if not window.pcm:
            return []
        files, data = transcription_request(pcm_to_wav(window.pcm), self._endpoint)
        token = WARMUP.set(True)   # fail-soft per window: do not count towards the breaker
        try:
            body = await self._llm.multipart(self._endpoint, STT_ROUTE, files=files, data=data,
                                             timeout=self._timeout)
        except LlmUnavailable as exc:
            self._fails += 1
            if self._fails >= WARMING_AFTER and not self._warming:
                self._warming = True
                self._status("warming")
            now = time.monotonic()
            if now - self._last_log > 5.0:   # one line per 5 s, not one per window
                self._last_log = now
                log.warning("STT decode failed %d window(s) in a row (%s: %s); no words this window",
                            self._fails, type(exc.__cause__ or exc).__name__, str(exc)[:160])
            return []
        finally:
            WARMUP.reset(token)
        if self._warming:
            self._warming = False
            self._status("ready")
        self._fails = 0
        return words_from_response(body, window.start_ms, window.end_ms)


class ParakeetTranscriber:
    """Whole-turn transcription for voice mode (SPEC 9.2): WAV in, text out. Raises LlmUnavailable.
    protocol="whisper_json" is the local-dev stand-in for the Databricks-hosted whisper endpoint."""

    def __init__(self, llm: LlmClient, endpoint: str, *, timeout_s: float = 10.0, protocol: str = "parakeet") -> None:
        self._llm = llm
        self._endpoint = endpoint
        self._timeout = timeout_s
        self._protocol = protocol

    async def transcribe(self, wav: bytes) -> str:
        if self._protocol == "whisper_json":
            body = await self._llm.json_post(self._endpoint, {"inputs": [base64.b64encode(wav).decode("ascii")]},
                                             timeout=self._timeout)
            preds = body.get("predictions", body)
            if isinstance(preds, list) and preds:
                first = preds[0]
                return str(first.get("text") if isinstance(first, dict) else first).strip()
            return str(preds or "").strip()
        files, data = transcription_request(wav, self._endpoint)
        body = await self._llm.multipart(self._endpoint, STT_ROUTE, files=files, data=data, timeout=self._timeout)
        return str(body.get("text") or "").strip()


def words_from_response(body: dict, offset_ms: float, window_end_ms: float) -> list[Word]:
    """Parakeet verbose_json -> absolute Words. Word times are clip-relative seconds; offset_ms is the
    window start on the call timeline. Without word timestamps, segment or full text is spread evenly."""
    words = body.get("words") or []
    out: list[Word] = []
    for item in words:
        text = (item.get("word") or "").strip()
        if not text or item.get("start") is None or item.get("end") is None:
            continue
        out.append(Word(w=text, s=int(offset_ms + round(float(item["start"]) * 1000)),
                        e=int(offset_ms + round(float(item["end"]) * 1000))))
    if out:
        return out
    for seg in body.get("segments") or []:
        seg_text = (seg.get("text") or "").strip()
        if not seg_text or seg.get("start") is None or seg.get("end") is None:
            continue
        s0 = offset_ms + float(seg["start"]) * 1000
        s1 = offset_ms + float(seg["end"]) * 1000
        out.extend(_spread(seg_text, s0, s1))
    if out:
        return out
    return _spread(body.get("text") or "", offset_ms, window_end_ms)


def _spread(text: str, start_ms: float, end_ms: float) -> list[Word]:
    tokens = text.split()
    if not tokens:
        return []
    span = max(end_ms - start_ms, 1.0)
    step = span / len(tokens)
    return [Word(w=t, s=int(start_ms + i * step), e=int(start_ms + (i + 1) * step)) for i, t in enumerate(tokens)]
