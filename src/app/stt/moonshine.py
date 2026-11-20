"""Moonshine STT, in-process (no serving endpoint). Two adapters matching the existing interfaces:

- MoonshineDecoder  -> Decoder protocol: `async decode(window) -> list[Word]`. Drop-in for
  ParakeetDecoder (call console) and VoiceDecoder (voice streams). Each window is transcribed
  independently (stateless), mirroring how the window decoder already treats windows.
- MoonshineTranscriber -> `async transcribe(wav) -> str`. Drop-in for ParakeetTranscriber
  (whole-turn voice-mode transcription).

The model loads once per process (lazily, in a thread) and is shared by both adapters via a module
singleton, so the ~150 MB weights are paid for once. Transcription runs in a thread executor so the
blocking ONNX call never stalls the event loop.

Model choice (measured): TINY_STREAMING decodes a window in ~130 ms on an M-series CPU — within the
<350 ms live-partial budget. SMALL_STREAMING (~266 ms) is more accurate and fine for whole-turn
transcription. Both are far below MEDIUM (~2 s), which is too slow for the rolling-window decoder.
`set_keyterms`/`set_context` (streaming archs only) bias toward jargon such as email addresses.
"""

from __future__ import annotations

import asyncio
import io
import logging
import os
import wave
from typing import Sequence

from stt.base import Window, Word
from stt.parakeet import words_from_response

log = logging.getLogger("sidekick.stt.moonshine")

STT_RATE = 16000
# model_arch names in moonshine_voice.ModelArch; resolved lazily so import never needs the package.
# Accuracy (LibriSpeech test-clean WER, shipped quantized): Tiny 4.83%, Small 2.61%, Medium 2.17%.
# Live partials need <~350ms/window (Tiny ~130ms, Small ~266ms); whole-turn tolerates more, so it uses the
# most accurate arch that still returns in ~1-2s. Overridable via STT_ARCH_FAST / STT_ARCH_ACCURATE env.
ARCH_FAST = os.environ.get("STT_ARCH_FAST", "SMALL_STREAMING")       # live-partial window decoder
ARCH_ACCURATE = os.environ.get("STT_ARCH_ACCURATE", "MEDIUM_STREAMING")  # whole-turn transcription


class _Model:
    """One loaded Moonshine Transcriber for an arch, with a lock (the C handle is not reentrant)."""

    def __init__(self, arch_name: str, keyterms: Sequence[str] = ()) -> None:
        self._arch_name = arch_name
        self._keyterms = tuple(keyterms)
        self._tx: object = None
        self._load_lock = asyncio.Lock()
        self._call_lock = asyncio.Lock()

    async def ensure(self) -> object:
        if self._tx is not None:
            return self._tx
        async with self._load_lock:
            if self._tx is not None:
                return self._tx

            def _load() -> object:
                import moonshine_voice as mv
                arch = getattr(mv.ModelArch, self._arch_name)
                path, resolved = mv.get_model_for_language("en", arch)
                tx = mv.Transcriber(path, model_arch=resolved)
                if self._keyterms:
                    try:
                        tx.set_keyterms(list(self._keyterms))
                    except Exception:  # noqa: BLE001 - keyterms only on streaming archs
                        pass
                return tx

            self._tx = await asyncio.to_thread(_load)
            log.info("Moonshine STT model loaded (arch=%s)", self._arch_name)
            return self._tx

    async def transcribe_pcm(self, pcm: bytes) -> dict:
        """Transcribe raw PCM16 mono 16 kHz -> a verbose_json-like dict (text + word/segment times in
        seconds) so the existing words_from_response can map it to absolute Words."""
        tx = await self.ensure()
        samples = _pcm16_to_floats(pcm)
        if not samples:
            return {"text": "", "words": [], "segments": []}

        def _run() -> dict:
            t = tx.transcribe_without_streaming(samples, sample_rate=STT_RATE)
            return _transcript_to_body(t)

        async with self._call_lock:
            return await asyncio.to_thread(_run)

    async def set_context(self, passage: str) -> None:
        tx = await self.ensure()

        def _ctx() -> None:
            try:
                tx.set_context(passage)
            except Exception:  # noqa: BLE001
                pass

        await asyncio.to_thread(_ctx)

    async def aclose(self) -> None:
        tx, self._tx = self._tx, None
        if tx is not None:
            close = getattr(tx, "close", None)
            if callable(close):
                try:
                    await asyncio.to_thread(close)
                except Exception:  # noqa: BLE001
                    pass


# Module singletons so the weights load once per process and are shared across sessions.
_MODELS: dict[str, _Model] = {}


def _model(arch_name: str, keyterms: Sequence[str] = ()) -> _Model:
    m = _MODELS.get(arch_name)
    if m is None:
        m = _Model(arch_name, keyterms)
        _MODELS[arch_name] = m
    return m


async def preload(arch_names: Sequence[str] = (ARCH_FAST, ARCH_ACCURATE)) -> None:
    """Load models at app boot so the first turn does not pay the download/compile cost."""
    for name in arch_names:
        try:
            await _model(name).ensure()
        except Exception as exc:  # noqa: BLE001 - a boot preload failure must not crash the app
            log.warning("Moonshine preload of %s failed: %s", name, exc)


class MoonshineDecoder:
    """Window decoder (Decoder protocol). Stateless per window; fail-soft (a failed decode yields no
    words, like ParakeetDecoder). Uses the fast arch for live partials."""

    def __init__(self, *, arch_name: str = ARCH_FAST, keyterms: Sequence[str] = ()) -> None:
        self._model = _model(arch_name, keyterms)

    async def decode(self, window: Window) -> list[Word]:
        if not window.pcm:
            return []
        try:
            body = await self._model.transcribe_pcm(window.pcm)
        except Exception as exc:  # noqa: BLE001 - never break the stream
            log.debug("moonshine window decode skipped: %s", type(exc).__name__)
            return []
        return words_from_response(body, window.start_ms, window.end_ms)


class MoonshineTranscriber:
    """Whole-turn transcription (voice mode). Uses the accurate arch. Returns plain text."""

    def __init__(self, *, arch_name: str = ARCH_ACCURATE, keyterms: Sequence[str] = ()) -> None:
        self._model = _model(arch_name, keyterms)

    async def transcribe(self, wav: bytes) -> str:
        pcm = _wav_to_pcm16(wav)
        body = await self._model.transcribe_pcm(pcm)
        return str(body.get("text") or "").strip()


# ------------------------------------------------------------------ helpers

def _pcm16_to_floats(pcm: bytes) -> list[float]:
    try:
        import numpy as np
        arr = np.frombuffer(pcm, dtype="<i2").astype("float32") / 32768.0
        return arr.tolist()
    except Exception:  # noqa: BLE001
        import struct
        n = len(pcm) // 2
        return [v / 32768.0 for v in struct.unpack("<%dh" % n, pcm[: n * 2])]


def _wav_to_pcm16(wav: bytes) -> bytes:
    """Extract raw PCM16 mono 16 kHz frames from a WAV container (what the voice turn hands us)."""
    with wave.open(io.BytesIO(wav), "rb") as f:
        return f.readframes(f.getnframes())


def _transcript_to_body(t: object) -> dict:
    """Map a Moonshine Transcript to the verbose_json-like dict words_from_response expects.

    Prefer per-word timings when present; else per-line (segment) timings; else bare text (the caller
    spreads it evenly). Moonshine line/word times are in seconds. Trailing low-confidence fragments on
    silence are dropped by keeping only lines with text."""
    # A Transcript exposes `.lines` (list[TranscriptLine]); each line has `text`, `start_time` and
    # `duration` (seconds), an optional `words` (often None), and a big `audio_data` we must NOT touch.
    words: list[dict] = []
    segments: list[dict] = []
    texts: list[str] = []
    for ln in (getattr(t, "lines", None) or []):
        ln_text = (getattr(ln, "text", "") or "").strip()
        if not ln_text:
            continue
        texts.append(ln_text)
        start = getattr(ln, "start_time", None)
        dur = getattr(ln, "duration", None)
        if start is not None and dur is not None:
            segments.append({"text": ln_text, "start": float(start), "end": float(start) + float(dur)})
        for w in (getattr(ln, "words", None) or []):
            w_text = (getattr(w, "text", "") or getattr(w, "word", "") or "").strip()
            ws, we = getattr(w, "start", None), getattr(w, "end", None)
            if w_text and ws is not None and we is not None:
                words.append({"word": w_text, "start": float(ws), "end": float(we)})
    text = _strip_timestamps(" ".join(texts))
    return {"text": text, "words": words, "segments": segments}


def _strip_timestamps(text: str) -> str:
    """Remove leading '[0.00s]' markers Moonshine's str(Transcript) can include."""
    import re
    return re.sub(r"\[\d+\.\d+s\]\s*", "", text).strip()
