"""Moonshine Phonon TTS, in-process (no serving endpoint). Matches the KokoroTts interface so voice
mode can use it interchangeably: `stream(text, voice) -> AsyncIterator[PcmChunk]` plus `aclose()`.

Moonshine runs on CPU in the app process. `TextToSpeech.synthesize` is blocking and returns
(float_samples in [-1, 1], sample_rate); we convert to PCM16 and chunk it to mimic the streaming
shape the WS layer already consumes. The model is loaded once (lazily) and reused; synthesis is run
in a thread so it never blocks the event loop.

Voice ids: Moonshine Phonon uses its own catalog (list_tts_voices), not Kokoro's `af_heart` names.
`voice_catalog()` exposes them to the UI; an unknown/empty voice falls back to the language default.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import AsyncIterator, Optional

from voice.tts import PcmChunk, TtsUnavailable  # reuse the same chunk type + error

log = logging.getLogger("sidekick.voice.moonshine_tts")

DEFAULT_RATE = 24000
CHUNK_MS = 120                       # emit ~120 ms PCM chunks, like the Kokoro adapter's cadence
_LANG_DEFAULT = "en-us"


@dataclass(frozen=True)
class _Loaded:
    tts: object
    rate: int


class MoonshineTts:
    """In-process Phonon TTS. One synthesizer, guarded by a lock (a synth speaks one reply at a time)."""

    def __init__(self, *, language: str = _LANG_DEFAULT, voice: str = "", chunk_ms: int = CHUNK_MS) -> None:
        self._language = language
        self._default_voice = voice
        self._chunk_ms = chunk_ms
        self._loaded: Optional[_Loaded] = None
        self._load_lock = asyncio.Lock()
        self._synth_lock = asyncio.Lock()

    @property
    def endpoint(self) -> str:
        return "moonshine:phonon"

    async def _ensure_loaded(self) -> _Loaded:
        if self._loaded is not None:
            return self._loaded
        async with self._load_lock:
            if self._loaded is not None:
                return self._loaded

            def _load() -> _Loaded:
                import moonshine_voice as mv
                tts = mv.TextToSpeech().language(self._language)
                if self._default_voice:
                    try:
                        tts = tts.voice(self._default_voice)
                    except Exception:  # noqa: BLE001 - unknown voice: fall back to language default
                        log.warning("unknown Moonshine voice %r; using language default", self._default_voice)
                tts.load()
                return _Loaded(tts=tts, rate=DEFAULT_RATE)

            self._loaded = await asyncio.to_thread(_load)
            log.info("Moonshine TTS loaded (language=%s voice=%s)", self._language, self._default_voice or "default")
            return self._loaded

    async def stream(self, text: str, voice: str) -> AsyncIterator[PcmChunk]:
        """Synthesize one sentence and yield PCM16 chunks. Raises TtsUnavailable on failure (never cold:
        there is no scale-to-zero endpoint to warm)."""
        text = (text or "").strip()
        if not text:
            return
        try:
            loaded = await self._ensure_loaded()
        except Exception as exc:  # noqa: BLE001
            raise TtsUnavailable(f"moonshine load: {exc}"[:300]) from exc

        def _synth() -> tuple[bytes, int]:
            # voice() returns a new configured handle; ignore per-call voice if it is the Kokoro default
            # name (callers pass af_heart etc.). Phonon voices are selected at load; a mismatched name
            # is simply ignored here so a Kokoro-era UI value never breaks synthesis.
            tts = loaded.tts
            samples, rate = tts.synthesize(text)
            return _floats_to_pcm16(samples), rate

        async with self._synth_lock:
            try:
                pcm, rate = await asyncio.to_thread(_synth)
            except Exception as exc:  # noqa: BLE001
                raise TtsUnavailable(f"moonshine synth: {exc}"[:300]) from exc

        step = max(2, int(rate * 2 * self._chunk_ms / 1000)) & ~1   # whole samples, even byte count
        for i in range(0, len(pcm), step):
            yield PcmChunk(rate, pcm[i:i + step])

    async def aclose(self) -> None:
        loaded, self._loaded = self._loaded, None
        if loaded is not None:
            close = getattr(loaded.tts, "close", None)
            if callable(close):
                try:
                    await asyncio.to_thread(close)
                except Exception:  # noqa: BLE001
                    pass


def _floats_to_pcm16(samples) -> bytes:
    """Moonshine returns float samples in [-1, 1] (a python list or numpy array) -> PCM16 LE bytes."""
    try:
        import numpy as np
        arr = np.asarray(samples, dtype=np.float32)
        arr = np.clip(arr, -1.0, 1.0)
        return (arr * 32767.0).astype("<i2").tobytes()
    except Exception:  # noqa: BLE001 - numpy should be present (moonshine dep), but stay safe
        import struct
        out = bytearray()
        for s in samples:
            v = int(max(-1.0, min(1.0, float(s))) * 32767.0)
            out += struct.pack("<h", v)
        return bytes(out)


def voice_catalog(language: str = "en") -> list[dict[str, str]]:
    """Phonon voices for the UI: {id, label}. Falls back to a single default entry if listing fails."""
    try:
        import moonshine_voice as mv
        voices = mv.list_tts_voices(language)
        ids: list[str] = []
        for bucket in ("downloaded", "available", "all"):
            got = getattr(voices, bucket, None)
            if got:
                ids = [getattr(v, "voice_id", getattr(v, "id", str(v))) for v in got]
                break
        return [{"id": v, "label": v.replace("_", " ").title()} for v in ids] or [{"id": "", "label": "Default"}]
    except Exception:  # noqa: BLE001
        return [{"id": "", "label": "Default"}]
