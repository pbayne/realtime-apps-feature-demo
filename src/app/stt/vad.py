"""Voice-activity gate. Silero when torch + silero-vad import; energy-threshold otherwise.

Silero is optional (it pulls in torch, which is not in requirements.txt). Any import
or model-load failure is logged once and the energy fallback is used, so the app
never fails to start because of VAD.
"""

from __future__ import annotations

import array
import logging
import math
from typing import Protocol

log = logging.getLogger("copilot.stt.vad")


class Vad(Protocol):
    name: str

    def is_speech(self, pcm: bytes) -> bool: ...


class EnergyVad:
    name = "energy"

    def __init__(self, rms_threshold: float = 400.0) -> None:
        self.rms_threshold = rms_threshold

    def is_speech(self, pcm: bytes) -> bool:
        if not pcm:
            return False
        samples = array.array("h")
        samples.frombytes(pcm[: len(pcm) - (len(pcm) % 2)])
        if not samples:
            return False
        rms = math.sqrt(sum(s * s for s in samples) / len(samples))
        return rms >= self.rms_threshold


class SileroVad:
    """Silero VAD (512-sample chunks @16 kHz). Speech if any chunk prob >= 0.5."""

    name = "silero"

    def __init__(self) -> None:
        import torch  # noqa: F401  (import check)
        from silero_vad import load_silero_vad

        self._model = load_silero_vad()

    def is_speech(self, pcm: bytes) -> bool:
        import torch

        samples = array.array("h")
        samples.frombytes(pcm[: len(pcm) - (len(pcm) % 2)])
        if len(samples) < 512:
            return False
        x = torch.tensor([s / 32768.0 for s in samples], dtype=torch.float32)
        self._model.reset_states()
        for i in range(0, len(x) - 511, 512):
            if float(self._model(x[i : i + 512], 16000)) >= 0.5:
                return True
        return False


def build_vad(backend: str, rms_threshold: float) -> Vad:
    if backend in ("auto", "silero"):
        try:
            return SileroVad()
        except Exception as exc:  # noqa: BLE001 - optional dependency, fail-soft
            if backend == "silero":
                log.warning("silero requested but unavailable (%s); using energy VAD", exc)
            else:
                log.info("silero VAD unavailable (%s); using energy VAD", exc)
    return EnergyVad(rms_threshold)
