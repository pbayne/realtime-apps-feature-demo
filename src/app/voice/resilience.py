"""Cold-start handling for the voice endpoints (SPEC 9.6).

STT (Parakeet) and TTS (Kokoro) run on GPU Model Serving endpoints that may scale to zero. The first call after
idle times out or is refused while the endpoint starts. Instead of failing the turn, the call is retried with
exponential backoff for up to `voice_warmup_budget_s`, and the client is told with `status` events:

  {"type":"status","stage":"stt|tts","state":"warming","message":"Warming up voice (GPU starting)…","turn":n}
  {"type":"status","stage":"stt|tts","state":"ready","elapsed_ms":..,"turn":n}
  {"type":"status","stage":"stt|tts","state":"failed","turn":n}     (budget spent; the turn gets an error)

Only cold failures are retried (LlmUnavailable.cold / TtsUnavailable.cold). Anything else fails the call at once.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Optional, TypeVar

WARMING_MESSAGE = "Warming up voice (GPU starting)…"

T = TypeVar("T")
Emit = Callable[[dict[str, Any]], None]


@dataclass(frozen=True)
class RetryPolicy:
    budget_s: float = 90.0
    base_s: float = 1.0
    cap_s: float = 8.0


class WarmState:
    """Tracks one stage (stt or tts) for one turn and emits warming / ready / failed status events."""

    def __init__(self, stage: str, emit: Emit, *, clock: Callable[[], float] = time.monotonic) -> None:
        self.stage = stage
        self._emit = emit
        self._clock = clock
        self.warming = False
        self._since: Optional[float] = None

    def failed_cold(self, attempt: int) -> None:
        if self.warming:
            return
        self.warming = True
        self._since = self._clock()
        self._emit({"type": "status", "stage": self.stage, "state": "warming", "message": WARMING_MESSAGE,
                    "attempt": attempt})

    def ok(self) -> None:
        if not self.warming:
            return
        self.warming = False
        since = self._since if self._since is not None else self._clock()
        self._emit({"type": "status", "stage": self.stage, "state": "ready",
                    "elapsed_ms": round((self._clock() - since) * 1000)})

    def give_up(self) -> None:
        if self.warming:
            self.warming = False
            self._emit({"type": "status", "stage": self.stage, "state": "failed"})


async def call_with_warmup(op: Callable[[], Awaitable[T]], warm: WarmState, policy: RetryPolicy, *,
                            clock: Callable[[], float] = time.monotonic,
                            sleep: Callable[[float], Awaitable[None]] = asyncio.sleep) -> T:
    """Run `op`. A cold failure waits (base * 2^n, capped) and tries again until the budget is spent.
    Any other failure, or a cold one after the budget, is raised to the caller."""
    started = clock()
    attempt = 0
    while True:
        try:
            result = await op()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - classified below
            cold = bool(getattr(exc, "cold", False))
            spent = clock() - started
            if not cold or spent >= policy.budget_s:
                if cold:
                    warm.give_up()
                raise
            attempt += 1
            warm.failed_cold(attempt)
            delay = min(policy.cap_s, policy.base_s * (2 ** (attempt - 1)))
            await sleep(max(0.0, min(delay, policy.budget_s - spent)))
            continue
        warm.ok()
        return result
