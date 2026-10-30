"""Per-socket message rate limit (token bucket, SPEC 10). Over-limit messages are dropped and counted, never answered,
so a flood costs the server one comparison per message."""

from __future__ import annotations

import time
from typing import Callable


class TokenBucket:
    def __init__(self, rate: float, burst: float, clock: Callable[[], float] = time.monotonic) -> None:
        self.rate = rate
        self.capacity = burst
        self.dropped = 0
        self._tokens = float(burst)
        self._clock = clock
        self._last = clock()

    def allow(self, cost: float = 1.0) -> bool:
        now = self._clock()
        self._tokens = min(self.capacity, self._tokens + (now - self._last) * self.rate)
        self._last = now
        if self._tokens >= cost:
            self._tokens -= cost
            return True
        self.dropped += 1
        return False
