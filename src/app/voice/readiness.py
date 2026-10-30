"""Voice readiness for /healthz: the state of the STT and TTS serving endpoints, from the SDK (SPEC 9.6).

Probes are cached for `ttl_s` (default 30 s), so a health check does not hit the control plane each time.
In local mode no probe is made and both endpoints report `local`. A probe that errors or times out reports
`unknown`; health never fails because of it.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Awaitable, Callable, Optional

from models import now_ms

log = logging.getLogger("sidekick.voice.readiness")

Fetch = Callable[[str], Awaitable[str]]   # endpoint name -> "ready" | "not_ready"


class EndpointReadiness:
    def __init__(self, endpoints: dict[str, str], fetch: Optional[Fetch], *, ttl_s: float = 30.0,
                 timeout_s: float = 3.0, clock: Callable[[], float] = time.monotonic) -> None:
        self._endpoints = dict(endpoints)           # role ("stt", "tts") -> endpoint name
        self._fetch = fetch
        self._ttl = ttl_s
        self._timeout = timeout_s
        self._clock = clock
        self._cache: Optional[dict[str, Any]] = None
        self._at = 0.0
        self._lock = asyncio.Lock()
        self.probes = 0

    async def snapshot(self) -> dict[str, Any]:
        async with self._lock:
            if self._cache is not None and self._clock() - self._at < self._ttl:
                return {**self._cache, "cached": True}
            roles: dict[str, Any] = {}
            for role, name in self._endpoints.items():
                roles[role] = {"endpoint": name, "state": await self._probe(name)}
            ok = all(r["state"] in ("ready", "local") for r in roles.values())
            self._cache = {"endpoints": roles, "ready": ok, "checked_at_ms": now_ms()}
            self._at = self._clock()
            return {**self._cache, "cached": False}

    async def _probe(self, name: str) -> str:
        if self._fetch is None:
            return "local"
        self.probes += 1
        try:
            return await asyncio.wait_for(self._fetch(name), self._timeout)
        except Exception as exc:  # noqa: BLE001 - a failed probe is "unknown", never a health failure
            log.info("endpoint readiness probe for %s failed: %s", name, type(exc).__name__)
            return "unknown"
