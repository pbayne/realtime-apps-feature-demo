"""In-process bus: one asyncio.Queue per subscriber, fan-out on publish."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from .base import Handler

log = logging.getLogger("sidekick.bus.memory")


class MemoryBus:
    def __init__(self) -> None:
        self._subs: dict[str, list[asyncio.Queue[tuple[str, dict[str, Any]]]]] = {}
        self._tasks: list[asyncio.Task[None]] = []
        self.published: dict[str, int] = {}

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        for t in self._tasks:
            try:
                await t
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        self._tasks.clear()

    async def publish(self, topic: str, key: str, value: dict[str, Any]) -> None:
        self.published[topic] = self.published.get(topic, 0) + 1
        for q in self._subs.get(topic, []):
            q.put_nowait((key, value))

    def subscribe(self, topic: str, handler: Handler) -> None:
        q: asyncio.Queue[tuple[str, dict[str, Any]]] = asyncio.Queue()
        self._subs.setdefault(topic, []).append(q)

        async def pump() -> None:
            while True:
                key, value = await q.get()
                try:
                    await handler(key, value)
                except Exception:  # noqa: BLE001 - one bad consumer must not kill the topic
                    log.exception("memory bus handler failed on topic %s", topic)

        self._tasks.append(asyncio.ensure_future(pump()))
