"""No bus. The direct backbone (BACKBONE=direct) has no Kafka and no in-memory topics: publishes are dropped."""

from __future__ import annotations

from typing import Any

from .base import Handler


class NullBus:
    def __init__(self) -> None:
        self.published: dict[str, int] = {}

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def publish(self, topic: str, key: str, value: dict[str, Any]) -> None:
        return None

    def subscribe(self, topic: str, handler: Handler) -> None:
        raise RuntimeError("the direct backbone has no bus topics; subscribe is only for BACKBONE=kafka")
