"""EventBus protocol. Values are JSON-serialisable dicts; key = call_id."""

from __future__ import annotations

from typing import Any, Awaitable, Callable, Protocol

Handler = Callable[[str, dict[str, Any]], Awaitable[None]]  # (key, value)


class EventBus(Protocol):
    async def start(self) -> None: ...

    async def stop(self) -> None: ...

    async def publish(self, topic: str, key: str, value: dict[str, Any]) -> None: ...

    def subscribe(self, topic: str, handler: Handler) -> None: ...
