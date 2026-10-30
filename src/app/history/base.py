"""HistorySink protocol (SPEC section 3.7). Hot-path contract: `emit` is synchronous, never blocks,
never raises for transport problems, and returns False when the record was not accepted."""

from __future__ import annotations

from typing import Any, Mapping, Protocol


class HistorySink(Protocol):
    backend: str

    async def start(self) -> None: ...

    async def stop(self) -> None: ...

    def emit(self, table: str, row: Mapping[str, Any]) -> bool: ...

    def stats(self) -> dict[str, Any]: ...
