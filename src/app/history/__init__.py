"""History sinks (SPEC section 3.7): Zerobus Ingest to UC Delta for backbone=direct, none for kafka."""

from __future__ import annotations

from config import Settings

from .base import HistorySink
from .memory import MemoryHistorySink, NullHistorySink
from .schema import TABLES, to_record


def build_history(settings: Settings) -> HistorySink:
    backend = settings.effective_history
    if backend == "zerobus":
        from .zerobus import ZerobusHistorySink

        return ZerobusHistorySink(settings)
    if backend == "memory":
        return MemoryHistorySink()
    return NullHistorySink()


__all__ = ["HistorySink", "MemoryHistorySink", "NullHistorySink", "TABLES", "build_history", "to_record"]
