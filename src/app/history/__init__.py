"""History sinks (SPEC section 3.7). For backbone=direct in databricks mode, history reaches UC
Delta through Lakebase Change Data Feed (CDF) — a managed Postgres->Delta mirror, so the app-side
sink is a no-op (history/cdf.py). Local mode keeps everything in memory; kafka mode carries history
over the bus to delta_history.py, so the sink is none. The legacy Zerobus Ingest sink is retained
for back-compat (HISTORY_BACKEND=zerobus) but is no longer the default path."""

from __future__ import annotations

from config import Settings

from .base import HistorySink
from .cdf import CdfHistorySink
from .memory import MemoryHistorySink, NullHistorySink
from .schema import TABLES, to_record


def build_history(settings: Settings) -> HistorySink:
    backend = settings.effective_history
    if backend == "cdf":
        return CdfHistorySink()
    if backend == "zerobus":
        from .zerobus import ZerobusHistorySink

        return ZerobusHistorySink(settings)
    if backend == "memory":
        return MemoryHistorySink()
    return NullHistorySink()


__all__ = ["HistorySink", "CdfHistorySink", "MemoryHistorySink", "NullHistorySink",
           "TABLES", "build_history", "to_record"]
