"""History sinks that do not touch the network: memory (local and tests) and none (kafka backbone)."""

from __future__ import annotations

import logging
from collections import deque
from typing import Any, Mapping

from .schema import TABLES, to_record

log = logging.getLogger("sidekick.history.memory")


class MemoryHistorySink:
    """Keeps the last `max_rows` records per table in process memory. Use in local mode and tests."""

    backend = "memory"

    def __init__(self, max_rows: int = 10000) -> None:
        self.rows: dict[str, deque[dict[str, Any]]] = {t: deque(maxlen=max_rows) for t in TABLES}
        self.max_rows = max_rows
        self.counters = {"queued": 0, "written": 0, "dropped": 0, "failed": 0, "skipped": 0}

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    def emit(self, table: str, row: Mapping[str, Any]) -> bool:
        record = to_record(table, row)
        bucket = self.rows[table]
        if len(bucket) == bucket.maxlen:
            self.counters["dropped"] += 1
        bucket.append(record)
        self.counters["queued"] += 1
        self.counters["written"] += 1
        return True

    def stats(self) -> dict[str, Any]:
        return {"backend": self.backend, "enabled": True, "disabled_reason": None, "queue_depth": 0,
                **self.counters}


class NullHistorySink:
    """No history. Kafka backbone: history is carried by the bus topics and delta_history.py."""

    backend = "none"

    def __init__(self) -> None:
        self.counters = {"queued": 0, "written": 0, "dropped": 0, "failed": 0, "skipped": 0}

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    def emit(self, table: str, row: Mapping[str, Any]) -> bool:
        if table not in TABLES:
            raise KeyError(f"unknown history table {table!r}")
        return False

    def stats(self) -> dict[str, Any]:
        return {"backend": self.backend, "enabled": False, "disabled_reason": "backbone=kafka uses Kafka history",
                "queue_depth": 0, **self.counters}
