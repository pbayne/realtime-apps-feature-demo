"""Lakebase Change Data Feed history "sink" (backbone=direct, databricks mode).

There is no app-side write here. With Lakebase CDF the history path is fully managed by the
platform: every INSERT/UPDATE/DELETE on the operational Postgres tables the app already writes
(src/app/store/lakebase.py) is captured from the write-ahead log and mirrored into UC managed
Delta tables named `lb_<table>_history`, batched and flushed by Lakebase every ~15 s.

So the app no longer streams its own history rows (that was ZerobusHistorySink). This sink is a
no-op that satisfies the HistorySink protocol: `emit()` validates the table name and returns False
(nothing queued), and `stats()` reports the managed backend so /healthz and the admin area can show
that history is handled by CDF rather than an in-process queue. Enablement of the feed itself is a
one-time platform step (Lakebase UI / Postgres REST API), not something the app does at runtime;
see src/notebooks/init_history.py and docs/ARCHITECTURE.md.

Why keep a sink at all instead of deleting the emit() call sites? The call sites (session.py,
services.py, voice/session.py, actions/service.py) stay identical across all three history modes
(cdf in databricks-direct, memory in local, none in kafka). Routing the difference through the
sink keeps those hot-path call sites untouched and keeps the local/memory test path working.
"""

from __future__ import annotations

import logging
from typing import Any, Mapping

from .schema import TABLES

log = logging.getLogger("sidekick.history.cdf")


class CdfHistorySink:
    """No-op sink: history is mirrored Postgres -> Delta by Lakebase Change Data Feed, out of band."""

    backend = "cdf"

    def __init__(self) -> None:
        # queued/written stay 0 by design: the app never writes history rows under CDF.
        self.counters = {"queued": 0, "written": 0, "dropped": 0, "failed": 0, "skipped": 0}
        log.info("history backend=cdf: Lakebase Change Data Feed mirrors Postgres to Delta (no app-side writes)")

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    def emit(self, table: str, row: Mapping[str, Any]) -> bool:
        if table not in TABLES:
            raise KeyError(f"unknown history table {table!r}")
        # Nothing to do: the Postgres write the caller already made is what CDF captures.
        self.counters["skipped"] += 1
        return False

    def stats(self) -> dict[str, Any]:
        return {"backend": self.backend, "enabled": True,
                "disabled_reason": None, "managed": True, "queue_depth": 0, **self.counters}
