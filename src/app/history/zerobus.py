"""Zerobus Ingest history sink: utterances, enrichments, cards, feedback, calls, voice_turns, voice_items
-> UC managed Delta tables in ${HISTORY_CATALOG}.${HISTORY_SCHEMA}.

SDK facts (verified 2026-10-30 against the Zerobus Ingest docs and the installed SDK 1.10.0):
- Package `databricks-zerobus-ingest-sdk` (pip). Sync import `from zerobus.sdk.sync import ZerobusSdk`;
  `from zerobus.sdk.shared import TableProperties, StreamConfigurationOptions, RecordType`.
  Async variant: `zerobus.sdk.aio`. Python >= 3.9.
- Endpoint `https://<workspace-id>.zerobus.<region>.cloud.databricks.com`; the second ZerobusSdk argument
  is the workspace URL (`unity_catalog_url`).
- Auth: service principal client id/secret (OAuth). The SP needs the Workspace access entitlement,
  USE CATALOG, USE SCHEMA, and MODIFY + SELECT on each table (see init_history.py).
- Record type JSON: pass a JSON string per record. `ingest_record_offset` is the non-deprecated call;
  `wait_for_offset` / `flush` confirm durability (acks mean durable, not yet queryable).
- Transient errors are retried inside the SDK (recovery=True); unrecoverable ones raise ZerobusException.
Docs: https://docs.databricks.com/aws/en/ingestion/zerobus-ingest
      https://docs.databricks.com/aws/en/ingestion/zerobus-message-blocking
      https://github.com/databricks/zerobus-sdk/blob/main/python/README.md

Hot-path contract: `emit()` is synchronous and only appends to a bounded deque (drop-oldest, counted,
logged). A background task drains it in batches; SDK calls run in worker threads (`asyncio.to_thread`),
so the event loop never waits on the network. Each blocking write has a time limit (`history_write_timeout_s`):
`wait_for_offset` can hang, and a timed-out write resets its stream. Streams are one per target table, created
lazily and reset on error. Fail-soft: missing config disables the sink (logged once); write failures are counted,
logged, and the batch is retried on a fresh stream with exponential backoff (`history_retry_attempts`).
Delivery is at-least-once for acknowledged writes: a retry after a partial ack can duplicate rows, so every table
has a natural key for downstream dedupe (calls.call_id, utterances/enrichments (call_id, seq), cards.card_id,
feedback (card_id, at_ms), voice_turns (session_id, turn), voice_items.item_id). A batch that still fails after
the last attempt is requeued once, at the front of the queue (bounded: a record is requeued at most once). If it
fails again it is counted as failed and logged. Records can still be lost (queue overflow, or a second failure),
so history is best effort, not a guarantee. The call never waits on history.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from collections import deque
from typing import Any, Callable, Mapping, Optional

from config import Settings

from .schema import TABLES, to_record

log = logging.getLogger("sidekick.history.zerobus")

_WARN_EVERY = 100          # drop warnings: log every Nth drop
_WARN_INTERVAL_S = 10.0    # failure warnings: at most one per interval


def target_table(settings: Settings, table: str) -> str:
    return f"{settings.history_catalog}.{settings.history_schema}.{table}"


def normalise_endpoint(endpoint: str) -> str:
    endpoint = endpoint.strip().rstrip("/")
    return endpoint if endpoint.startswith("https://") else f"https://{endpoint}"


def config_problem(settings: Settings) -> Optional[str]:
    """Why the sink cannot run, or None. Checked once at construction."""
    if not settings.zerobus_endpoint:
        return "ZEROBUS_ENDPOINT not set"
    if not settings.zerobus_client_id or not settings.zerobus_client_secret:
        return "ZEROBUS_CLIENT_ID / ZEROBUS_CLIENT_SECRET not set"
    if not (settings.databricks_host or os.environ.get("DATABRICKS_HOST")):
        return "DATABRICKS_HOST not set (needed as the Zerobus unity_catalog_url)"
    return None


class ZerobusSdkAdapter:
    """The only place the real SDK is imported. `open(table)` returns a stream with
    ingest_record_offset(str), wait_for_offset(int), flush(), close()."""

    def __init__(self, settings: Settings) -> None:
        from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties
        from zerobus.sdk.sync import ZerobusSdk

        host = (settings.databricks_host or os.environ.get("DATABRICKS_HOST", "")).rstrip("/")
        self._sdk = ZerobusSdk(normalise_endpoint(settings.zerobus_endpoint or ""), normalise_endpoint(host))
        self._client_id = settings.zerobus_client_id
        self._client_secret = settings.zerobus_client_secret
        self._table_properties = TableProperties
        self._options = StreamConfigurationOptions
        self._record_type = RecordType.JSON

    def open(self, table_full_name: str) -> Any:
        return self._sdk.create_stream(
            client_id=self._client_id,
            client_secret=self._client_secret,
            table_properties=self._table_properties(table_full_name),
            options=self._options(record_type=self._record_type),
        )


class ZerobusHistorySink:
    backend = "zerobus"

    def __init__(self, settings: Settings, *, sdk_factory: Optional[Callable[[Settings], Any]] = None) -> None:
        self._s = settings
        self._max = settings.history_queue_max
        self._batch_max = settings.history_batch_max
        self._queue: deque[tuple[str, dict[str, Any], bool]] = deque()   # (table, record, already requeued)
        self._sdk_factory = sdk_factory
        self._sdk: Any = None
        self._streams: dict[str, Any] = {}
        self._task: Optional[asyncio.Task[None]] = None
        self._wake: Optional[asyncio.Event] = None
        self.counters = {"queued": 0, "written": 0, "dropped": 0, "failed": 0, "skipped": 0, "retried": 0,
                         "requeued": 0, "timeouts": 0}
        self._last_warn = float("-inf")
        self.disabled_reason = config_problem(settings)
        if self.disabled_reason is None and sdk_factory is None:
            try:
                import zerobus.sdk.sync  # noqa: F401
            except ImportError:
                self.disabled_reason = "databricks-zerobus-ingest-sdk not installed"
        if self.disabled_reason:
            log.warning("history sink disabled: %s. Records are skipped; the call keeps running.", self.disabled_reason)

    @property
    def enabled(self) -> bool:
        return self.disabled_reason is None

    # ----------------------------------------------------------- lifecycle
    async def start(self) -> None:
        if not self.enabled:
            return
        self._wake = asyncio.Event()
        self._task = asyncio.ensure_future(self._run())
        log.info("zerobus history sink ready: %s.%s.* (queue %d, batch %d)", self._s.history_catalog,
                 self._s.history_schema, self._max, self._batch_max)

    async def stop(self) -> None:
        if self._task is None:
            return
        try:
            await asyncio.wait_for(self._drain_all(), timeout=self._s.history_flush_timeout_s)
        except asyncio.TimeoutError:
            log.warning("history drain timed out with %d records queued", len(self._queue))
        self._task.cancel()
        await asyncio.gather(self._task, return_exceptions=True)
        self._task = None
        for table in list(self._streams):
            await asyncio.to_thread(self._close_stream, table)

    # ------------------------------------------------------------- hot path
    def emit(self, table: str, row: Mapping[str, Any]) -> bool:
        if table not in TABLES:
            raise KeyError(f"unknown history table {table!r}")
        if not self.enabled or self._task is None:
            self.counters["skipped"] += 1
            return False
        if len(self._queue) >= self._max:
            self._queue.popleft()
            self.counters["dropped"] += 1
            if self.counters["dropped"] % _WARN_EVERY == 1:
                log.warning("history queue full (%d): dropped oldest (total dropped %d)", self._max,
                            self.counters["dropped"])
        self._queue.append((table, to_record(table, row), False))
        self.counters["queued"] += 1
        if len(self._queue) >= self._batch_max and self._wake is not None:
            self._wake.set()
        return True

    def stats(self) -> dict[str, Any]:
        return {"backend": self.backend, "enabled": self.enabled, "disabled_reason": self.disabled_reason,
                "queue_depth": len(self._queue), "queue_max": self._max, **self.counters}

    # ------------------------------------------------------- background work
    async def _run(self) -> None:
        assert self._wake is not None
        while True:
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=self._s.history_flush_interval_s)
            except asyncio.TimeoutError:
                pass
            self._wake.clear()
            await self._drain_all()

    async def _drain_all(self) -> None:
        while self._queue:
            await self._flush_batch()

    async def _flush_batch(self) -> None:
        batch: list[tuple[str, dict[str, Any], bool]] = []
        while self._queue and len(batch) < self._batch_max:
            batch.append(self._queue.popleft())
        by_table: dict[str, list[tuple[dict[str, Any], bool]]] = {}
        for table, record, requeued in batch:
            by_table.setdefault(table, []).append((record, requeued))
        attempts = max(1, self._s.history_retry_attempts)
        for table, entries in by_table.items():
            records = [record for record, _ in entries]
            for attempt in range(attempts):
                try:
                    await asyncio.wait_for(asyncio.to_thread(self._write, table, records),
                                           timeout=self._s.history_write_timeout_s)   # _write resets the stream on error
                    self.counters["written"] += len(records)
                    break
                except Exception as exc:  # noqa: BLE001 - fail-soft: retry, then requeue once or count and log
                    if isinstance(exc, asyncio.TimeoutError):
                        # The worker thread may still hold the stream: drop it so the next attempt starts clean.
                        self.counters["timeouts"] += 1
                        self._reset_stream(table)
                    if attempt < attempts - 1:
                        self.counters["retried"] += len(records)
                        await asyncio.sleep(self._s.history_retry_base_s * (2 ** attempt))
                        continue
                    self._give_up(table, entries, exc, attempts)

    def _give_up(self, table: str, entries: list[tuple[dict[str, Any], bool]], exc: BaseException, attempts: int) -> None:
        """A batch failed every attempt. Records not yet requeued go back to the FRONT of the queue once (bounded),
        and only if there is room; the rest are counted as failed."""
        room = max(0, self._max - len(self._queue))
        candidates = [record for record, requeued in entries if not requeued]
        requeue = candidates[:room]
        for record in reversed(requeue):
            self._queue.appendleft((table, record, True))
        self.counters["requeued"] += len(requeue)
        failed = len(entries) - len(requeue)
        self.counters["failed"] += failed
        if failed:
            now = time.monotonic()
            if now - self._last_warn >= _WARN_INTERVAL_S:
                self._last_warn = now
                log.warning("zerobus write to %s failed after %d attempts (%d records dropped, %d requeued, total "
                            "failed %d): %s", table, attempts, failed, len(requeue), self.counters["failed"],
                            str(exc)[:300])

    # -------------------------------------------------- blocking (thread) side
    def _write(self, table: str, records: list[dict[str, Any]]) -> None:
        stream = self._stream_for(table)
        try:
            last = None
            for record in records:
                last = stream.ingest_record_offset(json.dumps(record, default=str))
            if last is not None:
                stream.wait_for_offset(last)
            stream.flush()
        except Exception:
            self._reset_stream(table)
            raise

    def _stream_for(self, table: str) -> Any:
        stream = self._streams.get(table)
        if stream is not None:
            return stream
        if self._sdk is None:
            factory = self._sdk_factory or (lambda s: ZerobusSdkAdapter(s))
            self._sdk = factory(self._s)
        full = target_table(self._s, table)
        stream = self._sdk.open(full)
        self._streams[table] = stream
        return stream

    def _reset_stream(self, table: str) -> None:
        self._close_stream(table)

    def _close_stream(self, table: str) -> None:
        stream = self._streams.pop(table, None)
        if stream is None:
            return
        try:
            stream.close()
        except Exception:  # noqa: BLE001 - closing a broken stream must not raise
            log.debug("closing zerobus stream for %s failed", table, exc_info=True)
