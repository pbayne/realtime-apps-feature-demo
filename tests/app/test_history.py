"""History sinks (SPEC 3.7): Zerobus queue/backpressure/fail-soft with a fake SDK, the table contract shared with
init_history.py, row builders, and config validation for the two backbones. No network."""

import ast
import asyncio
import functools
import json
import re
from pathlib import Path

import pytest
from config import Settings
from history import schema as hs
from history.memory import MemoryHistorySink, NullHistorySink
from history.zerobus import ZerobusHistorySink, normalise_endpoint, target_table
from models import CallRecord, Card, Utterance

ROOT = Path(__file__).resolve().parents[2]
CATALOG, SCHEMA = "main", "call_sidekick"


def aio(fn):
    """The suite has no pytest-asyncio: run an async test on a fresh loop."""
    @functools.wraps(fn)
    def run(*args, **kwargs):
        return asyncio.run(fn(*args, **kwargs))
    return run


def _settings(**kw):
    base = dict(mode="databricks", zerobus_endpoint="1234.zerobus.us-west-2.cloud.databricks.com",
                zerobus_client_id="client-id", zerobus_client_secret="secret", databricks_host="https://dbc-x.cloud.databricks.com",
                history_catalog=CATALOG, history_schema=SCHEMA, history_flush_interval_s=30.0,
                history_flush_timeout_s=5.0)
    base.update(kw)
    return Settings(**base)


class FakeStream:
    def __init__(self, sdk, table):
        self.sdk, self.table = sdk, table
        self.records: list[dict] = []
        self.offsets = 0
        self.flushes = 0
        self.closed = False

    def ingest_record_offset(self, payload):
        if self.sdk.failures_left > 0:
            self.sdk.failures_left -= 1
            raise RuntimeError("transient zerobus failure")
        assert isinstance(payload, str), "JSON record type takes a JSON string"
        self.records.append(json.loads(payload))
        self.offsets += 1
        return self.offsets

    def wait_for_offset(self, offset):
        assert offset == self.offsets

    def flush(self):
        self.flushes += 1

    def close(self):
        self.closed = True


class FakeSdk:
    def __init__(self):
        self.opened: list[str] = []
        self.streams: list[FakeStream] = []
        self.failures_left = 0

    def open(self, table_full_name):
        self.opened.append(table_full_name)
        stream = FakeStream(self, table_full_name)
        self.streams.append(stream)
        return stream


def _utt(seq, text="hello there"):
    return Utterance(call_id="c-1", seq=seq, speaker="customer", text=text, t_start_ms=seq * 10,
                     t_end_ms=seq * 10 + 5, emitted_at_ms=1_700_000_000_000 + seq, playbook="advisor_coach")


def _card(seq):
    return Card(card_id=f"card-{seq}", call_id="c-1", seq=seq, kind="objection_pickup", title="Fees",
                body="Explain the fee.", confidence=0.8, source_doc="kb/x.md", source_clause="c1",
                trigger="objection:fees", created_at_ms=1_700_000_000_500)


# ------------------------------------------------------------- Zerobus sink
def test_endpoint_is_normalised_to_https_and_table_names_are_qualified():
    assert normalise_endpoint("1234.zerobus.us-west-2.cloud.databricks.com/") == \
        "https://1234.zerobus.us-west-2.cloud.databricks.com"
    assert target_table(_settings(), "cards") == "main.call_sidekick.cards"


@aio
async def test_batches_per_table_one_stream_each_and_acks_before_flush():
    sdk = FakeSdk()
    sink = ZerobusHistorySink(_settings(), sdk_factory=lambda s: sdk)
    await sink.start()
    for seq in range(3):
        assert sink.emit("utterances", hs.utterance_row(_utt(seq))) is True
    assert sink.emit("cards", hs.card_row(_card(1), {"candidates": []})) is True
    await sink.stop()
    assert sorted(sdk.opened) == ["main.call_sidekick.cards", "main.call_sidekick.utterances"]
    utt = next(s for s in sdk.streams if s.table.endswith(".utterances"))
    assert [r["seq"] for r in utt.records] == [0, 1, 2]
    assert utt.flushes >= 1 and utt.closed
    card = next(s for s in sdk.streams if s.table.endswith(".cards"))
    assert json.loads(card.records[0]["trace"]) == {"candidates": []}
    stats = sink.stats()
    assert stats["written"] == 4 and stats["failed"] == 0 and stats["dropped"] == 0 and stats["enabled"]


@aio
async def test_queue_is_bounded_and_drops_oldest_without_blocking():
    sdk = FakeSdk()
    sink = ZerobusHistorySink(_settings(history_queue_max=5), sdk_factory=lambda s: sdk)
    await sink.start()
    # emit is synchronous: no await between calls, so the flusher cannot drain while we fill the queue.
    for seq in range(20):
        sink.emit("utterances", hs.utterance_row(_utt(seq)))
    assert sink.stats()["queue_depth"] == 5
    assert sink.counters["dropped"] == 15
    await sink.stop()
    stream = sdk.streams[0]
    assert [r["seq"] for r in stream.records] == [15, 16, 17, 18, 19], "drop-oldest keeps the newest records"


@aio
async def test_transient_write_failure_is_retried_on_a_fresh_stream():
    sdk = FakeSdk()
    sink = ZerobusHistorySink(_settings(history_retry_base_s=0.0), sdk_factory=lambda s: sdk)
    await sink.start()
    sdk.failures_left = 1
    for seq in range(3):
        sink.emit("utterances", hs.utterance_row(_utt(seq)))
    await sink._drain_all()                    # attempt 1 fails, attempt 2 succeeds on a new stream
    assert sink.counters["written"] == 3 and sink.counters["failed"] == 0 and sink.counters["retried"] == 3
    assert sdk.streams[0].closed, "a failed stream is closed and dropped"
    assert len(sdk.opened) == 2
    await sink.stop()


@aio
async def test_persistent_write_failure_is_requeued_once_then_counted():
    sdk = FakeSdk()
    sink = ZerobusHistorySink(_settings(history_retry_base_s=0.0, history_retry_attempts=3), sdk_factory=lambda s: sdk)
    await sink.start()
    sdk.failures_left = 99
    for seq in range(2):
        sink.emit("utterances", hs.utterance_row(_utt(seq)))
    await sink._drain_all()
    # Each batch fails 3 attempts, is requeued once at the front, fails 3 more, and is counted as failed.
    assert sink.counters["requeued"] == 2 and sink.counters["failed"] == 2 and sink.counters["written"] == 0
    assert sink.counters["retried"] == 8
    assert len(sdk.opened) == 6, "one fresh stream per attempt"
    assert len(sink._queue) == 0, "a requeued record is not requeued again"
    sdk.failures_left = 0
    sink.emit("utterances", hs.utterance_row(_utt(9)))
    await sink._drain_all()                    # the sink keeps working afterwards
    assert sink.counters["written"] == 1
    await sink.stop()


@aio
async def test_missing_credentials_disable_the_sink_fail_soft(monkeypatch):
    monkeypatch.delenv("DATABRICKS_HOST", raising=False)
    sink = ZerobusHistorySink(_settings(zerobus_client_secret=None))
    assert sink.enabled is False and "ZEROBUS_CLIENT_ID" in sink.disabled_reason
    await sink.start()
    assert sink.emit("utterances", hs.utterance_row(_utt(1))) is False
    assert sink.counters["skipped"] == 1
    await sink.stop()
    assert sink.stats()["enabled"] is False


def test_missing_endpoint_and_host_are_reported():
    assert "ZEROBUS_ENDPOINT" in ZerobusHistorySink(_settings(zerobus_endpoint=None)).disabled_reason
    s = _settings(databricks_host=None)
    import os
    os.environ.pop("DATABRICKS_HOST", None)
    assert "DATABRICKS_HOST" in ZerobusHistorySink(s).disabled_reason


def test_unknown_table_is_a_programming_error():
    sink = ZerobusHistorySink(_settings(), sdk_factory=lambda s: FakeSdk())
    with pytest.raises(KeyError):
        sink.emit("not_a_table", {})


# ------------------------------------------------------------ memory / null
def test_memory_sink_keeps_typed_rows_and_drops_oldest_when_full():
    sink = MemoryHistorySink(max_rows=2)
    for seq in range(3):
        sink.emit("utterances", hs.utterance_row(_utt(seq)))
    rows = list(sink.rows["utterances"])
    assert [r["seq"] for r in rows] == [1, 2]
    assert sink.counters["dropped"] == 1


def test_null_sink_accepts_nothing():
    sink = NullHistorySink()
    assert sink.emit("calls", {}) is False
    assert sink.stats()["backend"] == "none"


# --------------------------------------------------------- table contract
def test_row_builders_emit_exactly_the_table_columns():
    utt = _utt(3)
    rec = CallRecord(call_id="c-1", playbook="advisor_coach", mode="replay", script="fee_objection",
                     started_at_ms=1, ended_at_ms=2, status="ended")
    rows = {
        "calls": hs.call_row(rec),
        "utterances": hs.utterance_row(utt),
        "enrichments": hs.enrichment_row("c-1", 3, "customer", "x", {"intent": "question", "sentiment": -0.5,
                                         "risk_flags": ["unsupported_claim"], "objection": "fees",
                                         "entities": {}, "methodology_hits": ["budget"]},
                                         1000, 1250, "direct"),
        "cards": hs.card_row(_card(3), {"x": 1}),
        "feedback": hs.feedback_row("card-3", "c-1", "accept", None, 5),
        "voice_turns": hs.voice_turn_row("v-1", {"turn": 1, "role": "assistant", "user_text": "hi", "agent_text": "yo",
                                                 "interrupted": False, "timing": {"total_ms": 9}, "created_at_ms": 4}),
        "voice_items": hs.voice_item_row("v-1", {"item_id": "i-1", "turn": 1, "kind": "action_item", "text": "call",
                                                 "owner": None, "due": None, "source_doc": None, "created_at_ms": 7}),
    }
    rows["actions"] = hs.action_row({"action_id": "a-1", "session_id": "v-1", "call_id": None, "owner": "o",
                                     "kind": "send_email", "summary": "s", "params": {"to": ["x@y.co"]},
                                     "status": "done", "created_at_ms": 1, "result": {"mode": "outbox"}})
    rows["contacts"] = hs.contact_row({"contact_id": "c-1", "owner": None, "first_name": "Dana", "updated_at_ms": 2})
    rows["enrollments"] = hs.enrollment_row({"enrollment_id": "e-1", "contact_id": "c-1", "program": "Autopay",
                                             "owner": "o", "created_at_ms": 3})
    assert set(rows) == set(hs.TABLES)
    for table, row in rows.items():
        assert list(hs.to_record(table, row)) == hs.column_names(table), table
    enr = hs.to_record("enrichments", rows["enrichments"])
    assert enr["latency_ms"] == 250 and enr["enrichment_source"] == "direct"
    assert json.loads(hs.to_record("enrichments", rows["enrichments"])["risk_flags"]) == ["unsupported_claim"]
    assert json.loads(hs.to_record("cards", rows["cards"])["trace"]) == {"x": 1}
    assert json.loads(hs.to_record("actions", rows["actions"])["params"]) == {"to": ["x@y.co"]}


def _init_history_ddl():
    tree = ast.parse((ROOT / "src/notebooks/init_history.py").read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(getattr(t, "id", None) == "TABLE_DDL" for t in node.targets):
            return ast.literal_eval(node.value)
    raise AssertionError("TABLE_DDL not found in init_history.py")


def test_init_history_creates_exactly_the_sink_tables_and_columns():
    ddl = _init_history_ddl()
    parsed = {}
    for stmt in ddl:
        m = re.match(r"CREATE TABLE IF NOT EXISTS (\w+) \((.*)\) USING DELTA\s*$", stmt.strip(), re.S)
        assert m, stmt[:80]
        cols = []
        for part in m.group(2).split(","):
            name, typ = part.split()
            cols.append((name, typ))
        parsed[m.group(1)] = cols
    assert set(parsed) == set(hs.TABLES)
    for table, cols in parsed.items():
        assert cols == hs.TABLES[table], table
        assert all(t in ("STRING", "INT", "BIGINT", "DOUBLE", "BOOLEAN") for _, t in cols), table


def test_init_history_grants_the_zerobus_principal_what_the_docs_require():
    text = (ROOT / "src/notebooks/init_history.py").read_text(encoding="utf-8")
    assert "GRANT USE CATALOG ON CATALOG" in text and "GRANT USE SCHEMA ON SCHEMA" in text
    assert "GRANT MODIFY, SELECT ON TABLE" in text


# --------------------------------------------------------------- config
def test_defaults_are_direct_and_haiku_4_5_voice():
    s = Settings()
    assert s.backbone == "direct" and s.effective_bus == "none"
    assert s.voice_model == "databricks-claude-haiku-4-5"


def test_direct_backbone_rejects_kafka_bus():
    with pytest.raises(ValueError, match="BACKBONE=direct"):
        Settings(backbone="direct", bus_backend="kafka")


def test_kafka_backbone_rejects_zerobus_history():
    with pytest.raises(ValueError, match="kafka"):
        Settings(backbone="kafka", history_backend="zerobus")


def test_effective_backends_per_backbone_and_mode():
    assert Settings(mode="local").effective_history == "memory"
    assert Settings(mode="databricks").effective_history == "zerobus"
    assert Settings(mode="databricks", history_backend="none").effective_history == "none"
    assert Settings(backbone="kafka", mode="local").effective_bus == "memory"
    assert Settings(backbone="kafka", mode="databricks").effective_bus == "kafka"
    assert Settings(backbone="kafka", mode="databricks").effective_history == "none"


def test_direct_build_uses_null_bus_not_kafka():
    import bus
    assert bus.build_bus(Settings(mode="databricks")).__class__.__name__ == "NullBus"
    assert bus.build_bus(Settings(backbone="kafka", mode="databricks")).__class__.__name__ == "KafkaBus"


@aio
async def test_a_timed_out_write_is_bounded_and_resets_its_stream():
    """A blocking SDK write that never returns (wait_for_offset hang) must not stall the drain loop."""
    import threading

    release = threading.Event()

    class HangingStream(FakeStream):
        def wait_for_offset(self, offset):
            release.wait(5)            # the worker thread blocks; the sink must not wait for it

    class HangingSdk(FakeSdk):
        def open(self, table_full_name):
            stream = HangingStream(self, table_full_name)
            self.streams.append(stream)
            return stream

    sdk = HangingSdk()
    sink = ZerobusHistorySink(_settings(history_retry_base_s=0.0, history_retry_attempts=1,
                                        history_write_timeout_s=0.05), sdk_factory=lambda s: sdk)
    await sink.start()
    sink.emit("utterances", hs.utterance_row(_utt(0)))
    try:
        await asyncio.wait_for(sink._drain_all(), timeout=2)
    finally:
        release.set()
    assert sink.counters["timeouts"] >= 1
    assert sink.counters["failed"] + sink.counters["requeued"] >= 1
    assert "utterances" not in sink._streams or sink._streams["utterances"] is not sdk.streams[0], \
        "the stream that timed out is dropped; the next attempt opens a fresh one"
    await sink.stop()
