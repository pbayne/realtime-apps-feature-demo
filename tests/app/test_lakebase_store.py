"""LakebaseStore against a real Postgres (LISTEN/NOTIFY + poll fallback).

Skipped unless TEST_LAKEBASE_HOST is set. Example (throwaway local Postgres):
  TEST_LAKEBASE_HOST=/tmp TEST_LAKEBASE_PORT=5544 TEST_LAKEBASE_DB=copilot_test \
  TEST_LAKEBASE_USER=postgres .venv/bin/python -m pytest tests/app/test_lakebase_store.py
"""

import asyncio
import os
import time
import uuid

import pytest
from config import Settings
from models import CallRecord, Card, Utterance, now_ms
from store.lakebase import LakebaseStore

HOST = os.environ.get("TEST_LAKEBASE_HOST")
pytestmark = pytest.mark.skipif(not HOST, reason="set TEST_LAKEBASE_HOST to run against a real Postgres")

TRIGGER_SQL = """
CREATE OR REPLACE FUNCTION copilot_notify_enrichment() RETURNS trigger AS $$
BEGIN
  PERFORM pg_notify('enrichment_ready', NEW.call_id || ':' || NEW.seq);
  RETURN NEW;
END; $$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS enrichments_notify ON enrichments;
CREATE TRIGGER enrichments_notify AFTER INSERT OR UPDATE ON enrichments
  FOR EACH ROW EXECUTE FUNCTION copilot_notify_enrichment();
"""


def _settings() -> Settings:
    return Settings(
        mode="databricks", lakebase_host=HOST, lakebase_port=int(os.environ.get("TEST_LAKEBASE_PORT", "5432")),
        lakebase_db=os.environ.get("TEST_LAKEBASE_DB", "postgres"),
        lakebase_user=os.environ.get("TEST_LAKEBASE_USER", "postgres"),
        lakebase_password=os.environ.get("TEST_LAKEBASE_PASSWORD") or "unused",
        lakebase_sslmode=os.environ.get("TEST_LAKEBASE_SSLMODE", "disable"),
        lakebase_pool_min=1, lakebase_pool_max=3, llm_enabled=False,
    )


def _row(call_id, seq, speaker="customer", text="what are the fees"):
    return {"call_id": call_id, "seq": seq, "speaker": speaker, "text": text, "intent": "question",
            "sentiment": 0.0, "risk_flags": [], "objection": "fees", "entities": {}, "methodology_hits": ["budget"],
            "enriched_at_ms": now_ms()}


async def _with_store(fn, *, trigger: bool):
    store = LakebaseStore(_settings())
    await store.start()
    try:
        if trigger:
            async with store._pool.acquire() as conn:
                await conn.execute(TRIGGER_SQL)
        else:
            async with store._pool.acquire() as conn:
                await conn.execute("DROP TRIGGER IF EXISTS enrichments_notify ON enrichments")
        return await fn(store)
    finally:
        await store.stop()


def test_roundtrip_utterance_card_feedback_and_snapshot():
    call_id = str(uuid.uuid4())

    async def scenario(store):
        await store.create_call(CallRecord(call_id=call_id, playbook="advisor_coach", mode="replay",
                                           script="fee_objection", started_at_ms=now_ms()))
        await store.put_utterance(Utterance(call_id=call_id, seq=1, speaker="customer", text="what are the fees",
                                            t_start_ms=0, t_end_ms=900, emitted_at_ms=now_ms(), playbook="advisor_coach"))
        card = Card(card_id=str(uuid.uuid4()), call_id=call_id, seq=1, kind="objection_pickup", title="Objection: fees",
                    body="Explain the fee.", confidence=0.85, source_doc="kb/x.md", source_clause="Say this",
                    trigger="objection:fees")
        await store.put_card(card, {"turn": 1})
        await store.add_feedback(card.card_id, call_id, "accept", None, now_ms())
        snap = await store.snapshot(call_id)
        await store.end_call(call_id, now_ms())
        return snap, await store.get_call(call_id)

    snap, call = asyncio.run(_with_store(scenario, trigger=False))
    assert call["status"] == "ended" and call["playbook"] == "advisor_coach"
    assert len(snap["utterances"]) == 1 and snap["utterances"][0]["text"] == "what are the fees"
    assert snap["cards"][0]["status"] == "accepted"
    assert snap["feedback"][0]["action"] == "accept"


def test_wait_resolves_via_notify_before_timeout():
    call_id = str(uuid.uuid4())

    async def scenario(store):
        async def writer():
            await asyncio.sleep(0.3)
            await store.put_enrichment(call_id, 7, _row(call_id, 7))

        task = asyncio.ensure_future(writer())
        t0 = time.monotonic()
        row = await store.wait_enrichment(call_id, 7, timeout_s=3.0)
        elapsed = time.monotonic() - t0
        await task
        return row, elapsed

    row, elapsed = asyncio.run(_with_store(scenario, trigger=True))
    assert row is not None and row["objection"] == "fees"
    assert elapsed < 1.5, f"notify path should be fast, took {elapsed:.2f}s"


def test_wait_falls_back_to_polling_without_trigger():
    call_id = str(uuid.uuid4())

    async def scenario(store):
        async def writer():
            await asyncio.sleep(0.25)
            # Written by "another writer" (e.g. the RTM sink): no NOTIFY because the trigger is absent.
            await store.put_enrichment(call_id, 3, _row(call_id, 3))

        task = asyncio.ensure_future(writer())
        row = await store.wait_enrichment(call_id, 3, timeout_s=2.0)
        await task
        return row

    assert asyncio.run(_with_store(scenario, trigger=False))["seq"] == 3


def test_wait_times_out_to_none():
    async def scenario(store):
        return await store.wait_enrichment(str(uuid.uuid4()), 1, timeout_s=0.4)

    assert asyncio.run(_with_store(scenario, trigger=True)) is None
