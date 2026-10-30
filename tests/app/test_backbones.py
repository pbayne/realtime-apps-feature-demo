"""Two backbones end to end, local mode, LLM faked (see test_e2e_replay.FakeLlm).

direct (default): no bus; each final enriched in process; enrichments to the store; history to the sink.
kafka (local stand-in): in-memory bus as Kafka, BusEnricher as RTM, no history sink.
"""

import sys
from pathlib import Path

import main
import pytest
from config import Settings
from fastapi.testclient import TestClient
from services import Services

sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_e2e_replay import FakeLlm  # noqa: E402  (shared fake LLM; tests/app is on sys.path via pytest)


def _client(monkeypatch, **kw):
    settings = Settings(mode="local", llm_enabled=True, replay_speed=40, debounce_ms=300, **kw)
    monkeypatch.setattr(main, "Services", lambda s: Services(settings, llm=FakeLlm(settings)))
    monkeypatch.setattr(main, "get_settings", lambda: settings)
    return settings


def _run_replay(client):
    call_id = client.post("/api/calls", json={"playbook": "advisor_coach", "mode": "replay",
                                              "script": "fee_objection", "speed": 40}).json()["call_id"]
    events = []
    with client.websocket_connect(f"/ws/call/{call_id}") as ws:
        while True:
            ev = ws.receive_json()
            events.append(ev)
            if ev["type"] == "call.ended":
                break
    return call_id, events


def test_direct_backbone_replay_enriches_in_process_and_writes_history(monkeypatch):
    _client(monkeypatch)  # default backbone is direct
    with TestClient(main.app) as client:
        services = client.app.state.services
        health = client.get("/healthz").json()
        assert health["backbone"] == "direct" and health["bus"] == "none"
        assert health["history"]["backend"] == "memory" and health["history"]["enabled"]

        call_id, events = _run_replay(client)
        services = client.app.state.services
        assert type(services.bus).__name__ == "NullBus", "direct backbone must not use a bus"
        assert services.bus_enricher is None

        finals = [e for e in events if e["type"] == "transcript.final"]
        enrich = [e for e in events if e["type"] == "enrichment" and e.get("source") != "perception"]
        assert len(finals) == 23 and len(enrich) == len(finals)
        assert {e["source"] for e in enrich} == {"direct"}, "enrichment is in process, never rtm or local_fallback"
        assert any(e["objection"] == "fees" for e in enrich)
        assert any(e["type"] == "card.done" for e in events)
        metrics = [e for e in events if e["type"] == "metrics"]
        assert metrics and all(m["rtm_ms"] is not None and m["rtm_ms"] < 50 for m in metrics), \
            "in-process enrichment is milliseconds, not an RTM wait"

        # Live state: enrichments reached the store (Lakebase in databricks mode).
        assert len([k for k in services.store.enrichments if k[0] == call_id]) == len(finals)

        # History: the sink received every table the direct path writes.
        rows = services.history.rows
        assert len(rows["utterances"]) == 23
        assert len(rows["enrichments"]) == 23
        assert all(r["enrichment_source"] == "direct" for r in rows["enrichments"])
        assert rows["cards"], "cards are written to history"
        assert {r["call_id"] for r in rows["calls"]} == {call_id}
        statuses = [r["status"] for r in rows["calls"]]
        assert statuses == ["active", "ended"]


def test_direct_backbone_history_survives_store_outage(monkeypatch):
    """A failing store (Lakebase down) must not stop enrichment or history."""
    _client(monkeypatch)

    class BrokenStore:
        def __getattr__(self, name):
            async def fail(*_a, **_k):
                raise ConnectionError("lakebase down")
            return fail

    with TestClient(main.app) as client:
        services = client.app.state.services
        services.store = BrokenStore()
        services.deps.store = services.store
        _, events = _run_replay(client)
        assert any(e["type"] == "card.done" for e in events)
        assert len(services.history.rows["utterances"]) == 23


def test_kafka_backbone_local_uses_memory_bus_and_rtm_stand_in(monkeypatch):
    _client(monkeypatch, backbone="kafka")
    with TestClient(main.app) as client:
        health = client.get("/healthz").json()
        assert health["backbone"] == "kafka" and health["bus"] == "memory"
        assert health["history"]["backend"] == "none"
        services = client.app.state.services
        assert services.bus_enricher is not None
        call_id, events = _run_replay(client)
        finals = [e for e in events if e["type"] == "transcript.final"]
        sources = {e["source"] for e in events if e["type"] == "enrichment" and e.get("source") != "perception"}
        assert sources == {"rtm"}, "the local RTM stand-in writes the row before the wait ends"
        assert len(finals) == 23
        assert len(services.store.enrichments) == 23
        assert services.history.stats()["enabled"] is False


@pytest.mark.parametrize("backbone", ["direct", "kafka"])
def test_settings_validate_for_both_backbones(backbone):
    s = Settings(backbone=backbone, mode="local")
    assert s.backbone == backbone
