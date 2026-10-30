"""End-to-end in local mode with the LLM replaced by a deterministic fake (no network).

Covers the HTTP + WebSocket contract: replay -> transcript.final -> enrichment -> coverage ->
card.start/delta/done -> metrics -> call.ended, plus browser_stt over /ws/call and the snapshot.
"""

import asyncio
import hashlib
import json
import math
import re
import time

import main
import pytest
from config import Settings
from fastapi.testclient import TestClient
from llm import LlmClient
from services import Services


class FakeLlm(LlmClient):
    """Perception and compose answered locally. embed() is a hashed bag-of-words.
    perception_delay simulates the haiku round trip (SPEC: perception never gates the card)."""

    def __init__(self, settings: Settings, perception_delay: float = 0.0):
        super().__init__(settings, sdk_config=object())
        self.calls = 0
        self.perception_delay = perception_delay

    @property
    def available(self) -> bool:
        return True

    async def _guard(self) -> None:
        return None

    async def chat(self, model, messages, *, max_tokens=300, timeout=None):
        self.calls += 1
        system = messages[0]["content"]
        user = messages[-1]["content"]
        if "ONE JSON object" in system:
            # One perception call returns the whole schema (see sidekick/perception.py).
            if self.perception_delay:
                await asyncio.sleep(self.perception_delay)
            question = user.rstrip().endswith("?") or "charge" in user.lower()
            return json.dumps({"intent": "question" if question else "statement", "sentiment": 0.0,
                               "objection": "fees" if "charge" in user.lower() else None,
                               "question": question, "confidence": 0.8})
        return "{}"

    async def chat_stream(self, model, messages, *, max_tokens=160, timeout=None):
        self.calls += 1
        for piece in ["Acknowledge the concern, ", "then restate the fee ", "in one plain sentence."]:
            yield piece

    async def multipart(self, endpoint, route, *, files, data, timeout=None):
        # Parakeet stand-in (SPEC 3.3 contract): text-only verbose_json, so words spread across the window.
        return {"text": "what are the fees"}

    async def embed(self, model, texts, *, timeout=None):
        out = []
        for text in texts:
            vec = [0.0] * 64
            for tok in re.findall(r"[a-z]+", text.lower()):
                vec[int(hashlib.md5(tok.encode()).hexdigest(), 16) % 64] += 1.0
            norm = math.sqrt(sum(v * v for v in vec)) or 1.0
            out.append([v / norm for v in vec])
        return out


@pytest.fixture()
def client(monkeypatch):
    settings = Settings(mode="local", llm_enabled=True, replay_speed=40, debounce_ms=300)
    monkeypatch.setattr(main, "Services", lambda s: Services(settings, llm=FakeLlm(settings)))
    monkeypatch.setattr(main, "get_settings", lambda: settings)
    with TestClient(main.app) as c:
        yield c


@pytest.fixture()
def slow_perception_client(monkeypatch):
    settings = Settings(mode="local", llm_enabled=True, replay_speed=40, debounce_ms=300, perception_budget_s=2.5)
    monkeypatch.setattr(main, "Services", lambda s: Services(settings, llm=FakeLlm(settings, perception_delay=1.2)))
    monkeypatch.setattr(main, "get_settings", lambda: settings)
    with TestClient(main.app) as c:
        yield c


def _collect(ws, until_type="call.ended", timeout_s=90):
    events = []
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        ev = ws.receive_json()
        events.append(ev)
        if ev["type"] == until_type:
            return events
    raise AssertionError(f"no {until_type} within {timeout_s}s; saw {len(events)} events")


def test_healthz_and_catalogue(client):
    health = client.get("/healthz").json()
    assert health["mode"] == "local" and health["bus"] == "memory" and health["store"] == "memory"
    assert health["llm"]["available"] is True
    names = [p["name"] for p in client.get("/api/playbooks").json()["playbooks"]]
    assert names == ["advisor_coach", "contact_center"]
    scripts = client.get("/api/scripts", params={"playbook": "advisor_coach"}).json()
    assert isinstance(scripts, list) and "fee_objection" in scripts
    assert client.get("/api/scripts", params={"playbook": "nope"}).status_code == 404


def test_replay_call_emits_full_event_flow(client):
    r = client.post("/api/calls", json={"playbook": "advisor_coach", "mode": "replay", "script": "fee_objection", "speed": 40})
    assert r.status_code == 201
    call_id = r.json()["call_id"]
    with client.websocket_connect(f"/ws/call/{call_id}") as ws:
        events = _collect(ws)
    types = [e["type"] for e in events]

    finals = [e for e in events if e["type"] == "transcript.final"]
    assert len(finals) == 23, "one final per scripted utterance"
    assert {f["speaker"] for f in finals} == {"agent", "customer"}
    assert [f["seq"] for f in finals] == sorted(f["seq"] for f in finals)
    assert any(e["type"] == "transcript.partial" for e in events)

    enrichments = [e for e in events if e["type"] == "enrichment" and e.get("source") != "perception"]
    assert len(enrichments) == len(finals)
    # Perception (one call per customer final) may refine the chips with its own enrichment event.
    perception_events = [e for e in events if e["type"] == "enrichment" and e.get("source") == "perception"]
    assert perception_events and all(e["speaker"] == "customer" for e in perception_events)
    assert any(e["objection"] == "fees" for e in enrichments)
    assert any("unsupported_claim" in e["risk_flags"] for e in enrichments)

    assert "coverage" in types
    starts = [e for e in events if e["type"] == "card.start"]
    dones = [e for e in events if e["type"] == "card.done"]
    assert starts and len(starts) == len(dones)
    kinds = {c["card"]["kind"] for c in starts}
    assert "compliance" in kinds and "objection_pickup" in kinds
    compliance_done = next(d for d in dones if d["card"]["kind"] == "compliance")
    assert "guarantee" in compliance_done["card"]["body"].lower()          # verbatim playbook text
    regular = [d for d in dones if d["card"]["kind"] != "compliance"]
    assert regular and all(d["card"]["body"] for d in regular)
    assert all(len(d["card"]["body"].split()) <= 35 for d in dones)
    assert all(len(c["card"]["title"].split()) <= 8 for c in starts)

    metrics = [e for e in events if e["type"] == "metrics"]
    assert metrics and all("stt_commit_ms" in m for m in metrics)
    assert any(m["card_ms"] is not None for m in metrics)
    assert all("emitted_at_ms" in e for e in events)
    assert types[-1] == "call.ended"

    snap = client.get(f"/api/calls/{call_id}").json()
    assert len(snap["transcript"]) == 23
    assert snap["cards"] and snap["call"]["status"] == "ended"
    assert any(c["kind"] == "objection_pickup" for c in snap["cards"])
    assert all(d["verdict"] in ("winner", "blocked", "allowed") for c in snap["decisions"] for d in c["candidates"])


def test_browser_stt_over_call_socket_flows_into_sidekick(client):
    r = client.post("/api/calls", json={"playbook": "advisor_coach", "mode": "mic"})
    call_id = r.json()["call_id"]
    with client.websocket_connect(f"/ws/call/{call_id}") as ws:
        assert ws.receive_json()["type"] == "coverage"
        ws.send_json({"type": "browser_stt", "speaker": "customer", "text": "What do you charge for all this?",
                      "is_final": True})
        seen = []
        while True:
            ev = ws.receive_json()
            seen.append(ev)
            if ev["type"] == "card.done":
                break
        ws.send_json({"type": "feedback", "card_id": seen[-1]["card"]["card_id"], "action": "accept"})
    final = next(e for e in seen if e["type"] == "transcript.final")
    assert final["speaker"] == "customer" and final["seq"] >= 1
    assert any(e["type"] == "enrichment" and e["objection"] == "fees" for e in seen)
    assert any(e["type"] == "card.start" and e["card"]["kind"] == "objection_pickup" for e in seen)
    client.post(f"/api/calls/{call_id}/end")
    snap = client.get(f"/api/calls/{call_id}").json()
    assert snap["feedback"] and snap["feedback"][0]["action"] == "accept"


def test_mic_audio_socket_produces_final_on_call_socket(client):
    """PCM16 frames over /ws/audio: energy VAD gates speech, Parakeet stand-in decodes, final reaches /ws/call."""
    r = client.post("/api/calls", json={"playbook": "advisor_coach", "mode": "mic"})
    call_id = r.json()["call_id"]
    loud = (2000).to_bytes(2, "little", signed=True) * 1600          # 100 ms of loud samples
    quiet = b"\x00\x00" * 1600                                       # 100 ms of silence
    with client.websocket_connect(f"/ws/call/{call_id}") as call_ws:
        call_ws.receive_json()  # coverage
        with client.websocket_connect(f"/ws/audio/{call_id}?channel=customer") as audio_ws:
            for _ in range(20):
                audio_ws.send_bytes(loud)
            for _ in range(10):
                audio_ws.send_bytes(quiet)
            audio_ws.close()
        seen = []
        deadline = time.time() + 20
        while time.time() < deadline:
            ev = call_ws.receive_json()
            seen.append(ev)
            if ev["type"] == "enrichment":
                break
    final = next(e for e in seen if e["type"] == "transcript.final")
    assert final["speaker"] == "customer"
    assert final["text"] == "what are the fees"
    assert any(e["type"] == "enrichment" and e["objection"] == "fees" for e in seen)
    client.post(f"/api/calls/{call_id}/end")


def test_audio_socket_rejects_replay_calls(client):
    call_id = client.post("/api/calls", json={"playbook": "advisor_coach", "mode": "replay", "script": "fee_objection",
                                              "speed": 50}).json()["call_id"]
    with client.websocket_connect(f"/ws/audio/{call_id}?channel=agent") as ws:
        msg = ws.receive_json()
        assert msg["type"] == "error" and "mode=mic" in msg["message"]


def test_unknown_script_and_playbook_are_rejected(client):
    assert client.post("/api/calls", json={"playbook": "advisor_coach", "script": "nope"}).status_code == 404
    assert client.post("/api/calls", json={"playbook": "nope", "mode": "mic"}).status_code == 404
    assert client.post("/api/calls", json={"playbook": "advisor_coach", "script": "../x"}).status_code == 400


def test_cards_do_not_wait_for_perception(slow_perception_client):
    """Perception answers 1.2 s after each final. Cards must start before the first perception event,
    and the decision trace must show the turn decided with perception pending plus a late verdict."""
    client = slow_perception_client
    call_id = client.post("/api/calls", json={"playbook": "advisor_coach", "mode": "replay",
                                              "script": "fee_objection", "speed": 40}).json()["call_id"]
    with client.websocket_connect(f"/ws/call/{call_id}") as ws:
        events = _collect(ws)
    types = [(e["type"], e.get("source")) for e in events]
    first_card = next(i for i, (t, _) in enumerate(types) if t == "card.start")
    first_perception = next(i for i, (t, src) in enumerate(types) if t == "enrichment" and src == "perception")
    assert first_card < first_perception, "a card must not be gated on perception"

    snap = client.get(f"/api/calls/{call_id}").json()
    customer_turns = [d for d in snap["decisions"] if d.get("customer_turn")]
    assert customer_turns
    pending = [d for d in customer_turns if d.get("perception_path") == "late_eligible"]
    assert pending, "with a 1.2 s perception round trip every turn is decided before perception returns"
    assert all(d["perception"] == {"state": "pending"} for d in pending)
    late = [d for d in snap["decisions"] if d.get("kind") == "late_perception"]
    assert late and {d["verdict"] for d in late} <= {"allowed", "blocked", "skipped"}
    for d in late:
        turn = next(t for t in customer_turns if t["seq"] == d["seq"])
        assert turn["late_perception"]["verdict"] == d["verdict"]
        if d["verdict"] == "skipped":
            assert turn["cards"], "late candidates may only fire when no card fired for the turn"
        if d["verdict"] == "allowed":
            assert not turn["cards"], "allowed late candidates never follow a card on the same turn"
            assert d["cards"], "an allowed verdict carries the card kinds that fired"
    if any(d["verdict"] == "allowed" for d in late):
        assert any(e["type"] == "metrics" and e.get("card_path") == "late" for e in events)


def test_perception_budget_miss_is_not_a_card_failure(monkeypatch):
    """Perception slower than its budget: cards still flow, perception shows a timeout in the trace."""
    settings = Settings(mode="local", llm_enabled=True, replay_speed=40, debounce_ms=300, perception_budget_s=0.2)
    monkeypatch.setattr(main, "Services", lambda s: Services(settings, llm=FakeLlm(settings, perception_delay=1.0)))
    monkeypatch.setattr(main, "get_settings", lambda: settings)
    with TestClient(main.app) as client:
        call_id = client.post("/api/calls", json={"playbook": "advisor_coach", "mode": "replay",
                                                  "script": "fee_objection", "speed": 40}).json()["call_id"]
        with client.websocket_connect(f"/ws/call/{call_id}") as ws:
            events = _collect(ws)
        snap = client.get(f"/api/calls/{call_id}").json()
    assert any(e["type"] == "card.done" for e in events)
    timeouts = [d for d in snap["decisions"] if (d.get("perception") or {}).get("errors", {}).get("perception") == "timeout"]
    assert timeouts
