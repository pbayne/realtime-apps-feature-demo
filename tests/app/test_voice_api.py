"""REST and WebSocket contract for voice mode (SPEC 9.1, 9.2), through the real app with fakes injected."""


import main
import pytest
from config import Settings
from fastapi.testclient import TestClient
from services import Services
from voice_fakes import FakeStt, FakeTts, FakeVoiceLlm, wav_bytes


@pytest.fixture()
def client(monkeypatch):
    settings = Settings(mode="local", llm_enabled=True, retrieval_backend="keyword", debounce_ms=100)
    llm = FakeVoiceLlm(["Sure. ", "Here is the answer. ", "<<ITEMS>>", '[{"kind": "summary", "text": "Fees discussed"}]'])

    def factory(s):
        svc = Services(settings, llm=llm)
        svc.voice_deps.tts = FakeTts()
        svc.voice_deps.stt = FakeStt("what are the fees")
        return svc

    monkeypatch.setattr(main, "Services", factory)
    monkeypatch.setattr(main, "get_settings", lambda: settings)
    with TestClient(main.app) as c:
        yield c


def _until_turn_done(ws, limit=60):
    events = []
    for _ in range(limit):
        ev = ws.receive_json()
        events.append(ev)
        if ev["type"] == "turn_done":
            return events
    raise AssertionError(f"no turn_done; saw {[e['type'] for e in events]}")


def test_voices_are_labelled_objects_and_playbook_scenarios_are_listed(client):
    voices = client.get("/api/voices").json()
    assert voices["default"] == "af_heart"
    assert {"id": "af_heart", "label": "Heart (US F)"} in voices["voices"]
    pbs = {p["name"]: p for p in client.get("/api/playbooks").json()["playbooks"]}
    for name in ("advisor_coach", "contact_center"):
        assert len(pbs[name]["scenarios"]) == 2
        sc = pbs[name]["scenarios"][0]
        assert sc["name"] == sc["key"] and sc["persona"] and sc["voice"] and sc["title"]


def test_create_assistant_session_and_validation_errors(client):
    r = client.post("/api/voice/sessions", json={"role": "assistant", "playbook": "advisor_coach"})
    assert r.status_code == 201
    body = r.json()
    assert body["role"] == "assistant" and body["call_id"] is None and body["voice"] == "af_heart"
    assert client.post("/api/voice/sessions", json={"role": "assistant", "playbook": "nope"}).status_code == 404
    assert client.post("/api/voice/sessions", json={"role": "assistant", "call_id": "missing"}).status_code == 404
    assert client.post("/api/voice/sessions", json={"role": "practice", "playbook": "advisor_coach",
                                                    "call_id": "x"}).status_code == 400
    assert client.post("/api/voice/sessions", json={"role": "practice", "playbook": "advisor_coach",
                                                    "scenario": "nope"}).status_code == 404
    assert client.post("/api/voice/sessions", json={"role": "wizard", "playbook": "advisor_coach"}).status_code == 422


def test_practice_session_creates_its_call_and_defaults_to_first_scenario(client):
    body = client.post("/api/voice/sessions", json={"role": "practice", "playbook": "advisor_coach"}).json()
    assert body["call_id"] and body["scenario"] == "fee_objection" and body["voice"] == "af_bella"
    call = client.get(f"/api/calls/{body['call_id']}")
    assert call.status_code == 200 and call.json()["call"]["mode"] == "voice"


def test_typed_turn_over_socket_then_snapshot_rehydrates(client):
    sid = client.post("/api/voice/sessions", json={"role": "assistant", "playbook": "advisor_coach"}).json()["session_id"]
    with client.websocket_connect(f"/ws/voice/{sid}") as ws:
        assert ws.receive_json()["type"] == "hello"
        ws.send_json({"type": "text", "text": "what are the fees"})
        events = _until_turn_done(ws)
    assert events[0]["type"] == "user_text" and events[0]["turn"] == 1 and events[0]["stt_ms"] == 0
    items = [e for e in events if e["type"] == "item"]
    assert items and items[0]["item"]["kind"] == "summary"
    assert events[-1]["timing"]["total_ms"] is not None

    snap = client.get(f"/api/voice/sessions/{sid}").json()
    assert snap["role"] == "assistant" and snap["playbook"] == "advisor_coach"
    assert snap["turns"][0]["user_text"] == "what are the fees" and snap["turns"][0]["interrupted"] is False
    assert snap["items"][0]["text"] == "Fees discussed"


def test_wav_frame_over_socket_runs_stt_and_bad_frame_is_reported_not_fatal(client):
    sid = client.post("/api/voice/sessions", json={"role": "assistant", "playbook": "advisor_coach"}).json()["session_id"]
    with client.websocket_connect(f"/ws/voice/{sid}") as ws:
        assert ws.receive_json()["type"] == "hello"
        ws.send_bytes(b"definitely not wav")
        err = ws.receive_json()
        assert err["type"] == "error" and "WAV" in err["message"]
        ws.send_bytes(wav_bytes())
        events = _until_turn_done(ws)
    assert events[0]["type"] == "user_text" and events[0]["text"] == "what are the fees" and events[0]["stt_ms"] >= 0


def test_malformed_and_unknown_messages_get_an_error_event(client):
    sid = client.post("/api/voice/sessions", json={"role": "assistant", "playbook": "advisor_coach"}).json()["session_id"]
    with client.websocket_connect(f"/ws/voice/{sid}") as ws:
        assert ws.receive_json()["type"] == "hello"
        ws.send_text("{not json")
        assert ws.receive_json()["type"] == "error"
        ws.send_json({"type": "dance"})
        assert "unknown message type" in ws.receive_json()["message"]
        ws.send_json({"type": "config", "voice": "BAD VOICE"})
        assert ws.receive_json()["type"] == "error"


def test_interrupt_with_no_turn_running_is_a_no_op(client):
    sid = client.post("/api/voice/sessions", json={"role": "assistant", "playbook": "advisor_coach"}).json()["session_id"]
    with client.websocket_connect(f"/ws/voice/{sid}") as ws:
        assert ws.receive_json()["type"] == "hello"
        ws.send_json({"type": "interrupt"})
        ws.send_json({"type": "text", "text": "hello"})
        events = _until_turn_done(ws)
    assert events[-1]["turn"] == 1 and events[-1]["interrupted"] is False


def test_practice_socket_forwards_call_events_and_accepts_card_feedback(client):
    sid = client.post("/api/voice/sessions", json={"role": "practice", "playbook": "advisor_coach"}).json()["session_id"]
    with client.websocket_connect(f"/ws/voice/{sid}") as ws:
        assert ws.receive_json()["type"] == "hello"
        first = ws.receive_json()
        assert first["type"] == "coverage"          # the call's checklist arrives on the voice socket first
        ws.send_json({"type": "feedback", "card_id": "missing-card", "action": "accept"})
        ws.send_json({"type": "text", "text": "Let me explain the fee."})
        seen = [first]
        # The call's transcript.final for the rep's line is pushed by the call loop, so it may arrive after turn_done.
        for _ in range(80):
            if any(e["type"] == "turn_done" for e in seen) and any(
                    e["type"] == "transcript.final" and e["speaker"] == "agent" for e in seen):
                break
            seen.append(ws.receive_json())
        assert any(e["type"] == "turn_done" for e in seen)
        assert any(e["type"] == "transcript.final" and e["speaker"] == "agent" for e in seen)
        assert not [e for e in seen if e["type"] == "error"]


def test_unknown_voice_session_socket_is_closed_with_error(client):
    with client.websocket_connect("/ws/voice/nope") as ws:
        ev = ws.receive_json()
        assert ev["type"] == "error"
