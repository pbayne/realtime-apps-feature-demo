"""Actions over the real app (SPEC 12): REST, WebSocket decisions and events, and proposals from a voice turn.
Local mode (caller "local"), with the voice fakes and the outbox executor (no SMTP configured)."""

import main
import pytest
from config import Settings
from fastapi.testclient import TestClient
from services import Services
from voice_fakes import FakeStt, FakeTts, FakeVoiceLlm

PROPOSAL = ('[{"kind": "send_email", "params": {"to": ["dana.lee@example.com"], "subject": "Your fee breakdown", '
            '"body_text": "Hi Dana, the breakdown is attached."}}]')
EMAIL = {"to": ["dana.lee@example.com"], "cc": [], "subject": "Fees", "body_text": "Breakdown to follow."}


@pytest.fixture()
def client(monkeypatch):
    settings = Settings(mode="local", llm_enabled=True, retrieval_backend="keyword", debounce_ms=100,
                        action_safety_lock=True)
    llm = FakeVoiceLlm(["Sure, I will send it. ", "<<ITEMS>>", "[]", "<<ACTIONS>>", PROPOSAL])

    def factory(s):
        svc = Services(settings, llm=llm)
        svc.voice_deps.tts = FakeTts()
        svc.voice_deps.stt = FakeStt("send the fee breakdown")
        return svc

    monkeypatch.setattr(main, "Services", factory)
    monkeypatch.setattr(main, "get_settings", lambda: settings)
    with TestClient(main.app) as c:
        yield c


def _voice_session(client) -> str:
    res = client.post("/api/voice/sessions", json={"role": "assistant", "playbook": "advisor_coach"})
    assert res.status_code == 201, res.text
    return res.json()["session_id"]


def _until(ws, types, limit=80):
    seen = []
    for _ in range(limit):
        ev = ws.receive_json()
        seen.append(ev)
        if ev["type"] in types:
            return seen
    raise AssertionError(f"no {types}; saw {[e['type'] for e in seen]}")


def test_manual_action_lifecycle_over_rest(client):
    sid = _voice_session(client)
    created = client.post("/api/actions", json={"session_id": sid, "kind": "send_email", "params": EMAIL})
    assert created.status_code == 201, created.text
    body = created.json()
    assert body["status"] == "proposed" and body["proposed_by"] == "user"
    assert body["safety"]["lock"] is True and body["safety"]["mode"] == "outbox"
    action_id = body["action_id"]

    listed = client.get("/api/actions", params={"session_id": sid}).json()
    assert [a["action_id"] for a in listed["actions"]] == [action_id]

    preview = client.get(f"/api/actions/{action_id}/message.eml")
    assert preview.status_code == 200 and b"[Sidekick test" in preview.content
    assert preview.headers["content-type"].startswith("message/rfc822")

    done = client.post(f"/api/actions/{action_id}/approve").json()
    assert done["status"] == "outboxed" and done["result"]["has_eml"] is True
    assert "eml" not in done["result"]              # rendered text is served by the message endpoint
    assert client.get(f"/api/actions/{action_id}/message.eml").status_code == 200

    again = client.post(f"/api/actions/{action_id}/approve").json()
    assert again["status"] == "outboxed"            # approving twice executes once

    undo = client.post(f"/api/actions/{action_id}/undo")
    assert undo.status_code == 409                   # an email cannot be undone


def test_reject_and_unknown_or_bad_requests(client):
    sid = _voice_session(client)
    action_id = client.post("/api/actions", json={"session_id": sid, "kind": "send_email", "params": EMAIL}).json()[
        "action_id"]
    assert client.post(f"/api/actions/{action_id}/reject").json()["status"] == "rejected"
    assert client.post(f"/api/actions/{action_id}/approve").status_code == 409
    assert client.post("/api/actions/nope/approve").status_code == 404
    assert client.get("/api/actions/nope/message.eml").status_code == 404
    bad = client.post("/api/actions", json={"session_id": sid, "kind": "send_email",
                                            "params": {**EMAIL, "to": ["not-an-address"]}})
    assert bad.status_code == 400
    assert client.post("/api/actions", json={"kind": "send_email", "params": EMAIL}).status_code == 400
    unknown = client.post("/api/actions", json={"session_id": "no-such-session", "kind": "send_email",
                                                "params": EMAIL})
    assert unknown.status_code == 404


def test_meeting_invite_endpoint_serves_calendar_text(client):
    sid = _voice_session(client)
    params = {"title": "Income review", "attendees": ["dana.lee@example.com"],
              "start_iso": "2026-11-03T14:00:00-05:00", "duration_min": 45}
    action_id = client.post("/api/actions", json={"session_id": sid, "kind": "create_meeting",
                                                  "params": params}).json()["action_id"]
    ics = client.get(f"/api/actions/{action_id}/invite.ics")
    assert ics.status_code == 200 and ics.text.startswith("BEGIN:VCALENDAR")
    assert "text/calendar" in ics.headers["content-type"]
    assert client.get(f"/api/actions/{action_id}/message.eml").status_code == 200


def test_voice_turn_proposes_and_websocket_decides(client):
    sid = _voice_session(client)
    with client.websocket_connect(f"/ws/voice/{sid}") as ws:
        assert ws.receive_json()["type"] == "hello"
        ws.send_json({"type": "text", "text": "Send Dana the fee breakdown"})
        events = _until(ws, {"turn_done"})
        proposed = [e for e in events if e["type"] == "action"]
        assert proposed and proposed[0]["action"]["status"] == "proposed"
        assert proposed[0]["action"]["kind"] == "send_email"
        action_id = proposed[0]["action"]["action_id"]

        ws.send_json({"type": "action_decision", "action_id": action_id, "decision": "approve"})
        statuses = []
        while "outboxed" not in statuses:
            ev = ws.receive_json()
            if ev["type"] == "action":
                statuses.append(ev["action"]["status"])
        assert statuses[-1] == "outboxed" and "executing" in statuses

    snap = client.get(f"/api/voice/sessions/{sid}").json()
    assert [a["action_id"] for a in snap["actions"]] == [action_id]
    assert snap["actions"][0]["status"] == "outboxed"


def test_websocket_decision_errors_do_not_close_the_socket(client):
    sid = _voice_session(client)
    with client.websocket_connect(f"/ws/voice/{sid}") as ws:
        ws.receive_json()
        ws.send_json({"type": "action_decision", "action_id": "missing", "decision": "approve"})
        err = ws.receive_json()
        assert err["type"] == "error" and "unknown action" in err["message"]
        ws.send_json({"type": "interrupt"})          # the socket is still usable
