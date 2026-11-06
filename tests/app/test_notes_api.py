"""Freeform notes over the real app: REST round-trip, scope exclusivity, ownership, and the size cap.

A note is attached to a live call or voice session the caller owns (the same scoping the actions API uses).
Local mode (caller "local") with the voice fakes; no network.
"""

import main
import pytest
from config import Settings
from fastapi.testclient import TestClient
from services import Services
from voice_fakes import FakeStt, FakeTts, FakeVoiceLlm


@pytest.fixture()
def client(monkeypatch):
    settings = Settings(mode="local", llm_enabled=False, retrieval_backend="keyword")
    llm = FakeVoiceLlm(["ok. ", "<<ITEMS>>", "[]"])

    def factory(s):
        svc = Services(settings, llm=llm)
        svc.voice_deps.tts = FakeTts()
        svc.voice_deps.stt = FakeStt("hello")
        return svc

    monkeypatch.setattr(main, "Services", factory)
    monkeypatch.setattr(main, "get_settings", lambda: settings)
    with TestClient(main.app) as c:
        yield c


def _call(client) -> str:
    res = client.post("/api/calls", json={"playbook": "advisor_coach", "mode": "replay", "script": "fee_objection"})
    assert res.status_code == 201, res.text
    return res.json()["call_id"]


def _voice_session(client) -> str:
    res = client.post("/api/voice/sessions", json={"role": "assistant", "playbook": "advisor_coach"})
    assert res.status_code == 201, res.text
    return res.json()["session_id"]


def test_a_new_call_has_an_empty_note(client):
    call_id = _call(client)
    got = client.get("/api/notes", params={"call_id": call_id})
    assert got.status_code == 200, got.text
    assert got.json() == {"text": "", "updated_at_ms": None, "call_id": call_id}


def test_note_round_trips_for_a_call(client):
    call_id = _call(client)
    saved = client.put("/api/notes", params={"call_id": call_id}, json={"text": "Dana worried about fees"})
    assert saved.status_code == 200, saved.text
    body = saved.json()
    assert body["text"] == "Dana worried about fees" and body["updated_at_ms"] is not None
    reread = client.get("/api/notes", params={"call_id": call_id}).json()
    assert reread["text"] == "Dana worried about fees"
    # An overwrite replaces the text.
    client.put("/api/notes", params={"call_id": call_id}, json={"text": "updated"})
    assert client.get("/api/notes", params={"call_id": call_id}).json()["text"] == "updated"


def test_note_round_trips_for_a_voice_session(client):
    sid = _voice_session(client)
    client.put("/api/notes", params={"session_id": sid}, json={"text": "ask about time horizon"})
    assert client.get("/api/notes", params={"session_id": sid}).json()["text"] == "ask about time horizon"


def test_exactly_one_scope_is_required(client):
    call_id = _call(client)
    sid = _voice_session(client)
    assert client.get("/api/notes").status_code == 400
    assert client.get("/api/notes", params={"call_id": call_id, "session_id": sid}).status_code == 400
    assert client.put("/api/notes", json={"text": "x"}).status_code == 400


def test_an_unknown_scope_is_not_found(client):
    assert client.get("/api/notes", params={"call_id": "no-such-call"}).status_code == 404
    assert client.put("/api/notes", params={"session_id": "no-such-session"}, json={"text": "x"}).status_code == 404


def test_an_overlong_note_is_refused(client):
    call_id = _call(client)
    # The pydantic max_length (100k chars) answers 422 before the byte cap is reached.
    assert client.put("/api/notes", params={"call_id": call_id}, json={"text": "x" * 100_001}).status_code == 422
    # Multi-byte text within the char limit but over the byte cap is a 400.
    big = "€" * 40_000  # 3 bytes each = 120 000 bytes, 40 000 chars
    assert client.put("/api/notes", params={"call_id": call_id}, json={"text": big}).status_code == 400
