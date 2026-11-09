"""Call history endpoints (request E): the user-scoped Past Calls list and detail. Local mode, memory store."""

import main
import pytest
from config import Settings
from fastapi.testclient import TestClient
from models import CallRecord, Card, Utterance
from services import Services
from voice_fakes import FakeStt, FakeTts, FakeVoiceLlm


@pytest.fixture()
def client(monkeypatch):
    settings = Settings(mode="local", llm_enabled=False, retrieval_backend="keyword")
    llm = FakeVoiceLlm(["hi"])

    def factory(s):
        svc = Services(settings, llm=llm)
        svc.voice_deps.tts = FakeTts()
        svc.voice_deps.stt = FakeStt("hello")
        return svc

    monkeypatch.setattr(main, "Services", factory)
    monkeypatch.setattr(main, "get_settings", lambda: settings)
    with TestClient(main.app) as c:
        yield c


async def _seed(store, call_id, owner):
    await store.create_call(CallRecord(call_id=call_id, playbook="advisor_coach", mode="replay",
                                       script="demo", started_at_ms=1000, owner=owner))
    await store.put_utterance(Utterance(call_id=call_id, seq=1, speaker="customer", text="Can you send fees?",
                                        t_start_ms=0, t_end_ms=1, emitted_at_ms=2, playbook="advisor_coach"))
    await store.put_card(Card(card_id=f"{call_id}-c1", call_id=call_id, seq=1, kind="suggestion",
                              title="Offer the fee breakdown", body="Share the PDF.", confidence=0.9,
                              source_doc=None, source_clause=None, trigger="ask", created_at_ms=3), {})


def test_list_and_detail_owner_scoped(client):
    svc = client.app.state.services
    # Seed two calls for "local" and one for another owner, directly in the store (app event loop).
    client.portal.call(_seed, svc.store, "call-local-1", "local")
    client.portal.call(_seed, svc.store, "call-local-2", "local")
    client.portal.call(_seed, svc.store, "call-other", "someone.else@x.com")

    listed = client.get("/api/history/calls").json()
    ids = [c["call_id"] for c in listed["calls"]]
    assert "call-local-1" in ids and "call-local-2" in ids
    assert "call-other" not in ids  # owner-scoped

    detail = client.get("/api/history/calls/call-local-1")
    assert detail.status_code == 200, detail.text
    d = detail.json()
    assert d["call"]["call_id"] == "call-local-1"
    assert len(d["transcript"]) == 1 and d["transcript"][0]["text"] == "Can you send fees?"
    assert len(d["cards"]) == 1 and d["cards"][0]["title"] == "Offer the fee breakdown"
    assert isinstance(d["actions"], list)

    # Another owner's call is 404, not visible.
    assert client.get("/api/history/calls/call-other").status_code == 404
    # Unknown call is 404.
    assert client.get("/api/history/calls/does-not-exist").status_code == 404
