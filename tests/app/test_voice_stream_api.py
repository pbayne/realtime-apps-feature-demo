"""WebSocket contract for streaming voice input and live analysis (SPEC 9.2, 9.7), through the real app with fakes."""

import main
import pytest
from config import Settings
from fastapi.testclient import TestClient
from services import Services
from stt.base import Word
from voice_fakes import FakeStt, FakeTts, FakeVoiceLlm, ScriptedStreamDecoder, pcm_ms

WORDS = [Word("what", 0, 400), Word("are", 400, 800), Word("your", 800, 1100), Word("fees", 1100, 1500)]


def build_client(monkeypatch, llm, **settings_kw):
    settings = Settings(mode="local", llm_enabled=True, retrieval_backend="keyword", debounce_ms=100,
                        **settings_kw)

    def factory(s):
        svc = Services(settings, llm=llm)
        svc.voice_deps.tts = FakeTts()
        svc.voice_deps.stt = FakeStt("what are the fees")
        svc.voice_deps.stream_decoder = lambda emit: ScriptedStreamDecoder(WORDS)
        return svc

    monkeypatch.setattr(main, "Services", factory)
    monkeypatch.setattr(main, "get_settings", lambda: settings)
    return TestClient(main.app)


@pytest.fixture()
def client(monkeypatch):
    llm = FakeVoiceLlm(["Sure. ", "The fee is one percent."],
                       suggestions=[{"kind": "next_step", "text": "Ask what result matters most."},
                                    {"kind": "risk", "text": "The fee objection is likely."}])
    with build_client(monkeypatch, llm) as c:
        yield c


def new_session(client, role="assistant"):
    body = {"role": role, "playbook": "advisor_coach"}
    return client.post("/api/voice/sessions", json=body).json()["session_id"]


def read_until(ws, pred, limit=120):
    seen = []
    for _ in range(limit):
        ev = ws.receive_json()
        seen.append(ev)
        if pred(seen):
            return seen
    raise AssertionError(f"condition not reached; saw {[e['type'] for e in seen]}")


def turn_done(turn):
    return lambda evs: any(e["type"] == "turn_done" and e["turn"] == turn for e in evs)


def test_streamed_pcm_over_the_socket_gives_partials_and_a_reply(client):
    sid = new_session(client)
    with client.websocket_connect(f"/ws/voice/{sid}") as ws:
        ws.send_json({"type": "stream_start", "sample_rate": 16000})
        for _ in range(15):
            ws.send_bytes(pcm_ms(100, True))            # raw PCM: no RIFF header, so it is a stream frame
        for _ in range(12):
            ws.send_bytes(pcm_ms(100, False))
        events = read_until(ws, turn_done(1))
    kinds = [e["type"] for e in events]
    assert kinds[0] == "stream_started"
    assert any(e["type"] == "user_partial" and e["turn"] == 1 for e in events)
    assert [e["text"] for e in events if e["type"] == "user_text"] == ["what are your fees"]
    assert "agent_text" in kinds


def test_live_events_and_suggestions_arrive_while_the_user_is_talking(client):
    sid = new_session(client)
    with client.websocket_connect(f"/ws/voice/{sid}") as ws:
        ws.send_json({"type": "stream_start"})
        # 2.5 s of speech: with the 250 ms decode lag, all four words are visible, which the 4-word floor needs.
        for _ in range(25):
            ws.send_bytes(pcm_ms(100, True))
        seen = read_until(ws, lambda evs: any(e["type"] == "suggestion" for e in evs))
    lives = [e for e in seen if e["type"] == "live"]
    assert lives and "sentiment" in lives[0] and "signals" in lives[0] and "coverage" in lives[0]
    chip = next(e for e in seen if e["type"] == "suggestion")
    assert chip["item"]["provisional"] is True and chip["item"]["kind"] in ("next_step", "risk")


def test_pin_persists_the_chip_as_an_outcome_and_dismiss_removes_another(client):
    sid = new_session(client)
    with client.websocket_connect(f"/ws/voice/{sid}") as ws:
        ws.send_json({"type": "stream_start"})
        for _ in range(25):
            ws.send_bytes(pcm_ms(100, True))
        seen = read_until(ws, lambda evs: len([e for e in evs if e["type"] == "suggestion"]) >= 2)
        chips = [e["item"]["item_id"] for e in seen if e["type"] == "suggestion"]
        ws.send_json({"type": "pin", "item_id": chips[0]})
        pinned = read_until(ws, lambda evs: any(e["type"] == "item" for e in evs))
        assert pinned[-1]["item"]["item_id"] == chips[0] and pinned[-1]["item"]["kind"] == "next_step"
        ws.send_json({"type": "dismiss", "item_id": chips[1]})
        ws.send_json({"type": "pin", "item_id": chips[1]})
        err = read_until(ws, lambda evs: any(e["type"] == "error" for e in evs))
        assert "unknown or superseded" in err[-1]["message"]
    snap = client.get(f"/api/voice/sessions/{sid}").json()
    assert [i["item_id"] for i in snap["items"]] == [chips[0]]
    assert snap["suggestions"] == [] and isinstance(snap["sentiment"], list) and snap["sentiment"]


def test_stream_stop_without_a_stream_is_a_no_op_and_bad_frames_are_reported(client):
    sid = new_session(client)
    with client.websocket_connect(f"/ws/voice/{sid}") as ws:
        ws.send_json({"type": "stream_stop", "flush": True})
        ws.send_bytes(pcm_ms(100, True))                # no stream open
        err = ws.receive_json()
        assert err["type"] == "error" and "WAV" in err["message"]
        ws.send_json({"type": "stream_start", "endpoint_silence_ms": 99999})
        assert ws.receive_json()["type"] == "error"
        ws.send_json({"type": "text", "text": "still alive"})
        read_until(ws, turn_done(1))


def test_mute_discards_the_utterance_over_the_socket(client):
    sid = new_session(client)
    with client.websocket_connect(f"/ws/voice/{sid}") as ws:
        ws.send_json({"type": "stream_start"})
        for _ in range(6):
            ws.send_bytes(pcm_ms(100, True))
        ws.send_json({"type": "stream_stop", "flush": False})
        stopped = read_until(ws, lambda evs: any(e["type"] == "stream_stopped" for e in evs))
        assert stopped[-1]["flush"] is False
        ws.send_json({"type": "text", "text": "ok"})
        events = read_until(ws, turn_done(1))
    assert [e["text"] for e in events if e["type"] == "user_text"] == ["ok"]


def test_session_deadline_ends_the_socket_with_a_reason(monkeypatch):
    llm = FakeVoiceLlm(["x"])
    with build_client(monkeypatch, llm, voice_max_session_minutes=0.001) as c:
        sid = new_session(c)
        with c.websocket_connect(f"/ws/voice/{sid}") as ws:
            ev = read_until(ws, lambda evs: any(e["type"] == "session_ended" for e in evs))
            assert ev[-1]["reason"] == "max_session_minutes"


def test_the_concurrent_voice_session_cap_answers_429(monkeypatch):
    llm = FakeVoiceLlm(["x"])
    with build_client(monkeypatch, llm, voice_max_concurrent_sessions=1) as c:
        assert c.post("/api/voice/sessions", json={"role": "assistant", "playbook": "advisor_coach"}).status_code == 201
        second = c.post("/api/voice/sessions", json={"role": "assistant", "playbook": "advisor_coach"})
        assert second.status_code == 429


def test_healthz_reports_voice_readiness_and_limits_in_local_mode(client):
    body = client.get("/healthz").json()
    readiness = body["voice"]["readiness"]
    assert readiness["ready"] is True
    assert readiness["endpoints"]["stt"]["state"] == "local"
    assert body["voice"]["limits"]["max_concurrent_sessions"] == 20
    assert body["voice"]["limits"]["endpoint_silence_ms"] == 700
