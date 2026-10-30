"""Production hardening (SPEC 10): session lifetime and reaper, socket takeover, ownership, input limits, store
degradation, circuit breakers per model, queued-turn supersede, and log context. No network."""

import asyncio
import logging
import time
from pathlib import Path
from types import SimpleNamespace

import main
import pytest
from config import Settings
from fastapi.testclient import TestClient
from llm import WARMUP, LlmClient, LlmUnavailable
from models import CallRecord, cap_text
from ratelimit import TokenBucket
from services import Services
from starlette.websockets import WebSocketDisconnect
from store.lakebase import LakebaseStore
from store.memory import MemoryStore
from voice_fakes import FakeStt, FakeTts, FakeVoiceLlm, make_voice_session

APP_DIR = Path(__file__).resolve().parents[2] / "src" / "app"


def build_client(monkeypatch, llm=None, **settings):
    base = dict(mode="local", llm_enabled=True, retrieval_backend="keyword", debounce_ms=100,
                session_reap_interval_s=3600)   # the reaper is driven explicitly in these tests
    base.update(settings)
    s = Settings(**base)
    llm = llm or FakeVoiceLlm(["Sure. ", "<<ITEMS>>", "[]"])

    def factory(st):
        svc = Services(st, llm=llm)
        svc.voice_deps.tts = FakeTts()
        svc.voice_deps.stt = FakeStt("what are the fees")
        return svc

    monkeypatch.setattr(main, "Services", factory)
    monkeypatch.setattr(main, "get_settings", lambda: s)
    return TestClient(main.app)


@pytest.fixture()
def client(monkeypatch):
    with build_client(monkeypatch) as c:
        yield c


def svc_of(client) -> Services:
    return client.app.state.services


def close_code(ws) -> int:
    """Read past any error frame the server sends before it closes, and return the close code."""
    with pytest.raises(WebSocketDisconnect) as exc:
        for _ in range(5):
            ws.receive_json()
    return exc.value.code


def new_voice(client, role="assistant", **body):
    r = client.post("/api/voice/sessions", json={"role": role, "playbook": "advisor_coach", **body})
    assert r.status_code == 201, r.text
    return r.json()


def new_call(client) -> str:
    r = client.post("/api/calls", json={"playbook": "advisor_coach", "mode": "mic"})
    assert r.status_code == 201, r.text
    return r.json()["call_id"]


# ------------------------------------------------------------- 1. lifetime and reaper

def test_a_voice_session_that_never_connects_is_reaped_after_its_ttl(client):
    sid = new_voice(client)["session_id"]
    svc = svc_of(client)
    vs = svc.voice_sessions[sid]
    asyncio.run(svc.reap_once(now=vs.detached_at + 1))          # inside the TTL: kept
    assert not vs.closed
    client.portal.call(svc.reap_once, vs.detached_at + svc.settings.voice_detached_ttl_s + 1)
    assert vs.closed, "armed at creation: a session that never connected is closed"
    client.portal.call(svc.reap_once, vs.closed_mono + svc.settings.session_grace_s + 1)
    assert sid not in svc.voice_sessions, "closed sessions are dropped after the grace period"


def test_an_attached_voice_session_is_not_reaped(client):
    sid = new_voice(client)["session_id"]
    svc = svc_of(client)
    with client.websocket_connect(f"/ws/voice/{sid}") as ws:
        assert ws.receive_json()["type"] == "hello"
        client.portal.call(svc.reap_once, time.monotonic() + 10_000)
        assert not svc.voice_sessions[sid].closed


def test_a_detached_call_is_finished_after_its_ttl_and_dropped_after_grace(client):
    cid = new_call(client)
    svc = svc_of(client)
    session = svc.sessions[cid]
    client.portal.call(svc.reap_once, session.detached_at + svc.settings.call_detached_ttl_s + 1)
    assert session.ended
    client.portal.call(svc.reap_once, session.ended_mono + svc.settings.session_grace_s + 1)
    assert cid not in svc.sessions


def test_delete_ends_the_session_and_the_attached_socket_closes_with_4410(client):
    sid = new_voice(client)["session_id"]
    with client.websocket_connect(f"/ws/voice/{sid}") as ws:
        assert ws.receive_json()["type"] == "hello"
        assert client.delete(f"/api/voice/sessions/{sid}").status_code == 204
        ev = ws.receive_json()
        assert ev == {**ev, "type": "session_ended", "reason": "closed"}
        assert close_code(ws) == 4410
    assert svc_of(client).voice_sessions[sid].closed
    with client.websocket_connect(f"/ws/voice/{sid}") as ws:
        assert close_code(ws) == 4404, "an ended session never reconnects"


def test_post_end_is_the_beacon_twin_of_delete(client):
    sid = new_voice(client)["session_id"]
    assert client.post(f"/api/voice/sessions/{sid}/end").status_code == 204
    assert svc_of(client).voice_sessions[sid].closed


def test_a_practice_socket_blip_keeps_the_session_for_the_grace_period(client):
    body = new_voice(client, role="practice")
    sid = body["session_id"]
    with client.websocket_connect(f"/ws/voice/{sid}") as ws:
        assert ws.receive_json()["type"] == "hello"
    vs = svc_of(client).voice_sessions[sid]
    assert not vs.closed and vs.socket is None, "detached, not closed"
    assert svc_of(client).sessions[body["call_id"]].attached == 0
    with client.websocket_connect(f"/ws/voice/{sid}") as ws:
        assert ws.receive_json()["type"] == "hello"
        assert svc_of(client).sessions[body["call_id"]].attached == 1, "reattach resubscribes the call"


# ------------------------------------------------------------- 2. socket takeover

def test_a_second_socket_evicts_the_first_with_4409_and_keeps_the_session(client):
    sid = new_voice(client)["session_id"]
    first = client.websocket_connect(f"/ws/voice/{sid}")
    ws1 = first.__enter__()
    assert ws1.receive_json()["type"] == "hello"
    with client.websocket_connect(f"/ws/voice/{sid}") as ws2:
        assert ws2.receive_json()["type"] == "hello"
        with pytest.raises(WebSocketDisconnect) as evicted:
            while True:
                ws1.receive_json()
        assert evicted.value.code == 4409
        first.__exit__(None, None, None)
        ws2.send_json({"type": "text", "text": "what are the fees"})
        seen = []
        for _ in range(60):
            ev = ws2.receive_json()
            seen.append(ev["type"])
            if ev["type"] == "turn_done":
                break
        assert "user_text" in seen and seen[-1] == "turn_done", "the new socket is served, not the evicted one"
    assert not svc_of(client).voice_sessions[sid].closed


def test_the_evicted_socket_does_not_interrupt_or_detach_its_successor(client):
    sid = new_voice(client)["session_id"]
    svc = svc_of(client)
    with client.websocket_connect(f"/ws/voice/{sid}") as ws1:
        ws1.receive_json()
        with client.websocket_connect(f"/ws/voice/{sid}") as ws2:
            ws2.receive_json()
            # ws1 is closed by the server now; its finally must not release ws2's claim.
            try:
                ws1.receive_json()
            except WebSocketDisconnect:
                pass
            assert svc.voice_sessions[sid].socket is not None
            assert svc.voice_sessions[sid].socket is not None and svc.voice_sessions[sid].detached_at <= time.monotonic()


def test_claim_and_release_unit(monkeypatch):
    llm = FakeVoiceLlm(["x"])
    vs, _ = make_voice_session(llm)

    async def run():
        t1 = asyncio.ensure_future(asyncio.sleep(60))
        t2 = asyncio.ensure_future(asyncio.sleep(60))
        ws1, ws2 = object(), object()
        assert vs.claim_socket(ws1, t1) is None
        assert vs.claim_socket(ws2, t2) is ws1, "the previous owner is returned for eviction"
        assert t1.cancelled() or t1.cancelling() or True
        await asyncio.sleep(0)
        assert t1.cancelled()
        assert vs.release_socket(ws1) is False, "an evicted socket cannot release the session"
        assert vs.release_socket(ws2) is True
        t1.cancel()
        t2.cancel()

    asyncio.run(run())


# ------------------------------------------------------------- 6. ownership

def test_another_owner_gets_404_on_every_call_route(client):
    cid = new_call(client)
    svc = svc_of(client)
    svc.sessions[cid].record.owner = "bob@example.com"
    assert client.get(f"/api/calls/{cid}").status_code == 404
    assert client.post(f"/api/calls/{cid}/end").status_code == 404
    assert not svc.sessions[cid].ended, "a foreign end is refused, not applied"
    with client.websocket_connect(f"/ws/call/{cid}") as ws:
        assert close_code(ws) == 4404


def test_another_owner_gets_404_on_every_voice_route(client):
    sid = new_voice(client)["session_id"]
    svc = svc_of(client)
    svc.voice_sessions[sid].owner = "bob@example.com"
    assert client.get(f"/api/voice/sessions/{sid}").status_code == 404
    assert client.delete(f"/api/voice/sessions/{sid}").status_code == 404
    assert not svc.voice_sessions[sid].closed
    with client.websocket_connect(f"/ws/voice/{sid}") as ws:
        assert close_code(ws) == 4404


def test_a_voice_session_cannot_link_a_call_owned_by_someone_else(client):
    cid = new_call(client)
    svc_of(client).sessions[cid].record.owner = "bob@example.com"
    r = client.post("/api/voice/sessions", json={"role": "assistant", "playbook": "advisor_coach", "call_id": cid})
    assert r.status_code == 404


def test_the_caller_is_the_forwarded_email_then_user_in_deployed_mode():
    def req(headers, mode):
        state = SimpleNamespace(services=SimpleNamespace(settings=SimpleNamespace(mode=mode)))
        return SimpleNamespace(headers=headers, app=SimpleNamespace(state=state))

    assert main._caller(req({"x-forwarded-email": "a@x.com", "x-forwarded-user": "u"}, "databricks")) == "a@x.com"
    assert main._caller(req({"x-forwarded-user": "u-1"}, "databricks")) == "u-1"
    assert main._caller(req({}, "databricks")) is None, "no identity: nothing is owned, every route says 404"
    assert main._caller(req({"x-forwarded-email": "a@x.com"}, "local")) == "local"


def test_the_owner_is_recorded_on_the_call_and_in_the_store(client):
    cid = new_call(client)
    assert svc_of(client).sessions[cid].owner == "local"
    assert CallRecord(call_id="c", playbook="p", mode="mic", script=None, started_at_ms=1, owner="u").to_wire()["owner"] == "u"


# ------------------------------------------------------------- 7. input limits

def test_cap_text_counts_utf8_bytes():
    assert cap_text("abc", 3) == "abc"
    with pytest.raises(ValueError):
        cap_text("é" * 2, 3)       # 4 bytes


def test_an_oversized_typed_turn_is_reported_and_the_socket_stays_up(client):
    sid = new_voice(client)["session_id"]
    with client.websocket_connect(f"/ws/voice/{sid}") as ws:
        ws.receive_json()
        ws.send_json({"type": "text", "text": "x" * 3000})
        ev = ws.receive_json()
        assert ev["type"] == "error" and "too long" in ev["message"]
        ws.send_json({"type": "dismiss", "item_id": "nope"})   # still served
        assert svc_of(client).voice_sessions[sid].closed is False


def test_a_socket_message_over_the_rate_is_dropped_not_answered():
    now = [0.0]
    bucket = TokenBucket(rate=50, burst=50, clock=lambda: now[0])
    assert sum(bucket.allow() for _ in range(80)) == 50, "a 50-message burst, then drops"
    assert bucket.dropped == 30
    now[0] += 1.0
    assert sum(bucket.allow() for _ in range(80)) == 50, "refills at the sustained rate"


def test_the_per_user_and_per_instance_call_caps_answer_429(monkeypatch):
    with build_client(monkeypatch, max_active_calls_per_user=1) as c:
        new_call(c)
        r = c.post("/api/calls", json={"playbook": "advisor_coach", "mode": "mic"})
        assert r.status_code == 429
    with build_client(monkeypatch, max_active_calls=1) as c:
        new_call(c)
        r = c.post("/api/calls", json={"playbook": "advisor_coach", "mode": "mic"})
        assert r.status_code == 429


def test_the_audio_socket_message_cap_is_set_on_the_server():
    command = (APP_DIR / "app.yaml").read_text(encoding="utf-8")
    assert "--ws-max-size 1048576" in command


def test_browser_stt_over_the_limit_is_refused(client):
    cid = new_call(client)
    session = svc_of(client).sessions[cid]
    with pytest.raises(ValueError):
        session.browser_result("agent", {"text": "x" * 3000, "is_final": True})


def test_feedback_text_over_the_limit_is_refused(client):
    cid = new_call(client)
    session = svc_of(client).sessions[cid]
    with pytest.raises(ValueError):
        asyncio.run(session.feedback("card-1", "edit", "x" * 3000))


# ------------------------------------------------------------- 8. feedback scope

def test_feedback_only_changes_a_card_of_the_same_call():
    store = MemoryStore()

    async def run():
        from models import Card
        card = Card(card_id="k-1", call_id="c-1", seq=1, kind="k", title="t", body="b", confidence=1.0,
                    source_doc=None, source_clause=None, trigger="x")
        await store.put_card(card, {})
        await store.add_feedback("k-1", "c-2", "reject", None, 1)
        assert store.cards["k-1"]["status"] == "shown", "another call's id does not change the card"
        await store.add_feedback("k-1", "c-1", "accept", None, 2)
        assert store.cards["k-1"]["status"] == "accepted"

    asyncio.run(run())


def test_lakebase_feedback_update_is_scoped_to_the_call():
    store = LakebaseStore(Settings(mode="local"))
    executed = []

    class Pool:
        async def execute(self, sql, *args):
            executed.append((sql, args))

    store._pool = Pool()
    asyncio.run(store.add_feedback("k-1", "c-2", "accept", None, 5))
    update_sql, update_args = executed[-1]
    assert "WHERE card_id=$1 AND call_id=$2" in update_sql
    assert update_args == ("k-1", "c-2", "accepted")


# ------------------------------------------------------------- 9. health does not leak

def test_health_reports_only_the_exception_type_of_the_last_llm_error():
    class Boom(Exception):
        pass

    llm = LlmClient(Settings(mode="local", llm_failure_threshold=99))
    llm._record_fail("fast", Boom("https://secret.example.internal/serving-endpoints token=abc"))
    status = llm.status()
    assert status["last_error"] == "Boom"
    assert "secret" not in str(status) and "token" not in str(status)


# ------------------------------------------------------------- 4. resilience

def test_the_store_failing_at_startup_runs_degraded_instead_of_crashing(monkeypatch):
    settings = Settings(mode="local", llm_enabled=False, retrieval_backend="keyword")
    svc = Services(settings, llm=FakeVoiceLlm(["x"]))
    attempts = []

    async def flaky_start():
        attempts.append(1)
        raise ConnectionError("lakebase unreachable")

    svc.store.start = flaky_start

    async def run():
        await svc.start()
        assert svc.store_ready is False
        health = svc.health()
        assert health["status"] == "degraded" and health["store_ready"] is False
        await svc.stop()

    asyncio.run(run())
    assert attempts, "the first start was attempted"


def test_get_call_serves_the_in_memory_call_when_the_store_is_down(client):
    cid = new_call(client)
    store = svc_of(client).store

    async def down(_call_id):
        raise ConnectionError("lakebase down")

    store.snapshot = down
    r = client.get(f"/api/calls/{cid}")
    assert r.status_code == 200
    body = r.json()
    assert body["degraded"] is True and body["call"]["call_id"] == cid


def test_lakebase_pool_gets_command_and_connect_timeouts(monkeypatch):
    import asyncpg

    seen = {}

    async def fake_create_pool(**kwargs):
        seen.update(kwargs)
        raise RuntimeError("stop after recording the arguments")

    monkeypatch.setattr(asyncpg, "create_pool", fake_create_pool)
    store = LakebaseStore(Settings(mode="local", lakebase_host="h", lakebase_user="u", lakebase_password="p" * 24,
                                   lakebase_command_timeout_s=2.0, lakebase_connect_timeout_s=5.0))
    with pytest.raises(RuntimeError):
        asyncio.run(store.start())
    assert seen["command_timeout"] == 2.0 and seen["timeout"] == 5.0


def test_the_voice_transcript_snapshot_is_bounded_and_fail_soft(monkeypatch):
    llm = FakeVoiceLlm(["x"])
    vs, store = make_voice_session(llm, call_id="c-1")

    class HungStore:
        async def snapshot(self, _call_id):
            await asyncio.sleep(30)

    vs.deps.store = HungStore()
    vs.deps.calls["c-1"] = SimpleNamespace(coverage_event=lambda: {"items": []})

    async def run():
        t0 = time.monotonic()
        block = await vs._call_block()
        assert time.monotonic() - t0 < 2.0, "a hung store costs at most the snapshot budget"
        assert isinstance(block, str)

    asyncio.run(run())


# ------------------------------------------------------------- 10/11. breakers and warm-up

def test_one_models_breaker_does_not_stop_another_model():
    s = Settings(mode="local", llm_enabled=True, llm_failure_threshold=2, llm_breaker_cooldown_s=60)
    llm = LlmClient(s)
    llm._record_fail("model-a", RuntimeError("x"))
    llm._record_fail("model-a", RuntimeError("x"))
    assert llm.breaker_open("model-a") and not llm.breaker_open("model-b")
    assert llm.available is False, "any open breaker is visible in status"
    asyncio.run(llm._guard("model-b"))          # does not raise
    with pytest.raises(LlmUnavailable):
        asyncio.run(llm._guard("model-a"))


def test_warm_up_failures_do_not_open_a_breaker():
    llm = LlmClient(Settings(mode="local", llm_failure_threshold=1))

    async def run():
        token = WARMUP.set(True)
        try:
            llm._record_fail("model-a", RuntimeError("cold"))
        finally:
            WARMUP.reset(token)

    asyncio.run(run())
    assert not llm.breaker_open("model-a")
    assert llm.last_error_type is None, "a warm-up failure is not an error report"


# ------------------------------------------------------------- 12. queued turns

def test_a_queued_turn_is_superseded_by_a_newer_one():
    llm = FakeVoiceLlm(["Reply. "])
    vs, _ = make_voice_session(llm)

    async def run():
        vs.submit_text("first question")
        first = vs._latest
        vs.submit_text("second question")    # the first is still queued: it must be cancelled, not run
        assert first.started is False
        for _ in range(400):
            await asyncio.sleep(0.01)
            if vs._task is not None and vs._task.done():
                break
        await asyncio.sleep(0.05)
        await asyncio.gather(vs._task, return_exceptions=True)
        return first

    first = asyncio.run(run())
    assert first.started is False and first.user_text == ""
    assert vs.turns and vs.turns[-1]["turn"] == 2, "the newer turn ran"


# ------------------------------------------------------------- 13. audio socket teardown

def test_end_of_audio_always_gets_through_a_full_queue():
    q: asyncio.Queue = asyncio.Queue(maxsize=3)
    for i in range(3):
        q.put_nowait(i)
    main._end_frames(q)
    items = []
    while not q.empty():
        items.append(q.get_nowait())
    assert items[-1] is None, "the end marker is delivered even when the queue was full"


# ------------------------------------------------------------- 15. log context

def test_stt_failures_log_the_call_id(caplog):
    from session import CallSession

    settings = Settings(mode="local", llm_enabled=False, retrieval_backend="keyword")
    svc = Services(settings, llm=FakeVoiceLlm(["x"]))
    record = CallRecord(call_id="call-xyz", playbook="advisor_coach", mode="mic", script=None, started_at_ms=1)
    session = CallSession(record=record, deps=svc.deps)

    class Broken:
        speaker = "agent"

        async def run(self, _frames):
            raise RuntimeError("decoder fell over")
            yield  # pragma: no cover

    caplog.set_level(logging.ERROR, logger="sidekick.session")
    asyncio.run(session._drive_stt(Broken(), iter(())))
    assert any("call_id=call-xyz" in r.getMessage() for r in caplog.records)
