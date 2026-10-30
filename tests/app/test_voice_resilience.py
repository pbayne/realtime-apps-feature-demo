"""Cold endpoints, limits and observability for voice (SPEC 9.6): warming/retry, budget, readiness, logs."""

import asyncio
import json
import logging

import pytest
from llm import LlmUnavailable
from stt.base import Window
from voice.readiness import EndpointReadiness
from voice.resilience import RetryPolicy, WarmState, call_with_warmup
from voice.stream import VoiceDecoder
from voice_fakes import FakeStt, FakeTts, FakeVoiceLlm, events_of, make_voice_session, pcm_ms, wav_bytes


def run(coro):
    return asyncio.run(coro)


async def until_turn_done(vs, turn, timeout=10.0):
    out = []
    while True:
        ev = await asyncio.wait_for(vs.out.get(), timeout)
        out.append(ev)
        if ev["type"] == "turn_done" and ev["turn"] == turn:
            return out


FAST = dict(voice_retry_base_s=0.0, voice_retry_cap_s=0.0, voice_warmup_budget_s=5.0)


def test_a_cold_tts_endpoint_is_retried_and_the_turn_still_speaks():
    tts = FakeTts(cold_failures=2)
    vs, _ = make_voice_session(FakeVoiceLlm(["Sure. ", "It is one percent."]), tts=tts, **FAST)

    async def go():
        vs.submit_text("what are the fees")
        return await until_turn_done(vs, 1)

    events = run(go())
    status = events_of(events, "status")
    assert [s["state"] for s in status] == ["warming", "ready"]
    assert all(s["stage"] == "tts" and s["turn"] == 1 for s in status)
    assert "Warming up voice" in status[0]["message"]
    assert events_of(events, "audio"), "audio arrives once the endpoint is up"
    assert not events_of(events, "error")
    assert tts.attempts >= 3


def test_a_cold_stt_endpoint_is_retried_and_the_turn_is_not_lost():
    vs, _ = make_voice_session(FakeVoiceLlm(["Okay."]), stt=FakeStt("what are the fees", cold_failures=1), **FAST)

    async def go():
        vs.submit_audio(wav_bytes())
        return await until_turn_done(vs, 1)

    events = run(go())
    assert [s["state"] for s in events_of(events, "status")] == ["warming", "ready"]
    assert events_of(events, "user_text")[0]["text"] == "what are the fees"
    assert not events_of(events, "error")


def test_a_cold_endpoint_past_the_budget_fails_the_sentence_with_a_failed_status():
    tts = FakeTts(cold_failures=10**6)
    policy = dict(voice_retry_base_s=0.01, voice_retry_cap_s=0.01, voice_warmup_budget_s=0.05)
    vs, _ = make_voice_session(FakeVoiceLlm(["Sure."]), tts=tts, **policy)

    async def go():
        vs.submit_text("hello")
        return await until_turn_done(vs, 1)

    events = run(go())
    states = [s["state"] for s in events_of(events, "status")]
    assert states[0] == "warming" and states[-1] == "failed"
    err = events_of(events, "error")
    assert err and err[0]["stage"] == "tts"
    assert events_of(events, "turn_done")[0]["interrupted"] is False, "the text reply still completes"


def test_failures_that_are_not_cold_are_not_retried():
    tts = FakeTts(fail=True)
    vs, _ = make_voice_session(FakeVoiceLlm(["Sure."]), tts=tts, **FAST)

    async def go():
        vs.submit_text("hello")
        return await until_turn_done(vs, 1)

    events = run(go())
    assert tts.attempts == 1, "a plain failure is final for the sentence"
    assert not events_of(events, "status")
    assert events_of(events, "error")[0]["stage"] == "tts"


def test_call_with_warmup_backs_off_exponentially_and_respects_the_budget():
    sleeps: list[float] = []
    calls = {"n": 0}
    events: list[dict] = []

    async def op():
        calls["n"] += 1
        if calls["n"] <= 3:
            raise LlmUnavailable("starting", cold=True)
        return "ok"

    async def fake_sleep(d):
        sleeps.append(d)

    warm = WarmState("stt", events.append)
    result = run(call_with_warmup(op, warm, RetryPolicy(budget_s=100, base_s=1.0, cap_s=3.0),
                                  sleep=fake_sleep))
    assert result == "ok" and sleeps == [1.0, 2.0, 3.0]
    assert [e["state"] for e in events] == ["warming", "ready"]


def test_streamed_partials_report_warming_without_retrying_and_finals_retry():
    llm = FakeVoiceLlm(["x"])
    llm.stt_script = [LlmUnavailable("starting", cold=True),
                      {"text": "hi", "words": [{"word": "hi", "start": 0.0, "end": 0.4}]},
                      LlmUnavailable("starting", cold=True),
                      {"text": "hi", "words": [{"word": "hi", "start": 0.0, "end": 0.4}]}]
    emitted: list[dict] = []

    async def no_sleep(_):
        return None

    dec = VoiceDecoder(llm, "sidekick-parakeet", emit_status=emitted.append,
                       policy=RetryPolicy(budget_s=5, base_s=0, cap_s=0), sleep=no_sleep)
    pcm = pcm_ms(400, True)

    async def go():
        first = await dec.decode(Window(0, 400, pcm, final=False))      # cold: no words, warming status
        second = await dec.decode(Window(0, 400, pcm, final=False))     # warm again
        final = await dec.decode(Window(0, 400, pcm, final=True))       # cold, then retried to success
        return first, second, final

    first, second, final = run(go())
    assert first == [] and second and final and final[0].w == "hi"
    assert llm.multipart_calls == 4, "a partial is one attempt; the final retried once"
    assert [e["state"] for e in emitted] == ["warming", "ready", "warming", "ready"]


def test_readiness_is_cached_for_the_ttl_and_never_fails_health():
    now = {"t": 0.0}
    probes: list[str] = []

    async def fetch(name):
        probes.append(name)
        if name == "sidekick-kokoro":
            raise RuntimeError("control plane down")
        return "ready"

    r = EndpointReadiness({"stt": "sidekick-parakeet", "tts": "sidekick-kokoro"}, fetch, ttl_s=30,
                          clock=lambda: now["t"])

    async def go():
        first = await r.snapshot()
        second = await r.snapshot()
        assert second["cached"] is True and len(probes) == 2
        now["t"] = 31.0
        third = await r.snapshot()
        assert third["cached"] is False and len(probes) == 4
        return first, third

    first, third = run(go())
    assert first["endpoints"]["stt"]["state"] == "ready"
    assert first["endpoints"]["tts"]["state"] == "unknown" and first["ready"] is False


def test_readiness_probe_timeout_is_unknown_not_an_error():
    async def hang(_name):
        await asyncio.sleep(10)
        return "ready"

    r = EndpointReadiness({"stt": "a"}, hang, timeout_s=0.05)
    snap = run(r.snapshot())
    assert snap["endpoints"]["stt"]["state"] == "unknown"


def test_local_readiness_probes_nothing():
    r = EndpointReadiness({"stt": "a", "tts": "b"}, None)
    snap = run(r.snapshot())
    assert snap["ready"] is True and r.probes == 0
    assert snap["endpoints"]["tts"]["state"] == "local"


def test_a_wav_turn_longer_than_the_limit_is_rejected_before_any_turn_starts():
    vs, _ = make_voice_session(FakeVoiceLlm(["x"]), voice_max_turn_audio_s=0.05)
    with pytest.raises(ValueError, match="too long"):
        vs.submit_audio(wav_bytes(0.1))
    assert vs.turn == 0


def test_logs_carry_session_and_turn_ids_and_never_the_transcript(caplog):
    caplog.set_level(logging.INFO, logger="sidekick.voice")
    vs, _ = make_voice_session(FakeVoiceLlm(["Sure."]))

    async def go():
        vs.submit_text("my private fee question about the account")
        return await until_turn_done(vs, 1)

    run(go())
    records = [r for r in caplog.records if r.name.startswith("sidekick.voice")]
    structured = [json.loads(r.getMessage()) for r in records if r.getMessage().startswith("{")]
    starts = [s for s in structured if s["event"] == "voice.turn_start"]
    assert starts and starts[0]["session_id"] == "s-1" and starts[0]["turn"] == 1
    assert any(s["event"] == "voice.turn_done" and s["turn"] == 1 for s in structured)
    info_text = " ".join(r.getMessage() for r in records if r.levelno >= logging.INFO)
    assert "private fee question" not in info_text



def test_interrupt_purges_queued_audio_of_the_cut_turn():
    """Barge-in on the deployed app still delivered 37 queued audio chunks; they must be dropped server-side."""

    async def scenario():
        llm = FakeVoiceLlm(["One. ", "Two. ", "Three. ", "Four. ", "Five."])
        vs = make_voice_session(llm, tts=FakeTts(chunks=6, delay_for=lambda i, t: 0.05 * i))[0]
        vs.submit_text("tell me a long story")
        for _ in range(400):  # no socket reader: audio piles up in the outbound queue
            await asyncio.sleep(0.005)
            if sum(1 for e in list(vs.out._queue) if isinstance(e, dict) and e.get("type") == "audio") >= 3:
                break
        assert any(isinstance(e, dict) and e.get("type") == "audio" for e in list(vs.out._queue))
        turn = vs._current.turn
        vs.interrupt()
        events = await until_turn_done(vs, turn)  # everything the socket would still send after the interrupt
        assert not [e for e in events if e.get("type") == "audio" and e.get("turn") == turn]
        assert any(e.get("type") == "turn_done" and e.get("interrupted") for e in events)

    run(scenario())
