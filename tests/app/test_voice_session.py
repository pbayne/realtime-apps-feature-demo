"""VoiceSession end-to-end with a scripted LLM, fake STT and fake Kokoro (SPEC 9.2 to 9.4). No network."""

import asyncio
import base64
from pathlib import Path

import pytest
from config import Settings
from enrich.rules import load_rules
from sidekick.playbooks import PlaybookRegistry
from sidekick.retrieval import Retriever
from store.memory import MemoryStore
from voice.session import VoiceDeps, VoiceSession
from voice_fakes import FakeStt, FakeTts, FakeVoiceLlm, events_of, wav_bytes

APP = Path(__file__).resolve().parents[2] / "src" / "app"
REGISTRY = PlaybookRegistry.load(APP / "playbooks", load_rules(), APP / "data" / "replay")
ITEMS = '[{"kind": "action_item", "text": "Send the fee schedule", "owner": "advisor"}]'


def make_session(llm, tts=None, stt=None, *, role="assistant", call_id=None, scenario=None, store=None, **settings):
    s = Settings(mode="local", llm_enabled=True, retrieval_backend="keyword", **settings)
    store = store or MemoryStore()

    async def no_finish(_):
        return None

    async def no_new(_):
        raise AssertionError("no practice call in this test")

    deps = VoiceDeps(settings=s, llm=llm, tts=tts or FakeTts(), stt=stt or FakeStt(), store=store,
                     retriever=Retriever(llm, s, None, backend="keyword"), calls={},
                     finish_call=no_finish, new_practice_call=no_new)
    pb = REGISTRY.get("advisor_coach")
    sc = pb.scenarios[0] if role == "practice" else None
    return VoiceSession(session_id="s-1", role=role, playbook=pb, voice="af_heart", deps=deps,
                        call_id=call_id, scenario=sc), store


async def until_turn_done(vs, turn, timeout=10.0):
    out = []
    while True:
        ev = await asyncio.wait_for(vs.out.get(), timeout)
        out.append(ev)
        if ev["type"] == "turn_done" and ev["turn"] == turn:
            return out


def run(coro):
    return asyncio.run(coro)


def test_turn_orders_audio_emits_items_and_reports_timing():
    script = ["Sure. ", "Here are two points for you. ", "Keep them short. ", "<<ITE", "MS>>\n", ITEMS]
    llm = FakeVoiceLlm(script)
    # Later sentences finish first: playback order must still be sentence order.
    tts = FakeTts(delay_for=lambda i, text: (3 - i) * 0.03)
    vs, store = make_session(llm, tts, FakeStt("what are your fees"))

    async def go():
        vs.submit_audio(wav_bytes())
        return await until_turn_done(vs, 1)

    events = run(go())
    types = [e["type"] for e in events]
    assert types[0] == "user_text" and events[0]["text"] == "what are your fees" and events[0]["turn"] == 1
    assert events[0]["stt_ms"] >= 0
    deltas = "".join(e["delta"] for e in events_of(events, "agent_text"))
    assert deltas.strip() == "Sure. Here are two points for you. Keep them short."
    audio = events_of(events, "audio")
    assert [a["seq"] for a in audio] == list(range(len(audio)))
    # Sentence index is the first byte of each chunk; chunks of one sentence are contiguous and in order.
    order = [base64.b64decode(a["pcm_b64"])[0] for a in audio]
    assert order == sorted(order), order
    assert all(a["rate"] == 24000 and a["turn"] == 1 for a in audio)
    items = events_of(events, "item")
    assert [i["item"]["kind"] for i in items] == ["action_item"]
    assert items[0]["item"]["owner"] == "advisor"
    # items come after the spoken reply's audio
    assert types.index("item") > max(i for i, t in enumerate(types) if t == "audio")
    done = events[-1]
    assert done["type"] == "turn_done" and done["interrupted"] is False
    timing = done["timing"]
    for key in ("stt_ms", "llm_first_token_ms", "tts_first_audio_ms", "first_audio_ms", "total_ms"):
        assert key in timing and timing[key] is not None, key
    assert timing["first_audio_ms"] <= timing["total_ms"]
    assert "item" in types and not any(e["type"] == "error" for e in events)
    assert all(r["closed"] for r in tts.opened), "every TTS stream is closed"
    assert llm.streams[-1].closed is True

    snap = run(store.voice_snapshot("s-1"))
    assert snap["turns"][0]["agent_text"].startswith("Sure.")
    assert snap["items"][0]["text"] == "Send the fee schedule"


def test_delimiter_split_across_tokens_never_leaks_into_speech():
    script = ["Right. ", "<", "<ITEMS", ">", ">", "[]"]
    llm = FakeVoiceLlm(script)
    vs, _ = make_session(llm)

    async def go():
        vs.submit_text("hi")
        return await until_turn_done(vs, 1)

    events = run(go())
    spoken = "".join(e["delta"] for e in events_of(events, "agent_text"))
    assert "<" not in spoken and "ITEMS" not in spoken and spoken.strip() == "Right."
    assert events_of(events, "item") == []


def test_typed_turn_emits_user_text_with_turn_number_and_no_stt():
    llm = FakeVoiceLlm(["Okay. ", "Here you go."])
    stt = FakeStt()
    vs, _ = make_session(llm)
    vs.deps.stt = stt

    async def go():
        vs.submit_text("  summarise the call  ")
        first = await until_turn_done(vs, 1)
        vs.submit_text("and the next step")
        second = await until_turn_done(vs, 2)
        return first, second

    first, second = run(go())
    assert stt.calls == 0
    assert first[0] == {**first[0], "type": "user_text", "turn": 1, "text": "summarise the call", "stt_ms": 0}
    assert second[0]["type"] == "user_text" and second[0]["turn"] == 2 and second[0]["text"] == "and the next step"
    assert [m["role"] for m in vs.history] == ["user", "assistant", "user", "assistant"]


def test_interrupt_cancels_llm_and_tts_closes_streams_and_marks_turn_interrupted():
    script = ["First sentence is here. ", "Second one follows after a pause. ", "And a third. "]
    llm = FakeVoiceLlm(script, voice_delay=0.4)
    tts = FakeTts(hang=0.5, chunks=20)
    vs, store = make_session(llm, tts)

    async def go():
        vs.submit_text("tell me about fees")
        seen = []
        while True:
            ev = await asyncio.wait_for(vs.out.get(), 10)
            seen.append(ev)
            if ev["type"] == "agent_text":
                vs.interrupt()
                break
        rest = await until_turn_done(vs, 1)
        return seen + rest

    events = run(go())
    done = events[-1]
    assert done["type"] == "turn_done" and done["interrupted"] is True
    assert llm.streams[0].closed is True, "LLM stream closed on interrupt"
    assert tts.opened, "at least one TTS stream was started"
    assert all(r["closed"] for r in tts.opened), "in-flight TTS streams are closed (SPEC 3.3a)"
    turns = run(store.voice_snapshot("s-1"))["turns"]
    assert turns[0]["interrupted"] is True


def test_interrupt_after_turn_done_is_ignored():
    llm = FakeVoiceLlm(["Done. "])
    vs, store = make_session(llm)

    async def go():
        vs.submit_text("hello")
        events = await until_turn_done(vs, 1)
        vs.interrupt()           # arrives after turn_done: no effect
        await asyncio.sleep(0.05)
        return events

    events = run(go())
    assert events[-1]["interrupted"] is False
    assert vs.turns and vs.turns[0]["interrupted"] is False
    assert not vs.busy


def test_tts_failure_keeps_the_text_and_reports_stage_with_turn():
    llm = FakeVoiceLlm(["Sure thing. ", "More text here."])
    vs, _ = make_session(llm, FakeTts(fail=True))

    async def go():
        vs.submit_text("ok")
        return await until_turn_done(vs, 1)

    events = run(go())
    errors = events_of(events, "error")
    assert errors and all(e["stage"] == "tts" and e["turn"] == 1 for e in errors)
    assert "Sure thing." in "".join(e["delta"] for e in events_of(events, "agent_text"))
    assert events[-1]["type"] == "turn_done" and events[-1]["interrupted"] is False


def test_llm_failure_is_reported_with_stage_and_turn_and_session_continues():
    llm = FakeVoiceLlm(["unused"], fail=True)
    vs, _ = make_session(llm)

    async def go():
        vs.submit_text("first")
        first = await until_turn_done(vs, 1)
        llm.fail = False
        llm.voice_script = ["Recovered. "]
        vs.submit_text("second")
        second = await until_turn_done(vs, 2)
        return first, second

    first, second = run(go())
    err = events_of(first, "error")
    assert err and err[0]["stage"] == "llm" and err[0]["turn"] == 1
    assert "Recovered." in "".join(e["delta"] for e in events_of(second, "agent_text"))


def test_stt_failure_is_a_turn_error_not_a_crash():
    llm = FakeVoiceLlm(["unused"])
    vs, _ = make_session(llm, stt=FakeStt(fail=True))

    async def go():
        vs.submit_audio(wav_bytes())
        return await until_turn_done(vs, 1)

    events = run(go())
    err = events_of(events, "error")
    assert err and err[0]["stage"] == "stt" and err[0]["turn"] == 1
    assert llm.voice_calls == [], "no reply is generated when nothing was heard"


def test_bad_inputs_are_rejected_before_any_turn_starts():
    vs, _ = make_session(FakeVoiceLlm(["x"]))
    with pytest.raises(ValueError):
        vs.submit_audio(b"not a wav file at all")
    with pytest.raises(ValueError):
        vs.submit_text("   ")
    with pytest.raises(ValueError):
        vs.configure(voice="Heart!!")
    assert vs.turn == 0


def test_assistant_prompt_contains_kb_reference_and_rules():
    llm = FakeVoiceLlm(["Okay."])
    vs, _ = make_session(llm)

    async def go():
        vs.submit_text("what are the fee objections about a percent of assets")
        return await until_turn_done(vs, 1)

    run(go())
    system = llm.voice_calls[0][0]["content"]
    assert "Voice rules" in system and "<<ITEMS>>" in system
    assert "Playbook reference" in system and "fee" in system.lower()
    assert llm.voice_calls[0][-1] == {"role": "user", "content": "what are the fee objections about a percent of assets"}

