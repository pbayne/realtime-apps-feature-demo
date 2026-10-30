"""Streaming voice input (SPEC 9.2, streaming): PCM frames in, partials out, endpoint starts the reply turn.
Streaming STT is a scripted decoder and the reply LLM is a fake, so these run offline."""

import asyncio

import pytest
from stt.base import Word
from voice_fakes import FakeVoiceLlm, events_of, make_voice_session, pcm_ms

WORDS = [Word("what", 0, 400), Word("are", 400, 800), Word("your", 800, 1100), Word("fees", 1100, 1500)]


def run(coro):
    return asyncio.run(coro)


async def drain_until(vs, pred, timeout=10.0):
    """Read the session's outbound queue until `pred(events)` is true. Returns every event read."""
    seen = []
    deadline = asyncio.get_event_loop().time() + timeout
    while not pred(seen):
        remaining = deadline - asyncio.get_event_loop().time()
        if remaining <= 0:
            raise AssertionError(f"timed out; saw {[e['type'] for e in seen]}")
        seen.append(await asyncio.wait_for(vs.out.get(), remaining))
    return seen


def turn_done(turn):
    return lambda evs: any(e["type"] == "turn_done" and e["turn"] == turn for e in evs)


def test_streamed_speech_emits_partials_then_endpoint_starts_the_reply_turn():
    llm = FakeVoiceLlm(["Sure. ", "The fee is one percent."])
    vs, _ = make_voice_session(llm, stream_words=WORDS)

    async def go():
        vs.stream_start()
        # 1.5 s of speech, then 1.2 s of silence: the 700 ms endpoint fires inside the silence.
        for _ in range(15):
            vs.stream_frame(pcm_ms(100, True))
            await asyncio.sleep(0)
        for _ in range(12):
            vs.stream_frame(pcm_ms(100, False))
            await asyncio.sleep(0)
        return await drain_until(vs, turn_done(1))

    events = run(go())
    kinds = [e["type"] for e in events]
    assert kinds[0] == "stream_started" and events[0]["endpoint_silence_ms"] == 700
    partials = events_of(events, "user_partial")
    assert partials, "partials are emitted while the user is still talking"
    assert all(p["turn"] == 1 for p in partials), "a partial carries the turn its endpoint will start"
    user_text = events_of(events, "user_text")
    assert len(user_text) == 1 and user_text[0]["turn"] == 1 and user_text[0]["text"] == "what are your fees"
    assert kinds.index("user_partial") < kinds.index("user_text") < kinds.index("turn_done")
    assert events_of(events, "agent_text"), "the endpoint starts the normal reply"
    assert vs.turn == 1 and vs.turns[0]["user_text"] == "what are your fees"


def test_stream_stop_with_flush_finalises_the_open_utterance_without_silence():
    words = [Word("hello", 0, 500), Word("there", 500, 900)]
    vs, _ = make_voice_session(FakeVoiceLlm(["Hi."]), stream_words=words)

    async def go():
        vs.stream_start()
        for _ in range(9):
            vs.stream_frame(pcm_ms(100, True))
            await asyncio.sleep(0)
        vs.stream_stop(flush=True)           # push-to-talk release
        return await drain_until(vs, turn_done(1))

    events = run(go())
    assert events_of(events, "user_text")[0]["text"] == "hello there"
    assert events_of(events, "stream_stopped")[0]["flush"] is True


def test_stream_stop_without_flush_discards_the_utterance():
    vs, _ = make_voice_session(FakeVoiceLlm(["unused"]), stream_words=WORDS)

    async def go():
        vs.stream_start()
        for _ in range(8):
            vs.stream_frame(pcm_ms(100, True))
            await asyncio.sleep(0)
        vs.stream_stop(flush=False)           # mute: nothing is sent to the reply
        await asyncio.sleep(0.2)
        out = []
        while not vs.out.empty():
            out.append(vs.out.get_nowait())
        return out

    events = run(go())
    assert not events_of(events, "user_text") and not events_of(events, "turn_done")
    assert vs.turn == 0 and vs.streaming is False


def test_frames_need_an_open_stream_and_whole_pcm_samples():
    vs, _ = make_voice_session(FakeVoiceLlm(["x"]), stream_words=WORDS)

    async def go():
        with pytest.raises(ValueError, match="WAV"):
            vs.stream_frame(pcm_ms(100, True))            # no stream_start yet
        vs.stream_start()
        with pytest.raises(ValueError, match="whole PCM16"):
            vs.stream_frame(b"\x01\x02\x03")
        with pytest.raises(ValueError, match="too large"):
            vs.stream_frame(pcm_ms(1500, False))         # 1.5 s in one frame
        with pytest.raises(ValueError, match="sample_rate"):
            vs.stream_start(sample_rate=48000)
        with pytest.raises(ValueError, match="endpoint_silence_ms"):
            vs.stream_start(endpoint_silence_ms=5000)
        vs.abort_stream()

    run(go())


def test_a_full_queue_drops_frames_and_counts_them_instead_of_blocking():
    vs, _ = make_voice_session(FakeVoiceLlm(["x"]), stream_words=WORDS, voice_stream_queue_max=2)

    async def go():
        vs.stream_start()
        for _ in range(6):                            # the consumer has not run yet: the queue holds two
            vs.stream_frame(pcm_ms(100, True))
        dropped = vs.dropped_frames
        vs.abort_stream()
        await asyncio.sleep(0)
        return dropped

    assert run(go()) == 4
    assert vs.dropped_frames == 4


def test_speech_longer_than_the_turn_limit_is_force_ended_with_a_final():
    words = [Word("one", 0, 400), Word("two", 400, 800), Word("three", 800, 1200), Word("four", 1200, 1600)]
    vs, _ = make_voice_session(FakeVoiceLlm(["Okay."]), stream_words=words, voice_max_turn_audio_s=1.0)

    async def go():
        vs.stream_start()
        for _ in range(20):                           # 2 s of unbroken speech against a 1 s turn limit
            vs.stream_frame(pcm_ms(100, True))
            await asyncio.sleep(0)
        events = await drain_until(vs, turn_done(1))
        vs.abort_stream()
        return events

    events = run(go())
    user_text = events_of(events, "user_text")
    assert user_text and user_text[0]["turn"] == 1 and "one two" in user_text[0]["text"]


def test_turn_numbers_continue_after_a_reply():
    # The second utterance opens at about 1.4 s (after the first endpoint), so its word must sit inside that window.
    words = [Word("first", 0, 400), Word("second", 1400, 1800)]
    vs, _ = make_voice_session(FakeVoiceLlm(["Right."]), stream_words=words)

    async def go():
        vs.stream_start()
        for _ in range(4):
            vs.stream_frame(pcm_ms(100, True))
            await asyncio.sleep(0)
        for _ in range(10):
            vs.stream_frame(pcm_ms(100, False))
            await asyncio.sleep(0)
        await drain_until(vs, turn_done(1))
        for _ in range(4):
            vs.stream_frame(pcm_ms(100, True))
            await asyncio.sleep(0)
        for _ in range(10):
            vs.stream_frame(pcm_ms(100, False))
            await asyncio.sleep(0)
        events = await drain_until(vs, turn_done(2))
        vs.abort_stream()
        return events

    events = run(go())
    assert vs.turn == 2
    assert [e["turn"] for e in events_of(events, "user_text")] == [2]


def test_practice_stream_injects_the_rep_line_into_the_call_pipeline():
    injected = []

    class CallStub:
        def inject_final(self, speaker, text):
            injected.append((speaker, text))

        def coverage_event(self):
            return {"type": "coverage", "items": []}

    vs, _ = make_voice_session(FakeVoiceLlm(["Mm."]), stream_words=WORDS, role="practice", call_id="call-1")
    vs.deps.calls["call-1"] = CallStub()

    async def go():
        vs.stream_start()
        for _ in range(15):
            vs.stream_frame(pcm_ms(100, True))
            await asyncio.sleep(0)
        for _ in range(12):
            vs.stream_frame(pcm_ms(100, False))
            await asyncio.sleep(0)
        events = await drain_until(vs, turn_done(1))
        vs.abort_stream()
        return events

    run(go())
    assert ("agent", "what are your fees") in injected
