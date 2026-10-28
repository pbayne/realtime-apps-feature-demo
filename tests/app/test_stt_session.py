"""StreamingSession end-to-end on synthetic frames: partials, finals, endpointing, fail-soft decode."""

import asyncio
import time

from llm import LlmUnavailable
from stt.base import Frame, Window, Word
from stt.parakeet import ParakeetDecoder, StreamingSession
from stt.vad import EnergyVad


class ScriptedDecoder:
    """Words become visible `lag` ms after they end (like replay), so partials grow."""

    def __init__(self, words, lag=250):
        self.words, self.lag = words, lag

    async def decode(self, window: Window):
        limit = window.end_ms if window.final else window.end_ms - self.lag
        return [w for w in self.words if w.s >= window.start_ms - 1 and w.e <= limit]


async def frames_for(speech_ms_ranges, total_ms):
    t = 0
    while t < total_ms:
        speech = any(a <= t < b for a, b in speech_ms_ranges)
        yield Frame(t_ms=float(t), wall=time.monotonic(), dur_ms=100, speech=speech)
        t += 100
        await asyncio.sleep(0)


def run_session(session, ranges, total):
    async def go():
        return [ev async for ev in session.run(frames_for(ranges, total))]
    return asyncio.run(go())


def test_partials_then_one_final_with_all_words():
    # Two utterances separated by 1.2 s of silence (> endpoint_silence_ms), so two finals.
    words = [
        Word("hello", 0, 400), Word("there", 400, 800), Word("good", 800, 1200), Word("morning", 1200, 1700),
        Word("fee", 3000, 3300), Word("question", 3300, 3900), Word("today", 3900, 4300),
    ]
    seq = iter(range(1, 100))
    session = StreamingSession("customer", ScriptedDecoder(words), EnergyVad(), lambda: next(seq),
                               frame_s=0.4, window_s=12, hold_words=1, endpoint_silence_ms=600)
    events = run_session(session, [(0, 2000), (3000, 4400)], total=5200)
    partials = [e for e in events if e.kind == "partial"]
    finals = [e for e in events if e.kind == "final"]
    assert partials, "expected partial events while speaking"
    assert len(finals) == 2
    assert [f.seq for f in finals] == [1, 2]
    assert finals[0].text == "hello there good morning"
    assert finals[1].text == "fee question today"
    assert all(p.seq in (1, 2) for p in partials)
    assert finals[0].first_word_ms is not None and finals[0].commit_ms is not None


def test_silence_only_produces_nothing():
    session = StreamingSession("agent", ScriptedDecoder([]), EnergyVad(), iter(range(1, 9)).__next__)
    assert run_session(session, [], total=2000) == []


def test_stream_ending_mid_utterance_flushes_final():
    words = [Word("ok", 0, 300)]
    session = StreamingSession("agent", ScriptedDecoder(words), EnergyVad(), iter(range(1, 9)).__next__,
                               frame_s=0.4, endpoint_silence_ms=600)
    events = run_session(session, [(0, 1000)], total=1000)
    assert events[-1].kind == "final" and events[-1].text == "ok"


def test_frame_floor_enforced():
    try:
        StreamingSession("agent", ScriptedDecoder([]), EnergyVad(), lambda: 1, frame_s=0.2)
    except ValueError:
        return
    raise AssertionError("frame_s below 0.3 must be rejected")


def test_energy_vad_thresholds():
    silence = b"\x00\x00" * 1600
    loud = (b"\xe8\x03" * 1600)  # int16 1000 across the frame
    vad = EnergyVad(rms_threshold=400)
    assert vad.is_speech(silence) is False
    assert vad.is_speech(loud) is True
    assert vad.is_speech(b"") is False


class _DownLlm:
    async def invoke(self, endpoint, payload, timeout=None, url=None):
        raise LlmUnavailable("down")


def test_parakeet_decode_is_fail_soft():
    dec = ParakeetDecoder(_DownLlm(), "parakeet-live")
    words = asyncio.run(dec.decode(Window(0, 1000, b"\x00\x00" * 16000, final=False)))
    assert words == []


class _FakeLlm:
    def __init__(self, body):
        self.body = body
        self.payload = None

    async def invoke(self, endpoint, payload, timeout=None, url=None):
        self.payload = payload
        return self.body


def test_parakeet_parses_word_timestamps_as_absolute_call_time():
    """The endpoint adds offset_ms (register_parakeet.py), so word times come back on the call timeline."""
    llm = _FakeLlm({"predictions": [{"words": [{"word": "fees", "start_ms": 5100, "end_ms": 5400},
                                               {"word": "apply", "start_ms": 5450, "end_ms": 5800}]}]})
    dec = ParakeetDecoder(llm, "parakeet-live", channel="customer")
    words = asyncio.run(dec.decode(Window(5000, 6000, b"\x00\x00" * 16000, final=False)))
    assert [(w.w, w.s, w.e) for w in words] == [("fees", 5100, 5400), ("apply", 5450, 5800)]
    record = llm.payload["dataframe_records"][0]
    assert set(record) == {"audio_b64", "offset_ms", "channel"}
    assert record["offset_ms"] == 5000 and record["channel"] == "customer"


def test_parakeet_accepts_mlflow_wrapped_predictions():
    llm = _FakeLlm({"predictions": {"predictions": [{"words": [{"word": "hi", "start_ms": 10, "end_ms": 90}]}]}})
    words = asyncio.run(ParakeetDecoder(llm, "p").decode(Window(0, 1000, b"\x00\x00" * 8000, final=True)))
    assert [(w.w, w.s, w.e) for w in words] == [("hi", 10, 90)]


def test_parakeet_text_only_response_spreads_words_evenly():
    llm = _FakeLlm({"predictions": [{"text": "one two three four"}]})
    words = asyncio.run(ParakeetDecoder(llm, "p").decode(Window(0, 1000, b"\x00\x00" * 8000, final=True)))
    assert [w.w for w in words] == ["one", "two", "three", "four"]
    assert words[0].s == 0 and words[-1].e == 1000


def test_word_starting_just_before_vad_onset_is_not_clipped():
    # Speech flagged from frame 1000, but the first word starts at 940 (off the 100 ms grid).
    words = [Word("That's", 940, 1200), Word("helpful.", 1250, 1700)]
    session = StreamingSession("agent", ScriptedDecoder(words), EnergyVad(), iter(range(100)).__next__)
    events = run_session(session, [(1000, 1800)], 3500)
    finals = [e for e in events if e.kind == "final"]
    assert [e.text for e in finals] == ["That's helpful."]


def test_replay_scripts_round_trip_every_word():
    from pathlib import Path

    from stt.replay import ReplayFrameSource, ScriptDecoder, load_script

    replay_dir = Path(__file__).resolve().parents[2] / "src" / "app" / "data" / "replay"
    for path in sorted(replay_dir.glob("*.jsonl")):
        script = load_script(path)
        for speaker in ("agent", "customer"):
            expected = [u.text for u in script.utterances if u.speaker == speaker]
            seqs = iter(range(10_000))
            session = StreamingSession(speaker, ScriptDecoder(script.words_for(speaker)), EnergyVad(), seqs.__next__)
            source = ReplayFrameSource(script, speaker, speed=10_000)

            async def go():
                return [ev.text async for ev in session.run(source.frames()) if ev.kind == "final"]

            assert asyncio.run(go()) == expected, f"{path.name}/{speaker}"
