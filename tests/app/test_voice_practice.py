"""Practice mode (SPEC 9): the rep's turns and the AI customer's replies run through the normal call pipeline
(enrichment, decide, cards, coverage), and those call events reach the voice socket. Fake LLM, no network."""

import asyncio

from config import Settings
from services import Services
from voice_fakes import FakeStt, FakeTts, FakeVoiceLlm, events_of

CUSTOMER_REPLY = "That sounds way too expensive for me, honestly. "
PRACTICE_SCRIPT = [CUSTOMER_REPLY, "What do you actually charge each year? ", "<<ITEMS>>", "[]"]


async def _drain_until(vs, seen, pred, timeout=10.0):
    """Read the voice socket's events into `seen` (shared across calls) until pred(seen) holds."""
    deadline = asyncio.get_running_loop().time() + timeout
    while not pred(seen):
        left = deadline - asyncio.get_running_loop().time()
        if left <= 0:
            raise AssertionError(f"timed out; saw {[e['type'] for e in seen]}")
        seen.append(await asyncio.wait_for(vs.out.get(), left))
    return seen


def test_practice_turns_drive_call_events_and_cards_on_the_voice_socket():
    settings = Settings(mode="local", llm_enabled=True, retrieval_backend="keyword", debounce_ms=100, replay_speed=40)
    llm = FakeVoiceLlm(PRACTICE_SCRIPT, card_text="Acknowledge the fee concern and restate the fee plainly.")

    async def scenario():
        svc = Services(settings, llm=llm)
        svc.voice_deps.tts = FakeTts()
        svc.voice_deps.stt = FakeStt()
        await svc.start()
        try:
            vs = await svc.create_voice_session(role="practice", playbook="advisor_coach",
                                                scenario="fee_objection", call_id=None, voice=None)
            assert vs.call_id in svc.sessions and svc.sessions[vs.call_id].record.mode == "voice"
            vs.attach_call_events(svc.hub)
            vs.submit_text("Let me walk you through what the advisory service costs.")
            seen = []
            await _drain_until(vs, seen, lambda s: any(e["type"] == "turn_done" for e in s))

            def call_done(s):
                types = [e["type"] for e in s]
                return "card.done" in types and "coverage" in types

            await _drain_until(vs, seen, call_done, timeout=10)
            return vs, seen
        finally:
            await svc.stop()

    vs, seen = asyncio.run(scenario())
    finals = events_of(seen, "transcript.final")
    speakers = [(f["speaker"], f["text"]) for f in finals]
    assert ("agent", "Let me walk you through what the advisory service costs.") in speakers
    assert any(sp == "customer" and "too expensive" in text for sp, text in speakers)
    cards = events_of(seen, "card.done")
    assert cards, "at least one card was pushed on the voice socket"
    assert cards[0]["card"]["kind"]
    assert events_of(seen, "coverage")
    assert events_of(seen, "turn_done")[0]["interrupted"] is False
    audio = events_of(seen, "audio")
    assert audio and audio[0]["turn"] == 1
    assert any(e["type"] == "enrichment" for e in seen)


def test_practice_opening_line_is_spoken_and_recorded_as_customer_turn():
    settings = Settings(mode="local", llm_enabled=True, retrieval_backend="keyword", debounce_ms=100)
    llm = FakeVoiceLlm(["unused"])
    tts = FakeTts()

    async def scenario():
        svc = Services(settings, llm=llm)
        svc.voice_deps.tts = tts
        await svc.start()
        try:
            vs = await svc.create_voice_session(role="practice", playbook="contact_center",
                                                scenario="billing_dispute", call_id=None, voice=None)
            assert vs.voice == "af_nicole"   # the scenario picks its own voice unless overridden
            vs.attach_call_events(svc.hub)
            vs.start_opening()
            seen = await _drain_until(vs, [], lambda s: any(e["type"] == "turn_done" for e in s))
            call = svc.sessions[vs.call_id]
            return vs, seen, call
        finally:
            await svc.stop()

    vs, seen, call = asyncio.run(scenario())
    opening = vs.scenario.opening
    assert events_of(seen, "agent_text")[0]["turn"] == 0
    assert "".join(e["delta"] for e in events_of(seen, "agent_text")).strip() == opening
    assert any(f["speaker"] == "customer" and f["text"] == opening for f in events_of(seen, "transcript.final"))
    assert events_of(seen, "turn_done")[0]["turn"] == 0
    assert tts.sentences and tts.sentences[0].startswith("I was charged twice")
