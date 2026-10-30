"""Live analysis while the user talks (SPEC 9.7): debounce, word floor, rate limit, cancellation, fail-soft, pins."""

import asyncio

import pytest
from enrich.rules import load_rules
from llm import LlmUnavailable
from voice.live import MAX_SUGGESTIONS, LiveAnalyzer, parse_suggestions
from voice_fakes import FakeVoiceLlm, events_of, make_voice_session, registry

RULES = load_rules()
PB = registry().get("advisor_coach")


def analyzer(events, llm=None, role="assistant", **kw):
    opts = dict(session_id="s-1", role=role, playbook_title=PB.title,
                methodology=[(m.key, m.label) for m in PB.methodology], rules=RULES, llm=llm,
                fast_model="fake-fast", emit=events.append, debounce_s=0.05, min_new_words=4,
                suggest_interval_s=0.3, suggest_timeout_s=1.5, max_tokens=220)
    opts.update(kw)
    return LiveAnalyzer(**opts)


def run(coro):
    return asyncio.run(coro)


def live_events(events, kind=None):
    return [e for e in events if e["type"] == "live" and (kind is None or e["kind"] == kind)]


def test_partials_are_debounced_and_need_four_new_words():
    events = []
    a = analyzer(events)

    async def go():
        a.on_partial(1, "what", "user")
        a.on_partial(1, "what are", "user")
        a.on_partial(1, "what are your", "user")
        await asyncio.sleep(0.12)
        assert live_events(events, "partial") == [], "three words is below the floor"
        a.on_partial(1, "what are your fees today", "user")
        await asyncio.sleep(0.12)
        assert len(live_events(events, "partial")) == 1
        # A burst of partials inside one debounce window is one analysis, not one per partial.
        for n in range(6, 12):
            a.on_partial(1, " ".join(["word"] * n), "user")
        await asyncio.sleep(0.12)
        assert len(live_events(events, "partial")) == 2
        a.close()

    run(go())


def test_a_final_is_analysed_at_once_and_drops_the_pending_partial():
    events = []
    a = analyzer(events, llm=None)

    async def go():
        a.on_partial(1, "i am really worried about the fees", "user")
        a.on_final(1, "i am really worried about the fees", "user")
        await asyncio.sleep(0.12)
        finals = live_events(events, "final")
        assert len(finals) == 1 and finals[0]["sentiment"] < 0
        assert live_events(events, "partial") == [], "the final superseded the pending partial"
        a.close()

    run(go())


def test_rules_give_signals_and_assistant_coverage_over_the_users_speech():
    events = []
    a = analyzer(events, llm=None)
    a.on_final(1, "We want to retire at 62 and the budget is tight. Is the fee worth it?", "user")
    ev = live_events(events, "final")[0]
    assert "question" in ev["signals"]
    assert "method:goals" in ev["signals"] and "method:budget" in ev["signals"]
    assert ev["coverage"] is not None
    covered = {c["key"] for c in ev["coverage"] if c["covered"]}
    assert {"goals", "budget"} <= covered
    a.close()


def test_practice_mode_leaves_coverage_to_the_call_pipeline():
    events = []
    a = analyzer(events, llm=None, role="practice")
    a.on_final(1, "We want to retire at 62", "agent")
    assert live_events(events)[0]["coverage"] is None
    a.close()


def test_suggestions_are_rate_limited_and_a_final_cancels_the_pass_in_flight():
    events = []
    llm = FakeVoiceLlm(["unused"], suggest_delay=0.05)
    a = analyzer(events, llm=llm, suggest_interval_s=0.3)

    async def go():
        a.on_final(1, "what does it cost", "user")          # first pass starts at once
        await asyncio.sleep(0.01)                           # ...and is in flight (the call takes 50 ms)
        a.on_final(1, "and what about the fee", "user")     # cancels it, rate limit closed: deferred
        a.on_final(1, "so what is the fee exactly", "user") # replaces the deferred request
        await asyncio.sleep(0.1)
        assert llm.suggest_calls == 1, "the second and third finals do not start a call while the slot is closed"
        assert llm.suggest_cancelled == 1, "the in-flight pass was cancelled by the newer final"
        await asyncio.sleep(0.6)
        assert llm.suggest_calls == 2, "exactly one deferred pass runs, at the first open slot"
        a.close()

    run(go())
    chips = [e for e in events if e["type"] == "suggestion"]
    assert chips, "the deferred pass produced chips"
    assert "so what is the fee exactly" in llm.suggest_messages[-1][1]["content"]


def test_a_partial_does_not_stack_a_second_pass_on_one_in_flight():
    events = []
    llm = FakeVoiceLlm(["unused"], suggest_delay=0.2)
    a = analyzer(events, llm=llm)

    async def go():
        a.on_final(1, "the fees are high for what i get", "user")
        a.on_partial(2, "the fees are high for what i get back each year", "user")
        await asyncio.sleep(0.12)
        assert llm.suggest_calls == 1
        a.close()

    run(go())


def test_a_slow_or_failing_suggestion_is_fail_soft():
    events = []

    class Broken(FakeVoiceLlm):
        async def chat(self, model, messages, **kw):
            raise LlmUnavailable("serving down")

    a = analyzer(events, llm=Broken(["x"]))
    slow_events = []
    b = analyzer(slow_events, llm=FakeVoiceLlm(["x"], suggest_delay=3.0), suggest_timeout_s=0.1)

    async def go():
        a.on_final(1, "what are the fees", "user")
        await asyncio.sleep(0.05)
        assert [e for e in events if e["type"] == "suggestion"] == []
        assert a.suggest_failures == 1
        a.close()
        b.on_final(1, "what are the fees", "user")
        await asyncio.sleep(0.4)
        assert b.suggest_failures == 1, "the 0.1 s budget cut the slow call short"
        b.close()

    run(go())
    assert not [e for e in slow_events if e["type"] == "suggestion"]


def test_parse_suggestions_is_tolerant_and_capped():
    raw = ('```json\n{"suggestions": [{"kind": "next_step", "text": "Ask about the time horizon."},'
           '{"kind": "bogus", "text": "dropped"},'
           '{"kind": "risk", "text": "Ask about the time horizon."},'
           '{"kind": "note", "text": "Confirm the 401k."},'
           '{"kind": "note", "text": "Third one is cut."}]}\n```')
    out = parse_suggestions(raw, session_id="s", turn=3, now=7)
    assert [o["kind"] for o in out] == ["next_step", "note"][:MAX_SUGGESTIONS]
    assert all(o["provisional"] is True and o["turn"] == 3 and o["created_at_ms"] == 7 for o in out)
    assert parse_suggestions("not json at all", session_id="s", turn=1, now=0) == []
    assert parse_suggestions('{"suggestions": "nope"}', session_id="s", turn=1, now=0) == []


def test_pins_and_dismissals_take_chips_out_of_the_live_lane():
    events = []
    llm = FakeVoiceLlm(["unused"], suggestions=[{"kind": "next_step", "text": "Ask about the 401k."},
                                                {"kind": "risk", "text": "Fee objection is coming."}])
    a = analyzer(events, llm=llm)

    async def go():
        a.on_final(1, "tell me about the fees", "user")
        await asyncio.sleep(0.1)
        ids = [e["item"]["item_id"] for e in events if e["type"] == "suggestion"]
        assert len(ids) == 2 and all(i in a.provisional for i in ids)
        taken = a.take(ids[0])
        assert taken["kind"] == "next_step" and ids[0] not in a.provisional
        with pytest.raises(ValueError):
            a.take(ids[0])                                  # a pin is one-shot
        assert a.drop(ids[1]) is True and a.drop(ids[1]) is False
        a.close()

    run(go())


def test_a_new_pass_supersedes_the_unpinned_chips_of_the_same_turn_only():
    events = []
    llm = FakeVoiceLlm(["unused"], suggestions=[{"kind": "note", "text": "Check the rollover IRA."}])
    a = analyzer(events, llm=llm, suggest_interval_s=0.0)

    async def go():
        a.on_final(1, "first thought", "user")
        await asyncio.sleep(0.05)
        a.on_final(2, "a different turn", "user")
        await asyncio.sleep(0.05)
        a.on_final(2, "second thought on turn two", "user")
        await asyncio.sleep(0.05)
        a.close()

    run(go())
    retire = [e for e in events if e["type"] == "suggestion_retire"]
    assert retire and all(r["reason"] == "superseded" for r in retire)
    assert {r["turn"] for r in retire} == {2}, "turn 1 chips are not touched by turn 2's pass"


def test_the_end_of_the_turn_retires_what_was_not_pinned():
    events = []
    llm = FakeVoiceLlm(["unused"], suggestions=[{"kind": "follow_up_question", "text": "Ask about the cost."}])
    a = analyzer(events, llm=llm)

    async def go():
        a.on_final(4, "what do they pay today", "user")
        await asyncio.sleep(0.05)
        a.retire_turn(4)
        a.close()

    run(go())
    ret = [e for e in events if e["type"] == "suggestion_retire"][-1]
    assert ret["reason"] == "turn_done" and ret["turn"] == 4 and len(ret["item_ids"]) == 1
    assert a.provisional == {}


def test_sentiment_series_is_rolling_and_keeps_partial_snapshots():
    events = []
    a = analyzer(events, llm=None)
    for i in range(320):
        a.on_final(i, "good thanks", "user")
    assert len(a.series) == 300
    assert a.series[-1]["kind"] == "final" and a.series[-1]["value"] > 0


def test_practice_customer_reply_is_analysed_without_a_suggestion_pass():
    events = []
    llm = FakeVoiceLlm(["unused"])
    a = analyzer(events, llm=llm, role="practice")
    a.on_final(2, "honestly the fees worry me", "customer", suggest=False)
    run(asyncio.sleep(0.05))
    assert llm.suggest_calls == 0
    assert live_events(events, "final")[0]["speaker"] == "customer"
    a.close()


def test_session_pin_persists_the_item_and_dismiss_drops_the_chip():
    llm = FakeVoiceLlm(["unused"], suggestions=[{"kind": "next_step", "text": "Ask what result matters most."},
                                                {"kind": "risk", "text": "Fee objection is likely."}],
                       )
    vs, store = make_voice_session(llm, live_suggest_interval_s=0.0)

    async def go():
        vs.live.on_final(1, "what are the fees", "user")
        seen = []
        while len(events_of(seen, "suggestion")) < 2:
            seen.append(await asyncio.wait_for(vs.out.get(), 5))
        chips = events_of(seen, "suggestion")
        pinned_id = chips[0]["item"]["item_id"]
        dismissed_id = chips[1]["item"]["item_id"]
        item = await vs.pin(pinned_id)
        vs.dismiss(dismissed_id)
        out = []
        while not vs.out.empty():
            out.append(vs.out.get_nowait())
        assert [e for e in out if e["type"] == "item"][0]["item"]["item_id"] == pinned_id
        assert not vs.live.provisional, "pinned and dismissed chips both leave the live lane"
        assert item["kind"] == "next_step" and "provisional" not in item
        snap = await store.voice_snapshot(vs.session_id)
        assert [i["item_id"] for i in snap["items"]] == [pinned_id]
        assert vs.snapshot()["items"][0]["item_id"] == pinned_id
        with pytest.raises(ValueError):
            await vs.pin(dismissed_id)
        vs.live.close()

    run(go())
