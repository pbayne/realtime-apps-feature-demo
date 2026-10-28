"""Decision policy (SPEC section 4): silence, budget, cooldown, compliance bypass, dedupe."""

from config import get_settings
from copilot.decide import (
    Candidate,
    GuardConfig,
    GuardState,
    TurnContext,
    compliance_candidates,
    decide,
    grounded_confidence,
    turn_candidates,
)
from copilot.playbooks import PlaybookRegistry
from copilot.retrieval import Hit
from enrich.rules import load_rules

WEIGHTS = {"objection_pickup": 0.95, "suggested_response": 0.9, "next_question": 0.8, "compliance": 1.0,
           "missing_info": 0.75, "next_best_action": 0.7}
CFG = GuardConfig(min_confidence=0.6, cooldown_turns=3, budget=12)


def cand(kind="suggested_response", conf=0.9, trigger=None, bypass=False):
    return Candidate(kind=kind, title="t", trigger=trigger or f"{kind}:{conf}:{bypass}", confidence=conf,
                     source_doc=None, source_clause=None, hint="h", bypass=bypass)


def run(cands, state, customer_turn=True, cfg=CFG):
    return decide(cands, state, WEIGHTS, cfg, customer_turn=customer_turn)


def test_silence_when_nothing_passes_guardrails():
    state = GuardState(turn=1)
    d = run([cand(conf=0.5)], state)
    assert d.silent and d.cards == []
    assert d.trace["silent"] is True
    assert d.trace["reason"] == "silent: no candidate passed guardrails"
    assert d.trace["candidates"][0]["guardrails_fired"] == ["confidence_threshold"]


def test_silence_with_no_candidates_is_recorded():
    d = run([], GuardState(turn=1))
    assert d.silent and d.trace["reason"] == "no candidates"


def test_best_rank_wins_and_only_one_regular_card_per_turn():
    state = GuardState(turn=1)
    d = run([cand("suggested_response", 0.9, "a"), cand("objection_pickup", 0.95, "b")], state)
    assert [c.kind for c in d.cards] == ["objection_pickup"]  # 0.95*0.95 beats 0.9*0.9
    blocked = [e for e in d.trace["candidates"] if e["verdict"] == "blocked"]
    assert blocked and blocked[0]["guardrails_fired"] == ["one_per_turn"]


def test_cooldown_blocks_same_kind_for_configured_turns():
    state = GuardState(turn=1)
    run([cand("suggested_response", 0.9, "q1")], state)           # shown at turn 1
    for turn in (2, 3):
        state.turn = turn
        d = run([cand("suggested_response", 0.9, f"q{turn}")], state)
        assert d.silent
        assert "cooldown" in d.trace["candidates"][0]["guardrails_fired"]
    state.turn = 4                                                  # 3 turns later it is allowed again
    d = run([cand("suggested_response", 0.9, "q4")], state)
    assert [c.kind for c in d.cards] == ["suggested_response"]


def test_budget_caps_non_compliance_cards():
    state = GuardState(turn=1, budget_used=0)
    cfg = GuardConfig(min_confidence=0.6, cooldown_turns=0, budget=1)
    assert run([cand("objection_pickup", 0.9, "a")], state, cfg=cfg).cards
    state.turn = 2
    d = run([cand("objection_pickup", 0.9, "b")], state, cfg=cfg)
    assert d.silent and "budget" in d.trace["candidates"][0]["guardrails_fired"]


def test_duplicate_trigger_is_suppressed():
    state = GuardState(turn=1)
    run([cand("objection_pickup", 0.9, "objection:fees")], state)
    state.turn = 10  # cooldown long gone; the trigger is still a duplicate
    d = run([cand("objection_pickup", 0.9, "objection:fees")], state)
    assert d.silent and "duplicate_trigger" in d.trace["candidates"][0]["guardrails_fired"]


def test_compliance_bypasses_budget_cooldown_and_threshold():
    state = GuardState(turn=1, budget_used=99, last_shown_turn={"compliance": 1})
    comp = Candidate(kind="compliance", title="Compliance", trigger="risk:unsupported_claim", confidence=1.0,
                     source_doc=None, source_clause=None, hint="Do not guarantee.", compose=False, bypass=True)
    d = run([comp], state, customer_turn=False)
    assert [c.kind for c in d.cards] == ["compliance"]
    assert d.trace["candidates"][0]["verdict"] == "allowed"
    assert state.budget_used == 99  # compliance does not spend budget


def test_compliance_fires_even_on_agent_turn_and_with_regular_card_blocked_by_turn_type():
    state = GuardState(turn=3)
    comp = Candidate(kind="compliance", title="c", trigger="risk:x", confidence=1.0, source_doc=None,
                     source_clause=None, hint="m", compose=False, bypass=True)
    d = run([comp, cand("suggested_response", 0.95, "q")], state, customer_turn=False)
    assert [c.kind for c in d.cards] == ["compliance"]


def test_grounded_confidence_mapping():
    assert grounded_confidence(0.5, 0.58) == 0.45          # below threshold: always blocked
    assert grounded_confidence(0.58, 0.58) == 0.6          # at threshold: passes guardrail
    assert grounded_confidence(1.0, 0.58) == 0.95          # perfect match: capped
    assert grounded_confidence(0.9, 0.25) > grounded_confidence(0.4, 0.25) > 0.6


def test_compliance_candidates_map_flags_to_playbook_messages():
    s = get_settings()
    reg = PlaybookRegistry.load(s.playbooks_dir, load_rules(), s.replay_dir)
    pb = reg.get("advisor_coach")
    comps = compliance_candidates(pb, ["unsupported_claim", "vulnerability"])
    assert [c.kind for c in comps] == ["compliance"]          # vulnerability has no advisor rule
    assert comps[0].compose is False and comps[0].bypass is True
    assert "guarantee" in comps[0].hint.lower()


def _ctx(pb, text, *, intent="statement", objection=None, risk=None, hits=None, coverage=None, open_turns=None):
    return TurnContext(
        playbook=pb, customer_text=text, intent=intent, objection=objection, risk_flags=risk or [],
        perception=None, hits=hits or [], coverage=coverage or {m.key: False for m in pb.methodology},
        coverage_turns_open=open_turns or {}, min_retrieval_score=0.58,
    )


def _pb():
    s = get_settings()
    return PlaybookRegistry.load(s.playbooks_dir, load_rules(), s.replay_dir).get("advisor_coach")


def test_gap_candidates_offered_in_playbook_order_and_fall_back():
    pb = _pb()
    cands = turn_candidates(_ctx(pb, "Sounds good, thanks."))
    gaps = [c for c in cands if c.trigger.startswith("gap:")]
    assert [g.trigger for g in gaps] == ["gap:goals", "gap:time_horizon", "gap:risk_tolerance"]
    assert gaps[0].confidence > gaps[1].confidence > gaps[2].confidence


def test_objection_candidate_uses_playbook_kb_ref():
    pb = _pb()
    cands = turn_candidates(_ctx(pb, "That's too expensive.", objection="fees", intent="statement"))
    obj = next(c for c in cands if c.kind == "objection_pickup")
    assert obj.source_doc == "kb/advisor_coach/fee_objections.md"
    assert obj.trigger == "objection:fees"


def test_no_gap_card_while_customer_is_asking_a_question():
    pb = _pb()
    cands = turn_candidates(_ctx(pb, "What do you charge?", intent="question"))
    assert not [c for c in cands if c.trigger.startswith("gap:")]


def test_ungrounded_question_is_silent():
    pb = _pb()
    hit = Hit("kb/advisor_coach/account_types.md", "Account types", "Overview", "text", 0.2, "keyword")
    cands = turn_candidates(_ctx(pb, "Do you sponsor rocket launches?", intent="question", hits=[hit]))
    sr = next(c for c in cands if c.kind == "suggested_response")
    assert sr.confidence < 0.6
    d = run(cands, GuardState(turn=1))
    assert not [c for c in d.cards if c.kind == "suggested_response"]
