"""Golden tests for the pure enrichment transform (mirrors rtm_lib.build_enrichment semantics)."""

import json
from pathlib import Path

import pytest
from enrich.rules import build_rules, enrich, extend_rules, load_rules

APP_RULES = Path(__file__).resolve().parents[2] / "src" / "app" / "enrich" / "enrich_rules.json"
NOTEBOOK_RULES = Path(__file__).resolve().parents[2] / "src" / "notebooks" / "enrich_rules.json"


@pytest.fixture(scope="module")
def rules():
    return load_rules(APP_RULES)


def test_bundled_rules_match_notebook_copy():
    """The RTM job and the app must apply identical rules. Skip if the notebook copy is absent."""
    if not NOTEBOOK_RULES.exists():
        pytest.skip("notebook copy not present")
    assert json.loads(APP_RULES.read_text()) == json.loads(NOTEBOOK_RULES.read_text())


def test_intent_question_and_statement(rules):
    assert enrich("What are the fees?", "customer", rules)["intent"] == "question"
    assert enrich("Can you tell me more", "customer", rules)["intent"] == "question"
    assert enrich("Okay, sounds good.", "customer", rules)["intent"] == "statement"


def test_sentiment_formula_matches_spark(rules):
    # (pos - neg) / (pos + neg + 1) over lowercase tokens split on [^a-z']+
    assert enrich("great, thanks, bad", "customer", rules)["sentiment"] == pytest.approx((2 - 1) / (2 + 1 + 1))
    assert enrich("", "customer", rules)["sentiment"] == 0.0
    assert enrich("nothing relevant here", "customer", rules)["sentiment"] == 0.0


def test_objection_is_customer_only(rules):
    text = "What do you charge? That sounds too expensive."
    assert enrich(text, "customer", rules)["objection"] == "fees"
    assert enrich(text, "agent", rules)["objection"] is None


def test_objection_first_match_in_rule_order(rules):
    assert enrich("I want to think about it, the fees are high", "customer", rules)["objection"] == "fees"


def test_risk_flags_apply_to_all_speakers(rules):
    row = enrich("This fund is guaranteed to outperform.", "agent", rules)
    assert "unsupported_claim" in row["risk_flags"]
    assert enrich("The complaint is with the regulator", "customer", rules)["risk_flags"] == ["complaint"]


def test_methodology_hits_and_entities(rules):
    row = enrich("I have $300,000 in a 401k and I want 20% growth in 9 years", "customer", rules)
    assert "assets" in row["methodology_hits"]
    assert "goals" in row["methodology_hits"] or "time_horizon" in row["methodology_hits"]
    # The shared amount regex also matches "401k" (same as the Spark job); both are expected.
    assert row["entities"]["amount"] == ["$300,000", "401k"]
    assert row["entities"]["percent"] == ["20%"]
    assert row["entities"]["year"] == []


def test_golden_case_table(rules):
    """Pinned outputs. If these change, the Spark job's rtm_lib must be checked for drift."""
    cases = [
        ("Can I roll my 401k into an IRA?", "customer",
         {"intent": "question", "objection": None, "risk_flags": [], "methodology_hits": ["assets"]}),
        ("Let me think about it and get back to you.", "customer",
         {"intent": "statement", "objection": "think_about_it", "risk_flags": []}),
        ("I will file a complaint with the ombudsman.", "customer",
         {"risk_flags": ["complaint"], "objection": None}),
    ]
    for text, speaker, expected in cases:
        row = enrich(text, speaker, rules)
        for key, value in expected.items():
            if key == "methodology_hits":
                assert set(value) <= set(row[key]), text
            else:
                assert row[key] == value, (text, key)


def test_extension_appends_patterns_and_scopes_objections(rules):
    ext = extend_rules(
        rules,
        {"objections": [{"key": "cancel_intent", "patterns": ["cancel my"]}],
         "risk": [{"key": "fraud_indicator", "patterns": ["extra fraud phrase"]}]},
        objection_keys=["cancel_intent", "affordability"],
    )
    keys = [o.key for o in ext.objections]
    assert keys == ["affordability", "cancel_intent"]  # fees/think_about_it are out of scope here
    assert enrich("I want to cancel my plan", "customer", ext)["objection"] == "cancel_intent"
    assert enrich("too expensive", "customer", ext)["objection"] is None


def test_bad_rules_are_rejected():
    with pytest.raises(ValueError):
        build_rules({"objections": [], "risk": [{"key": "a", "patterns": ["x"]}], "methodology": [{"key": "b", "patterns": ["y"]}],
                     "sentiment": {}})
    with pytest.raises(ValueError):
        build_rules({"objections": [{"key": "Bad-Key", "patterns": ["x"]}], "risk": [{"key": "a", "patterns": ["x"]}],
                     "methodology": [{"key": "b", "patterns": ["y"]}], "sentiment": {"positive": [], "negative": []}})


GOLDEN_CASES = json.loads((Path(__file__).resolve().parents[1] / "golden" / "enrich_cases.json").read_text(encoding="utf-8"))


@pytest.mark.parametrize("case", GOLDEN_CASES, ids=[c["text"][:50] for c in GOLDEN_CASES])
def test_shared_golden_cases(rules, case):
    """Same cases as tests/notebooks/test_enrich_rules.py (Spark). Guarantee disclaimers do not fire
    unsupported_claim; real guarantee claims still do."""
    row = enrich(case["text"], case["speaker"], rules)
    assert row["risk_flags"] == case["risk_flags"], case["text"]
    if "objection" in case:
        assert row["objection"] == case["objection"], case["text"]


def test_exclude_patterns_survive_playbook_extension(rules):
    ext = extend_rules(rules, {"risk": [{"key": "unsupported_claim", "patterns": ["cant lose money"]}]})
    item = next(it for it in ext.risk if it.key == "unsupported_claim")
    assert item.exclude_patterns == next(it for it in rules.risk if it.key == "unsupported_claim").exclude_patterns
    assert enrich("Nobody can guarantee this, you cant lose money", "agent", ext)["risk_flags"] == ["unsupported_claim"]
    assert enrich("Nobody can guarantee this", "agent", ext)["risk_flags"] == []


def test_exclude_patterns_must_be_string_lists():
    base = {"objections": [{"key": "fees", "patterns": ["fee"]}], "risk": [{"key": "a", "patterns": ["x"], "exclude_patterns": "no"}],
            "methodology": [{"key": "b", "patterns": ["y"]}], "sentiment": {"positive": [], "negative": []}}
    with pytest.raises(ValueError):
        build_rules(base)
