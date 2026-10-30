"""talk_track_lib: the prompt cap, the output contract (invalid items are dropped and counted), and run ids."""

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src" / "notebooks"))

import talk_track_lib as T  # noqa: E402


def test_prompt_stops_at_the_cap_and_truncates_long_calls():
    big = "x" * 50_000   # truncated to MAX_CALL_CHARS per call, so the cap is reached only with many calls
    prompt, included = T.build_prompt([(f"c{i}", big) for i in range(300)])
    assert len(prompt) <= T.MAX_CHARS
    assert 0 < included < 300
    assert "c0 " in prompt or "c0 ---" in prompt
    assert f"c{included}" not in prompt
    # A single long call never exceeds its own cap.
    one, _ = T.build_prompt([("only", big)])
    assert "x" * T.MAX_CALL_CHARS in one and "x" * (T.MAX_CALL_CHARS + 1) not in one


def test_prompt_with_no_calls_is_the_head_only():
    prompt, included = T.build_prompt([])
    assert included == 0 and prompt == T.PROMPT_HEAD


def test_valid_items_are_kept_and_clamped():
    raw = '{"items": [{"kind": "objection", "label": "Fee too high", "detail": "d", "occurrences": 7, "evidence_calls": -3}]}'
    items, dropped = T.parse_items(raw)
    assert dropped == 0
    assert items == [{"kind": "objection", "label": "Fee too high", "detail": "d", "occurrences": 7, "evidence_calls": 0}]


def test_fenced_json_is_accepted():
    raw = '```json\n{"items": [{"kind": "kb_gap", "label": "Roth conversion", "detail": "add a doc", "occurrences": 2, "evidence_calls": 2}]}\n```'
    items, dropped = T.parse_items(raw)
    assert dropped == 0 and items[0]["kind"] == "kb_gap"


@pytest.mark.parametrize("raw", [None, "", "not json", '{"nope": 1}', '{"items": "x"}'])
def test_unparseable_responses_yield_no_items_and_one_drop(raw):
    assert T.parse_items(raw) == ([], 1)


def test_items_outside_the_contract_are_dropped_and_counted():
    raw = ('{"items": [{"kind": "rant", "label": "x"}, {"kind": "objection", "label": "   "},'
           ' {"kind": "best_response", "label": "Show the net-of-fee table", "detail": "works"}, "string"]}')
    items, dropped = T.parse_items(raw)
    assert [i["kind"] for i in items] == ["best_response"]
    assert dropped == 3


def test_run_id_is_sortable_utc_digits():
    assert T.run_id("2026-10-30T06:00:00") == "20261030060000"
    assert T.run_id("2026-10-30T06:00:00") < T.run_id("2026-10-31T06:00:00")
    with pytest.raises(ValueError):
        T.run_id("2026")
