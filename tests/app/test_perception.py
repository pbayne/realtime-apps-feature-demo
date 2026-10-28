"""Single-call perception: strict JSON contract, code-fence stripping, tolerant parsing, budgets."""

import asyncio
import json

import pytest
from config import Settings
from copilot.perception import Perception, parse_perception, perceive, strip_fences
from copilot.playbooks import PlaybookRegistry
from enrich.rules import load_rules
from llm import LlmClient, LlmUnavailable

KEYS = ["fees", "think_about_it", "market_timing", "existing_advisor"]


@pytest.fixture(scope="module")
def pb():
    s = Settings(mode="local")
    return PlaybookRegistry.load(s.playbooks_dir, load_rules(), s.replay_dir).get("advisor_coach")


@pytest.mark.parametrize("raw", [
    '{"intent": "question", "sentiment": -0.4, "objection": "fees", "question": true, "confidence": 0.9}',
    '```json\n{"intent": "question", "sentiment": -0.4, "objection": "fees", "question": true, "confidence": 0.9}\n```',
    'Here you go: {"intent": "question", "sentiment": -0.4, "objection": "fees", "question": true, "confidence": 0.9}',
])
def test_json_variants_parse_to_the_same_fields(raw):
    fields, mode = parse_perception(raw, KEYS)
    assert mode == "json"
    assert fields == {"intent": "question", "sentiment": -0.4, "objection": "fees", "question": True, "confidence": 0.9}


def test_truncated_json_falls_back_to_field_regex():
    raw = '{"intent": "objection", "sentiment": -0.2, "objection": "think_about_it", "question": false, "confid'
    fields, mode = parse_perception(raw, KEYS)
    assert mode == "fields"
    assert fields["intent"] == "objection" and fields["objection"] == "think_about_it"
    assert fields["question"] is False and fields["sentiment"] == -0.2


def test_null_objection_and_unknown_values_are_normalised():
    fields, mode = parse_perception('{"intent": "chitchat", "sentiment": 7, "objection": "made_up", "question": "yes"}', KEYS)
    assert mode == "json"
    assert fields["intent"] == "question"    # unknown intent dropped; question=true supplies it
    assert fields["sentiment"] == 1.0        # clamped
    assert fields["objection"] is None       # not a playbook key
    assert fields["question"] is True        # "yes" accepted
    assert fields["confidence"] == 0.6       # default when missing


def test_question_flag_implies_question_intent_when_intent_missing():
    fields, _ = parse_perception('{"question": true, "sentiment": 0}', KEYS)
    assert fields["intent"] == "question"


@pytest.mark.parametrize("raw", ["", "   ", "I cannot help with that.", '{"unrelated": 1}', "```json\n```"])
def test_garbage_is_a_failed_parse_not_an_exception(raw):
    fields, mode = parse_perception(raw, KEYS)
    assert fields == {} and mode == "failed"


def test_strip_fences_keeps_inner_json():
    assert strip_fences('```json\n{"a": 1}\n```') == '{"a": 1}'
    assert strip_fences('{"a": 1}') == '{"a": 1}'


class _ScriptedLlm(LlmClient):
    def __init__(self, settings, reply=None, delay=0.0, error=None):
        super().__init__(settings, sdk_config=object())
        self.reply, self.delay, self.error = reply, delay, error
        self.calls = []

    @property
    def available(self):
        return True

    async def chat(self, model, messages, *, max_tokens=300, timeout=None):
        self.calls.append({"model": model, "max_tokens": max_tokens, "timeout": timeout, "messages": messages})
        await asyncio.sleep(self.delay)
        if self.error:
            raise LlmUnavailable(self.error)
        return self.reply


def test_perceive_makes_exactly_one_fast_model_call(pb):
    s = Settings(mode="local")
    llm = _ScriptedLlm(s, reply='{"intent": "statement", "sentiment": 0.1, "objection": null, "question": false, "confidence": 0.5}')
    p = asyncio.run(perceive(llm, pb, "Okay thanks", [], budget_s=2.5))
    assert len(llm.calls) == 1
    assert llm.calls[0]["model"] == s.fast_model
    assert "ONE JSON object" in llm.calls[0]["messages"][0]["content"]
    assert p.intent == "statement" and p.sentiment == 0.1 and p.question is False and p.parse == "json"
    assert p.errors == {}


def test_perceive_budget_timeout_is_recorded_and_never_raises(pb):
    llm = _ScriptedLlm(Settings(mode="local"), reply="{}", delay=5.0)
    p = asyncio.run(perceive(llm, pb, "What do you charge?", [], budget_s=0.05))
    assert p.errors == {"perception": "timeout"}
    assert p.intent is None and p.parse == "none"


def test_perceive_unavailable_llm(pb):
    llm = _ScriptedLlm(Settings(mode="local"), error="HTTP 503")
    p = asyncio.run(perceive(llm, pb, "hello", [], budget_s=1.0))
    assert "HTTP 503" in p.errors["perception"]


def test_perceive_unparseable_reply_is_an_error_not_a_crash(pb):
    llm = _ScriptedLlm(Settings(mode="local"), reply="")
    p = asyncio.run(perceive(llm, pb, "hello", [], budget_s=1.0))
    assert p.parse == "failed" and p.errors["perception"].startswith("unparseable")


def test_perception_trace_shape():
    p = Perception(intent="objection", objection="fees", confidence=0.8)
    assert p.objection_conf == 0.8 and p.intent_conf == 0.8
    trace = p.to_trace()
    assert set(trace) >= {"intent", "sentiment", "objection", "question", "confidence", "parse", "errors", "latency_ms"}
    assert json.dumps(trace)
