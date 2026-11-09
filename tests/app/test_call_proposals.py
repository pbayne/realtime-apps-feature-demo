"""Call-mode action proposals (SPEC 12, request D): proposals now fire on EITHER speaker's finals, and the prompt
is speaker-aware so an agent commitment ("I'll send the paperwork") surfaces an action, not just customer asks."""

from __future__ import annotations

import asyncio

from actions.proposals import call_messages, parse_proposals, propose_for_call


def test_agent_prompt_targets_agent_commitments():
    m = call_messages("Retirement review", ("Plan A",), "agent", "I'll send you the paperwork today", ["customer: ok"])
    sys_text, user_text = m[0]["content"], m[1]["content"]
    assert "human agent" in sys_text.lower()
    assert "commit" in sys_text.lower() and "paperwork" in sys_text.lower()
    assert "Agent just said:" in user_text and "paperwork" in user_text


def test_customer_prompt_unchanged_shape():
    m = call_messages("Retirement review", (), "customer", "Can you send me the fee breakdown?", [])
    assert "Customer just said:" in m[1]["content"]
    assert "explicitly asks" in m[0]["content"]


class _FakeLLM:
    def __init__(self, reply: str) -> None:
        self.reply = reply
        self.seen: list = []

    async def chat(self, model, messages, **kw):
        self.seen.append(messages)
        return self.reply


def test_propose_for_call_parses_agent_commitment():
    llm = _FakeLLM('[{"kind":"send_email","params":{"to":["dana@example.com"],"cc":[],'
                   '"subject":"Your paperwork","body_text":"Here is the paperwork we discussed."}}]')
    out = asyncio.run(propose_for_call(
        llm, "fast", playbook_title="Retirement review", programs=(),
        speaker="agent", speaker_text="I'll email Dana the paperwork at dana@example.com", recent=[], timeout_s=5))
    assert len(out) == 1 and out[0][0] == "send_email"
    # The agent-labelled prompt was used.
    assert "Agent just said:" in llm.seen[0][1]["content"]


def test_propose_for_call_failsoft_on_error():
    class Boom:
        async def chat(self, *a, **k):
            raise RuntimeError("endpoint down")

    out = asyncio.run(propose_for_call(
        Boom(), "fast", playbook_title="x", programs=(), speaker="customer",
        speaker_text="hi", recent=[], timeout_s=1))
    assert out == []


def test_parse_proposals_still_tolerant():
    assert parse_proposals("not json") == []
    assert parse_proposals("") == []
