"""System prompts for the two voice roles (SPEC 9), built from the playbook and the context a turn needs.

Assistant: the rep talks to Sidekick. Context = the linked call (recent transcript, coverage, cards) and
the playbook KB hits. Practice: Sidekick is the customer persona from the playbook `practice:` scenario.
Both share VOICE_RULES (from the reference voice-agent app) and the <<ITEMS>> instructions (SPEC 9.4).
"""

from __future__ import annotations

from typing import Any

from sidekick.playbooks import Playbook, PracticeScenario
from sidekick.retrieval import Hit

# Always appended, and they win: the reply is spoken, so it must read well aloud.
VOICE_RULES = (
    "Voice rules, which override any instructions above: you are in a live voice conversation, and everything "
    "you write is read aloud by a text-to-speech voice. Speak in plain sentences, with no emoji, markdown, lists, "
    "headings, tables or links. Keep replies to one to three short sentences unless the user asks for more. "
    "Write numbers, dates, symbols and abbreviations the way they are said, for example twenty dollars, not $20. "
    "If you didn't catch or understand what the user said, ask them to say it again."
)

ITEMS_RULES = (
    "After your spoken reply, write a new line containing exactly <<ITEMS>>, then on the next line a JSON array "
    "(possibly empty []) of the items this turn produced worth keeping. Each item is an object with "
    '"kind" (one of action_item, follow_up_question, follow_up_response, next_step, summary, risk, note), '
    '"text" (one short sentence), and optional "owner", "due". Never put the JSON in the spoken part.'
)

HISTORY_MESSAGES = 12   # conversation messages kept in the prompt


def assistant_messages(pb: Playbook, user_text: str, history: list[dict[str, str]], call_text: str,
                       hits: list[Hit]) -> list[dict[str, str]]:
    kb = "\n".join(f"- [{h.title} / {h.section}] {h.text[:500]}" for h in hits) or "(no playbook reference matched)"
    system = "\n\n".join([
        pb.persona.strip(),
        "You are Sidekick, the live assistant for the person you are talking to. They may ask what to ask next, "
        "for a summary, or for a draft follow-up. Be direct and specific.",
        ("Linked call:\n" + call_text) if call_text else "No call is linked to this conversation.",
        "Playbook reference (use it for facts; if it does not cover the question, say you will confirm):\n" + kb,
        VOICE_RULES,
        ITEMS_RULES,
    ])
    return [{"role": "system", "content": system}, *history[-HISTORY_MESSAGES:],
            {"role": "user", "content": user_text}]


def practice_messages(pb: Playbook, scenario: PracticeScenario, history: list[dict[str, str]],
                      rep_text: str) -> list[dict[str, str]]:
    system = "\n\n".join([
        "You are role-playing the customer in a practice conversation. The person you are talking to is "
        f"practising {pb.title.lower()}. You are the customer, not the coach: stay in character and never mention "
        "coaching, scores or the practice itself.",
        f"Your persona: {scenario.persona}",
        "Goals in this conversation:\n" + "\n".join(f"- {g}" for g in scenario.goals),
        "Objections to raise naturally, one at a time, when it fits the conversation:\n"
        + "\n".join(f"- {o}" for o in scenario.objections),
        "Reply to the advisor's or agent's last turn in character.",
        VOICE_RULES,
        "After your spoken reply, write a new line containing exactly <<ITEMS>> then [] unless the practising "
        "person clearly committed to something, in which case list it as an action_item or next_step.",
    ])
    return [{"role": "system", "content": system}, *history[-HISTORY_MESSAGES:],
            {"role": "user", "content": rep_text}]


def call_text(snapshot: dict[str, Any], coverage: list[dict[str, Any]], max_lines: int = 20) -> str:
    """Recent transcript, coverage and cards of the linked call, as plain text for the system prompt."""
    parts = []
    lines = [f"{u.get('speaker', '?')}: {u.get('text', '')}" for u in (snapshot.get("utterances") or [])[-max_lines:]]
    if lines:
        parts.append("Recent transcript:\n" + "\n".join(lines))
    if coverage:
        parts.append("Checklist: " + ", ".join(
            f"{c['label']} ({'covered' if c.get('covered') else 'open'})" for c in coverage))
    cards = (snapshot.get("cards") or [])[-5:]
    if cards:
        parts.append("Cards shown: " + "; ".join(str(c.get("title", "")) for c in cards))
    return "\n".join(parts)
