"""App-side perception of a customer turn: ONE fast-model call (haiku) per customer final.

The model returns a strict JSON object {intent, sentiment, objection, question, confidence}.
It is never on the critical path: session.py decides immediately from RTM/local enrichment,
rules, retrieval and coverage, and perception runs concurrently with `perception_budget_s`.
A result that arrives after the decision can add a LATE candidate (see session.py).

Parsing is tolerant on purpose (the old three-call design failed ~15% of parses):
  1. strip Markdown code fences, take the outermost {...} span, json.loads;
  2. if that fails, pull each known key out with a field-level regex;
  3. if nothing parses, the perception result is empty and the rules result stands.
Values are normalised: unknown intents/objections become None, sentiment is clamped to [-1, 1],
`question` accepts true/false/"true"/"yes", and confidence is clamped to [0, 1].
"""

from __future__ import annotations

import asyncio
import re
import time
from dataclasses import dataclass, field
from typing import Any, Optional

from llm import LlmClient, LlmUnavailable, parse_json_object

from copilot.playbooks import Playbook

INTENTS = ("question", "objection", "request", "agreement", "statement", "closing")
DEFAULT_CONFIDENCE = 0.6
MAX_TOKENS = 90

_FENCE = re.compile(r"^\s*```[a-zA-Z]*\s*\n?|\n?```\s*$", re.MULTILINE)
_FIELD_STR = r'"{key}"\s*:\s*(?:"([^"]*)"|(null))'
_FIELD_NUM = r'"{key}"\s*:\s*(-?\d+(?:\.\d+)?)'
_FIELD_BOOL = r'"{key}"\s*:\s*("?)(true|false|yes|no)\1'


@dataclass
class Perception:
    intent: Optional[str] = None
    sentiment: Optional[float] = None
    objection: Optional[str] = None
    question: Optional[bool] = None
    confidence: float = 0.0
    parse: str = "none"                  # json | fields | failed | none
    errors: dict[str, str] = field(default_factory=dict)
    latency_ms: dict[str, float] = field(default_factory=dict)

    # Compatibility names used by decide.py (one model call, one confidence).
    @property
    def intent_conf(self) -> float:
        return self.confidence if self.intent else 0.0

    @property
    def objection_conf(self) -> float:
        return self.confidence if self.objection else 0.0

    @property
    def has_fields(self) -> bool:
        return any(v is not None for v in (self.intent, self.sentiment, self.objection, self.question))

    def to_trace(self) -> dict[str, Any]:
        return {
            "intent": self.intent, "sentiment": self.sentiment, "objection": self.objection,
            "question": self.question, "confidence": self.confidence, "parse": self.parse,
            "errors": self.errors, "latency_ms": self.latency_ms,
        }


def strip_fences(raw: str) -> str:
    return _FENCE.sub("", raw or "").strip()


def _norm_intent(v: Any) -> Optional[str]:
    return v if isinstance(v, str) and v in INTENTS else None


def _norm_objection(v: Any, keys: list[str]) -> Optional[str]:
    return v if isinstance(v, str) and v in keys else None


def _norm_sentiment(v: Any) -> Optional[float]:
    try:
        return max(-1.0, min(1.0, float(v)))
    except (TypeError, ValueError):
        return None


def _norm_bool(v: Any) -> Optional[bool]:
    if isinstance(v, bool):
        return v
    if isinstance(v, str) and v.strip().lower() in ("true", "yes", "false", "no"):
        return v.strip().lower() in ("true", "yes")
    return None


def _norm_conf(v: Any) -> float:
    try:
        return max(0.0, min(1.0, float(v)))
    except (TypeError, ValueError):
        return DEFAULT_CONFIDENCE


def _fields_from_regex(text: str, objection_keys: list[str]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    m = re.search(_FIELD_STR.format(key="intent"), text)
    if m:
        out["intent"] = m.group(1)
    m = re.search(_FIELD_NUM.format(key="sentiment"), text)
    if m:
        out["sentiment"] = m.group(1)
    m = re.search(_FIELD_STR.format(key="objection"), text)
    if m:
        out["objection"] = m.group(1)  # null -> group(2) set, group(1) None
    m = re.search(_FIELD_BOOL.format(key="question"), text)
    if m:
        out["question"] = m.group(2)
    m = re.search(_FIELD_NUM.format(key="confidence"), text)
    if m:
        out["confidence"] = m.group(1)
    return out


def parse_perception(raw: str, objection_keys: list[str]) -> tuple[dict[str, Any], str]:
    """Return (normalised fields, parse mode). Never raises."""
    text = strip_fences(raw)
    obj = parse_json_object(text)
    mode = "json"
    if obj is None:
        obj = _fields_from_regex(text, objection_keys)
        mode = "fields" if obj else "failed"
    if not obj:
        return {}, "failed"
    intent = _norm_intent(obj.get("intent"))
    question = _norm_bool(obj.get("question"))
    if intent is None and question:
        intent = "question"
    fields = {
        "intent": intent,
        "sentiment": _norm_sentiment(obj.get("sentiment", obj.get("score"))),
        "objection": _norm_objection(obj.get("objection"), objection_keys),
        "question": question,
        "confidence": _norm_conf(obj.get("confidence")),
    }
    if all(fields[k] is None for k in ("intent", "sentiment", "objection", "question")):
        return {}, "failed"
    return fields, mode


def _messages(system: str, user: str) -> list[dict[str, str]]:
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


def _system_prompt(pb: Playbook, objection_keys: list[str]) -> str:
    return (
        f"{pb.prompts.get('perception', pb.persona)}\n\n"
        "Analyse the customer's latest turn. Return ONE JSON object and nothing else (no prose, no code fences).\n"
        "Exact keys:\n"
        f'{{"intent": one of {list(INTENTS)}, '
        '"sentiment": number from -1 (very negative) to 1 (very positive), '
        f'"objection": one of {objection_keys} or null, '
        '"question": true if the customer asks a question else false, '
        '"confidence": number 0-1}}'
    )


def _context_block(customer_text: str, recent: list[str]) -> str:
    history = "\n".join(f"- {t}" for t in recent[-6:]) or "(none)"
    return f"Recent turns:\n{history}\n\nCustomer's latest turn:\n{customer_text}"


async def perceive(llm: LlmClient, pb: Playbook, customer_text: str, recent: list[str], budget_s: float) -> Perception:
    """One fast-model call within `budget_s`. Never raises; a failed call yields an empty Perception."""
    p = Perception()
    if not llm.available:
        p.errors["perception"] = "unavailable"
        return p
    keys = [o.key for o in pb.objections]
    t0 = time.monotonic()
    try:
        raw = await asyncio.wait_for(
            llm.chat(llm.fast_model, _messages(_system_prompt(pb, keys), _context_block(customer_text, recent)),
                     max_tokens=MAX_TOKENS, timeout=budget_s),
            timeout=budget_s,
        )
    except asyncio.TimeoutError:
        p.errors["perception"] = "timeout"
        raw = None
    except LlmUnavailable as exc:
        p.errors["perception"] = str(exc)[:200]
        raw = None
    finally:
        p.latency_ms["perception"] = round((time.monotonic() - t0) * 1000.0, 1)
    if raw is None:
        return p
    fields, mode = parse_perception(raw, keys)
    p.parse = mode
    if mode == "failed":
        p.errors["perception"] = f"unparseable: {raw[:120]!r}"
        return p
    p.intent = fields["intent"]
    p.sentiment = fields["sentiment"]
    p.objection = fields["objection"]
    p.question = fields["question"]
    p.confidence = fields["confidence"]
    return p
