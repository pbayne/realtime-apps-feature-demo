"""Candidate generation and guardrails (SPEC section 4). Pure functions, no I/O.

Policy:
  - compliance candidates (risk flag with a playbook compliance entry) are always
    allowed: they bypass confidence, cooldown, budget and dedupe. Their body is the
    playbook's verbatim message (never model-generated).
  - all other candidates are ranked by confidence x playbook card weight. The best
    one that passes guardrails wins. At most one non-compliance card per customer turn.
  - guardrails: confidence >= min_confidence; per-kind cooldown (turns); per-call
    budget; no duplicate trigger.
  - perception may be absent (still pending) when the turn is decided. A late perception
    result is evaluated by session.py with the same functions and only fires when no card
    fired for that turn (SPEC section 4).
  - nothing passes -> silent (no card, decision trace still recorded).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

from copilot.perception import Perception
from copilot.playbooks import Playbook
from copilot.retrieval import Hit


@dataclass
class Candidate:
    kind: str
    title: str
    trigger: str
    confidence: float
    source_doc: Optional[str]
    source_clause: Optional[str]
    hint: str                        # template body (rules-only) and compose context
    compose: bool = True             # False -> body is `hint` verbatim
    bypass: bool = False
    rationale: str = ""
    grounding: str = ""              # KB text the compose step may quote from

    def rank(self, weights: dict[str, float]) -> float:
        return self.confidence * weights.get(self.kind, 0.5)


@dataclass
class TurnContext:
    playbook: Playbook
    customer_text: str
    intent: Optional[str]            # rules intent, overridden by perception when present
    objection: Optional[str]         # rules objection (customer turns)
    risk_flags: list[str]
    perception: Optional[Perception]
    hits: list[Hit]
    coverage: dict[str, bool]
    coverage_turns_open: dict[str, int]   # customer turns each uncovered item has been open
    min_retrieval_score: float


@dataclass
class GuardConfig:
    min_confidence: float = 0.6
    cooldown_turns: int = 3
    budget: int = 12


@dataclass
class GuardState:
    turn: int = 0
    budget_used: int = 0
    last_shown_turn: dict[str, int] = field(default_factory=dict)
    shown_triggers: set[str] = field(default_factory=set)


@dataclass
class Decision:
    cards: list[Candidate]
    trace: dict[str, Any]

    @property
    def silent(self) -> bool:
        return not self.cards


# ----------------------------------------------------------------- candidates

def compliance_candidates(playbook: Playbook, risk_flags: list[str]) -> list[Candidate]:
    out: list[Candidate] = []
    for flag in risk_flags:
        spec = playbook.compliance_for(flag)
        if spec is None:
            continue
        out.append(Candidate(
            kind="compliance", title=spec.title, trigger=f"risk:{flag}", confidence=1.0,
            source_doc=None, source_clause=None, hint=spec.message, compose=False, bypass=True,
            rationale=f"risk flag {flag} matched playbook compliance rule",
        ))
    return out


def _best_hit(ctx: TurnContext) -> Optional[Hit]:
    return ctx.hits[0] if ctx.hits else None


def grounded_confidence(score: float, min_score: float) -> float:
    """Map a retrieval score to card confidence. At the grounding threshold -> 0.6 (passes the
    guardrail); a perfect match -> 0.95; below threshold -> 0.45 (always blocked)."""
    if score < min_score:
        return 0.45
    span = max(1e-6, 1.0 - min_score)
    return round(min(0.95, 0.6 + 0.35 * (score - min_score) / span), 4)


def turn_candidates(ctx: TurnContext) -> list[Candidate]:
    """Regular (non-compliance) candidates for one customer turn."""
    pb = ctx.playbook
    out: list[Candidate] = []
    best = _best_hit(ctx)
    grounded = best is not None and best.score >= ctx.min_retrieval_score
    intent = (ctx.perception.intent if ctx.perception and ctx.perception.intent else ctx.intent) or "statement"
    # The perception `question` flag (when perception ran) is the authority; rules intent is the fallback.
    if ctx.perception is not None and ctx.perception.question is not None:
        is_question = ctx.perception.question
    else:
        is_question = intent == "question"
    kb_snip = f" Grounding: {best.title} / {best.section}." if best else ""

    # 1. objection -> objection_pickup (rules first, then perception)
    obj_key, obj_conf, obj_src = None, 0.0, ""
    if ctx.objection and pb.objection(ctx.objection):
        obj_key, obj_conf, obj_src = ctx.objection, 0.85, "rules"
    elif ctx.perception and ctx.perception.objection and pb.objection(ctx.perception.objection):
        obj_key = ctx.perception.objection
        obj_conf = min(0.95, ctx.perception.objection_conf or 0.0)
        obj_src = "perception"
    if obj_key:
        spec = pb.objection(obj_key)
        doc = spec.kb_ref if spec and spec.kb_ref else (best.doc_id if best else None)
        clause = best.section if best and best.doc_id == doc else None
        out.append(Candidate(
            kind="objection_pickup", title=f"Objection: {obj_key.replace('_', ' ')}",
            trigger=f"objection:{obj_key}", confidence=obj_conf, source_doc=doc, source_clause=clause,
            hint=f"Customer objection ({obj_key.replace('_', ' ')}): respond to '{ctx.customer_text[:200]}'.{kb_snip}",
            rationale=f"objection via {obj_src}", grounding=best.text if best and best.doc_id == doc else "",
        ))

    # 2. question -> suggested_response (grounded or silent)
    if is_question and ctx.customer_text:
        conf = grounded_confidence(best.score, ctx.min_retrieval_score) if best else 0.45
        out.append(Candidate(
            kind="suggested_response", title="Answer the customer's question",
            trigger=f"question:{best.doc_id}#{best.section}" if best else "question:ungrounded",
            confidence=conf, source_doc=best.doc_id if best else None,
            source_clause=best.section if best else None,
            hint=(best.text if best else "") or "Answer the question from policy, or say you will confirm.",
            rationale=f"retrieval score {best.score:.2f} via {best.backend}" if best else "no retrieval hit",
            grounding=best.text if best else "",
        ))

    # 3. request -> next_best_action
    if intent == "request" and grounded and best is not None:
        out.append(Candidate(
            kind="next_best_action", title="Suggested next step",
            trigger=f"request:{best.doc_id}#{best.section}",
            confidence=min(0.9, grounded_confidence(best.score, ctx.min_retrieval_score)),
            source_doc=best.doc_id, source_clause=best.section, hint=best.text,
            rationale=f"request intent, retrieval score {best.score:.2f}", grounding=best.text,
        ))

    # 4. coverage gap during a lull -> next_question / missing_info. One candidate per open item
    #    (first three in playbook order), confidence decays by order, so a deduped item falls back
    #    to the next open one instead of silencing the coach.
    lull = not is_question and intent != "request" and obj_key is None and not ctx.risk_flags
    gaps = [m for m in pb.methodology if not ctx.coverage.get(m.key)][:3]
    if lull:
        for i, gap in enumerate(gaps):
            kind = "missing_info" if ctx.coverage_turns_open.get(gap.key, 0) >= 4 else "next_question"
            out.append(Candidate(
                kind=kind, title=f"Ask about {gap.label.lower()}", trigger=f"gap:{gap.key}",
                confidence=round(0.72 - 0.02 * i, 4), source_doc=None, source_clause=None,
                hint=gap.question, compose=True,
                rationale=f"coverage gap '{gap.key}' during a lull",
            ))
    return out


# ------------------------------------------------------------------ guardrails

def decide(
    candidates: list[Candidate],
    state: GuardState,
    weights: dict[str, float],
    cfg: GuardConfig,
    *,
    customer_turn: bool,
) -> Decision:
    entries: list[dict[str, Any]] = []
    cards: list[Candidate] = []

    for c in candidates:
        if c.bypass:
            cards.append(c)
            entries.append(_entry(c, weights, "allowed", "compliance bypasses guardrails", []))

    regular = sorted((c for c in candidates if not c.bypass), key=lambda c: (c.rank(weights), c.confidence), reverse=True)
    winner: Optional[Candidate] = None
    if customer_turn:
        for c in regular:
            fired: list[str] = []
            if c.confidence < cfg.min_confidence:
                fired.append("confidence_threshold")
            last = state.last_shown_turn.get(c.kind)
            if last is not None and state.turn - last < cfg.cooldown_turns:
                fired.append("cooldown")
            if c.trigger in state.shown_triggers:
                fired.append("duplicate_trigger")
            if state.budget_used >= cfg.budget:
                fired.append("budget")
            if fired:
                entries.append(_entry(c, weights, "blocked", ",".join(fired), fired))
            elif winner is None:
                winner = c
                entries.append(_entry(c, weights, "winner", "highest rank passing guardrails", []))
            else:
                entries.append(_entry(c, weights, "blocked", "one card per customer turn", ["one_per_turn"]))
    else:
        for c in regular:
            entries.append(_entry(c, weights, "blocked", "not a customer turn", ["not_customer_turn"]))

    if winner is not None:
        cards.append(winner)
        state.budget_used += 1
        state.last_shown_turn[winner.kind] = state.turn
        state.shown_triggers.add(winner.trigger)

    if cards:
        reason = "cards emitted"
    elif not candidates:
        reason = "no candidates"
    else:
        reason = "silent: no candidate passed guardrails"

    trace = {
        "turn": state.turn,
        "customer_turn": customer_turn,
        "candidates": entries,
        "winner": winner.kind if winner else None,
        "cards": [c.kind for c in cards],
        "silent": not cards,
        "reason": reason,
        "budget_used": state.budget_used,
    }
    return Decision(cards=cards, trace=trace)


def _entry(c: Candidate, weights: dict[str, float], verdict: str, reason: str, fired: list[str]) -> dict[str, Any]:
    return {
        "kind": c.kind,
        "trigger": c.trigger,
        "confidence": round(c.confidence, 4),
        "weight": weights.get(c.kind, 0.5),
        "rank": round(c.rank(weights), 4),
        "verdict": verdict,
        "reason": reason,
        "guardrails_fired": fired,
        "source_doc": c.source_doc,
        "source_clause": c.source_clause,
        "rationale": c.rationale,
    }
