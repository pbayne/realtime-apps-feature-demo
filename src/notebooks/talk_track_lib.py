"""Pure helpers for talk_track_insights.py (no Spark): prompt building, output parsing, run ids.

The model is asked for JSON only. Anything that does not fit the contract is dropped and counted, never guessed.
"""

from __future__ import annotations

import json
import re
from typing import Iterable

KINDS = ("objection", "best_response", "kb_gap", "coverage_gap")
MAX_CHARS = 150_000        # whole prompt cap (characters)
MAX_CALL_CHARS = 1_200     # per call transcript cap, so one long call cannot crowd out the others

PROMPT_HEAD = """You are a sales-enablement analyst. Below are recent call transcripts from a
customer-conversation coaching tool, with the coaching cards the sidekick showed and whether the rep accepted,
edited or rejected each card.

Find patterns across the calls. Return ONLY a JSON object of this exact shape:
{"items": [{"kind": "objection" | "best_response" | "kb_gap" | "coverage_gap",
            "label": string (<= 8 words),
            "detail": string (<= 40 words),
            "occurrences": integer (how many calls show it),
            "evidence_calls": integer}]}

Kinds:
- objection: a recurring customer objection.
- best_response: a response reps used that customers accepted (say what worked).
- kb_gap: a question the knowledge base did not answer, so a new knowledge-base entry would help. Detail says what to add.
- coverage_gap: a methodology step that calls often skip.
Use only what the transcripts say. Do not invent. Return at most 12 items.

CALLS:
"""


def build_prompt(calls: Iterable[tuple[str, str]]) -> tuple[str, int]:
    """calls: (call_id, rendered transcript with cards). Returns (prompt, calls_included).
    Stops adding calls once the prompt would exceed MAX_CHARS; each call is truncated to MAX_CALL_CHARS."""
    parts = [PROMPT_HEAD]
    size = len(PROMPT_HEAD)
    included = 0
    for call_id, text in calls:
        block = f"--- call {call_id} ---\n{text[:MAX_CALL_CHARS]}\n"
        if size + len(block) > MAX_CHARS:
            break
        parts.append(block)
        size += len(block)
        included += 1
    return "".join(parts), included


_FENCE = re.compile(r"^\s*```(?:json)?\s*|\s*```\s*$", re.MULTILINE)


def parse_items(raw: str | None) -> tuple[list[dict], int]:
    """Returns (valid items, number dropped). An unparseable response yields ([], 1)."""
    if not raw:
        return [], 1
    try:
        data = json.loads(_FENCE.sub("", raw).strip())
        items = data["items"]
        if not isinstance(items, list):
            raise ValueError("items is not a list")
    except (ValueError, KeyError, TypeError):
        return [], 1
    good, dropped = [], 0
    for it in items:
        if not isinstance(it, dict) or it.get("kind") not in KINDS:
            dropped += 1
            continue
        label = str(it.get("label") or "").strip()[:200]
        if not label:
            dropped += 1
            continue
        good.append({
            "kind": it["kind"],
            "label": label,
            "detail": str(it.get("detail") or "").strip()[:600],
            "occurrences": _clamp_int(it.get("occurrences")),
            "evidence_calls": _clamp_int(it.get("evidence_calls")),
        })
    return good, dropped


def _clamp_int(v: object) -> int:
    try:
        return max(0, min(100_000, int(v)))  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0


def run_id(generated_at_utc: str) -> str:
    """Sortable run id (UTC timestamp, yyyyMMddHHmmss). The app reads MAX(run_id) as the latest run."""
    digits = re.sub(r"\D", "", generated_at_utc)
    if len(digits) < 14:
        raise ValueError("expected a yyyy-MM-ddTHH:mm:ss timestamp")
    return digits[:14]
