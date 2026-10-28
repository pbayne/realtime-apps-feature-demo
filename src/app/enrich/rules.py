"""Pure deterministic enrichment transform. Stdlib only, no third-party imports.

This module is the Python mirror of the RTM job's Spark expressions in
`src/notebooks/rtm_lib.py` (`build_enrichment`). Both sides load the same
`enrich_rules.json`. Keep the semantics identical:

- intent: "question" if QUESTION_RE matches, else "statement".
- sentiment: (pos - neg) / (pos + neg + 1) over lowercase tokens split on [^a-z']+.
- risk_flags: every risk key whose pattern group matches (all speakers).
- objection: first objection key whose pattern group matches (customer turns only).
- methodology_hits: every methodology key whose pattern group matches (all speakers).
- entities: {type: [all regex matches]} for every entity key.
- exclude_patterns (optional, per item): before an item's patterns are tested, every match of
  the item's exclude group is replaced by a single space. This is how disclaimers such as
  "no one can guarantee" stop counting as the claim "guaranteed". Both sides use one
  case-insensitive alternation per item, so the replacement is identical.

Patterns are case-insensitive `re.search` (Spark `rlike` is a find, not a full match).
The copy of this module that the notebook workstream copies must stay byte-compatible
with these semantics; `tests/app/test_rules.py` pins golden outputs, and
`tests/notebooks/test_enrich_rules.py` runs the same golden cases through Spark.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Sequence

# QUESTION_RE copied verbatim from src/notebooks/rtm_lib.py.
QUESTION_RE = r"(?i)(\?\s*$)|(^\s*(what|how|when|why|who|where|can|could|do|does|is|are|will|would|should)\b)"
_QUESTION_PAT = re.compile(QUESTION_RE)
_TOKEN_SPLIT = re.compile(r"[^a-z']+")

_APP_COPY = Path(__file__).resolve().with_name("enrich_rules.json")
_NOTEBOOK_COPY = Path(__file__).resolve().parents[2] / "notebooks" / "enrich_rules.json"
REQUIRED_RULE_GROUPS = ("objections", "risk", "methodology")
RULE_GROUPS = ("objections", "risk", "methodology", "entities")
EXCLUDE_REPLACEMENT = " "


def resolve_rules_path(explicit: str | Path | None = None) -> Path:
    """ENRICH_RULES_PATH (explicit) > bundled copy in src/app/enrich > notebooks copy."""
    if explicit:
        return Path(explicit)
    if _APP_COPY.exists():
        return _APP_COPY
    return _NOTEBOOK_COPY


@dataclass(frozen=True)
class RuleItem:
    key: str
    regex: re.Pattern[str]
    patterns: tuple[str, ...] = ()
    exclude_patterns: tuple[str, ...] = ()
    exclude_regex: Optional[re.Pattern[str]] = None

    def cleaned(self, text: str) -> str:
        """Text with every exclude match replaced by a space (identity when no excludes)."""
        if self.exclude_regex is None:
            return text
        return self.exclude_regex.sub(EXCLUDE_REPLACEMENT, text)

    def matches(self, text: str) -> bool:
        return self.regex.search(self.cleaned(text)) is not None


@dataclass(frozen=True)
class Rules:
    objections: tuple[RuleItem, ...]
    risk: tuple[RuleItem, ...]
    methodology: tuple[RuleItem, ...]
    entities: tuple[RuleItem, ...]
    positive: frozenset[str]
    negative: frozenset[str]
    source: str = "inline"
    # Raw document kept so the playbook layer can report and merge without re-reading the file.
    raw: dict[str, Any] = field(default_factory=dict, compare=False, hash=False)


def _group_regex(patterns: Sequence[str]) -> re.Pattern[str]:
    # Mirrors rtm_lib._group_pattern: one case-insensitive alternation per item (or exclude group).
    return re.compile("(?i)(?:" + "|".join(f"(?:{p})" for p in patterns) + ")")


def _compile_items(group: str, items: Iterable[Mapping[str, Any]]) -> tuple[RuleItem, ...]:
    out: list[RuleItem] = []
    seen: set[str] = set()
    for item in items:
        key = item.get("key")
        patterns = item.get("patterns")
        excludes = item.get("exclude_patterns") or []
        if not isinstance(key, str) or not re.fullmatch(r"[a-z][a-z0-9_]*", key):
            raise ValueError(f"enrich rules: {group} key {key!r} must be snake_case")
        if key in seen:
            raise ValueError(f"enrich rules: duplicate {group} key {key!r}")
        if not isinstance(patterns, list) or not patterns:
            raise ValueError(f"enrich rules: {group}.{key} needs at least one pattern")
        if not isinstance(excludes, list) or not all(isinstance(e, str) for e in excludes):
            raise ValueError(f"enrich rules: {group}.{key} exclude_patterns must be a list of strings")
        seen.add(key)
        out.append(RuleItem(
            key=key,
            regex=_group_regex(patterns),
            patterns=tuple(patterns),
            exclude_patterns=tuple(excludes),
            exclude_regex=_group_regex(excludes) if excludes else None,
        ))
    return tuple(out)


def build_rules(raw: Mapping[str, Any], source: str = "inline") -> Rules:
    """Validate a parsed enrich_rules.json document and compile it."""
    for group in REQUIRED_RULE_GROUPS:
        if not isinstance(raw.get(group), list) or not raw.get(group):
            raise ValueError(f"enrich rules: '{group}' must be a non-empty list")
    sentiment = raw.get("sentiment")
    if not isinstance(sentiment, Mapping):
        raise ValueError("enrich rules: 'sentiment' must be an object")
    positive = frozenset(str(w).lower() for w in sentiment.get("positive", []))
    negative = frozenset(str(w).lower() for w in sentiment.get("negative", []))
    return Rules(
        objections=_compile_items("objections", raw["objections"]),
        risk=_compile_items("risk", raw["risk"]),
        methodology=_compile_items("methodology", raw["methodology"]),
        entities=_compile_items("entities", raw.get("entities", [])),
        positive=positive,
        negative=negative,
        source=source,
        raw=dict(raw),
    )


def load_rules(path: str | Path | None = None) -> Rules:
    resolved = resolve_rules_path(path)
    with open(resolved, encoding="utf-8") as fh:
        raw = json.load(fh)
    return build_rules(raw, source=str(resolved))


def extend_rules(
    base: Rules,
    extension: Mapping[str, Any],
    source: str = "playbook",
    objection_keys: Optional[Iterable[str]] = None,
) -> Rules:
    """Return base + extension without mutating base.

    `extension` uses the same group names as enrich_rules.json. Items with a key
    already present in base are merged by appending patterns (their exclude_patterns
    are kept), so a playbook can add phrases to a shared item without redefining it.
    """
    merged: dict[str, list[dict[str, Any]]] = {}
    for group in RULE_GROUPS:
        merged[group] = [
            {
                "key": it.key,
                "patterns": list(it.patterns),
                "exclude_patterns": list(it.exclude_patterns),
            }
            for it in getattr(base, group)
        ]
    for group in RULE_GROUPS:
        for item in extension.get(group, []) or []:
            key = item["key"]
            patterns = list(item.get("patterns", []))
            for existing in merged[group]:
                if existing["key"] == key:
                    existing["patterns"].extend(patterns)
                    break
            else:
                # an unknown key with no patterns cannot be compiled; skip it
                if patterns:
                    merged[group].append({"key": key, "patterns": patterns, "exclude_patterns": []})
    if objection_keys is not None:
        # A playbook only sees its own objection taxonomy (e.g. "fees" does not exist for contact_center).
        allowed = set(objection_keys)
        merged["objections"] = [o for o in merged["objections"] if o["key"] in allowed]
    raw: dict[str, Any] = {group: [_document_item(o) for o in merged[group]] for group in RULE_GROUPS}
    raw["sentiment"] = {"positive": sorted(base.positive), "negative": sorted(base.negative)}
    return build_rules(raw, source=source)


def _document_item(item: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {"key": item["key"], "patterns": item["patterns"]}
    if item.get("exclude_patterns"):
        out["exclude_patterns"] = item["exclude_patterns"]
    return out


def _hits(text: str, items: Sequence[RuleItem]) -> list[str]:
    return [it.key for it in items if it.matches(text)]


def _sentiment(text: str, rules: Rules) -> float:
    tokens = _TOKEN_SPLIT.split(text.lower())
    pos = sum(1 for t in tokens if t in rules.positive)
    neg = sum(1 for t in tokens if t in rules.negative)
    return (pos - neg) / (pos + neg + 1.0)


def _entities(text: str, rules: Rules) -> dict[str, list[str]]:
    return {it.key: [m.group(0) for m in it.regex.finditer(it.cleaned(text))] for it in rules.entities}


def enrich(text: str, speaker: str, rules: Rules) -> dict[str, Any]:
    """Pure transform: one utterance -> the enrichment fields (SPEC section 3).

    Returns intent, sentiment, risk_flags, objection, entities, methodology_hits.
    Callers add call_id, seq, speaker, text and timestamps.
    """
    text = text or ""
    objection = None
    if speaker == "customer":
        objection = next((it.key for it in rules.objections if it.matches(text)), None)
    return {
        "intent": "question" if _QUESTION_PAT.search(text) else "statement",
        "sentiment": _sentiment(text, rules),
        "risk_flags": _hits(text, rules.risk),
        "objection": objection,
        "entities": _entities(text, rules),
        "methodology_hits": _hits(text, rules.methodology),
    }
