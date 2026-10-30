"""Load playbooks/*.yaml (SPEC section 5) and their KB markdown.

A playbook adds regex extensions on top of the shared enrich_rules.json. Those
extensions are applied app-side only (the RTM job only sees the shared rules).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import yaml
from enrich.rules import Rules, extend_rules


@dataclass(frozen=True)
class MethodologyItem:
    key: str
    label: str
    patterns: tuple[str, ...]
    question: str


@dataclass(frozen=True)
class ObjectionSpec:
    key: str
    patterns: tuple[str, ...]
    kb_ref: Optional[str]


@dataclass(frozen=True)
class ComplianceSpec:
    key: str
    title: str
    message: str
    patterns: tuple[str, ...]


@dataclass(frozen=True)
class PracticeScenario:
    """A practice persona (SPEC 9): Sidekick plays the customer. `voice` is a Kokoro preset."""
    key: str
    title: str
    persona: str
    voice: str
    opening: str
    objections: tuple[str, ...]
    goals: tuple[str, ...]

    def to_wire(self) -> dict:
        return {"key": self.key, "name": self.key, "title": self.title, "persona": self.persona, "voice": self.voice,
                "opening": self.opening, "objections": list(self.objections), "goals": list(self.goals)}


@dataclass(frozen=True)
class KbDoc:
    doc_id: str          # path relative to playbooks/, e.g. "kb/advisor_coach/fee_objections.md"
    title: str
    text: str


@dataclass(frozen=True)
class Playbook:
    name: str
    title: str
    persona: str
    card_weights: dict[str, float]
    methodology: tuple[MethodologyItem, ...]
    objections: tuple[ObjectionSpec, ...]
    compliance: tuple[ComplianceSpec, ...]
    prompts: dict[str, str]
    kb_docs: tuple[KbDoc, ...]
    replay_scripts: tuple[str, ...] = ()
    scenarios: tuple[PracticeScenario, ...] = ()
    extra: dict[str, Any] = field(default_factory=dict, compare=False)

    def scenario(self, key: Optional[str]) -> Optional[PracticeScenario]:
        return next((s for s in self.scenarios if s.key == key), None)

    def rules_extension(self) -> dict[str, Any]:
        """Shape consumed by enrich.rules.extend_rules."""
        return {
            "objections": [{"key": o.key, "patterns": list(o.patterns)} for o in self.objections],
            "risk": [{"key": c.key, "patterns": list(c.patterns)} for c in self.compliance],
            "methodology": [{"key": m.key, "patterns": list(m.patterns)} for m in self.methodology],
        }

    def objection(self, key: Optional[str]) -> Optional[ObjectionSpec]:
        return next((o for o in self.objections if o.key == key), None)

    def compliance_for(self, key: str) -> Optional[ComplianceSpec]:
        return next((c for c in self.compliance if c.key == key), None)

    def doc(self, doc_id: Optional[str]) -> Optional[KbDoc]:
        return next((d for d in self.kb_docs if d.doc_id == doc_id), None)


def _keyword_patterns(keywords: list[str]) -> tuple[str, ...]:
    return tuple(r"\b" + re.escape(k.strip().lower()) + r"\b" for k in keywords if k.strip())


def _parse_doc(doc_id: str, text: str) -> KbDoc:
    title_match = re.search(r"^#\s+(.+)$", text, re.MULTILINE)
    title = title_match.group(1).strip() if title_match else doc_id
    return KbDoc(doc_id=doc_id, title=title, text=text)


def load_playbook(path: Path, playbooks_root: Path, replay_dir: Optional[Path] = None) -> Playbook:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    name = raw["name"]
    kb_dir = playbooks_root / raw["kb_dir"]
    kb_docs = tuple(
        _parse_doc(str(p.relative_to(playbooks_root)), p.read_text(encoding="utf-8"))
        for p in sorted(kb_dir.glob("*.md"))
    )
    methodology = tuple(
        MethodologyItem(
            key=m["key"],
            label=m["label"],
            patterns=_keyword_patterns(m.get("keywords", [])) + tuple(m.get("patterns", [])),
            question=m.get("question", f"Can you tell me more about {m['label'].lower()}?"),
        )
        for m in raw.get("methodology", {}).get("items", [])
    )
    objections = tuple(
        ObjectionSpec(key=o["key"], patterns=tuple(o["patterns"]), kb_ref=o.get("kb_ref"))
        for o in raw.get("objections", [])
    )
    compliance = tuple(
        ComplianceSpec(key=c["key"], title=c.get("title", "Compliance"), message=c["message"], patterns=tuple(c["patterns"]))
        for c in raw.get("compliance", [])
    )
    scenarios = tuple(
        PracticeScenario(
            key=str(sc["key"]), title=sc.get("title", sc["key"]), persona=sc.get("persona", "").strip(),
            voice=sc.get("voice", "af_heart"), opening=sc.get("opening", "").strip(),
            objections=tuple(sc.get("objections") or ()), goals=tuple(sc.get("goals") or ()),
        )
        for sc in raw.get("practice") or []
    )
    scripts: tuple[str, ...] = ()
    if replay_dir is not None and replay_dir.exists():
        scripts = tuple(sorted(p.stem.split("__", 1)[1] for p in replay_dir.glob(f"{name}__*.jsonl")))
    return Playbook(
        name=name,
        title=raw.get("title", name),
        persona=raw.get("persona", ""),
        card_weights={k: float(v) for k, v in (raw.get("card_weights") or {}).items()},
        methodology=methodology,
        objections=objections,
        compliance=compliance,
        prompts=dict(raw.get("prompts") or {}),
        kb_docs=kb_docs,
        replay_scripts=scripts,
        scenarios=scenarios,
    )


class PlaybookRegistry:
    def __init__(self, playbooks: list[Playbook], base_rules: Rules) -> None:
        self._by_name = {p.name: p for p in playbooks}
        self._base = base_rules
        self._rules_cache: dict[str, Rules] = {}

    @classmethod
    def load(cls, root: Path, base_rules: Rules, replay_dir: Optional[Path] = None) -> "PlaybookRegistry":
        files = sorted(root.glob("*.yaml"))
        if not files:
            raise FileNotFoundError(f"no playbooks found in {root}")
        return cls([load_playbook(f, root, replay_dir) for f in files], base_rules)

    def names(self) -> list[str]:
        return sorted(self._by_name)

    def get(self, name: str) -> Playbook:
        try:
            return self._by_name[name]
        except KeyError as exc:
            raise KeyError(f"unknown playbook {name!r}; known: {self.names()}") from exc

    def all(self) -> list[Playbook]:
        return [self._by_name[n] for n in self.names()]

    def rules_for(self, name: str) -> Rules:
        """Shared rules + this playbook's extensions (cached)."""
        if name not in self._rules_cache:
            pb = self.get(name)
            self._rules_cache[name] = extend_rules(
                self._base, pb.rules_extension(), source=f"shared+{name}",
                objection_keys=[o.key for o in pb.objections],
            )
        return self._rules_cache[name]
