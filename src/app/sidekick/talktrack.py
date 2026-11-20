"""Talk-track matching (request C): did the agent actually say what a suggestion card recommended?

A small, dependency-free lexical matcher. When the agent's spoken line covers enough of a card's key content
words, the card is auto-accepted (the rep no longer has to click Accept for advice they already followed). It is
deliberately conservative — a miss just leaves the card for a manual decision, so a false auto-accept is costly
but a false miss is harmless.
"""

from __future__ import annotations

import re

_WORD = re.compile(r"[a-z0-9]+")

# Common words carry no signal for "did they say the same thing", so they are ignored on both sides.
_STOP = frozenset(
    "a an and the to of for in on at is are was were be been being do does did you your we our i it this that "
    "with as or if so but not no yes can could would should will just about into over under than then them they "
    "he she his her their there here what when where who how why which while per via also any all more most some "
    "such like from by up out off down ok okay sure thanks thank please let lets me my mine ours us".split()
)


def _stem(word: str) -> str:
    """Very small suffix stripper so morphological variants collapse to one token: retiring/retirement/retired
    -> retir, fees/fee -> fee, timeline stays. Not linguistically perfect — just enough that natural speech
    ("when are you retiring") matches a card written in another form ("ask about retirement")."""
    for suf in ("ement", "ements", " ing", "ings", "ing", "tion", "tions", "ed", "es", "s", "ly", "ance", "ence"):
        if word.endswith(suf) and len(word) - len(suf) >= 3:
            return word[: -len(suf)]
    return word


def _content_words(text: str) -> set[str]:
    return {_stem(w) for w in _WORD.findall((text or "").lower()) if w not in _STOP and len(w) > 2}


def coverage_score(said: str, suggestion: str) -> float:
    """Fraction of the suggestion's content words that appear in what was said (0..1). Asymmetric on purpose:
    the agent may say more than the card; we only ask whether they covered the card's substance."""
    want = _content_words(suggestion)
    if not want:
        return 0.0
    said_words = _content_words(said)
    if not said_words:
        return 0.0
    hit = len(want & said_words)
    return hit / len(want)


def matches_talk_track(said: str, *, title: str, body: str, threshold: float = 0.6,
                       min_overlap: int = 2) -> bool:
    """True when the spoken line covers the card well enough to auto-accept it. Scores against the title and the
    body separately and takes the better of the two (a short title can be fully covered; a long body rarely is).
    Requires at least `min_overlap` shared content words so one generic word cannot trip it."""
    said_words = _content_words(said)
    best = 0.0
    overlap = 0
    for target in (title, body):
        want = _content_words(target)
        if not want:
            continue
        shared = len(want & said_words)
        overlap = max(overlap, shared)
        best = max(best, shared / len(want))
    return best >= threshold and overlap >= min_overlap


def matches_action(said: str, *, summary: str, params: dict | None = None, threshold: float = 0.6,
                   min_overlap: int = 2) -> bool:
    """True when a spoken line covers a proposed action well enough to auto-accept and run it (demo autonomy).

    Scores the utterance against the action's plain-language `summary` and the salient free-text params
    (email subject/body, meeting title/description, enroll note) — NOT structural params like addresses, which
    STT rarely gets verbatim. Same conservative coverage logic as talk tracks: a miss just leaves the action
    for a manual decision, so a false auto-run is costly but a false miss is harmless. Under the outbox safety
    lock nothing actually leaves, so this is safe to run eagerly in a demo."""
    targets = [summary or ""]
    p = params or {}
    for key in ("subject", "body_text", "title", "description", "note", "program"):
        val = p.get(key)
        if isinstance(val, str) and val.strip():
            targets.append(val)
    said_words = _content_words(said)
    best = 0.0
    overlap = 0
    for target in targets:
        want = _content_words(target)
        if not want:
            continue
        shared = len(want & said_words)
        overlap = max(overlap, shared)
        best = max(best, shared / len(want))
    return best >= threshold and overlap >= min_overlap
