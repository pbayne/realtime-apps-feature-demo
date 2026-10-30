"""Speakable text and the streaming sentence chunker (SPEC 9.4).

Ported from the reference voice-agent app (`reference/.../voice-agent-app/app/app.py`): sentences end
at `[.!?]` followed by whitespace; the first piece of a reply may also end at a clause (`,;:` or an em
dash) once it is FIRST_CLAUSE_CHARS long, so speech starts sooner. Markdown symbols and emoji are
removed, because the text is read aloud.
"""

from __future__ import annotations

import re

SENTENCE_END = re.compile(r"(?<=[.!?])\s+")
CLAUSE_END = re.compile(r"(?<=[,;:])\s+|(?<=—)")  # em dash
FIRST_CLAUSE_CHARS = 25
EMOJI = re.compile("[\U0001F000-\U0001FAFF☀-➿️‍]")
MARKUP = re.compile(r"[*_#`~>|]+|^\s*[-•]\s+", re.M)


def speakable(text: str) -> str:
    """The text as it should be spoken: no emoji, no markdown symbols, single spaces."""
    return re.sub(r"\s+", " ", MARKUP.sub(" ", EMOJI.sub("", text or ""))).strip()


class SentenceChunker:
    """Feed streamed text; get back the sentences that are complete. flush() returns the rest at the end."""

    def __init__(self) -> None:
        self._pending = ""
        self._spoken_any = False

    def feed(self, text: str) -> list[str]:
        self._pending += text
        *done, self._pending = SENTENCE_END.split(self._pending)
        if not self._spoken_any and not done and len(self._pending) >= FIRST_CLAUSE_CHARS:
            *done, self._pending = CLAUSE_END.split(self._pending)
        self._spoken_any = self._spoken_any or bool(done)
        return [s for s in (speakable(d) for d in done) if s]

    def flush(self) -> list[str]:
        rest, self._pending = self._pending, ""
        s = speakable(rest)
        return [s] if s else []


def split_sentences(text: str) -> list[str]:
    """Whole-text helper (tests and fixed openings): the chunker applied to one string."""
    chunker = SentenceChunker()
    return chunker.feed(text) + chunker.flush()
