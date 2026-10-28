"""LocalAgreement-2 commit logic with hold_words.

Each decode produces a full hypothesis over the rolling window. A word is committed
once it appears in the same position in two consecutive hypotheses (the agreement),
and the last `hold_words` words of each hypothesis are never committed (they are
still moving). Words that start before the last committed word's end are ignored,
so overlapping windows cannot re-commit or duplicate text.
"""

from __future__ import annotations

import re

from .base import Word

_NORM = re.compile(r"[^\w']+")
_TIME_SLACK_MS = 50
_DRIFT_MS = 1000  # how far past the committed boundary a re-decoded run may start and still be a repeat


def norm(word: str) -> str:
    return _NORM.sub("", word.lower())


def common_prefix_len(a: list[Word], b: list[Word]) -> int:
    k = 0
    for x, y in zip(a, b):
        if norm(x.w) != norm(y.w):
            break
        k += 1
    return k


class LocalAgreement2:
    def __init__(self, hold_words: int = 1) -> None:
        if hold_words < 0:
            raise ValueError("hold_words must be >= 0")
        self.hold_words = hold_words
        self.committed: list[Word] = []
        self.tail: list[Word] = []      # uncommitted words from the previous hypothesis

    def _fresh(self, hyp: list[Word]) -> list[Word]:
        if not self.committed:
            return list(hyp)
        boundary = self.committed[-1].e
        cut = boundary - _TIME_SLACK_MS
        fresh = [w for w in hyp if w.s >= cut]
        # Boundary guard: if timestamps drift between windows, a committed run can reappear just
        # after the boundary. Drop a leading run that repeats the committed tail.
        near = 0
        while near < len(fresh) and fresh[near].s < boundary + _DRIFT_MS:
            near += 1
        tail = self.committed[-8:]
        for k in range(min(len(tail), near), 0, -1):
            if [norm(w.w) for w in tail[-k:]] == [norm(w.w) for w in fresh[:k]]:
                # A lone repeated word is usually real speech ("four four", "no no"); treat it
                # as a re-decode only if it overlaps the committed word in time.
                if k == 1 and fresh[0].s >= tail[-1].e:
                    return fresh
                return fresh[k:]
        return fresh

    def update(self, hyp: list[Word]) -> list[Word]:
        """Feed one decode. Returns the newly committed words (possibly empty)."""
        cur = self._fresh(hyp)
        k = common_prefix_len(self.tail, cur)
        stable = min(k, max(0, len(cur) - self.hold_words))
        newly = cur[:stable]
        self.committed.extend(newly)
        self.tail = cur[stable:]
        return newly

    def flush(self, hyp: list[Word]) -> list[Word]:
        """End of utterance: commit everything in the final hypothesis."""
        cur = self._fresh(hyp) if hyp else list(self.tail)
        self.committed.extend(cur)
        self.tail = []
        return cur

    def text(self) -> str:
        return " ".join(w.w for w in self.committed + self.tail)
