"""LocalAgreement-2 commit logic with hold_words.

Each decode produces a full hypothesis over the rolling window. A word is committed
once it appears in the same position in two consecutive hypotheses (the agreement),
and the last `hold_words` words of each hypothesis are never committed (they are
still moving).

Re-decoding an overlapping window moves word timestamps by a few hundred ms (measured on
Parakeet: the same word drifted 160-400 ms between consecutive 12 s windows), so a time cut
alone either re-commits words ("a a couple") or drops them. Instead, look back generously and
align the hypothesis on the committed text: find where the committed tail (up to 5 words)
reappears near the start of the hypothesis and keep only what follows it. Genuine repeats
survive ("in four" + "four four two" keeps the second "four").
"""

from __future__ import annotations

import re

from .base import Word

_NORM = re.compile(r"[^\w']+")
_TIME_SLACK_MS = 50     # fallback cut when the committed tail is not found in the hypothesis
_LOOKBACK_MS = 1000     # how far before the committed boundary a drifted re-decode may start
_MAX_NGRAM = 5          # longest committed tail used for alignment
_MAX_OFFSET = 6         # how deep into the look-back region the committed tail may reappear
_MAX_OFFSET_SINGLE = 2  # a one-word match is weak evidence: only accept it near the start


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
        region = [w for w in hyp if w.s >= boundary - _LOOKBACK_MS]
        toks = [norm(w.w) for w in region]
        tail = [norm(w.w) for w in self.committed[-_MAX_NGRAM:]]
        for k in range(min(_MAX_NGRAM, len(tail)), 0, -1):
            suffix = tail[-k:]
            deepest = _MAX_OFFSET if k > 1 else _MAX_OFFSET_SINGLE
            for i in range(0, min(deepest, len(toks) - k) + 1):
                if toks[i:i + k] == suffix:
                    return region[i + k:]
        return [w for w in region if w.s >= boundary - _TIME_SLACK_MS]

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
