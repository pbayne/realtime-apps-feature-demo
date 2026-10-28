"""Scripted replay (SPEC section 3, replay mode).

A replay script is JSONL (built by src/app/data/replay/build_replay.py):
  line 1  {"type":"meta", "name", "playbook", "title", "duration_ms", "expect": {...}}
  others  {"type":"utterance", "speaker":"agent|customer", "t_start_ms", "t_end_ms",
           "text", "words":[{"w","s","e"}]}

ReplayFrameSource emits 100 ms frames for one channel at real pace (divided by speed).
ScriptDecoder plays the role of the model: a word becomes visible `lag_ms` after its
end, so partials grow at a realistic cadence. The same StreamingSession and
LocalAgreement-2 code path runs as in mic mode.
"""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, AsyncIterator

from stt.base import Frame, Window, Word

FRAME_MS = 100


@dataclass
class ScriptUtterance:
    speaker: str
    t_start_ms: int
    t_end_ms: int
    text: str
    words: list[Word]


@dataclass
class ReplayScript:
    name: str
    playbook: str
    title: str
    duration_ms: int
    utterances: list[ScriptUtterance]
    expect: dict[str, Any] = field(default_factory=dict)
    path: Path | None = None

    def words_for(self, speaker: str) -> list[Word]:
        out: list[Word] = []
        for u in self.utterances:
            if u.speaker == speaker:
                out.extend(u.words)
        return sorted(out, key=lambda w: w.s)

    def spans_for(self, speaker: str) -> list[tuple[int, int]]:
        return [(u.t_start_ms, u.t_end_ms) for u in self.utterances if u.speaker == speaker]


def load_script(path: Path) -> ReplayScript:
    meta: dict[str, Any] | None = None
    utts: list[ScriptUtterance] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        rec = json.loads(line)
        if rec.get("type") == "meta":
            meta = rec
        elif rec.get("type") == "utterance":
            utts.append(ScriptUtterance(
                speaker=rec["speaker"],
                t_start_ms=int(rec["t_start_ms"]),
                t_end_ms=int(rec["t_end_ms"]),
                text=rec["text"],
                words=[Word(w=w["w"], s=int(w["s"]), e=int(w["e"])) for w in rec.get("words", [])],
            ))
    if meta is None:
        raise ValueError(f"{path}: missing meta line")
    utts.sort(key=lambda u: u.t_start_ms)
    return ReplayScript(
        name=meta.get("name", path.stem),
        playbook=meta["playbook"],
        title=meta.get("title", path.stem),
        duration_ms=int(meta["duration_ms"]),
        utterances=utts,
        expect=meta.get("expect", {}),
        path=path,
    )


class ReplayFrameSource:
    def __init__(self, script: ReplayScript, speaker: str, speed: float = 1.0) -> None:
        if speed <= 0:
            raise ValueError("speed must be > 0")
        self.script = script
        self.speaker = speaker
        self.speed = speed
        self._spans = script.spans_for(speaker)

    def _speaking(self, t_ms: int) -> bool:
        return any(a <= t_ms < b for a, b in self._spans)

    async def frames(self) -> AsyncIterator[Frame]:
        start = time.monotonic()
        t = 0
        while t < self.script.duration_ms:
            yield Frame(t_ms=float(t), wall=time.monotonic(), dur_ms=FRAME_MS, speech=self._speaking(t))
            t += FRAME_MS
            target = start + (t / 1000.0) / self.speed
            delay = target - time.monotonic()
            if delay > 0:
                await asyncio.sleep(delay)


class ScriptDecoder:
    """Stands in for the model. Words become visible `lag_ms` after they end."""

    def __init__(self, words: list[Word], lag_ms: int = 250) -> None:
        self._words = words
        self._lag = lag_ms

    async def decode(self, window: Window) -> list[Word]:
        visible_until = window.end_ms if window.final else window.end_ms - self._lag
        return [w for w in self._words if w.s >= window.start_ms - 1 and w.e <= visible_until]
