"""Shared STT types: frames in, words and transcript events out."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, Protocol


@dataclass
class Frame:
    """One audio (or simulated) frame on a single channel.

    t_ms is stream time (audio clock, ms since the channel started). wall is
    time.monotonic() when the frame was produced, used for latency metrics.
    pcm is PCM16 mono 16 kHz (None for replay). speech overrides the VAD when set.
    """

    t_ms: float
    wall: float
    dur_ms: float = 100.0
    pcm: Optional[bytes] = None
    speech: Optional[bool] = None


@dataclass(frozen=True)
class Word:
    w: str
    s: int   # start, absolute stream ms
    e: int   # end, absolute stream ms


@dataclass
class Window:
    start_ms: int
    end_ms: int
    pcm: Optional[bytes]
    final: bool


class Decoder(Protocol):
    async def decode(self, window: Window) -> list[Word]: ...


@dataclass
class TranscriptEvent:
    kind: str                       # "partial" | "final"
    speaker: str
    seq: int
    text: str
    t_start_ms: int
    t_end_ms: int
    first_word_ms: Optional[float] = None   # final: wall ms from speech onset to first visible word
    commit_ms: Optional[float] = None       # final: wall ms from last speech frame to final emit
    words: list[Word] = field(default_factory=list)

    def to_wire(self) -> dict:
        d = {"type": f"transcript.{self.kind}", "speaker": self.speaker, "text": self.text, "seq": self.seq}
        if self.kind == "final":
            d["latency_ms"] = round(self.commit_ms or 0.0)
        return d
