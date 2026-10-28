"""Pass-through for browser Web Speech API results (local fallback when no STT endpoint).

Client message on /ws/audio/{id} (text frame):
  {"type":"result", "text":"...", "is_final":true|false, "t_start_ms":0, "t_end_ms":0}
Each result maps to a TranscriptEvent. A final closes the current seq; the next
result starts a new utterance and gets a new seq.
"""

from __future__ import annotations

import time
from typing import Callable, Optional

from stt.base import TranscriptEvent


class BrowserPassthrough:
    def __init__(self, speaker: str, next_seq: Callable[[], int]) -> None:
        self.speaker = speaker
        self._next_seq = next_seq
        self._open_seq: Optional[int] = None
        self._started: Optional[float] = None

    def handle(self, msg: dict) -> Optional[TranscriptEvent]:
        text = (msg.get("text") or "").strip()
        if not text:
            return None
        is_final = bool(msg.get("is_final"))
        if self._open_seq is None:
            self._open_seq = self._next_seq()
            self._started = time.monotonic()
        seq = self._open_seq
        t_start = int(msg.get("t_start_ms") or 0)
        t_end = int(msg.get("t_end_ms") or 0)
        first_word = (time.monotonic() - self._started) * 1000.0 if self._started else None
        if is_final:
            self._open_seq = None
            self._started = None
            return TranscriptEvent(kind="final", speaker=self.speaker, seq=seq, text=text,
                                   t_start_ms=t_start, t_end_ms=t_end, first_word_ms=first_word, commit_ms=0.0)
        return TranscriptEvent(kind="partial", speaker=self.speaker, seq=seq, text=text,
                               t_start_ms=t_start, t_end_ms=t_end, first_word_ms=first_word)
