"""Structured voice logs (one JSON object per line). Session and turn ids are present wherever they apply.

Transcript text never goes to INFO or above: the fields here are ids, counts, states and timings only.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Optional

log = logging.getLogger("sidekick.voice")


def vlog(event: str, *, session_id: Optional[str] = None, turn: Optional[int] = None,
         level: int = logging.INFO, **fields: Any) -> None:
    payload: dict[str, Any] = {"event": f"voice.{event}", "session_id": session_id, "turn": turn}
    payload.update({k: v for k, v in fields.items() if v is not None})
    log.log(level, json.dumps(payload, default=str, sort_keys=True))
