"""Build replay JSONL from the dialogue sources in src/.

Usage:  python build_replay.py            (from src/app/data/replay; writes <playbook>__<name>.jsonl)

Source format (src/*.txt):
  # key: value       meta (name, playbook, title, expect: comma-separated card kinds)
  agent: text        one turn; speakers are "agent" and "customer"

Timing model (natural pace, ~145 wpm): each word lasts 0.16 + 0.03*len(word) s (clamped
0.2..0.6 s) followed by a 50 ms gap; sentence-final punctuation adds a 0.3 s pause; a
speaker change adds a 0.6 s pause, same-speaker turns 0.3 s. Output is dual-channel, sorted by
start time, with word-level timings for the replay decoder.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
SRC = HERE / "src"
START_MS = 1000
TAIL_MS = 1500


def parse(path: Path) -> dict:
    meta: dict[str, str] = {}
    turns: list[tuple[str, str]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("#"):
            k, _, v = line[1:].partition(":")
            meta[k.strip()] = v.strip()
            continue
        speaker, _, text = line.partition(":")
        speaker = speaker.strip().lower()
        if speaker not in ("agent", "customer"):
            raise ValueError(f"{path.name}: unknown speaker {speaker!r}")
        turns.append((speaker, text.strip()))
    return {"meta": meta, "turns": turns}


def build(path: Path) -> tuple[str, list[dict]]:
    parsed = parse(path)
    meta = parsed["meta"]
    t = START_MS
    prev_speaker = None
    utterances: list[dict] = []
    for speaker, text in parsed["turns"]:
        if prev_speaker is not None:
            t += 600 if speaker != prev_speaker else 300
        words = []
        t_start = t
        for tok in text.split():
            core = re.sub(r"[^\w']", "", tok)
            dur = min(600, max(200, int(160 + 30 * len(core))))
            words.append({"w": tok, "s": t, "e": t + dur})
            t += dur + 50
            if tok[-1:] in ".?!":
                t += 300
        utterances.append({
            "type": "utterance", "speaker": speaker, "t_start_ms": t_start,
            "t_end_ms": words[-1]["e"], "text": text, "words": words,
        })
        prev_speaker = speaker
    duration = utterances[-1]["t_end_ms"] + TAIL_MS
    name = meta["name"]
    playbook = meta["playbook"]
    header = {
        "type": "meta", "name": name, "playbook": playbook, "title": meta.get("title", name),
        "duration_ms": duration,
        "expect": {"cards": [c.strip() for c in meta.get("expect", "").split(",") if c.strip()]},
        "source": path.name,
    }
    return f"{playbook}__{name}.jsonl", [header, *utterances]


def main(argv: list[str]) -> int:
    sources = [Path(a) for a in argv] or sorted(SRC.glob("*.txt"))
    for src in sources:
        out_name, records = build(src)
        out = HERE / out_name
        with out.open("w", encoding="utf-8") as fh:
            for rec in records:
                fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
        dur = records[0]["duration_ms"] / 1000
        n_words = sum(len(r["words"]) for r in records[1:])
        print(f"{out_name}: {dur:.0f}s, {len(records) - 1} utterances, {n_words} words")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
