"""Live measurement client: drive one replay through a running server and report latencies.

Usage (server running on :8000 in MODE=local with DATABRICKS_CONFIG_PROFILE=e2-demo):
  python tests/app/live_replay.py advisor_coach fee_objection --speed 1.0 --base http://127.0.0.1:8000

Records server metrics (SPEC section 7) and client-side timings (customer final received -> card.start and
card.done received), then fetches the snapshot for perception parse modes, late verdicts and retrieval.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import time
from collections import Counter
from typing import Any

import httpx
import websockets


def pct(values: list[float], p: float) -> float | None:
    if not values:
        return None
    values = sorted(values)
    k = max(0, min(len(values) - 1, round(p / 100 * (len(values) - 1))))
    return values[k]


def summary(name: str, values: list[float]) -> str:
    if not values:
        return f"{name:22s} n=0"
    return (f"{name:22s} n={len(values):3d} p50={statistics.median(values):8.0f} ms  "
            f"p90={pct(values, 90):8.0f} ms  max={max(values):8.0f} ms")


async def run(base: str, playbook: str, script: str, speed: float) -> dict[str, Any]:
    ws_base = base.replace("http://", "ws://").replace("https://", "wss://")
    async with httpx.AsyncClient(base_url=base, timeout=30) as http:
        r = await http.post("/api/calls", json={"playbook": playbook, "mode": "replay", "script": script, "speed": speed})
        r.raise_for_status()
        call_id = r.json()["call_id"]
        print(f"call {call_id} playbook={playbook} script={script} speed={speed}")

    final_seen: dict[int, float] = {}          # every final (customer and agent), by seq
    customer_final: dict[int, float] = {}
    card_start_seen: dict[str, tuple[int, float]] = {}
    card_done_seen: dict[str, tuple[int, float]] = {}
    events: list[dict[str, Any]] = []
    t0 = time.monotonic()
    async with websockets.connect(f"{ws_base}/ws/call/{call_id}", max_size=None) as ws:
        while True:
            ev = json.loads(await ws.recv())
            now = time.monotonic()
            ev["_recv"] = now
            events.append(ev)
            if ev["type"] == "transcript.final":
                final_seen[ev["seq"]] = now
                if ev["speaker"] == "customer":
                    customer_final[ev["seq"]] = now
            if ev["type"] == "card.start":
                card_start_seen.setdefault(ev["card"]["card_id"], (ev["card"]["seq"], now))
            if ev["type"] == "card.done":
                card_done_seen.setdefault(ev["card"]["card_id"], (ev["card"]["seq"], now))
            if ev["type"] == "call.ended":
                break
    wall = time.monotonic() - t0

    metrics = [e for e in events if e["type"] == "metrics"]
    cards = [e["card"] for e in events if e["type"] == "card.start"]
    done = [e["card"] for e in events if e["type"] == "card.done"]
    finals = [e for e in events if e["type"] == "transcript.final"]

    async with httpx.AsyncClient(base_url=base, timeout=30) as http:
        snap = (await http.get(f"/api/calls/{call_id}")).json()

    def col(key: str) -> list[float]:
        return [float(m[key]) for m in metrics if m.get(key) is not None]

    # Client-side: customer final -> card.start / card.done for every card (the SPEC 7 budget).
    cs_ms = [(t - customer_final[seq]) * 1000 for cid, (seq, t) in card_start_seen.items() if seq in customer_final]
    cd_ms = [(t - customer_final[seq]) * 1000 for cid, (seq, t) in card_done_seen.items() if seq in customer_final]
    # Turns that produced a card at all, against every customer final.
    print(f"\nwall-clock {wall:.1f}s, {len(finals)} finals, {len(cards)} cards, {len(events)} events")
    print("\n-- server metrics (SPEC 7) --")
    for key in ("stt_first_word_ms", "stt_commit_ms", "rtm_ms", "card_ms", "card_done_ms"):
        print(summary(key, col(key)))
    print(summary("client customer-final->card.start", cs_ms))
    print(summary("client customer-final->card.done", cd_ms))
    print("card paths:", Counter(m.get("card_path") for m in metrics if m.get("card_ms") is not None))
    print("\n-- cards --")
    print(Counter(c["kind"] for c in cards))
    for c in done:
        print(f"  [{c['kind']:18s}] conf={c['confidence']:.2f} {c['title']!r}: {c['body'][:110]!r}")

    decisions = snap.get("decisions", [])
    customer_turns = [d for d in decisions if d.get("customer_turn")]
    print("\n-- decisions --")
    print("compose modes:", Counter(m.get("compose_mode") for m in metrics if m.get("compose_mode")))
    print("perception parse:", Counter((d.get("perception") or {}).get("parse", "pending") for d in customer_turns))
    perc_err: Counter = Counter()
    for d in decisions:
        for k, v in ((d.get("perception") or {}).get("errors") or {}).items():
            perc_err[(k, v[:40])] += 1
    print("perception errors:", dict(perc_err) or "none")
    perc_lat = [float(((d.get("perception") or {}).get("latency_ms") or {}).get("perception"))
                for d in decisions if ((d.get("perception") or {}).get("latency_ms") or {}).get("perception") is not None]
    print(summary("perception call (haiku)", perc_lat))
    print("perception path at decide:", Counter(d.get("perception_path") for d in customer_turns))
    print("late verdicts:", Counter(d.get("verdict") for d in decisions if d.get("kind") == "late_perception"))
    print(summary("decided_after_final", [float(d["decided_after_final_ms"]) for d in customer_turns if d.get("decided_after_final_ms") is not None]))
    print("retrieval backends:", Counter((d.get("retrieval") or {}).get("backend") for d in customer_turns),
          "speculative:", sum(1 for d in customer_turns if (d.get("retrieval") or {}).get("speculative")),
          "fallback:", sum(1 for d in customer_turns if (d.get("retrieval") or {}).get("fallback")))
    print("silent turns:", sum(1 for d in customer_turns if d.get("silent")), "of", len(customer_turns))
    print("coverage:", [(c["key"], c["covered"]) for c in snap.get("coverage", [])])
    return {
        "playbook": playbook, "script": script,
        "card_start_ms": cs_ms, "card_done_ms": cd_ms,
        "server_card_ms": col("card_ms"), "server_card_done_ms": col("card_done_ms"),
    }


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("playbook")
    p.add_argument("script")
    p.add_argument("--speed", type=float, default=1.0)
    p.add_argument("--base", default="http://127.0.0.1:8000")
    a = p.parse_args()
    asyncio.run(run(a.base, a.playbook, a.script, a.speed))


if __name__ == "__main__":
    main()
