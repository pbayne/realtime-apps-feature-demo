#!/usr/bin/env python3
"""Warm up the voice path before a demo: ping the Parakeet (STT) and Kokoro (TTS) GPU endpoints until both
answer, and print the time each took to become ready.

Why: with gpu_scale_to_zero=true (the dev default) an endpoint that has been idle is scaled to zero, and the
first request waits for a cold GPU. The first voice turn after idle can take minutes (about 156 s was measured
for a cold whisper endpoint on e2-demo). Run this a few minutes before a demo. For real demos or latency
measurements, deploy with GPU_SCALE_TO_ZERO=false so the endpoints stay warm.

Usage:
  .venv/bin/python scripts/warmup_voice.py --profile <databricks-profile>
  .venv/bin/python scripts/warmup_voice.py --profile <p> --parakeet sidekick-parakeet --kokoro sidekick-kokoro --timeout-s 900

Needs databricks-sdk and httpx (both in tests/app/requirements-dev.txt or the app's requirements).
Sends no data beyond a half-second of silence and one short sentence. Makes no workspace changes.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import io
import json
import sys
import time
import wave

import httpx
from databricks.sdk import WorkspaceClient

SAMPLE_RATE = 16000


def silent_wav(seconds: float = 0.5) -> bytes:
    buf = io.BytesIO()
    with wave.open(buf, "wb") as w:
        w.setnchannels(1)
        w.setsampwidth(2)
        w.setframerate(SAMPLE_RATE)
        w.writeframes(b"\x00\x00" * int(SAMPLE_RATE * seconds))
    return buf.getvalue()


def ping_parakeet(host: str, headers: dict[str, str], endpoint: str) -> None:
    url = f"{host}/serving-endpoints/{endpoint}/audio/transcriptions"
    files = {"file": ("warmup.wav", silent_wav(), "audio/wav")}
    data = {"model": endpoint, "response_format": "verbose_json"}
    with httpx.Client(timeout=httpx.Timeout(120.0)) as client:
        r = client.post(url, headers=headers, files=files, data=data)
        r.raise_for_status()


def ping_kokoro(host: str, headers: dict[str, str], endpoint: str) -> None:
    url = f"{host}/serving-endpoints/{endpoint}/invocations"
    body = {"messages": [{"role": "user", "content": "Ready."}], "voice": "af_heart", "stream": True}
    with httpx.Client(timeout=httpx.Timeout(120.0)) as client:
        with client.stream("POST", url, headers={**headers, "Content-Type": "application/json"},
                           content=json.dumps(body)) as r:
            r.raise_for_status()
            for line in r.iter_lines():
                if line.strip() == "data: [DONE]":
                    break


def wait_until_ready(name: str, ping, timeout_s: float, poll_s: float) -> float:
    start = time.monotonic()
    attempt = 0
    while True:
        attempt += 1
        try:
            ping()
            elapsed = time.monotonic() - start
            print(f"[{name}] ready after {elapsed:.1f} s ({attempt} attempt{'s' if attempt > 1 else ''})", flush=True)
            return elapsed
        except Exception as exc:  # noqa: BLE001 - a cold or scaling endpoint returns 4xx/5xx until it is up
            if time.monotonic() - start > timeout_s:
                raise TimeoutError(f"{name} not ready after {timeout_s:.0f} s: {exc}") from exc
            print(f"[{name}] attempt {attempt} not ready yet ({str(exc)[:120]}); retrying in {poll_s:.0f} s",
                  flush=True)
            time.sleep(poll_s)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--profile", required=True, help="Databricks CLI profile for the target workspace")
    p.add_argument("--parakeet", default="sidekick-parakeet", help="Parakeet STT endpoint name")
    p.add_argument("--kokoro", default="sidekick-kokoro", help="Kokoro TTS endpoint name")
    p.add_argument("--timeout-s", type=float, default=900.0, help="give up after this many seconds per endpoint")
    p.add_argument("--poll-s", type=float, default=10.0, help="seconds between attempts")
    args = p.parse_args(argv)

    w = WorkspaceClient(profile=args.profile)
    host = w.config.host.rstrip("/")
    headers = dict(w.config.authenticate())
    print(f"warming {args.parakeet} and {args.kokoro} on {host}", flush=True)

    started = time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        futures = {
            pool.submit(wait_until_ready, args.parakeet, lambda: ping_parakeet(host, headers, args.parakeet),
                        args.timeout_s, args.poll_s): args.parakeet,
            pool.submit(wait_until_ready, args.kokoro, lambda: ping_kokoro(host, headers, args.kokoro),
                        args.timeout_s, args.poll_s): args.kokoro,
        }
        failed = False
        for fut in concurrent.futures.as_completed(futures):
            try:
                fut.result()
            except TimeoutError as exc:
                failed = True
                print(f"ERROR: {exc}", file=sys.stderr, flush=True)
    print(f"both endpoints checked in {time.monotonic() - started:.1f} s wall time", flush=True)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
