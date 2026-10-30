"""End-to-end voice smoke test against a running Sidekick (local or the deployed Databricks App).

Speaks a sentence (macOS `say`, or any 16 kHz mono WAV via --wav) into /ws/voice in streaming mode at
real-time pace, then records what comes back: live captions, live read, provisional suggestions, the reply
text, the reply audio (saved as a WAV you can play) and the turn timing.

  .venv/bin/python scripts/voice_smoke.py --profile realtime-call-sidekick            # deployed app
  .venv/bin/python scripts/voice_smoke.py --base http://localhost:8000                  # local
  ... --role practice --scenario fee_objection --text "Our fee is one percent of assets."
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import os
import subprocess
import sys
import tempfile
import time
import wave
from collections import Counter

import httpx
import websockets

FRAME_MS = 100


def speech_pcm(text: str, wav_path: str | None) -> bytes:
    if not wav_path:
        tmp = tempfile.mkdtemp()
        aiff, wav_path = os.path.join(tmp, "say.aiff"), os.path.join(tmp, "say.wav")
        subprocess.run(["say", "-o", aiff, text], check=True)
        subprocess.run(["afconvert", "-f", "WAVE", "-d", "LEI16@16000", "-c", "1", aiff, wav_path], check=True)
    with wave.open(wav_path, "rb") as w:
        if (w.getframerate(), w.getnchannels(), w.getsampwidth()) != (16000, 1, 2):
            sys.exit(f"{wav_path}: need 16 kHz mono PCM16")
        return w.readframes(w.getnframes())


def app_target(profile: str) -> tuple[str, dict[str, str]]:
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient(profile=profile)
    app = w.apps.get(os.environ.get("SIDEKICK_APP", "call-sidekick"))
    return app.url.rstrip("/"), w.config.authenticate()


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", help="Databricks CLI profile: test the deployed app")
    ap.add_argument("--base", default="http://localhost:8000", help="local server (ignored with --profile)")
    ap.add_argument("--role", default="assistant", choices=["assistant", "practice"])
    ap.add_argument("--playbook", default="advisor_coach")
    ap.add_argument("--scenario", default="fee_objection")
    ap.add_argument("--text", default="I have a first meeting with a couple who are worried about fees. "
                                      "What should I ask them, and what should I follow up on afterwards?")
    ap.add_argument("--wav", help="use this 16 kHz mono WAV instead of macOS say")
    ap.add_argument("--out", default="/tmp/sidekick-reply.wav")
    ap.add_argument("--timeout", type=float, default=240, help="seconds to wait for the reply (cold GPUs)")
    args = ap.parse_args()

    base, headers = app_target(args.profile) if args.profile else (args.base.rstrip("/"), {})
    pcm = speech_pcm(args.text, args.wav)
    body = {"role": args.role, "playbook": args.playbook}
    if args.role == "practice":
        body["scenario"] = args.scenario
    async with httpx.AsyncClient(headers=headers, timeout=60, follow_redirects=False) as http:
        r = await http.post(f"{base}/api/voice/sessions", json=body)
        if r.status_code >= 300:
            sys.exit(f"create session: HTTP {r.status_code} {r.text[:300]}")
        session = r.json()
    ws_url = base.replace("https://", "wss://").replace("http://", "ws://") + f"/ws/voice/{session['session_id']}"
    print(f"session {session['session_id']} ({args.role}) → {ws_url.split('/ws/')[0]}")

    counts, partials, chips, statuses = Counter(), [], [], []
    reply, audio, live, timing, user_text = "", bytearray(), None, None, None
    rate, turns_done, t_speech_end = 24000, 0, None
    want_turns = 2 if args.role == "practice" else 1  # practice: the customer's opening line is turn 0

    async with websockets.connect(ws_url, additional_headers=headers, max_size=None, open_timeout=60) as ws:
        async def speak() -> None:
            nonlocal t_speech_end
            await ws.send(json.dumps({"type": "stream_start", "sample_rate": 16000}))
            step = 16000 * 2 * FRAME_MS // 1000
            for i in range(0, len(pcm), step):
                await ws.send(pcm[i:i + step])
                await asyncio.sleep(FRAME_MS / 1000)
            t_speech_end = time.monotonic()
            silence = b"\x00\x00" * (16000 * FRAME_MS // 1000)
            for _ in range(15):  # 1.5 s of silence lets the server endpoint the turn
                await ws.send(silence)
                await asyncio.sleep(FRAME_MS / 1000)

        # assistant: speak right away; practice: wait for the AI customer's opening line (turn 0) to finish
        speaker = asyncio.create_task(speak()) if args.role == "assistant" else None
        deadline = time.monotonic() + args.timeout
        while turns_done < want_turns and time.monotonic() < deadline:
            try:
                m = json.loads(await asyncio.wait_for(ws.recv(), max(1.0, deadline - time.monotonic())))
            except asyncio.TimeoutError:
                break
            kind = m.get("type")
            counts[kind] += 1
            mine = m.get("turn", 0) >= want_turns - 1  # events of the turn our speech starts
            if kind == "user_partial":
                partials.append(m["text"])
            elif kind == "user_text":
                user_text = m["text"]
            elif kind == "live":
                live = m
            elif kind == "suggestion":
                chips.append((m["item"]["kind"], m["item"]["text"]))
            elif kind == "status":
                statuses.append(f'{m.get("stage")}:{m.get("state")}')
            elif kind == "agent_text" and mine:
                reply += m["delta"]
            elif kind == "audio" and mine:
                rate = m.get("rate", rate)
                audio += base64.b64decode(m["pcm_b64"])
            elif kind == "error":
                print(f'  error [{m.get("stage")}]: {str(m.get("message"))[:200]}')
            elif kind == "turn_done":
                turns_done += 1
                if mine:
                    timing = m.get("timing")
                if speaker is None:
                    speaker = asyncio.create_task(speak())
        if speaker and not speaker.done():
            speaker.cancel()

    if audio:
        with wave.open(args.out, "wb") as w:
            w.setnchannels(1), w.setsampwidth(2), w.setframerate(rate), w.writeframes(bytes(audio))
    print("events:", dict(counts))
    print("statuses:", statuses or "-")
    print("live captions:", len(partials), "| last:", (partials[-1][:90] if partials else "-"))
    print("heard:", user_text)
    if live:
        print(f'live read: sentiment={live.get("sentiment")} intent={live.get("intent")} signals={live.get("signals")}')
    print("live suggestions:", chips or "-")
    print("reply:", reply.strip()[:300])
    print(f"reply audio: {len(audio) / 2 / rate:.1f} s → {args.out}" if audio else "reply audio: none")
    print("timing:", timing)
    sys.exit(0 if (user_text and reply and audio) else 1)


if __name__ == "__main__":
    asyncio.run(main())
