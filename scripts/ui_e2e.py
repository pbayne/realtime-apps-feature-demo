"""Browser end-to-end test of the deployed Sidekick voice console (headless Chrome over CDP).

Chrome's microphone is replaced by a recording of real speech (getUserMedia shim), so the whole browser path
runs as it does for a person: getUserMedia -> AudioWorklet -> streamed PCM -> Parakeet -> live caption, live read,
suggestion chips -> spoken reply. The test then clicks the controls a user has: pin and dismiss a chip, Esc to
interrupt Sidekick, M to mute and unmute, push-to-talk with Space. Auth is the CLI profile's OAuth token, sent as
a header on every request (Databricks Apps accept bearer tokens).

  say -o q.aiff "..." && afconvert -f WAVE -d LEI16@48000 -c 1 q.aiff q.wav
  .venv/bin/python scripts/ui_e2e.py --profile realtime-call-sidekick --wav q.wav

Exit code 0 only if every check passes. Screenshots go to --out.
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

import httpx
import websockets

CHROME = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"

# Chrome's --use-file-for-fake-audio-capture yields digital silence in this headless build (verified with a raw
# getUserMedia + AnalyserNode probe), so replace only the device: getUserMedia returns a MediaStream that plays the
# speech WAV after a short lead-in, then stays silent. The app's capture path (AudioWorklet, resampling, VAD,
# streaming) is unchanged.
MIC_SHIM = """(() => {
  const WAV_B64 = "%s";
  const orig = navigator.mediaDevices.getUserMedia.bind(navigator.mediaDevices);
  navigator.mediaDevices.getUserMedia = async (c) => {
    if (!c || !c.audio) return orig(c);
    const ctx = new AudioContext();
    await ctx.resume();
    const bytes = Uint8Array.from(atob(WAV_B64), (ch) => ch.charCodeAt(0));
    const buf = await ctx.decodeAudioData(bytes.buffer);
    const dest = ctx.createMediaStreamDestination();
    // Speak once: the app re-acquires the mic on unmute / mode switch, and a real user would not repeat the
    // question then. Later acquisitions get a silent stream.
    if (!window.__sidekickSpoke) {
      window.__sidekickSpoke = true;
      const src = ctx.createBufferSource();
      src.buffer = buf; src.connect(dest); src.start(ctx.currentTime + %s);
      window.__sidekickMicEndsAt = performance.now() + (%s + buf.duration) * 1000;
    }
    return dest.stream;
  };
})();"""

PROBE = """(() => {
  const q = (s) => [...document.querySelectorAll(s)];
  const text = (el) => (el && typeof el.innerText === "string" ? el.innerText.trim() : "");
  const orb = q('p[aria-live="polite"]').map(text).find((t) => t.length && t.length < 40) || "";
  const hearing = q("section, div").find((el) => el.firstElementChild && /^HEARING$/i.test(text(el.firstElementChild)));
  return {
    orb,
    caption: hearing ? text(hearing).replace(/^HEARING\\s*/i, "") : "",
    chips: q('button[aria-label^="Pin suggestion:"]').map((b) => b.getAttribute("aria-label").slice(16)),
    outcomes: q("section").filter((s) => /Outcomes/.test(text(s.querySelector("h2, summary") || s))).map(text).join(" | "),
    cutOff: document.body.innerText.includes("cut off"),
    muteBtn: (q("button").find((b) => /^(Mute|Unmute)/.test(text(b))) || {}).innerText || "",
    sidekickTurns: q("div").filter((d) => /^SIDEKICK\\b/.test(text(d.firstElementChild || {}))).length,
    tone: (document.querySelector('section[aria-label="Live read"] [role="img"]') || {getAttribute: () => ""}).getAttribute("aria-label") || "",
    toneWhileSpeaking: text(document.querySelector('section[aria-label="Live read"]')).includes("while speaking"),
  };
})()"""


class Cdp:
    def __init__(self, ws):
        self.ws, self.n, self.console = ws, 0, []

    async def call(self, method, **params):
        self.n += 1
        mid = self.n
        await self.ws.send(json.dumps({"id": mid, "method": method, "params": params}))
        while True:
            m = json.loads(await self.ws.recv())
            if m.get("method") == "Runtime.exceptionThrown":
                d = m["params"]["exceptionDetails"]
                self.console.append("EXC " + str(d.get("exception", {}).get("description", d.get("text")))[:300])
            elif m.get("method") == "Runtime.consoleAPICalled" and m["params"]["type"] == "error":
                self.console.append(" ".join(str(a.get("value", a.get("description", "")))[:200] for a in m["params"]["args"]))
            if m.get("id") == mid:
                return m.get("result", {})

    async def js(self, expr):
        r = await self.call("Runtime.evaluate", expression=expr, awaitPromise=True, returnByValue=True)
        return r.get("result", {}).get("value")

    async def key(self, key, code, vk, typ="both"):
        base = {"key": key, "code": code, "windowsVirtualKeyCode": vk, "nativeVirtualKeyCode": vk}
        if typ in ("down", "both"):
            await self.call("Input.dispatchKeyEvent", type="keyDown", **base)
        if typ in ("up", "both"):
            await self.call("Input.dispatchKeyEvent", type="keyUp", **base)

    async def shot(self, path):
        r = await self.call("Page.captureScreenshot", format="png")
        with open(path, "wb") as f:
            f.write(base64.b64decode(r["data"]))


async def run(args) -> int:
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient(profile=args.profile)
    app = w.apps.get(args.app).url.rstrip("/")
    auth = w.config.authenticate()
    os.makedirs(args.out, exist_ok=True)
    port = 9340
    proc = subprocess.Popen([CHROME, "--headless=new", f"--remote-debugging-port={port}",
                             f"--user-data-dir={tempfile.mkdtemp()}", "--window-size=1440,900",
                             "--use-fake-ui-for-media-stream",
                             *([] if args.real_mic else ["--use-fake-device-for-media-stream"]),
                             "--autoplay-policy=no-user-gesture-required", "about:blank"],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    checks: dict[str, bool] = {}
    notes: dict[str, str] = {}
    speaker = None
    try:
        for _ in range(60):
            try:
                tabs = httpx.get(f"http://127.0.0.1:{port}/json").json()
                break
            except Exception:
                await asyncio.sleep(0.2)
        page = next(t for t in tabs if t["type"] == "page")
        async with websockets.connect(page["webSocketDebuggerUrl"], max_size=None) as ws:
            cdp = Cdp(ws)
            for d in ("Network", "Runtime", "Page"):
                await cdp.call(f"{d}.enable")
            await cdp.call("Network.setExtraHTTPHeaders", headers=auth)
            if not args.real_mic:
                with open(args.wav, "rb") as f:
                    wav_b64 = base64.b64encode(f.read()).decode()
                await cdp.call("Page.addScriptToEvaluateOnNewDocument",
                               source=MIC_SHIM % (wav_b64, args.lead_in, args.lead_in))
            await cdp.call("Page.navigate", url=f"{app}/?autostart=1&voice=assistant&playbook={args.playbook}")
            t0 = time.monotonic()
            speaker = None
            if args.real_mic:
                for _ in range(80):
                    if (await cdp.js(PROBE) or {}).get("orb") == "Listening":
                        break
                    await asyncio.sleep(0.25)
                await asyncio.sleep(1.0)
                # Real acoustic path: the Mac speaks out loud and the built-in microphone hears it.
                speaker = subprocess.Popen(["say", "-r", "175", args.say])
            seen_speaking = seen_caption = None
            seen_tone = tone_while_speaking = None
            pinned = dismissed = None
            interrupted_at = None
            deadline = t0 + args.timeout
            # Phase 1: talk (the WAV plays as the mic) and watch the live UI; pin/dismiss chips while they show.
            while time.monotonic() < deadline:
                s = await cdp.js(PROBE) or {}
                t = time.monotonic() - t0
                if s.get("orb") == "You're speaking" and seen_speaking is None:
                    seen_speaking = t
                if len(s.get("caption", "")) > 12 and not s["caption"].startswith("Listening") and seen_caption is None:
                    seen_caption = t
                    notes["first live caption"] = f"{t:.1f} s: {s['caption'][:80]}"
                    await cdp.shot(f"{args.out}/1-live-caption.png")
                if s.get("tone", "").startswith("Tone ") and "waiting" not in s["tone"] and seen_tone is None:
                    seen_tone = t
                    notes["first sentiment"] = f"{t:.1f} s: {s['tone']}"
                if s.get("tone", "").startswith("Tone ") and seen_tone is not None:
                    notes["last sentiment"] = f"{t:.1f} s: {s['tone']}"
                if s.get("toneWhileSpeaking") and tone_while_speaking is None:
                    tone_while_speaking = t
                chips = s.get("chips", [])
                if chips and pinned is None:
                    pinned = chips[0]
                    await cdp.js(f'document.querySelector(\'button[aria-label="Pin suggestion: {pinned.replace(chr(39), "")}"]\')?.click()'
                                 if "'" not in pinned else
                                 '[...document.querySelectorAll(\'button[aria-label^="Pin suggestion:"]\')][0].click()')
                    notes["pinned"] = pinned[:90]
                    await cdp.shot(f"{args.out}/2-chips.png")
                elif len(chips) >= 1 and pinned is not None and dismissed is None and chips[0] != pinned:
                    dismissed = chips[0]
                    await cdp.js('[...document.querySelectorAll(\'button[aria-label^="Dismiss suggestion:"]\')][0].click()')
                    await asyncio.sleep(0.4)
                    after = (await cdp.js(PROBE) or {}).get("chips", [])
                    checks["dismiss removes the chip"] = dismissed not in after
                if s.get("orb") == "Sidekick speaking" and interrupted_at is None and s.get("sidekickTurns", 0) >= 1:
                    await asyncio.sleep(1.5)                         # let it talk, then barge in with Esc
                    await cdp.shot(f"{args.out}/3-sidekick-speaking.png")
                    await cdp.key("Escape", "Escape", 27)
                    interrupted_at = time.monotonic() - t0
                    for _ in range(20):
                        await asyncio.sleep(0.25)
                        s2 = await cdp.js(PROBE) or {}
                        if s2.get("cutOff") or s2.get("orb") in ("Interrupted", "Listening"):
                            break
                    checks["Esc interrupts Sidekick"] = bool(s2.get("cutOff")) or s2.get("orb") in ("Interrupted", "Listening")
                    notes["after Esc"] = f"orb={s2.get('orb')!r} cut_off={s2.get('cutOff')}"
                    break
                await asyncio.sleep(0.25)
            checks["mic → 'You're speaking'"] = seen_speaking is not None
            checks["live caption while talking"] = seen_caption is not None
            checks["live sentiment reading"] = seen_tone is not None
            checks["sentiment updates while still speaking"] = tone_while_speaking is not None
            checks["suggestion chip appeared and was pinned"] = pinned is not None
            timeline = []
            for _ in range(25):                                   # orb after Esc, every 100 ms
                timeline.append((await cdp.js(PROBE) or {}).get("orb", "?"))
                await asyncio.sleep(0.1)
            notes["orb after Esc"] = " > ".join(o for i, o in enumerate(timeline) if i == 0 or o != timeline[i - 1])
            s = await cdp.js(PROBE) or {}
            if pinned:
                checks["pinned chip is filed as an outcome"] = pinned[:40] in s.get("outcomes", "")
            checks["Sidekick replied (spoke)"] = s.get("sidekickTurns", 0) >= 1
            # Phase 2: keyboard controls.
            await cdp.key("m", "KeyM", 77)
            await asyncio.sleep(0.5)
            s = await cdp.js(PROBE) or {}
            checks["M mutes"] = s.get("muteBtn", "").startswith("Unmute") and s.get("orb") == "Muted"
            await cdp.key("m", "KeyM", 77)
            await asyncio.sleep(0.5)
            s = await cdp.js(PROBE) or {}
            checks["M unmutes"] = s.get("muteBtn", "").startswith("Mute")
            await cdp.js('[...document.querySelectorAll("button")].find(b => /Push to talk/.test(b.innerText))?.click()')
            await asyncio.sleep(0.5)
            s = await cdp.js(PROBE) or {}
            checks["Push-to-talk mode"] = s.get("orb") == "Hold to talk"
            await cdp.key(" ", "Space", 32, "down")
            await asyncio.sleep(0.6)
            held = (await cdp.js(PROBE) or {}).get("orb")
            await cdp.key(" ", "Space", 32, "up")
            await asyncio.sleep(0.6)
            released = (await cdp.js(PROBE) or {}).get("orb")
            checks["Space holds to talk"] = held != "Hold to talk" and released in ("Hold to talk", "Thinking", "Listening")
            notes["space"] = f"held={held!r} released={released!r}"
            await cdp.shot(f"{args.out}/4-final.png")
            checks["no console errors"] = not cdp.console
            if cdp.console:
                notes["console"] = " || ".join(cdp.console[:5])
    finally:
        if speaker is not None and speaker.poll() is None:
            speaker.terminate()
        proc.terminate()
    width = max(len(k) for k in checks)
    for k, ok in checks.items():
        print(f"{'PASS' if ok else 'FAIL'}  {k.ljust(width)}")
    for k, v in notes.items():
        print(f"      {k}: {v}")
    print(f"screenshots: {args.out}")
    return 0 if checks and all(checks.values()) else 1


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", required=True)
    ap.add_argument("--app", default="call-sidekick")
    ap.add_argument("--wav", help="speech WAV (any rate) played as the mic after --lead-in seconds")
    ap.add_argument("--real-mic", action="store_true",
                    help="use the machine's real default microphone and speak --say aloud through the speakers")
    ap.add_argument("--say", default="Hi Sidekick. I have a first meeting tomorrow with a couple who are worried about "
                                     "fees. What should I ask them, and what should I follow up on afterwards?")
    ap.add_argument("--lead-in", type=float, default=3.0)
    ap.add_argument("--playbook", default="advisor_coach")
    ap.add_argument("--timeout", type=float, default=90)
    ap.add_argument("--out", default="/tmp/sidekick-ui-e2e")
    args = ap.parse_args()
    if not args.real_mic and not args.wav:
        ap.error("--wav is required unless --real-mic")
    sys.exit(asyncio.run(run(args)))


if __name__ == "__main__":
    main()
