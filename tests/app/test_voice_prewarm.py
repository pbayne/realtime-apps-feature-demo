"""Opening a voice session wakes the scale-to-zero GPU endpoints and the voice model, throttled and fail-soft."""

import asyncio

from config import Settings
from services import Services
from voice_fakes import FakeVoiceLlm


class CountingTts:
    def __init__(self, fail=False):
        self.calls, self.fail = 0, fail

    async def stream(self, text, voice):
        self.calls += 1
        if self.fail:
            raise RuntimeError("cold")
        yield b"\x00\x00"

    async def aclose(self):
        pass


class CountingStt:
    def __init__(self):
        self.calls = 0

    async def transcribe(self, wav):
        self.calls += 1
        assert wav[:4] == b"RIFF"
        return ""


def test_prewarm_pings_each_endpoint_once_per_interval_and_never_raises():
    async def scenario():
        svc = Services(Settings(mode="local", llm_enabled=True, voice_prewarm_interval_s=60), llm=FakeVoiceLlm(["ok"]))
        svc.tts, svc.transcriber = CountingTts(fail=True), CountingStt()
        svc.prewarm_voice()
        svc.prewarm_voice()          # throttled: same interval
        await asyncio.gather(*list(svc._background))
        assert (svc.tts.calls, svc.transcriber.calls) == (1, 1)
        svc._last_prewarm -= 61      # interval elapsed
        svc.prewarm_voice()
        await asyncio.gather(*list(svc._background))
        assert (svc.tts.calls, svc.transcriber.calls) == (2, 2)

    asyncio.run(scenario())


class SlowOkTts(CountingTts):
    async def stream(self, text, voice):
        self.calls += 1
        await asyncio.sleep(0.05)
        yield b"\x00\x00"


def test_warmth_goes_cold_to_warming_to_warm_and_expires():
    async def scenario():
        svc = Services(Settings(mode="local", llm_enabled=True, voice_warm_window_s=60), llm=FakeVoiceLlm(["ok"]))
        svc.tts, svc.transcriber = SlowOkTts(), CountingStt()
        assert svc.voice_warmth()["state"] == "cold"
        svc.prewarm_voice(force=True)
        await asyncio.sleep(0)
        assert svc.voice_warmth()["endpoints"]["tts"]["state"] == "warming"
        svc.prewarm_voice(force=True)            # no second warm-up while one is running
        await asyncio.gather(*list(svc._background))
        assert svc.tts.calls == 1
        w = svc.voice_warmth()
        assert w["state"] == "warm", w
        assert w["endpoints"]["tts"]["last_ms"] is not None
        for name in ("stt", "tts", "llm"):       # older than the window: cold again
            svc._warmth[name]["last_ok_ms"] -= 61_000
        assert svc.voice_warmth()["state"] == "cold"
        svc.note_voice_ok("tts")                 # a real turn reached TTS
        assert svc.voice_warmth()["endpoints"]["tts"]["state"] == "warm"

    asyncio.run(scenario())


def test_warmup_api_starts_a_warmup_and_reports_progress(monkeypatch):
    import main
    from fastapi.testclient import TestClient

    settings = Settings(mode="local", llm_enabled=True)

    def factory(s):
        svc = Services(settings, llm=FakeVoiceLlm(["ok"]))
        svc.tts, svc.transcriber = SlowOkTts(), CountingStt()
        return svc

    monkeypatch.setattr(main, "Services", factory)
    monkeypatch.setattr(main, "get_settings", lambda: settings)
    with TestClient(main.app) as client:
        assert client.get("/api/voice/warmup").json()["state"] == "cold"
        r = client.post("/api/voice/warmup")
        assert r.status_code == 202 and r.json()["state"] in ("warming", "warm")
        for _ in range(100):
            if client.get("/api/voice/warmup").json()["state"] == "warm":
                break
            import time
            time.sleep(0.02)
        assert client.get("/api/voice/warmup").json()["state"] == "warm"
