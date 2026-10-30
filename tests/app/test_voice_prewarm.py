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
