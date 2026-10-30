"""Kokoro SSE client (SPEC 3.3a): header skip, base64 PCM, DONE, errors, closing the stream. Fake httpx transport."""

import asyncio
import base64
import json

import httpx
import pytest
from voice.tts import VOICES, KokoroTts, PcmParser, TtsUnavailable, voice_catalog


def sse(*events: str) -> bytes:
    return "".join(f"data: {e}\n\n" for e in events).encode()


def chunk(content: str, finish=None) -> str:
    return json.dumps({"object": "chat.completion.chunk", "choices": [{"index": 0,
                       "delta": {"role": "assistant", "content": content}, "finish_reason": finish}]})


HEADER = "#audio/pcm;rate=24000;channels=1;format=s16le;opts=voice;inflight=1\n"
PCM_A = b"\x01\x00\x02\x00"
PCM_B = b"\x03\x00"


def tts_with(body: bytes, status: int = 200, seen: dict | None = None) -> KokoroTts:
    def handler(request: httpx.Request) -> httpx.Response:
        if seen is not None:
            seen["url"] = str(request.url)
            seen["body"] = json.loads(request.read())
            seen["auth"] = request.headers.get("authorization")
        return httpx.Response(status, content=body, headers={"content-type": "text/event-stream"})

    async def auth():
        return "https://ws.example.com", {"Authorization": "Bearer test"}

    return KokoroTts(auth, "sidekick-kokoro", transport=httpx.MockTransport(handler))


def test_parser_skips_header_decodes_pcm_and_ignores_done():
    p = PcmParser()
    lines = [f"data: {json.dumps({'choices': [{'delta': {'content': HEADER}}]})}", "",
             f"data: {chunk(base64.b64encode(PCM_A).decode())}", "",
             f"data: {chunk(base64.b64encode(PCM_B).decode(), 'stop')}", "", "data: [DONE]", ""]
    out = [c for line in lines for c in p.feed(line)]
    assert [(c.rate, c.pcm) for c in out] == [(24000, PCM_A), (24000, PCM_B)]
    assert p.header_seen


def test_parser_takes_rate_from_header():
    p = PcmParser()
    p.feed("data: " + chunk("#audio/pcm;rate=16000;channels=1;format=s16le\n"))
    out = p.feed("data: " + chunk(base64.b64encode(PCM_A).decode()))
    assert out[0].rate == 16000


def test_parser_raises_on_error_event_and_skips_junk():
    p = PcmParser()
    assert p.feed("data: not-json") == []
    assert p.feed(": keep-alive") == []
    with pytest.raises(TtsUnavailable):
        p.feed('data: {"error": {"message": "unknown voice xx"}}')


def test_stream_over_fake_transport_yields_pcm_and_sends_expected_request():
    seen = {}
    body = sse(chunk(HEADER), chunk(base64.b64encode(PCM_A).decode()),
               chunk(base64.b64encode(PCM_B).decode(), "stop"), "[DONE]")
    tts = tts_with(body, seen=seen)

    async def go():
        return [c async for c in tts.stream("Your balance is forty two dollars.", "af_heart")]

    chunks = asyncio.run(go())
    assert [c.pcm for c in chunks] == [PCM_A, PCM_B]
    assert seen["url"] == "https://ws.example.com/serving-endpoints/sidekick-kokoro/invocations"
    assert seen["auth"] == "Bearer test"
    assert seen["body"] == {"messages": [{"role": "user", "content": "Your balance is forty two dollars."}],
                            "voice": "af_heart", "stream": True}


def test_http_error_is_tts_unavailable():
    tts = tts_with(b'{"error": {"message": "model is still loading"}}', status=503)

    async def go():
        return [c async for c in tts.stream("hi", "af_heart")]

    with pytest.raises(TtsUnavailable):
        asyncio.run(go())


def test_closing_a_stream_early_closes_the_http_response():
    closed = {"flag": False}
    body = sse(chunk(HEADER), chunk(base64.b64encode(PCM_A).decode()), chunk(base64.b64encode(PCM_B).decode()), "[DONE]")

    class Watch(httpx.AsyncByteStream):
        def __init__(self, data):
            self._data = data

        async def __aiter__(self):
            yield self._data

        async def aclose(self):
            closed["flag"] = True

    def handler(request):
        return httpx.Response(200, stream=Watch(body))

    async def auth():
        return "https://ws.example.com", {}

    tts = KokoroTts(auth, "sidekick-kokoro", transport=httpx.MockTransport(handler))

    async def go():
        gen = tts.stream("hi", "af_heart")
        first = await gen.__anext__()
        await gen.aclose()           # caller stops early (barge-in): the response must be closed
        return first

    first = asyncio.run(go())
    assert first.pcm == PCM_A
    assert closed["flag"] is True


def test_voice_list_is_labelled_and_default_voice_is_present():
    cat = voice_catalog()
    assert len(cat) == len(VOICES)
    heart = next(v for v in cat if v["id"] == "af_heart")
    assert heart == {"id": "af_heart", "label": "Heart (US F)"}
    assert "af_heart" in VOICES and "bm_george" in VOICES
