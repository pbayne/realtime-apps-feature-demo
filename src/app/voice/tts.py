"""Kokoro TTS over the Custom LLM Serving endpoint (SPEC 3.3a, reference kokoro-tts/serving/adapter.py).

Request : POST {host}/serving-endpoints/{TTS_ENDPOINT}/invocations
          {"messages":[{"role":"user","content":"<one sentence>"}],"voice":"af_heart","stream":true}
Response: SSE chat.completion.chunk events. The first delta.content is a header
          "#audio/pcm;rate=24000;channels=1;format=s16le;...". Every later delta.content is base64 of raw
          s16le mono PCM. The stream ends with finish_reason "stop" and [DONE].

Cancellation: `stream()` is an async generator that holds the HTTP response open. Callers must close it
(`await gen.aclose()` in a finally) when they stop early, or an unread Kokoro stream can stall its worker
(SPEC 3.3a). KokoroTts.stream closes the response in its own `async with`, so aclose is enough.
"""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from typing import AsyncIterator, Awaitable, Callable, Optional

import httpx

DEFAULT_RATE = 24000

# Kokoro-82M presets (54 in total). Names confirmed against the reference audio-serving docs.
# ja/zh voices need extra misaki packages on the endpoint, so the list keeps them out of the UI.
VOICES: tuple[str, ...] = (
    "af_alloy", "af_aoede", "af_bella", "af_heart", "af_jessica", "af_kore", "af_nicole", "af_nova",
    "af_river", "af_sarah", "af_sky",
    "am_adam", "am_echo", "am_eric", "am_fenrir", "am_liam", "am_michael", "am_onyx", "am_puck", "am_santa",
    "bf_alice", "bf_emma", "bf_isabella", "bf_lily",
    "bm_daniel", "bm_fable", "bm_george", "bm_lewis",
    "ef_dora", "em_alex", "em_santa", "ff_siwis",
    "hf_alpha", "hf_beta", "hm_omega", "hm_psi",
    "if_sara", "im_nicola", "pf_dora", "pm_alex", "pm_santa",
)

AuthFn = Callable[[], Awaitable[tuple[str, dict[str, str]]]]

_LANG = {"a": "US", "b": "UK", "e": "ES", "f": "FR", "h": "HI", "i": "IT", "p": "PT", "j": "JA", "z": "ZH"}


def voice_catalog() -> list[dict[str, str]]:
    """Voices for the UI: {id, label} where label reads like 'Heart (US F)'."""
    out = []
    for vid in VOICES:
        lang_gender, name = vid.split("_", 1)
        region = _LANG.get(lang_gender[0], lang_gender[0].upper())
        gender = "F" if lang_gender[1] == "f" else "M"
        out.append({"id": vid, "label": f"{name.replace('_', ' ').title()} ({region} {gender})"})
    return out


class TtsUnavailable(RuntimeError):
    """The TTS endpoint failed for this sentence. The text still goes out; only its audio is lost.
    `cold` marks a failure of a warming (scale-to-zero) endpoint: voice mode retries those with backoff."""

    def __init__(self, message: str = "", *, cold: bool = False) -> None:
        super().__init__(message)
        self.cold = cold


@dataclass(frozen=True)
class PcmChunk:
    rate: int
    pcm: bytes


class PcmParser:
    """Line-by-line SSE parser for the Kokoro adapter stream. Pure: no I/O, easy to test."""

    def __init__(self) -> None:
        self.rate = DEFAULT_RATE
        self.header_seen = False

    def feed(self, line: str) -> list[PcmChunk]:
        if not line.startswith("data:"):
            return []
        data = line[5:].strip()
        if data in ("", "[DONE]"):
            return []
        try:
            event = json.loads(data)
        except json.JSONDecodeError:
            return []
        if isinstance(event, dict) and event.get("error"):
            err = event["error"]
            msg = err.get("message") if isinstance(err, dict) else str(err)
            raise TtsUnavailable(str(msg)[:300])
        out: list[PcmChunk] = []
        for choice in (event.get("choices") or []) if isinstance(event, dict) else []:
            content = (choice.get("delta") or {}).get("content")
            if not content:
                continue
            if content.startswith("#audio/pcm"):
                self.rate = _header_rate(content)
                self.header_seen = True
                continue
            try:
                pcm = base64.b64decode(content, validate=False)
            except ValueError:
                continue
            if len(pcm) % 2:  # whole samples only; a stray odd byte cannot be played
                pcm = pcm[:-1]
            if pcm:
                out.append(PcmChunk(self.rate, pcm))
        return out


def _header_rate(header: str) -> int:
    fields = dict(p.split("=", 1) for p in header.strip().split(";")[1:] if "=" in p)
    try:
        return int(fields.get("rate", DEFAULT_RATE))
    except ValueError:
        return DEFAULT_RATE


class KokoroTts:
    def __init__(self, auth: AuthFn, endpoint: str, *, timeout_s: float = 20.0,
                 transport: Optional[httpx.AsyncBaseTransport] = None) -> None:
        self._auth = auth
        self._endpoint = endpoint
        self._client = httpx.AsyncClient(timeout=httpx.Timeout(timeout_s, connect=10.0), transport=transport)

    @property
    def endpoint(self) -> str:
        return self._endpoint

    def request_body(self, text: str, voice: str) -> dict:
        return {"messages": [{"role": "user", "content": text}], "voice": voice, "stream": True}

    async def stream(self, text: str, voice: str) -> AsyncIterator[PcmChunk]:
        """Yield PCM chunks for one sentence as the endpoint produces them. Raises TtsUnavailable."""
        try:
            host, headers = await self._auth()
        except Exception as exc:  # noqa: BLE001 - auth failure is a TTS failure for this sentence
            raise TtsUnavailable(f"auth: {exc}"[:300]) from exc
        url = f"{host.rstrip('/')}/serving-endpoints/{self._endpoint}/invocations"
        parser = PcmParser()
        try:
            async with self._client.stream("POST", url, headers={**headers, "Content-Type": "application/json"},
                                           json=self.request_body(text, voice)) as resp:
                if resp.status_code >= 400:
                    body = (await resp.aread())[:200]
                    raise TtsUnavailable(f"HTTP {resp.status_code}: {body!r}",
                                         cold=resp.status_code in (429, 503, 504))
                async for line in resp.aiter_lines():
                    for chunk in parser.feed(line):
                        yield chunk
        except httpx.HTTPError as exc:
            cold = isinstance(exc, (httpx.TimeoutException, httpx.ConnectError, httpx.RemoteProtocolError))
            raise TtsUnavailable(f"{type(exc).__name__}: {exc}"[:300], cold=cold) from exc

    async def aclose(self) -> None:
        await self._client.aclose()
