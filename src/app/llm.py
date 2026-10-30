"""OpenAI-compatible client to Databricks Model Serving.

Auth comes from the databricks-sdk Config (DATABRICKS_CONFIG_PROFILE locally, Apps
injected env in production). Headers are re-authenticated per request so OAuth
tokens refresh. Claude 5.x rejects `temperature`, so it is never sent.

Degradation: every call can raise LlmUnavailable. A small circuit breaker opens
after `llm_failure_threshold` consecutive failures and short-circuits calls for
`llm_breaker_cooldown_s`, so an outage costs one timeout, not one per turn.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, AsyncIterator, Optional, Sequence

import httpx
from config import Settings

log = logging.getLogger("sidekick.llm")


class LlmUnavailable(RuntimeError):
    """The LLM endpoint could not serve the request. Callers fall back to rules.

    `cold` marks a failure that looks like a scale-to-zero endpoint still starting (timeout, connect error,
    HTTP 429/503/504). Voice mode retries cold failures with backoff (voice/resilience.py); other failures
    are final for the call."""

    def __init__(self, message: str = "", *, cold: bool = False) -> None:
        super().__init__(message)
        self.cold = cold


COLD_STATUS = frozenset({429, 503, 504})


def is_cold_failure(exc: BaseException) -> bool:
    """True for failures that a warming serving endpoint produces (timeouts, refused or reset connections)."""
    if getattr(exc, "cold", False):
        return True
    return isinstance(exc, (httpx.TimeoutException, httpx.ConnectError, httpx.RemoteProtocolError))


class LlmClient:
    def __init__(self, settings: Settings, *, sdk_config: Any = None, transport: httpx.AsyncBaseTransport | None = None):
        self._s = settings
        self._sdk_config = sdk_config
        self._transport = transport
        self._http: Optional[httpx.AsyncClient] = None
        self._fail_streak = 0
        self._breaker_open_until = 0.0
        self.last_error: Optional[str] = None

    # ------------------------------------------------------------------ state
    @property
    def enabled(self) -> bool:
        return self._s.llm_enabled

    @property
    def available(self) -> bool:
        """False when disabled or the breaker is open."""
        return self.enabled and time.monotonic() >= self._breaker_open_until

    @property
    def fast_model(self) -> str:
        return self._s.fast_model

    @property
    def smart_model(self) -> str:
        return self._s.smart_model

    @property
    def embed_model(self) -> str:
        return self._s.embed_model

    def status(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "available": self.available,
            "last_error": self.last_error,
            "fast_model": self._s.fast_model,
            "smart_model": self._s.smart_model,
            "embed_model": self._s.embed_model,
        }

    def _record_ok(self) -> None:
        self._fail_streak = 0

    def _record_fail(self, exc: BaseException) -> None:
        self.last_error = f"{type(exc).__name__}: {exc}"[:300]
        self._fail_streak += 1
        if self._fail_streak >= self._s.llm_failure_threshold:
            self._breaker_open_until = time.monotonic() + self._s.llm_breaker_cooldown_s
            self._fail_streak = 0
            log.warning("LLM breaker open for %.0fs: %s", self._s.llm_breaker_cooldown_s, self.last_error)

    # ------------------------------------------------------------- plumbing
    def _config(self) -> Any:
        if self._sdk_config is None:
            from databricks.sdk.core import Config

            self._sdk_config = Config()  # respects DATABRICKS_CONFIG_PROFILE / Apps env
        return self._sdk_config

    def sdk_config(self) -> Any:
        """The databricks-sdk Config this client authenticates with (readiness probes reuse it)."""
        return self._config()

    async def workspace_auth(self) -> tuple[str, dict[str, str]]:
        """Workspace host and the app's auth headers (service principal in Apps, profile locally).
        Re-authenticated on every call so OAuth tokens refresh. Shared by every serving client
        (chat, embeddings, multipart audio, the Kokoro TTS stream in voice/tts.py)."""
        cfg = self._config()
        host = (cfg.host or "").rstrip("/")
        if not host:
            raise LlmUnavailable("no Databricks host configured")
        headers = await asyncio.to_thread(cfg.authenticate)
        return host, dict(headers)

    async def _url_and_headers(self, model: str, suffix: str) -> tuple[str, dict[str, str]]:
        host, headers = await self.workspace_auth()
        headers = {**headers, "Content-Type": "application/json"}
        return f"{host}/serving-endpoints/{model}/{suffix}", headers

    def _client(self) -> httpx.AsyncClient:
        if self._http is None:
            self._http = httpx.AsyncClient(timeout=self._s.llm_timeout_s, transport=self._transport)
        return self._http

    async def _guard(self) -> None:
        if not self.enabled:
            raise LlmUnavailable("LLM disabled (LLM_ENABLED=false)")
        if not self.available:
            raise LlmUnavailable("LLM circuit breaker open")

    # ------------------------------------------------------------ endpoints
    async def chat(
        self,
        model: str,
        messages: Sequence[dict[str, str]],
        *,
        max_tokens: int = 300,
        timeout: Optional[float] = None,
    ) -> str:
        """Non-streaming chat completion. Returns the assistant text."""
        await self._guard()
        payload = {"messages": list(messages), "max_tokens": max_tokens, **_model_extras(model)}
        try:
            url, headers = await self._url_and_headers(model, "invocations")
            resp = await self._client().post(url, headers=headers, json=payload, timeout=timeout or self._s.llm_timeout_s)
            if resp.status_code >= 400:
                raise LlmUnavailable(f"HTTP {resp.status_code}: {resp.text[:200]}")
            body = resp.json()
            text = content_text(body["choices"][0]["message"]["content"])
            self._record_ok()
            return text
        except Exception as exc:  # noqa: BLE001 - every failure is a degradation, not a crash
            self._record_fail(exc)
            raise LlmUnavailable(str(exc)) from exc

    async def chat_stream(
        self,
        model: str,
        messages: Sequence[dict[str, str]],
        *,
        max_tokens: int = 160,
        timeout: Optional[float] = None,
    ) -> AsyncIterator[str]:
        """Streaming chat completion (SSE). Yields text deltas."""
        await self._guard()
        payload = {"messages": list(messages), "max_tokens": max_tokens, "stream": True, **_model_extras(model)}
        try:
            url, headers = await self._url_and_headers(model, "invocations")
            async with self._client().stream("POST", url, headers=headers, json=payload, timeout=timeout or self._s.llm_timeout_s) as resp:
                if resp.status_code >= 400:
                    body = (await resp.aread())[:200]
                    raise LlmUnavailable(f"HTTP {resp.status_code}: {body!r}")
                async for line in resp.aiter_lines():
                    if not line.startswith("data:"):
                        continue
                    data = line[5:].strip()
                    if data in ("", "[DONE]"):
                        continue
                    try:
                        chunk = json.loads(data)
                    except json.JSONDecodeError:
                        continue
                    for choice in chunk.get("choices") or []:
                        delta = content_text((choice.get("delta") or {}).get("content"))
                        if delta:
                            yield delta
            self._record_ok()
        except GeneratorExit:
            raise
        except Exception as exc:  # noqa: BLE001
            self._record_fail(exc)
            raise LlmUnavailable(str(exc)) from exc

    async def embed(self, model: str, texts: Sequence[str], *, timeout: Optional[float] = None) -> list[list[float]]:
        """Embeddings via the serving endpoint's OpenAI-style `input` payload."""
        await self._guard()
        try:
            url, headers = await self._url_and_headers(model, "invocations")
            resp = await self._client().post(url, headers=headers, json={"input": list(texts)}, timeout=timeout or self._s.llm_timeout_s)
            if resp.status_code >= 400:
                raise LlmUnavailable(f"HTTP {resp.status_code}: {resp.text[:200]}")
            data = sorted(resp.json()["data"], key=lambda d: d.get("index", 0))
            self._record_ok()
            return [d["embedding"] for d in data]
        except Exception as exc:  # noqa: BLE001
            self._record_fail(exc)
            raise LlmUnavailable(str(exc)) from exc

    async def json_post(self, endpoint: str, payload: dict[str, Any], *, timeout: Optional[float] = None) -> dict[str, Any]:
        """Raw JSON POST to {endpoint}/invocations, e.g. the Databricks whisper endpoint's {"inputs": [...]} contract.
        Same auth and breaker as the other calls."""
        await self._guard()
        try:
            url, headers = await self._url_and_headers(endpoint, "invocations")
            resp = await self._client().post(url, headers=headers, json=payload, timeout=timeout or self._s.llm_timeout_s)
            if resp.status_code >= 400:
                raise LlmUnavailable(f"HTTP {resp.status_code}: {resp.text[:200]}")
            body = resp.json()
            self._record_ok()
            return body
        except Exception as exc:  # noqa: BLE001
            self._record_fail(exc)
            raise LlmUnavailable(str(exc)) from exc

    async def multipart(self, endpoint: str, route: str, *, files: dict[str, Any], data: dict[str, str],
                        timeout: Optional[float] = None) -> dict[str, Any]:
        """Multipart POST to a serving endpoint route, e.g. `audio/transcriptions` for Parakeet
        (SPEC 3.3). Same workspace auth and circuit breaker as the JSON calls. Content-Type is left to
        httpx so the multipart boundary is set. Returns the parsed JSON body."""
        await self._guard()
        try:
            host, headers = await self.workspace_auth()
            url = f"{host}/serving-endpoints/{endpoint}/{route}"
            resp = await self._client().post(url, headers=headers, files=files, data=data,
                                             timeout=timeout or self._s.llm_timeout_s)
            if resp.status_code >= 400:
                raise LlmUnavailable(f"HTTP {resp.status_code}: {resp.text[:200]}",
                                     cold=resp.status_code in COLD_STATUS)
            body = resp.json()
            self._record_ok()
            return body
        except Exception as exc:  # noqa: BLE001 - every failure is a degradation, not a crash
            self._record_fail(exc)
            raise LlmUnavailable(str(exc), cold=is_cold_failure(exc)) from exc

    async def aclose(self) -> None:
        if self._http is not None:
            await self._http.aclose()
            self._http = None


def _model_extras(model: str) -> dict[str, Any]:
    """Claude 5.x on Databricks thinks by default, which spends max_tokens on hidden reasoning and
    adds seconds. Sonnet accepts `between_tools` as its off-switch. Haiku 5.5 also starts a
    `reasoning` block on longer prompts (perception returned empty text at max_tokens=90), and
    `thinking: disabled` is the switch it accepts (probed 2026-10-28; reasoning_effort is ignored)."""
    if "claude" in model and "haiku" in model:
        return {"thinking": {"type": "disabled"}}
    if "claude" in model:
        return {"thinking": {"type": "between_tools"}}
    return {}


def content_text(content: Any) -> str:
    """OpenAI-style content may be a string or a list of typed blocks (Claude via Databricks)."""
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for block in content:
            if isinstance(block, dict) and block.get("type", "text") == "text":
                parts.append(block.get("text") or "")
            elif isinstance(block, str):
                parts.append(block)
        return "".join(parts)
    return str(content)


def parse_json_object(raw: str) -> Optional[dict[str, Any]]:
    """Tolerant JSON extraction, same fallback as contact-center-ai's /assist route:
    take the outermost {...} span; return None rather than raise."""
    if not raw:
        return None
    start, end = raw.find("{"), raw.rfind("}")
    if start == -1 or end <= start:
        return None
    try:
        obj = json.loads(raw[start : end + 1])
    except json.JSONDecodeError:
        return None
    return obj if isinstance(obj, dict) else None
