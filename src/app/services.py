"""Service container: builds the bus, store, history sink, LLM, retrieval, playbooks and live call sessions.

BACKBONE=direct (default): no bus. Each final is enriched in process (enrich/direct.py), Lakebase holds live
state, and the HistorySink (Zerobus in databricks mode, memory in local mode) receives history.
BACKBONE=kafka: Kafka bus + Lakebase store; enrichments come from RTM (local fallback after RTM_WAIT_MS).
In local kafka mode the in-memory bus stands in for Kafka and BusEnricher stands in for RTM.

Lifetime (SPEC 10): a background reaper closes voice sessions with no socket for voice_detached_ttl_s and finishes
calls with no socket or voice attachment for call_detached_ttl_s, then drops closed and ended entries after
session_grace_s. A store that fails to start does not stop the app: it retries in the background and the app runs
degraded (health reports store_ready=false).
"""

from __future__ import annotations

import asyncio
import io
import logging
import re
import time
import uuid
import wave
from pathlib import Path
from typing import Optional

from bus import build_bus
from config import Settings
from enrich.direct import BusEnricher, DirectEnricher
from enrich.rules import load_rules
from history import build_history
from history import schema as hs
from hub import Hub
from llm import WARMUP, LlmClient
from models import CallRecord, now_ms
from session import CallSession, Deps
from sidekick.playbooks import Playbook, PlaybookRegistry
from sidekick.retrieval import Retriever, VectorSearchStub
from store import build_store
from stt.parakeet import ParakeetTranscriber
from stt.replay import ReplayScript, load_script
from stt.vad import Vad, build_vad
from voice.readiness import EndpointReadiness
from voice.resilience import RetryPolicy
from voice.session import VoiceDeps, VoiceSession
from voice.stream import VoiceDecoder
from voice.tts import KokoroTts

log = logging.getLogger("sidekick.services")


def _silent_wav(ms: int = 300) -> bytes:
    buf = io.BytesIO()
    with wave.open(buf, "wb") as w:
        w.setnchannels(1)
        w.setsampwidth(2)
        w.setframerate(16000)
        w.writeframes(b"\x00\x00" * (16 * ms))
    return buf.getvalue()


_SILENT_WAV = _silent_wav()  # Parakeet warm-up ping
VOICE_RE = re.compile(r"^[a-z]{2}_[a-z]+$")


class ServiceError(Exception):
    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.message = message


class Services:
    def __init__(self, settings: Settings, llm: Optional[LlmClient] = None) -> None:
        self.settings = settings
        self.bus = build_bus(settings)
        self.store = build_store(settings)
        self.llm = llm or LlmClient(settings)
        self.vad: Vad = build_vad(settings.vad_backend, settings.vad_rms_threshold)
        self.rules = load_rules(settings.enrich_rules_path)
        self.registry = PlaybookRegistry.load(settings.playbooks_dir, self.rules, settings.replay_dir)
        backend = settings.retrieval_backend
        if backend == "auto":
            backend = "memory"
        self.retrieval_backend = backend
        self.retriever = Retriever(self.llm, settings,
                                   VectorSearchStub(settings) if backend == "vector_search" else None,
                                   backend=backend)
        self.history = build_history(settings)
        self.enricher = DirectEnricher(self.registry.rules_for)
        self.hub = Hub()
        self.deps = Deps(settings=settings, bus=self.bus, store=self.store, llm=self.llm,
                         retriever=self.retriever, registry=self.registry, hub=self.hub, vad=self.vad,
                         history=self.history, enricher=self.enricher)
        self.bus_enricher: Optional[BusEnricher] = None
        self.sessions: dict[str, CallSession] = {}
        # Voice mode (SPEC 9): Kokoro TTS and Parakeet over the same workspace auth as the LLM.
        self.tts = KokoroTts(self.llm.workspace_auth, settings.tts_endpoint, timeout_s=settings.tts_timeout_s)
        self.transcriber = ParakeetTranscriber(self.llm, settings.stt_endpoint,
                                               timeout_s=settings.voice_stt_timeout_s,
                                               protocol=settings.stt_protocol)
        self.voice_sessions: dict[str, VoiceSession] = {}
        self.store_ready = False
        self._reaper: Optional[asyncio.Task[None]] = None
        self._store_retry: Optional[asyncio.Task[None]] = None
        self._last_prewarm = float("-inf")
        self._warmth: dict[str, dict] = {}
        self._background: set[asyncio.Task] = set()
        policy = RetryPolicy(budget_s=settings.voice_warmup_budget_s, base_s=settings.voice_retry_base_s,
                             cap_s=settings.voice_retry_cap_s)

        def stream_decoder(emit):
            return VoiceDecoder(self.llm, settings.stt_endpoint, emit_status=emit, policy=policy)

        self.voice_deps = VoiceDeps(
            settings=settings, llm=self.llm, tts=self.tts, stt=self.transcriber, store=self.store,
            retriever=self.retriever, calls=self.sessions, finish_call=self.finish_call,
            new_practice_call=self.new_practice_call, history=self.history,
            stream_decoder=stream_decoder, vad=self.vad, rules_for=self.registry.rules_for,
            note_ok=self.note_voice_ok,
        )
        self.readiness = EndpointReadiness(
            {"stt": settings.stt_endpoint, "tts": settings.tts_endpoint},
            self._endpoint_state_async if settings.mode == "databricks" else None,
            ttl_s=settings.readiness_ttl_s,
        )

    # ------------------------------------------------------------ lifecycle
    async def start(self) -> None:
        try:
            await self.store.start()
            self.store_ready = True
        except Exception as exc:  # noqa: BLE001 - a fresh workspace or an outage must not crash-loop the app
            log.error("store start failed (%s); running degraded and retrying in the background", type(exc).__name__)
            self._store_retry = asyncio.ensure_future(self._retry_store())
        await self.bus.start()
        await self.history.start()
        if self.settings.effective_bus == "memory":
            self.bus_enricher = BusEnricher(self.store, self.registry.rules_for)
            self.bus_enricher.attach(self.bus, self.settings.kafka_utterances_topic)
        # Build and embed KB indexes in the background so the first customer turn does not pay for it.
        self._warmup = asyncio.ensure_future(self.retriever.warm(self.registry.all()))
        self._reaper = asyncio.ensure_future(self._reap_loop())
        log.info("services ready: mode=%s backbone=%s bus=%s store=%s history=%s vad=%s retrieval=%s llm=%s",
                 self.settings.mode, self.settings.backbone, self.settings.effective_bus,
                 self.settings.effective_store, self.settings.effective_history, self.vad.name,
                 self.retrieval_backend, self.llm.enabled)

    async def _retry_store(self) -> None:
        delay = 2.0
        while not self.store_ready:
            await asyncio.sleep(delay)
            try:
                await self.store.start()
                self.store_ready = True
                log.info("store ready after retry")
            except Exception as exc:  # noqa: BLE001
                log.warning("store still unavailable (%s); next try in %.0f s", type(exc).__name__, delay)
                delay = min(delay * 2, 30.0)

    async def stop(self) -> None:
        for task in (getattr(self, "_warmup", None), self._reaper, self._store_retry):
            if task is not None:
                task.cancel()
        for vs in list(self.voice_sessions.values()):
            try:
                await vs.close()
            except Exception:  # noqa: BLE001
                log.exception("voice session close on shutdown failed")
        await self.tts.aclose()
        for session in list(self.sessions.values()):
            if not session.ended:
                try:
                    await session.finish()
                except Exception:  # noqa: BLE001
                    log.exception("finish on shutdown failed call_id=%s", session.call_id)
        for name, closer in (("bus", self.bus.stop), ("history", self.history.stop), ("store", self.store.stop)):
            try:
                await closer()
            except Exception:  # noqa: BLE001 - shutdown is best effort; a dead dependency must not block it
                log.exception("%s stop failed on shutdown", name)
        await self.llm.aclose()

    # ------------------------------------------------------------- lifetime
    async def _reap_loop(self) -> None:
        while True:
            await asyncio.sleep(self.settings.session_reap_interval_s)
            try:
                await self.reap_once()
            except Exception:  # noqa: BLE001 - the reaper must survive a bad pass
                log.exception("session reaper pass failed")

    async def reap_once(self, now: Optional[float] = None) -> None:
        """One reaper pass (SPEC 10). Detached voice sessions and calls are ended after their TTL; closed and ended
        entries are dropped after the grace period, so a reload right after a close still reads the session."""
        now = time.monotonic() if now is None else now
        grace = self.settings.session_grace_s
        for vs in list(self.voice_sessions.values()):
            if vs.closed:
                if vs.closed_mono is not None and now - vs.closed_mono > grace:
                    self.voice_sessions.pop(vs.session_id, None)
            elif vs.socket is None and now - vs.detached_at > self.settings.voice_detached_ttl_s:
                log.info("closing detached voice session session_id=%s", vs.session_id)
                await self._close_quietly(vs)
        for call_id, session in list(self.sessions.items()):
            if session.ended:
                if session.ended_mono is not None and now - session.ended_mono > grace:
                    self.sessions.pop(call_id, None)
            elif session.attached == 0 and now - session.detached_at > self.settings.call_detached_ttl_s:
                log.info("finishing detached call call_id=%s", call_id)
                try:
                    await session.finish()
                except Exception:  # noqa: BLE001
                    log.exception("finish of detached call failed call_id=%s", call_id)

    async def _close_quietly(self, vs: VoiceSession) -> None:
        try:
            await vs.close()
        except Exception:  # noqa: BLE001
            log.exception("closing voice session failed session_id=%s", vs.session_id)

    def active_calls(self, owner: Optional[str] = None) -> int:
        return sum(1 for s in self.sessions.values() if not s.ended and (owner is None or s.owner == owner))

    # --------------------------------------------------------------- calls
    def replay_path(self, playbook: str, script: str) -> Path:
        if "/" in script or ".." in script or "/" in playbook:
            raise ServiceError(400, "invalid script or playbook name")
        return self.settings.replay_dir / f"{playbook}__{script}.jsonl"

    def scripts_for(self, playbook: Optional[str] = None) -> dict[str, list[str]]:
        if playbook:
            return {playbook: list(self.registry.get(playbook).replay_scripts)}
        return {pb.name: list(pb.replay_scripts) for pb in self.registry.all()}

    async def create_call(
        self,
        playbook: str,
        mode: str,
        script: Optional[str],
        speed: Optional[float],
        customer_id: Optional[str],
        owner: Optional[str] = None,
    ) -> CallSession:
        try:
            pb: Playbook = self.registry.get(playbook)
        except KeyError as exc:
            raise ServiceError(404, str(exc)) from exc
        if mode not in ("replay", "mic"):
            raise ServiceError(400, "mode must be replay or mic")
        replay: Optional[ReplayScript] = None
        if mode == "replay":
            name = script or (pb.replay_scripts[0] if pb.replay_scripts else None)
            if not name:
                raise ServiceError(404, f"no replay scripts for playbook {playbook}")
            path = self.replay_path(playbook, name)
            if not path.exists():
                raise ServiceError(404, f"replay script {name!r} not found for {playbook}")
            replay = load_script(path)
            if replay.playbook != playbook:
                raise ServiceError(400, f"script {name!r} belongs to {replay.playbook}")
            script = name
        record = CallRecord(
            call_id=str(uuid.uuid4()), playbook=playbook, mode=mode, script=script,
            started_at_ms=now_ms(), customer_id=customer_id, owner=owner,
        )
        return await self._start_call(record, replay, speed)

    async def _start_call(self, record: CallRecord, replay: Optional[ReplayScript],
                          speed: Optional[float] = None) -> CallSession:
        if self.active_calls() >= self.settings.max_active_calls:
            raise ServiceError(429, "too many live calls on this app instance; try again shortly")
        if record.owner is not None and self.active_calls(record.owner) >= self.settings.max_active_calls_per_user:
            raise ServiceError(429, "too many live calls for this user; end one first")
        try:
            await self.store.create_call(record)
        except Exception:  # noqa: BLE001 - store outage: the live call still works
            log.exception("create_call persistence failed call_id=%s", record.call_id)
        self.history.emit("calls", hs.call_row(record))
        session = CallSession(record=record, deps=self.deps)
        session.start()
        self.sessions[record.call_id] = session
        if replay is not None:
            session.start_replay(replay, speed or self.settings.replay_speed)
        return session

    # -------------------------------------------------------------- voice
    async def new_practice_call(self, playbook: str, owner: Optional[str] = None) -> str:
        """The live call a practice session plays into (mode `voice`: no audio socket, text in both channels)."""
        record = CallRecord(call_id=str(uuid.uuid4()), playbook=playbook, mode="voice", script=None,
                            started_at_ms=now_ms(), owner=owner)
        session = await self._start_call(record, None)
        return session.call_id

    async def finish_call(self, call_id: str) -> None:
        session = self.sessions.get(call_id)
        if session is not None and not session.ended:
            await session.finish()

    async def create_voice_session(self, role: str, playbook: str, scenario: Optional[str],
                                   call_id: Optional[str], voice: Optional[str],
                                   owner: Optional[str] = None) -> VoiceSession:
        try:
            pb: Playbook = self.registry.get(playbook)
        except KeyError as exc:
            raise ServiceError(404, str(exc)) from exc
        sc = None
        if self.active_voice_sessions() >= self.settings.voice_max_concurrent_sessions:
            raise ServiceError(429, "too many voice sessions on this app instance; try again shortly")
        if role == "practice":
            if call_id:
                raise ServiceError(400, "a practice session creates its own call; do not pass call_id")
            if not pb.scenarios:
                raise ServiceError(404, f"no practice scenarios for playbook {playbook}")
            sc = pb.scenario(scenario) if scenario else pb.scenarios[0]
            if sc is None:
                raise ServiceError(404, f"unknown practice scenario {scenario!r} for {playbook}")
            call_id = await self.new_practice_call(pb.name, owner)
        else:
            if scenario:
                raise ServiceError(400, "scenario applies to practice sessions only")
            linked = self.sessions.get(call_id) if call_id else None
            # Same answer for "not here" and "not yours": a call id is not a capability.
            if call_id and (linked is None or linked.owner != owner):
                raise ServiceError(404, "call not active in this process")
        chosen_voice = voice or (sc.voice if sc is not None else self.settings.tts_voice)
        if not VOICE_RE.match(chosen_voice):
            raise ServiceError(400, "voice must look like af_heart")
        vs = VoiceSession(session_id=str(uuid.uuid4()), role=role, playbook=pb, voice=chosen_voice,
                          deps=self.voice_deps, call_id=call_id, scenario=sc, owner=owner)
        self.voice_sessions[vs.session_id] = vs
        self.prewarm_voice()
        return vs

    def prewarm_voice(self, *, force: bool = False) -> None:
        """Start waking scale-to-zero GPU endpoints as soon as someone opens a voice session (or presses
        "Warm up voice"), so the first turn rarely waits for a cold start. Fire-and-forget, throttled per app
        instance unless forced, never raises. Progress is in `voice_warmth()`."""
        now = time.monotonic()
        if not force and now - self._last_prewarm < self.settings.voice_prewarm_interval_s:
            return
        if any(w.get("warming") for w in self._warmth.values()):
            return  # a warm-up is already running
        self._last_prewarm = now

        async def ping(name: str, coro) -> None:
            WARMUP.set(True)   # this task only: a failed warm-up must not open an LLM breaker (llm.py)
            t0 = time.monotonic()
            try:
                await asyncio.wait_for(coro, timeout=self.settings.voice_warmup_budget_s)
                elapsed = time.monotonic() - t0
                self._warmth[name] = {"warming": False, "last_ok_ms": now_ms(), "last_ms": round(elapsed * 1000)}
                log.info("prewarm %s ok in %.1f s", name, elapsed)
            except Exception as exc:  # warm-up is best effort; the turn path has its own retries
                self._warmth[name] = {**self._warmth.get(name, {}), "warming": False, "error": type(exc).__name__}
                log.info("prewarm %s not ready after %.1f s: %s", name, time.monotonic() - t0, type(exc).__name__)

        async def tts_ping() -> None:
            async for _ in self.tts.stream("Hi.", self.settings.tts_voice):
                break

        async def llm_ping() -> None:
            # The first request to the voice model can take ~3 s (measured); later ones ~0.5 s to first token.
            stream = self.llm.chat_stream(self.settings.voice_model, [{"role": "user", "content": "Say ok."}],
                                          max_tokens=4, timeout=30)
            try:
                async for _ in stream:
                    break
            finally:
                await stream.aclose()

        pings = (("tts", tts_ping()), ("stt", self.transcriber.transcribe(_SILENT_WAV)), ("llm", llm_ping()))
        for name, coro in pings:
            # Mark warming before the task runs, so a status read right after this call is accurate.
            self._warmth[name] = {**self._warmth.get(name, {}), "warming": True, "started_ms": now_ms()}
            task = asyncio.create_task(ping(name, coro))
            self._background.add(task)
            task.add_done_callback(self._background.discard)

    def note_voice_ok(self, name: str) -> None:
        """A real turn reached this endpoint (stt | tts | llm): it is warm now."""
        self._warmth[name] = {**self._warmth.get(name, {}), "warming": False, "last_ok_ms": now_ms()}

    def voice_warmth(self) -> dict:
        """Best-effort warmth per voice endpoint. Serving reports READY even when scaled to zero, so this tracks
        when each endpoint last answered: warm (answered within voice_warm_window_s), warming (a ping is in
        flight), or cold (never answered in this app instance, or idle longer than the window)."""
        now = now_ms()
        window_ms = self.settings.voice_warm_window_s * 1000
        out = {}
        for name in ("stt", "tts", "llm"):
            w = self._warmth.get(name, {})
            if w.get("warming"):
                state = "warming"
            elif w.get("last_ok_ms") and now - w["last_ok_ms"] < window_ms:
                state = "warm"
            else:
                state = "cold"
            out[name] = {"state": state, "last_ok_ms": w.get("last_ok_ms"), "started_ms": w.get("started_ms"),
                         "last_ms": w.get("last_ms"), "error": w.get("error")}
        states = {v["state"] for v in out.values()}
        overall = "warm" if states == {"warm"} else ("warming" if "warming" in states else "cold")
        return {"state": overall, "endpoints": out, "scale_to_zero": self.settings.gpu_scale_to_zero_hint}

    def voice_session(self, session_id: str) -> VoiceSession:
        vs = self.voice_sessions.get(session_id)
        if vs is None:
            raise ServiceError(404, "voice session not active in this process")
        return vs

    def session(self, call_id: str) -> CallSession:
        s = self.sessions.get(call_id)
        if s is None:
            raise ServiceError(404, "call not active in this process")
        return s

    def active_voice_sessions(self) -> int:
        return sum(1 for v in self.voice_sessions.values() if not v.closed)

    def _endpoint_state(self, name: str) -> str:
        """Serving endpoint state from the SDK (runs in a worker thread). ready | not_ready."""
        from databricks.sdk import WorkspaceClient

        ep = WorkspaceClient(config=self.llm.sdk_config()).serving_endpoints.get(name=name)
        ready = getattr(getattr(ep, "state", None), "ready", None)
        return "ready" if ready is not None and getattr(ready, "value", "") == "READY" else "not_ready"

    async def _endpoint_state_async(self, name: str) -> str:
        return await asyncio.to_thread(self._endpoint_state, name)

    async def health_full(self) -> dict:
        """health() plus voice readiness (SPEC 9.6). Readiness is cached (readiness_ttl_s) and never fails health."""
        base = self.health()
        base["voice"]["readiness"] = await self.readiness.snapshot()
        base["voice"]["limits"] = {
            "active_sessions": self.active_voice_sessions(),
            "max_concurrent_sessions": self.settings.voice_max_concurrent_sessions,
            "max_session_minutes": self.settings.voice_max_session_minutes,
            "max_turn_audio_s": self.settings.voice_max_turn_audio_s,
            "endpoint_silence_ms": self.settings.voice_endpoint_silence_ms,
        }
        return base

    def health(self) -> dict:
        return {
            "status": "ok" if self.store_ready else "degraded",
            "mode": self.settings.mode,
            "backbone": self.settings.backbone,
            "bus": self.settings.effective_bus,
            "store": self.settings.effective_store,
            "store_ready": self.store_ready,
            "history": self.history.stats(),
            "vad": self.vad.name,
            "retrieval": self.retrieval_backend,
            "llm": self.llm.status(),
            "active_calls": self.active_calls(),
            "playbooks": self.registry.names(),
            "enrich_rules": self.rules.source,
            "voice": {"model": self.settings.voice_model, "tts": self.settings.tts_endpoint,
                      "stt": self.settings.stt_endpoint, "active_sessions": sum(1 for v in self.voice_sessions.values() if not v.closed)},
        }
