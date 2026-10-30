"""Service container: builds the bus, store, LLM, retrieval, playbooks and live call sessions.

Local (MODE=local): memory bus + memory store, and the LocalEnricher subscribes to
`utterances` as the stand-in for the RTM job. Databricks: Kafka bus + Lakebase store,
and enrichments come from RTM (with a local fallback after RTM_WAIT_MS).
"""

from __future__ import annotations

import asyncio
import logging
import re
import uuid
from pathlib import Path
from typing import Optional

from bus import build_bus
from config import Settings
from enrich.local_enricher import LocalEnricher
from enrich.rules import load_rules
from hub import Hub
from llm import LlmClient
from models import CallRecord, now_ms
from session import CallSession, Deps
from sidekick.playbooks import Playbook, PlaybookRegistry
from sidekick.retrieval import Retriever, VectorSearchStub
from store import build_store
from stt.parakeet import ParakeetTranscriber
from stt.replay import ReplayScript, load_script
from stt.vad import Vad, build_vad
from voice.session import VoiceDeps, VoiceSession
from voice.tts import KokoroTts

log = logging.getLogger("sidekick.services")
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
            backend = "vector_search" if settings.mode == "databricks" else "memory"
        self.retrieval_backend = backend
        self.retriever = Retriever(self.llm, settings,
                                   VectorSearchStub(settings) if backend == "vector_search" else None,
                                   backend=backend)
        self.hub = Hub()
        self.deps = Deps(settings=settings, bus=self.bus, store=self.store, llm=self.llm,
                         retriever=self.retriever, registry=self.registry, hub=self.hub, vad=self.vad)
        self.local_enricher: Optional[LocalEnricher] = None
        self.sessions: dict[str, CallSession] = {}
        # Voice mode (SPEC 9): Kokoro TTS and Parakeet over the same workspace auth as the LLM.
        self.tts = KokoroTts(self.llm.workspace_auth, settings.tts_endpoint, timeout_s=settings.tts_timeout_s)
        self.transcriber = ParakeetTranscriber(self.llm, settings.stt_endpoint,
                                               timeout_s=settings.voice_stt_timeout_s,
                                               protocol=settings.stt_protocol)
        self.voice_sessions: dict[str, VoiceSession] = {}
        self.voice_deps = VoiceDeps(
            settings=settings, llm=self.llm, tts=self.tts, stt=self.transcriber, store=self.store,
            retriever=self.retriever, calls=self.sessions, finish_call=self.finish_call,
            new_practice_call=self.new_practice_call,
        )

    # ------------------------------------------------------------ lifecycle
    async def start(self) -> None:
        await self.store.start()
        await self.bus.start()
        if self.settings.effective_bus == "memory":
            self.local_enricher = LocalEnricher(self.store, self.registry.rules_for)
            self.local_enricher.attach(self.bus, self.settings.kafka_utterances_topic)
        # Build and embed KB indexes in the background so the first customer turn does not pay for it.
        self._warmup = asyncio.ensure_future(self.retriever.warm(self.registry.all()))
        log.info("services ready: mode=%s bus=%s store=%s vad=%s retrieval=%s llm=%s",
                 self.settings.mode, self.settings.effective_bus, self.settings.effective_store,
                 self.vad.name, self.retrieval_backend, self.llm.enabled)

    async def stop(self) -> None:
        if getattr(self, "_warmup", None) is not None:
            self._warmup.cancel()
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
                    log.exception("finish on shutdown failed")
        await self.bus.stop()
        await self.store.stop()
        await self.llm.aclose()

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
            started_at_ms=now_ms(), customer_id=customer_id,
        )
        return await self._start_call(record, replay, speed)

    async def _start_call(self, record: CallRecord, replay: Optional[ReplayScript],
                          speed: Optional[float] = None) -> CallSession:
        try:
            await self.store.create_call(record)
        except Exception:  # noqa: BLE001 - store outage: the live call still works
            log.exception("create_call persistence failed")
        session = CallSession(record=record, deps=self.deps)
        session.start()
        self.sessions[record.call_id] = session
        if replay is not None:
            session.start_replay(replay, speed or self.settings.replay_speed)
        return session

    # -------------------------------------------------------------- voice
    async def new_practice_call(self, playbook: str) -> str:
        """The live call a practice session plays into (mode `voice`: no audio socket, text in both channels)."""
        record = CallRecord(call_id=str(uuid.uuid4()), playbook=playbook, mode="voice", script=None,
                            started_at_ms=now_ms())
        session = await self._start_call(record, None)
        return session.call_id

    async def finish_call(self, call_id: str) -> None:
        session = self.sessions.get(call_id)
        if session is not None and not session.ended:
            await session.finish()

    async def create_voice_session(self, role: str, playbook: str, scenario: Optional[str],
                                   call_id: Optional[str], voice: Optional[str]) -> VoiceSession:
        try:
            pb: Playbook = self.registry.get(playbook)
        except KeyError as exc:
            raise ServiceError(404, str(exc)) from exc
        sc = None
        if role == "practice":
            if call_id:
                raise ServiceError(400, "a practice session creates its own call; do not pass call_id")
            if not pb.scenarios:
                raise ServiceError(404, f"no practice scenarios for playbook {playbook}")
            sc = pb.scenario(scenario) if scenario else pb.scenarios[0]
            if sc is None:
                raise ServiceError(404, f"unknown practice scenario {scenario!r} for {playbook}")
            call_id = await self.new_practice_call(pb.name)
        else:
            if scenario:
                raise ServiceError(400, "scenario applies to practice sessions only")
            if call_id and call_id not in self.sessions:
                raise ServiceError(404, "call not active in this process")
        chosen_voice = voice or (sc.voice if sc is not None else self.settings.tts_voice)
        if not VOICE_RE.match(chosen_voice):
            raise ServiceError(400, "voice must look like af_heart")
        vs = VoiceSession(session_id=str(uuid.uuid4()), role=role, playbook=pb, voice=chosen_voice,
                          deps=self.voice_deps, call_id=call_id, scenario=sc)
        self.voice_sessions[vs.session_id] = vs
        return vs

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

    def health(self) -> dict:
        return {
            "status": "ok",
            "mode": self.settings.mode,
            "bus": self.settings.effective_bus,
            "store": self.settings.effective_store,
            "vad": self.vad.name,
            "retrieval": self.retrieval_backend,
            "llm": self.llm.status(),
            "active_calls": sum(1 for s in self.sessions.values() if not s.ended),
            "playbooks": self.registry.names(),
            "enrich_rules": self.rules.source,
            "voice": {"model": self.settings.voice_model, "tts": self.settings.tts_endpoint,
                      "stt": self.settings.stt_endpoint, "active_sessions": sum(1 for v in self.voice_sessions.values() if not v.closed)},
        }
