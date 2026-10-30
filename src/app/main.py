"""Realtime call sidekick: FastAPI app.

Run locally:  MODE=local uvicorn main:app --port 8000   (from src/app)
Deployed via app.yaml with ${DATABRICKS_APP_PORT:-8000}.

REST:
  GET  /healthz
  GET  /api/playbooks
  GET  /api/scripts?playbook=<name>
  POST /api/calls            {"playbook","mode":"replay|mic","script","speed","customer_id"}
  GET  /api/calls/{id}       snapshot: call, transcript, cards, coverage, feedback, decisions (live)
  POST /api/calls/{id}/end
WebSocket:
  /ws/call/{id}              server -> client events; client -> server feedback | browser_stt
  /ws/audio/{id}?channel=agent|customer   binary PCM16 16 kHz frames, or text browser results
  POST /api/voice/sessions   {"role":"assistant|practice","playbook","scenario"?,"call_id"?,"voice"?}
  GET  /api/voice/sessions/{id}   turns and items
  GET  /api/voices          Kokoro voice presets
  /ws/voice/{id}             binary: WAV turns (RIFF) or streamed PCM16 frames after stream_start;
                             text: interrupt | text | config | stream_start | stream_stop | pin | dismiss
                             (events per SPEC 9.2, 9.2a, 9.7)
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Literal, Optional

from config import get_settings
from fastapi import FastAPI, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from models import now_ms
from pydantic import BaseModel, Field
from services import ServiceError, Services
from stt.base import Frame
from stt.parakeet import ParakeetDecoder
from voice.tts import voice_catalog

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("sidekick.main")

STATIC_DIR = Path(__file__).resolve().parent / "static"
SAMPLE_RATE = 16000


@asynccontextmanager
async def lifespan(app: FastAPI):
    services = Services(get_settings())
    await services.start()
    app.state.services = services
    try:
        yield
    finally:
        await services.stop()


app = FastAPI(title="Realtime call sidekick", lifespan=lifespan)


def _svc(request_or_ws: Any) -> Services:
    return request_or_ws.app.state.services


class CreateCall(BaseModel):
    playbook: str = "advisor_coach"
    mode: Literal["replay", "mic"] = "replay"
    script: Optional[str] = None
    speed: Optional[float] = Field(default=None, gt=0, le=100)
    customer_id: Optional[str] = None


class CreateVoiceSession(BaseModel):
    role: Literal["assistant", "practice"] = "assistant"
    playbook: str = "advisor_coach"
    scenario: Optional[str] = None
    call_id: Optional[str] = None
    voice: Optional[str] = None


@app.exception_handler(ServiceError)
async def _service_error(_request: Request, exc: ServiceError):
    return JSONResponse(status_code=exc.status, content={"detail": exc.message})


@app.get("/healthz")
async def healthz(request: Request):
    return await _svc(request).health_full()


@app.get("/api/playbooks")
async def list_playbooks(request: Request):
    svc = _svc(request)
    return {"playbooks": [
        {
            "name": pb.name,
            "title": pb.title,
            "persona": pb.persona,
            "card_weights": pb.card_weights,
            "methodology": [{"key": m.key, "label": m.label} for m in pb.methodology],
            "objections": [o.key for o in pb.objections],
            "compliance": [c.key for c in pb.compliance],
            "kb_docs": [d.doc_id for d in pb.kb_docs],
            "replay_scripts": list(pb.replay_scripts),
            "scenarios": [sc.to_wire() for sc in pb.scenarios],
        }
        for pb in svc.registry.all()
    ]}


@app.get("/api/scripts")
async def list_scripts(request: Request, playbook: Optional[str] = Query(default=None)):
    """With ?playbook=<name>: a JSON array of replay script names. Without: {playbook: [names]}."""
    svc = _svc(request)
    try:
        mapping = svc.scripts_for(playbook)
    except KeyError as exc:
        raise ServiceError(404, str(exc)) from exc
    if playbook:
        return mapping[playbook]
    return mapping


@app.post("/api/calls", status_code=201)
async def create_call(body: CreateCall, request: Request):
    svc = _svc(request)
    session = await svc.create_call(body.playbook, body.mode, body.script, body.speed, body.customer_id)
    return {
        "call_id": session.call_id,
        "playbook": session.record.playbook,
        "mode": session.record.mode,
        "script": session.record.script,
        "started_at_ms": session.record.started_at_ms,
    }


def _coverage_from_enrichments(keys: list[tuple[str, str]], enrichments: list[dict[str, Any]]) -> list[dict[str, Any]]:
    hit: set[str] = set()
    for row in enrichments:
        if row.get("speaker") == "customer":
            hit.update(row.get("methodology_hits") or [])
    return [{"key": k, "label": label, "covered": k in hit} for k, label in keys]


@app.get("/api/calls/{call_id}")
async def get_call(call_id: str, request: Request):
    svc = _svc(request)
    snap = await svc.store.snapshot(call_id)
    session = svc.sessions.get(call_id)
    if snap.get("call") is None and session is None:
        raise ServiceError(404, "unknown call")
    pb_name = (snap.get("call") or {}).get("playbook") or session.record.playbook
    pb = svc.registry.get(pb_name)
    if session is not None:
        live = session.live_state()
        coverage, decisions = live["coverage"], live["decisions"]
    else:
        coverage = _coverage_from_enrichments([(m.key, m.label) for m in pb.methodology], snap.get("enrichments", []))
        decisions = []
    return {
        "call": snap.get("call"),
        "playbook": pb_name,
        "transcript": snap.get("utterances", []),
        "cards": snap.get("cards", []),
        "feedback": snap.get("feedback", []),
        "coverage": coverage,
        "decisions": decisions,
        "as_of_ms": snap.get("as_of_ms"),
    }


@app.get("/api/voices")
async def list_voices(request: Request):
    return {"voices": voice_catalog(), "default": _svc(request).settings.tts_voice}


@app.post("/api/voice/sessions", status_code=201)
async def create_voice_session(body: CreateVoiceSession, request: Request):
    svc = _svc(request)
    vs = await svc.create_voice_session(body.role, body.playbook, body.scenario, body.call_id, body.voice)
    return {
        "session_id": vs.session_id, "call_id": vs.call_id, "role": vs.role, "playbook": vs.playbook.name,
        "scenario": vs.scenario.key if vs.scenario else None, "voice": vs.voice,
    }


@app.get("/api/voice/sessions/{session_id}")
async def get_voice_session(session_id: str, request: Request):
    svc = _svc(request)
    vs = svc.voice_sessions.get(session_id)
    if vs is not None:
        return vs.snapshot()
    snap = await svc.store.voice_snapshot(session_id)
    if not snap["turns"] and not snap["items"]:
        raise ServiceError(404, "unknown voice session")
    # Not active in this process: turns and items come from Lakebase. Session metadata (role, playbook,
    # call_id) is kept in memory only, so it is null here.
    return {"session_id": session_id, "role": None, "playbook": None, "call_id": None,
            "turns": snap["turns"], "items": snap["items"], "sentiment": [], "suggestions": [],
            "streaming": False, "active": False}


@app.post("/api/calls/{call_id}/end")
async def end_call(call_id: str, request: Request):
    svc = _svc(request)
    session = svc.session(call_id)
    await session.finish()
    return {"call_id": call_id, "ended": True}


# ----------------------------------------------------------------- WebSockets

@app.websocket("/ws/call/{call_id}")
async def ws_call(ws: WebSocket, call_id: str):
    svc = _svc(ws)
    await ws.accept()
    session = svc.sessions.get(call_id)
    if session is None:
        await ws.send_json({"type": "error", "message": "call not active in this process"})
        await ws.close(code=4404)
        return
    q = svc.hub.subscribe(call_id)
    if session.ended:
        await ws.send_json({"type": "call.ended", "emitted_at_ms": now_ms()})
    else:
        await ws.send_json({**session.coverage_event(), "emitted_at_ms": now_ms()})

    async def pump_out() -> None:
        while True:
            ev = await q.get()
            if ev is None:
                break
            await ws.send_json(ev)

    async def pump_in() -> None:
        while True:
            msg = await ws.receive_json()
            kind = msg.get("type")
            try:
                if kind == "feedback":
                    await session.feedback(msg.get("card_id", ""), msg.get("action", ""), msg.get("text"))
                elif kind == "browser_stt":
                    session.browser_result(msg.get("speaker", "agent"), msg)
                else:
                    await ws.send_json({"type": "error", "message": f"unknown message type {kind!r}"})
            except (ValueError, KeyError, TypeError) as exc:
                # A bad message is reported, not allowed to tear down the call socket.
                await ws.send_json({"type": "error", "message": str(exc)[:200]})

    out_task = asyncio.ensure_future(pump_out())
    in_task = asyncio.ensure_future(pump_in())
    try:
        done, pending = await asyncio.wait({out_task, in_task}, return_when=asyncio.FIRST_COMPLETED)
        for t in pending:
            t.cancel()
        for t in done:
            exc = t.exception() if not t.cancelled() else None
            if exc is not None and not isinstance(exc, (WebSocketDisconnect, ValueError, KeyError)):
                log.warning("ws/call error: %r", exc)
    finally:
        svc.hub.unsubscribe(call_id, q)


@app.websocket("/ws/audio/{call_id}")
async def ws_audio(ws: WebSocket, call_id: str, channel: Literal["agent", "customer"] = Query(default="agent")):
    svc = _svc(ws)
    await ws.accept()
    session = svc.sessions.get(call_id)
    if session is None or session.ended:
        await ws.send_json({"type": "error", "message": "call not active"})
        await ws.close(code=4404)
        return
    if session.record.mode != "mic":
        await ws.send_json({"type": "error", "message": "audio socket is only used for mode=mic calls"})
        await ws.close(code=4400)
        return

    frames: asyncio.Queue[Optional[Frame]] = asyncio.Queue(maxsize=500)
    decoder = ParakeetDecoder(svc.llm, svc.settings.stt_endpoint)

    async def frame_source():
        while True:
            f = await frames.get()
            if f is None:
                return
            yield f

    stt_task = session.start_mic(channel, frame_source(), decoder)
    t_ms = 0.0
    try:
        while True:
            msg = await ws.receive()
            if msg["type"] == "websocket.disconnect":
                break
            if msg.get("bytes"):
                pcm = msg["bytes"]
                dur_ms = (len(pcm) / 2) / SAMPLE_RATE * 1000.0
                try:
                    frames.put_nowait(Frame(t_ms=t_ms, wall=time.monotonic(), dur_ms=dur_ms, pcm=pcm))
                except asyncio.QueueFull:
                    log.warning("audio backlog on %s/%s; dropping frame", call_id, channel)
                t_ms += dur_ms
            elif msg.get("text"):
                try:
                    payload = json.loads(msg["text"])
                except json.JSONDecodeError:
                    continue
                if payload.get("type") == "result":
                    session.browser_result(channel, payload)
    except WebSocketDisconnect:
        pass
    finally:
        frames.put_nowait(None)
        try:
            await asyncio.wait_for(stt_task, timeout=10)
        except (asyncio.TimeoutError, asyncio.CancelledError, Exception):  # noqa: BLE001
            stt_task.cancel()


@app.websocket("/ws/voice/{session_id}")
async def ws_voice(ws: WebSocket, session_id: str):
    svc = _svc(ws)
    await ws.accept()
    vs = svc.voice_sessions.get(session_id)
    if vs is None or vs.closed:
        await ws.send_json({"type": "error", "message": "voice session not active in this process"})
        await ws.close(code=4404)
        return
    vs.attach_call_events(svc.hub)
    vs.arm_deadline(svc.settings.voice_max_session_minutes * 60.0)
    if vs.role == "practice":
        vs.start_opening()

    async def pump_out() -> None:
        while True:
            ev = await vs.out.get()
            if ev is None:
                break
            await ws.send_json(ev)

    async def pump_in() -> None:
        while True:
            msg = await ws.receive()
            if msg["type"] == "websocket.disconnect":
                return
            try:
                if msg.get("bytes"):
                    data = msg["bytes"]
                    # A RIFF header is one complete WAV turn (SPEC 9.2). Anything else is streamed PCM (SPEC 9.2a).
                    if data[:4] == b"RIFF":
                        vs.submit_audio(data)
                    else:
                        vs.stream_frame(data)
                    continue
                if not msg.get("text"):
                    continue
                payload = json.loads(msg["text"])
                kind = payload.get("type")
                if kind == "interrupt":
                    vs.interrupt()
                elif kind == "stream_start":
                    vs.stream_start(sample_rate=payload.get("sample_rate", 16000),
                                    endpoint_silence_ms=payload.get("endpoint_silence_ms"))
                elif kind == "stream_stop":
                    vs.stream_stop(flush=payload.get("flush", True) is not False)
                elif kind == "pin":
                    await vs.pin(str(payload.get("item_id", "")))
                elif kind == "dismiss":
                    vs.dismiss(str(payload.get("item_id", "")))
                elif kind == "feedback":
                    # Same handling as the call socket: the card belongs to the linked call.
                    call = svc.sessions.get(vs.call_id) if vs.call_id else None
                    if call is None:
                        raise ValueError("feedback needs a linked call")
                    await call.feedback(msg_card_id(payload), payload.get("action", ""), payload.get("text"))
                elif kind == "text":
                    vs.submit_text(payload.get("text") or "")
                elif kind == "config":
                    vs.configure(voice=payload.get("voice"), role=payload.get("role"),
                                 scenario_key=payload.get("scenario"))
                    await vs.ensure_practice_call(svc.hub)
                    vs.start_opening()
                else:
                    await ws.send_json({"type": "error", "message": f"unknown message type {kind!r}"})
            except (ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
                # A bad message is reported, not allowed to tear down the voice socket.
                await ws.send_json({"type": "error", "message": str(exc)[:200]})

    out_task = asyncio.ensure_future(pump_out())
    in_task = asyncio.ensure_future(pump_in())
    try:
        done, pending = await asyncio.wait({out_task, in_task}, return_when=asyncio.FIRST_COMPLETED)
        for t in pending:
            t.cancel()
        for t in done:
            exc = t.exception() if not t.cancelled() else None
            if exc is not None and not isinstance(exc, (WebSocketDisconnect, ValueError, KeyError)):
                log.warning("ws/voice error: %r", exc)
    finally:
        out_task.cancel()
        in_task.cancel()
        vs.abort_stream()              # the mic is gone: discard any open utterance
        vs.detach_call_events()
        if vs.role == "practice":
            await vs.close()           # a practice call lives with its socket
        else:
            vs.interrupt()             # an assistant session survives a reconnect


def msg_card_id(payload: dict) -> str:
    return str(payload.get("card_id", ""))


# ----------------------------------------------------------------- static UI

if (STATIC_DIR / "index.html").exists():
    app.mount("/", StaticFiles(directory=STATIC_DIR, html=True), name="static")
else:
    @app.get("/", response_class=HTMLResponse)
    async def placeholder() -> str:
        return (
            "<!doctype html><title>Call sidekick</title><h1>Call sidekick</h1>"
            "<p>API is running. The UI has not been built into src/app/static yet.</p>"
            "<p><a href='/docs'>API docs</a> &middot; <a href='/healthz'>healthz</a></p>"
        )
