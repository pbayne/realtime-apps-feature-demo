"""Realtime call sidekick: FastAPI app.

Run locally:  MODE=local uvicorn main:app --port 8000   (from src/app)
Deployed via app.yaml with ${DATABRICKS_APP_PORT:-8000}.

Ownership (SPEC 10): every call and voice session belongs to the user the Databricks Apps proxy identifies
(X-Forwarded-Email, then X-Forwarded-User; MODE=local: "local"). Any other caller gets 404, REST and WebSocket alike.

REST:
  GET  /healthz  (and /api/health)
  GET  /api/playbooks
  GET  /api/scripts?playbook=<name>
  POST /api/calls            {"playbook","mode":"replay|mic","script","speed","customer_id"}
  GET  /api/calls/{id}       snapshot: call, transcript, cards, coverage, feedback, decisions (live)
  POST /api/calls/{id}/end
  GET  /api/voices          Kokoro voice presets
  POST /api/voice/sessions   {"role":"assistant|practice","playbook","scenario"?,"call_id"?,"voice"?}
  GET  /api/voice/sessions/{id}   turns and items (live sessions only)
  DELETE /api/voice/sessions/{id}  end the session now (POST .../end is the same, for navigator.sendBeacon)
  GET  /api/actions?session_id=|call_id=   the caller's actions in one scope (SPEC 12)
  POST /api/actions                        manual create {session_id|call_id, kind, params}
  POST /api/actions/{id}/approve|reject|undo, optional {params} on approve
  GET  /api/actions/{id}/message.eml and /invite.ics   rendered (sent or preview) message text
WebSocket:
  /ws/call/{id}              server -> client events; client -> server feedback | browser_stt
  /ws/audio/{id}?channel=agent|customer   binary PCM16 16 kHz frames, or text browser results
  /ws/voice/{id}             binary: WAV turns (RIFF) or streamed PCM16 frames after stream_start;
                             text: interrupt | text | config | stream_start | stream_stop | pin | dismiss
                             (events per SPEC 9.2, 9.2a, 9.7). The first event is `hello`.
Close codes (SPEC 10): 4400 bad request, 4403 no caller identity, 4404 not found / not yours / not active,
4409 superseded by a newer socket for the same session, 4410 the session ended (a `session_ended` event
precedes it). The client does not reconnect after any of these.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Literal, Optional

from actions.service import ActionError
from admin import insights as admin_insights
from admin import prefs as admin_prefs
from config import get_settings
from fastapi import FastAPI, Query, Request, Response, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from models import now_ms
from pydantic import BaseModel, Field
from ratelimit import TokenBucket
from services import ServiceError, Services
from stt.base import Frame
from stt.parakeet import ParakeetDecoder
from voice.tts import voice_catalog

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("sidekick.main")

STATIC_DIR = Path(__file__).resolve().parent / "static"
SAMPLE_RATE = 16000
CLOSE_BAD_REQUEST = 4400
CLOSE_NO_IDENTITY = 4403
CLOSE_NOT_FOUND = 4404
CLOSE_SUPERSEDED = 4409
CLOSE_ENDED = 4410


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


def _caller(request_or_ws: Any) -> Optional[str]:
    """The user the Databricks Apps proxy identified (SPEC 10). MODE=local has one user, "local"."""
    if _svc(request_or_ws).settings.mode == "local":
        return "local"
    headers = request_or_ws.headers
    return (headers.get("x-forwarded-email") or headers.get("x-forwarded-user") or "").strip() or None


class CreateCall(BaseModel):
    playbook: str = "advisor_coach"
    mode: Literal["replay", "mic"] = "replay"
    script: Optional[str] = None
    speed: Optional[float] = Field(default=None, gt=0, le=100)
    customer_id: Optional[str] = None


class CreateAction(BaseModel):
    session_id: Optional[str] = None
    call_id: Optional[str] = None
    kind: Literal["send_email", "create_meeting", "update_contact", "enroll"]
    params: dict[str, Any] = Field(default_factory=dict)


class DecideAction(BaseModel):
    params: Optional[dict[str, Any]] = None     # approve with edited params (re-validated)


class CreateVoiceSession(BaseModel):
    role: Literal["assistant", "practice"] = "assistant"
    playbook: str = "advisor_coach"
    scenario: Optional[str] = None
    call_id: Optional[str] = None
    voice: Optional[str] = None


@app.exception_handler(ServiceError)
async def _service_error(_request: Request, exc: ServiceError):
    return JSONResponse(status_code=exc.status, content={"detail": exc.message})


@app.exception_handler(ActionError)
async def _action_error(_request: Request, exc: ActionError):
    return JSONResponse(status_code=exc.status, content={"detail": exc.message})


@app.get("/healthz")
@app.get("/api/health")  # the Databricks Apps proxy answers /healthz itself, so expose the report under /api too
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
            "programs": list(pb.programs),
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
    caller = _caller(request)
    if caller is None:
        raise ServiceError(403, "the app could not identify the caller")
    session = await svc.create_call(body.playbook, body.mode, body.script, body.speed, body.customer_id, caller)
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
    caller = _caller(request)
    session = svc.sessions.get(call_id)
    try:
        snap = await svc.store.snapshot(call_id)
        degraded = False
    except Exception:  # noqa: BLE001 - Lakebase down or slow: an in-memory call is still readable
        log.warning("snapshot unavailable; serving the in-memory call call_id=%s", call_id)
        snap = {"call": None, "utterances": [], "enrichments": [], "cards": [], "feedback": []}
        degraded = True
    stored = snap.get("call")
    if stored is None and session is None:
        raise ServiceError(404, "unknown call")
    record_owner = session.owner if session is not None else (stored or {}).get("owner")
    if caller is None or record_owner != caller:
        raise ServiceError(404, "unknown call")
    pb_name = (stored or {}).get("playbook") or session.record.playbook
    pb = svc.registry.get(pb_name)
    if session is not None:
        live = session.live_state()
        coverage, decisions = live["coverage"], live["decisions"]
    else:
        coverage = _coverage_from_enrichments([(m.key, m.label) for m in pb.methodology], snap.get("enrichments", []))
        decisions = []
    return {
        "call": stored if stored is not None else session.record.to_wire(),
        "playbook": pb_name,
        "transcript": snap.get("utterances", []),
        "cards": snap.get("cards", []),
        "feedback": snap.get("feedback", []),
        "actions": await svc.actions.list(caller, call_id=call_id),
        "coverage": coverage,
        "decisions": decisions,
        "as_of_ms": snap.get("as_of_ms") or now_ms(),
        "degraded": degraded,
    }


@app.post("/api/calls/{call_id}/end")
async def end_call(call_id: str, request: Request):
    svc = _svc(request)
    session = _owned_call(svc, call_id, _caller(request))
    await session.finish()
    return {"call_id": call_id, "ended": True}


def _owned_call(svc: Services, call_id: str, caller: Optional[str]):
    session = svc.sessions.get(call_id)
    if session is None or caller is None or session.owner != caller:
        raise ServiceError(404, "call not active in this process")
    return session


def _owned_voice(svc: Services, session_id: str, caller: Optional[str]):
    vs = svc.voice_sessions.get(session_id)
    if vs is None or caller is None or vs.owner != caller:
        raise ServiceError(404, "unknown voice session")
    return vs


@app.get("/api/voices")
async def list_voices(request: Request):
    return {"voices": voice_catalog(), "default": _svc(request).settings.tts_voice}


@app.get("/api/voice/warmup")
async def voice_warmup_status(request: Request):
    """Warmth of the voice endpoints (stt, tts, llm): warm | warming | cold."""
    return _svc(request).voice_warmth()


@app.post("/api/voice/warmup", status_code=202)
async def voice_warmup(request: Request):
    """Start waking the voice endpoints now (no-op while a warm-up is running). Poll GET for progress."""
    svc = _svc(request)
    svc.prewarm_voice(force=True)
    return svc.voice_warmth()


@app.post("/api/voice/sessions", status_code=201)
async def create_voice_session(body: CreateVoiceSession, request: Request):
    svc = _svc(request)
    caller = _caller(request)
    if caller is None:
        raise ServiceError(403, "the app could not identify the caller")
    vs = await svc.create_voice_session(body.role, body.playbook, body.scenario, body.call_id, body.voice, caller)
    return {
        "session_id": vs.session_id, "call_id": vs.call_id, "role": vs.role, "playbook": vs.playbook.name,
        "scenario": vs.scenario.key if vs.scenario else None, "voice": vs.voice,
    }


@app.get("/api/voice/sessions/{session_id}")
async def get_voice_session(session_id: str, request: Request):
    """Live sessions only. Turns and items of an ended or unknown session are not readable: the owner check
    needs the in-memory session."""
    svc = _svc(request)
    vs = _owned_voice(svc, session_id, _caller(request))
    return {**vs.snapshot(), "actions": await svc.actions.list(vs.owner, session_id=session_id)}


async def _end_voice_session(session_id: str, request: Request) -> Response:
    vs = _owned_voice(_svc(request), session_id, _caller(request))
    await vs.close(reason="closed")
    return Response(status_code=204)


@app.delete("/api/voice/sessions/{session_id}", status_code=204)
async def delete_voice_session(session_id: str, request: Request):
    return await _end_voice_session(session_id, request)


@app.post("/api/voice/sessions/{session_id}/end", status_code=204)
async def end_voice_session(session_id: str, request: Request):
    """POST twin of DELETE: navigator.sendBeacon (used on page hide) can only send POST."""
    return await _end_voice_session(session_id, request)


# ----------------------------------------------------------------- identity and preferences (SPEC 14)

class PutPrefs(BaseModel):
    theme: Literal["midnight", "aurora", "daylight", "databricks"]


def _is_admin(request: Request) -> bool:
    return admin_prefs.is_admin(_caller(request), admin_prefs.parse_admin_emails(_svc(request).settings.admin_emails))


@app.get("/api/me")
async def get_me(request: Request):
    """Who the proxy says the caller is, and whether they see the admin-only pages (ADMIN_EMAILS)."""
    svc = _svc(request)
    return {
        "email": _caller(request),
        "is_admin": _is_admin(request),
        "default_theme": admin_prefs.resolve_default(svc.settings.app_default_theme),
    }


@app.get("/api/prefs")
async def get_prefs(request: Request):
    svc = _svc(request)
    caller = _caller(request)
    if caller is None:
        raise ServiceError(403, "the app could not identify the caller")
    try:
        return await admin_prefs.get_theme(svc.store, caller, admin_prefs.resolve_default(svc.settings.app_default_theme))
    except Exception as exc:  # noqa: BLE001 - Lakebase down: the UI keeps its cached theme
        log.warning("prefs unavailable: %r", exc)
        raise ServiceError(503, "preferences are unavailable right now") from exc


@app.put("/api/prefs")
async def put_prefs(body: PutPrefs, request: Request):
    svc = _svc(request)
    caller = _caller(request)
    if caller is None:
        raise ServiceError(403, "the app could not identify the caller")
    try:
        return await admin_prefs.set_theme(svc.store, caller, body.theme, now_ms())
    except ValueError as exc:
        raise ServiceError(400, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        log.warning("prefs write failed: %r", exc)
        raise ServiceError(503, "preferences could not be saved right now") from exc


# ----------------------------------------------------------------- notes (Granola-style, per call or session)

MAX_NOTE_BYTES = 100_000


class PutNote(BaseModel):
    text: str = Field(default="", max_length=100_000)


def _note_scope_from_query(session_id: Optional[str], call_id: Optional[str]) -> None:
    if bool(session_id) == bool(call_id):
        raise ServiceError(400, "give exactly one of session_id or call_id")


def _owned_note_scope(svc: Services, caller: Optional[str], session_id: Optional[str],
                      call_id: Optional[str]) -> dict[str, Optional[str]]:
    """Check the caller owns the live call or voice session the note belongs to, and return the store kwargs."""
    _note_scope_from_query(session_id, call_id)
    if session_id:
        _owned_voice(svc, session_id, caller)
        return {"session_id": session_id}
    _owned_call(svc, call_id, caller)
    return {"call_id": call_id}


@app.get("/api/notes")
async def get_note(request: Request, session_id: Optional[str] = Query(default=None),
                   call_id: Optional[str] = Query(default=None)):
    """The caller's freeform note for one live call or voice session. Empty text when none exists yet."""
    svc = _svc(request)
    scope = _owned_note_scope(svc, _caller(request), session_id, call_id)
    try:
        row = await svc.store.get_note(**scope)
    except Exception as exc:  # noqa: BLE001 - store outage: the pad still opens, empty
        log.warning("note read unavailable: %r", exc)
        return {"text": "", "updated_at_ms": None, "degraded": True, **scope}
    return row or {"text": "", "updated_at_ms": None, **scope}


@app.put("/api/notes")
async def put_note(body: PutNote, request: Request, session_id: Optional[str] = Query(default=None),
                   call_id: Optional[str] = Query(default=None)):
    svc = _svc(request)
    caller = _caller(request)
    scope = _owned_note_scope(svc, caller, session_id, call_id)
    if len(body.text.encode("utf-8")) > MAX_NOTE_BYTES:
        raise ServiceError(400, "note is too long")
    try:
        return await svc.store.put_note(owner=caller, text=body.text, updated_at_ms=now_ms(), **scope)
    except Exception as exc:  # noqa: BLE001
        log.warning("note write failed: %r", exc)
        raise ServiceError(503, "the note could not be saved right now") from exc


def _insights_for(svc: Services) -> admin_insights.Insights:
    """Built on first use: the SQL warehouse comes from DATABRICKS_WAREHOUSE_ID; without it the page is empty."""
    existing = getattr(svc, "insights", None)
    if existing is None:
        s = svc.settings
        wh = s.databricks_warehouse_id
        executor = admin_insights.make_sdk_executor(wh) if wh and s.mode == "databricks" else None
        existing = admin_insights.Insights(s.history_catalog, s.history_schema, wh, s.insights_cache_s, executor,
                                          principal=os.environ.get("DATABRICKS_CLIENT_ID"))
        svc.insights = existing
    return existing


def _admin_only(request: Request) -> None:
    if not _is_admin(request):
        raise ServiceError(403, "admin only")


@app.get("/api/admin/insights")
async def admin_insights_overview(request: Request, days: int = Query(default=7, ge=1, le=90)):
    _admin_only(request)
    return await _insights_for(_svc(request)).overview(days)


@app.get("/api/admin/calls/{call_id}")
async def admin_call_detail(call_id: str, request: Request):
    """Drill-down from the insights page: transcript, cards and the post-call summary, from Delta."""
    _admin_only(request)
    if not re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", call_id):
        raise ServiceError(404, "unknown call")
    return {"call_id": call_id, **(await _insights_for(_svc(request)).call_detail(call_id))}


# ------------------------------------------------------------------ actions (SPEC 12)

def _action_scope(svc: Services, caller: Optional[str], body: "CreateAction") -> tuple[str, dict[str, Any]]:
    """(playbook, scope) for a manual create. The scope must be live and owned by the caller (404 otherwise)."""
    if bool(body.session_id) == bool(body.call_id):
        raise ServiceError(400, "give exactly one of session_id or call_id")
    if body.session_id:
        vs = _owned_voice(svc, body.session_id, caller)
        return vs.playbook.name, {"session_id": body.session_id}
    session = _owned_call(svc, body.call_id, caller)
    return session.playbook.name, {"call_id": body.call_id}


@app.get("/api/actions")
async def list_actions(request: Request, session_id: Optional[str] = Query(default=None),
                       call_id: Optional[str] = Query(default=None)):
    """The caller's actions in one session or call, plus the safety and delivery mode they run under."""
    svc = _svc(request)
    caller = _caller(request)
    if caller is None:
        raise ServiceError(404, "unknown session or call")
    if bool(session_id) == bool(call_id):
        raise ServiceError(400, "give exactly one of session_id or call_id")
    return {"actions": await svc.actions.list(caller, session_id=session_id, call_id=call_id),
            "safety": svc.actions.safety_info()}


@app.post("/api/actions", status_code=201)
async def create_action(body: CreateAction, request: Request):
    svc = _svc(request)
    caller = _caller(request)
    if caller is None:
        raise ServiceError(403, "the app could not identify the caller")
    playbook, scope = _action_scope(svc, caller, body)
    rec = await svc.actions.create(owner=caller, playbook=playbook, kind=body.kind, params=body.params, **scope)
    return svc.actions.wire(rec)


async def _decide_action(request: Request, action_id: str, decision: str,
                         body: Optional[DecideAction] = None) -> dict[str, Any]:
    svc = _svc(request)
    rec = await svc.actions.decide(_caller(request), action_id, decision, body.params if body else None)
    return svc.actions.wire(rec)


@app.post("/api/actions/{action_id}/approve")
async def approve_action(action_id: str, request: Request, body: Optional[DecideAction] = None):
    return await _decide_action(request, action_id, "approve", body)


@app.post("/api/actions/{action_id}/reject")
async def reject_action(action_id: str, request: Request):
    return await _decide_action(request, action_id, "reject")


@app.post("/api/actions/{action_id}/undo")
async def undo_action(action_id: str, request: Request):
    svc = _svc(request)
    return svc.actions.wire(await svc.actions.undo(_caller(request), action_id))


@app.get("/api/actions/{action_id}/message.eml")
async def action_message(action_id: str, request: Request):
    svc = _svc(request)
    name, text = await svc.actions.rendered(_caller(request), action_id, "eml")
    return Response(content=text.encode("utf-8"), media_type="message/rfc822",
                    headers={"Content-Disposition": f'attachment; filename="{name}"'})


@app.get("/api/actions/{action_id}/invite.ics")
async def action_invite(action_id: str, request: Request):
    svc = _svc(request)
    name, text = await svc.actions.rendered(_caller(request), action_id, "ics")
    return Response(content=text.encode("utf-8"), media_type="text/calendar; charset=utf-8",
                    headers={"Content-Disposition": f'attachment; filename="{name}"'})


# ----------------------------------------------------------------- WebSockets

def _limiter(svc: Services) -> TokenBucket:
    return TokenBucket(rate=svc.settings.ws_msgs_per_s, burst=svc.settings.ws_msgs_per_s)


async def _evict(ws: Any) -> None:
    """Close a socket that a newer one has taken over (SPEC 10). It may already be dead: that is fine."""
    try:
        await ws.close(code=CLOSE_SUPERSEDED)
    except Exception:  # noqa: BLE001
        pass


@app.websocket("/ws/call/{call_id}")
async def ws_call(ws: WebSocket, call_id: str):
    svc = _svc(ws)
    await ws.accept()
    session = svc.sessions.get(call_id)
    caller = _caller(ws)
    if session is None or caller is None or session.owner != caller:
        await ws.send_json({"type": "error", "message": "call not active in this process"})
        await ws.close(code=CLOSE_NOT_FOUND)
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
        bucket = _limiter(svc)
        while True:
            msg = await ws.receive_json()
            if not bucket.allow():
                continue                       # over the per-socket rate: dropped, not answered
            kind = msg.get("type")
            try:
                if kind == "feedback":
                    await session.feedback(msg.get("card_id", ""), msg.get("action", ""), msg.get("text"))
                elif kind == "browser_stt":
                    session.browser_result(msg.get("speaker", "agent"), msg)
                elif kind == "action_decision":
                    await svc.actions.decide(session.owner, str(msg.get("action_id", "")),
                                             str(msg.get("decision", "")), msg.get("params"))
                else:
                    await ws.send_json({"type": "error", "message": f"unknown message type {kind!r}"})
            except (ValueError, KeyError, TypeError) as exc:
                # A bad message is reported, not allowed to tear down the call socket.
                await ws.send_json({"type": "error", "message": str(exc)[:200]})

    out_task = asyncio.ensure_future(pump_out())
    in_task = asyncio.ensure_future(pump_in())
    evicted = session.claim_socket(ws, q, out_task)
    if evicted is not None:
        await _evict(evicted)
    try:
        done, pending = await asyncio.wait({out_task, in_task}, return_when=asyncio.FIRST_COMPLETED)
        for t in pending:
            t.cancel()
        for t in done:
            exc = t.exception() if not t.cancelled() else None
            if exc is not None and not isinstance(exc, (WebSocketDisconnect, ValueError, KeyError)):
                log.warning("ws/call error call_id=%s: %r", call_id, exc)
    finally:
        out_task.cancel()
        in_task.cancel()
        session.release_socket(ws, q)  # False when this socket was evicted: the successor keeps the call


@app.websocket("/ws/audio/{call_id}")
async def ws_audio(ws: WebSocket, call_id: str, channel: Literal["agent", "customer"] = Query(default="agent")):
    svc = _svc(ws)
    await ws.accept()
    session = svc.sessions.get(call_id)
    caller = _caller(ws)
    if session is None or session.ended or caller is None or session.owner != caller:
        await ws.send_json({"type": "error", "message": "call not active"})
        await ws.close(code=CLOSE_NOT_FOUND)
        return
    if session.record.mode != "mic":
        await ws.send_json({"type": "error", "message": "audio socket is only used for mode=mic calls"})
        await ws.close(code=CLOSE_BAD_REQUEST)
        return

    frames: asyncio.Queue[Optional[Frame]] = asyncio.Queue(maxsize=500)
    # Tell the call's sockets while transcription is warming (scale-to-zero GPU), and start waking the voice
    # endpoints now: a mic call is the moment someone is about to talk.
    decoder = ParakeetDecoder(svc.llm, svc.settings.stt_endpoint,
                              on_status=lambda ev: svc.hub.emit(call_id, {**ev, "channel": channel}))
    svc.prewarm_voice()

    async def frame_source():
        while True:
            f = await frames.get()
            if f is None:
                return
            yield f

    session.attach()
    stt_task = session.start_mic(channel, frame_source(), decoder)
    bucket = _limiter(svc)
    t_ms = 0.0
    try:
        while True:
            msg = await ws.receive()
            if msg["type"] == "websocket.disconnect":
                break
            if not bucket.allow():
                continue
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
        _end_frames(frames)
        try:
            await asyncio.wait_for(stt_task, timeout=10)
        except (asyncio.TimeoutError, asyncio.CancelledError, Exception):  # noqa: BLE001
            pass
        finally:
            stt_task.cancel()   # always: a stuck STT task must not outlive its socket
            session.detach()


def _end_frames(frames: "asyncio.Queue[Optional[Frame]]") -> None:
    """Signal end-of-audio even when the queue is full: the oldest frame makes room (the call is ending anyway)."""
    while True:
        try:
            frames.put_nowait(None)
            return
        except asyncio.QueueFull:
            try:
                frames.get_nowait()
            except asyncio.QueueEmpty:
                return


async def _pump_voice_out(ws: WebSocket, vs: Any) -> None:
    while True:
        ev = await vs.out.get()
        if ev is None:
            return
        await ws.send_json(ev)


@app.websocket("/ws/voice/{session_id}")
async def ws_voice(ws: WebSocket, session_id: str):
    svc = _svc(ws)
    await ws.accept()
    vs = svc.voice_sessions.get(session_id)
    caller = _caller(ws)
    if vs is None or vs.closed or caller is None or vs.owner != caller:
        await ws.send_json({"type": "error", "message": "voice session not active in this process"})
        await ws.close(code=CLOSE_NOT_FOUND)
        return
    out_task = asyncio.ensure_future(_pump_voice_out(ws, vs))
    evicted = vs.claim_socket(ws, out_task)
    if evicted is not None:
        await _evict(evicted)
    vs.hello()
    vs.attach_call_events(svc.hub)   # idempotent: a re-attach replaces the earlier subscription
    vs.arm_deadline(svc.settings.voice_max_session_minutes * 60.0)
    if vs.role == "practice":
        vs.start_opening()

    async def pump_in() -> None:
        bucket = _limiter(svc)
        while True:
            msg = await ws.receive()
            if msg["type"] == "websocket.disconnect":
                return
            if not bucket.allow():
                continue
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
                elif kind == "action_decision":
                    await svc.actions.decide(vs.owner, str(payload.get("action_id", "")),
                                             str(payload.get("decision", "")), payload.get("params"))
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

    in_task = asyncio.ensure_future(pump_in())
    try:
        done, pending = await asyncio.wait({out_task, in_task}, return_when=asyncio.FIRST_COMPLETED)
        for t in pending:
            t.cancel()
        for t in done:
            exc = t.exception() if not t.cancelled() else None
            if exc is not None and not isinstance(exc, (WebSocketDisconnect, ValueError, KeyError)):
                log.warning("ws/voice error session_id=%s: %r", session_id, exc)
        if out_task in done and not out_task.cancelled() and vs.closed:
            try:
                await ws.close(code=CLOSE_ENDED)     # the session ended: its last event was session_ended
            except Exception:  # noqa: BLE001 - the client may already be gone
                pass
    finally:
        out_task.cancel()
        in_task.cancel()
        if vs.release_socket(ws):
            # The mic is gone: discard any open utterance. The session itself lives on for the reattach grace
            # (SPEC 10): an assistant session keeps its context, a practice session keeps its call. The reaper
            # closes it after voice_detached_ttl_s.
            vs.abort_stream()
            vs.detach_call_events()
            if vs.role == "assistant":
                vs.interrupt()


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
            "<p><a href='/docs'>API docs</a> &middot; <a href='/api/health'>health</a></p>"
        )
