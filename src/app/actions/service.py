"""ActionService: the only place an action changes status (SPEC 12).

State machine (every transition is persisted and emitted as an `action` event):

  proposed --approve--> executing --> done | outboxed | failed
  proposed | failed --reject--> rejected
  failed --approve (retry, optionally with edited params)--> executing
  done | outboxed --undo (not for send_email)--> undone

Approve is idempotent: an action that is executing, done or outboxed returns as it is, so a double click
executes once. A per-action lock serialises decisions on the same action.

Safety lock (SAFETY, user decision): while ACTION_SAFETY_LOCK is on, every recipient and attendee is replaced by
ACTION_SAFE_RECIPIENT and the subject is prefixed "[Sidekick test -> <original recipients>]". The originals are kept
in result.safety. Preview and execution use the same routing, so a preview shows what would be sent.

Outbox mode: with no SMTP relay configured, email and invites are rendered (eml/ics) and stored with status
`outboxed`. Nothing is sent.
"""

from __future__ import annotations

import asyncio
import logging
import smtplib
from datetime import datetime, timezone
from email.utils import parseaddr
from typing import Any, Callable, Optional

from config import Settings
from history import schema as hs
from models import Action, new_id, now_ms
from sidekick.playbooks import PlaybookRegistry

from actions import ics, mail, records
from actions.proposals import dedupe_key
from actions.schemas import ActionValidationError, summarize, validate_params

log = logging.getLogger("sidekick.actions")

UNDOABLE = frozenset({"create_meeting", "update_contact", "enroll", "update_record"})
EXECUTED = frozenset({"executing", "done", "outboxed"})
DEFAULT_SENDER = "Sidekick <sidekick@sidekick.invalid>"
INVITE_PREFIX = "Invitation: "


class ActionError(ValueError):
    """Refused by the service (404 not yours or unknown, 409 wrong state, 400 bad params). ValueError so the
    socket handlers report it without closing the socket."""

    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.message = message


class SendError(RuntimeError):
    """The relay refused or dropped the message. The action is `failed` and can be retried."""


def _public_error(exc: BaseException) -> str:
    if isinstance(exc, (LookupError, ValueError, SendError)):
        return str(exc)[:300]
    log.exception("action execution failed unexpectedly")
    return "unexpected error; see the server log"


class ActionService:
    def __init__(self, *, settings: Settings, store: Any, registry: PlaybookRegistry, history: Any = None,
                 emit: Optional[Callable[[Action], None]] = None,
                 send: Optional[Callable[[mail.SmtpConfig, Any, list[str]], None]] = None,
                 records_exec: Optional[records.Executor] = None,
                 clock: Callable[[], int] = now_ms) -> None:
        self.settings = settings
        self.store = store
        self.registry = registry
        self.history = history
        self._emit = emit
        self._send = send or mail.send_smtp
        self._records_exec = records_exec
        self._clock = clock
        self._smtp = mail.SmtpConfig.from_settings(settings)
        self._record_target = self._build_record_target(settings)
        self._cache: dict[str, Action] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    @staticmethod
    def _build_record_target(settings: Settings) -> Optional[records.RecordTarget]:
        """The UC Delta table update_record writes to, or None when it is misconfigured (the action then refuses)."""
        try:
            return records.RecordTarget(settings.crm_catalog, settings.crm_schema, settings.crm_table)
        except records.RecordError:
            log.warning("CRM record target is misconfigured; update_record will be refused")
            return None

    # ------------------------------------------------------------------ views
    @property
    def mode(self) -> str:
        return "smtp" if self._smtp is not None else "outbox"

    def safety_info(self) -> dict[str, Any]:
        return {"lock": bool(self.settings.action_safety_lock), "safe_recipient": self.settings.action_safe_recipient,
                "mode": self.mode}

    def wire(self, rec: Action) -> dict[str, Any]:
        """The client view of an action, with the safety and delivery mode it was decided under."""
        return {**rec.to_wire(), "safety": self.safety_info()}

    async def get(self, owner: Optional[str], action_id: str) -> Action:
        """The action if `owner` owns it. Otherwise 404, the same answer as for an unknown id."""
        rec = self._cache.get(action_id)
        if rec is None:
            try:
                row = await self.store.get_action(action_id)
            except Exception:  # noqa: BLE001 - store outage: an action from this process is still readable
                log.warning("action lookup unavailable action_id=%s", action_id)
                row = None
            if row is None:
                raise ActionError(404, "unknown action")
            rec = Action.from_row(row)
            self._cache[action_id] = rec
        if not owner or rec.owner != owner:
            raise ActionError(404, "unknown action")
        return rec

    async def list(self, owner: str, *, session_id: Optional[str] = None,
                   call_id: Optional[str] = None) -> list[dict[str, Any]]:
        merged: dict[str, Action] = {}
        try:
            for row in await self.store.list_actions(owner, session_id=session_id, call_id=call_id):
                merged[row["action_id"]] = Action.from_row(row)
        except Exception:  # noqa: BLE001 - store outage: show what this process knows
            log.warning("action list unavailable; showing this process only")
        for rec in self._cache.values():
            if rec.owner == owner and _in_scope(rec, session_id, call_id):
                merged[rec.action_id] = rec
        ordered = sorted(merged.values(), key=lambda r: (r.created_at_ms, r.action_id))
        return [self.wire(r) for r in ordered]

    # --------------------------------------------------------------- proposals
    async def propose(self, *, owner: Optional[str], playbook: Optional[str], proposals: list[tuple[str, dict]],
                      session_id: Optional[str] = None, call_id: Optional[str] = None,
                      proposed_by: str = "model") -> list[Action]:
        """Store proposals as `proposed` actions. Never executes. Duplicates of a pending or done action in the same
        session are skipped (SPEC 12 dedupe). Returns the new actions."""
        if not owner or not proposals:
            return []
        created: list[Action] = []
        for kind, params in proposals:
            try:
                clean = self._validate(kind, params, playbook)
            except ActionError:
                log.info("dropped proposal that no longer validates kind=%s", kind)
                continue
            if self._duplicate(owner, kind, clean, session_id, call_id):
                continue
            rec = Action(action_id=new_id(), owner=owner, kind=kind, params=clean, status="proposed",
                         summary=summarize(kind, clean), session_id=session_id, call_id=call_id, playbook=playbook,
                         proposed_by=proposed_by)
            await self._commit(rec)
            created.append(rec)
        return created

    async def create(self, *, owner: str, playbook: Optional[str], kind: str, params: Any,
                     session_id: Optional[str] = None, call_id: Optional[str] = None) -> Action:
        """Manual create from the UI (proposed_by=user). The caller has already been checked against the scope."""
        clean = self._validate(kind, params, playbook)
        if self._duplicate(owner, kind, clean, session_id, call_id):
            raise ActionError(409, "the same action is already pending in this session")
        rec = Action(action_id=new_id(), owner=owner, kind=kind, params=clean, status="proposed",
                     summary=summarize(kind, clean), session_id=session_id, call_id=call_id, playbook=playbook,
                     proposed_by="user")
        await self._commit(rec)
        return rec

    def _duplicate(self, owner: str, kind: str, params: dict, session_id: Optional[str], call_id: Optional[str]) -> bool:
        key = dedupe_key(kind, params)
        for rec in self._cache.values():
            if rec.owner != owner or rec.status == "undone" or not _in_scope(rec, session_id, call_id):
                continue
            if rec.kind == kind and dedupe_key(rec.kind, rec.params) == key:
                return True
        return False

    def _validate(self, kind: str, params: Any, playbook: Optional[str]) -> dict[str, Any]:
        try:
            clean = validate_params(kind, params)
        except ActionValidationError as exc:
            raise ActionError(400, str(exc)) from exc
        if kind == "enroll":
            programs: tuple[str, ...] = ()
            if playbook:
                try:
                    programs = self.registry.get(playbook).programs
                except KeyError:
                    programs = ()
            if clean["program"] not in programs:
                allowed = ", ".join(programs) if programs else "none are defined for this playbook"
                raise ActionError(400, f"program must be one of: {allowed}")
        return clean

    # --------------------------------------------------------------- decisions
    def _lock(self, action_id: str) -> asyncio.Lock:
        return self._locks.setdefault(action_id, asyncio.Lock())

    async def decide(self, owner: Optional[str], action_id: str, decision: str,
                     params: Optional[dict[str, Any]] = None) -> Action:
        if decision not in ("approve", "reject"):
            raise ActionError(400, "decision must be approve or reject")
        async with self._lock(action_id):
            rec = await self.get(owner, action_id)
            if decision == "reject":
                if rec.status == "rejected":
                    return rec
                if rec.status not in ("proposed", "failed"):
                    raise ActionError(409, f"cannot reject an action that is {rec.status}")
                rec.status = "rejected"
                rec.decided_at_ms, rec.decided_by = self._clock(), owner
                await self._commit(rec)
                return rec
            if rec.status in EXECUTED:
                return rec                                  # already executing, sent or outboxed: execute once
            if rec.status not in ("proposed", "failed"):
                raise ActionError(409, f"cannot approve an action that is {rec.status}")
            if params is not None:
                rec.params = self._validate(rec.kind, params, rec.playbook)
                rec.summary = summarize(rec.kind, rec.params)
            rec.status, rec.error = "executing", None
            rec.decided_at_ms, rec.decided_by = self._clock(), owner
            await self._commit(rec)
            try:
                status, result = await self._execute(rec)
            except Exception as exc:  # noqa: BLE001 - every failure is reported on the card, none escapes
                rec.status, rec.error = "failed", _public_error(exc)
                await self._commit(rec)
                return rec
            rec.status, rec.result, rec.error = status, result, None
            await self._commit(rec)
            return rec

    async def undo(self, owner: Optional[str], action_id: str) -> Action:
        async with self._lock(action_id):
            rec = await self.get(owner, action_id)
            if rec.status == "undone":
                return rec
            if rec.status not in ("done", "outboxed"):
                raise ActionError(409, f"cannot undo an action that is {rec.status}")
            if rec.kind not in UNDOABLE:
                raise ActionError(409, "an email cannot be undone once it is sent; send a follow-up instead")
            try:
                extra = await self._undo(rec)
            except Exception as exc:  # noqa: BLE001
                rec.error = _public_error(exc)
                await self._commit(rec)
                raise ActionError(500, f"undo failed: {rec.error}") from exc
            rec.result = {**(rec.result or {}), **extra, "undone_at_ms": self._clock()}
            rec.status, rec.error = "undone", None
            await self._commit(rec)
            return rec

    # --------------------------------------------------------------- execution
    async def _execute(self, rec: Action) -> tuple[str, dict[str, Any]]:
        if rec.kind == "send_email":
            msg, recipients, safety = self._build_email(rec, self._now())
            return await self._deliver(msg, recipients, safety, {"to": safety["to"], "cc": safety["cc"]})
        if rec.kind == "create_meeting":
            msg, ics_text, attendees, safety, info = self._build_invite(rec, self._now())
            status, result = await self._deliver(msg, attendees, safety, info)
            result["ics"] = ics_text
            return status, result
        if rec.kind == "update_contact":
            return "done", await self._exec_update_contact(rec)
        if rec.kind == "enroll":
            return "done", await self._exec_enroll(rec)
        if rec.kind == "update_record":
            return await self._exec_update_record(rec)
        raise ValueError(f"unknown kind {rec.kind!r}")

    async def _undo(self, rec: Action) -> dict[str, Any]:
        result = rec.result or {}
        if rec.kind == "create_meeting":
            now = self._now()
            p = rec.params
            start = datetime.fromisoformat(p["start_iso"])
            attendees = list(result.get("attendees") or [])
            event_title = result.get("event_title", p["title"])
            cancel_ics = ics.build_cancel(uid=result["uid"], title=event_title, start=start,
                                          duration_min=p["duration_min"], attendees=attendees,
                                          organizer=result.get("organizer") or self._sender_address(),
                                          dtstamp=_utc(now), sequence=int(result.get("sequence", 0)) + 1)
            msg = mail.build_message(sender=self._sender(), to=attendees, cc=[],
                                     subject=f"Cancelled: {event_title}",
                                     body_text="This meeting has been cancelled.",
                                     message_id=mail.new_message_id(rec.action_id + ":cancel"), date=_utc(now),
                                     invite=(cancel_ics, "CANCEL"))
            if self._smtp is None:
                return {"cancel_ics": cancel_ics, "cancel_eml": mail.to_eml(msg), "undo_mode": "outbox"}
            await self._send_off_thread(msg, attendees)
            return {"cancel_ics": cancel_ics, "undo_mode": "smtp"}
        if rec.kind == "update_contact":
            row = await self.store.find_contact(rec.owner, contact_id=result["contact_id"])
            if row is None:
                raise LookupError("the contact no longer exists")
            restored = {**row, **(result.get("previous") or {}), "updated_at_ms": self._clock()}
            await self._put_contact(restored)
            return {"restored": result.get("previous") or {}}
        if rec.kind == "enroll":
            await self.store.delete_enrollment(result["enrollment_id"])
            return {"enrollment_removed": True}
        if rec.kind == "update_record":
            return await self._undo_update_record(rec)
        raise ValueError(f"{rec.kind} cannot be undone")

    def _build_email(self, rec: Action, now: int) -> tuple[Any, list[str], dict[str, Any]]:
        p = rec.params
        to, cc, tag, safety = self._route(p["to"], p["cc"])
        msg = mail.build_message(sender=self._sender(), to=to, cc=cc, subject=tag + p["subject"],
                                 body_text=p["body_text"], message_id=mail.new_message_id(rec.action_id),
                                 date=_utc(now))
        return msg, to + cc, {**safety, "to": to, "cc": cc, "subject": tag + p["subject"]}

    def _build_invite(self, rec: Action, now: int) -> tuple[Any, str, list[str], dict[str, Any], dict[str, Any]]:
        p = rec.params
        attendees, _, tag, safety = self._route(p["attendees"], [])
        start = datetime.fromisoformat(p["start_iso"])
        event_title = tag + p["title"]
        uid = f"{rec.action_id}@sidekick.invalid"
        organizer = self._organizer_address(rec.owner)
        ics_text = ics.build_invite(uid=uid, title=event_title, start=start, duration_min=p["duration_min"],
                                    attendees=attendees, organizer=organizer, reminders=p["reminders"],
                                    dtstamp=_utc(now), description=p["description"], location=p["location"],
                                    sequence=0)
        body = _invite_body(p, start, event_title)
        subject = INVITE_PREFIX + event_title
        msg = mail.build_message(sender=self._sender(), to=attendees, cc=[], subject=subject, body_text=body,
                                 message_id=mail.new_message_id(rec.action_id), date=_utc(now),
                                 invite=(ics_text, "REQUEST"))
        info = {"attendees": attendees, "event_title": event_title, "title": p["title"], "start_iso": p["start_iso"],
                "duration_min": p["duration_min"], "uid": uid, "sequence": 0, "organizer": organizer}
        return msg, ics_text, attendees, {**safety, "subject": subject, "original_attendees": p["attendees"]}, info

    def _route(self, to: list[str], cc: list[str]) -> tuple[list[str], list[str], str, dict[str, Any]]:
        """Recipients as they will be sent, the subject tag, and the safety record. Without the lock: unchanged."""
        if not self.settings.action_safety_lock:
            return list(to), list(cc), "", {"lock": False, "original_to": list(to), "original_cc": list(cc)}
        safe = self.settings.action_safe_recipient
        originals = [*to, *cc]
        tag = f"[Sidekick test → {', '.join(originals)}] "
        return [safe], [], tag, {"lock": True, "original_to": list(to), "original_cc": list(cc),
                                 "safe_recipient": safe}

    async def _deliver(self, msg: Any, recipients: list[str], safety: dict[str, Any],
                       info: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        base = {**info, "message_id": str(msg["Message-ID"]), "safety": safety, "eml": mail.to_eml(msg),
                "sent_at_ms": self._now()}
        if self._smtp is None:
            return "outboxed", {"mode": "outbox", **base}
        await self._send_off_thread(msg, recipients)
        return "done", {"mode": "smtp", **base}

    async def _send_off_thread(self, msg: Any, recipients: list[str]) -> None:
        if self._smtp is None:
            raise SendError("no SMTP relay is configured")
        try:
            await asyncio.to_thread(self._send, self._smtp, msg, recipients)
        except (smtplib.SMTPException, OSError) as exc:
            raise SendError(f"SMTP send failed ({type(exc).__name__}); retry to send again") from exc

    async def _exec_update_contact(self, rec: Action) -> dict[str, Any]:
        p = rec.params
        found = await self._find_for(rec.owner, p)
        if found is None:
            raise LookupError("no contact matches that id or email")
        changes = {k: v for k, v in p["fields"].items() if v is not None}
        previous = {k: found.get(k) for k in changes}
        now = self._now()
        if found.get("owner") == rec.owner:
            row = {**found, **changes, "updated_at_ms": now}
            copied_from = None
        else:
            # The shared directory is read-only for everyone: an update makes the caller's own copy.
            copied_from = found["contact_id"]
            row = {**{k: found.get(k) for k in _CONTACT_COLS}, "contact_id": new_id(), "owner": rec.owner,
                   **changes, "updated_at_ms": now}
        await self._put_contact(row)
        return {"contact_id": row["contact_id"], "previous": previous, "fields": changes, "copied_from": copied_from}

    async def _exec_enroll(self, rec: Action) -> dict[str, Any]:
        p = rec.params
        contact = await self._find_for(rec.owner, p)
        created = False
        if contact is None:
            if not p.get("email"):
                raise LookupError("no contact with that id")
            contact = {"contact_id": new_id(), "owner": rec.owner, "first_name": None, "last_name": None,
                       "email": p["email"], "phone": None, "company": None, "updated_at_ms": self._now()}
            await self._put_contact(contact)
            created = True
        enrollment = {"enrollment_id": new_id(), "contact_id": contact["contact_id"], "program": p["program"],
                      "owner": rec.owner, "note": p.get("note"), "created_at_ms": self._now()}
        await self.store.put_enrollment(enrollment)
        self._history("enrollments", hs.enrollment_row(enrollment))
        return {"enrollment_id": enrollment["enrollment_id"], "contact_id": contact["contact_id"],
                "program": p["program"], "created_contact": created, "contact_email": contact.get("email")}

    async def _find_for(self, owner: str, params: dict[str, Any]) -> Optional[dict[str, Any]]:
        if params.get("contact_id"):
            return await self.store.find_contact(owner, contact_id=params["contact_id"])
        return await self.store.find_contact(owner, email=params["email"])

    # ------------------------------------------------------ update_record (UC Delta via SQL warehouse)
    async def _exec_update_record(self, rec: Action) -> tuple[str, dict[str, Any]]:
        """MERGE the proposed fields into the CRM account on the SQL warehouse, or render the MERGE for the outbox
        when no warehouse executor is configured. The rendered SQL is for display only; the real call binds every
        value as a parameter."""
        if self._record_target is None:
            raise ValueError("the CRM record target is not configured (CRM_CATALOG/CRM_SCHEMA/CRM_TABLE)")
        target = self._record_target
        p = rec.params
        account_id = p["account_id"]
        fields = {k: v for k, v in p["fields"].items() if v is not None}
        now = self._now()
        sql, params, types = records.build_merge(target, account_id, fields, now)
        rendered = records.render_sql(sql, params)
        base = {"table": target.full_name, "account_id": account_id, "fields": fields, "merge_sql": rendered}
        if self._records_exec is None:
            # Outbox: no warehouse. Show exactly what would run.
            return "outboxed", {"mode": "outbox", **base}
        # Capture the previous row first, so undo can restore it (or delete a row this update creates).
        previous = await self._record_row(target, account_id)
        await self._records_exec(sql, params, types)
        return "done", {"mode": "warehouse", "previous": previous, "existed": previous is not None, **base}

    async def _undo_update_record(self, rec: Action) -> dict[str, Any]:
        if self._record_target is None or self._records_exec is None:
            raise LookupError("the CRM record target is not available to undo")
        target = self._record_target
        result = rec.result or {}
        account_id = result.get("account_id") or rec.params["account_id"]
        previous = result.get("previous")
        if previous:
            fields = {c: previous.get(c) for c in records.FIELD_COLUMNS}
            sql, params, types = records.build_merge(target, account_id, fields, self._now())
            await self._records_exec(sql, params, types)
            return {"restored": fields}
        sql, params, types = records.build_delete(target, account_id)
        await self._records_exec(sql, params, types)
        return {"record_deleted": True}

    async def _record_row(self, target: records.RecordTarget, account_id: str) -> Optional[dict[str, Any]]:
        if self._records_exec is None:
            return None
        sql, params, types = records.build_select(target, account_id)
        rows = await self._records_exec(sql, params, types)
        return rows[0] if rows else None

    async def _put_contact(self, row: dict[str, Any]) -> None:
        await self.store.put_contact(row)
        self._history("contacts", hs.contact_row(row))

    # ---------------------------------------------------------------- renders
    async def rendered(self, owner: Optional[str], action_id: str, which: str) -> tuple[str, str]:
        """(filename, text) for the message endpoints. which: "eml" (email or invite) or "ics" (meetings only).
        Executed actions return what was sent (or outboxed); others return a preview of what would be sent."""
        rec = await self.get(owner, action_id)
        result = rec.result or {}
        if which == "ics":
            if rec.kind != "create_meeting":
                raise ActionError(404, "no calendar invite for this action")
            text = result.get("ics") or self._build_invite(rec, self._now())[1]
            return f"invite-{rec.action_id[:8]}.ics", text
        if rec.kind not in ("send_email", "create_meeting"):
            raise ActionError(404, "no message for this action")
        if result.get("cancel_eml") and not result.get("eml"):
            return f"cancel-{rec.action_id[:8]}.eml", result["cancel_eml"]
        if result.get("eml"):
            return f"message-{rec.action_id[:8]}.eml", result["eml"]
        if rec.kind == "send_email":
            text = mail.to_eml(self._build_email(rec, self._now())[0])
        else:
            text = mail.to_eml(self._build_invite(rec, self._now())[0])
        return f"preview-{rec.action_id[:8]}.eml", text

    # ----------------------------------------------------------------- helpers
    def _now(self) -> int:
        return self._clock()

    def _sender(self) -> str:
        return self.settings.smtp_from or DEFAULT_SENDER

    def _sender_address(self) -> str:
        return parseaddr(self._sender())[1]

    def _organizer_address(self, owner: Optional[str]) -> str:
        return owner if (owner and "@" in owner and " " not in owner) else self._sender_address()

    async def _commit(self, rec: Action) -> None:
        rec.updated_at_ms = self._clock()
        self._cache[rec.action_id] = rec
        row = rec.to_row()
        try:
            await self.store.put_action(row)
        except Exception:  # noqa: BLE001 - persistence is best effort; the live card still updates
            log.exception("action persist failed action_id=%s", rec.action_id)
        self._history("actions", hs.action_row(row))
        if self._emit is not None:
            try:
                self._emit(rec)
            except Exception:  # noqa: BLE001 - a socket problem must not undo a decision
                log.exception("action event failed action_id=%s", rec.action_id)

    def _history(self, table: str, row: dict[str, Any]) -> None:
        if self.history is not None:
            self.history.emit(table, row)


_CONTACT_COLS = ("first_name", "last_name", "email", "phone", "company")


def _in_scope(rec: Action, session_id: Optional[str], call_id: Optional[str]) -> bool:
    if session_id is not None:
        return rec.session_id == session_id
    if call_id is not None:
        return rec.call_id == call_id
    return False


def _utc(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def _invite_body(p: dict[str, Any], start: datetime, title: str) -> str:
    when = start.strftime("%A %d %B %Y, %H:%M ") + (start.tzname() or "")
    lines = [f"You are invited: {title}", f"When: {when.strip()} ({p['duration_min']} minutes)"]
    if p.get("location"):
        lines.append(f"Where: {p['location']}")
    if p.get("description"):
        lines += ["", p["description"]]
    reminders = ", ".join(
        f"{'email' if r['method'] == 'email' else 'alert'} {_minutes_label(r['minutes'])} before"
        for r in p.get("reminders") or []
    )
    if reminders:
        lines += ["", f"Reminders: {reminders}."]
    return "\n".join(lines) + "\n"


def _minutes_label(minutes: int) -> str:
    if minutes and minutes % 1440 == 0:
        days = minutes // 1440
        return f"{days} day" + ("s" if days != 1 else "")
    return f"{minutes} minutes"


