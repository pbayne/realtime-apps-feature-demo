"""Actions (SPEC 12): schemas, RFC 5545 invites, MIME and SMTP, the approval state machine, safety lock, undo,
ownership, and the store and history parity for the new tables."""

from __future__ import annotations

import asyncio
import email
import email.policy
import smtplib
from datetime import datetime, timezone
from pathlib import Path

import pytest
from actions import ics, mail
from actions.schemas import ActionValidationError, summarize, validate_params
from actions.service import ActionError, ActionService
from config import Settings
from enrich.rules import load_rules
from history import schema as hs
from sidekick.playbooks import PlaybookRegistry
from store.memory import MemoryStore

APP = Path(__file__).resolve().parents[2] / "src" / "app"
OWNER = "dana.advisor@databricks.com"
OTHER = "someone.else@databricks.com"
SAFE = "patrick.bayne@databricks.com"


def _registry() -> PlaybookRegistry:
    return PlaybookRegistry.load(APP / "playbooks", load_rules(None), APP / "data" / "replay")


def _settings(**overrides) -> Settings:
    base = dict(mode="local", llm_enabled=False, action_safety_lock=False, smtp_host=None, smtp_from=None)
    base.update(overrides)
    return Settings(**base)


class FakeSmtp:
    """Records what would have been sent. Replaces mail.send_smtp."""

    def __init__(self, fail: bool = False) -> None:
        self.sent: list[tuple[list[str], bytes]] = []
        self.fail = fail

    def __call__(self, cfg, msg, recipients):
        if self.fail:
            raise smtplib.SMTPServerDisconnected("relay went away")
        self.sent.append((list(recipients), msg.as_bytes(policy=email.policy.SMTP)))


class Clock:
    def __init__(self) -> None:
        self.ms = 1_767_225_600_000        # 2026-01-01T00:00:00Z

    def __call__(self) -> int:
        self.ms += 1000
        return self.ms


def _service(*, smtp: FakeSmtp | None = None, **settings) -> tuple[ActionService, MemoryStore, FakeSmtp | None]:
    store = MemoryStore()
    svc_settings = _settings(**settings)
    if smtp is not None and svc_settings.smtp_host is None:
        svc_settings = _settings(smtp_host="relay.example.com", smtp_from="Sidekick <sidekick@example.com>", **settings)
    svc = ActionService(settings=svc_settings, store=store, registry=_registry(), send=smtp or FakeSmtp(), clock=Clock())
    return svc, store, smtp


EMAIL_PARAMS = {"to": ["dana.lee@example.com"], "cc": [], "subject": "Your fee breakdown",
                "body_text": "Hi Dana, here is the breakdown."}
MEETING_PARAMS = {"title": "Retirement income review", "attendees": ["dana.lee@example.com"],
                  "start_iso": "2026-11-03T14:00:00-05:00", "duration_min": 45, "description": "Bring statements.",
                  "location": "Zoom"}


# ---------------------------------------------------------------- schemas

def test_send_email_validates_and_rejects_bad_input():
    clean = validate_params("send_email", EMAIL_PARAMS)
    assert clean["to"] == ["dana.lee@example.com"] and clean["cc"] == []
    with pytest.raises(ActionValidationError, match="not an email address"):
        validate_params("send_email", {**EMAIL_PARAMS, "to": ["nope"]})
    with pytest.raises(ActionValidationError, match="single line"):
        validate_params("send_email", {**EMAIL_PARAMS, "subject": "Hi\r\nBcc: x@y.co"})
    with pytest.raises(ActionValidationError, match="Extra inputs"):
        validate_params("send_email", {**EMAIL_PARAMS, "bcc": ["x@y.co"]})


def test_meeting_needs_timezone_and_gets_default_reminders():
    clean = validate_params("create_meeting", MEETING_PARAMS)
    assert clean["reminders"] == [{"method": "email", "minutes": 1440}, {"method": "display", "minutes": 10}]
    with pytest.raises(ActionValidationError, match="timezone"):
        validate_params("create_meeting", {**MEETING_PARAMS, "start_iso": "2026-11-03T14:00:00"})
    with pytest.raises(ActionValidationError, match="ISO 8601"):
        validate_params("create_meeting", {**MEETING_PARAMS, "start_iso": "next tuesday at two-ish"})
    with pytest.raises(ActionValidationError):
        validate_params("create_meeting", {**MEETING_PARAMS, "duration_min": 2})


def test_contact_and_enroll_need_exactly_one_reference():
    with pytest.raises(ActionValidationError, match="exactly one"):
        validate_params("enroll", {"contact_id": "c-1", "email": "a@b.co", "program": "Autopay"})
    with pytest.raises(ActionValidationError, match="exactly one"):
        validate_params("update_contact", {"fields": {"phone": "+1 555 010 1234"}})
    with pytest.raises(ActionValidationError, match="at least one"):
        validate_params("update_contact", {"email": "a@b.co", "fields": {}})
    with pytest.raises(ActionValidationError, match="phone"):
        validate_params("update_contact", {"email": "a@b.co", "fields": {"phone": "call me"}})
    assert validate_params("enroll", {"contact_id": "c-1", "program": "Autopay"})["contact_id"] == "c-1"


def test_unknown_kind_and_summaries_read_as_plain_language():
    with pytest.raises(ActionValidationError, match="unknown action kind"):
        validate_params("transfer_money", {})
    assert summarize("send_email", validate_params("send_email", EMAIL_PARAMS)) == "Email Dana: Your fee breakdown"
    meeting = summarize("create_meeting", validate_params("create_meeting", MEETING_PARAMS))
    assert "Retirement income review" in meeting and "45 min" in meeting and "UTC-05" in meeting


# ---------------------------------------------------------------- invites

def _invite(attendees=("dana.lee@example.com",), description="", location=None):
    start = datetime(2026, 11, 3, 19, 0, tzinfo=timezone.utc)
    rem = [{"method": "email", "minutes": 1440}, {"method": "display", "minutes": 10}]
    return ics.build_invite(uid="a-1@sidekick.invalid", title="Retirement, income; review " + "é" * 50, start=start,
                            duration_min=45, attendees=list(attendees), organizer="dana.advisor@databricks.com",
                            reminders=rem, dtstamp=start, description=description, location=location)


def test_invite_is_strict_rfc5545_with_alarms_and_utc_times():
    text = _invite(description="Line one\nline, two; three", location="Zoom")
    assert text.endswith("\r\n") and "\r\r" not in text and "\n" not in text.replace("\r\n", "")
    props = ics.parse_strict(text)
    assert props["DTSTART"] == "20261103T190000Z" and props["DTEND"] == "20261103T194500Z"
    assert props["UID"] == "a-1@sidekick.invalid" and props["DTSTAMP"].endswith("Z")
    triggers = sorted((a["ACTION"], a["TRIGGER"]) for a in props["_alarms"])
    assert triggers == [("DISPLAY", "-PT10M"), ("EMAIL", "-P1D")]
    assert props["_attendees"] == ["mailto:dana.lee@example.com"]
    assert "Line one\\nline\\, two\\; three" in text          # TEXT escaping, not raw punctuation
    assert "METHOD:REQUEST" in text


def test_long_lines_fold_at_75_octets_and_never_split_a_character():
    text = _invite()
    physical = text.split("\r\n")[:-1]
    assert max(len(line.encode("utf-8")) for line in physical) <= 75
    assert any(line.startswith(" ") for line in physical)      # the title was folded
    assert ics.parse_strict(text)["SUMMARY"].count("é") == 50   # unfolding restores every character


def test_round_trip_through_icalendar_package():
    icalendar = pytest.importorskip("icalendar")
    cal = icalendar.Calendar.from_ical(_invite(location="Zoom"))
    event = next(c for c in cal.walk() if c.name == "VEVENT")
    assert str(event["UID"]) == "a-1@sidekick.invalid"
    assert event["DTSTART"].dt == datetime(2026, 11, 3, 19, 0, tzinfo=timezone.utc)
    actions = sorted(str(a["ACTION"]) for a in event.walk("VALARM"))
    assert actions == ["DISPLAY", "EMAIL"]


def test_cancel_reuses_uid_with_higher_sequence():
    start = datetime(2026, 11, 3, 19, 0, tzinfo=timezone.utc)
    text = ics.build_cancel(uid="a-1@sidekick.invalid", title="x", start=start, duration_min=30,
                            attendees=["dana.lee@example.com"], organizer="o@example.com", dtstamp=start)
    props = ics.parse_strict(text)
    assert "METHOD:CANCEL" in text and props["STATUS"] == "CANCELLED" and props["SEQUENCE"] == "1"


def test_strict_parser_refuses_bare_lf_and_long_lines():
    with pytest.raises(ics.IcsError):
        ics.parse_strict(_invite().replace("\r\n", "\n"))
    bad = _invite().replace("END:VCALENDAR\r\n", "X-LONG:" + "a" * 90 + "\r\nEND:VCALENDAR\r\n")
    with pytest.raises(ics.IcsError, match="75 octets"):
        ics.parse_strict(bad)


def test_naive_times_are_refused_for_calendars():
    with pytest.raises(ValueError):
        ics.fmt_utc(datetime(2026, 11, 3, 19, 0))


# ---------------------------------------------------------------- mime

def test_email_message_headers_and_body():
    msg = mail.build_message(sender="Sidekick <sidekick@example.com>", to=["dana.lee@example.com"], cc=[],
                             subject="Fees — breakdown", body_text="Hello Dana.",
                             message_id=mail.new_message_id("a-9"), date=datetime(2026, 1, 1, tzinfo=timezone.utc))
    parsed = email.message_from_bytes(msg.as_bytes(policy=email.policy.SMTP), policy=email.policy.default)
    assert parsed["To"] == "dana.lee@example.com"
    assert str(parsed["Subject"]) == "Fees — breakdown"
    assert parsed["Message-ID"].endswith("@sidekick.invalid>")
    assert parsed.get_body(("plain",)).get_content().strip() == "Hello Dana."


def test_invite_message_carries_calendar_part_and_attachment():
    msg = mail.build_message(sender="s@example.com", to=["dana.lee@example.com"], cc=[], subject="Invitation: x",
                             body_text="You are invited.", message_id=mail.new_message_id("a-2"),
                             date=datetime(2026, 1, 1, tzinfo=timezone.utc), invite=(_invite(), "REQUEST"))
    parsed = email.message_from_bytes(msg.as_bytes(policy=email.policy.SMTP), policy=email.policy.default)
    calendar_parts = [p for p in parsed.walk() if p.get_content_type() == "text/calendar"]
    assert calendar_parts and all(p.get_param("method") == "REQUEST" for p in calendar_parts)
    attachments = [p for p in parsed.iter_attachments()]
    assert attachments and attachments[0].get_filename() == "invite.ics"
    assert "BEGIN:VCALENDAR" in attachments[0].get_content()


def test_smtp_sender_uses_starttls_and_login(monkeypatch):
    calls: list[tuple] = []

    class Fake:
        def __init__(self, host, port, timeout):
            calls.append(("connect", host, port, timeout))

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def ehlo(self):
            calls.append(("ehlo",))

        def starttls(self, context):
            calls.append(("starttls",))

        def login(self, user, password):
            calls.append(("login", user))

        def send_message(self, msg, from_addr, to_addrs):
            calls.append(("send", from_addr, list(to_addrs)))

    monkeypatch.setattr(mail.smtplib, "SMTP", Fake)
    msg = mail.build_message(sender="Sidekick <sk@example.com>", to=["a@example.com"], cc=["b@example.com"],
                             subject="s", body_text="b", message_id="<x@y>", date=datetime(2026, 1, 1, tzinfo=timezone.utc))
    cfg = mail.SmtpConfig(host="relay", port=587, username="u", password="p", timeout_s=5)
    mail.send_smtp(cfg, msg, ["a@example.com", "b@example.com"])
    assert ("connect", "relay", 587, 5) in calls and ("starttls",) in calls and ("login", "u") in calls
    assert ("send", "sk@example.com", ["a@example.com", "b@example.com"]) in calls


# ---------------------------------------------------------------- service: outbox and smtp

def test_no_smtp_means_outbox_and_nothing_is_sent():
    async def run():
        svc, store, _ = _service()
        smtp = FakeSmtp()
        svc._send = smtp
        rec = await svc.create(owner=OWNER, playbook="contact_center", kind="send_email", params=EMAIL_PARAMS,
                               session_id="v-1")
        done = await svc.decide(OWNER, rec.action_id, "approve")
        assert done.status == "outboxed" and done.result["mode"] == "outbox"
        assert smtp.sent == []
        assert "Subject: Your fee breakdown" in done.result["eml"]
        assert "Content-Type: text/plain" in done.result["eml"]
        name, text = await svc.rendered(OWNER, rec.action_id, "eml")
        assert text == done.result["eml"] and name.endswith(".eml")
    asyncio.run(run())


def test_approve_sends_through_smtp_with_headers_and_invite():
    async def run():
        smtp = FakeSmtp()
        svc, store, _ = _service(smtp=smtp)
        email_rec = await svc.create(owner=OWNER, playbook="advisor_coach", kind="send_email", params=EMAIL_PARAMS,
                                     session_id="v-1")
        meet = await svc.create(owner=OWNER, playbook="advisor_coach", kind="create_meeting", params=MEETING_PARAMS,
                                session_id="v-1")
        sent_mail = await svc.decide(OWNER, email_rec.action_id, "approve")
        sent_meet = await svc.decide(OWNER, meet.action_id, "approve")
        assert sent_mail.status == "done" and sent_meet.status == "done"
        assert len(smtp.sent) == 2
        recipients, raw = smtp.sent[0]
        parsed = email.message_from_bytes(raw, policy=email.policy.default)
        assert recipients == ["dana.lee@example.com"] and parsed["To"] == "dana.lee@example.com"
        assert parsed["Subject"] == "Your fee breakdown" and parsed["Message-ID"]
        assert "Hi Dana" in parsed.get_body(("plain",)).get_content()
        _, meet_raw = smtp.sent[1]
        invite_msg = email.message_from_bytes(meet_raw, policy=email.policy.default)
        attachments = list(invite_msg.iter_attachments())
        assert attachments and attachments[0].get_filename() == "invite.ics"
        assert "METHOD:REQUEST" in attachments[0].get_content()
        assert sent_meet.result["mode"] == "smtp" and sent_meet.result["sequence"] == 0
    asyncio.run(run())


def test_relay_failure_marks_failed_and_retry_sends_once():
    async def run():
        smtp = FakeSmtp(fail=True)
        svc, _, _ = _service(smtp=smtp)
        rec = await svc.create(owner=OWNER, playbook="advisor_coach", kind="send_email", params=EMAIL_PARAMS)
        failed = await svc.decide(OWNER, rec.action_id, "approve")
        assert failed.status == "failed" and "SMTP send failed" in failed.error
        smtp.fail = False
        retried = await svc.decide(OWNER, rec.action_id, "approve")
        assert retried.status == "done" and len(smtp.sent) == 1
    asyncio.run(run())


# ---------------------------------------------------------------- state machine

def test_double_approve_executes_once():
    async def run():
        smtp = FakeSmtp()
        svc, _, _ = _service(smtp=smtp)
        rec = await svc.create(owner=OWNER, playbook="advisor_coach", kind="send_email", params=EMAIL_PARAMS)
        first, second = await asyncio.gather(svc.decide(OWNER, rec.action_id, "approve"),
                                             svc.decide(OWNER, rec.action_id, "approve"))
        third = await svc.decide(OWNER, rec.action_id, "approve")
        assert len(smtp.sent) == 1
        assert first.status == second.status == third.status == "done"
    asyncio.run(run())


def test_only_the_owner_can_see_or_decide_and_others_get_404():
    async def run():
        svc, _, _ = _service()
        rec = await svc.create(owner=OWNER, playbook="advisor_coach", kind="send_email", params=EMAIL_PARAMS)
        for call in (svc.decide(OTHER, rec.action_id, "approve"), svc.decide(OTHER, rec.action_id, "reject"),
                     svc.undo(OTHER, rec.action_id), svc.rendered(OTHER, rec.action_id, "eml")):
            with pytest.raises(ActionError) as err:
                await call
            assert err.value.status == 404
        with pytest.raises(ActionError) as err:
            await svc.decide(None, rec.action_id, "approve")
        assert err.value.status == 404
        assert (await svc.list(OTHER, session_id="v-1")) == []
    asyncio.run(run())


def test_reject_is_final_and_blocks_approval():
    async def run():
        smtp = FakeSmtp()
        svc, _, _ = _service(smtp=smtp)
        rec = await svc.create(owner=OWNER, playbook="advisor_coach", kind="send_email", params=EMAIL_PARAMS)
        rejected = await svc.decide(OWNER, rec.action_id, "reject")
        assert rejected.status == "rejected"
        with pytest.raises(ActionError) as err:
            await svc.decide(OWNER, rec.action_id, "approve")
        assert err.value.status == 409 and smtp.sent == []
    asyncio.run(run())


def test_edit_on_approve_revalidates_params():
    async def run():
        svc, _, _ = _service()
        rec = await svc.create(owner=OWNER, playbook="advisor_coach", kind="send_email", params=EMAIL_PARAMS)
        with pytest.raises(ActionError) as err:
            await svc.decide(OWNER, rec.action_id, "approve", {**EMAIL_PARAMS, "to": ["not-an-address"]})
        assert err.value.status == 400
        edited = await svc.decide(OWNER, rec.action_id, "approve",
                                  {**EMAIL_PARAMS, "subject": "Edited subject"})
        assert edited.params["subject"] == "Edited subject" and edited.summary.endswith("Edited subject")
    asyncio.run(run())


def test_enroll_program_must_belong_to_the_playbook():
    async def run():
        svc, _, _ = _service()
        with pytest.raises(ActionError, match="program must be one of"):
            await svc.create(owner=OWNER, playbook="advisor_coach", kind="enroll",
                             params={"email": "dana.lee@example.com", "program": "Autopay"})
        ok = await svc.create(owner=OWNER, playbook="contact_center", kind="enroll",
                              params={"email": "dana.lee@example.com", "program": "Autopay"})
        assert ok.status == "proposed"
    asyncio.run(run())


# ---------------------------------------------------------------- safety lock

def test_safety_lock_rewrites_recipients_and_records_originals():
    async def run():
        smtp = FakeSmtp()
        svc, _, _ = _service(smtp=smtp, action_safety_lock=True, action_safe_recipient=SAFE)
        rec = await svc.create(owner=OWNER, playbook="advisor_coach", kind="send_email",
                               params={**EMAIL_PARAMS, "cc": ["cc.person@example.com"]})
        done = await svc.decide(OWNER, rec.action_id, "approve")
        recipients, raw = smtp.sent[0]
        assert recipients == [SAFE]
        parsed = email.message_from_bytes(raw, policy=email.policy.default)
        assert parsed["To"] == SAFE and parsed["Cc"] is None
        assert str(parsed["Subject"]).startswith("[Sidekick test → dana.lee@example.com, cc.person@example.com] ")
        safety = done.result["safety"]
        assert safety["lock"] is True and safety["original_to"] == ["dana.lee@example.com"]
        assert safety["original_cc"] == ["cc.person@example.com"]
    asyncio.run(run())


def test_safety_lock_applies_to_meeting_attendees_and_outbox_preview():
    async def run():
        svc, _, _ = _service(action_safety_lock=True, action_safe_recipient=SAFE)
        rec = await svc.create(owner=OWNER, playbook="advisor_coach", kind="create_meeting", params=MEETING_PARAMS)
        _, preview = await svc.rendered(OWNER, rec.action_id, "ics")
        assert ics.parse_strict(preview)["_attendees"] == ["mailto:" + SAFE]   # unfolded, strictly parsed
        assert "mailto:dana.lee@example.com" not in preview.replace("\r\n ", "")   # only the title keeps the name
        assert "SUMMARY:[Sidekick test" in preview
        done = await svc.decide(OWNER, rec.action_id, "approve")
        assert done.status == "outboxed" and done.result["attendees"] == [SAFE]
        assert done.result["safety"]["original_attendees"] == ["dana.lee@example.com"]
    asyncio.run(run())


# ---------------------------------------------------------------- undo

def test_email_cannot_be_undone():
    async def run():
        svc, _, _ = _service(smtp=FakeSmtp())
        rec = await svc.create(owner=OWNER, playbook="advisor_coach", kind="send_email", params=EMAIL_PARAMS)
        await svc.decide(OWNER, rec.action_id, "approve")
        with pytest.raises(ActionError, match="cannot be undone") as err:
            await svc.undo(OWNER, rec.action_id)
        assert err.value.status == 409
    asyncio.run(run())


def test_update_contact_undo_restores_previous_values_and_copies_shared_contacts():
    async def run():
        svc, store, _ = _service()
        await store.put_contact({"contact_id": "demo-dana", "owner": None, "first_name": "Dana", "last_name": "Lee",
                                 "email": "dana.lee@example.com", "phone": None, "company": "Lee Trust",
                                 "updated_at_ms": 1})
        rec = await svc.create(owner=OWNER, playbook="advisor_coach", kind="update_contact",
                               params={"email": "dana.lee@example.com", "fields": {"company": "Lee Family Trust"}})
        done = await svc.decide(OWNER, rec.action_id, "approve")
        assert done.status == "done" and done.result["previous"] == {"company": "Lee Trust"}
        own = await store.find_contact(OWNER, contact_id=done.result["contact_id"])
        assert own["company"] == "Lee Family Trust" and own["owner"] == OWNER
        shared = await store.find_contact(OTHER, contact_id="demo-dana")
        assert shared["company"] == "Lee Trust"                  # the shared directory is not changed
        undone = await svc.undo(OWNER, rec.action_id)
        assert undone.status == "undone"
        restored = await store.find_contact(OWNER, contact_id=done.result["contact_id"])
        assert restored["company"] == "Lee Trust"
    asyncio.run(run())


def test_update_contact_on_unknown_email_fails_with_a_reason():
    async def run():
        svc, _, _ = _service()
        rec = await svc.create(owner=OWNER, playbook="advisor_coach", kind="update_contact",
                               params={"email": "ghost@example.com", "fields": {"phone": "+1 555 010 9999"}})
        failed = await svc.decide(OWNER, rec.action_id, "approve")
        assert failed.status == "failed" and "no contact matches" in failed.error
    asyncio.run(run())


def test_enroll_creates_contact_and_undo_removes_enrollment():
    async def run():
        svc, store, _ = _service()
        rec = await svc.create(owner=OWNER, playbook="contact_center", kind="enroll",
                               params={"email": "new.person@example.com", "program": "Autopay", "note": "by phone"})
        done = await svc.decide(OWNER, rec.action_id, "approve")
        assert done.status == "done" and done.result["created_contact"] is True
        assert done.result["program"] == "Autopay"
        assert len(store.enrollments) == 1
        await svc.undo(OWNER, rec.action_id)
        assert store.enrollments == {}
    asyncio.run(run())


def test_meeting_undo_sends_or_outboxes_a_cancel_with_the_same_uid():
    async def run():
        smtp = FakeSmtp()
        svc, _, _ = _service(smtp=smtp)
        rec = await svc.create(owner=OWNER, playbook="advisor_coach", kind="create_meeting", params=MEETING_PARAMS)
        done = await svc.decide(OWNER, rec.action_id, "approve")
        undone = await svc.undo(OWNER, rec.action_id)
        assert undone.status == "undone" and undone.result["undo_mode"] == "smtp"
        cancel = ics.parse_strict(undone.result["cancel_ics"])
        assert cancel["UID"] == done.result["uid"] and "METHOD:CANCEL" in undone.result["cancel_ics"]
        assert len(smtp.sent) == 2
    asyncio.run(run())


def test_undo_twice_is_harmless_and_undo_needs_a_done_action():
    async def run():
        svc, store, _ = _service()
        rec = await svc.create(owner=OWNER, playbook="contact_center", kind="enroll",
                               params={"email": "x@example.com", "program": "Loyalty discount"})
        with pytest.raises(ActionError) as err:
            await svc.undo(OWNER, rec.action_id)
        assert err.value.status == 409
        await svc.decide(OWNER, rec.action_id, "approve")
        await svc.undo(OWNER, rec.action_id)
        again = await svc.undo(OWNER, rec.action_id)
        assert again.status == "undone"
    asyncio.run(run())


# ---------------------------------------------------------------- messages and wire

def test_wire_hides_rendered_text_but_flags_it_and_reports_safety():
    async def run():
        svc, _, _ = _service()
        rec = await svc.create(owner=OWNER, playbook="advisor_coach", kind="create_meeting", params=MEETING_PARAMS)
        await svc.decide(OWNER, rec.action_id, "approve")
        wire = svc.wire(svc._cache[rec.action_id])
        assert "ics" not in wire["result"] and wire["result"]["has_ics"] is True
        assert wire["safety"] == {"lock": False, "safe_recipient": SAFE, "mode": "outbox"}
        name, text = await svc.rendered(OWNER, rec.action_id, "ics")
        assert name.endswith(".ics") and text.startswith("BEGIN:VCALENDAR")
        enroll = await svc.create(owner=OWNER, playbook="contact_center", kind="enroll",
                                  params={"email": "a@example.com", "program": "Autopay"})
        with pytest.raises(ActionError) as err:
            await svc.rendered(OWNER, enroll.action_id, "ics")
        assert err.value.status == 404
    asyncio.run(run())


def test_preview_renders_before_any_decision_without_sending():
    async def run():
        smtp = FakeSmtp()
        svc, _, _ = _service(smtp=smtp)
        rec = await svc.create(owner=OWNER, playbook="advisor_coach", kind="send_email", params=EMAIL_PARAMS)
        name, text = await svc.rendered(OWNER, rec.action_id, "eml")
        assert name.startswith("preview-") and "Subject: Your fee breakdown" in text
        assert smtp.sent == [] and rec.status == "proposed"
    asyncio.run(run())


def test_store_round_trip_keeps_params_and_result():
    async def run():
        svc, store, _ = _service()
        rec = await svc.create(owner=OWNER, playbook="advisor_coach", kind="send_email", params=EMAIL_PARAMS,
                               session_id="v-1")
        await svc.decide(OWNER, rec.action_id, "approve")
        svc._cache.clear()
        loaded = await svc.get(OWNER, rec.action_id)
        assert loaded.status == "outboxed" and loaded.params["to"] == ["dana.lee@example.com"]
        assert loaded.result["mode"] == "outbox"
        rows = await svc.list(OWNER, session_id="v-1")
        assert [r["action_id"] for r in rows] == [rec.action_id]
    asyncio.run(run())


# ---------------------------------------------------------------- history and schema

def test_action_history_row_projects_json_columns():
    row = hs.action_row({"action_id": "a", "session_id": "v", "owner": "o", "kind": "send_email", "summary": "s",
                         "params": {"to": ["x@y.co"]}, "status": "done", "created_at_ms": 1, "updated_at_ms": 2,
                         "result": {"mode": "outbox"}})
    rec = hs.to_record("actions", row)
    assert list(rec) == hs.column_names("actions")
    assert rec["params"] == '{"to": ["x@y.co"]}' and rec["call_id"] is None
    assert hs.to_record("enrollments", hs.enrollment_row({"enrollment_id": "e", "contact_id": "c", "program": "p",
                                                          "created_at_ms": 3}))["note"] is None
