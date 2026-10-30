"""Live check of human-in-the-loop actions on the deployed app (SPEC 12). Nothing leaves the safety lock.

Opens a voice session, then through the REST API: creates and approves an email and a meeting invite (addressed to
example.com, so the safety lock must rewrite every recipient to the safe address), downloads the rendered .eml and
.ics, enrolls a contact and undoes it, rejects one action, and checks that bad params are refused. Exit 0 only if
every check passes.

  .venv/bin/python scripts/actions_smoke.py --profile realtime-call-sidekick
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta, timezone

import httpx

TERMINAL = {"done", "outboxed", "sent", "failed", "rejected", "undone"}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", required=True)
    ap.add_argument("--app", default="call-sidekick")
    args = ap.parse_args()

    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient(profile=args.profile)
    base = w.apps.get(args.app).url.rstrip("/")
    c = httpx.Client(base_url=base, headers=w.config.authenticate(), timeout=90)
    checks: dict[str, bool] = {}
    notes: dict[str, str] = {}

    vs = c.post("/api/voice/sessions", json={"role": "assistant", "playbook": "advisor_coach"})
    vs.raise_for_status()
    sid = vs.json()["session_id"]
    try:
        listing = c.get("/api/actions", params={"session_id": sid}).json()
        safety = listing["safety"]
        notes["safety"] = str(safety)
        safe = safety.get("safe_recipient") or ""
        checks["safety lock is on"] = bool(safety.get("lock")) and bool(safe)

        def create(kind: str, params: dict) -> httpx.Response:
            return c.post("/api/actions", json={"session_id": sid, "kind": kind, "params": params})

        def approve(action_id: str) -> dict:
            r = c.post(f"/api/actions/{action_id}/approve")
            r.raise_for_status()
            return r.json()

        # Email: approved, rendered, recipients rewritten.
        email = create("send_email", {"to": ["sam.client@example.com"], "cc": ["priya.client@example.com"],
                                      "subject": "Follow-up from today's review",
                                      "body_text": "Thanks for your time today. Attached is the fee summary."})
        checks["email proposal created (201)"] = email.status_code == 201
        done = approve(email.json()["id"] if "id" in email.json() else email.json()["action_id"])
        aid = done.get("action_id") or done.get("id")
        notes["email status"] = done.get("status", "?")
        checks["approved email reached a final state"] = done.get("status") in TERMINAL - {"failed", "rejected"}
        eml = c.get(f"/api/actions/{aid}/message.eml")
        text = eml.text
        checks[".eml downloads"] = eml.status_code == 200 and "Subject:" in text
        checks["safety lock rewrote email recipients"] = safe in text and "example.com" not in _headers(text, ("To", "Cc"))

        # Meeting: .ics with the day-before reminder and a 10-minute alert, attendee rewritten.
        start = (datetime.now(timezone.utc) + timedelta(days=3)).replace(hour=15, minute=0, second=0, microsecond=0)
        meet = create("create_meeting", {"title": "Portfolio review", "attendees": ["sam.client@example.com"],
                                         "start_iso": start.isoformat(), "duration_min": 30})
        checks["meeting proposal created (201)"] = meet.status_code == 201
        mdone = approve(meet.json().get("action_id") or meet.json()["id"])
        mid = mdone.get("action_id") or mdone.get("id")
        notes["meeting status"] = mdone.get("status", "?")
        ics = c.get(f"/api/actions/{mid}/invite.ics").text
        checks[".ics has VEVENT"] = "BEGIN:VEVENT" in ics and "BEGIN:VCALENDAR" in ics
        checks[".ics reminds 1 day before"] = "TRIGGER:-P1D" in ics
        checks[".ics alerts 10 min before"] = "TRIGGER:-PT10M" in ics
        attendees = [ln for ln in ics.replace("\r\n ", "").splitlines() if ln.startswith("ATTENDEE")]
        checks["safety lock rewrote attendees"] = bool(attendees) and all(safe in a and "example.com" not in a
                                                                          for a in attendees)

        # Enroll, then undo.
        enr = create("enroll", {"email": "sam.client@example.com", "program": "Retirement income review"})
        notes["enroll create"] = f"{enr.status_code} {enr.text[:160]}"
        if enr.status_code == 201:
            edone = approve(enr.json().get("action_id") or enr.json()["id"])
            eid = edone.get("action_id") or edone.get("id")
            notes["enroll status"] = edone.get("status", "?")
            undo = c.post(f"/api/actions/{eid}/undo")
            notes["undo"] = f"{undo.status_code} {undo.json().get('status') if undo.status_code == 200 else undo.text[:160]}"
            checks["enroll can be undone"] = undo.status_code == 200 and undo.json().get("status") == "undone"
        else:
            checks["enroll can be undone"] = False

        # Reject, and validation.
        rej = create("send_email", {"to": ["x@example.com"], "subject": "Draft", "body_text": "Not this one."})
        rid = rej.json().get("action_id") or rej.json()["id"]
        r = c.post(f"/api/actions/{rid}/reject")
        checks["reject works"] = r.status_code == 200 and r.json().get("status") == "rejected"
        bad = create("send_email", {"to": ["not-an-address"], "subject": "x", "body_text": "y"})
        checks["bad params refused (4xx)"] = 400 <= bad.status_code < 500
        bad_prog = create("enroll", {"email": "sam.client@example.com", "program": "Made-up program"})
        checks["unknown program refused (4xx)"] = 400 <= bad_prog.status_code < 500
        final = c.get("/api/actions", params={"session_id": sid}).json()["actions"]
        notes["actions in session"] = ", ".join(f"{a.get('kind')}={a.get('status')}" for a in final)
    finally:
        c.post(f"/api/voice/sessions/{sid}/end")

    width = max(len(k) for k in checks)
    for k, ok in checks.items():
        print(f"{'PASS' if ok else 'FAIL'}  {k.ljust(width)}")
    for k, v in notes.items():
        print(f"      {k}: {v}")
    sys.exit(0 if checks and all(checks.values()) else 1)


def _headers(eml: str, names: tuple[str, ...]) -> str:
    head = eml.split("\r\n\r\n", 1)[0].split("\n\n", 1)[0]
    return "\n".join(ln for ln in head.splitlines() if ln.split(":", 1)[0] in names)


if __name__ == "__main__":
    main()
