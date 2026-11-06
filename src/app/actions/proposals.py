"""Action proposals from the model (SPEC 12). Proposals are data: they become `proposed` actions, never executed here.

Voice mode: the `<<ACTIONS>>` JSON array that follows `<<ITEMS>>` in the same LLM call (parse_proposals).
Call mode: a fast-model pass on customer finals (propose_for_call), rate-limited by the call session.
Both parsers are tolerant (a bad array gives no proposals and never raises) and validate every entry with the
same schemas a human form uses. Dedupe is per session, on the kind and its key params.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Optional

from models import ACTION_KINDS

from actions.schemas import ActionValidationError, contact_key, validate_params

log = logging.getLogger("sidekick.actions.proposals")

MAX_PROPOSALS = 5
ACTIONS_HINT = "send_email | create_meeting | update_contact | enroll | update_record"

Proposal = tuple[str, dict[str, Any]]    # (kind, validated params)


def parse_proposals(raw: str, *, program_names: tuple[str, ...] = ()) -> list[Proposal]:
    """Outermost JSON array (or {"actions": [...]}) of {"kind", "params"} objects. Invalid entries are dropped
    one by one. Never raises. `program_names` restricts enroll.program when the playbook defines programs."""
    if not raw or not raw.strip():
        return []
    start, end = raw.find("["), raw.rfind("]")
    data: Any
    if start == -1 or end <= start:
        obj = _parse_json(raw)
        data = obj.get("actions") if isinstance(obj, dict) else None
    else:
        data = _parse_json(raw[start : end + 1])
    if not isinstance(data, list):
        return []
    out: list[Proposal] = []
    for entry in data:
        if not isinstance(entry, dict) or entry.get("kind") not in ACTION_KINDS:
            continue
        kind = entry["kind"]
        params = entry.get("params")
        try:
            clean = validate_params(kind, params)
        except ActionValidationError as exc:
            log.info("dropped invalid %s proposal: %s", kind, exc)
            continue
        if kind == "enroll" and program_names and clean["program"] not in program_names:
            log.info("dropped enroll proposal for unknown program %r", clean["program"])
            continue
        out.append((kind, clean))
        if len(out) >= MAX_PROPOSALS:
            break
    return out


def dedupe_key(kind: str, params: dict[str, Any]) -> str:
    """Two proposals with the same key are the same action (per session). Stable across case and order."""
    if kind == "send_email":
        parts = [sorted(a.lower() for a in params["to"]), sorted(a.lower() for a in params.get("cc", [])),
                 params["subject"].strip().lower()]
    elif kind == "create_meeting":
        parts = [params["title"].strip().lower(), params["start_iso"], sorted(a.lower() for a in params["attendees"])]
    elif kind == "update_contact":
        parts = [contact_key(params), sorted((k, str(v).lower()) for k, v in params["fields"].items())]
    elif kind == "enroll":
        parts = [contact_key(params), params["program"].strip().lower()]
    elif kind == "update_record":
        parts = [params["account_id"].lower(), sorted((k, str(v).lower()) for k, v in params["fields"].items())]
    else:
        parts = [json.dumps(params, sort_keys=True)]
    return kind + "|" + json.dumps(parts, sort_keys=True, default=str)


def call_messages(playbook_title: str, programs: tuple[str, ...], customer_text: str,
                  recent: list[str]) -> list[dict[str, str]]:
    """Prompt for the call-mode proposal pass: JSON only, empty unless the customer asked for something."""
    system = "\n".join([
        f"You watch a live {playbook_title.lower()} call for the human agent. The customer just said something.",
        "Propose an action only when the customer explicitly asks for it, or a commitment is clearly agreed.",
        f"Allowed kinds: {ACTIONS_HINT}.",
        "Reply with ONLY a JSON array of {\"kind\": ..., \"params\": {...}} objects, or [] when nothing applies.",
        "Never invent an email address; leave the action out if no address was given.",
        ("Enrollment programs: " + ", ".join(programs)) if programs else "No enrollment programs are defined.",
    ])
    user = "\n".join(["Earlier in the call:", *recent[-4:], "Customer just said:", customer_text])
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


async def propose_for_call(llm: Any, model: str, *, playbook_title: str, programs: tuple[str, ...],
                           customer_text: str, recent: list[str], timeout_s: float,
                           max_tokens: int = 300) -> list[Proposal]:
    """One fast-model pass. Fail-soft: any error or timeout gives no proposals."""
    messages = call_messages(playbook_title, programs, customer_text, recent)
    try:
        raw = await asyncio.wait_for(llm.chat(model, messages, max_tokens=max_tokens, timeout=timeout_s),
                                     timeout=timeout_s)
    except Exception:  # noqa: BLE001 - a proposal pass must never affect the call
        log.info("call action proposal pass unavailable")
        return []
    return parse_proposals(raw, program_names=programs)


def _parse_json(text: str) -> Optional[Any]:
    try:
        return json.loads(text)
    except (json.JSONDecodeError, ValueError):
        return None
