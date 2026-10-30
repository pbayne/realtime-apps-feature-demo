"""Zerobus history contract (SPEC section 3.7). One record = one JSON object = one row.

Every table is a UC managed Delta table in ${HISTORY_CATALOG}.${HISTORY_SCHEMA}, created by
src/notebooks/init_history.py. The column names and order here must match that notebook's
TABLE_DDL (tests/app/test_history.py checks it). Types are restricted to STRING, INT, BIGINT,
DOUBLE and BOOLEAN, the types the Zerobus docs name. Epoch times are BIGINT milliseconds.
JSON-valued columns are STRING holding JSON, same convention as the Lakebase tables.
"""

from __future__ import annotations

import json
from typing import Any, Mapping

# table -> [(column, sql_type)]
TABLES: dict[str, list[tuple[str, str]]] = {
    "calls": [
        ("call_id", "STRING"), ("playbook", "STRING"), ("started_at", "BIGINT"), ("ended_at", "BIGINT"),
        ("customer_id", "STRING"), ("status", "STRING"), ("mode", "STRING"), ("script", "STRING"),
        ("owner", "STRING"),
    ],
    "utterances": [
        ("call_id", "STRING"), ("seq", "INT"), ("speaker", "STRING"), ("text", "STRING"),
        ("t_start_ms", "BIGINT"), ("t_end_ms", "BIGINT"), ("emitted_at_ms", "BIGINT"), ("playbook", "STRING"),
    ],
    "enrichments": [
        ("call_id", "STRING"), ("seq", "INT"), ("speaker", "STRING"), ("text", "STRING"), ("intent", "STRING"),
        ("sentiment", "DOUBLE"), ("risk_flags", "STRING"), ("objection", "STRING"), ("entities", "STRING"),
        ("methodology_hits", "STRING"), ("emitted_at_ms", "BIGINT"), ("enriched_at_ms", "BIGINT"),
        ("latency_ms", "BIGINT"), ("enrichment_source", "STRING"),
    ],
    "cards": [
        ("card_id", "STRING"), ("call_id", "STRING"), ("seq", "INT"), ("kind", "STRING"), ("title", "STRING"),
        ("body", "STRING"), ("confidence", "DOUBLE"), ("source_doc", "STRING"), ("source_clause", "STRING"),
        ("trigger", "STRING"), ("status", "STRING"), ("created_at_ms", "BIGINT"), ("trace", "STRING"),
    ],
    "feedback": [
        ("card_id", "STRING"), ("call_id", "STRING"), ("action", "STRING"), ("edited_text", "STRING"),
        ("at_ms", "BIGINT"),
    ],
    "voice_turns": [
        ("session_id", "STRING"), ("turn", "INT"), ("role", "STRING"), ("user_text", "STRING"),
        ("agent_text", "STRING"), ("interrupted", "BOOLEAN"), ("timing", "STRING"), ("created_at_ms", "BIGINT"),
    ],
    "voice_items": [
        ("item_id", "STRING"), ("session_id", "STRING"), ("turn", "INT"), ("kind", "STRING"), ("text", "STRING"),
        ("owner", "STRING"), ("due", "STRING"), ("source_doc", "STRING"), ("created_at_ms", "BIGINT"),
    ],
    # Actions (SPEC 12). One row per status change; reconcile keeps the latest per action_id.
    "actions": [
        ("action_id", "STRING"), ("session_id", "STRING"), ("call_id", "STRING"), ("playbook", "STRING"),
        ("owner", "STRING"), ("kind", "STRING"), ("summary", "STRING"), ("params", "STRING"), ("status", "STRING"),
        ("proposed_by", "STRING"), ("created_at_ms", "BIGINT"), ("updated_at_ms", "BIGINT"),
        ("decided_at_ms", "BIGINT"), ("decided_by", "STRING"), ("result", "STRING"), ("error", "STRING"),
    ],
    "contacts": [
        ("contact_id", "STRING"), ("owner", "STRING"), ("first_name", "STRING"), ("last_name", "STRING"),
        ("email", "STRING"), ("phone", "STRING"), ("company", "STRING"), ("updated_at_ms", "BIGINT"),
    ],
    "enrollments": [
        ("enrollment_id", "STRING"), ("contact_id", "STRING"), ("program", "STRING"), ("owner", "STRING"),
        ("note", "STRING"), ("created_at_ms", "BIGINT"),
    ],
}


def column_names(table: str) -> list[str]:
    return [name for name, _ in TABLES[table]]


def _coerce(sql_type: str, value: Any) -> Any:
    if value is None:
        return None
    if sql_type == "STRING":
        return value if isinstance(value, str) else json.dumps(value, default=str)
    if sql_type in ("BIGINT", "INT"):
        return int(value)
    if sql_type == "DOUBLE":
        return float(value)
    if sql_type == "BOOLEAN":
        return bool(value)
    raise ValueError(f"unsupported sql type {sql_type}")


def to_record(table: str, row: Mapping[str, Any]) -> dict[str, Any]:
    """Project a row onto the table's columns (missing -> null, extras dropped) and coerce types.
    Raises KeyError for an unknown table."""
    return {name: _coerce(typ, row.get(name)) for name, typ in TABLES[table]}


# ---------------------------------------------------------------- row builders
# Each takes the app's wire objects and returns a dict keyed by the table's columns.

def call_row(record: Any) -> dict[str, Any]:
    return {
        "call_id": record.call_id, "playbook": record.playbook, "started_at": record.started_at_ms,
        "ended_at": record.ended_at_ms, "customer_id": record.customer_id, "status": record.status,
        "mode": record.mode, "script": record.script, "owner": getattr(record, "owner", None),
    }


def utterance_row(utt: Any) -> dict[str, Any]:
    return {
        "call_id": utt.call_id, "seq": utt.seq, "speaker": utt.speaker, "text": utt.text,
        "t_start_ms": utt.t_start_ms, "t_end_ms": utt.t_end_ms, "emitted_at_ms": utt.emitted_at_ms,
        "playbook": utt.playbook,
    }


def enrichment_row(call_id: str, seq: int, speaker: str, text: str, fields: Mapping[str, Any],
                   emitted_at_ms: int, enriched_at_ms: int, source: str) -> dict[str, Any]:
    return {
        "call_id": call_id, "seq": seq, "speaker": speaker, "text": text,
        "intent": fields.get("intent"), "sentiment": fields.get("sentiment"),
        "risk_flags": list(fields.get("risk_flags") or []), "objection": fields.get("objection"),
        "entities": fields.get("entities") or {}, "methodology_hits": list(fields.get("methodology_hits") or []),
        "emitted_at_ms": emitted_at_ms, "enriched_at_ms": enriched_at_ms,
        "latency_ms": enriched_at_ms - emitted_at_ms, "enrichment_source": source,
    }


def card_row(card: Any, trace: Mapping[str, Any]) -> dict[str, Any]:
    wire = card.to_wire()
    return {**wire, "trace": dict(trace)}


def feedback_row(card_id: str, call_id: str, action: str, edited_text: str | None, at_ms: int) -> dict[str, Any]:
    return {"card_id": card_id, "call_id": call_id, "action": action, "edited_text": edited_text, "at_ms": at_ms}


def voice_turn_row(session_id: str, row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "session_id": session_id, "turn": row.get("turn"), "role": row.get("role"),
        "user_text": row.get("user_text"), "agent_text": row.get("agent_text"),
        "interrupted": bool(row.get("interrupted")), "timing": dict(row.get("timing") or {}),
        "created_at_ms": row.get("created_at_ms"),
    }


def action_row(action: Mapping[str, Any]) -> dict[str, Any]:
    """JSON-valued params and result are STRING columns, same convention as the Lakebase tables."""
    out = {name: action.get(name) for name, _ in TABLES["actions"]}
    for key in ("params", "result"):
        value = action.get(key)
        out[key] = None if value is None else json.dumps(value, default=str, sort_keys=True)
    return out


def contact_row(contact: Mapping[str, Any]) -> dict[str, Any]:
    return {name: contact.get(name) for name, _ in TABLES["contacts"]}


def enrollment_row(enrollment: Mapping[str, Any]) -> dict[str, Any]:
    return {name: enrollment.get(name) for name, _ in TABLES["enrollments"]}


def voice_item_row(session_id: str, item: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "item_id": item.get("item_id"), "session_id": session_id, "turn": item.get("turn"),
        "kind": item.get("kind"), "text": item.get("text"), "owner": item.get("owner"), "due": item.get("due"),
        "source_doc": item.get("source_doc"), "created_at_ms": item.get("created_at_ms"),
    }
