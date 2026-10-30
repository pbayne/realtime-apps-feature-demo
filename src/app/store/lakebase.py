"""Lakebase (Postgres) store: asyncpg pool + LISTEN enrichment_ready.

Tables follow SPEC section 3. The RTM job owns `enrichments` writes and the NOTIFY
trigger (created by src/notebooks/init.py). This store still creates the tables
IF NOT EXISTS so a fresh local Postgres works, and it does not create triggers.

Enrichment waits: a NOTIFY (payload `call_id:seq`) resolves waiters immediately.
Because the trigger may be missing or a notification may be lost, waiters also poll
the table every 100 ms until their deadline. Either path is fail-soft: a None
return makes the caller fall back to the local rules transform.

Auth: LAKEBASE_PASSWORD when set (Apps secret binding), otherwise a short-lived
credential from the SDK for LAKEBASE_ENDPOINT (diva/db.py pattern).
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, Optional

from config import Settings
from models import CallRecord, Card, Utterance, now_ms

log = logging.getLogger("sidekick.store.lakebase")

DDL = """
CREATE TABLE IF NOT EXISTS calls (
  call_id TEXT PRIMARY KEY, playbook TEXT NOT NULL, started_at BIGINT NOT NULL,
  ended_at BIGINT, customer_id TEXT, status TEXT NOT NULL DEFAULT 'active', mode TEXT, script TEXT);
CREATE TABLE IF NOT EXISTS utterances (
  call_id TEXT NOT NULL, seq INT NOT NULL, speaker TEXT NOT NULL, text TEXT NOT NULL,
  t_start_ms BIGINT, t_end_ms BIGINT, emitted_at_ms BIGINT, playbook TEXT,
  PRIMARY KEY (call_id, seq));
CREATE TABLE IF NOT EXISTS enrichments (
  call_id TEXT NOT NULL, seq INTEGER NOT NULL, speaker TEXT, text TEXT, intent TEXT,
  sentiment DOUBLE PRECISION, risk_flags TEXT, objection TEXT, entities TEXT,
  methodology_hits TEXT, emitted_at_ms BIGINT, enriched_at_ms BIGINT, latency_ms BIGINT,
  PRIMARY KEY (call_id, seq));
CREATE TABLE IF NOT EXISTS cards (
  card_id TEXT PRIMARY KEY, call_id TEXT NOT NULL, seq INT, kind TEXT, title TEXT, body TEXT,
  confidence DOUBLE PRECISION, source_doc TEXT, source_clause TEXT, trigger TEXT,
  status TEXT, created_at_ms BIGINT, trace TEXT);
CREATE TABLE IF NOT EXISTS feedback (
  card_id TEXT NOT NULL, call_id TEXT NOT NULL, action TEXT NOT NULL, edited_text TEXT, at_ms BIGINT);
CREATE TABLE IF NOT EXISTS voice_turns (
  session_id TEXT NOT NULL, turn INTEGER NOT NULL, role TEXT, user_text TEXT, agent_text TEXT,
  interrupted BOOLEAN NOT NULL DEFAULT FALSE, timing TEXT, created_at_ms BIGINT,
  PRIMARY KEY (session_id, turn));
CREATE TABLE IF NOT EXISTS voice_items (
  item_id TEXT PRIMARY KEY, session_id TEXT NOT NULL, turn INTEGER, kind TEXT, text TEXT,
  owner TEXT, due TEXT, source_doc TEXT, created_at_ms BIGINT);
"""


def _j(v: Any) -> Optional[str]:
    return None if v is None else json.dumps(v)


class LakebaseStore:
    def __init__(self, settings: Settings) -> None:
        self._s = settings
        self._pool: Any = None
        self._listen_conn: Any = None
        self._cache: dict[tuple[str, int], dict[str, Any]] = {}
        self._waiters: dict[tuple[str, int], list[asyncio.Future[None]]] = {}

    # ------------------------------------------------------------ connection
    def _credentials(self) -> dict[str, Any]:
        s = self._s
        password = s.lakebase_password
        user = s.lakebase_user
        host = s.lakebase_host
        if not password and s.lakebase_endpoint:
            from databricks.sdk import WorkspaceClient

            w = WorkspaceClient()
            ep = s.lakebase_endpoint
            host = host or w.postgres.get_endpoint(ep).as_dict()["status"]["hosts"]["host"]
            cred = w.postgres.generate_database_credential(ep)
            password = getattr(cred, "token", None) or cred.as_dict().get("token")
            user = user or w.current_user.me().user_name
        if not (host and user and password):
            raise RuntimeError("Lakebase not configured: need LAKEBASE_HOST/USER/PASSWORD or LAKEBASE_ENDPOINT")
        return {"host": host, "port": s.lakebase_port, "database": s.lakebase_db, "user": user, "password": password, "ssl": s.lakebase_sslmode}

    async def start(self) -> None:
        import asyncpg

        creds = await asyncio.to_thread(self._credentials)
        self._pool = await asyncpg.create_pool(min_size=self._s.lakebase_pool_min, max_size=self._s.lakebase_pool_max, **creds)
        async with self._pool.acquire() as conn:
            await conn.execute(DDL)
        self._listen_conn = await asyncpg.connect(**creds)
        await self._listen_conn.add_listener("enrichment_ready", self._on_notify)
        log.info("Lakebase store ready at %s/%s", creds["host"], creds["database"])

    async def stop(self) -> None:
        if self._listen_conn is not None:
            await self._listen_conn.close()
            self._listen_conn = None
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    def _on_notify(self, _conn: Any, _pid: int, _channel: str, payload: str) -> None:
        try:
            call_id, seq_s = payload.rsplit(":", 1)
            key = (call_id, int(seq_s))
        except ValueError:
            return
        asyncio.ensure_future(self._fetch_into_cache(key))

    async def _fetch_into_cache(self, key: tuple[str, int]) -> Optional[dict[str, Any]]:
        row = await self._fetch_enrichment(*key)
        if row is not None:
            self._cache[key] = row
            for fut in self._waiters.pop(key, []):
                if not fut.done():
                    fut.set_result(None)
        return row

    async def _fetch_enrichment(self, call_id: str, seq: int) -> Optional[dict[str, Any]]:
        rec = await self._pool.fetchrow(
            "SELECT call_id, seq, speaker, text, intent, sentiment, risk_flags, objection, entities, "
            "methodology_hits, emitted_at_ms, enriched_at_ms, latency_ms FROM enrichments WHERE call_id=$1 AND seq=$2",
            call_id, seq,
        )
        if rec is None:
            return None
        d = dict(rec)
        for k in ("risk_flags", "entities", "methodology_hits"):
            if isinstance(d.get(k), str):
                d[k] = json.loads(d[k])
        return d

    # --------------------------------------------------------------- writes
    async def create_call(self, call: CallRecord) -> None:
        await self._pool.execute(
            "INSERT INTO calls (call_id, playbook, started_at, customer_id, status, mode, script) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (call_id) DO NOTHING",
            call.call_id, call.playbook, call.started_at_ms, call.customer_id, call.status, call.mode, call.script,
        )

    async def get_call(self, call_id: str) -> Optional[dict[str, Any]]:
        rec = await self._pool.fetchrow(
            "SELECT call_id, playbook, started_at AS started_at_ms, ended_at AS ended_at_ms, customer_id, status, mode, script "
            "FROM calls WHERE call_id=$1",
            call_id,
        )
        return dict(rec) if rec else None

    async def end_call(self, call_id: str, ended_at_ms: int) -> None:
        await self._pool.execute("UPDATE calls SET ended_at=$2, status='ended' WHERE call_id=$1", call_id, ended_at_ms)

    async def put_utterance(self, utt: Utterance) -> None:
        await self._pool.execute(
            "INSERT INTO utterances (call_id, seq, speaker, text, t_start_ms, t_end_ms, emitted_at_ms, playbook) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (call_id, seq) DO NOTHING",
            utt.call_id, utt.seq, utt.speaker, utt.text, utt.t_start_ms, utt.t_end_ms, utt.emitted_at_ms, utt.playbook,
        )

    async def put_enrichment(self, call_id: str, seq: int, row: dict[str, Any]) -> None:
        # Used by the local enricher only; in databricks mode the RTM sink owns this table.
        # JSON-valued columns are TEXT (same as src/notebooks/init.py), so they bind as strings.
        emitted, enriched = row.get("emitted_at_ms"), row.get("enriched_at_ms")
        latency = (enriched - emitted) if emitted is not None and enriched is not None else None
        await self._pool.execute(
            "INSERT INTO enrichments (call_id, seq, speaker, text, intent, sentiment, risk_flags, objection, entities, "
            "methodology_hits, emitted_at_ms, enriched_at_ms, latency_ms) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) "
            "ON CONFLICT (call_id, seq) DO UPDATE SET intent=EXCLUDED.intent, sentiment=EXCLUDED.sentiment, "
            "risk_flags=EXCLUDED.risk_flags, objection=EXCLUDED.objection, entities=EXCLUDED.entities, "
            "methodology_hits=EXCLUDED.methodology_hits, enriched_at_ms=EXCLUDED.enriched_at_ms, "
            "latency_ms=EXCLUDED.latency_ms",
            call_id, seq, row.get("speaker"), row.get("text"), row.get("intent"), row.get("sentiment"),
            _j(row.get("risk_flags", [])), row.get("objection"), _j(row.get("entities", {})),
            _j(row.get("methodology_hits", [])), emitted, enriched, latency,
        )
        self._cache[(call_id, seq)] = dict(row)

    async def get_enrichment(self, call_id: str, seq: int) -> Optional[dict[str, Any]]:
        key = (call_id, seq)
        if key in self._cache:
            return self._cache[key]
        return await self._fetch_into_cache(key)

    async def wait_enrichment(self, call_id: str, seq: int, timeout_s: float) -> Optional[dict[str, Any]]:
        key = (call_id, seq)
        deadline = time.monotonic() + max(timeout_s, 0.0)
        while True:
            if key in self._cache:
                return self._cache[key]
            if time.monotonic() >= deadline:
                return None
            fut: asyncio.Future[None] = asyncio.get_running_loop().create_future()
            self._waiters.setdefault(key, []).append(fut)
            remaining = deadline - time.monotonic()
            try:
                await asyncio.wait_for(asyncio.shield(fut), timeout=min(0.1, remaining))
            except asyncio.TimeoutError:
                # Backup path: the NOTIFY may never have fired, so read the table.
                await self._fetch_into_cache(key)
            except Exception:  # noqa: BLE001
                return None
            if key in self._cache:
                return self._cache[key]

    async def put_card(self, card: Card, trace: dict[str, Any]) -> None:
        await self._pool.execute(
            "INSERT INTO cards (card_id, call_id, seq, kind, title, body, confidence, source_doc, source_clause, trigger, "
            "status, created_at_ms, trace) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) "
            "ON CONFLICT (card_id) DO NOTHING",
            card.card_id, card.call_id, card.seq, card.kind, card.title, card.body, card.confidence,
            card.source_doc, card.source_clause, card.trigger, card.status, card.created_at_ms, _j(trace),
        )

    async def add_feedback(self, card_id: str, call_id: str, action: str, edited_text: Optional[str], at_ms: int) -> None:
        status = {"accept": "accepted", "reject": "rejected", "edit": "edited"}.get(action)
        await self._pool.execute(
            "INSERT INTO feedback (card_id, call_id, action, edited_text, at_ms) VALUES ($1,$2,$3,$4,$5)",
            card_id, call_id, action, edited_text, at_ms,
        )
        if status:
            await self._pool.execute("UPDATE cards SET status=$2 WHERE card_id=$1", card_id, status)

    async def snapshot(self, call_id: str) -> dict[str, Any]:
        call = await self.get_call(call_id)
        utts = await self._pool.fetch(
            "SELECT call_id, seq, speaker, text, t_start_ms, t_end_ms, emitted_at_ms, playbook FROM utterances "
            "WHERE call_id=$1 ORDER BY seq", call_id)
        cards = await self._pool.fetch(
            "SELECT card_id, call_id, seq, kind, title, body, confidence, source_doc, source_clause, trigger, status, "
            "created_at_ms FROM cards WHERE call_id=$1 ORDER BY created_at_ms", call_id)
        fb = await self._pool.fetch("SELECT card_id, call_id, action, edited_text, at_ms FROM feedback WHERE call_id=$1", call_id)
        enr = await self._pool.fetch(
            "SELECT call_id, seq, speaker, intent, sentiment, risk_flags, objection, methodology_hits, enriched_at_ms "
            "FROM enrichments WHERE call_id=$1 ORDER BY seq", call_id)
        enrichments = []
        for r in enr:
            d = dict(r)
            for k in ("risk_flags", "methodology_hits"):
                if isinstance(d.get(k), str):
                    d[k] = json.loads(d[k])
            enrichments.append(d)
        return {
            "call": call,
            "utterances": [dict(r) for r in utts],
            "enrichments": enrichments,
            "cards": [dict(r) for r in cards],
            "feedback": [dict(r) for r in fb],
            "as_of_ms": int(time.time() * 1000),
        }

    # ------------------------------------------------------------ voice mode
    async def put_voice_turn(self, session_id: str, row: dict[str, Any]) -> None:
        # Upserted: an interrupted turn is written once when it ends, and a repair write must not duplicate it.
        await self._pool.execute(
            "INSERT INTO voice_turns (session_id, turn, role, user_text, agent_text, interrupted, timing, created_at_ms) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (session_id, turn) DO UPDATE SET role=EXCLUDED.role, "
            "user_text=EXCLUDED.user_text, agent_text=EXCLUDED.agent_text, interrupted=EXCLUDED.interrupted, "
            "timing=EXCLUDED.timing",
            session_id, int(row["turn"]), row.get("role"), row.get("user_text"), row.get("agent_text"),
            bool(row.get("interrupted")), _j(row.get("timing") or {}), int(row.get("created_at_ms") or now_ms()),
        )

    async def put_voice_item(self, session_id: str, item: dict[str, Any]) -> None:
        await self._pool.execute(
            "INSERT INTO voice_items (item_id, session_id, turn, kind, text, owner, due, source_doc, created_at_ms) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT (item_id) DO NOTHING",
            item["item_id"], session_id, int(item["turn"]), item.get("kind"), item.get("text"), item.get("owner"),
            item.get("due"), item.get("source_doc"), int(item["created_at_ms"]),
        )

    async def voice_snapshot(self, session_id: str) -> dict[str, Any]:
        turns = await self._pool.fetch(
            "SELECT turn, role, user_text, agent_text, interrupted, timing, created_at_ms FROM voice_turns "
            "WHERE session_id=$1 ORDER BY turn", session_id)
        items = await self._pool.fetch(
            "SELECT item_id, session_id, turn, kind, text, owner, due, source_doc, created_at_ms FROM voice_items "
            "WHERE session_id=$1 ORDER BY created_at_ms, item_id", session_id)
        out_turns = []
        for r in turns:
            d = dict(r)
            if isinstance(d.get("timing"), str):
                d["timing"] = json.loads(d["timing"])
            out_turns.append(d)
        return {"turns": out_turns, "items": [dict(r) for r in items]}

