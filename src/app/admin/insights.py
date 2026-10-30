"""Admin insights over the UC Delta history (SPEC 13).

Reads realtime_call_sidekick_catalog.call_sidekick.* through the SQL Statement Execution API on the SQL warehouse
named by DATABRICKS_WAREHOUSE_ID (app resource `sql-warehouse`, app service principal auth).

Rules:
- Every value that comes from a request is a bound parameter (`:name`). Only the catalog and schema names are put
  into the text, and they are checked against a strict identifier pattern at construction.
- `days` is validated by the route (1..90) and turned into a cutoff timestamp here, as a parameter.
- Unconfigured (no warehouse) or failing: the page gets an empty result with a reason, never a 500.
- Results are cached per process for insights_cache_s seconds.
"""

from __future__ import annotations

import asyncio
import re
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Optional

from models import now_ms

IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")
DAY_MS = 86_400_000
MAX_DAYS = 90

def _queries() -> dict[str, str]:
    """Named statements. {t_<table>} is replaced by `catalog.schema.<table>` (identifiers only)."""
    return {
        "calls_daily": """
            SELECT date_format(from_unixtime(started_at / 1000), 'yyyy-MM-dd') AS day,
                   COUNT(*) AS calls,
                   AVG((ended_at - started_at) / 60000.0) AS avg_minutes
            FROM {t_calls}
            WHERE started_at >= :cutoff_ms AND ended_at IS NOT NULL
            GROUP BY 1 ORDER BY 1""",
        "voice_daily": """
            SELECT date_format(from_unixtime(created_at_ms / 1000), 'yyyy-MM-dd') AS day,
                   COUNT(DISTINCT session_id) AS voice_sessions
            FROM {t_voice_turns}
            WHERE created_at_ms >= :cutoff_ms
            GROUP BY 1 ORDER BY 1""",
        "sentiment_daily": """
            SELECT date_format(from_unixtime(enriched_at_ms / 1000), 'yyyy-MM-dd') AS day,
                   AVG(sentiment) AS avg_sentiment,
                   COUNT(*) AS customer_turns,
                   SUM(CASE WHEN sentiment < -0.2 THEN 1 ELSE 0 END) AS negative_turns
            FROM {t_enrichments}
            WHERE speaker = 'customer' AND enriched_at_ms >= :cutoff_ms
            GROUP BY 1 ORDER BY 1""",
        "top_objections": """
            SELECT objection AS label, COUNT(*) AS n
            FROM {t_enrichments}
            WHERE objection IS NOT NULL AND objection <> '' AND enriched_at_ms >= :cutoff_ms
            GROUP BY 1 ORDER BY n DESC LIMIT 10""",
        "top_intents": """
            SELECT intent AS label, COUNT(*) AS n
            FROM {t_enrichments}
            WHERE intent IS NOT NULL AND intent <> '' AND enriched_at_ms >= :cutoff_ms
            GROUP BY 1 ORDER BY n DESC LIMIT 10""",
        "top_risk_flags": """
            SELECT flag AS label, COUNT(*) AS n
            FROM {t_enrichments} e
            LATERAL VIEW explode(from_json(e.risk_flags, 'array<string>')) x AS flag
            WHERE e.enriched_at_ms >= :cutoff_ms
            GROUP BY flag ORDER BY n DESC LIMIT 10""",
        "card_outcomes": """
            SELECT kind, status, COUNT(*) AS n
            FROM {t_cards}
            WHERE created_at_ms >= :cutoff_ms
            GROUP BY kind, status ORDER BY kind, status""",
        "latency": """
            SELECT percentile_approx(latency_ms, 0.5) AS p50_ms,
                   percentile_approx(latency_ms, 0.9) AS p90_ms,
                   COUNT(*) AS n
            FROM {t_enrichments}
            WHERE latency_ms IS NOT NULL AND enriched_at_ms >= :cutoff_ms""",
        "methodology": """
            SELECT AVG(size(from_json(methodology_hits, 'array<string>'))) AS avg_items_hit,
                   COUNT(*) AS customer_turns
            FROM {t_enrichments}
            WHERE speaker = 'customer' AND enriched_at_ms >= :cutoff_ms""",
        "recent_calls": """
            SELECT call_id, playbook, mode, started_at, ended_at, status
            FROM {t_calls}
            WHERE started_at >= :cutoff_ms
            ORDER BY started_at DESC LIMIT 25""",
        "talk_tracks": """
            SELECT kind, playbook, label, detail, occurrences, evidence_calls, generated_at
            FROM {t_talk_track_insights}
            WHERE run_id = (SELECT MAX(run_id) FROM {t_talk_track_insights})
            ORDER BY occurrences DESC LIMIT 50""",
        "call_utterances": """
            SELECT seq, speaker, text, emitted_at_ms
            FROM {t_utterances} WHERE call_id = :call_id ORDER BY seq""",
        "call_cards": """
            SELECT card_id, kind, title, body, status, created_at_ms
            FROM {t_cards} WHERE call_id = :call_id ORDER BY created_at_ms""",
        "call_summary": """
            SELECT summary, overall_score, parse_ok, scored_at
            FROM {t_gold_call_scores} WHERE call_id = :call_id""",
    }


PARAM_TYPES = {"cutoff_ms": "BIGINT", "call_id": "STRING"}

# Executor: (statement, parameters {name: value}, types {name: sql type}) -> rows as dicts.
Executor = Callable[[str, dict[str, Any], dict[str, str]], Awaitable[list[dict[str, Any]]]]


class InsightsError(Exception):
    pass


@dataclass
class Insights:
    catalog: str
    schema: str
    warehouse_id: Optional[str]
    cache_s: float = 60.0
    executor: Optional[Executor] = None
    _cache: dict[tuple[Any, ...], tuple[float, Any]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not (IDENT.match(self.catalog) and IDENT.match(self.schema)):
            raise ValueError("catalog and schema must be plain identifiers")
        self._templates = _queries()

    @property
    def configured(self) -> bool:
        return bool(self.warehouse_id)

    def statement(self, name: str) -> str:
        """The SQL text for a named query, with table identifiers filled in from the validated catalog/schema."""
        tmpl = self._templates[name]
        fill = {f"t_{t}": f"{self.catalog}.{self.schema}.{t}" for t in TABLES}
        out = tmpl
        for key, full in fill.items():
            out = out.replace("{" + key + "}", full)
        if re.search(r"\{t_\w+\}", out):
            raise ValueError(f"unfilled table placeholder in {name}")
        return out

    async def _run(self, name: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        if self.executor is None:
            raise InsightsError("no executor")
        types = {k: PARAM_TYPES[k] for k in params}
        return await self.executor(self.statement(name), params, types)

    async def _cached(self, key: tuple[Any, ...], load: Callable[[], Awaitable[Any]]) -> Any:
        hit = self._cache.get(key)
        if hit and time.monotonic() - hit[0] < self.cache_s:
            return hit[1]
        value = await load()
        self._cache[key] = (time.monotonic(), value)
        return value

    async def overview(self, days: int) -> dict[str, Any]:
        """Everything the dashboard shows for the last `days`. Each section fails on its own."""
        if not self.configured:
            return {"configured": False, "reason": "No SQL warehouse is configured for this app (DATABRICKS_WAREHOUSE_ID).",
                    "days": days, "as_of_ms": now_ms(), "sections": {}}
        return await self._cached(("overview", days), lambda: self._overview(days))

    async def _overview(self, days: int) -> dict[str, Any]:
        params = {"cutoff_ms": now_ms() - days * DAY_MS}
        names = ["calls_daily", "voice_daily", "sentiment_daily", "top_objections", "top_intents",
                 "top_risk_flags", "card_outcomes", "latency", "methodology", "recent_calls", "talk_tracks"]
        sections: dict[str, Any] = {}
        errors: dict[str, str] = {}
        for name in names:
            try:
                sections[name] = await self._run(name, params)
            except Exception as exc:  # noqa: BLE001 - one section must not blank the page
                sections[name] = []
                errors[name] = str(exc)[:200]
        return {"configured": True, "days": days, "as_of_ms": now_ms(), "sections": sections, "errors": errors}

    async def call_detail(self, call_id: str) -> dict[str, Any]:
        if not self.configured:
            return {"configured": False, "utterances": [], "cards": [], "summary": None}

        async def load() -> dict[str, Any]:
            params = {"call_id": call_id}
            utts = await self._run("call_utterances", params)
            cards = await self._run("call_cards", params)
            try:
                summary = (await self._run("call_summary", params) or [None])[0]
            except Exception:  # noqa: BLE001 - the gold table exists only after the scoring job has run
                summary = None
            return {"configured": True, "utterances": utts, "cards": cards, "summary": summary}

        return await self._cached(("call", call_id), load)


TABLES = ("calls", "utterances", "enrichments", "cards", "voice_turns", "gold_call_scores", "talk_track_insights")


def make_sdk_executor(warehouse_id: str) -> Executor:
    """Executor over the Statement Execution API. Auth: the app's service principal (WorkspaceClient defaults)."""
    state: dict[str, Any] = {}

    def run_sync(statement: str, params: dict[str, Any], types: dict[str, str]) -> list[dict[str, Any]]:
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service.sql import StatementParameterListItem, StatementState

        if "w" not in state:
            state["w"] = WorkspaceClient()
        w = state["w"]
        resp = w.statement_execution.execute_statement(
            statement=statement,
            warehouse_id=warehouse_id,
            parameters=[StatementParameterListItem(name=k, value=str(v), type=types[k]) for k, v in params.items()],
            wait_timeout="30s",
        )
        if resp.status is None or resp.status.state != StatementState.SUCCEEDED:
            msg = resp.status.error.message if resp.status and resp.status.error else "statement did not succeed"
            raise InsightsError(msg)
        cols = [c.name for c in (resp.manifest.schema.columns or [])]
        data = resp.result.data_array if resp.result and resp.result.data_array else []
        return [dict(zip(cols, row)) for row in data]

    async def run(statement: str, params: dict[str, Any], types: dict[str, str]) -> list[dict[str, Any]]:
        return await asyncio.to_thread(run_sync, statement, params, types)

    return run
