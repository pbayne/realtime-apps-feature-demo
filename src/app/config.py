"""Settings for the copilot app (pydantic-settings; env vars, optional .env).

MODE=local swaps Kafka -> in-memory bus and Lakebase -> in-memory store.
Every other knob has a SPEC-sourced default and can be overridden by env.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Literal, Optional

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

APP_DIR = Path(__file__).resolve().parent            # src/app (bundled into the Databricks App)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", case_sensitive=False)

    mode: Literal["local", "databricks"] = "local"
    # auto = memory in local, kafka/lakebase in databricks. Explicit values override.
    bus_backend: Literal["auto", "memory", "kafka"] = "auto"
    store_backend: Literal["auto", "memory", "lakebase"] = "auto"

    # --- models (SPEC section 6) ---
    llm_enabled: bool = True
    fast_model: str = "databricks-claude-haiku-5-5"
    smart_model: str = "databricks-claude-sonnet-5-5"
    embed_model: str = "databricks-gte-large-en"
    stt_endpoint: str = "parakeet-live"
    stt_endpoint_url: Optional[str] = None  # route-optimized data-plane URL (register_parakeet.py prints it)
    llm_timeout_s: float = 8.0
    llm_failure_threshold: int = 3          # consecutive failures before the breaker opens
    llm_breaker_cooldown_s: float = 30.0

    # --- retrieval ---
    retrieval_backend: Literal["auto", "memory", "keyword", "vector_search"] = "auto"
    retrieval_top_k: int = 3
    # Calibrated on e2-demo databricks-gte-large-en over the shipped KB: relevant top-1 cosine
    # ~0.60-0.77, unrelated ~0.45-0.53. Keyword (idf overlap) is a noisier offline fallback.
    retrieval_min_score_embed: float = 0.58
    retrieval_min_score_keyword: float = 0.25
    vs_index_template: str = "main.call_copilot.kb_{playbook}"  # Vector Search index, databricks mode

    # --- decision policy (SPEC section 4) ---
    # Perception (one haiku call) runs concurrently and never gates the decision. Its budget only
    # bounds how late a LATE candidate may arrive. Retrieval gets a budget before keyword fallback.
    perception_budget_s: float = 2.5
    retrieval_budget_s: float = 0.8
    rtm_wait_ms: int = 300
    debounce_ms: int = 300
    card_min_confidence: float = 0.6
    card_cooldown_turns: int = 3
    card_budget: int = 12
    compose_max_words: int = 35
    title_max_words: int = 8

    # --- STT (SPEC section 1) ---
    stt_frame_s: float = 0.4
    stt_window_s: float = 12.0
    stt_hold_words: int = 1
    stt_endpoint_silence_ms: int = 600
    stt_decode_lag_ms: int = 250            # replay: decoder sees audio this far behind real time
    vad_rms_threshold: float = 400.0        # energy-VAD fallback on PCM16
    vad_backend: Literal["auto", "silero", "energy"] = "auto"

    # --- replay ---
    replay_speed: float = 1.0

    # --- enrichment / data files ---
    enrich_rules_path: Optional[Path] = None
    playbooks_dir: Path = APP_DIR / "playbooks"
    replay_dir: Path = APP_DIR / "data" / "replay"

    # --- Kafka (databricks mode) ---
    kafka_bootstrap_servers: Optional[str] = None
    kafka_security_protocol: str = "SASL_SSL"
    kafka_sasl_mechanism: str = "PLAIN"
    kafka_username: Optional[str] = None
    kafka_password: Optional[str] = None
    kafka_utterances_topic: str = "utterances"
    kafka_cards_topic: str = "cards"

    # --- Lakebase (databricks mode) ---
    lakebase_host: Optional[str] = None
    lakebase_port: int = 5432
    lakebase_db: str = "databricks_postgres"
    lakebase_user: Optional[str] = None
    lakebase_password: Optional[str] = None
    # Lakebase Autoscaling endpoint resource path, e.g. projects/p/branches/b/endpoints/e.
    # When set and no password is given, a short-lived credential is generated via the SDK.
    lakebase_endpoint: Optional[str] = None
    lakebase_sslmode: str = "require"       # asyncpg ssl arg; "disable" only for local Postgres tests
    lakebase_pool_min: int = 1
    lakebase_pool_max: int = 6

    @field_validator("stt_frame_s")
    @classmethod
    def _frame_floor(cls, v: float) -> float:
        if v < 0.3:
            raise ValueError("stt_frame_s must be >= 0.3 (SPEC section 1)")
        return v

    @property
    def effective_bus(self) -> str:
        if self.bus_backend != "auto":
            return self.bus_backend
        return "memory" if self.mode == "local" else "kafka"

    @property
    def effective_store(self) -> str:
        if self.store_backend != "auto":
            return self.store_backend
        return "memory" if self.mode == "local" else "lakebase"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
