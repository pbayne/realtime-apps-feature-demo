"""Settings for the sidekick app (pydantic-settings; env vars, optional .env).

BACKBONE selects the data path (SPEC section 1 / docs/ARCHITECTURE.md):
  direct (default): no Kafka. Each final is enriched in-process; Lakebase holds live state;
                    history goes to Delta through Zerobus Ingest (HistorySink).
  kafka:            utterances go to Kafka for Spark Real-Time Mode (enrichment comes back through
                    Lakebase); history goes to Delta through the Kafka jobs.
MODE=local swaps Kafka -> in-memory bus and Lakebase -> in-memory store.
Every other knob has a SPEC-sourced default and can be overridden by env.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Literal, Optional

from pydantic import field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

APP_DIR = Path(__file__).resolve().parent            # src/app (bundled into the Databricks App)


KAFKA_SECURITY_MODES = ("SASL_SSL_PLAIN", "SASL_SSL_SCRAM_SHA_256", "SASL_SSL_SCRAM_SHA_512")

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", case_sensitive=False)

    mode: Literal["local", "databricks"] = "local"
    # SPEC section 1 / docs/ARCHITECTURE.md. direct = no Kafka (default); kafka = scale mode with RTM.
    backbone: Literal["direct", "kafka"] = "direct"
    # auto = memory in local, kafka/lakebase in databricks. Explicit values override.
    # With backbone=direct the bus is always "none" (no Kafka client is created).
    bus_backend: Literal["auto", "memory", "kafka"] = "auto"
    store_backend: Literal["auto", "memory", "lakebase"] = "auto"
    # HistorySink (SPEC section 3.7). auto: direct+databricks -> zerobus, direct+local -> memory,
    # kafka -> none (history flows over Kafka to delta_history.py). Explicit values override.
    history_backend: Literal["auto", "memory", "zerobus", "none"] = "auto"

    # --- models (SPEC section 6) ---
    llm_enabled: bool = True
    fast_model: str = "databricks-claude-haiku-5-5"
    smart_model: str = "databricks-claude-sonnet-5-5"
    embed_model: str = "databricks-gte-large-en"
    stt_endpoint: str = "sidekick-parakeet"  # Custom LLM Serving audio endpoint (register_parakeet.py)
    # parakeet = multipart /audio/transcriptions (SPEC 3.3). whisper_json = {"inputs": [base64 WAV]} on
    # /invocations, the contract of the Databricks-hosted system.ai whisper endpoint (e2-demo shared-whisper).
    # Local-dev stand-in for voice mode only: the call path's windowed decoder needs the Parakeet contract.
    stt_protocol: Literal["parakeet", "whisper_json"] = "parakeet"
    llm_timeout_s: float = 8.0
    llm_failure_threshold: int = 3          # consecutive failures before the breaker opens
    llm_breaker_cooldown_s: float = 30.0

    # --- voice mode (SPEC section 9) ---
    voice_model: str = "databricks-claude-haiku-4-5"   # measured fastest first token on e2-demo (ARCHITECTURE.md)
    voice_max_tokens: int = 400
    voice_timeout_s: float = 15.0          # per LLM stream (first token + reply)
    tts_endpoint: str = "sidekick-kokoro"  # Custom LLM Serving TTS endpoint (register_kokoro.py)
    tts_voice: str = "af_heart"
    tts_timeout_s: float = 20.0
    voice_stt_timeout_s: float = 10.0      # one whole user turn (<= 30 s of audio)
    voice_max_audio_bytes: int = 2_000_000  # hard byte cap for one WAV turn (the seconds cap below is tighter)

    # --- voice streaming, live analysis, limits, cold endpoints (SPEC 9.6, v0.5) ---
    voice_endpoint_silence_ms: int = 700     # streaming: silence that ends a user turn (300..2000 accepted per stream)
    voice_max_turn_audio_s: float = 30.0     # one user turn (WAV or stream); a stream is force-ended at this length
    voice_stream_queue_max: int = 300        # 100 ms frames buffered per stream; beyond this frames are dropped
    voice_max_session_minutes: float = 30.0  # wall-clock limit per voice socket
    voice_max_concurrent_sessions: int = 20  # voice sessions per app instance; more gets HTTP 429
    voice_out_queue_max: int = 2000          # outbound events per socket; live events are dropped beyond this
    voice_retry_base_s: float = 1.0          # cold STT/TTS retry: first backoff, doubling
    voice_retry_cap_s: float = 8.0           # cold STT/TTS retry: largest single backoff
    voice_warmup_budget_s: float = 240.0     # total time one STT or TTS call keeps retrying a cold endpoint
                                             # (Kokoro measured 112 s from scale-to-zero on an A10)
    voice_prewarm_interval_s: float = 60.0   # opening a voice session pings both GPU endpoints at most this often
    live_debounce_ms: int = 600              # live analysis on stable partials: at most one run per window
    live_min_new_words: int = 4              # a partial must add this many words before it is analysed
    live_suggest_interval_s: float = 2.5     # live suggestion pass: at most one LLM call per session per interval
    live_suggest_timeout_s: float = 2.5      # live suggestion pass: fail-soft after this long (= the interval,
                                             # so at most one call is in flight)
    # Model for the live suggestion pass. Default: the voice model (haiku-4-5, ~0.5 s first token in region);
    # FAST_MODEL (haiku-5-5, ~1.2 s round trip) often missed a 1.5 s budget, so chips did not appear.
    live_suggest_model: Optional[str] = None
    live_suggest_max_tokens: int = 220
    readiness_ttl_s: float = 30.0            # /healthz voice readiness (serving endpoint states) cache

    # --- retrieval ---
    retrieval_backend: Literal["auto", "memory", "keyword", "vector_search"] = "auto"
    retrieval_top_k: int = 3
    # Calibrated on e2-demo databricks-gte-large-en over the shipped KB: relevant top-1 cosine
    # ~0.60-0.77, unrelated ~0.45-0.53. Keyword (idf overlap) is a noisier offline fallback.
    retrieval_min_score_embed: float = 0.58
    retrieval_min_score_keyword: float = 0.25
    vs_index_template: str = "main.call_sidekick.kb_{playbook}"  # Vector Search index, databricks mode

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

    # --- history: Zerobus Ingest -> UC Delta (backbone=direct, SPEC section 3.7) ---
    history_catalog: str = "main"
    history_schema: str = "call_sidekick"
    zerobus_endpoint: Optional[str] = None     # <workspace-id>.zerobus.<region>.cloud.databricks.com
    zerobus_client_id: Optional[str] = None    # service principal application id
    zerobus_client_secret: Optional[str] = None
    databricks_host: Optional[str] = None      # Zerobus unity_catalog_url; Apps sets DATABRICKS_HOST
    history_queue_max: int = 10000             # bounded in-process queue; drop-oldest when full
    history_batch_max: int = 500               # records per flush per table
    history_flush_interval_s: float = 0.5
    history_flush_timeout_s: float = 10.0      # shutdown drain budget

    # --- Kafka (databricks mode, backbone=kafka) ---
    kafka_bootstrap_servers: Optional[str] = None
    # Same mode names as the notebooks (rtm_lib.KAFKA_SECURITY_MODES). When set, it overrides
    # kafka_security_protocol / kafka_sasl_mechanism below.
    # Validated only when backbone=kafka: with backbone=direct the deployed app still binds the Kafka secret
    # keys, which deploy.sh fills with a placeholder.
    kafka_security: Optional[str] = None
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

    @model_validator(mode="after")
    def _check_backbone(self) -> "Settings":
        if self.backbone == "direct" and self.bus_backend == "kafka":
            raise ValueError("BACKBONE=direct does not use Kafka; set BACKBONE=kafka to use BUS_BACKEND=kafka")
        if self.backbone == "kafka" and self.kafka_security not in (None, *KAFKA_SECURITY_MODES):
            raise ValueError(f"KAFKA_SECURITY must be one of {KAFKA_SECURITY_MODES}, got {self.kafka_security!r}")
        if self.backbone == "kafka" and self.history_backend == "zerobus":
            raise ValueError("HISTORY_BACKEND=zerobus is the direct-mode history path; in kafka mode history "
                             "flows over Kafka to delta_history.py (use auto or none)")
        return self

    @property
    def effective_bus(self) -> str:
        if self.backbone == "direct":
            return "none"
        if self.bus_backend != "auto":
            return self.bus_backend
        return "memory" if self.mode == "local" else "kafka"

    @property
    def effective_history(self) -> str:
        if self.history_backend != "auto":
            return self.history_backend
        if self.backbone == "kafka":
            return "none"
        return "memory" if self.mode == "local" else "zerobus"

    @property
    def effective_store(self) -> str:
        if self.store_backend != "auto":
            return self.store_backend
        return "memory" if self.mode == "local" else "lakebase"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
