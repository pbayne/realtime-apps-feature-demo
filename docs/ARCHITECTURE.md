# Architecture: two backbones

The sidekick has two data paths. Both produce the same live cards and the same history. They differ in
how an utterance gets enriched and how history reaches Delta. Pick one with `BACKBONE` (app) and the bundle
target (`dev` = direct, `kafka` = kafka). The wire contract is in [`SPEC.md`](SPEC.md).

| | **direct** (default) | **kafka** (scale mode) |
|---|---|---|
| Enrichment | in process, `src/app/enrich/direct.py` (same rules as RTM) | Spark Real-Time Mode job reads Kafka, writes Lakebase |
| Live state | Lakebase (utterances, enrichments, cards, feedback) | Lakebase (written by RTM for enrichments) |
| History to Delta | **Zerobus Ingest** from the app (`src/app/history/zerobus.py`) | Kafka `utterances` and `cards` → `delta_history.py` (micro-batch, 5 s) |
| Broker | none | Kafka (Redpanda Serverless, Confluent, or MSK) |
| Compute | serverless jobs, Lakebase, GPU endpoints | the above, plus a classic RTM cluster and a classic history job |
| Workspace | any (serverless OK) | classic compute required for RTM and the history job |
| Bundle target | `dev` (default) | `kafka` |

Everything the app does per turn (perception, retrieval, decision, compose, guardrails) is identical in both.
Only the path from a final utterance to enrichment, and the path to history, change.

## 1. direct backbone

```
 Browser                Databricks App (FastAPI, one process)                         Databricks
 ───────                ─────────────────────────────────────                         ──────────
 mic / replay ─PCM─►  STT: Parakeet GPU endpoint (rolling window, LA-2) ─┐
                                                                         │ final utterance
                      ┌──────────────────────────────────────────────────┘
                      │ 1. store.put_utterance (background)  ─────────────────────►  Lakebase  utterances
                      │ 2. enrich in process, ~1 ms (enrich/direct.py)  ──────────►  Lakebase  enrichments
                      │ 3. customer final: perception (haiku, budget 2.5 s) + KB retrieval (in-app gte embeddings; Vector Search opt-in)
                      │ 4. decide (candidates → rank → guardrails), compose (sonnet, streamed)
                      │ 5. card.start / card.delta / card.done over WS  ─► UI
                      │ 6. HistorySink.emit()  (sync, bounded queue, never blocks)
                      │        └─ background flusher ─► Zerobus Ingest ─────────────►  UC Delta
                      │                                                               calls, utterances, enrichments,
                      │                                                               cards, feedback, voice_turns,
                      │                                                               voice_items  (init_history.py)
                      └─ voice mode: Parakeet STT, Claude (haiku), Kokoro TTS (same app)
```

Nothing on the card path waits for a broker or for Lakebase. The enrichment is computed before the card
decision, and the Lakebase and history writes run in the background.

## 2. kafka backbone (scale mode)

```
 Browser                Databricks App                                              Databricks
 ───────                ──────────────                                              ──────────
 mic / replay ─PCM─► STT: Parakeet ─► final utterance
                      │ 1. store.put_utterance (background)
                      │ 2. produce  utterances  (key=call_id, acks=all) ──► Kafka ─┬─► RTM job (classic, update mode)
                      │                                                             │   built-in rules only
                      │                                                             │   format("postgresql") upsert
                      │ 3. wait for enrichment row, rtm_wait_ms (300 ms);            ▼
                      │    fall back to the same pure transform on timeout    Lakebase enrichments
                      │                                                             │ AFTER INSERT/UPDATE trigger
                      │ ◄──────────────────────── pg_notify('enrichment_ready') ◄───┘
                      │ 4. decide / compose / cards as in direct
                      │ 5. produce  cards  ──────────────────────────────────► Kafka topic cards
                      │                                                       │
                      └──────────────────────────────────────────────────────┴─► delta_history.py (micro-batch, 5 s)
                                                                                   ─► bronze_utterances, bronze_cards
                                                                                   ─► post_call_scoring (reads bronze)
```

The RTM job is continuous and runs on a dedicated classic cluster (fixed workers, Photon off, on-demand).
Its limits are in the README ("Kafka backbone: RTM restrictions"): output mode `update` only, no
`foreachBatch`, built-in functions only, sources Kafka/Event Hubs/Kinesis.

## 3. Latency budget

Labels: **MEASURED** = taken from a run (location noted); **ESTIMATED** = from a document or a model, not reproduced here.
Replay numbers use scripted STT timing (`stt_decode_lag_ms`), not the Parakeet endpoint, so STT rows are
the replay's configured behaviour unless marked otherwise.

### 3.1 Measured inputs

| Stage | Value | Label and where |
|---|---|---|
| STT commit (replay) | 300 ms (brief, earlier run); **600 ms p50, first word 401 ms p50 (n = 21–25, this run)** | MEASURED, replay, laptop. Replay STT uses the scripted decoder lag (`stt_decode_lag_ms`), not Parakeet |
| In-process enrichment, per final | ~1 ms (p50 0 ms rounded, max 3 ms; n = 25 and 21 finals) | MEASURED, direct backbone, laptop, `live_replay` run 2026-10-30 |
| Lakebase write + NOTIFY round trip | ~300 ms p50 | MEASURED, laptop → us-west-2 (in-region expected far lower) |
| Kafka produce, Redpanda Serverless, acks=all | ~100 ms p50 steady; first send ~1.5 s (TLS + SCRAM) | MEASURED, laptop → us-west-2 |
| customer final → card.start | 0.3–0.9 s (kafka-era runs); **direct: advisor_coach/fee_objection p50 301 ms (n = 7, max 1219); contact_center/billing_dispute p50 1002 ms (n = 5, max 1272)** | MEASURED, direct backbone, e2-demo LLMs, 2026-10-30 |
| customer final → card.done | 1.9–2.9 s (kafka-era runs); **direct: fee_objection p50 1992 ms (p90 2178); billing_dispute p50 2299 ms (p90 3088)** | MEASURED, as above |
| Voice LLM first token | haiku-4-5 563 ms p50; sonnet-5-5 751 ms; haiku-5-5 1139 ms | MEASURED, e2-demo, small sample, to re-measure in region |
| Parakeet endpoint RTT | ~150 ms; first word ~0.6 s; commit ~1.5 s | MEASURED, Hera reference, not this app |
| Kokoro first audio | 0.15 s (1 stream), 0.30 s (8), 1.27 s (32) | MEASURED, reference |
| Voice agent first audio, end to end | 0.83 s | MEASURED, field-eng voice-agent reference |
| Cold GPU endpoint after scale-to-zero | minutes (about 156 s for a cold whisper endpoint on e2-demo) | MEASURED, e2-demo |

### 3.2 Budget per stage

| Stage | direct | kafka (+ RTM) |
|---|---|---|
| Final → enrichment available | ~1 ms (MEASURED) | produce ~100 ms (MEASURED, laptop) + RTM processing 130–200 ms (ESTIMATED) + Lakebase write and NOTIFY ~300 ms (MEASURED, laptop); wait capped at `rtm_wait_ms` = 300 ms, then local fallback |
| Enrichment → decision | 0 (same process) | 0 after the wait |
| Final → card.start | 0.3 s p50 (fee_objection) and 1.0 s p50 (billing_dispute), MEASURED | 0.3–0.9 s (MEASURED, kafka-era runs) |
| Final → card.done | 2.0 s p50 (fee_objection), 2.3 s p50 (billing_dispute), MEASURED | 1.9–2.9 s (MEASURED, kafka-era runs) |
| In-region produce | n/a | 5–20 ms (ESTIMATED) |
| History → Delta visible | Zerobus, "within seconds" (ESTIMATED from the docs; not on the live path) | Kafka → bronze, 5 s trigger plus write (ESTIMATED) |
| Lakebase write (live state) | background, off the card path (MEASURED ~300 ms from laptop) | on the path to enrichment (see above) |

The card.done target (≤ 3 s, SPEC 7) held on both measured direct runs, with billing_dispute p90 at 3.1 s, just over.
Compose (sonnet streaming) is most of card.done; the backbone does not change it. The card.start spread between
playbooks (0.3 s vs 1.0 s p50) is not explained by enrichment (about 1 ms in both). It likely comes from the
retrieval and decide path (`retrieval backends` differ: embed in one run, keyword-only fallback in the other).
Not investigated further.

### 3.3 Measured on the deployed app (direct backbone, in region)

MEASURED 2026-10-30, FEVM `realtime-call-sidekick` (AWS us-west-2, serverless), warm GPU endpoints, streamed speech
from `scripts/voice_smoke.py` (macOS `say`, 16 kHz), small samples (n = 1–3 per row).

| Stage | Value |
|---|---|
| End of speech → final transcript (Parakeet rolling window, includes the 700 ms endpoint silence) | 0.84–0.97 s (2026-10-30, latest run) |
| Parakeet window decode round trip (from a laptop; in region is lower) | p50 312 ms |
| Voice model first token, practice prompt | 0.41 s (0.52–0.65 s standalone; first call after idle ~3.2 s) |
| Voice model first token, assistant prompt (+ KB retrieval) | 1.03 s |
| Kokoro first audio per sentence | 107–167 ms |
| **Turn start → first audible reply** | practice **0.60–0.69 s**, assistant **0.82 s** (2026-10-30; was 1.29 s before the per-turn Vector Search call that failed was removed, so the assistant turn no longer waits on it) |
| Kokoro cold start from scale-to-zero | 112 s |
| Zerobus history | 41 queued / 41 written / 0 dropped / 0 failed |

Transcript accuracy note: Parakeet word timestamps drift 160–400 ms between overlapping 12 s windows, so LocalAgreement
aligns re-decoded windows on the committed text (see `src/app/stt/agreement.py`; regression fixture recorded from the
deployed endpoint).

## 4. When to choose which

**direct** (default) for demos, POCs, and most customers:
- Fewer moving parts. No broker, no RTM cluster, no Kafka secrets.
- Lower latency on the card path (enrichment is in process).
- Works on a serverless workspace. The bundle's default target deploys only serverless jobs and apps.
- Cost is the always-on Lakebase and GPU endpoints, not a broker or a classic cluster.
- Limits: one app process owns each call. Many concurrent calls scale by app instances, which is fine for
  call-center and advisor volumes, not for a central bus.

**kafka + RTM** when:
- The customer already runs Kafka, Event Hubs (Kafka API), or MSK, and wants calls on that bus.
- Many concurrent calls, or other consumers need every utterance (fan-out).
- Stateful stream processing joins call events with other live feeds (fraud, CRM, telemetry).
- Exactly-once delivery to several sinks matters.
- Sub-second Spark-side features are needed.

**Costs and prerequisites of kafka:**
- An always-on RTM classic cluster (2 × m5d.xlarge workers plus a driver on-demand) while the RTM job runs.
  Autotermination does not stop an active stream. Cancel the run when no calls are live.
- A broker (Redpanda Serverless, Confluent, or MSK).
- A **classic** workspace. RTM requires classic compute (DBR 16.4+ for Kafka, 18.3+ for the Lakebase sink).
  Serverless cannot run RTM.
- The Kafka secrets (`kafka_bootstrap`, `kafka_api_key`/`_secret` for jobs, `kafka_app_api_key`/`_secret` for the app).

## 5. Cost and ops

| | direct | kafka |
|---|---|---|
| Lakebase (0.5–2 CU, no suspension) | yes | yes |
| GPU endpoints: Parakeet (and Kokoro for voice) | yes; scale-to-zero configurable (below) | same |
| Broker | none | Redpanda Serverless / Confluent / MSK |
| RTM cluster (always on while RTM runs) | none | 2 × m5d.xlarge + driver |
| Delta history job | serverless batch only (post-call scoring) | classic job cluster, continuous 5 s micro-batch |
| History writes | Zerobus (service principal, MODIFY+SELECT on 7 tables) | Kafka producer + bronze job |
| Secrets | Lakebase, Zerobus client id and secret, endpoint | the above plus Kafka secrets |
| Failure surface | Zerobus queue (bounded, drop-oldest, counted); Lakebase (background) | broker, RTM job, checkpoint, topic ACLs |
| On a failed history write | retried on a fresh stream (3 attempts, backoff), requeued once, then counted; acknowledged writes are at-least-once; the call continues | the bronze job retries from the checkpoint |

### GPU endpoints and scale-to-zero (both backbones)

`gpu_scale_to_zero` (bundle variable, env `GPU_SCALE_TO_ZERO` for `deploy.sh`; default **true** for dev cost).
With scale-to-zero on, an idle Parakeet or Kokoro endpoint is scaled to zero and its first request waits for a
cold GPU. We measured about 156 s for a cold whisper endpoint on e2-demo. Before a demo, run
`scripts/warmup_voice.py --profile <p>`, which pings both endpoints and prints time-to-ready. For demos and
latency measurements, deploy with `GPU_SCALE_TO_ZERO=false` so the endpoints stay warm. The cost of that is
two always-on A10G endpoints.

## 6. History contract (Zerobus, direct)

- Tables: `calls`, `utterances`, `enrichments`, `cards`, `feedback`, `voice_turns`, `voice_items` in
  `${HISTORY_CATALOG}.${HISTORY_SCHEMA}`. Created by `src/notebooks/init_history.py` (managed Delta, SQL only).
- Records are JSON objects, one per row. Column names and types are in `src/app/history/schema.py`, checked
  against the notebook by `tests/app/test_history.py`. Epoch times are BIGINT milliseconds. JSON-valued
  columns are STRING.
- One stream per table. Batches are flushed every 0.5 s or 500 records. The queue holds 10 000 records and
  drops the oldest when full (counted in `/healthz` → `history.dropped`).
- Acks mean durable, not queryable (the docs). On a write error a failed batch is retried on a fresh stream with exponential backoff (`HISTORY_RETRY_ATTEMPTS`, default 3; `retried` counts), and requeued once at the front of the queue after the last attempt, then counted in `failed`. Acknowledged writes are at-least-once: a retry after a partial ack can duplicate rows, and a batch that fails twice is lost from Zerobus.
- **Completeness: Lakebase → Delta reconcile [v0.7].** Lakebase holds every record under the same natural keys
  (calls.call_id, utterances/enrichments (call_id, seq), cards.card_id, feedback (card_id, at_ms), voice_turns
  (session_id, turn), voice_items.item_id), so it is the source of truth. `src/notebooks/reconcile_history.py` runs as
  serverless job `call_sidekick_reconcile_history` every 15 minutes (max concurrent runs 1). It reads each Lakebase
  table through the UC catalog registered on the Lakebase database (`postgres_catalogs` in `resources/lakebase.yml`,
  `<lakebase_catalog>.public.<table>`, read-only, serverless compute). Each run reads a 48 h watermark window, takes one
  row per key, removes Delta duplicates for those keys, and MERGEs by key (inserts missing rows, updates changed rows).
  Recovery point objective: 15 min plus job runtime after a record reaches Lakebase. `dedupe_existing` and
  `full_sync` are one-off backfill switches. Why the UC catalog and not a pg8000 read: it is declarative in the bundle,
  needs no password in the job, and Spark reads it directly; pg8000 would have needed the scramp pin and a password
  in the job path.
- Sources: Zerobus Ingest docs (<https://docs.databricks.com/aws/en/ingestion/zerobus-ingest>), message blocking
  and acknowledgment (<https://docs.databricks.com/aws/en/ingestion/zerobus-message-blocking>), and the Python
  SDK README (<https://github.com/databricks/zerobus-sdk/blob/main/python/README.md>). SDK 1.10.0,
  package `databricks-zerobus-ingest-sdk`.

Kafka mode writes history through `delta_history.py` instead (utterances and cards only). `calls`, `enrichments`
and `feedback` have no Delta copy in kafka mode; they stay in Lakebase.

## 7. Switching backbones

```bash
# direct (default): serverless jobs, Zerobus history, no broker
BACKBONE=direct scripts/deploy.sh <profile> <catalog>
# kafka: adds the RTM cluster, the rtm_enrich and delta_history jobs, Kafka topics and secrets
BACKBONE=kafka scripts/deploy.sh <profile> <catalog> SASL_SSL_PLAIN
```

- `BACKBONE` sets the bundle target (`dev` or `kafka`) and the app's `backbone` secret. The app reads
  `BACKBONE` at start.
- `databricks bundle deploy -t kafka` creates: the RTM cluster, `call-sidekick-rtm-enrich`,
  `call-sidekick-delta-history` (classic), and the Kafka topics (setup job).
- `databricks bundle deploy -t dev` should delete the resources that only the kafka target declared. This is
  expected DAB behaviour for resources removed from a bundle, not verified here. Cancel the RTM run first
  (`databricks jobs list-runs --active-only`, then `databricks jobs cancel-run <run_id>`), and confirm the
  cluster is gone with `databricks bundle summary -t dev`.
- Switching direct → kafka needs the Kafka secrets in the scope first. `deploy.sh` checks them and stops if
  they are missing.
- Switching does not migrate history. Zerobus rows and bronze rows are separate tables.

## 8. Measurement notes

- The direct live run used `tests/app/live_replay.py` against e2-demo (haiku perception, sonnet compose,
  gte-large embeddings), local mode, memory store and memory history, `advisor_coach/fee_objection`, 2026-10-30.
- The voice model choice (`databricks-claude-haiku-4-5`) rests on a small e2-demo sample. Re-measure in the
  deployed region before a commitment.
- Zerobus has not been exercised against a live workspace from this repo. The sink is tested with a fake SDK,
  and the SDK's signatures were checked against the installed package.

## 9. Session lifetime, ownership and limits (v0.6)

- Every call and voice session records its owner (`X-Forwarded-Email`, else `X-Forwarded-User`; `local` in MODE=local).
  Routes and sockets return 404 or 4404 to anyone else.
- A reaper (`services.reap_once`) closes voice sessions with no socket after `VOICE_DETACHED_TTL_S` and finishes calls
  with no attachment after `CALL_DETACHED_TTL_S`. Ended and closed entries are dropped after `SESSION_GRACE_S`.
- One socket per session: a newer socket evicts the older one (close 4409). The first voice-socket event is `hello`.
- Sockets have a message rate limit (token bucket), text is capped in bytes, and the app runs with a 1 MiB
  WebSocket message cap.
- Lakebase is optional at startup. The store retries in the background, and the app runs degraded until it connects.
- The LLM circuit breaker is per model; warm-up pings do not count.

