# Realtime Call Sidekick — Build Spec (v0.6)

Live call → sub-second transcription → enrichment → Lakebase → sidekick agent → suggestion cards
pushed to the rep/agent UI over WebSocket. Two backbones (docs/ARCHITECTURE.md):

- **direct** (default): enrichment in process; live state in Lakebase; history to Delta through
  Zerobus Ingest (section 3.7). No Kafka.
- **kafka** (scale mode): utterances to Kafka, Spark Real-Time Mode enrichment into Lakebase, history to Delta
  through Kafka jobs.

v0.6 records what the code does (2026-10-30). Changes from v0.5 are marked **[v0.6]**. Earlier changes are
**[v0.2]** to **[v0.5]**.

## 1. Hard constraints (verified)
- **Backbone [v0.4].** `BACKBONE=direct` (default) or `kafka`. Direct: no bus (`bus/null.py`), enrichment in
  process (`enrich/direct.py`), history through `HistorySink` (`history/zerobus.py` in databricks mode,
  `history/memory.py` in local mode). Kafka: `bus/kafka.py` (databricks) or `bus/memory.py` (local stand-in),
  RTM job for enrichment, history none (history flows through `delta_history.py`). Settings reject
  `BACKBONE=direct` with `BUS_BACKEND=kafka`, and `HISTORY_BACKEND=zerobus` with `BACKBONE=kafka`.
- RTM sources: Kafka / Event Hubs (Kafka API) / Kinesis. **Not Delta.** RTM [kafka backbone only] 
  supports `update` output mode only; **no `foreachBatch`** (use `foreach` or the
  native `format("postgresql")` sink, DBR 18.3+, classic cluster, Photon off,
  `spark.databricks.streaming.realTimeMode.enabled=true`).
- **Zerobus Ingest [v0.4]** is the history path in direct mode (section 3.7). It needs pre-created UC managed
  Delta tables, a service principal with MODIFY and SELECT on them, and the SDK package
  `databricks-zerobus-ingest-sdk`. Sources: https://docs.databricks.com/aws/en/ingestion/zerobus-ingest
- **Compute [v0.4].** The direct backbone deploys serverless jobs only (environment version 6). GPU jobs use
  `compute.hardware_accelerator: GPU_1xA10`. RTM and the Delta history job need classic compute (kafka only).
- STT: Parakeet-tdt-0.6b-v3 on a GPU Model Serving endpoint (route-optimized,
  no scale-to-zero), driven Hera-style: rolling 12 s window re-decoded every
  `frame_s`=0.4 s, LocalAgreement-2 commit, `hold_words`=1, Silero VAD gate,
  dual channel (agent / customer). Target: first word ~0.6 s, commit ~1.5 s.
  Never go below `frame_s` 0.3.
- Local dev has **no Docker**. Everything must run locally in `MODE=local` with
  in-memory substitutes behind the same interfaces.
- Rules run inside Spark with built-ins only (`rlike`, `regexp_replace`,
  `regexp_extract_all`, `filter`, `from_json`, `to_json`). **No Python UDFs** in the RTM plan.
  `tests/notebooks/test_enrich_rules.py` asserts this on the analyzed plan.

## 2. Components & directories (each owned by one workstream)

```
src/app/                      FastAPI Databricks App (bundled as the DAB app source_code_path)
  main.py                     app factory, lifespan, REST + WS routes
  config.py                   pydantic-settings; MODE, BACKBONE, HISTORY_BACKEND; every SPEC default
  services.py                 wires bus, store, history sink, enricher, LLM, retrieval, playbooks, sessions
  session.py                  CallSession: per-call pipeline and turn worker (section 4)
  hub.py                      per-call WebSocket fan-out; stamps emitted_at_ms
  bus/        base.py         EventBus protocol: publish(topic, key, value), subscribe
              null.py         no bus: direct backbone [v0.4]
              memory.py       asyncio in-memory bus (local; kafka backbone stand-in)
              kafka.py        aiokafka producer (kafka backbone; SASL/SSL via env)
  history/    base.py         HistorySink protocol: emit(table, row) sync, never blocks [v0.4]
              zerobus.py      Zerobus Ingest sink: bounded queue, background flusher, one stream per table [v0.4]
              memory.py       in-memory sink (local, tests) and NullHistorySink (kafka) [v0.4]
              schema.py       the section 3.7 table contract and row builders [v0.4]
  store/      base.py         Store protocol (calls, utterances, enrichments, cards, feedback)
              memory.py       in-memory + asyncio change notifications
              lakebase.py     asyncpg pool; LISTEN/NOTIFY on `enrichment_ready`; DDL matches section 3
  stt/        base.py         SttEngine protocol
              parakeet.py     StreamingSession: VAD + rolling window + LocalAgreement-2 → serving endpoint
              replay.py       scripted replay: emits partial/final words at real speaking pace
              browser.py      pass-through for browser Web Speech API results (local fallback)
              vad.py          energy VAD (local) / Silero (when installed)
              agreement.py    LocalAgreement-2
  enrich/     rules.py        pure transform (Python mirror of rtm_lib); extend_rules for playbooks
              enrich_rules.json  shared rules, byte-identical to src/notebooks/enrich_rules.json
              direct.py       DirectEnricher (direct backbone, in process) [v0.4]; BusEnricher = local RTM
                              stand-in for kafka mode (was local_enricher.py)
  sidekick/    perception.py   ONE fast-model call per customer final: intent, sentiment, objection, question
              retrieval.py    KB retrieval: in-memory embed/keyword (local), Vector Search (databricks)
              decide.py       candidate cards → rank → guardrails → trace (pure)
              compose.py      smart-model card text, streamed tokens (sonnet)
              playbooks.py    load playbooks/*.yaml + KB markdown
  playbooks/                  contact_center.yaml, advisor_coach.yaml, kb/<playbook>/*.md   [v0.2: moved here]
  data/replay/                scripted calls: <playbook>__<script>.jsonl + build_replay.py   [v0.2: moved here]
  llm.py                      OpenAI-compatible client to Databricks serving (databricks-sdk auth)
  static/                     built frontend (committed; CI fails if stale)
frontend/                     React 19 + Vite + Tailwind; `bun run build` writes into src/app/static
src/notebooks/                Databricks side
  init.py                     Lakebase tables (section 3) + NOTIFY trigger + secrets; Kafka topics only when backbone=kafka
  init_history.py             Zerobus history tables (section 3.7) + grants to the ingest principal [v0.4]
  rtm_lib.py                  pure Spark expressions for enrichment (no UDFs)
  rtm_enrich.py               [kafka] Kafka `utterances` → RTM → enrichment → postgresql sink (`enrichments`)
  delta_history.py            [kafka] Kafka `utterances` + `cards` → micro-batch → Delta bronze
  register_parakeet.py        MLflow pyfunc for nvidia/parakeet-tdt-0.6b-v3; GPU endpoint (section 3.3)
  build_kb_index.py           src/app/playbooks/kb → Delta → Vector Search index
  post_call_scoring.py        Lakeflow job: ai_query() call scoring, methodology fill, summary
resources/                    DAB yml: serverless jobs, app, lakebase; targets.kafka in databricks.yml holds the RTM cluster and Kafka jobs [v0.4]
scripts/deploy.sh             BACKBONE, Zerobus and Kafka secrets, ordered deploy; warmup_voice.py for GPU endpoints [v0.4]
tests/app/                    pytest, local mode, no network (LLM replaced by fakes)
tests/notebooks/              pytest: rules and Spark expressions (skip without pyspark), bundle YAML
tests/golden/enrich_cases.json  golden enrichment cases shared by both sides
```

## 3. Data contracts (shared — do not change without updating this file)

### 3.1 Kafka topics (JSON values, key = call_id) [kafka backbone only, v0.4]
`utterances`:
```json
{"call_id":"c-123","seq":17,"speaker":"customer|agent","text":"...",
 "is_final":true,"t_start_ms":12340,"t_end_ms":14010,"emitted_at_ms":1730000000000,
 "playbook":"contact_center"}
```
Only `is_final=true` utterances go to Kafka. Partials go straight to the UI.

`cards` (written by app for history): the Card object below.

### 3.2 Lakebase / Store tables
Created by `src/notebooks/init.py` in Databricks. The app's `store/lakebase.py` uses the same DDL with `IF NOT EXISTS`.
JSON-valued columns are **TEXT holding JSON** (the sink binds Spark strings).

- `calls(call_id PK, playbook, started_at, ended_at, customer_id, status, mode, script)`
- `utterances(call_id, seq, speaker, text, t_start_ms, t_end_ms, emitted_at_ms, playbook, PK(call_id,seq))`
- `enrichments(call_id, seq, speaker, text, intent, sentiment DOUBLE (-1..1), risk_flags TEXT (JSON array),
   objection TEXT NULL, entities TEXT (JSON object of arrays), methodology_hits TEXT (JSON array),
   emitted_at_ms BIGINT, enriched_at_ms BIGINT, latency_ms BIGINT (= enriched − emitted), PK(call_id,seq))`
  — written by the RTM job (kafka backbone) or by the app in process (direct backbone), upsert on PK. The trigger
  `AFTER INSERT OR UPDATE` → `pg_notify('enrichment_ready', call_id||':'||seq)` is used by the kafka backbone only [v0.4].
  `latency_ms` and `emitted_at_ms` are **[v0.2]** additions.
- `cards(card_id PK, call_id, seq, kind, title, body, confidence, source_doc, source_clause,
   trigger, status (shown|accepted|edited|rejected|expired), created_at_ms, trace TEXT (JSON))`
- `feedback(card_id, call_id, action, edited_text, at_ms)`

Enrichment is **deterministic + cheap** [v0.4: in process in direct mode; RTM in kafka mode] (lexicon/regex for objections,
methodology keyword hits, entity regex, sentiment lexicon, question intent) so it stays
sub-second inside Spark. LLM perception runs app-side (section 4) and is merged with the RTM row.

### 3.1a Backbone and history, summary [v0.4]
| | direct (default) | kafka |
|---|---|---|
| Enrichment | `enrich/direct.py`, in process, ~1 ms | RTM job, wait ≤ `rtm_wait_ms`, local fallback |
| Live state | Lakebase (`utterances`, `enrichments`, `cards`, `feedback`, `calls`) | same |
| History | Zerobus Ingest, section 3.7 | Kafka `utterances` and `cards` → `delta_history.py` |

### 3.3 Parakeet STT endpoint **[v0.3: adopt the field-eng audio-serving server]**
Deployed by `src/notebooks/register_parakeet.py`, ported from
`reference/custom-llm-serving-solutions/audio-serving/parakeet-tdt-0.6b-v3.ipynb`: a Custom LLM Serving
entrypoint (FastAPI + NeMo, bf16, 5 ms micro-batching, task `llm/v1/audio/transcriptions`,
`env_pack="databricks_model_serving"`), GPU_MEDIUM (A10), scale-to-zero **off**, **not** route-optimized
(unproven for custom-entrypoint audio endpoints). Called from both the call path (`stt/parakeet.py`
rolling-window decoder) and voice mode.

Request: multipart `POST {host}/serving-endpoints/{STT_ENDPOINT}/audio/transcriptions`
with `file` (16 kHz mono PCM16 **WAV**, ≤ 30 s), `model` (endpoint name), `response_format=verbose_json`.
Response: `{"duration": 4.3, "text": "...", "segments": [{"start","end","text"}], "words": [{"word","start","end"}]}`
- Times are **seconds relative to the clip**. The caller converts to absolute call time:
  `start_ms = offset_ms + round(start * 1000)`. The endpoint does not know about channels.
- Auth: the app's service principal via `WorkspaceClient().config.authenticate()` headers on the workspace
  host. No data-plane URL. Failures are fail-soft (no words for that window / turn).
- Local dev stand-in: any Databricks-hosted Whisper endpoint with the same route (e.g. `shared-whisper`
  on e2-demo) if it supports `/audio/transcriptions`; otherwise the replay/browser sources.

### 3.3a Kokoro TTS endpoint **[v0.3]**
Deployed by `src/notebooks/register_kokoro.py`, ported verbatim from
`reference/custom-llm-serving-solutions/audio-serving/kokoro-tts/` (`serving/kokoro_server.py`,
`serving/adapter.py`, 01-serving cells): placeholder ChatModel with an `entrypoint` (4 Kokoro workers +
adapter), `kokoro==0.9.4 misaki[en]==0.9.4`, GPU_MEDIUM (A10), workload Small, scale-to-zero **off**.

Request: `POST {host}/serving-endpoints/{TTS_ENDPOINT}/invocations`
`{"messages":[{"role":"user","content":"<one sentence>"}],"voice":"af_heart","stream":true}` (optional `speed`).
Response: SSE `chat.completion.chunk`. First `delta.content` is a header
`#audio/pcm;rate=24000;channels=1;format=s16le;...\n` (skip it); each later chunk is base64 raw s16le mono
24 kHz PCM; ends with `finish_reason:"stop"` then `[DONE]`.
- One request per sentence. Keep sentences short (the worker holds the GPU per sentence).
- Always close the HTTP stream on cancel (an unread stream can stall a Kokoro worker).
- Scale-to-zero is configured per deployment: `gpu_scale_to_zero` (default true for dev cost). A cold endpoint
  makes the first turn after idle wait minutes. Warm both endpoints before a demo (`scripts/warmup_voice.py`)
  or deploy with scale-to-zero off. [v0.4]
- Voices: 54 presets, `af_heart` default; the practice-customer persona picks its own voice per playbook.

### 3.4 Card
```json
{"card_id":"uuid","call_id":"c-123","seq":17,
 "kind":"next_question|suggested_response|objection_pickup|missing_info|compliance|example|next_best_action",
 "title":"short", "body":"one or two lines", "confidence":0.82,
 "source_doc":"kb/advisor_coach/fee_objections.md","source_clause":"...", "trigger":"objection:fees",
 "status":"shown","created_at_ms":0}
```
`card.start` omits `body`. The stored `trace` also records `card_path` (section 4.4).

### 3.5 REST **[v0.2]**

| Method and path | Response |
|---|---|
| `GET /healthz` | `{status, mode, backbone, bus, store, store_ready, history, vad, retrieval, llm, active_calls, playbooks, enrich_rules, voice}`. `status` is `degraded` while the store is not ready [v0.6]. `llm.last_error` is the exception type name only [v0.6]. `history` = sink stats (`backend, enabled, queue_depth, queued, written, dropped, failed, skipped`) [v0.4] |
| `GET /api/playbooks` | `{"playbooks": [{name, title, persona, card_weights, methodology:[{key,label}], objections:[key], compliance:[key], kb_docs:[doc_id], replay_scripts:[name]}]}`. The UI maps `title` → label. |
| `GET /api/scripts?playbook=<name>` | JSON array of replay script names for that playbook. 404 for an unknown playbook. Without `playbook`: `{<playbook>: [names]}`. |
| `POST /api/calls` | body `{playbook, mode: replay\|mic, script?, speed?, customer_id?}` → 201 `{call_id, playbook, mode, script, started_at_ms}` |
| `GET /api/calls/{id}` | snapshot `{call, playbook, transcript, cards, feedback, coverage, decisions, as_of_ms}`. `decisions` holds the live trace (last 50) while the call is active in this process. |
| `POST /api/calls/{id}/end` | `{call_id, ended: true}`; emits `call.ended`. Owner only (404 otherwise). [v0.6] |
| `GET /api/calls/{id}` (v0.6) | adds `degraded: true` when the store is down and the call is served from memory |
| `DELETE /api/voice/sessions/{id}` [v0.6] | 204; ends the session now (`session_ended` reason `closed`, then close 4410). Owner only. |
| `POST /api/voice/sessions/{id}/end` [v0.6] | 204; the same as DELETE, for `navigator.sendBeacon`, which can only POST |

### 3.6 UI WebSocket `/ws/call/{call_id}` (server → client events)
Every event carries `emitted_at_ms`. On connect the server sends a `coverage` event (or `call.ended` if the call is over).
```
{"type":"transcript.partial","speaker":..,"text":..,"seq":..,"first_word_ms":..}
{"type":"transcript.final", "speaker":..,"text":..,"seq":..,"latency_ms":..}
{"type":"enrichment","seq":..,"speaker":..,"source":"direct|rtm|local_fallback|perception","intent":..,"sentiment":..,
  "risk_flags":[..],"objection":..}
{"type":"card.start","card":{...without body}}
{"type":"card.delta","card_id":..,"delta":"tokens"}
{"type":"card.done","card":{...with body}}
{"type":"coverage","items":[{"key":"budget","label":"Budget","covered":true}]}
{"type":"metrics","seq":..,"stt_first_word_ms":..,"stt_commit_ms":..,"rtm_ms":..,"card_ms":..,"card_done_ms":..,
  "card_path":"in_time|late|compliance|null","compose_mode":"llm|template|verbatim|null","enrichment_source":".."}
{"type":"error","scope":"stt","speaker":..,"message":..}
{"type":"call.ended"}
```
Client → server:
- `{"type":"feedback","card_id":..,"action":"accept|reject|edit","text":..}`
- `{"type":"browser_stt","speaker":"agent|customer","text":..,"is_final":true|false}` **[v0.2]**: Web Speech API results
  (browser STT fallback). Finals become transcript finals on the same path as server STT.

A malformed client message returns an `error` event and keeps the socket open.

### 3.7 Audio WebSocket `/ws/audio/{call_id}?channel=agent|customer`
Binary frames: PCM16 mono 16 kHz, ~100 ms per frame. Browser mic = `agent`
channel by default (toggle). Only for `mode=mic` calls; a replay call gets an `error` and close (4400).
Replay mode needs no audio socket.

## 4. Sidekick decision policy

### 4.1 Per final **customer** utterance
1. **Start immediately** when the final arrives: app-side perception (one fast-model call, budget
   `perception_budget_s` = 2.5 s) and KB retrieval. Retrieval may already be running from a speculative
   stable partial (4.3). Nothing waits on perception.
2. Wait for enrichment (RTM, at most `rtm_wait_ms`); fall back to the same pure transform in `enrich/rules.py`.
   Merge RTM and local results (union of risk flags and methodology hits).
3. Coverage update (customer turns) and compliance (any speaker, 4.5).
4. The turn enters a debounced serial worker (`debounce_ms` = 300, coalesces bursts; only the last final of a burst is decided).

### 4.2 Decide, and perception design **[v0.2]**
- **Decide at once.** The turn waits for retrieval for at most `retrieval_budget_s` (0.8 s). On a miss it re-runs
  retrieval keyword-only (no network) and records `fallback: true`. Candidates come from enrichment + rules + retrieval + coverage.
- **Perception is never on the critical path.** If perception has already finished, its fields are used in the decision (`perception_path: in_time`).
  Otherwise the turn is decided with perception **pending** (`perception: {"state": "pending"}`, `perception_path: late_eligible`).
- **One call, strict JSON.** The fast model (`FAST_MODEL`, haiku) returns exactly
  `{"intent": one of [question, objection, request, agreement, statement, closing], "sentiment": -1..1,
    "objection": <playbook objection key or null>, "question": true|false, "confidence": 0..1}`.
  Code fences are stripped. Parsing is tolerant: outermost `{...}`, then a per-field regex if the JSON is truncated,
  then an empty result. Values are normalised (unknown intent or objection → null, sentiment clamped, `question`
  accepts true/yes). `question: true` with no intent implies intent `question`.
- **Thinking off.** Haiku 5.5 starts a `reasoning` block on longer prompts and then returns no text within
  `max_tokens`. The client sends `thinking: {type: disabled}` for haiku (`llm._model_extras`).
- **Late candidate (the SPEC's late rule).** When perception finishes after the decision, the app records a `late_perception`
  trace entry with the perception result and a verdict:
  - `skipped`: a card (regular or compliance) already fired for this turn. Nothing else is evaluated.
  - `allowed`: no card fired for this turn, and the late candidates pass the guardrails (4.3). Card(s) emit with `card_path: late`.
  - `blocked`: no late candidate passed the guardrails. Silent.
  The turn's own trace gets `late_perception: {verdict, reason, cards, arrived_after_final_ms}`.
- Perception may refine the enrichment chips with an `enrichment` event, `source: "perception"`. Absent fields are null,
  so the UI keeps the RTM values.
- Perception budget miss: `errors.perception = "timeout"`. The cards still flow. Failures never raise.

### 4.3 Candidates and guardrails
- Candidates by playbook rules:
  - objection → `objection_pickup` (rules objection first, then perception);
  - question (perception `question`, else rules intent) → `suggested_response`, grounded or silent;
  - request → `next_best_action` when grounded;
  - coverage gap during a lull → `next_question` or `missing_info` (one per open item, first three in playbook order);
  - risk flag → `compliance` (4.5).
- Rank by confidence × playbook type weight. Guardrails: confidence ≥ 0.6, at most 1 non-compliance card per customer turn,
  per-kind cooldown (3 turns), per-call budget (default 12), no duplicate trigger. Nothing passes → silent.
- **Speculative retrieval [v0.2].** A customer partial with at least 3 words starts a retrieval, one in flight at a time.
  A final reuses the most recent speculative search whose token Jaccard with the final is ≥ 0.8. Otherwise it searches afresh.
- Retrieval: embedding cosine (`EMBED_MODEL`, threshold 0.58) when available, keyword idf overlap otherwise (threshold 0.25).

### 4.4 Output and trace
- Compose the winner with the smart model and stream tokens: `card.start` → `card.delta`* → `card.done`.
  Title ≤ 8 words, body ≤ 35 words. Template text when the model is unavailable; compliance bodies are the playbook's verbatim message.
- Persist the card with the full decision trace (candidates, verdicts, perception, retrieval, timings, `card_path`, `late_perception`). Trace kept in memory per call (last 200), exposed by `GET /api/calls/{id}`.
- MLflow tracing when available (fail-soft). Not wired yet.

### 4.5 Compliance **[v0.2]**
- Compliance candidates come from the playbook's `compliance` entries when a risk flag fires. They **bypass** confidence,
  cooldown, budget and dedupe, and fire for **any speaker**. The body is the playbook's verbatim message.
- **Negation-aware exclusions.** Each enrichment rule item may carry `exclude_patterns`. Before the item's patterns are tested,
  every exclude match is replaced by a space (Python `re.sub`; Spark `regexp_replace`, same alternation). Disclaimers such as
  "no one can guarantee", "not guaranteed", "can't guarantee" and "no investment is guaranteed" do not set `unsupported_claim`.
  A real claim in the same sentence still does ("no one can guarantee returns, but we guarantee the fee cap").
  Golden cases: `tests/golden/enrich_cases.json`, run on both sides.
- **Re-fire suppression.** The same compliance trigger does not fire again within `COMPLIANCE_REPEAT_FINALS` = 3 finals
  (customer asks, agent restates). Suppressed repeats are listed in the trace.
- A compliance card on a final suppresses late candidates for that turn (4.2).

### 4.6 Enrichment rule schema **[v0.2]**
`enrich_rules.json` groups: `objections`, `risk`, `methodology`, `entities` (each `{key, patterns[], exclude_patterns[]?}`),
`sentiment{positive[], negative[]}`. Keys are snake_case and unique per group. Patterns are case-insensitive regexes in the
common subset of Java and Python syntax. The file must be byte-identical in `src/app/enrich/` and `src/notebooks/`
(enforced by a test). Playbooks extend it with patterns (`enrich/rules.py::extend_rules`); exclude patterns carry over.

## 5. Playbooks
YAML: `name, title, persona, card_weights{kind: w}, methodology{items:[{key,label,keywords[],patterns[],question}]},
objections[{key, patterns[], kb_ref}], compliance[{key, title, patterns[], message}], kb_dir, prompts{perception, compose}`.
Ship `contact_center` (claims/billing service: NBA, compliance, empathy) and `advisor_coach` (wealth-advisor discovery:
goals, time horizon, risk tolerance, assets, budget, decision makers; fees, think-about-it, market-timing and existing-advisor
objections; coverage checklist). Files live in `src/app/playbooks/`; replay scripts in `src/app/data/replay/` (section 2).

## 6. Models and runtime settings (configurable via env)
FAST_MODEL=databricks-claude-haiku-5-5 (perception), SMART_MODEL=databricks-claude-sonnet-5-5 (compose),
EMBED_MODEL=databricks-gte-large-en (retrieval), STT_ENDPOINT=sidekick-parakeet (+ optional STT_ENDPOINT_URL).
[v0.4] `BACKBONE=direct|kafka` (default direct). `HISTORY_BACKEND=auto|memory|zerobus|none` (auto: direct+databricks → zerobus,
direct+local → memory, kafka → none). `HISTORY_CATALOG`, `HISTORY_SCHEMA`, `ZEROBUS_ENDPOINT`, `ZEROBUS_CLIENT_ID`,
`ZEROBUS_CLIENT_SECRET`, `DATABRICKS_HOST` (Zerobus unity_catalog_url), `HISTORY_QUEUE_MAX` (10000),
`HISTORY_BATCH_MAX` (500), `HISTORY_FLUSH_INTERVAL_S` (0.5).
Local dev uses the Databricks CLI profile `DATABRICKS_CONFIG_PROFILE=e2-demo`.
Everything degrades gracefully: if the LLM is unavailable, rules-only cards still flow (template bodies).
A circuit breaker opens after 3 consecutive failures for 30 s.

## 7. Latency instrumentation and targets
Every event carries `emitted_at_ms`. The app pushes `metrics` per final. Targets:
- STT first word ≤ 0.8 s; STT commit ≈ 1.5 s.
- final → enrichment: direct ≈ 1 ms in process [v0.4]; kafka (RTM) ≤ 300 ms.
- **customer-final → card.start ≤ 1.5 s (aim ≤ 1.0 s).** Measured, see the table below.
- **customer-final → card.done ≤ 3 s.**

Measured with `tests/app/live_replay.py` at speed 2.0 against the real backend (e2-demo, haiku/sonnet/gte), 2026-10-28:

| Playbook / script | Metric | Before (v0.1) p50 / p90 | After (v0.2) p50 / p90 |
|---|---|---|---|
| advisor_coach / fee_objection | card.start | 1734 / 1912 ms | 303 / 933 ms |
| advisor_coach / fee_objection | card.done | 3162 / 3941 ms | 2132 / 2761 ms |
| contact_center / billing_dispute | card.start | 1680 / 2564 ms | 388 / 491 ms |
| contact_center / billing_dispute | card.done | 2977 / 3741 ms | 1946 / 3205 ms |

Before: every perception call timed out at 1.2 s, so every card waited for the full timeout. After: perception parses on
the first call (`parse: json`), and decisions do not wait for it (`decided_after_final` p50 ≈ 300 ms). The card.done p90 on
contact_center is over budget. That is the sonnet compose stream and is an open item.

## 8. Packaging **[v0.2]**
The DAB app's `source_code_path` is `src/app`, so everything the app reads at runtime lives under `src/app`:
`playbooks/`, `data/replay/`, and `enrich/enrich_rules.json`. The KB index job reads `${workspace.file_path}/src/app/playbooks/kb`.

## 9. Voice mode **[v0.3]** (v0.4: VOICE_MODEL default changed, see 9.4)
Two-way voice inside the same Databricks App. **Only Databricks-hosted models** (Parakeet STT, Claude on
Databricks serving, Kokoro TTS). Two roles, switchable per session:
- `assistant`: the rep talks to Sidekick before / during / after a call ("what should I ask next?",
  "summarize", "draft the follow-up"). Context = the linked call's transcript, cards, coverage (if any) +
  playbook KB retrieval.
- `practice`: Sidekick plays the **customer** from a playbook scenario (persona, goals, objections to raise).
  The rep's speech is the `agent` channel and the AI customer's speech is the `customer` channel of a normal
  call session, so the existing Sidekick pipeline (enrichment → decide → cards, coverage) coaches live.

### 9.1 Session REST
`POST /api/voice/sessions {"role":"assistant|practice","playbook":"advisor_coach","scenario":"fee_objection"?,"call_id":"<link>"?,"voice":"af_heart"?}`
→ `{"session_id","call_id"}` (practice always creates a call_id; assistant may link one). The linked call must belong to the caller (404 otherwise) [v0.6].
`GET /api/voice/sessions/{id}` → snapshot (turns, items), live sessions of the caller only [v0.6]. Ended sessions are not readable. `GET /api/voices` → TTS voice list.
`GET /api/playbooks` gains `scenarios` per playbook (practice personas, from playbook YAML `practice:`).

### 9.2 WebSocket `/ws/voice/{session_id}`
Client → server:
- binary frame = **one complete user turn** as 16 kHz mono PCM16 WAV (client-side VAD decides turn end). **[v0.5]**
  Streamed input is the default in the UI: raw PCM16 frames between `stream_start` and `stream_stop` (section 9.2a).
  A WAV frame (RIFF header) is still one turn, so both modes share the socket.
- `{"type":"interrupt"}` = barge-in: server cancels LLM + all in-flight TTS streams for the current turn and
  records the partial reply as interrupted.
- `{"type":"text","text":"..."}` = typed turn (same pipeline, no STT).
- `{"type":"config","voice":..,"role":..}` = change voice/role mid-session.
Server → client (in order per turn; `turn` increments per user turn):
```
{"type":"user_text","turn":3,"text":"...","stt_ms":180}
{"type":"agent_text","turn":3,"delta":"tokens"}                 (spoken reply text, streamed)
{"type":"audio","turn":3,"seq":0,"rate":24000,"pcm_b64":"..."}  (s16le mono; play in seq order)
{"type":"item","turn":3,"item":{Item}}                           (0..n per turn, after the spoken reply)
{"type":"turn_done","turn":3,"interrupted":false,"timing":{"stt_ms","llm_first_token_ms","tts_first_audio_ms","first_audio_ms","total_ms"}}
{"type":"error","turn":3,"stage":"stt|llm|tts","message":"..."}
```
In practice mode the session also pushes the normal call events (transcript.final, enrichment, card.*,
coverage) for its `call_id` on the same socket, so one UI can show both.

### 9.3 Item (structured outcome of a turn)
`{"item_id","session_id","turn","kind","text","owner"?,"due"?,"source_doc"?,"created_at_ms"}`
`kind ∈ action_item | follow_up_question | follow_up_response | next_step | summary | risk | note`.
Persisted to Lakebase table `voice_items` (and `voice_turns(session_id, turn, role, user_text, agent_text,
interrupted, timing JSON, created_at_ms)`), both added to the app DDL **and** `init.py` (parity test).

### 9.4 One LLM call per turn
The voice model streams the **spoken reply first**, then a delimiter line `<<ITEMS>>`, then a JSON array of
items. The app sends text before the delimiter to TTS sentence-by-sentence (split on `[.!?]\s+`; the first
chunk may end at `,;:—` once ≥ 25 chars; strip markdown/emoji; `VOICE_RULES` appended to the prompt, from
the reference app) and parses the JSON after it (tolerant; parse failure = no items, never breaks audio).
Each sentence's TTS stream starts as soon as the sentence is complete; playback order is preserved.
`VOICE_MODEL` env (default `databricks-claude-haiku-4-5` [v0.4], measured fastest first token on e2-demo; thinking
disabled where the model accepts it). Re-measure in region.

### 9.5 Targets
first audio (user stops speaking → first audible reply) ≤ 1.2 s p50 in-region; STT ≤ 250 ms; TTS first
audio ≤ 200 ms; barge-in stops playback ≤ 150 ms client-side and cancels server work ≤ 300 ms.
Browser: AudioWorklet capture, echoCancellation + noiseSuppression, energy VAD (start 200 ms, end 800 ms,
preroll 300 ms, barge-in 300 ms at 2× threshold) — ported from the reference `index.html`.

### 9.2a Streaming input **[v0.5]**
Client → server, on `/ws/voice/{session_id}`:
- `{"type":"stream_start","sample_rate":16000,"endpoint_silence_ms":700?}` opens a stream. Answer:
  `{"type":"stream_started","sample_rate":16000,"endpoint_silence_ms":700,"frame_ms":100}`. Only sample_rate 16000 is
  accepted; endpoint_silence_ms must be 300..2000 (default `VOICE_ENDPOINT_SILENCE_MS`, 700).
- Binary frames **without a RIFF header** are raw PCM16 mono 16 kHz, about 100 ms each, at most 1 s. They are stream
  frames while a stream is open. Without an open stream they are an error. The server never blocks the socket:
  when its frame queue is full (`VOICE_STREAM_QUEUE_MAX`, 300 frames), the frame is dropped and counted in
  `dropped_frames`.
- `{"type":"stream_stop","flush":true|false}`. `flush:true` finalises the open utterance into a turn (push-to-talk
  release). `flush:false` discards it (mute, switch to push-to-talk). Answer: `stream_stopped {flush, dropped_frames}`.
- Endpointing: silence of `endpoint_silence_ms` ends the utterance. The final starts the turn exactly as a WAV or typed
  turn: `user_text` (stt_ms = commit latency), then the reply, then `turn_done`. A continuous utterance is force-ended
  with silence after `VOICE_MAX_TURN_AUDIO_S` (30 s).
- Barge-in is still the client's `interrupt` (the VAD on the client decides it).

Server → client (same socket):
```
{"type":"user_partial","turn":n,"text":"...","seq":..,"first_word_ms":..}
```
`turn` is the turn the endpoint will start, so the caption and the reply share a number. Partials come from the same
StreamingSession as the call path (Parakeet rolling window, LocalAgreement-2).

### 9.7 Live analysis while the user talks **[v0.5]**
Two passes, both on partials and finals of the user (assistant mode) or the rep (practice mode).

1. **Rules, no network.** `enrich/rules.py` on the text. Partials are debounced (600 ms, `LIVE_DEBOUNCE_MS`) and
   analysed only with at least 4 new words (`LIVE_MIN_NEW_WORDS`). Finals are analysed at once and cancel a pending partial.
```
{"type":"live","turn":n,"speaker":"user|agent|customer","kind":"partial|final","sentiment":-1..1,"intent":"question|statement",
 "signals":["question","objection:fees","risk:unsupported_claim","method:budget"],"coverage":[{key,label,covered}]|null}
```
   Coverage is the playbook methodology over the **user's** speech in assistant mode. In practice mode it is `null`, because
   the call pipeline owns coverage. The rep's line is analysed as `agent` and the customer's reply as `customer`.
   Every analysis appends a point to the session's **sentiment series** (`t_ms, turn, speaker, kind, value`, the last 300).
2. **Suggestions, one fast-model call (fail-soft).** `LIVE_SUGGEST_INTERVAL_S` (2.5): at most one call per session per
   interval, and never two in flight. A final cancels a pass in flight and, if the interval is still closed, runs one pass
   at the first open slot. Timeout `LIVE_SUGGEST_TIMEOUT_S` (1.5). Failure produces nothing. The result is 0 to 2 items:
```
{"type":"suggestion","turn":n,"item":{Item, "provisional":true}}
{"type":"suggestion_retire","turn":n,"item_ids":[..],"reason":"superseded|turn_done"}
```
   Kinds are the Item kinds (section 9.3). A new pass supersedes the unpinned chips of the same turn. The end of the turn
   retires the rest, because the `<<ITEMS>>` list is authoritative.

Client → server:
- `{"type":"pin","item_id":..}`: the chip becomes an outcome item with the **same id**. It is persisted (`voice_items`)
  and sent as `{"type":"item"}`. An unknown or superseded id is an `error` event.
- `{"type":"dismiss","item_id":..}`: the chip is removed. Nothing is persisted.

`GET /api/voice/sessions/{id}` adds `sentiment` (the series), `suggestions` (live chips), `streaming` (bool) and
`dropped_frames`.

### 9.6 Limits, cold endpoints and observability **[v0.5]**
**Limits.** Each one is an env setting with a default.
- `VOICE_MAX_SESSION_MINUTES` (30). At the limit the socket gets `{"type":"session_ended","reason":"max_session_minutes"}`
  and closes.
- `VOICE_MAX_CONCURRENT_SESSIONS` (20) per app instance. Creating more voice sessions returns HTTP 429.
- `VOICE_MAX_TURN_AUDIO_S` (30) per turn, for WAV turns as well as streams (`VOICE_MAX_AUDIO_BYTES` is a hard byte cap).
- `VOICE_STREAM_QUEUE_MAX` (300 frames, 30 s). `VOICE_OUT_QUEUE_MAX` (2000): when a socket falls behind, only
  `user_partial`, `live` and `suggestion` are dropped. Turn events are never dropped.

**Cold endpoints (scale-to-zero).** An STT or TTS call that fails cold (timeout, refused or reset connection, HTTP 429,
503 or 504) is retried with exponential backoff (`VOICE_RETRY_BASE_S` 1, `VOICE_RETRY_CAP_S` 8) for up to
`VOICE_WARMUP_BUDGET_S` (90). Status events:
```
{"type":"status","stage":"stt|tts","state":"warming","message":"Warming up voice (GPU starting)…","turn":n}
{"type":"status","stage":"stt|tts","state":"ready","elapsed_ms":..,"turn":n}
{"type":"status","stage":"stt|tts","state":"failed","turn":n}     (budget spent; the turn gets an error event)
```
Streaming partials never retry, because a late partial is stale. Final decodes retry. TTS retries only before a sentence's
first audio chunk. Every other failure is final, as before.

**Readiness.** `GET /healthz` adds `voice.readiness`: `{endpoints: {stt|tts: {endpoint, state}}, ready, checked_at_ms,
cached}`. The state comes from the SDK's serving endpoint (`ready`, `not_ready`, or `unknown` when the probe fails or times
out). It is cached for `READINESS_TTL_S` (30). In local mode the state is `local` and nothing is probed. `voice.limits`
reports the limits above.

**Logs.** One JSON object per line on logger `sidekick.voice`: `{"event":"voice.<name>","session_id","turn",…}`. Events
include `stream_started`, `stream_stopped`, `turn_start`, `turn_done`, `stream_backpressure`, `limit_hit`, `session_closed`.
No transcript text at INFO or above.

## 3.7 History contract: Zerobus Ingest (direct backbone) **[v0.4]**

Module `src/app/history/`. The sink is the only writer of history in direct mode. Kafka mode writes history
through `delta_history.py` instead (utterances and cards only).

### Tables
Managed Delta tables in `${HISTORY_CATALOG}.${HISTORY_SCHEMA}`, created by `src/notebooks/init_history.py`.
Zerobus does not create tables. Columns follow SPEC 3.2 names. Epoch times are BIGINT milliseconds. JSON-valued
columns are STRING. Types are limited to STRING, INT, BIGINT, DOUBLE, BOOLEAN.

| Table | Written when | Columns |
|---|---|---|
| `calls` | call start, call end (latest `ended_at` wins) | call_id, playbook, started_at, ended_at, customer_id, status, mode, script |
| `utterances` | each final (both speakers) | call_id, seq, speaker, text, t_start_ms, t_end_ms, emitted_at_ms, playbook |
| `enrichments` | each final, after in-process enrichment | call_id, seq, speaker, text, intent, sentiment, risk_flags (JSON), objection, entities (JSON), methodology_hits (JSON), emitted_at_ms, enriched_at_ms, latency_ms, enrichment_source |
| `cards` | each card when persisted | card_id, call_id, seq, kind, title, body, confidence, source_doc, source_clause, trigger, status, created_at_ms, trace (JSON) |
| `feedback` | accept, reject, or edit | card_id, call_id, action, edited_text, at_ms |
| `voice_turns` | each voice turn | session_id, turn, role, user_text, agent_text, interrupted, timing (JSON), created_at_ms |
| `voice_items` | each structured item | item_id, session_id, turn, kind, text, owner, due, source_doc, created_at_ms |

`src/app/history/schema.py` (`TABLES`) is the source of truth. `tests/app/test_history.py` checks that the
notebook's DDL has the same columns and types.

### Ingest
- Zerobus Ingest, Python SDK `databricks-zerobus-ingest-sdk` (`zerobus.sdk.sync`). Endpoint
  `https://<workspace-id>.zerobus.<region>.cloud.databricks.com`, `unity_catalog_url` = workspace URL.
  Auth: service principal client id and secret (OAuth). JSON record type, one JSON string per record.
- Grants (init_history.py): `USE CATALOG`, `USE SCHEMA`, and `MODIFY, SELECT` on each table, for the principal.
- One stream per table, created lazily, closed and recreated on error.
- Non-blocking hot path: `emit()` is synchronous, appends to a bounded deque (`HISTORY_QUEUE_MAX`, drop-oldest,
  counted in `dropped`, warning logged). A background task flushes batches of up to `HISTORY_BATCH_MAX` records
  every `HISTORY_FLUSH_INTERVAL_S`, in worker threads.
- Acks: the sink waits on the last offset of each batch, then flushes. Acks mean durable, not yet queryable.
- Failures: the stream is closed and a failed batch is retried on a fresh stream with exponential backoff (`HISTORY_RETRY_ATTEMPTS`, default 3; `retried` counts), and requeued once at the front of the queue after the last attempt, then counted in `failed` [v0.6]. Acknowledged writes are at-least-once: a retry after a partial ack can duplicate rows, so dedupe downstream on the natural keys (calls.call_id, utterances/enrichments (call_id, seq), cards.card_id, feedback (card_id, at_ms), voice_turns (session_id, turn), voice_items.item_id).
- Fail-soft: missing endpoint, client id or secret, or host disables the sink (one warning; `skipped` counts).
  The call keeps running.
- Shutdown drains the queue up to `HISTORY_FLUSH_TIMEOUT_S`.

### Delivery guarantee
Zerobus is the low-latency path, and on its own it is best effort [v0.6]: acknowledged writes are at-least-once, and a
batch that still fails after one requeue is dropped and counted in `failed`. A full queue drops its oldest records.

Completeness comes from Lakebase [v0.7]. Lakebase holds every record under the same natural keys, so it is the source
of truth. `src/notebooks/reconcile_history.py` (serverless job `call_sidekick_reconcile_history`, every 15 minutes, UTC)
reads each Lakebase table through the registered UC catalog `${LAKEBASE_CATALOG}.public` and, per table:
- reads the rows in a watermark window (`updated` time at or after now minus 48 h; `cards` also includes cards with
  feedback in the window, since feedback changes `cards.status`; `calls` uses `GREATEST(started_at, ended_at)`).
  `full_sync=true` reads everything instead.
- takes one row per natural key (QUALIFY row_number), so the MERGE source has no duplicates;
- removes Delta duplicates for the keys in the window, then MERGEs by natural key: inserts missing rows and updates
  rows that differ (null-safe). `enrichments.enrichment_source` is set on insert ('direct') and never updated, since
  Lakebase has no such column.
- `dedupe_existing=true` (one-off) keeps one row per duplicated key across the whole table first.

Recovery point objective: a record reaches Delta within one schedule interval (15 min) plus the job runtime after it
reaches Lakebase, for changes inside the 48 h watermark. Older changes need a `full_sync` run. A record that Zerobus
dropped is restored by the next run. The job never deletes a key that is absent from Lakebase.

Live state is in Lakebase. History in Delta is for analytics and post-call scoring; with the reconcile job it is
complete to the recovery point above, but it is eventually complete, not queryable at the instant of the write.

Sources: https://docs.databricks.com/aws/en/ingestion/zerobus-ingest ,
https://docs.databricks.com/aws/en/ingestion/zerobus-message-blocking ,
https://github.com/databricks/zerobus-sdk/blob/main/python/README.md

## 10. Changes from v0.3 **[v0.4]** (summary)
- Two backbones, selected by `BACKBONE`. Direct is the default, with no Kafka and no RTM.
- In-process enrichment in direct mode. The `enrichments` row is still written to Lakebase, with `source` = `direct`.
- History is the `HistorySink`: Zerobus in direct mode, none in kafka mode.
- Serverless compute for direct-mode jobs. Classic compute only for kafka-mode RTM and the history job.
- GPU endpoints: serverless GPU (`GPU_1xA10`). Scale-to-zero configurable (`gpu_scale_to_zero`, default true).
- `VOICE_MODEL` default `databricks-claude-haiku-4-5`.
- Packaging: `resources/cluster.yml` is removed. The RTM cluster and Kafka jobs are in `targets.kafka`.

## 11. Changes from v0.4 **[v0.5]** (summary)
- Voice input streams: `stream_start`, PCM16 frames, `stream_stop` (section 9.2a). Partials (`user_partial`) appear
  while the user talks. The endpoint (700 ms of silence) starts the same reply turn as a WAV or typed turn. WAV turns
  are unchanged.
- Live analysis while the user talks: `live` events from the rules (sentiment, intent, signals, coverage) and provisional
  `suggestion` chips from one fast-model pass, rate-limited and fail-soft. Pin and dismiss (section 9.7). The sentiment
  series is in the snapshot.
- Production limits: session minutes, concurrent sessions (HTTP 429), turn audio, stream backpressure, outbound
  backpressure (section 9.6).
- Cold STT and TTS endpoints retry with backoff and emit `status` events. `/healthz` reports voice readiness (SDK,
  cached 30 s).
- Structured JSON voice logs keyed by session and turn. No transcript text at INFO.
- UI (not a contract change): the console shows a live caption, the live read (sentiment gauge and sparkline), the live
  lane, separate mic and Sidekick levels on the orb, a warming banner, and keys M (mute), Esc (interrupt) and Space
  (push-to-talk). `?stream=0` is the WAV-per-turn fallback. The mock (`?mock=1`) simulates all of it.

## 12. Changes from v0.5 **[v0.6]** (summary)
- **Ownership.** Calls and voice sessions record their owner (`X-Forwarded-Email`, then `X-Forwarded-User`; `local` in
  MODE=local). `calls.owner` is a nullable column in the Lakebase DDL and the history schema. Other callers get 404 on REST
  and close 4404 on WebSockets. A voice session cannot link a call owned by someone else.
- **Close codes.** 4400 bad request, 4403 no caller identity, 4404 not found, not yours or not active, 4409 superseded by a newer socket
  for the same session, 4410 the session ended (`session_ended` is sent first). Clients stop reconnecting on any 44xx code and
  after `session_ended`.
- **Voice socket `hello`.** The first event on `/ws/voice/{id}` is `{"type":"hello","session_id","role","call_id"}`. Events queued
  while no socket was attached are discarded at attach; a reconnect rebuilds from the snapshot.
- **Lifetime.** A reaper closes voice sessions with no socket after `VOICE_DETACHED_TTL_S` (90 s, armed at creation) and
  finishes calls with no attachment after `CALL_DETACHED_TTL_S` (120 s). Closed and ended entries are dropped after
  `SESSION_GRACE_S` (60 s). Practice sessions survive a socket blip for the same grace period.
- **Input limits.** Text (typed turns, browser STT, feedback) is capped at `MAX_TEXT_BYTES` (2048). Each socket has a token
  bucket (`WS_MSGS_PER_S`, default 50); excess messages are dropped and counted. `--ws-max-size 1048576` on uvicorn.
  Active calls are capped per user (5) and per instance (100), with 429 beyond.
- **Store degraded mode.** Lakebase unreachable at startup does not stop the app. The store retries with backoff, `/healthz`
  reports `status: degraded` and `store_ready: false`, and `GET /api/calls/{id}` serves in-memory calls with `degraded: true`.
  Queries time out after `LAKEBASE_COMMAND_TIMEOUT_S` (2 s), and connects after `LAKEBASE_CONNECT_TIMEOUT_S` (5 s).
- **Circuit breakers.** One per model (or endpoint). Warm-up pings do not count. `/healthz` shows the exception type of the last error only.
- **Queued turns** are superseded like running turns by a newer turn or an interrupt.
- **Zerobus.** Blocking writes are limited to `HISTORY_WRITE_TIMEOUT_S` (15 s). A batch that fails every attempt is requeued once.
- **Secrets.** `lakebase_password` is read from the secret scope only. It is no longer a job parameter or bundle variable.
  `ROTATE_PG_PASSWORD=1` rotates it and restarts the app.

