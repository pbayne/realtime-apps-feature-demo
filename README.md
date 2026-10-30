# Realtime Call Sidekick — Databricks side

Live call audio becomes sub-second transcripts, Kafka carries final utterances,
Structured Streaming **Real-Time Mode (RTM)** enriches each utterance, and a
Lakebase table plus Postgres `NOTIFY` push the result to the sidekick app. The
app turns it into suggestion cards for the agent or advisor. Contract:
[`docs/SPEC.md`](docs/SPEC.md). This README covers the Databricks half
(Workstream B): bundle, notebooks, jobs, the STT endpoint, and the batch jobs.

## Architecture

```mermaid
flowchart LR
  subgraph Browser / Audio
    MIC[Mic or replay] -->|PCM16 16 kHz 100 ms| APP
  end
  subgraph Databricks App (Workstream A)
    APP[FastAPI + WS UI + sidekick agent]
  end
  APP -->|VAD window <= 30 s, base64 PCM16| STT[(Parakeet STT<br/>route-optimized GPU endpoint)]
  STT -->|words + timestamps| APP
  APP -->|final utterances JSON| K1[[Kafka: utterances]]
  APP -->|cards JSON| K2[[Kafka: cards]]

  K1 -->|RTM job, update mode, realTime 5 min| RTM[rtm_enrich.py<br/>built-in rules, sentiment, entities]
  RTM -->|format postgresql, upsert call_id,seq| LB[(Lakebase Postgres)]
  LB -->|AFTER INSERT/UPDATE trigger<br/>pg_notify enrichment_ready| APP
  LB -->|live reads, writes| APP

  K1 -->|micro-batch 5 s| DH[delta_history.py]
  K2 --> DH
  DH --> BRZ[(UC bronze Delta<br/>bronze_utterances, bronze_cards)]
  BRZ -->|every 30 min, idle calls| PCS[post_call_scoring.py<br/>ai_query]
  PCS --> GOLD[(UC gold_call_scores)]

  KB[src/app/playbooks/kb/*.md] --> KBI[build_kb_index.py] --> VS[(Vector Search<br/>delta-sync, gte-large-en)]
  VS -->|top-3 retrieval| APP
```

ASCII summary of the Databricks path:

```
Kafka utterances (4 partitions, key=call_id)
   └─► RTM cluster (classic, 2 x m5d.xlarge, Photon off, DBR 18.3)
        readStream kafka → from_json → filter is_final → built-in enrich
        → writeStream format("postgresql") upsertkey=call_id,seq
        → trigger(realTime="5 minutes"), outputMode("update")
             └─► Lakebase enrichments ──trigger──► pg_notify('enrichment_ready') ──► app
Kafka utterances + cards ──► batch cluster ──► Delta bronze (5 s micro-batch)
bronze ──► ai_query scoring (30 min) ──► gold_call_scores
src/app/playbooks/kb ──► Delta chunks ──► Vector Search index (databricks-gte-large-en)
Parakeet pyfunc ──► UC model ──► route-optimized GPU endpoint (A10G, no scale-to-zero)
```

## Latency targets (SPEC section 7)

| Hop | Target | Where it is measured |
|---|---|---|
| STT first word | <= 0.8 s | app (`metrics.stt_first_word_ms`) |
| Final utterance → enrichment row (RTM) | <= 300 ms | `enrichments.latency_ms` = `enriched_at_ms - emitted_at_ms` |
| Customer final → card.start | <= 1.5 s | app |
| card.done | <= 3 s | app |

`latency_ms` is written for every enrichment row. Track its p50/p95 with a
query on `enrichments`. RTM's `realTime` trigger interval is a checkpoint
interval, not a processing delay, so it does not add to this number. See
[RTM overview](https://docs.databricks.com/aws/en/structured-streaming/real-time).

## Prerequisites

- Databricks workspace on AWS with Unity Catalog, Lakebase Autoscaling, Databricks
  Apps, and Model Serving (GPU capacity for `g5.xlarge`, A10G).
- A recent Databricks CLI (supports `bundle`, `apps`, `secrets`) with a profile for the target workspace (`databricks auth login --profile <name>`), `jq`, `openssl`, Python 3.11 with `uv` for tests.
- Permission to create clusters, UC schemas and volumes, secret scopes, jobs, apps, serving endpoints, and Vector Search endpoints.
- **Kafka**, one of:
  - **Confluent Cloud** (`SASL_SSL_PLAIN`). Create a cluster, a service-account API key with
    read/write on the topics, and the topics `utterances` and `cards` (4 partitions;
    the setup job will create them if missing). Store the bootstrap endpoint and key/secret in the scope.
  - **AWS MSK Serverless / IAM** (`SASL_SSL_MSK_IAM`). Use the IAM bootstrap string
    (port 9098). The cluster instance profile must allow `kafka-cluster:Connect`,
    `DescribeTopic`, `ReadData`, `WriteData`, `DescribeGroup`, `AlterGroup`. The Databricks Kafka auth doc
    also covers a UC service credential as an alternative.
    Note that the setup job's topic-creation step uses `aws-msk-iam-sasl-signer-python` on the driver. This path is **not yet exercised** (see Risks).
- Secret scope (created by `scripts/deploy.sh`) containing the Kafka values:

| Key | Mode | Value |
|---|---|---|
| `kafka_bootstrap` | all | Broker list (Confluent / Redpanda seed broker, or MSK IAM bootstrap string) |
| `kafka_security` | all | Written by `deploy.sh`; read by the app |
| `kafka_api_key` / `kafka_api_secret` | user/password modes | **Jobs** identity: read-only on `utterances`, `cards` (+ `spark-kafka-source*` groups) |
| `kafka_app_api_key` / `kafka_app_api_secret` | user/password modes | **App** identity: write-only on `utterances`, `cards` |

User/password modes are `SASL_SSL_PLAIN` (Confluent API key/secret) and
`SASL_SSL_SCRAM_SHA_256` / `_512` (Redpanda Cloud SCRAM users). Check connectivity and
least privilege with `scripts/kafka_smoke.py <env-file>`.

Lakebase keys (`lakebase_host`, `_db`, `_user`, `_password`, `_endpoint`) are
written by the setup job. Do not put them in git.

## Kafka and RTM compatibility

| Item | Confluent Cloud | MSK Serverless |
|---|---|---|
| RTM source | supported (Kafka) | supported as **source only** (docs) — fine here, since MSK is the source, not the sink |
| Auth | SASL_SSL PLAIN with `kafkashaded.` login module | SASL_SSL, `AWS_MSK_IAM`, `shadedmskiam.` classes |
| Topic creation | AdminClient, PLAIN | AdminClient, OAUTHBEARER (IAM signer) |
| Status here | the primary path | implemented, **untested** against a live cluster |

## Real-Time Mode restrictions (verified)

Checked against Databricks docs at the time of writing. Re-check before a
production release. The RTM overview and setup pages are current; some sections
of the RTM reference were only available through summaries. The `postgresql`
sink page returned 404 to the fetch tool, so its options were taken from a
search summary of the Lakebase structured-streaming docs.

| Constraint | Status | Source |
|---|---|---|
| Supported sources | Kafka, Event Hubs (Kafka API), Kinesis (source). **AWS MSK is source-only.** Delta, Pub/Sub, Pulsar are **not** supported. | [RTM reference](https://docs.databricks.com/aws/en/structured-streaming/real-time/reference) |
| Supported sinks | Kafka, Event Hubs, `forEach` (ForeachWriter). Native Lakebase `format("postgresql")` sink for Lakebase (DBR 18+). | RTM reference; [Lakebase sink](https://docs.databricks.com/aws/en/structured-streaming/lakebase) (search summary) |
| Output mode | `update` only. `append` and `complete` are not supported. | RTM reference |
| `foreachBatch` | **Not supported.** `forEach` is. This is why `rtm_enrich.py` uses the native sink. | RTM reference |
| Stateless ops | selection, projection, union supported. `mapPartitions` not. | RTM reference |
| UDFs | Scala UDFs with limits. Python stateless scalar/pandas/Arrow UDFs supported, with `maxRecordsPerBatch=1` advised for latency. **Not** used here: enrichment is built-in functions only (see Risks). | RTM reference |
| Aggregations | sum/count/min/max/avg and built-in aggregates supported. Not used by enrichment. | RTM reference |
| Joins | stream-to-table only with broadcast ≤ 10 MB. Not used here. | RTM reference |
| Delta source/sink | **Not supported** as RTM source or sink. Bronze writes are in `delta_history.py`, a separate micro-batch job. | RTM reference |
| Cluster | classic, Dedicated or Standard (Python) access mode. Runtime 16.4+ for Kafka RTM setup. Autoscaling off, Photon off, spot off. `spark.databricks.streaming.realTimeMode.enabled=true`. | [RTM setup](https://docs.databricks.com/aws/en/structured-streaming/real-time/setup) |
| Trigger | `.trigger(realTime="5 minutes")` in Python. Interval controls checkpointing, not delay. | RTM setup |
| Task slots | Total slots ≥ number of tasks across all stages. With a Kafka topic, default parallelism = partitions (4 here). | RTM setup |
| Serverless | Not supported for direct RTM queries. Serverless notebooks/jobs don't run this job. | RTM reference |
| Lakebase sink options | `endpoint` (`project.branch[.endpoint]`), `database`, `dbtable` (`schema.table`), `batchinterval` (default 100 ms), `batchsize` (default 1000), `upsertkey`, `checkpointLocation`. Upsert uses `ON CONFLICT` on the PK. | [Lakebase sink](https://docs.databricks.com/aws/en/structured-streaming/lakebase) (search summary) |
| Kafka auth options | `kafka.security.protocol`, `kafka.sasl.mechanism`, `kafka.sasl.jaas.config`, `kafka.sasl.client.callback.handler.class` (MSK IAM). Shaded classes: `kafkashaded.` (PLAIN), `shadedmskiam.` (IAM). | [Kafka authentication](https://docs.databricks.com/aws/en/connect/streaming/kafka/authentication) (search summary) |

## Design choices

- **Built-in functions only in the RTM job.** `rlike`, `regexp_extract_all`,
  `filter`/`size` lambdas, `from_json`, `to_json`. A test
  (`tests/notebooks/test_enrich_rules.py::test_enrichment_plan_is_streaming_and_has_no_python_udf_or_aggregation`)
  fails if the analyzed plan gains a Python UDF, aggregate, join, or window.
- **Shared rules file.** `src/notebooks/enrich_rules.json` is the single source. The RTM job loads it
  with `rtm_lib.load_rules()`. Workstream A should load the same file in `src/app/enrich`
  so the local stand-in and the RTM job apply identical rules.
- **Upsert on PK.** `upsertkey=call_id,seq` matches the table's primary key, which the docs require.
- **JSON-valued enrichment columns are `TEXT`**, not `JSONB`. The sink binds Spark strings;
  an assignment from text to `jsonb` would fail without a cast. Switch if the sink's type mapping is confirmed.
- **Two clusters.** The RTM cluster runs only the RTM job. Batch and streaming-to-Delta
  jobs run on separate job clusters, so they never take RTM task slots.
- **Fixed-cadence delta history.** `delta_history.py` uses `toTable()` on a plain
  micro-batch stream with `processingTime="5 seconds"` (or `availableNow` for backfill). It has no `foreachBatch`.
- **GPU audio endpoints, Custom LLM Serving.** Parakeet (STT) and Kokoro (TTS) are logged with the
  `llm/v1/audio/transcriptions` and `llm/v1/chat` tasks and a FastAPI entrypoint, following the field-eng
  audio-serving reference. Neither is route-optimized: the app calls them through the workspace host with its
  service principal. The app declares `CAN_QUERY` on each as an app resource.

## Parakeet STT endpoint (contract v0.3, SPEC 3.3)

Deployed by `src/notebooks/register_parakeet.py` from `src/notebooks/parakeet_serving/asr_server.py`
(NeMo, bf16, 5 ms micro-batching). Endpoint `sidekick-parakeet` by default, `GPU_MEDIUM`, scale-to-zero off.

Request: multipart `POST {host}/serving-endpoints/{STT_ENDPOINT}/audio/transcriptions` with
`file` (16 kHz mono PCM16 WAV, ≤ 30 s), `model` (the endpoint name), `response_format=verbose_json`.

Response: `{"duration", "text", "segments": [{"start","end","text"}], "words": [{"word","start","end"}]}`.
Times are seconds relative to the clip. The app converts them: `start_ms = offset_ms + round(start * 1000)`.

Auth is the app's service principal (`LlmClient.multipart`, the same workspace auth as the other calls).
A failed window yields no words for the call path, and a failed turn yields a voice error event.

The call path's rolling-window decoder (`stt/parakeet.py`, SPEC 1) sends 12 s windows every 0.4 s. Voice mode
sends one WAV per user turn through `ParakeetTranscriber`.

For local development against the e2-demo workspace, `STT_PROTOCOL=whisper_json` points voice mode at the
Databricks-hosted `shared-whisper` endpoint. It takes `{"inputs": [base64 WAV]}` on `/invocations` (not multipart)
and returns `predictions` as strings. That switch applies to voice mode only.

## Voice mode (SPEC 9)

Two-way voice inside the same App, using only Databricks-hosted models: Parakeet (STT), Claude on Databricks
serving (the voice model), and Kokoro-82M (TTS).

- **assistant**: the rep talks to Sidekick before, during or after a call. Context is the linked call's transcript,
  coverage and cards (if linked), plus playbook KB hits.
- **practice**: Sidekick plays the customer from a playbook `practice:` scenario (persona, opening line, objections,
  goals). The rep's turns and the customer's replies run through a normal call session, so enrichment, decide,
  cards and coverage run live and their events arrive on the voice socket too.

Endpoints: `POST /api/voice/sessions`, `GET /api/voice/sessions/{id}`, `GET /api/voices`, `WS /ws/voice/{id}`.
`GET /api/playbooks` lists the scenarios. Each binary WebSocket frame is one complete 16 kHz mono PCM16 WAV turn.

One LLM call per turn: the spoken reply streams first, then `<<ITEMS>>`, then a JSON array of structured items
(`action_item`, `next_step`, `summary`, and so on), persisted to `voice_items`. Each turn is persisted to
`voice_turns`. Sentences go to Kokoro as they complete, and audio is emitted in sentence order.

Configuration (env, defaults in `src/app/config.py`): `VOICE_MODEL=databricks-claude-haiku-5-5` (thinking disabled),
`TTS_ENDPOINT=sidekick-kokoro`, `TTS_VOICE=af_heart`, `STT_ENDPOINT=sidekick-parakeet`.

Local development against e2-demo: `STT_PROTOCOL=whisper_json` uses the Databricks-hosted `shared-whisper` endpoint
as the STT stand-in. Kokoro is not deployed there, so local TTS needs a deployed `sidekick-kokoro` (or
`TTS_ENDPOINT` pointed at one).

Measured on e2-demo (2026-10-29): see the report notes in the PR description. Targets are in SPEC 9.5.

## Repository layout

```
databricks.yml                 bundle variables and the dev target (no host)
resources/
  cluster.yml                  RTM cluster (classic, fixed workers, RTM conf)
  lakebase.yml                 Lakebase project, UC schema, checkpoint volume
  job.yml                      setup, rtm_enrich, delta_history, kb_index, post_call_scoring, parakeet_register, kokoro_register
  app.yml                      Databricks App and its secret and serving-endpoint resources
src/notebooks/
  init.py                      Lakebase role, SPEC §3 tables, NOTIFY trigger, Kafka topics, secrets
  rtm_lib.py                   shared pure functions (rules, enrichment expressions, Kafka options)
  rtm_enrich.py                Kafka → RTM → native postgresql sink
  delta_history.py             Kafka → Delta bronze (micro-batch, not RTM)
  register_parakeet.py         Parakeet audio serving (Custom LLM task) → UC → GPU endpoint
  register_kokoro.py           Kokoro-82M TTS serving (chat task, 4 workers) → UC → GPU endpoint
  parakeet_serving/, kokoro_serving/   server code the two notebooks log with their models
  build_kb_index.py            src/app/playbooks/kb → Delta → Vector Search index
  post_call_scoring.py         ai_query scoring → gold_call_scores
  enrich_rules.json            shared deterministic rules
scripts/deploy.sh              preflight, bundle validate/deploy, ordered runs
tests/notebooks/               pytest: rules, enrichment (local Spark), resources YAML
```

## Deploy

Deployment is one command. It needs the Kafka secrets in the scope, which are created out of band:

```bash
export DATABRICKS_CONFIG_PROFILE=<profile>
# Kafka secrets first (values never go in git)
databricks secrets create-scope call-sidekick
databricks secrets put-secret call-sidekick kafka_bootstrap
databricks secrets put-secret call-sidekick kafka_api_key          # jobs user (not MSK IAM)
databricks secrets put-secret call-sidekick kafka_api_secret
databricks secrets put-secret call-sidekick kafka_app_api_key      # app user (not MSK IAM)
databricks secrets put-secret call-sidekick kafka_app_api_secret

scripts/deploy.sh <profile> <catalog> SASL_SSL_SCRAM_SHA_256  # Redpanda; or SASL_SSL_PLAIN / SASL_SSL_MSK_IAM
DEPLOY_VOICE=1 scripts/deploy.sh <profile> <catalog> SASL_SSL_PLAIN   # also registers Parakeet and Kokoro (GPU)
```

Manual equivalent, in order:

```bash
databricks bundle validate --strict --target dev --profile <profile>
databricks bundle deploy --target dev --profile <profile>
databricks bundle run call_sidekick_setup --target dev        # Lakebase + topics + secrets
databricks bundle run call_sidekick_app --target dev          # after the secrets are real
databricks bundle run call_sidekick_kb_index --target dev     # needs src/app/playbooks/kb/*.md
databricks bundle run call_sidekick_rtm_enrich --target dev --no-wait
databricks bundle run call_sidekick_delta_history --target dev --no-wait
databricks bundle run call_sidekick_parakeet_register --target dev   # once, GPU quota needed
databricks bundle run call_sidekick_kokoro_register --target dev      # voice mode only
```

Then restart the app once so it reads the real Lakebase secrets:
`databricks apps stop call-sidekick && databricks apps start call-sidekick`.

**Stop the RTM job:** `databricks jobs list-runs --active-only`, then
`databricks jobs cancel-run <run_id>`. The job has no timeout by design.

## Secrets and env (app)

Workstream A declares env in `src/app/app.yaml` with `valueFrom` against the
resource names in `resources/app.yml`. Values never live in git.

| Env var | Resource name |
|---|---|
| `LAKEBASE_HOST`, `LAKEBASE_DB`, `LAKEBASE_USER`, `LAKEBASE_PASSWORD` | `lakebase-host`, `lakebase-db`, `lakebase-user`, `lakebase-password` |
| `KAFKA_BOOTSTRAP`, `KAFKA_API_KEY`, `KAFKA_API_SECRET` | `kafka-bootstrap`, `kafka-api-key`, `kafka-api-secret` |
| `STT_ENDPOINT`, `FAST_MODEL`, `SMART_MODEL`, `EMBED_MODEL` | `parakeet-stt`, `fast-llm`, `smart-llm`, `embedding` (defaults in `src/app/config.py`: `sidekick-parakeet`, `databricks-claude-haiku-5-5`, `databricks-claude-sonnet-5-5`, `databricks-gte-large-en`) |
| `TTS_ENDPOINT`, `VOICE_MODEL`, `TTS_VOICE` | `kokoro-tts`, `voice-llm`, plain `af_heart` (defaults: `sidekick-kokoro`, `databricks-claude-haiku-5-5`) |

For MSK IAM, delete the two `kafka-api-*` resources in `resources/app.yml`.

## Cost notes

- **RTM cluster.** Two `m5d.xlarge` workers plus a driver, on-demand, running the
  whole time the RTM job runs. The 120-minute autotermination should not fire while the streaming job is active (confirm on first run).
  Stop the job when no calls are live.
- **Lakebase.** `no_suspension: true` at 0.5–2 CU. A warm endpoint is required:
  a cold start on the sink adds latency to the first enrichment of each burst. Expect
  continuous charges.
- **GPU endpoints.** `GPU_MEDIUM` (A10G), `scale_to_zero_enabled=false`, always on. Parakeet is the largest
  fixed cost. Scale-to-zero would break the STT first-word budget.
- **Voice mode adds a second always-on A10G.** Kokoro (`sidekick-kokoro`) runs continuously, even when no one
  uses voice. It is the second fixed GPU cost on top of Parakeet, so a voice-only deployment is about two
  always-on A10G endpoints. Delete the Kokoro endpoint, or skip `DEPLOY_VOICE=1`, to remove it.
- **Batch.** Post-call scoring runs every 30 minutes on a job cluster. It is idle-cheap.
  Check current pricing before a demo; this README does not quote dollar figures.

## Risks and open items

1. **`postgresql` sink details.** The docs page 404'd on direct fetch. Options and
   the `endpoint` format come from a search summary. Verify on DBR 18.3 with a
   tiny test that `upsertkey` works and that `TEXT` JSON columns accept Spark strings.
2. **Python-regex vs Java-regex.** The rule patterns are common syntax. Spark
   uses Java regex. Tests compile with Python `re` and run the rules under Spark's
   Java engine. Keep patterns simple; add a Java check if rules get complex.
3. **Sentiment tokenization.** The lexicon matches single tokens only. Multi-word
   phrases ("not interested") won't score. The app's LLM perception covers those.
4. **MSK IAM** driver-side topic creation (`aws-msk-iam-sasl-signer-python`) is not exercised.
5. **Parakeet.** Not validated on real audio. The NeMo transcribe timestamp output
   shape (`hyp.timestamp["word"]`) and the `.nemo` filename on Hugging Face are from memory.
   Run `register_parakeet.py` once and check the smoke test output. Model quality and
   first-word latency need a benchmark on the chosen GPU.
6. **Route-optimized creation via SDK.** `route_optimized=True` and `data_plane_info`
   field names are from memory. The notebook prints the response so they can be checked.
7. **Model and runtime strings.** `gpu_spark_version` (`17.3.x-gpu-ml-scala2.13`) is a
   placeholder. Confirm with `databricks clusters spark-versions`.
8. **Vector Search app access.** Confirm the app's service principal can query the index
   (UC `SELECT` on the index's source table plus the endpoint CAN_USE). The app resource list has
   no UC securable entry for the index yet.
9. **Enrichment columns beyond SPEC §3.** `latency_ms`, `emitted_at_ms`, and the
   `entities` rule group are additions. These need to be reflected in `docs/SPEC.md` by its owner;
   Workstream B was not permitted to edit SPEC.
10. **Checkpoint compatibility.** Changing the RTM query shape (column list, sink options) can
    require a new checkpoint path. Rename `rtm_enrich` in `resources/job.yml` to get one.
11. **Single user.** SINGLE_USER access mode runs as the deploying user. The app's
    service principal is separate. Acceptable for a demo; for production use a service principal.

## Tests

```bash
# App (local mode, no network, LLM disabled by conftest)
.venv/bin/python -m pytest tests/app -q

# Notebooks: rules, Spark expressions, bundle YAML. Spark tests skip when pyspark is absent.
uv venv /tmp/cc-venv --python 3.11
VIRTUAL_ENV=/tmp/cc-venv uv pip install -r tests/notebooks/requirements-dev.txt
/tmp/cc-venv/bin/python -m pytest tests/notebooks -q

# Frontend (use bun; the Homebrew node install is broken on the dev laptop)
cd frontend && bun run typecheck && bun run build      # build writes src/app/static

# Lint (ruff.toml: pyflakes, E4/E7/E9, import order)
.venv/bin/python -m ruff check src/app tests/app tests/notebooks
```

CI (`.github/workflows/quality.yml`) runs the same three suites and fails if the committed
`src/app/static` is stale.

The Spark tests need Java 17 and the stock PyPI `pyspark==3.5.1`. A patched local pyspark
that only supports Databricks Connect will not start a local session.
