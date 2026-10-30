# Databricks notebook source
# MAGIC %md
# MAGIC # Call sidekick — bootstrap (idempotent)
# MAGIC
# MAGIC One-shot task, safe to re-run. It does four things:
# MAGIC
# MAGIC 1. Resolves the Lakebase endpoint, mints an admin Postgres credential, and
# MAGIC    creates or rotates the app/writer role.
# MAGIC 2. Creates the SPEC section 3 tables in Lakebase (`calls`, `utterances`,
# MAGIC    `enrichments`, `cards`, `feedback`) plus the `pg_notify('enrichment_ready')`
# MAGIC    trigger on `enrichments`.
# MAGIC 3. Creates the Kafka topics (`utterances`, `cards`) if they are missing (**backbone=kafka only**),
# MAGIC    using an AdminClient. Auth is SASL_SSL PLAIN (Confluent Cloud) or AWS MSK IAM.
# MAGIC 4. Writes the Lakebase connection secrets to the secret scope so the app and
# MAGIC    the RTM job can read them.
# MAGIC
# MAGIC Kafka credentials are created out of band and stored in the scope (see README):
# MAGIC - `SASL_SSL_PLAIN` (Confluent) or `SASL_SSL_SCRAM_SHA_256` / `_512` (Redpanda Cloud):
# MAGIC   `kafka_bootstrap`, `kafka_api_key` (username), `kafka_api_secret` (password)
# MAGIC - `SASL_SSL_MSK_IAM`: `kafka_bootstrap` (the IAM bootstrap string). No key needed.
# MAGIC
# MAGIC The database role password is never printed. It is read from the `lakebase_password`
# MAGIC secret in the scope, which `scripts/deploy.sh` writes once (or on `ROTATE_PG_PASSWORD=1`).

# COMMAND ----------

# MAGIC %md ## Install dependencies

# COMMAND ----------

# MAGIC %pip install --quiet "pg8000==1.31.2" "scramp==1.4.6" "confluent-kafka>=2.5,<3" "aws-msk-iam-sasl-signer-python>=1.0,<2"

# COMMAND ----------

# MAGIC %md ## 1. Configuration via widgets
# MAGIC
# MAGIC Defaults are for interactive runs. The bundle passes real values via `base_parameters`.

# COMMAND ----------

DEFAULTS = {
    "project_id": "call-sidekick",
    "branch_name": "production",
    "database_name": "databricks_postgres",
    "pg_role": "sidekick_writer",
    "secret_scope": "call-sidekick",
    "backbone": "direct",                   # direct (no Kafka; default, serverless OK) | kafka (classic cluster + topics)
    "kafka_security": "SASL_SSL_PLAIN",     # SASL_SSL_PLAIN | SASL_SSL_SCRAM_SHA_256 | SASL_SSL_SCRAM_SHA_512 | SASL_SSL_MSK_IAM
    "kafka_topics": "utterances,cards",
    "kafka_partitions": "4",
    "kafka_replication_factor": "3",
    "aws_region": "us-east-1",              # MSK IAM only
}
for k, v in DEFAULTS.items():
    dbutils.widgets.text(k, v)
p = {k: dbutils.widgets.get(k) for k in DEFAULTS}

import re

if p["backbone"] not in ("direct", "kafka"):
    raise ValueError(f"backbone must be direct or kafka, got {p['backbone']!r}")

# Used by both backbones: the Kafka admin client (kafka) and the Lakebase secrets step (always).
SCOPE = p["secret_scope"]

for field in ("project_id", "branch_name", "database_name", "pg_role", "secret_scope"):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_-]{0,62}", p[field]):
        raise ValueError(f"Invalid {field}: expected a simple identifier, got {p[field]!r}")

# Role password: from the secret scope (written by scripts/deploy.sh). Never printed.
role_password = dbutils.secrets.get(SCOPE, "lakebase_password")
if len(role_password) < 24:
    raise ValueError(f"lakebase_password in scope '{SCOPE}' must be at least 24 characters; "
                     "run scripts/deploy.sh with ROTATE_PG_PASSWORD=1 to regenerate it")

# COMMAND ----------

# MAGIC %run ./rtm_lib

# COMMAND ----------

if p["kafka_security"] not in KAFKA_SECURITY_MODES:
    raise ValueError(f"kafka_security must be one of {KAFKA_SECURITY_MODES}, got {p['kafka_security']!r}")

# COMMAND ----------

# MAGIC %md ## 2. Workspace client + REST helper

# COMMAND ----------

import requests
from databricks.sdk import WorkspaceClient
from pg8000 import dbapi

w = WorkspaceClient()
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
workspace_url = ctx.apiUrl().get()
headers = {"Authorization": f"Bearer {ctx.apiToken().get()}"}


def post(path, body):
    r = requests.post(f"{workspace_url}{path}", headers=headers, json=body, timeout=30)
    r.raise_for_status()
    return r.json() if r.content else None

# COMMAND ----------

# MAGIC %md ## 3. Resolve the Lakebase endpoint

# COMMAND ----------

branch_path = f"projects/{p['project_id']}/branches/{p['branch_name']}"
resp = requests.get(f"{workspace_url}/api/2.0/postgres/{branch_path}/endpoints",
                    headers=headers, timeout=30)
resp.raise_for_status()
payload = resp.json()
endpoints = payload if isinstance(payload, list) else payload.get("endpoints", [])
if not endpoints:
    raise RuntimeError(f"No Postgres endpoints under {branch_path}; project still provisioning?")
endpoint = endpoints[0]
lakebase_host = endpoint["status"]["hosts"]["host"]
endpoint_name = endpoint["name"]
print(f"Endpoint: {endpoint_name}")
print(f"Host:     {lakebase_host}")

# COMMAND ----------

# MAGIC %md ## 4. Mint admin Postgres credential
# MAGIC
# MAGIC Lakebase only accepts a short-lived OAuth JWT minted via this endpoint as the
# MAGIC Postgres password. The notebook session token is rejected.

# COMMAND ----------

admin_pg_token = post("/api/2.0/postgres/credentials", {"endpoint": endpoint_name})["token"]

# COMMAND ----------

# MAGIC %md ## 5. Role, grants, SPEC section 3 tables, NOTIFY trigger
# MAGIC
# MAGIC Each statement runs on its own. The pg8000 extended protocol rejects
# MAGIC multi-statement strings when parameters are absent. DDL can't take bind
# MAGIC parameters, so the role password is inlined as an escaped SQL literal.
# MAGIC Every statement is idempotent.

# COMMAND ----------

role = p["pg_role"]
db = p["database_name"]
pw_lit = "'" + role_password.replace("'", "''") + "'"

TABLE_DDL = [
    """CREATE TABLE IF NOT EXISTS calls (
        call_id     TEXT PRIMARY KEY,
        playbook    TEXT NOT NULL,
        started_at  BIGINT NOT NULL,          -- epoch ms, like every other *_ms column
        ended_at    BIGINT,
        customer_id TEXT,
        status      TEXT NOT NULL DEFAULT 'active',
        mode        TEXT,                     -- replay | mic
        script      TEXT,                     -- replay script name
        owner       TEXT                      -- the user who created the call (SPEC 10)
    )""",
    # Existing deployments: the owner column is newer than the table (idempotent).
    "ALTER TABLE calls ADD COLUMN IF NOT EXISTS owner TEXT",
    """CREATE TABLE IF NOT EXISTS utterances (
        call_id      TEXT NOT NULL,
        seq          INTEGER NOT NULL,
        speaker      TEXT NOT NULL,
        text         TEXT NOT NULL,
        t_start_ms   BIGINT,
        t_end_ms     BIGINT,
        emitted_at_ms BIGINT,
        playbook     TEXT,
        PRIMARY KEY (call_id, seq)
    )""",
    # Written by the RTM job via the native sink (upsert on call_id,seq).
    # JSON-valued columns are TEXT holding JSON, not JSONB: the sink binds
    # Spark strings, and JSONB would need an explicit cast.
    # latency_ms and emitted_at_ms are additions to SPEC section 3 (see README).
    """CREATE TABLE IF NOT EXISTS enrichments (
        call_id          TEXT NOT NULL,
        seq              INTEGER NOT NULL,
        speaker          TEXT,
        text             TEXT,
        intent           TEXT,
        sentiment        DOUBLE PRECISION,
        risk_flags       TEXT,
        objection        TEXT,
        entities         TEXT,
        methodology_hits TEXT,
        emitted_at_ms    BIGINT,
        enriched_at_ms   BIGINT,
        latency_ms       BIGINT,
        PRIMARY KEY (call_id, seq)
    )""",
    """CREATE TABLE IF NOT EXISTS cards (
        card_id       TEXT PRIMARY KEY,
        call_id       TEXT NOT NULL,
        seq           INTEGER,
        kind          TEXT,
        title         TEXT,
        body          TEXT,
        confidence    DOUBLE PRECISION,
        source_doc    TEXT,
        source_clause TEXT,
        "trigger"     TEXT,
        status        TEXT NOT NULL DEFAULT 'shown'
                      CHECK (status IN ('shown','accepted','edited','rejected','expired')),
        created_at_ms BIGINT,
        trace         TEXT
    )""",
    """CREATE TABLE IF NOT EXISTS feedback (
        card_id     TEXT NOT NULL,
        call_id     TEXT NOT NULL,
        action      TEXT NOT NULL CHECK (action IN ('accept','reject','edit')),
        edited_text TEXT,
        at_ms       BIGINT
    )""",
    # Voice mode (SPEC 9.3): one row per user turn, and the structured items it produced.
    """CREATE TABLE IF NOT EXISTS voice_turns (
        session_id    TEXT NOT NULL,
        turn          INTEGER NOT NULL,
        role          TEXT,
        user_text     TEXT,
        agent_text    TEXT,
        interrupted   BOOLEAN NOT NULL DEFAULT FALSE,
        timing        TEXT,
        created_at_ms BIGINT,
        PRIMARY KEY (session_id, turn)
    )""",
    """CREATE TABLE IF NOT EXISTS voice_items (
        item_id       TEXT PRIMARY KEY,
        session_id    TEXT NOT NULL,
        turn          INTEGER,
        kind          TEXT,
        text          TEXT,
        owner         TEXT,
        due           TEXT,
        source_doc    TEXT,
        created_at_ms BIGINT
    )""",
    # Per-user preferences (the app's theme choice). One row per owner (SPEC 14, admin area).
    """CREATE TABLE IF NOT EXISTS user_prefs (
        owner         TEXT PRIMARY KEY,
        theme         TEXT NOT NULL,
        updated_at_ms BIGINT NOT NULL
    )""",
    # Actions (SPEC 12): proposed and executed actions. params, result are JSON held as TEXT.
    """CREATE TABLE IF NOT EXISTS actions (
        action_id     TEXT PRIMARY KEY,
        session_id    TEXT,
        call_id       TEXT,
        playbook      TEXT,
        owner         TEXT NOT NULL,
        kind          TEXT NOT NULL,
        summary       TEXT,
        params        TEXT,
        status        TEXT NOT NULL,
        proposed_by   TEXT,
        created_at_ms BIGINT NOT NULL,
        updated_at_ms BIGINT,
        decided_at_ms BIGINT,
        decided_by    TEXT,
        result        TEXT,
        error         TEXT
    )""",
    # Contacts (SPEC 12). owner NULL = the shared demo directory every user can read (seeded below).
    """CREATE TABLE IF NOT EXISTS contacts (
        contact_id    TEXT PRIMARY KEY,
        owner         TEXT,
        first_name    TEXT,
        last_name     TEXT,
        email         TEXT,
        phone         TEXT,
        company       TEXT,
        updated_at_ms BIGINT
    )""",
    """CREATE TABLE IF NOT EXISTS enrollments (
        enrollment_id TEXT PRIMARY KEY,
        contact_id    TEXT NOT NULL,
        program       TEXT NOT NULL,
        owner         TEXT,
        note          TEXT,
        created_at_ms BIGINT NOT NULL
    )""",
    "CREATE INDEX IF NOT EXISTS cards_call_seq_idx ON cards (call_id, seq)",
    "CREATE INDEX IF NOT EXISTS feedback_card_idx ON feedback (card_id)",
    # Trigger: the app LISTENs on enrichment_ready and pushes `enrichment` events.
    """CREATE OR REPLACE FUNCTION notify_enrichment_ready() RETURNS trigger AS $$
    BEGIN
        PERFORM pg_notify('enrichment_ready', NEW.call_id || ':' || NEW.seq::text);
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql""",
    "DROP TRIGGER IF EXISTS enrichments_notify ON enrichments",
    """CREATE TRIGGER enrichments_notify
        AFTER INSERT OR UPDATE ON enrichments
        FOR EACH ROW EXECUTE FUNCTION notify_enrichment_ready()""",
]

# Shared demo directory for the action executors (SPEC 12). Fictitious people on example.com; idempotent.
DEMO_CONTACTS = [
    ("demo-dana-lee", "Dana", "Lee", "dana.lee@example.com", "+1 555 010 0101", "Lee Family Trust"),
    ("demo-marcus-chen", "Marcus", "Chen", "marcus.chen@example.com", "+1 555 010 0102", "Chen Consulting"),
    ("demo-priya-raman", "Priya", "Raman", "priya.raman@example.com", "+1 555 010 0103", "Raman Dental Group"),
]

with dbapi.connect(
    host=lakebase_host, port=5432, database=db,
    user=w.current_user.me().user_name, password=admin_pg_token,
    ssl_context=True,
) as conn:
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (role,))
    verb = "ALTER" if cur.fetchone() else "CREATE"
    cur.execute(f'{verb} ROLE "{role}" WITH LOGIN PASSWORD {pw_lit}')
    for stmt in [
        f'GRANT CONNECT ON DATABASE "{db}" TO "{role}"',
        f'GRANT USAGE, CREATE ON SCHEMA public TO "{role}"',
    ] + TABLE_DDL:
        cur.execute(stmt)
    # Table-level grants for both the sink writer and the app (same role in this build).
    for t in ("calls", "utterances", "enrichments", "cards", "feedback", "voice_turns", "voice_items", "user_prefs",
              "actions", "contacts", "enrollments"):
        cur.execute(f'GRANT SELECT, INSERT, UPDATE, DELETE ON {t} TO "{role}"')
    cur.execute(f'GRANT EXECUTE ON FUNCTION notify_enrichment_ready() TO "{role}"')
    # Demo directory for the actions (SPEC 12): idempotent, owner NULL, so every user can read and update a copy.
    for contact_id, first, last, email, phone, company in DEMO_CONTACTS:
        cur.execute(
            "INSERT INTO contacts (contact_id, owner, first_name, last_name, email, phone, company, updated_at_ms) "
            "VALUES (%s, NULL, %s, %s, %s, %s, %s, 1767225600000) ON CONFLICT (contact_id) DO NOTHING",
            (contact_id, first, last, email, phone, company),
        )
print(f"Role {role} ready; SPEC section 3 tables, indexes, and enrichment_ready trigger in place.")

# COMMAND ----------

# MAGIC %md ## 6. Kafka topics (create if missing; backbone=kafka only)
# MAGIC
# MAGIC Topic names and partition counts are variables. `utterances` should keep 4
# MAGIC partitions to match the RTM sizing in `resources/cluster.yml`. Existing topics
# MAGIC are left alone: partition count is never reduced or changed.

# COMMAND ----------

if p["backbone"] != "kafka":
    print("backbone=direct: no Kafka topics to create (history goes to Zerobus)")
else:
    from confluent_kafka import KafkaException
    from confluent_kafka.admin import AdminClient, NewTopic

    bootstrap = dbutils.secrets.get(SCOPE, "kafka_bootstrap")


    def msk_oauth_cb(_config):
        # Driver instance profile supplies the credentials (no key material in the scope).
        from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
        token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(p["aws_region"])
        return token, expiry_ms / 1000.0


    if kafka_needs_credentials(p["kafka_security"]):
        admin_conf = {
            "bootstrap.servers": bootstrap,
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": kafka_sasl_mechanism(p["kafka_security"]),
            "sasl.username": dbutils.secrets.get(SCOPE, "kafka_api_key"),
            "sasl.password": dbutils.secrets.get(SCOPE, "kafka_api_secret"),
        }
    else:
        admin_conf = {
            "bootstrap.servers": bootstrap,
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "OAUTHBEARER",
            "oauth_cb": msk_oauth_cb,
        }

    admin = AdminClient(admin_conf)
    existing = set(admin.list_topics(timeout=30).topics.keys())
    to_create = [t for t in topic_names(p["kafka_topics"]) if t not in existing]
    if to_create:
        futures = admin.create_topics([
            NewTopic(t, num_partitions=int(p["kafka_partitions"]),
                     replication_factor=int(p["kafka_replication_factor"]))
            for t in to_create
        ])
        for topic, fut in futures.items():
            try:
                fut.result()
                print(f"created topic {topic}")
            except KafkaException as exc:
                # Racing creators are fine; anything else is a real failure.
                if "TOPIC_ALREADY_EXISTS" not in str(exc):
                    raise
    print("topics present: " + ", ".join(sorted(t for t in topic_names(p["kafka_topics"]))))

# COMMAND ----------

# MAGIC %md ## 7. Refresh Lakebase connection secrets
# MAGIC
# MAGIC - `lakebase_endpoint` (`project_id.branch_id`) is the native sink's `endpoint` option.
# MAGIC - `lakebase_password` is not written here: it is the source of the role password above and is
# MAGIC   written by `scripts/deploy.sh` only.
# MAGIC - The other four are for password auth in the Databricks App, which runs
# MAGIC   in its own security context.

# COMMAND ----------

lakebase_endpoint = f"{p['project_id']}.{p['branch_name']}"
for key, val in [("lakebase_host", lakebase_host),
                 ("lakebase_db", db),
                 ("lakebase_user", role),
                 ("lakebase_endpoint", lakebase_endpoint)]:
    post("/api/2.0/secrets/put", {"scope": SCOPE, "key": key, "string_value": val})
# The app reads one bundled JSON secret (Databricks Apps allow at most 20 app resources); jobs read the keys above.
import json as _json
post("/api/2.0/secrets/put", {"scope": SCOPE, "key": "lakebase_config", "string_value": _json.dumps(
    {"host": lakebase_host, "db": db, "user": role, "password": role_password, "endpoint": lakebase_endpoint})})
print(f"Wrote 4 Lakebase connection secrets + lakebase_config to scope `{SCOPE}` (lakebase_password unchanged).")
