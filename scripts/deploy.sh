#!/usr/bin/env bash
# Deploy the call-sidekick bundle to a workspace.
#
# Usage:
#   BACKBONE=direct  scripts/deploy.sh <databricks-profile> [catalog] [uc_schema]           # default
#   BACKBONE=kafka   scripts/deploy.sh <databricks-profile> [catalog] [kafka_security]      # scale mode
#     kafka_security (kafka only): SASL_SSL_PLAIN (default, Confluent) | SASL_SSL_SCRAM_SHA_256 (Redpanda Cloud)
#                                  | SASL_SSL_SCRAM_SHA_512 | SASL_SSL_MSK_IAM (jobs only)
#
# Backbones (docs/ARCHITECTURE.md):
#   direct (default, bundle target `dev`): no Kafka, no RTM cluster. Serverless jobs. Zerobus history.
#   kafka  (bundle target `kafka`): Kafka secrets, RTM cluster and jobs, Delta history job. Classic workspace.
#
# Environment:
#   BACKBONE              direct (default) | kafka
#   BUNDLE_TARGET         dev (default for direct) | prod (direct, always-on GPUs, restricted root, no name prefixes)
#   ZEROBUS_ENDPOINT      <workspace-id>.zerobus.<region>.cloud.databricks.com (direct; written to the scope if set)
#   ZEROBUS_CLIENT_ID     service principal application id (direct; written to the scope if set)
#   ZEROBUS_CLIENT_SECRET service principal OAuth secret (direct; written to the scope if set; never printed)
#   ZEROBUS_PRINCIPAL     application id granted MODIFY+SELECT by init_history (defaults to ZEROBUS_CLIENT_ID)
#   SKIP_ZEROBUS=1        deploy without history (the app then writes no history; not recommended)
#   GPU_SCALE_TO_ZERO     true (default) | false. Scale-to-zero for the Parakeet and Kokoro endpoints. With true,
#                         warm the voice path before a demo: scripts/warmup_voice.py <profile>
#   DEPLOY_VOICE=1        also register both GPU endpoints for voice mode: Parakeet (STT) and Kokoro (TTS)
#   DEPLOY_PARAKEET=1     register only the Parakeet endpoint. Implied by DEPLOY_VOICE=1.
#   VOICE_BACKEND         endpoints (default) | moonshine. moonshine runs STT+TTS in-process (no GPU endpoints):
#                         the app's parakeet/kokoro serving-endpoint bindings are repointed at an existing LLM
#                         endpoint so the bind resolves (the app ignores them in moonshine mode), and the GPU
#                         register steps are skipped. Nothing GPU is created or billed.
#   SIDEKICK_SCOPE        secret scope name (default call-sidekick)
#   ROTATE_PG_PASSWORD=1  generate a new lakebase_password (the setup job re-ALTERs the role; the app is restarted).
#                         Without it the password is written only when the key is absent.
#   BUNDLE_VAR_*          any bundle variable can be overridden, e.g. BUNDLE_VAR_project_id
#
# Order (the app's serving_endpoint resources must exist when the app is deployed):
#   1. secret scope; direct: Zerobus secrets; kafka: Kafka secrets (must exist before the setup job)
#   2. bundle validate; bundle deploy (phase 1)
#   3. setup job (Lakebase role and tables; Kafka topics in kafka mode)
#   4. direct: init_history job (Zerobus tables and grants), then reconcile_history once (Lakebase -> Delta)
#   5. GPU endpoints, when requested (serverless GPU, blocking). Phase 1 may have failed on app
#      serving-endpoint bindings if the endpoints did not exist yet; that is expected on a first deploy.
#   6. bundle deploy (phase 2) so the app binds the endpoints, then the app; restart the app if the
#      Lakebase password was written in this run (see 1c)
#   7. KB index job (blocking)
#   8. kafka: RTM enrichment and Delta history jobs (continuous, --no-wait)
#
# The Lakebase password is written to the secret scope by this script (once, or on ROTATE_PG_PASSWORD=1). The setup
# job reads it and ALTERs the role to match. It is never printed.
set -euo pipefail

if [[ $# -lt 1 || $# -gt 3 ]]; then
  echo "Usage: BACKBONE=direct|kafka $0 <databricks-profile> [catalog] [uc_schema | kafka_security]" >&2
  exit 2
fi

profile=$1
catalog=${2:-realtime_call_sidekick_catalog}
backbone=${BACKBONE:-direct}
case "$backbone" in
  direct) target=${BUNDLE_TARGET:-dev}; uc_schema=${3:-call_sidekick}; kafka_security=SASL_SSL_PLAIN ;;
  kafka)  target=kafka; uc_schema=call_sidekick; kafka_security=${3:-SASL_SSL_PLAIN} ;;
  *) echo "BACKBONE must be direct or kafka" >&2; exit 2 ;;
esac
scope=${SIDEKICK_SCOPE:-call-sidekick}

# Voice backend (resolved early: app_config needs it). endpoints (default) | moonshine.
voice_backend=${VOICE_BACKEND:-endpoints}
case "$voice_backend" in endpoints|moonshine) ;; *) echo "VOICE_BACKEND must be endpoints or moonshine" >&2; exit 2 ;; esac

case "$kafka_security" in
  SASL_SSL_PLAIN|SASL_SSL_SCRAM_SHA_256|SASL_SSL_SCRAM_SHA_512|SASL_SSL_MSK_IAM) ;;
  *) echo "kafka_security must be SASL_SSL_PLAIN, SASL_SSL_SCRAM_SHA_256, SASL_SSL_SCRAM_SHA_512 or SASL_SSL_MSK_IAM" >&2; exit 2 ;;
esac

for tool in databricks jq openssl; do
  command -v "$tool" >/dev/null || { echo "missing required tool: $tool" >&2; exit 2; }
done

# Only read the bundle; never run from a path that could override it.
cd "$(dirname "$0")/.."

db() { databricks "$@" --profile "$profile"; }

have_secret() {
  db secrets list-secrets "$scope" -o json \
    | jq -e --arg k "$1" '(.secrets? // .)[]? | select(.key == $k)' >/dev/null 2>&1  # CLI returns a bare array
}

put_secret() {  # put_secret <key> <value>: the value goes through stdin, never argv or the log
  printf '%s' "$2" | db secrets put-secret "$scope" "$1"
}

# --- 1. Secret scope -------------------------------------------------------
if ! db secrets list-scopes -o json | jq -e --arg s "$scope" '.[]? | select(.name == $s)' >/dev/null 2>&1 \
   && ! db secrets list-scopes -o json | jq -e --arg s "$scope" '.scopes[]? | select(.name == $s)' >/dev/null 2>&1; then
  db secrets create-scope "$scope"
  echo "created secret scope '$scope'"
fi

# Backbone and history location: plain settings bound to the app as secret resources (see resources/app.yml).
put_secret backbone "$backbone"
put_secret history_catalog "$catalog"
put_secret history_schema "$uc_schema"

# --- 1a. History (direct backbone = Lakebase Change Data Feed) --------------
# CDF is a managed Postgres->Delta mirror: no service principal, no Zerobus secrets, no app-side
# history write. The only history setup is (a) REPLICA IDENTITY FULL on the Lakebase tables (done by
# the setup job via init.py) and (b) starting the feed once at the platform level (Lakebase UI /
# Postgres REST API), which is not a bundle resource. init_history then builds the silver
# current-state views over the lb_*_history feeds. Legacy Zerobus secrets are honoured if present
# (HISTORY_BACKEND=zerobus) but are no longer required.
if [[ "$backbone" == "direct" ]]; then
  [[ -n "${ZEROBUS_ENDPOINT:-}" ]] && put_secret zerobus_endpoint "$ZEROBUS_ENDPOINT"
  [[ -n "${ZEROBUS_CLIENT_ID:-}" ]] && put_secret zerobus_client_id "$ZEROBUS_CLIENT_ID"
  [[ -n "${ZEROBUS_CLIENT_SECRET:-}" ]] && put_secret zerobus_client_secret "$ZEROBUS_CLIENT_SECRET"
fi
# Kept for the init_history job parameter and the (now optional) legacy Zerobus path.
zerobus_principal=${ZEROBUS_PRINCIPAL:-${ZEROBUS_CLIENT_ID:-}}

# --- 1b. Kafka secrets (kafka backbone) or placeholders (direct) ----------
if [[ "$backbone" == "kafka" ]]; then
  required_kafka=(kafka_bootstrap)
  [[ "$kafka_security" != "SASL_SSL_MSK_IAM" ]] && required_kafka+=(kafka_api_key kafka_api_secret kafka_app_api_key kafka_app_api_secret)
  missing=()
  for key in "${required_kafka[@]}"; do
    have_secret "$key" || missing+=("$key")
  done
  if (( ${#missing[@]} > 0 )); then
    cat >&2 <<EOM
Missing Kafka secrets in scope '$scope': ${missing[*]}
Create them first (values never go in git):
  databricks secrets put-secret $scope kafka_bootstrap --profile $profile
EOM
    if [[ "$kafka_security" != "SASL_SSL_MSK_IAM" ]]; then
      cat >&2 <<EOM
  databricks secrets put-secret $scope kafka_api_key --profile $profile        # jobs: read-only user
  databricks secrets put-secret $scope kafka_api_secret --profile $profile
  databricks secrets put-secret $scope kafka_app_api_key --profile $profile    # app: write-only user
  databricks secrets put-secret $scope kafka_app_api_secret --profile $profile
EOM
    fi
    exit 1
  fi
  # The app reads the auth mode from the scope too, so jobs and app can't disagree.
  put_secret kafka_security "$kafka_security"
else
  # Direct backbone: the app never reads Kafka, but its resources bind these keys at deploy time.
  for key in kafka_bootstrap kafka_security kafka_api_key kafka_api_secret kafka_app_api_key kafka_app_api_secret; do
    have_secret "$key" || put_secret "$key" "unused-direct-backbone"
  done
fi

# --- 1c. Lakebase password (lakebase_password) -----------------------------
# The setup job reads this key and ALTERs the role to match; the app binds it as a secret resource.
# It is written only when absent, or when ROTATE_PG_PASSWORD=1. Why not on every deploy: the setup job
# re-ALTERs the role each run, but the running app keeps pooled connections authenticated with the old
# password and only picks up the secret at process start, so a per-deploy password breaks new connections.
# The value goes through stdin (put_secret), so it never appears in argv, the log or the bundle variables.
lakebase_password_written=0
if [[ "${ROTATE_PG_PASSWORD:-0}" == "1" ]] || ! have_secret lakebase_password; then
  put_secret lakebase_password "$(openssl rand -hex 24)"
  lakebase_password_written=1
  echo "wrote lakebase_password to scope '$scope' (value not printed)"
fi

# The app's secret bindings need these keys to exist at deploy time. The setup job
# overwrites them with real values. Placeholders are written only if absent.
# SMTP relay for actions (SPEC 12): one JSON secret, smtp_config. The placeholder keeps the app's binding valid and
# "unset" means outbox mode (nothing is sent). Replace it with the real JSON (host, port, username, password, from,
# timeout_s) through stdin, never argv: databricks secrets put-secret <scope> smtp_config
if ! have_secret smtp_config; then
  put_secret smtp_config '{"host":"unset","from":"unset"}'
fi

for key in lakebase_host lakebase_db lakebase_user lakebase_endpoint; do
  if ! have_secret "$key"; then
    put_secret "$key" placeholder
  fi
done

# --- 1d. Bundled app config (Databricks Apps allow at most 20 app resources) ---
# The app binds one JSON secret per group; config.py expands them (BUNDLED_ENV). Jobs keep the individual keys.
get_secret() {  # get_secret <key>: decoded value on stdout (never printed by this script)
  db secrets get-secret "$scope" "$1" -o json | jq -r '.value' | base64 --decode
}
secret_or() {   # secret_or <key> <fallback>
  if have_secret "$1"; then get_secret "$1"; else printf '%s' "$2"; fi
}
put_secret app_config "$(jq -cn --arg backbone "$backbone" --arg c "$catalog" --arg s "$uc_schema" --arg k "$kafka_security" \
  --arg vb "$voice_backend" \
  '{backbone: $backbone, history_catalog: $c, history_schema: $s, kafka_security: $k, voice_backend: $vb}')"
put_secret zerobus_config "$(jq -cn --arg e "$(secret_or zerobus_endpoint "")" --arg i "$(secret_or zerobus_client_id "")" \
  --arg x "$(secret_or zerobus_client_secret "")" '{endpoint: $e, client_id: $i, client_secret: $x}')"
put_secret kafka_config "$(jq -cn --arg b "$(secret_or kafka_bootstrap unused-direct-backbone)" \
  --arg u "$(secret_or kafka_app_api_key unused-direct-backbone)" --arg p "$(secret_or kafka_app_api_secret unused-direct-backbone)" \
  '{bootstrap: $b, username: $u, password: $p}')"
have_secret lakebase_config || put_secret lakebase_config '{"host":"placeholder"}'   # the setup job writes the real one
echo "wrote bundled app config secrets (app_config, zerobus_config, kafka_config; values not printed)"

# --- 2. Bundle variables, validate, deploy (phase 1) ----------------------
export BUNDLE_VAR_catalog=$catalog
export BUNDLE_VAR_uc_schema=$uc_schema
export BUNDLE_VAR_secret_scope=$scope
export BUNDLE_VAR_kafka_security=$kafka_security
export BUNDLE_VAR_zerobus_principal=$zerobus_principal
export BUNDLE_VAR_gpu_scale_to_zero=${GPU_SCALE_TO_ZERO:-true}
case "$BUNDLE_VAR_gpu_scale_to_zero" in true|false) ;; *) echo "GPU_SCALE_TO_ZERO must be true or false" >&2; exit 2 ;; esac

# Voice backend. In moonshine mode the app runs STT+TTS in-process, so the GPU endpoints are neither
# created nor needed. The app resource still declares parakeet-stt/kokoro-tts serving-endpoint bindings,
# so repoint those names at an endpoint that exists (the voice LLM) to keep the bind valid; the app ignores
# STT_ENDPOINT/TTS_ENDPOINT in moonshine mode. Also force the GPU register steps off. (voice_backend was
# validated near the top; it is already in the app_config secret.)
export BUNDLE_VAR_voice_backend=$voice_backend
if [[ "$voice_backend" == "moonshine" ]]; then
  existing_endpoint=${BUNDLE_VAR_voice_model:-databricks-claude-haiku-4-5}
  export BUNDLE_VAR_parakeet_endpoint=$existing_endpoint
  export BUNDLE_VAR_kokoro_endpoint=$existing_endpoint
  DEPLOY_VOICE=0
  DEPLOY_PARAKEET=0
  echo "VOICE_BACKEND=moonshine: in-process STT+TTS; GPU endpoint bindings repointed at $existing_endpoint, no GPU endpoints registered."
fi

db bundle validate --strict --target "$target"
if [[ "${DEPLOY_VOICE:-0}" == "1" || "${DEPLOY_PARAKEET:-0}" == "1" ]]; then
  # The app resource may not bind on a first deploy (its endpoints do not exist yet). Phase 2 binds it.
  db bundle deploy --target "$target" || echo "phase 1 deploy reported an error, expected before the GPU endpoints exist; continuing"
else
  db bundle deploy --target "$target"
fi

# --- 3. Setup: Lakebase role and tables (and Kafka topics in kafka mode) ---
db bundle run call_sidekick_setup --target "$target"

# --- 4. History views over the Lakebase CDF feeds (direct) -----------------
if [[ "$backbone" == "direct" ]]; then
  # init_history builds the silver current-state views over the lb_*_history feeds and creates/seeds
  # crm_accounts. It is idempotent and guards feeds that do not exist yet: on a first deploy, before
  # Lakebase CDF has been Started on the project (a one-time platform step), no lb_*_history tables
  # exist, so the view creation for those tables is skipped and reported as "pending" — re-run this
  # job (or the whole deploy) once CDF is live and has mirrored at least one row. No reconcile job and
  # no Zerobus grants: CDF is WAL-complete and managed, so there is nothing to dedupe or ingest.
  db bundle run call_sidekick_init_history --target "$target" \
    || echo "WARNING: init_history reported an error (often: CDF not yet Started, so no lb_*_history feeds). Re-run after enabling CDF." >&2
fi

# --- 5. GPU endpoints (opt-in, before the app deploy) ----------------------
# Parakeet is the call path's STT and the voice mode's STT. Kokoro is voice mode's TTS. Both are Custom LLM
# Serving endpoints on serverless GPU (A10) with scale-to-zero off, so each bills while it exists.
if [[ "${DEPLOY_VOICE:-0}" == "1" || "${DEPLOY_PARAKEET:-0}" == "1" ]]; then
  db bundle run call_sidekick_parakeet_register --target "$target"
  echo "Parakeet endpoint registered (sidekick-parakeet by default; the app reads it through STT_ENDPOINT)."
fi
if [[ "${DEPLOY_VOICE:-0}" == "1" ]]; then
  db bundle run call_sidekick_kokoro_register --target "$target"
  echo "Kokoro endpoint registered (sidekick-kokoro by default; the app reads it through TTS_ENDPOINT)."
fi

# --- 6. Phase 2 (only when endpoints were just registered), then the app ----
if [[ "${DEPLOY_VOICE:-0}" == "1" || "${DEPLOY_PARAKEET:-0}" == "1" ]]; then
  db bundle deploy --target "$target"
fi
db bundle run call_sidekick_app --target "$target"

# A new Lakebase password reaches the app only when its process starts (secret-backed env vars are read at start),
# and the app's pool keeps authenticated connections alive. So after a write in this run, stop and start the app.
# bundle run deploys the source and does not document a restart of a running app; stop/start is the restart the
# script's previous closing hint used, so it is kept here.
if [[ "$lakebase_password_written" == "1" ]]; then
  db apps stop call-sidekick
  db apps start call-sidekick
  echo "restarted app call-sidekick so it reads the new lakebase_password"
fi

# --- 7. KB index (blocking; cheap) ----------------------------------------
db bundle run call_sidekick_kb_index --target "$target"

# --- 8. Kafka backbone: continuous jobs -----------------------------------
if [[ "$backbone" == "kafka" ]]; then
  db bundle run call_sidekick_rtm_enrich --target "$target" --no-wait
  db bundle run call_sidekick_delta_history --target "$target" --no-wait
fi

cat <<EOM

Deployment complete (backbone=$backbone, bundle target=$target).
- Setup job, app, KB index finished. Post-call scoring is scheduled every 30 minutes (UTC).
$(if [[ "$backbone" == "direct" ]]; then echo "- Direct backbone: history is Lakebase Change Data Feed (managed Postgres->Delta). If you have not yet Started CDF on the Lakebase project, do it now (Lakebase Postgres -> Branch overview -> Lakebase CDF -> Start; source schema public -> destination $catalog.$uc_schema), then re-run: db bundle run call_sidekick_init_history --target $target  to build the current-state views."; else echo "- Kafka backbone: RTM enrichment and Delta history are running (--no-wait). Requires a classic workspace."; fi)
- The Postgres password is in secret scope '$scope' under lakebase_password. It is kept across deploys;
  set ROTATE_PG_PASSWORD=1 to rotate it (the setup job re-ALTERs the role and the app is restarted).
EOM
