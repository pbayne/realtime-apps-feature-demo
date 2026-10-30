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
#   ZEROBUS_ENDPOINT      <workspace-id>.zerobus.<region>.cloud.databricks.com (direct; written to the scope if set)
#   ZEROBUS_CLIENT_ID     service principal application id (direct; written to the scope if set)
#   ZEROBUS_CLIENT_SECRET service principal OAuth secret (direct; written to the scope if set; never printed)
#   ZEROBUS_PRINCIPAL     application id granted MODIFY+SELECT by init_history (defaults to ZEROBUS_CLIENT_ID)
#   SKIP_ZEROBUS=1        deploy without history (the app then writes no history; not recommended)
#   GPU_SCALE_TO_ZERO     true (default) | false. Scale-to-zero for the Parakeet and Kokoro endpoints. With true,
#                         warm the voice path before a demo: scripts/warmup_voice.py <profile>
#   DEPLOY_VOICE=1        also register both GPU endpoints for voice mode: Parakeet (STT) and Kokoro (TTS)
#   DEPLOY_PARAKEET=1     register only the Parakeet endpoint. Implied by DEPLOY_VOICE=1.
#   SIDEKICK_SCOPE        secret scope name (default call-sidekick)
#   BUNDLE_VAR_*          any bundle variable can be overridden, e.g. BUNDLE_VAR_project_id
#
# Order (the app's serving_endpoint resources must exist when the app is deployed):
#   1. secret scope; direct: Zerobus secrets; kafka: Kafka secrets (must exist before the setup job)
#   2. bundle validate; bundle deploy (phase 1)
#   3. setup job (Lakebase role and tables; Kafka topics in kafka mode)
#   4. direct: init_history job (Zerobus tables and grants)
#   5. GPU endpoints, when requested (serverless GPU, blocking). Phase 1 may have failed on app
#      serving-endpoint bindings if the endpoints did not exist yet; that is expected on a first deploy.
#   6. bundle deploy (phase 2) so the app binds the endpoints, then the app
#   7. KB index job (blocking)
#   8. kafka: RTM enrichment and Delta history jobs (continuous, --no-wait)
#
# The generated Postgres password is stored in the secret scope by the setup job. It is never printed.
set -euo pipefail

if [[ $# -lt 1 || $# -gt 3 ]]; then
  echo "Usage: BACKBONE=direct|kafka $0 <databricks-profile> [catalog] [uc_schema | kafka_security]" >&2
  exit 2
fi

profile=$1
catalog=${2:-realtime_call_sidekick_catalog}
backbone=${BACKBONE:-direct}
case "$backbone" in
  direct) target=dev; uc_schema=${3:-call_sidekick}; kafka_security=SASL_SSL_PLAIN ;;
  kafka)  target=kafka; uc_schema=call_sidekick; kafka_security=${3:-SASL_SSL_PLAIN} ;;
  *) echo "BACKBONE must be direct or kafka" >&2; exit 2 ;;
esac
scope=${SIDEKICK_SCOPE:-call-sidekick}

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
    | jq -e --arg k "$1" '(.secrets // .)[]? | select(.key == $k)' >/dev/null 2>&1
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

# --- 1a. Zerobus (direct backbone history) ---------------------------------
if [[ "$backbone" == "direct" && "${SKIP_ZEROBUS:-0}" != "1" ]]; then
  [[ -n "${ZEROBUS_ENDPOINT:-}" ]] && put_secret zerobus_endpoint "$ZEROBUS_ENDPOINT"
  [[ -n "${ZEROBUS_CLIENT_ID:-}" ]] && put_secret zerobus_client_id "$ZEROBUS_CLIENT_ID"
  [[ -n "${ZEROBUS_CLIENT_SECRET:-}" ]] && put_secret zerobus_client_secret "$ZEROBUS_CLIENT_SECRET"
  missing=()
  for key in zerobus_endpoint zerobus_client_id zerobus_client_secret; do
    have_secret "$key" || missing+=("$key")
  done
  if (( ${#missing[@]} > 0 )); then
    cat >&2 <<EOM
Missing Zerobus secrets in scope '$scope': ${missing[*]}
Create a service principal with the Workspace access entitlement, then either export the values and rerun,
or put them directly (values never go in git):
  export ZEROBUS_ENDPOINT=<workspace-id>.zerobus.<region>.cloud.databricks.com
  export ZEROBUS_CLIENT_ID=<application id>  ZEROBUS_CLIENT_SECRET=<oauth secret>
  databricks secrets put-secret $scope zerobus_client_secret --profile $profile
Set SKIP_ZEROBUS=1 to deploy without history.
EOM
    exit 1
  fi
  if [[ -z "${ZEROBUS_PRINCIPAL:-${ZEROBUS_CLIENT_ID:-}}" ]]; then
    echo "ZEROBUS_PRINCIPAL (or ZEROBUS_CLIENT_ID) must be set: init_history grants the principal's MODIFY+SELECT" >&2
    exit 2
  fi
fi
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

# The app's secret bindings need these keys to exist at deploy time. The setup job
# overwrites them with real values. Placeholders are written only if absent.
for key in lakebase_host lakebase_db lakebase_user lakebase_password lakebase_endpoint; do
  if ! have_secret "$key"; then
    put_secret "$key" placeholder
  fi
done

# --- 2. Bundle variables, validate, deploy (phase 1) ----------------------
password=$(openssl rand -hex 24)
export BUNDLE_VAR_catalog=$catalog
export BUNDLE_VAR_uc_schema=$uc_schema
export BUNDLE_VAR_secret_scope=$scope
export BUNDLE_VAR_kafka_security=$kafka_security
export BUNDLE_VAR_pg_password=$password
export BUNDLE_VAR_zerobus_principal=$zerobus_principal
export BUNDLE_VAR_gpu_scale_to_zero=${GPU_SCALE_TO_ZERO:-true}
case "$BUNDLE_VAR_gpu_scale_to_zero" in true|false) ;; *) echo "GPU_SCALE_TO_ZERO must be true or false" >&2; exit 2 ;; esac

db bundle validate --strict --target "$target"
if [[ "${DEPLOY_VOICE:-0}" == "1" || "${DEPLOY_PARAKEET:-0}" == "1" ]]; then
  # The app resource may not bind on a first deploy (its endpoints do not exist yet). Phase 2 binds it.
  db bundle deploy --target "$target" || echo "phase 1 deploy reported an error, expected before the GPU endpoints exist; continuing"
else
  db bundle deploy --target "$target"
fi

# --- 3. Setup: Lakebase role and tables (and Kafka topics in kafka mode) ---
db bundle run call_sidekick_setup --target "$target"

# --- 4. History tables and Zerobus grants (direct) -------------------------
if [[ "$backbone" == "direct" && "${SKIP_ZEROBUS:-0}" != "1" ]]; then
  db bundle run call_sidekick_init_history --target "$target"
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
$(if [[ "$backbone" == "direct" ]]; then echo "- Direct backbone: no RTM cluster or Kafka job. History goes to Zerobus tables in $catalog.$uc_schema."; else echo "- Kafka backbone: RTM enrichment and Delta history are running (--no-wait). Requires a classic workspace."; fi)
- The Postgres password is stored in secret scope '$scope' under lakebase_password.
- Restart the app once after the first setup run so it picks up the real secret values:
    databricks apps stop call-sidekick --profile $profile && databricks apps start call-sidekick --profile $profile
EOM
