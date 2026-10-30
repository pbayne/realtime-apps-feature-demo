#!/usr/bin/env bash
# Deploy the call-sidekick bundle to a workspace.
#
# Usage:
#   scripts/deploy.sh <databricks-profile> [catalog] [kafka_security]
#     kafka_security: SASL_SSL_PLAIN (default, Confluent Cloud) | SASL_SSL_SCRAM_SHA_256 (Redpanda Cloud)
#                     | SASL_SSL_SCRAM_SHA_512 | SASL_SSL_MSK_IAM (jobs only; the app producer needs a user/password mode)
#
# Environment:
#   DEPLOY_VOICE=1      also register both GPU endpoints for voice mode: Parakeet (STT) and Kokoro (TTS)
#   DEPLOY_PARAKEET=1   register only the Parakeet endpoint (GPU; needs quota). Implied by DEPLOY_VOICE=1.
#   SIDEKICK_SCOPE       secret scope name (default call-sidekick)
#   BUNDLE_VAR_*        any bundle variable can be overridden, e.g. BUNDLE_VAR_project_id
#
# Order matters:
#   1. secret scope + Kafka secrets (must exist before the setup job and app deploy)
#   2. bundle validate, bundle deploy
#   3. setup job  (Lakebase role/tables/trigger, Kafka topics, Lakebase secrets)
#   4. app deploy (its secret bindings need the Lakebase keys written in step 3)
#   5. KB index job (blocking)
#   6. RTM enrichment job and Delta history job (continuous, --no-wait)
#   7. optional GPU endpoints (blocking; cost: two always-on A10G endpoints, see README "Voice mode")
#
# The generated Postgres password is stored in the secret scope by the setup job.
# It is never printed.
set -euo pipefail

if [[ $# -lt 1 || $# -gt 3 ]]; then
  echo "Usage: $0 <databricks-profile> [catalog] [kafka_security]" >&2
  exit 2
fi

profile=$1
catalog=${2:-main}
kafka_security=${3:-SASL_SSL_PLAIN}
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

# --- 1. Secret scope and Kafka secrets ------------------------------------
if ! db secrets list-scopes -o json | jq -e --arg s "$scope" '.[]? | select(.name == $s)' >/dev/null 2>&1 \
   && ! db secrets list-scopes -o json | jq -e --arg s "$scope" '.scopes[]? | select(.name == $s)' >/dev/null 2>&1; then
  db secrets create-scope "$scope"
  echo "created secret scope '$scope'"
fi

have_secret() {
  db secrets list-secrets "$scope" -o json \
    | jq -e --arg k "$1" '(.secrets // .)[]? | select(.key == $k)' >/dev/null 2>&1
}

required_kafka=(kafka_bootstrap)
# Jobs (consumer) and app (producer) authenticate as separate users.
[[ "$kafka_security" != "SASL_SSL_MSK_IAM" ]] && required_kafka+=(kafka_api_key kafka_api_secret kafka_app_api_key kafka_app_api_secret)
missing=()
for key in "${required_kafka[@]}"; do
  have_secret "$key" || missing+=("$key")
done
if (( ${#missing[@]} > 0 )); then
  cat >&2 <<EOF
Missing Kafka secrets in scope '$scope': ${missing[*]}
Create them first (values never go in git):
  databricks secrets put-secret $scope kafka_bootstrap --profile $profile
EOF
  if [[ "$kafka_security" != "SASL_SSL_MSK_IAM" ]]; then
    cat >&2 <<EOF
  databricks secrets put-secret $scope kafka_api_key --profile $profile        # jobs: read-only user
  databricks secrets put-secret $scope kafka_api_secret --profile $profile
  databricks secrets put-secret $scope kafka_app_api_key --profile $profile    # app: write-only user
  databricks secrets put-secret $scope kafka_app_api_secret --profile $profile
EOF
  fi
  exit 1
fi

# The app reads the auth mode from the scope too, so jobs and app can't disagree.
printf '%s' "$kafka_security" | db secrets put-secret "$scope" kafka_security

# The app's secret bindings need these keys to exist at deploy time. The setup job
# overwrites them with real values. Placeholders are written only if absent.
for key in lakebase_host lakebase_db lakebase_user lakebase_password lakebase_endpoint; do
  if ! have_secret "$key"; then
    printf '%s' placeholder | db secrets put-secret "$scope" "$key"
  fi
done

# --- 2. Bundle variables and validate/deploy ------------------------------
password=$(openssl rand -hex 24)
export BUNDLE_VAR_catalog=$catalog
export BUNDLE_VAR_secret_scope=$scope
export BUNDLE_VAR_kafka_security=$kafka_security
export BUNDLE_VAR_pg_password=$password

db bundle validate --strict --target dev
db bundle deploy --target dev

# --- 3. Setup: Lakebase role, tables, trigger, Kafka topics, secrets -------
db bundle run call_sidekick_setup --target dev

# --- 4. App (after the Lakebase keys are real) -----------------------------
db bundle run call_sidekick_app --target dev

# --- 5. KB index (blocking; cheap) ----------------------------------------
db bundle run call_sidekick_kb_index --target dev

# --- 6. Continuous jobs ----------------------------------------------------
db bundle run call_sidekick_rtm_enrich --target dev --no-wait
db bundle run call_sidekick_delta_history --target dev --no-wait

# --- 7. GPU endpoints (opt-in) ---------------------------------------------
# Parakeet is the call path's STT and the voice mode's STT. Kokoro is voice mode's TTS. Both are Custom LLM
# Serving endpoints on A10G with scale-to-zero off, so each bills while it exists.
if [[ "${DEPLOY_VOICE:-0}" == "1" || "${DEPLOY_PARAKEET:-0}" == "1" ]]; then
  db bundle run call_sidekick_parakeet_register --target dev
  echo "Parakeet endpoint registered (sidekick-parakeet by default; the app reads it through STT_ENDPOINT)."
fi
if [[ "${DEPLOY_VOICE:-0}" == "1" ]]; then
  db bundle run call_sidekick_kokoro_register --target dev
  echo "Kokoro endpoint registered (sidekick-kokoro by default; the app reads it through TTS_ENDPOINT)."
fi

cat <<EOF

Deployment complete.
- Setup job, app, and KB index finished. RTM enrichment and Delta history are running (--no-wait).
- Post-call scoring is scheduled every 30 minutes (UTC).
- The Postgres password is stored in secret scope '$scope' under lakebase_password.
- Restart the app once after the first setup run so it picks up the real secret values:
    databricks apps stop call-sidekick --profile $profile && databricks apps start call-sidekick --profile $profile
EOF
