"""The deployed app's env (src/app/app.yaml bindings, filled the way scripts/deploy.sh fills the scope) must load.

Regression: deploy.sh writes "unused-direct-backbone" into the Kafka keys for backbone=direct, and a strict
KAFKA_SECURITY literal crashed the app at startup.
"""

from pathlib import Path

import pytest
import yaml
from config import Settings

APP_YAML = Path(__file__).resolve().parents[2] / "src" / "app" / "app.yaml"

# What each app resource resolves to after a direct-backbone deploy.
DEPLOYED = {
    "kafka-bootstrap": "unused-direct-backbone", "kafka-security": "unused-direct-backbone",
    "kafka-api-key": "unused-direct-backbone", "kafka-api-secret": "unused-direct-backbone",
    "backbone": "direct", "history-catalog": "realtime_call_sidekick_catalog", "history-schema": "call_sidekick",
    "zerobus-endpoint": "7474650324322090.zerobus.us-west-2.cloud.databricks.com",
    "zerobus-client-id": "00000000-0000-0000-0000-000000000000", "zerobus-client-secret": "secret",
    "lakebase-host": "ep-x.database.us-west-2.cloud.databricks.com", "lakebase-db": "databricks_postgres",
    "lakebase-user": "sidekick_app", "lakebase-password": "p" * 48, "lakebase-endpoint": "projects/p/branches/b",
}


def _deployed_env() -> dict[str, str]:
    env = {}
    for item in yaml.safe_load(APP_YAML.read_text())["env"]:
        if "value" in item:
            env[item["name"]] = str(item["value"])
        else:
            env[item["name"]] = DEPLOYED.get(item["valueFrom"], f"endpoint-{item['valueFrom']}")
    return env


def test_direct_backbone_deploy_env_loads(monkeypatch):
    for k, v in _deployed_env().items():
        monkeypatch.setenv(k, v)
    s = Settings()
    assert s.mode == "databricks" and s.backbone == "direct"
    assert s.effective_bus == "none"


def test_kafka_backbone_still_validates_security_mode(monkeypatch):
    monkeypatch.setenv("BACKBONE", "kafka")
    monkeypatch.setenv("KAFKA_SECURITY", "unused-direct-backbone")
    with pytest.raises(ValueError, match="KAFKA_SECURITY"):
        Settings(mode="databricks", history_backend="none")
    monkeypatch.setenv("KAFKA_SECURITY", "SASL_SSL_SCRAM_SHA_256")
    assert Settings(mode="databricks", history_backend="none").kafka_security == "SASL_SSL_SCRAM_SHA_256"
