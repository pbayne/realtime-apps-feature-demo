"""The deployed app's env (src/app/app.yaml bindings, filled the way scripts/deploy.sh fills the scope) must load.

Regression: deploy.sh writes "unused-direct-backbone" into the Kafka keys for backbone=direct, and a strict
KAFKA_SECURITY literal crashed the app at startup.
"""

import json
from pathlib import Path

import pytest
import yaml
from config import Settings

APP_YAML = Path(__file__).resolve().parents[2] / "src" / "app" / "app.yaml"

# What each app resource resolves to after a direct-backbone deploy.
DEPLOYED = {
    "app-config": json.dumps({"backbone": "direct", "history_catalog": "realtime_call_sidekick_catalog",
                              "history_schema": "call_sidekick", "kafka_security": "SASL_SSL_PLAIN"}),
    "lakebase-config": json.dumps({"host": "ep-x.database.us-west-2.cloud.databricks.com", "db": "databricks_postgres",
                                   "user": "sidekick_app", "password": "p" * 48, "endpoint": "projects/p/branches/b"}),
    "zerobus-config": json.dumps({"endpoint": "7474650324322090.zerobus.us-west-2.cloud.databricks.com",
                                  "client_id": "00000000-0000-0000-0000-000000000000", "client_secret": "secret"}),
    "kafka-config": json.dumps({"bootstrap": "unused-direct-backbone", "username": "unused-direct-backbone",
                                "password": "unused-direct-backbone"}),
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
    from config import BUNDLED_ENV, expand_bundled_env

    for names in BUNDLED_ENV.values():
        for name in names.values():
            monkeypatch.delenv(name, raising=False)
    env = dict(_deployed_env())
    expand_bundled_env(env)                 # on a copy: monkeypatch must own every env change
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    s = Settings()
    assert s.lakebase_host.startswith("ep-x") and s.lakebase_password == "p" * 48
    assert s.history_catalog == "realtime_call_sidekick_catalog" and s.zerobus_client_id.startswith("0000")
    assert s.mode == "databricks" and s.backbone == "direct"
    assert s.effective_bus == "none"


def test_kafka_backbone_still_validates_security_mode(monkeypatch):
    monkeypatch.setenv("BACKBONE", "kafka")
    monkeypatch.setenv("KAFKA_SECURITY", "unused-direct-backbone")
    with pytest.raises(ValueError, match="KAFKA_SECURITY"):
        Settings(mode="databricks", history_backend="none")
    monkeypatch.setenv("KAFKA_SECURITY", "SASL_SSL_SCRAM_SHA_256")
    assert Settings(mode="databricks", history_backend="none").kafka_security == "SASL_SSL_SCRAM_SHA_256"


def test_app_declares_at_most_20_resources():
    """Databricks Apps reject more than 20 app resources (a deploy failed at 21)."""
    import re

    app_yml = (APP_YAML.parents[2] / "resources" / "app.yml").read_text()
    n = len(re.findall(r"^\s*- name: ", app_yml, re.M))
    assert n <= 20, f"{n} app resources"


def test_bundled_env_does_not_override_explicit_values_and_ignores_bad_json():
    from config import expand_bundled_env

    env = {"LAKEBASE_CONFIG": json.dumps({"host": "from-bundle", "db": "d"}), "LAKEBASE_HOST": "explicit",
           "ZEROBUS_CONFIG": "{not json"}
    assert expand_bundled_env(env) == ["LAKEBASE_DB"]
    assert env["LAKEBASE_HOST"] == "explicit" and env["LAKEBASE_DB"] == "d"
