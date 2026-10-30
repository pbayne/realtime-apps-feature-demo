"""Static checks on the DAB bundle (docs/ARCHITECTURE.md): YAML parses, job/notebook references resolve,
the direct target deploys no RTM or Kafka resources and only serverless jobs, the kafka target carries the
classic RTM and Delta jobs, and deploy.sh registers the GPU endpoints before the app deploy."""

import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
NOTEBOOKS = ROOT / "src" / "notebooks"


def _load(path):
    with open(path, encoding="utf-8") as fh:
        return yaml.safe_load(fh)


BUNDLE = _load(ROOT / "databricks.yml")
YAML_FILES = [ROOT / "databricks.yml", *sorted((ROOT / "resources").glob("*.yml"))]


def _direct_jobs():
    """Jobs in resources/*.yml: deployed by the default (direct) target."""
    jobs = {}
    for path in (ROOT / "resources").glob("*.yml"):
        for key, job in ((_load(path) or {}).get("resources", {}).get("jobs") or {}).items():
            jobs[key] = job
    return jobs


def _kafka_target():
    return BUNDLE["targets"]["kafka"]


@pytest.mark.parametrize("path", YAML_FILES, ids=lambda p: p.name)
def test_yaml_parses(path):
    assert _load(path) is not None


def test_jobs_reference_existing_notebooks():
    missing = []
    for path in YAML_FILES:
        doc = _load(path) or {}
        jobs = list((doc.get("resources", {}).get("jobs") or {}).values())
        jobs += list(((doc.get("targets") or {}).get("kafka", {}).get("resources", {}).get("jobs") or {}).values())
        for job in jobs:
            for task in job.get("tasks", []):
                nb = task.get("notebook_task", {}).get("notebook_path")
                if nb and not (path.parent / nb).resolve().exists():
                    missing.append((path.name, nb))
    assert not missing, missing


def test_every_job_task_has_compute():
    for key, job in _direct_jobs().items():
        env_keys = {e["environment_key"] for e in job.get("environments", [])}
        cluster_keys = {c["job_cluster_key"] for c in job.get("job_clusters", [])}
        for task in job["tasks"]:
            has = task.get("environment_key") in env_keys or task.get("job_cluster_key") in cluster_keys \
                or task.get("existing_cluster_id")
            assert has, f"{key}.{task['task_key']} has no compute"
    for key, job in _kafka_target()["resources"]["jobs"].items():
        cluster_keys = {c["job_cluster_key"] for c in job.get("job_clusters", [])}
        for task in job["tasks"]:
            assert task.get("existing_cluster_id") or task.get("job_cluster_key") in cluster_keys, key


def test_direct_target_has_no_rtm_or_kafka_jobs_or_clusters():
    dev_resources = BUNDLE["targets"]["dev"].get("resources") or {}
    assert not dev_resources
    jobs = _direct_jobs()
    assert "call_sidekick_rtm_enrich" not in jobs and "call_sidekick_delta_history" not in jobs
    assert not (_load(ROOT / "resources" / "cluster.yml") if (ROOT / "resources" / "cluster.yml").exists() else None)
    assert not any(p.name == "cluster.yml" for p in (ROOT / "resources").glob("*.yml"))


def test_direct_jobs_are_serverless_with_no_classic_clusters():
    for key, job in _direct_jobs().items():
        assert "job_clusters" not in job, key
        assert job.get("environments"), f"{key} needs a serverless environment"
        for task in job["tasks"]:
            assert task.get("environment_key"), f"{key}.{task['task_key']} needs environment_key"
            assert "existing_cluster_id" not in task and "new_cluster" not in task, key


def test_kafka_target_carries_rtm_cluster_with_real_time_mode_requirements():
    cluster = _kafka_target()["resources"]["clusters"]["rtm_cluster"]
    assert cluster["data_security_mode"] == "SINGLE_USER"
    assert cluster["runtime_engine"] == "STANDARD"           # Photon off
    assert cluster["aws_attributes"]["availability"] == "ON_DEMAND"
    assert cluster["spark_conf"]["spark.databricks.streaming.realTimeMode.enabled"] == "true"
    assert "autoscale" not in cluster                         # fixed num_workers
    assert cluster["num_workers"] == "${var.rtm_num_workers}"


def test_rtm_job_is_only_on_rtm_cluster_and_has_no_timeout():
    job = _kafka_target()["resources"]["jobs"]["call_sidekick_rtm_enrich"]
    assert job["tasks"][0]["existing_cluster_id"] == "${resources.clusters.rtm_cluster.id}"
    assert "timeout_seconds" not in job


def test_kafka_target_sets_backbone_and_classic_history_job():
    target = _kafka_target()
    assert target["variables"]["backbone"] == "kafka"
    assert BUNDLE["variables"]["backbone"]["default"] == "direct"
    delta = target["resources"]["jobs"]["call_sidekick_delta_history"]
    assert delta["tasks"][0]["notebook_task"]["notebook_path"] == "./src/notebooks/delta_history.py"


def test_dev_target_has_no_hardcoded_host():
    dev = BUNDLE["targets"]["dev"]
    assert "host" not in dev and "profile" not in dev and "workspace" not in dev
    assert dev["mode"] == "development"


def test_pg_password_is_not_a_bundle_variable_or_job_parameter():
    # The Lakebase password lives only in the secret scope (lakebase_password), never in job parameters.
    for path in (ROOT / "databricks.yml", ROOT / "resources" / "job.yml"):
        assert "pg_password" not in path.read_text(encoding="utf-8"), path.name
    assert "pg_password" not in BUNDLE["variables"]
    for name, target in BUNDLE["targets"].items():
        assert "pg_password" not in ((target or {}).get("variables") or {}), name


def test_setup_job_passes_no_password_parameter():
    params = _direct_jobs()["call_sidekick_setup"]["tasks"][0]["notebook_task"]["base_parameters"]
    assert not [k for k in params if "password" in k.lower()], params


def test_setup_notebook_reads_password_from_secret_scope_and_does_not_print_it():
    text = (NOTEBOOKS / "init.py").read_text(encoding="utf-8")
    assert "pg_password" not in text
    assert 'dbutils.secrets.get(SCOPE, "lakebase_password")' in text
    # The ALTER ROLE that syncs the role to the secret stays.
    assert 'f\'{verb} ROLE "{role}" WITH LOGIN PASSWORD {pw_lit}\'' in text
    # The password value is never printed or written back by the notebook.
    assert "print(role_password" not in text and '"lakebase_password"' not in text.split("## 7.")[-1]


def test_deploy_script_writes_lakebase_password_only_when_absent_or_rotating():
    lines = (ROOT / "scripts" / "deploy.sh").read_text(encoding="utf-8").splitlines()
    text = "\n".join(lines)
    writes = [i for i, line in enumerate(lines) if "put_secret lakebase_password" in line]
    assert len(writes) == 1, "exactly one write of lakebase_password"
    guard = lines[max(0, writes[0] - 3):writes[0]]
    assert any(re.search(r"^if .*ROTATE_PG_PASSWORD.*have_secret lakebase_password", line) for line in guard), guard
    # The password is never a bundle variable and never a command-line argument.
    assert "BUNDLE_VAR_pg_password" not in text
    assert "--string-value" not in text
    # The app restart after a write comes after the app deploy.
    restart = text.index('if [[ "$lakebase_password_written" == "1" ]]')
    assert text.index("bundle run call_sidekick_app") < restart < text.index("apps stop call-sidekick")


def test_notebooks_exist():
    for name in ("init.py", "init_history.py", "rtm_enrich.py", "rtm_lib.py", "delta_history.py",
                 "register_parakeet.py", "register_kokoro.py", "build_kb_index.py", "post_call_scoring.py",
                 "enrich_rules.json",
                 "parakeet_serving/asr_server.py", "kokoro_serving/kokoro_server.py", "kokoro_serving/adapter.py"):
        assert (NOTEBOOKS / name).exists(), name


def test_voice_endpoints_are_custom_llm_serving_not_route_optimized():
    text = (NOTEBOOKS / "register_parakeet.py").read_text(encoding="utf-8")
    assert "route_optimized" not in text and "data_plane" not in text
    assert "llm/v1/audio/transcriptions" in text and "/audio/transcriptions" in text
    kokoro = (NOTEBOOKS / "register_kokoro.py").read_text(encoding="utf-8")
    assert '"llm/v1/chat"' in kokoro and "scale_to_zero_enabled=SCALE_TO_ZERO" in kokoro


def test_voice_jobs_register_both_gpu_endpoints_on_serverless_gpu():
    jobs = _direct_jobs()
    for key, nb in (("call_sidekick_parakeet_register", "register_parakeet.py"),
                     ("call_sidekick_kokoro_register", "register_kokoro.py")):
        task = jobs[key]["tasks"][0]
        assert task["notebook_task"]["notebook_path"].endswith(nb)
        assert task["compute"]["hardware_accelerator"] == "${var.gpu_accelerator}"
        assert jobs[key]["environments"][0]["spec"]["environment_version"] == "6"
    assert BUNDLE["variables"]["gpu_accelerator"]["default"] == "GPU_1xA10"
    assert BUNDLE["variables"]["gpu_scale_to_zero"]["default"] == "true"
    for key in ("call_sidekick_parakeet_register", "call_sidekick_kokoro_register"):
        assert jobs[key]["tasks"][0]["notebook_task"]["base_parameters"]["gpu_scale_to_zero"] == "${var.gpu_scale_to_zero}"
    for nb in ("register_parakeet.py", "register_kokoro.py"):
        text = (NOTEBOOKS / nb).read_text(encoding="utf-8")
        assert 'widgets.dropdown("gpu_scale_to_zero", "true"' in text and "scale_to_zero_enabled=SCALE_TO_ZERO" in text


def test_app_serving_resources_cover_voice_mode():
    doc = _load(ROOT / "resources" / "app.yml")
    names = {r["name"]: r for r in doc["resources"]["apps"]["call_sidekick_app"]["resources"]}
    assert names["kokoro-tts"]["serving_endpoint"]["name"] == "${var.kokoro_endpoint}"
    assert names["voice-llm"]["serving_endpoint"]["name"] == "${var.voice_model}"
    app_yaml = _load(ROOT / "src" / "app" / "app.yaml")
    env = {e["name"]: e for e in app_yaml["env"]}
    assert env["TTS_ENDPOINT"]["valueFrom"] == "kokoro-tts"
    assert env["VOICE_MODEL"]["valueFrom"] == "voice-llm"
    assert env["STT_ENDPOINT"]["valueFrom"] == "parakeet-stt"


def test_app_binds_backbone_and_zerobus_from_secret_scope():
    # One JSON secret per group (Databricks Apps allow at most 20 app resources); config.py expands them.
    doc = _load(ROOT / "resources" / "app.yml")
    res = {r["name"]: r for r in doc["resources"]["apps"]["call_sidekick_app"]["resources"]}
    for name, key in (("app-config", "app_config"), ("lakebase-config", "lakebase_config"),
                      ("zerobus-config", "zerobus_config"), ("kafka-config", "kafka_config")):
        assert res[name]["secret"]["key"] == key
    assert len(res) <= 20
    env = {e["name"]: e for e in _load(ROOT / "src" / "app" / "app.yaml")["env"]}
    assert env["APP_CONFIG"]["valueFrom"] == "app-config"
    assert env["ZEROBUS_CONFIG"]["valueFrom"] == "zerobus-config"
    assert env["LAKEBASE_CONFIG"]["valueFrom"] == "lakebase-config"


def test_init_history_job_is_serverless_and_takes_the_principal():
    job = _direct_jobs()["call_sidekick_init_history"]
    assert job["tasks"][0]["notebook_task"]["base_parameters"]["zerobus_principal"] == "${var.zerobus_principal}"


def test_deploy_script_registers_gpu_endpoints_before_app_deploy():
    text = (ROOT / "scripts" / "deploy.sh").read_text(encoding="utf-8")
    register = text.index("call_sidekick_parakeet_register")
    app = text.index("bundle run call_sidekick_app")
    assert register < app, "GPU endpoints must exist before the app's serving_endpoint resources bind"
