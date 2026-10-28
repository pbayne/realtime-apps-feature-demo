"""Static checks on the DAB bundle: YAML parses, job/notebook references resolve,
and no workspace host is hard-coded in the dev target."""

from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
NOTEBOOKS = ROOT / "src" / "notebooks"


def _load(path):
    with open(path, encoding="utf-8") as fh:
        return yaml.safe_load(fh)


YAML_FILES = [ROOT / "databricks.yml", *sorted((ROOT / "resources").glob("*.yml"))]


@pytest.mark.parametrize("path", YAML_FILES, ids=lambda p: p.name)
def test_yaml_parses(path):
    assert _load(path) is not None


def test_jobs_reference_existing_notebooks():
    missing = []
    for path in (ROOT / "resources").glob("*.yml"):
        doc = _load(path) or {}
        for job in (doc.get("resources", {}).get("jobs") or {}).values():
            for task in job.get("tasks", []):
                nb = task.get("notebook_task", {}).get("notebook_path")
                if nb and not (path.parent / nb).resolve().exists():
                    missing.append((path.name, nb))
    assert not missing, missing


def test_every_job_task_has_a_cluster():
    for path in (ROOT / "resources").glob("*.yml"):
        doc = _load(path) or {}
        for key, job in (doc.get("resources", {}).get("jobs") or {}).items():
            job_cluster_keys = {c["job_cluster_key"] for c in job.get("job_clusters", [])}
            for task in job["tasks"]:
                has = task.get("existing_cluster_id") or task.get("job_cluster_key") in job_cluster_keys
                assert has, f"{key}.{task['task_key']} has no cluster"


def test_rtm_cluster_meets_real_time_mode_requirements():
    doc = _load(ROOT / "resources" / "cluster.yml")
    cluster = doc["resources"]["clusters"]["rtm_cluster"]
    assert cluster["data_security_mode"] == "SINGLE_USER"
    assert cluster["runtime_engine"] == "STANDARD"           # Photon off
    assert "aws_attributes" in cluster and cluster["aws_attributes"]["availability"] == "ON_DEMAND"
    assert cluster["spark_conf"]["spark.databricks.streaming.realTimeMode.enabled"] == "true"
    assert "autoscale" not in cluster                         # fixed num_workers
    assert cluster["num_workers"] == "${var.rtm_num_workers}"


def test_rtm_job_is_only_on_rtm_cluster_and_has_no_timeout():
    doc = _load(ROOT / "resources" / "job.yml")
    job = doc["resources"]["jobs"]["call_copilot_rtm_enrich"]
    assert job["tasks"][0]["existing_cluster_id"] == "${resources.clusters.rtm_cluster.id}"
    assert "timeout_seconds" not in job


def test_dev_target_has_no_hardcoded_host():
    doc = _load(ROOT / "databricks.yml")
    dev = doc["targets"]["dev"]
    assert "host" not in dev and "profile" not in dev and "workspace" not in dev
    assert dev["mode"] == "development"


def test_pg_password_has_no_default():
    doc = _load(ROOT / "databricks.yml")
    assert "default" not in doc["variables"]["pg_password"]


def test_notebooks_exist():
    for name in ("init.py", "rtm_enrich.py", "rtm_lib.py", "delta_history.py",
                 "register_parakeet.py", "build_kb_index.py", "post_call_scoring.py",
                 "enrich_rules.json"):
        assert (NOTEBOOKS / name).exists(), name
