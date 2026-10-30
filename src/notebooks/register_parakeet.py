# Databricks notebook source
# MAGIC %md
# MAGIC # Call sidekick — register Parakeet STT and deploy the GPU audio endpoint
# MAGIC
# MAGIC Ported from `reference/custom-llm-serving-solutions/audio-serving/parakeet-tdt-0.6b-v3.ipynb` (SPEC 3.3, contract v0.3).
# MAGIC It serves `nvidia/parakeet-tdt-0.6b-v3` (NeMo) as a Custom LLM Serving entrypoint:
# MAGIC `parakeet_serving/asr_server.py` (FastAPI + NeMo, bf16, 5 ms micro-batching) answers
# MAGIC `POST /v1/audio/transcriptions` on port 8080. The model is logged with task `llm/v1/audio/transcriptions`.
# MAGIC
# MAGIC Endpoint: `GPU_MEDIUM` (A10G on AWS), workload `Small`. Scale-to-zero is set by the `gpu_scale_to_zero` widget
# MAGIC (default `true` for dev cost). With scale-to-zero on, the first request after idle waits for a cold GPU
# MAGIC (minutes), so warm the endpoint before a demo (`scripts/warmup_voice.py`) or set `gpu_scale_to_zero=false`.
# MAGIC It is **not** route-optimized.
# MAGIC The app calls it through the workspace host with its service principal (`LlmClient.multipart`).
# MAGIC
# MAGIC ## Contract (what the app sends and reads)
# MAGIC Request: multipart `POST {host}/serving-endpoints/{endpoint}/audio/transcriptions` with
# MAGIC `file` (16 kHz mono PCM16 WAV, up to 30 s), `model` (endpoint name), `response_format=verbose_json`.
# MAGIC
# MAGIC Response: `{"duration", "text", "segments": [{"start","end","text"}], "words": [{"word","start","end"}]}`.
# MAGIC Times are seconds relative to the clip. The app adds the window offset itself.
# MAGIC
# MAGIC ## Stages (widget `stage`)
# MAGIC - `deploy` (default): smoke test, register to Unity Catalog, create or update the endpoint, wait until it transcribes.
# MAGIC - `verify`: smoke test only; nothing is registered or deployed.

# COMMAND ----------

# MAGIC %pip install --quiet "nemo_toolkit[asr]>=2.4" av==18.1.0 fastapi uvicorn python-multipart openai==2.6.1 "mlflow==3.15.0" "databricks-sdk==0.134.0" soundfile huggingface_hub requests
# MAGIC %restart_python

# COMMAND ----------

dbutils.widgets.text("uc_catalog", "main")
dbutils.widgets.text("uc_schema", "call_sidekick")
dbutils.widgets.text("model_name", "parakeet_tdt_v3")
dbutils.widgets.text("endpoint_name", "sidekick-parakeet")
dbutils.widgets.dropdown("workload_type", "GPU_MEDIUM", ["GPU_SMALL", "GPU_MEDIUM", "GPU_LARGE", "MULTIGPU_MEDIUM"])
dbutils.widgets.text("workload_size", "Small")
dbutils.widgets.dropdown("gpu_scale_to_zero", "true", ["true", "false"])   # false = always on (demos, latency)
dbutils.widgets.dropdown("stage", "deploy", ["deploy", "verify"])

catalog = dbutils.widgets.get("uc_catalog").strip()
schema = dbutils.widgets.get("uc_schema").strip()
model_name = dbutils.widgets.get("model_name").strip()
endpoint_name = dbutils.widgets.get("endpoint_name").strip()
workload_type = dbutils.widgets.get("workload_type").strip()
workload_size = dbutils.widgets.get("workload_size").strip()
STAGE = dbutils.widgets.get("stage")

SCALE_TO_ZERO = dbutils.widgets.get("gpu_scale_to_zero").strip().lower() == "true"
UC_MODEL_NAME = f"{catalog}.{schema}.{model_name}"
MODEL_REPO_ID = "nvidia/parakeet-tdt-0.6b-v3"   # Hugging Face model
MODEL_DIR = "parakeet-tdt-0.6b-v3"              # weights folder: the entrypoint loads it by this name
SERVER_DIR = "server"                           # server code folder: logged with the model
SERVED_MODEL_NAME = "parakeet-tdt-0.6b-v3"      # name the server reports on /v1/models

# Tuned on an A10 (see the reference benchmark): 32 files per batch, 5 ms batching window, bf16.
BATCH_SIZE = 32
BATCH_WAIT_MS = 5
DTYPE = "bfloat16"
LOCAL_ATTENTION = False   # True for hour-long files

assert workload_type in ("GPU_SMALL", "GPU_MEDIUM", "GPU_LARGE", "MULTIGPU_MEDIUM"), workload_type

# The command the endpoint runs (port 8080 is what Model Serving forwards audio requests to).
def server_command(port):
    return " ".join([
        f"python -u {SERVER_DIR}/asr_server.py",
        f"--model-dir {MODEL_DIR}",
        f"--served-model-name {SERVED_MODEL_NAME}",
        f"--port {port}",
        f"--batch-size {BATCH_SIZE}",
        f"--batch-wait-ms {BATCH_WAIT_MS}",
        f"--dtype {DTYPE}",
    ] + (["--local-attention"] if LOCAL_ATTENTION else []))

print(STAGE, "|", MODEL_REPO_ID, "->", UC_MODEL_NAME, "->", endpoint_name, "|", workload_type)

# COMMAND ----------

# MAGIC %md ## 1. Create the model in Unity Catalog (fails fast on a permission problem)

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import AlreadyExists, DatabricksError, ResourceAlreadyExists

w = WorkspaceClient()
if STAGE == "deploy":
    try:
        w.registered_models.create(catalog_name=catalog, schema_name=schema, name=model_name)
    except (AlreadyExists, ResourceAlreadyExists):
        pass
    except DatabricksError as error:
        raise RuntimeError(f"Cannot create the model {UC_MODEL_NAME}. Set uc_catalog/uc_schema to a schema "
                           f"you can create models in.\n{error}") from None

# COMMAND ----------

# MAGIC %md ## 2. Download the checkpoint and copy the server code

# COMMAND ----------

import os
import shutil
import tempfile

os.environ["HF_HUB_DISABLE_PROGRESS_BARS"] = "1"
from huggingface_hub import hf_hub_download, snapshot_download

NOTEBOOK_DIR = "/Workspace" + os.path.dirname(
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())
workdir = tempfile.mkdtemp()          # the notebook folder does not take multi-GB files
os.chdir(workdir)
snapshot_download(MODEL_REPO_ID, local_dir=MODEL_DIR)
shutil.copytree(os.path.join(NOTEBOOK_DIR, "parakeet_serving"), SERVER_DIR, ignore=shutil.ignore_patterns("__pycache__"))
# A short public recording for the smoke test.
SAMPLE_AUDIO = hf_hub_download("Narsil/asr_dummy", "1.flac", repo_type="dataset", local_dir=".")
print("server files:", sorted(os.listdir(SERVER_DIR)))

# COMMAND ----------

# MAGIC %md ## 3. Smoke test: start the server here and transcribe the sample
# MAGIC The notebook cannot use port 8080, so the test runs the same server on port 3080.

# COMMAND ----------

import signal
import subprocess
import time

import requests

LOCAL_URL = "http://localhost:3080"

def server_log():
    return open("server.log").read()[-5000:]

def is_ready():
    try:
        return requests.get(LOCAL_URL + "/v1/models", timeout=2).ok
    except requests.ConnectionError:
        return False

with open("server.log", "w") as log:
    server = subprocess.Popen(server_command(3080), shell=True, stdout=log, stderr=log, start_new_session=True)

def stop_server():
    if server.poll() is None:
        os.killpg(server.pid, signal.SIGTERM)

try:
    deadline = time.time() + 30 * 60
    while not is_ready():
        if server.poll() is not None:
            raise RuntimeError("server stopped with exit code %s:\n%s" % (server.returncode, server_log()))
        if time.time() > deadline:
            raise RuntimeError("server did not load within 30 minutes:\n%s" % server_log())
        time.sleep(5)
    with open(SAMPLE_AUDIO, "rb") as audio:
        reply = requests.post(LOCAL_URL + "/v1/audio/transcriptions", files={"file": audio},
                              data={"model": SERVED_MODEL_NAME, "response_format": "verbose_json"}, timeout=180)
    if not reply.ok:
        raise RuntimeError("smoke test failed: HTTP %d %s" % (reply.status_code, reply.text[:500]))
    print("transcript:", reply.json().get("text"))
    print("first words:", reply.json().get("words", [])[:5])
finally:
    stop_server()

if STAGE == "verify":
    dbutils.notebook.exit("verify passed")

# COMMAND ----------

# MAGIC %md ## 4. Log the model and register it to Unity Catalog

# COMMAND ----------

import mlflow
from mlflow.pyfunc.model import ChatCompletionResponse, ChatModel

os.environ["MLFLOW_USE_DATABRICKS_SDK_MODEL_ARTIFACTS_REPO_FOR_UC"] = "true"
os.environ["MLFLOW_HTTP_REQUEST_TIMEOUT"] = "600"
os.environ["MLFLOW_ARTIFACT_UPLOAD_DOWNLOAD_TIMEOUT"] = "600"
mlflow.set_registry_uri("databricks-uc")

# The endpoint runs the server, so this class is never called. MLflow still needs a model class to log.
class Placeholder(ChatModel):
    def predict(self, context, messages, params):
        return ChatCompletionResponse.from_dict({"choices": []})

model_info = mlflow.pyfunc.log_model(
    name=SERVED_MODEL_NAME.replace(".", "_"),
    python_model=Placeholder(),
    artifacts={"model_dir": MODEL_DIR, "server": SERVER_DIR},
    metadata={"task": "llm/v1/audio/transcriptions", "entrypoint": server_command(8080)},
    extra_pip_requirements=["mlflow==3.15.0"],
)
version = mlflow.register_model(model_info.model_uri, UC_MODEL_NAME, env_pack="databricks_model_serving").version
print(UC_MODEL_NAME, "version", version)

# COMMAND ----------

# MAGIC %md ## 5. Create or update the endpoint (GPU, scale-to-zero per the widget, not route-optimized)

# COMMAND ----------

from datetime import timedelta

from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput, ServingModelWorkloadType

served_entities = [ServedEntityInput(
    entity_name=UC_MODEL_NAME,
    entity_version=str(version),
    workload_type=ServingModelWorkloadType(workload_type),
    workload_size=workload_size,
    scale_to_zero_enabled=SCALE_TO_ZERO,
)]
existing = {e.name for e in w.serving_endpoints.list()}
if endpoint_name in existing:
    w.serving_endpoints.update_config_and_wait(name=endpoint_name, served_entities=served_entities,
                                               timeout=timedelta(minutes=45))
else:
    w.serving_endpoints.create_and_wait(name=endpoint_name, config=EndpointCoreConfigInput(
        name=endpoint_name, served_entities=served_entities), timeout=timedelta(minutes=45))
print("endpoint", endpoint_name, "is up; waiting until it transcribes")

# COMMAND ----------

# MAGIC %md ## 6. Wait until the endpoint transcribes
# MAGIC READY is not servable: the container can pass its health check while the model loads, so poll
# MAGIC the audio route until it returns 200 (404 and 503 mean "not yet").

# COMMAND ----------

host = w.config.host.rstrip("/")
AUDIO_URL = f"{host}/serving-endpoints/{endpoint_name}/audio/transcriptions"
deadline = time.time() + 15 * 60
while True:
    with open(SAMPLE_AUDIO, "rb") as audio:
        reply = requests.post(AUDIO_URL, headers=w.config.authenticate(), files={"file": audio},
                              data={"model": SERVED_MODEL_NAME, "response_format": "verbose_json"}, timeout=180)
    if reply.status_code == 200 and reply.json().get("text"):
        break
    if time.time() > deadline:
        raise RuntimeError(f"endpoint did not transcribe: HTTP {reply.status_code} {reply.text[:300]}")
    time.sleep(15)
print("Endpoint is transcribing:", reply.json()["text"][:120])
