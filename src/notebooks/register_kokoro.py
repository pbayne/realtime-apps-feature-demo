# Databricks notebook source
# MAGIC %md
# MAGIC # Call sidekick — register Kokoro TTS and deploy the GPU endpoint
# MAGIC
# MAGIC Ported from `reference/custom-llm-serving-solutions/audio-serving/kokoro-tts/01-serving.ipynb` (SPEC 3.3a).
# MAGIC `hexgrad/Kokoro-82M` is logged as a **chat** model (Custom LLM Serving has no text-to-speech task). The entrypoint
# MAGIC starts `WORKERS` Kokoro servers (`kokoro_serving/kokoro_server.py`) and the adapter (`kokoro_serving/adapter.py`) on
# MAGIC port 8080. The adapter turns a chat request into speech and streams base64 16-bit PCM at 24 kHz.
# MAGIC
# MAGIC Endpoint: `GPU_MEDIUM` (A10G on AWS), workload `Small`. Scale-to-zero comes from the `gpu_scale_to_zero`
# MAGIC widget (default `true` for dev cost). Warm the endpoint before a demo (`scripts/warmup_voice.py`) or set
# MAGIC `gpu_scale_to_zero=false`: with it on, the first voice turn after idle waits minutes for a cold GPU.
# MAGIC
# MAGIC ## Contract (what the app sends and reads)
# MAGIC Request: `POST {host}/serving-endpoints/{endpoint}/invocations` with
# MAGIC `{"messages":[{"role":"user","content":"<one sentence>"}],"voice":"af_heart","stream":true}`.
# MAGIC
# MAGIC Response: SSE. The first `delta.content` is a header `#audio/pcm;rate=24000;channels=1;format=s16le;...` (skipped).
# MAGIC Each later chunk is base64 PCM. The stream ends with `finish_reason: stop` and `[DONE]`.
# MAGIC
# MAGIC ## Stages (widget `stage`)
# MAGIC - `deploy` (default): smoke test, register, create or update the endpoint, wait until it returns audio.
# MAGIC - `verify`: smoke test only.

# COMMAND ----------

# MAGIC %pip install --quiet "kokoro==0.9.4" "misaki[en]==0.9.4" soundfile fastapi uvicorn httpx requests huggingface_hub "mlflow==3.15.0" "databricks-sdk==0.134.0" "https://github.com/explosion/spacy-models/releases/download/en_core_web_sm-3.8.0/en_core_web_sm-3.8.0-py3-none-any.whl"
# MAGIC %restart_python

# COMMAND ----------

dbutils.widgets.text("uc_catalog", "main")
dbutils.widgets.text("uc_schema", "call_sidekick")
dbutils.widgets.text("uc_model", "kokoro_82m")
dbutils.widgets.text("endpoint_name", "sidekick-kokoro")
dbutils.widgets.dropdown("workload_type", "GPU_MEDIUM", ["GPU_SMALL", "GPU_MEDIUM", "GPU_LARGE"])
dbutils.widgets.text("workload_size", "Small")
dbutils.widgets.dropdown("gpu_scale_to_zero", "true", ["true", "false"])   # false = always on (demos, latency)
dbutils.widgets.dropdown("stage", "deploy", ["deploy", "verify"])

catalog = dbutils.widgets.get("uc_catalog").strip()
schema = dbutils.widgets.get("uc_schema").strip()
uc_model = dbutils.widgets.get("uc_model").strip()
endpoint_name = dbutils.widgets.get("endpoint_name").strip()
workload_type = dbutils.widgets.get("workload_type").strip()
workload_size = dbutils.widgets.get("workload_size").strip()
STAGE = dbutils.widgets.get("stage")

SCALE_TO_ZERO = dbutils.widgets.get("gpu_scale_to_zero").strip().lower() == "true"
UC_MODEL_NAME = f"{catalog}.{schema}.{uc_model}"
MODEL_REPO_ID = "hexgrad/Kokoro-82M"
ARTIFACTS_PATH = "model_dir"        # weights and voices; the entrypoint loads it by this name
SERVED_MODEL_NAME = "kokoro-82m"
DEFAULT_VOICE = "af_heart"          # used when a request names no voice
WARM_VOICES = "af_heart,bf_emma"    # one voice per language, so no first request waits for a phonemizer
WORKERS = 4                         # Kokoro worker processes on one GPU (~1 GB each)
MAX_SILENCE_SECONDS = 10            # adapter silence guard; 0 turns it off

SERVING_PORT, SERVING_INTERNAL_PORT = 8080, 8000
LOCAL_PORT, LOCAL_INTERNAL_PORT = 3080, 3081   # notebook ports must be 3000-3999

assert workload_type in ("GPU_SMALL", "GPU_MEDIUM", "GPU_LARGE"), workload_type
print(STAGE, "|", MODEL_REPO_ID, "->", UC_MODEL_NAME, "->", endpoint_name, "|", workload_type)

# COMMAND ----------

# MAGIC %md ## 1. Create the model in Unity Catalog (fails fast on a permission problem)

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import AlreadyExists, DatabricksError, ResourceAlreadyExists

w = WorkspaceClient()
if STAGE == "deploy":
    try:
        w.registered_models.create(catalog_name=catalog, schema_name=schema, name=uc_model)
    except (AlreadyExists, ResourceAlreadyExists):
        pass
    except DatabricksError as error:
        raise RuntimeError(f"Cannot create the model {UC_MODEL_NAME}. Set uc_catalog/uc_schema to a schema "
                           f"you can create models in.\n{error}") from None

# COMMAND ----------

# MAGIC %md ## 2. Download the model and copy the serving code

# COMMAND ----------

import os
import shutil
import tempfile

os.environ["HF_HUB_DISABLE_PROGRESS_BARS"] = "1"
from huggingface_hub import snapshot_download

# This workspace's /Workspace FUSE mount can fail with EIO from serverless compute while the
# Workspace API still reads the same files, so copy the serving code through the API.
import io
import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ExportFormat, ObjectType

def copy_workspace_dir(src, dst, attempts=4):
    w = WorkspaceClient()
    os.makedirs(dst, exist_ok=True)
    for obj in w.workspace.list(src, recursive=True):
        if obj.object_type != ObjectType.FILE or "__pycache__" in obj.path:
            continue
        rel = os.path.relpath(obj.path.removeprefix("/Workspace"), src.removeprefix("/Workspace"))
        target = os.path.join(dst, rel)
        os.makedirs(os.path.dirname(target), exist_ok=True)
        for i in range(attempts):
            try:
                with w.workspace.download(obj.path, format=ExportFormat.AUTO) as f, open(target, "wb") as out:
                    out.write(f.read())
                break
            except Exception as exc:  # transient storage errors: back off and retry
                if i == attempts - 1:
                    raise
                print(f"retry {i + 1} downloading {obj.path}: {type(exc).__name__}")
                time.sleep(2 ** i)

def with_retries(fn, attempts=3, what="call"):
    for i in range(attempts):
        try:
            return fn()
        except Exception as exc:
            if i == attempts - 1:
                raise
            print(f"{what} failed ({type(exc).__name__}: {str(exc)[:160]}); retry {i + 1} in {15 * (i + 1)} s")
            time.sleep(15 * (i + 1))

NOTEBOOK_DIR = "/Workspace" + os.path.dirname(
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())
workdir = tempfile.mkdtemp()
os.chdir(workdir)
snapshot_download(MODEL_REPO_ID, local_dir=ARTIFACTS_PATH, allow_patterns=["*.json", "*.pth", "voices/*"])
print(len(os.listdir(f"{ARTIFACTS_PATH}/voices")), "voices")
# The entrypoint runs serving/kokoro_server.py and serving/adapter.py (copied from kokoro_serving/).
copy_workspace_dir(os.path.join(NOTEBOOK_DIR, "kokoro_serving"), "serving")

# COMMAND ----------

# MAGIC %md ## 3. The entrypoint
# MAGIC One shell command starts `WORKERS` Kokoro servers and the adapter in front of them. If any process exits, the
# MAGIC command exits and the container restarts. The adapter answers `GET /v1/models` with 503 until every worker is
# MAGIC loaded and warmed up, so READY is reported only once the model can speak.

# COMMAND ----------

def entrypoint(port: int = SERVING_PORT, internal_port: int = SERVING_INTERNAL_PORT) -> str:
    workers = " ".join(
        " ".join(["python", "-u", "serving/kokoro_server.py", "--model-dir", ARTIFACTS_PATH,
                  "--port", str(internal_port + i), "--default-voice", DEFAULT_VOICE,
                  "--warm-voices", WARM_VOICES, "&"])
        for i in range(WORKERS))
    upstreams = ",".join(f"http://127.0.0.1:{internal_port + i}" for i in range(WORKERS))
    adapter = " ".join([
        "python", "-u", "serving/adapter.py",
        "--port", str(port),
        "--upstream", upstreams,
        "--model-name", SERVED_MODEL_NAME,
        "--engine", "kokoro",
        "--default-sample-rate", "24000",
        "--max-silence-seconds", str(MAX_SILENCE_SECONDS),
    ])
    return f"bash -c 'export PYTHONUNBUFFERED=1; {workers} {adapter} & wait -n; exit 1'"

print(entrypoint(SERVING_PORT, SERVING_INTERNAL_PORT))

# COMMAND ----------

# MAGIC %md ## 4. Smoke test: start the same entrypoint here and request audio

# COMMAND ----------

import signal
import subprocess
import time

import requests

LOCAL_URL = f"http://localhost:{LOCAL_PORT}"
proc_log = open("process.log", "w")
proc = subprocess.Popen(["bash", "-lc", entrypoint(LOCAL_PORT, LOCAL_INTERNAL_PORT)], stdout=proc_log,
                        stderr=subprocess.STDOUT, text=True, start_new_session=True)
try:
    start = time.time()
    while True:
        try:
            if requests.get(LOCAL_URL + "/v1/models", timeout=2).status_code == 200:
                print(f"[{time.time() - start:.0f}s] ready")
                break
        except requests.RequestException:
            pass
        if proc.poll() is not None:
            raise RuntimeError("server exited:\n" + open("process.log").read()[-4000:])
        time.sleep(10)
    payload = {"messages": [{"role": "user", "content": "Your package should arrive by six this evening."}],
               "voice": DEFAULT_VOICE, "stream": True}
    with requests.post(LOCAL_URL + "/invocations", json=payload, stream=True, timeout=120) as reply:
        reply.raise_for_status()
        pcm_bytes = 0
        for line in reply.iter_lines(decode_unicode=True):
            if line.startswith("data:") and "[DONE]" not in line and '"content"' in line:
                import base64, json
                for choice in json.loads(line[5:]).get("choices", []):
                    content = choice.get("delta", {}).get("content") or ""
                    if content and not content.startswith("#"):
                        pcm_bytes += len(base64.b64decode(content))
    print(f"smoke test: {pcm_bytes / 2 / 24000:.1f} s of audio")
    if pcm_bytes == 0:
        raise RuntimeError("smoke test produced no audio")
finally:
    os.killpg(proc.pid, signal.SIGTERM)
    proc.wait(60)

if STAGE == "verify":
    dbutils.notebook.exit("verify passed: Kokoro-82M streaming OK")

# COMMAND ----------

# MAGIC %md ## 5. Log the model with the entrypoint and register it to Unity Catalog

# COMMAND ----------

import mlflow
from mlflow.pyfunc.model import ChatCompletionResponse, ChatModel
from mlflow.pyfunc.model import ChatModel as _ChatModel  # noqa: F401  (same class; kept for readability)

os.environ["MLFLOW_USE_DATABRICKS_SDK_MODEL_ARTIFACTS_REPO_FOR_UC"] = "true"
mlflow.set_registry_uri("databricks-uc")

class LLMModel(ChatModel):   # placeholder: Serving runs the entrypoint, not predict()
    def predict(self, context, messages, params):
        return ChatCompletionResponse.from_dict({"choices": []})

model_info = with_retries(lambda: mlflow.pyfunc.log_model(
    name=SERVED_MODEL_NAME.replace(".", "_"),
    python_model=LLMModel(),
    artifacts={"model_dir": ARTIFACTS_PATH, "serving": "serving"},
    metadata={"task": "llm/v1/chat", "entrypoint": entrypoint(SERVING_PORT, SERVING_INTERNAL_PORT)},
    extra_pip_requirements=["mlflow==3.15.0"],
), what="log_model")
version = with_retries(lambda: mlflow.register_model(model_info.model_uri, UC_MODEL_NAME, env_pack="databricks_model_serving"), what="register_model").version
print(UC_MODEL_NAME, "version", version)

# COMMAND ----------

# MAGIC %md ## 6. Create or update the endpoint (GPU, scale-to-zero per the widget)

# COMMAND ----------

from datetime import timedelta

from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput, ServingModelWorkloadType

served_entity = ServedEntityInput(
    entity_name=UC_MODEL_NAME,
    entity_version=str(version),
    workload_type=ServingModelWorkloadType(workload_type),
    workload_size=workload_size,
    scale_to_zero_enabled=SCALE_TO_ZERO,
)
if endpoint_name in {e.name for e in w.serving_endpoints.list()}:
    w.serving_endpoints.update_config_and_wait(name=endpoint_name, served_entities=[served_entity],
                                               timeout=timedelta(minutes=45))
else:
    w.serving_endpoints.create_and_wait(name=endpoint_name, config=EndpointCoreConfigInput(
        name=endpoint_name, served_entities=[served_entity]), timeout=timedelta(minutes=45))
print("endpoint", endpoint_name, "is up; waiting until it returns audio")

# COMMAND ----------

# MAGIC %md ## 7. Wait until the endpoint returns audio
# MAGIC READY is not servable. Poll the invocations route until a non-stream request returns a base64 WAV.

# COMMAND ----------

import base64

host = w.config.host.rstrip("/")
INVOKE_URL = f"{host}/serving-endpoints/{endpoint_name}/invocations"
deadline = time.time() + 15 * 60
while True:
    reply = requests.post(INVOKE_URL, headers=w.config.authenticate(), timeout=120,
                          json={"messages": [{"role": "user", "content": "Ready."}]})
    if reply.ok and reply.json()["choices"][0]["message"]["content"]:
        break
    if time.time() > deadline:
        raise RuntimeError(f"endpoint did not return audio: HTTP {reply.status_code} {reply.text[:300]}")
    time.sleep(15)
wav = base64.b64decode(reply.json()["choices"][0]["message"]["content"])
print(f"Endpoint is returning audio ({len(wav)} bytes).")
