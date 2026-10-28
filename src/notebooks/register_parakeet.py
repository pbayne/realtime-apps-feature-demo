# Databricks notebook source
# MAGIC %md
# MAGIC # Call sidekick — register Parakeet STT and deploy a GPU endpoint
# MAGIC
# MAGIC Wraps `nvidia/parakeet-tdt-0.6b-v3` (NeMo) as an MLflow pyfunc, registers it
# MAGIC in Unity Catalog, and creates a **route-optimized GPU Model Serving endpoint**
# MAGIC (default `GPU_MEDIUM`, A10G-class, workload size Small, scale-to-zero off).
# MAGIC
# MAGIC ## Request / response contract
# MAGIC
# MAGIC Request (`dataframe_records` format, one record per audio window):
# MAGIC ```json
# MAGIC {"dataframe_records": [{
# MAGIC   "audio_b64": "<base64 of little-endian PCM16, mono, 16000 Hz>",
# MAGIC   "offset_ms": 12340,
# MAGIC   "channel": "customer"
# MAGIC }]}
# MAGIC ```
# MAGIC - The window is a rolling window of **at most 30 s** (SPEC: 12 s, re-decoded every 0.4 s).
# MAGIC - `offset_ms` (optional, default 0) is added to every word timestamp so the app
# MAGIC   can map words back to the call timeline.
# MAGIC - `channel` is echoed back (`agent` | `customer`), optional.
# MAGIC
# MAGIC Response:
# MAGIC ```json
# MAGIC {"predictions": [{
# MAGIC   "text": "i want to think about the fees",
# MAGIC   "words": [{"word": "i", "start_ms": 12340, "end_ms": 12420}, ...],
# MAGIC   "audio_ms": 4300,
# MAGIC   "channel": "customer",
# MAGIC   "model": "nvidia/parakeet-tdt-0.6b-v3"
# MAGIC }]}
# MAGIC ```
# MAGIC
# MAGIC ## How the app calls it (route-optimized)
# MAGIC
# MAGIC Route-optimized endpoints use a per-endpoint data-plane URL, not
# MAGIC `/serving-endpoints/<name>/invocations` on the workspace host. The last cell
# MAGIC prints the URL under `data_plane_info`. Auth is **OAuth M2M** (service
# MAGIC principal client credentials, or the App's service principal), not a PAT.
# MAGIC See README "STT endpoint" for the token exchange.

# COMMAND ----------

# MAGIC %pip install --quiet "nemo_toolkit[asr]>=2.3,<3" "mlflow>=2.19" soundfile "databricks-sdk>=0.40"

# COMMAND ----------

dbutils.widgets.text("uc_catalog", "main")
dbutils.widgets.text("uc_schema", "call_sidekick")
dbutils.widgets.text("model_name", "parakeet_tdt_v3")
dbutils.widgets.text("endpoint_name", "parakeet-live")
dbutils.widgets.text("workload_type", "GPU_MEDIUM")     # A10G-class
dbutils.widgets.text("workload_size", "Small")
dbutils.widgets.text("hf_repo", "nvidia/parakeet-tdt-0.6b-v3")
dbutils.widgets.text("hf_file", "parakeet-tdt-0.6b-v3.nemo")

catalog = dbutils.widgets.get("uc_catalog")
schema = dbutils.widgets.get("uc_schema")
model_name = dbutils.widgets.get("model_name")
endpoint_name = dbutils.widgets.get("endpoint_name")
workload_type = dbutils.widgets.get("workload_type")
workload_size = dbutils.widgets.get("workload_size")
hf_repo = dbutils.widgets.get("hf_repo")
hf_file = dbutils.widgets.get("hf_file")

UC_MODEL = f"{catalog}.{schema}.{model_name}"
assert workload_type in ("GPU_SMALL", "GPU_MEDIUM", "GPU_LARGE", "MULTIGPU_MEDIUM"), workload_type

# COMMAND ----------

# MAGIC %md ## 1. Fetch the NeMo checkpoint
# MAGIC
# MAGIC The `.nemo` archive is downloaded once and logged as an MLflow artifact, so the
# MAGIC serving container does not need Hugging Face egress at startup.

# COMMAND ----------

import os
from huggingface_hub import hf_hub_download

nemo_path = hf_hub_download(repo_id=hf_repo, filename=hf_file)
print(f"checkpoint: {nemo_path} ({os.path.getsize(nemo_path) / 1e9:.2f} GB)")

# COMMAND ----------

# MAGIC %md ## 2. Pyfunc wrapper

# COMMAND ----------

import base64
import json
import tempfile

import mlflow
import numpy as np
import soundfile as sf

SAMPLE_RATE = 16000
MAX_WINDOW_S = 30.0


class ParakeetPyfunc(mlflow.pyfunc.PythonModel):
    """PCM16 16 kHz mono window -> words with timestamps (Parakeet TDT, NeMo)."""

    def load_context(self, context):
        import torch
        from nemo.collections.asr.models import ASRModel

        self.model = ASRModel.restore_from(context.artifacts["nemo"], map_location="cuda" if torch.cuda.is_available() else "cpu")
        self.model.eval()
        self.model_name = "nvidia/parakeet-tdt-0.6b-v3"  # literal: no notebook globals in the pickled class

    def _decode_pcm16(self, audio_b64):
        raw = base64.b64decode(audio_b64)
        if len(raw) % 2:
            raise ValueError("PCM16 payload has odd byte length")
        pcm = np.frombuffer(raw, dtype="<i2").astype(np.float32) / 32768.0
        dur = len(pcm) / SAMPLE_RATE
        if dur <= 0:
            raise ValueError("empty audio window")
        if dur > MAX_WINDOW_S + 0.01:
            raise ValueError(f"window is {dur:.1f}s; maximum is {MAX_WINDOW_S}s")
        return pcm, dur

    def _transcribe(self, pcm):
        # NeMo's timestamped transcribe() takes file paths; write a short-lived wav.
        with tempfile.NamedTemporaryFile(suffix=".wav", delete=True) as fh:
            sf.write(fh.name, pcm, SAMPLE_RATE, subtype="PCM_16")
            out = self.model.transcribe([fh.name], timestamps=True)
        hyp = out[0] if isinstance(out, (list, tuple)) else out
        text = getattr(hyp, "text", hyp if isinstance(hyp, str) else "")
        ts = getattr(hyp, "timestamp", None) or {}
        words = []
        for w in ts.get("word", []):
            words.append({
                "word": w.get("word", ""),
                "start": float(w.get("start", 0.0)),
                "end": float(w.get("end", 0.0)),
            })
        return text.strip(), words

    def predict(self, context, model_input, params=None):
        records = model_input.to_dict(orient="records") if hasattr(model_input, "to_dict") else list(model_input)
        results = []
        for rec in records:
            pcm, dur = self._decode_pcm16(rec["audio_b64"])
            offset_ms = int(rec.get("offset_ms") or 0)
            text, words = self._transcribe(pcm)
            results.append({
                "text": text,
                "words": [
                    {"word": w["word"],
                     "start_ms": offset_ms + int(round(w["start"] * 1000)),
                     "end_ms": offset_ms + int(round(w["end"] * 1000))}
                    for w in words
                ],
                "audio_ms": int(round(dur * 1000)),
                "channel": rec.get("channel"),
                "model": self.model_name,
            })
        return {"predictions": results}


# COMMAND ----------

# MAGIC %md ## 3. Smoke test locally on the driver (GPU cluster)
# MAGIC
# MAGIC Synthesizes one second of silence. That only proves the path runs. Use a real
# MAGIC recording from `src/app/data/replay/` for a quality check.

# COMMAND ----------

import mlflow.pyfunc  # noqa: F401  (ensure pyfunc is loaded before the model class is pickled)

_ctx = type("Ctx", (), {"artifacts": {"nemo": nemo_path}})()
_model = ParakeetPyfunc()
_model.load_context(_ctx)
_silence = base64.b64encode(np.zeros(SAMPLE_RATE, dtype="<i2").tobytes()).decode()
print(_model.predict(None, [{"audio_b64": _silence, "offset_ms": 0, "channel": "agent"}]))

# COMMAND ----------

# MAGIC %md ## 4. Log and register to Unity Catalog

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(f"/Shared/call-sidekick/{model_name}")

from mlflow.models.signature import ModelSignature
from mlflow.types import ColSpec, Schema

signature = ModelSignature(
    inputs=Schema([ColSpec("string", "audio_b64"), ColSpec("long", "offset_ms"), ColSpec("string", "channel")]),
    outputs=None,
)

with mlflow.start_run(run_name="parakeet-tdt-0.6b-v3") as run:
    info = mlflow.pyfunc.log_model(
        artifact_path="model",
        python_model=ParakeetPyfunc(),
        artifacts={"nemo": nemo_path},
        pip_requirements=[
            "nemo_toolkit[asr]>=2.3,<3",
            "torch",
            "soundfile",
            "numpy",
        ],
        signature=signature,
        registered_model_name=UC_MODEL,
    )
    print(f"registered {UC_MODEL} from run {run.info.run_id}")

# COMMAND ----------

# MAGIC %md ## 5. Create the route-optimized GPU endpoint (idempotent)
# MAGIC
# MAGIC `route_optimized=True` gives the low-latency data plane. `scale_to_zero_enabled=False`
# MAGIC keeps it warm: a cold start would blow the 0.8 s first-word budget (SPEC section 7).
# MAGIC Re-running with an existing endpoint updates its served model version instead.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput

w = WorkspaceClient()

client = mlflow.tracking.MlflowClient()
version = max(int(v.version) for v in client.search_model_versions(f"name='{UC_MODEL}'"))

served = ServedEntityInput(
    entity_name=UC_MODEL,
    entity_version=str(version),
    workload_type=workload_type,
    workload_size=workload_size,
    scale_to_zero_enabled=False,
)

existing = None
try:
    existing = w.serving_endpoints.get(endpoint_name)
except Exception:
    existing = None

if existing is None:
    ep = w.serving_endpoints.create_and_wait(
        name=endpoint_name,
        route_optimized=True,
        config=EndpointCoreConfigInput(served_entities=[served]),
    )
    print(f"created endpoint {endpoint_name}")
else:
    w.serving_endpoints.update_config_and_wait(
        name=endpoint_name,
        served_entities=[served],
    )
    print(f"updated endpoint {endpoint_name} to version {version}")

# COMMAND ----------

# MAGIC %md ## 6. Print the data-plane URL for the app (route-optimized)

# COMMAND ----------

ep = w.serving_endpoints.get(endpoint_name)
dpi = ep.as_dict().get("data_plane_info") or {}
print(json.dumps(dpi, indent=2))
# Expected: data_plane_info.query_info.endpoint_url (the URL the app POSTs to).
# Store it in the secret scope as `parakeet_endpoint_url` if the app will read it from there.
