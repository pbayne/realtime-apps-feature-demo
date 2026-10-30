import argparse, asyncio, glob, io
import av, numpy as np, torch, uvicorn
import nemo.collections.asr as nemo_asr
from fastapi import FastAPI, File, Form, UploadFile
from fastapi.responses import JSONResponse, PlainTextResponse

RATE = 16000
parser = argparse.ArgumentParser()
parser.add_argument("--model-dir", required=True)
parser.add_argument("--served-model-name", required=True)
parser.add_argument("--port", type=int, default=8080)
parser.add_argument("--batch-size", type=int, default=32)
parser.add_argument("--batch-wait-ms", type=float, default=5)
parser.add_argument("--batch-seconds", type=float, default=1200, help="most audio per GPU batch")
parser.add_argument("--dtype", default="bfloat16", choices=["bfloat16", "float16", "float32"])
parser.add_argument("--local-attention", action="store_true", help="for hour-long files: memory linear in length")
args = parser.parse_args()
app, state = FastAPI(), {"model": None}
STATS = {"requests": 0, "errors": 0, "inflight": 0, "audio_seconds": 0.0, "batches": 0, "batched_files": 0}


def decode(data):
  """Any audio file (wav, flac, mp3, m4a, ogg, ...) as 16 kHz mono float32."""
  with av.open(io.BytesIO(data)) as container:
    resampler = av.AudioResampler(format="flt", layout="mono", rate=RATE)
    frames = [f for packet in container.decode(container.streams.audio[0]) for f in resampler.resample(packet)]
    frames += resampler.resample(None)
  return np.concatenate([f.to_ndarray().reshape(-1) for f in frames]) if frames else np.zeros(0, np.float32)


def transcribe(model, audios, timestamps):
  dtype = getattr(torch, args.dtype)
  STATS["batches"] += 1
  STATS["batched_files"] += len(audios)
  with torch.inference_mode(), torch.autocast("cuda", dtype=dtype, enabled=dtype != torch.float32):
    return model.transcribe(audios, batch_size=len(audios), timestamps=timestamps, verbose=False)


def load():
  model = nemo_asr.models.ASRModel.restore_from(glob.glob(args.model_dir + "/*.nemo")[0], map_location="cuda").eval()
  if args.local_attention:
    model.change_attention_model("rel_pos_local_attn", [256, 256])
    model.change_subsampling_conv_chunking_factor(1)
  for batch in (1, 8):  # warm up kernels for single and batched requests
    transcribe(model, [np.zeros(RATE * 5, np.float32)] * batch, False)
  STATS.update(batches=0, batched_files=0)
  return model


async def batcher(queue):
  """Collect requests that arrive within batch-wait-ms into one GPU batch."""
  loop = asyncio.get_running_loop()
  while True:
    items = [await queue.get()]
    deadline = loop.time() + args.batch_wait_ms / 1000
    while len(items) < args.batch_size and sum(len(i[0]) for i in items) < args.batch_seconds * RATE:
      try:
        items.append(await asyncio.wait_for(queue.get(), max(deadline - loop.time(), 0)))
      except asyncio.TimeoutError:
        break
    for timestamps in {i[1] for i in items}:  # NeMo takes one timestamps flag per call
      group = [i for i in items if i[1] == timestamps]
      try:
        results = await asyncio.to_thread(transcribe, state["model"], [i[0] for i in group], timestamps)
        for item, result in zip(group, results):
          item[2].set_result(result)
      except Exception as error:
        for item in group:
          item[2].set_exception(error)


@app.on_event("startup")
async def startup():
  app.state.queue = asyncio.Queue()
  asyncio.create_task(batcher(app.state.queue))
  async def load_in_background():
    state["model"] = await asyncio.to_thread(load)
    print("model loaded", flush=True)
  asyncio.create_task(load_in_background())


@app.get("/v1/models")
@app.get("/health")
async def models():
  if state["model"] is None:
    return JSONResponse({"error": {"message": "model is still loading"}}, status_code=503)
  return {"object": "list", "data": [{"id": args.served_model_name, "object": "model", "owned_by": "nvidia"}]}


@app.get("/metrics")
async def metrics():
  return PlainTextResponse("".join("asr_%s %s\n" % (name, value) for name, value in STATS.items()))


@app.post("/v1/audio/transcriptions")
@app.post("/invocations")
async def transcriptions(file: UploadFile = File(...), model: str = Form(None), language: str = Form(None),
                         response_format: str = Form("json"), prompt: str = Form(None), temperature: float = Form(None)):
  if state["model"] is None:
    return JSONResponse({"error": {"message": "model is still loading"}}, status_code=503)
  try:
    audio = await asyncio.to_thread(decode, await file.read())
  except Exception as error:
    return JSONResponse({"error": {"message": "could not decode audio: %s" % error}}, status_code=400)
  if not len(audio):
    return JSONResponse({"error": {"message": "empty audio"}}, status_code=400)
  future = asyncio.get_running_loop().create_future()
  STATS["requests"] += 1
  STATS["inflight"] += 1
  try:
    await app.state.queue.put((audio, response_format == "verbose_json", future))
    result = await future
  except Exception:
    STATS["errors"] += 1
    raise
  finally:
    STATS["inflight"] -= 1
  STATS["audio_seconds"] += len(audio) / RATE
  if response_format == "text":
    return PlainTextResponse(result.text)
  if response_format == "verbose_json":
    stamps = getattr(result, "timestamp", None) or {}
    return {"task": "transcribe", "language": language or "auto", "duration": round(len(audio) / RATE, 3),
            "text": result.text,
            "segments": [{"id": i, "start": s["start"], "end": s["end"], "text": s["segment"]}
                         for i, s in enumerate(stamps.get("segment", []))],
            "words": [{"word": s["word"], "start": s["start"], "end": s["end"]} for s in stamps.get("word", [])]}
  return {"text": result.text}


uvicorn.run(app, host="0.0.0.0", port=args.port)
