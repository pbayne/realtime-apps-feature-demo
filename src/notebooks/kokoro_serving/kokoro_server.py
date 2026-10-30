"""OpenAI-style /v1/audio/speech server for Kokoro-82M (hexgrad/Kokoro-82M), streaming per sentence.

  POST /v1/audio/speech  {input, voice, speed, response_format, stream}
       stream=true + response_format=pcm: raw 16-bit mono PCM at 24 kHz, one piece per sentence,
       with x-sample-rate / x-channels headers (what serving/adapter.py expects)
       otherwise: a whole wav (or pcm) file
  GET  /v1/audio/voices, /v1/models, /health

Text is split into sentences and synthesized one at a time, so the first audio arrives after
one sentence instead of the whole input. Requests share the GPU through a lock held per sentence,
so concurrent streams interleave sentence by sentence.

Voices are the .pt files under <model_dir>/voices; the first letter of a voice picks the language
(a: American English, b: British English, e: Spanish, f: French, h: Hindi, i: Italian, p: Portuguese).
"""

import argparse
import asyncio
import io
import os
import re
import threading
import time
import wave

import numpy as np
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse

SAMPLE_RATE = 24000
app = FastAPI()
state = {"model": None, "ready": False, "pipelines": {}, "model_dir": "model_dir", "default_voice": "af_heart"}
gpu_lock = threading.Lock()  # one sentence on the GPU at a time; streams interleave between sentences


def voices():
  folder = os.path.join(state["model_dir"], "voices")
  return sorted(f[:-3] for f in os.listdir(folder) if f.endswith(".pt"))


def pipeline(lang_code):
  from kokoro import KPipeline
  if lang_code not in state["pipelines"]:
    try:
      state["pipelines"][lang_code] = KPipeline(lang_code=lang_code, repo_id="hexgrad/Kokoro-82M", model=state["model"])
    except ImportError as error:  # Japanese and Chinese need misaki[ja] / misaki[zh], not installed by default
      raise ValueError("voices starting with %r need an extra package: %s" % (lang_code, error)) from None
  return state["pipelines"][lang_code]


def sentences(text):
  """Split on sentence ends and line breaks; keep very short pieces with the next one."""
  parts = [p.strip() for p in re.split(r"(?<=[.!?。！？])\s+|\n+", text.strip()) if p.strip()]
  merged = []
  for part in parts:
    if merged and len(merged[-1]) < 20:
      merged[-1] += " " + part
    else:
      merged.append(part)
  return merged


def synthesize(text, voice, speed):
  """Yield 16-bit PCM bytes, one piece per sentence."""
  voice_path = os.path.join(state["model_dir"], "voices", voice + ".pt")
  if not os.path.exists(voice_path):
    raise ValueError("unknown voice %r; GET /v1/audio/voices lists them" % voice)
  pipe = pipeline(voice[0])
  for sentence in sentences(text):
    with gpu_lock:
      chunks = [r.audio for r in pipe(sentence, voice=voice_path, speed=speed, split_pattern=None) if r.audio is not None]
    if chunks:
      audio = np.concatenate([c.cpu().numpy() for c in chunks])
      yield (np.clip(audio, -1, 1) * 32767).astype("<i2").tobytes()


def to_wav(pcm):
  buffer = io.BytesIO()
  with wave.open(buffer, "wb") as f:
    f.setnchannels(1); f.setsampwidth(2); f.setframerate(SAMPLE_RATE); f.writeframes(pcm)
  return buffer.getvalue()


async def iterate(generator):
  """Run a blocking generator in a worker thread, yielding its items to the event loop."""
  loop, queue, done = asyncio.get_running_loop(), asyncio.Queue(), object()

  def run():
    try:
      for item in generator:
        loop.call_soon_threadsafe(queue.put_nowait, item)
    except Exception as error:
      loop.call_soon_threadsafe(queue.put_nowait, error)
    loop.call_soon_threadsafe(queue.put_nowait, done)

  threading.Thread(target=run, daemon=True).start()
  while True:
    item = await queue.get()
    if item is done:
      return
    if isinstance(item, Exception):
      raise item
    yield item


@app.post("/v1/audio/speech")
async def speech(request: Request):
  if not state["ready"]:
    return JSONResponse({"error": {"message": "model is still loading"}}, status_code=503)
  body = await request.json()
  text = (body.get("input") or "").strip()
  voice = body.get("voice") or state["default_voice"]
  voice = state["default_voice"] if voice == "default" else voice
  speed = float(body.get("speed") or 1.0)
  if not text:
    return JSONResponse({"error": {"message": "input is required"}}, status_code=400)
  headers = {"x-sample-rate": str(SAMPLE_RATE), "x-channels": "1", "x-bit-depth": "16"}
  try:
    pieces = iterate(synthesize(text, voice, speed))
    first = await pieces.__anext__()  # raise errors (unknown voice) as HTTP errors, before streaming
  except StopAsyncIteration:
    first = b""
  except ValueError as error:
    return JSONResponse({"error": {"message": str(error), "type": "invalid_request_error"}}, status_code=400)
  if body.get("stream"):
    async def stream():
      yield first
      async for piece in pieces:
        yield piece
    return StreamingResponse(stream(), media_type="audio/pcm", headers=headers)
  pcm = first + b"".join([piece async for piece in pieces])
  if body.get("response_format", "wav") == "pcm":
    return Response(pcm, media_type="audio/pcm", headers=headers)
  return Response(to_wav(pcm), media_type="audio/wav", headers=headers)


@app.get("/v1/audio/voices")
async def list_voices():
  return {"voices": voices(), "default": state["default_voice"]}


@app.get("/v1/models")
@app.get("/health")
async def health():
  if not state["ready"]:
    return JSONResponse({"error": {"message": "model is still loading"}}, status_code=503)
  return {"object": "list", "data": [{"id": "kokoro-82m", "object": "model", "owned_by": "hexgrad"}]}


def load(model_dir, warm_voices):
  import torch
  from kokoro import KModel
  device = "cuda" if torch.cuda.is_available() else "cpu"
  weights = [f for f in os.listdir(model_dir) if f.endswith(".pth")][0]
  model = KModel(repo_id="hexgrad/Kokoro-82M", config=os.path.join(model_dir, "config.json"),
                 model=os.path.join(model_dir, weights)).to(device).eval()
  state["model"] = model
  # Build each language's pipeline and run it twice before serving traffic: the first calls load the
  # phonemizer (several seconds), so without this the first users of each language would wait.
  for voice in warm_voices:
    for text in ("Warming up the speech model.", "Ready to serve requests now."):
      for _ in synthesize(text, voice, 1.0):
        pass
  state["ready"] = True


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("--model-dir", default="model_dir")
  parser.add_argument("--host", default="127.0.0.1")
  parser.add_argument("--port", type=int, default=8000)
  parser.add_argument("--default-voice", default="af_heart")
  parser.add_argument("--warm-voices", default="af_heart,bf_emma", help="comma-separated; one per language to preload")
  args = parser.parse_args()
  state.update(model_dir=args.model_dir, default_voice=args.default_voice)

  @app.on_event("startup")
  async def startup():
    async def run():
      t0 = time.time()
      await asyncio.to_thread(load, args.model_dir, [v for v in args.warm_voices.split(",") if v])
      print("Kokoro loaded in %.1f s" % (time.time() - t0), flush=True)
    asyncio.create_task(run())

  uvicorn.run(app, host=args.host, port=args.port, log_level="info")


if __name__ == "__main__":
  main()
