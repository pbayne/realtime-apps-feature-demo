"""Chat-completions adapter in front of a /v1/audio/speech server: SGLang-Omni, vLLM-Omni or Kokoro.

Databricks Custom LLM Serving only forwards the route for its declared task, and has no
text-to-speech task. This server is logged with task llm/v1/chat: it accepts a chat request,
calls the speech engine, and returns the audio as base64 in the chat response.

Streaming (stream=true) returns chat.completion.chunk SSE events:
  1st chunk  delta.content = "#audio/pcm;rate=<hz>;channels=1;format=s16le;opts=<a+b>;inflight=<n>\n"
             (header; inflight = requests in this container when this one started, to measure load)
  next       delta.content = base64 of raw 16-bit mono PCM (a whole number of samples)
  last       finish_reason = "stop", usage.completion_tokens = audio milliseconds
Non-streaming returns one message whose content is base64 of the whole file (wav by default).

TTS options can be top-level request fields (OpenAI client: extra_body={...}), or under
"custom_inputs", or a JSON object as the system message, in case the serving proxy drops
unknown fields. Supported: voice, references, response_format, speed, temperature, top_p,
top_k, max_new_tokens, previous_takes. A reference may carry "audio_base64" instead of
"audio_path", because the endpoint may not reach the internet.

references use SGLang-Omni's shape, [{"audio_path" or "audio_base64", "text"}]. With
--engine vllm-omni the first reference becomes vLLM-Omni's ref_audio and ref_text.

Silence guard: Drama-3 occasionally (~1% of long generations on SGLang-Omni, more on vLLM-Omni)
stops speaking but keeps emitting silence until its token cap. After speech has started, a stream
that goes silent for more than --max-silence-seconds is ended there (finish_reason "stop"), and a
non-streaming request with such a gap is generated again once. 0 turns the guard off.

--upstream can list several engine servers (comma-separated, e.g. several Kokoro worker processes on
one GPU); each request goes to the one with the fewest requests in flight.

GET /metrics serves Prometheus counters (requests, errors, in-flight, time to first audio, audio
seconds), which Model Serving can export to the endpoint's telemetry tables.
"""

import argparse
import base64
import json
import os
import tempfile
import time
import uuid

import httpx
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse, StreamingResponse

TTS_FIELDS = ("voice", "references", "response_format", "speed", "temperature", "top_p",
              "top_k", "max_new_tokens", "previous_takes")
CHUNK_BYTES = 9600  # 16-bit mono PCM; 100 ms at 48 kHz. Must stay even (whole samples).

app = FastAPI()
UPSTREAMS = []  # engine base URLs
LOAD = {}       # requests in flight per upstream
MODEL_NAME = ""
DEFAULT_RATE = 44100
UPSTREAM_MODEL = None
ENGINE = "sglang-omni"
client: httpx.AsyncClient
STATS = {"requests": 0, "errors": 0, "inflight": 0, "inflight_max": 0, "ttfa_sum": 0.0, "ttfa_count": 0,
         "audio_seconds": 0.0, "silence_cutoffs": 0, "silence_retries": 0}
MAX_SILENCE_SECONDS = 10.0
SILENCE_LEVEL = 0.003  # RMS (full scale = 1) below which 100 ms of audio counts as silence


class SilenceGuard:
  """Tracks how long the audio has been silent since speech last appeared, 100 ms at a time."""
  def __init__(self, rate):
    self.window, self.buffer, self.spoke, self.silent = int(rate * 0.1) * 2, b"", False, 0.0

  def feed(self, pcm):
    """Add PCM; return True when the audio has been silent for too long after speech."""
    import numpy as np
    self.buffer += pcm
    while len(self.buffer) >= self.window:
      chunk, self.buffer = self.buffer[:self.window], self.buffer[self.window:]
      samples = np.frombuffer(chunk, dtype="<i2").astype(np.float32)
      rms = float(np.sqrt(np.mean(samples * samples))) / 32768
      if rms >= SILENCE_LEVEL:
        self.spoke, self.silent = True, 0.0
      elif self.spoke:
        self.silent += 0.1
    return bool(MAX_SILENCE_SECONDS) and self.spoke and self.silent > MAX_SILENCE_SECONDS


def _pick_upstream():
  return min(UPSTREAMS, key=lambda u: LOAD[u])


class Tracked:
  """Counts a request as in flight from the time it arrives until its response is complete."""
  def __enter__(self):
    STATS["requests"] += 1
    STATS["inflight"] += 1
    STATS["inflight_max"] = max(STATS["inflight_max"], STATS["inflight"])
    self.inflight, self.start = STATS["inflight"], time.time()
    return self

  def __exit__(self, kind, *_):
    STATS["inflight"] -= 1
    STATS["errors"] += kind is not None


def _text(content):
  if isinstance(content, str):
    return content
  # OpenAI content parts: [{"type": "text", "text": ...}, ...]
  return "".join(p.get("text", "") for p in content or [] if isinstance(p, dict))


def _speech_request(body):
  """Map a chat request to a /v1/audio/speech request for the upstream engine."""
  messages = body.get("messages") or []
  users = [m for m in messages if m.get("role") == "user"]
  if not users:
    raise ValueError("send the text to speak as a user message")
  options = {}
  for m in messages:
    if m.get("role") == "system":
      try:
        options.update(json.loads(_text(m.get("content"))))
      except (TypeError, ValueError):
        pass
  options.update(body.get("custom_inputs") or {})
  options.update({k: body[k] for k in TTS_FIELDS if k in body})

  speech = {"input": _text(users[-1].get("content"))}
  speech.update({k: options[k] for k in TTS_FIELDS if k in options})
  if ENGINE == "vllm-omni":
    references = speech.pop("references", None) or []
    speech.pop("previous_takes", None)  # SGLang-Omni only
    if references:
      ref = references[0]
      if "audio_base64" in ref:
        speech["ref_audio"] = "data:audio/wav;base64," + ref["audio_base64"]
      else:
        path = ref["audio_path"]
        speech["ref_audio"] = path if "://" in path else "file://" + os.path.abspath(path)
      speech["ref_text"] = ref.get("text", "")
  else:
    for ref in speech.get("references") or []:
      if "audio_base64" in ref:
        with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as f:
          f.write(base64.b64decode(ref.pop("audio_base64")))
        ref["audio_path"] = f.name
  return speech, sorted(k for k in TTS_FIELDS if k in options)


async def _upstream_model():
  """The model id the engine expects in /v1/audio/speech, read once from its /v1/models."""
  global UPSTREAM_MODEL
  if UPSTREAM_MODEL is None:
    try:
      UPSTREAM_MODEL = (await client.get(UPSTREAMS[0] + "/v1/models", timeout=5)).json()["data"][0]["id"]
    except (httpx.HTTPError, ValueError, KeyError, IndexError):
      UPSTREAM_MODEL = MODEL_NAME
  return UPSTREAM_MODEL


def _sample_rate(headers):
  # The engines report the PCM sample rate in a response header (vLLM-Omni: x-audio-sample-rate;
  # SGLang-Omni's name is not documented), so take the first header that mentions the sample rate.
  for name, value in headers.items():
    if "sample" in name.lower() and "rate" in name.lower():
      try:
        return int(value)
      except ValueError:
        pass
  return DEFAULT_RATE


def _long_silence(wav_bytes):
  """True if a wav has more than MAX_SILENCE_SECONDS of silence after speech starts."""
  import io, wave
  try:
    with wave.open(io.BytesIO(wav_bytes)) as f:
      rate, pcm = f.getframerate(), f.readframes(f.getnframes())
  except wave.Error:
    return False
  return SilenceGuard(rate).feed(pcm)


def _chunk(cid, created, delta, finish=None, usage=None):
  event = {"id": cid, "object": "chat.completion.chunk", "created": created, "model": MODEL_NAME,
           "choices": [{"index": 0, "delta": delta, "finish_reason": finish}]}
  if usage:
    event["usage"] = usage
  return "data: %s\n\n" % json.dumps(event)


def _usage(text, audio_ms):
  return {"prompt_tokens": len(text), "completion_tokens": audio_ms, "total_tokens": len(text) + audio_ms}


async def _stream(speech, received):
  speech.update({"stream": True, "response_format": "pcm", "stream_format": "audio"})
  cid, created = "chatcmpl-" + uuid.uuid4().hex, int(time.time())
  upstream = _pick_upstream()
  LOAD[upstream] += 1
  try:
    async for event in _stream_from(upstream, speech, received, cid, created):
      yield event
  finally:
    LOAD[upstream] -= 1


async def _stream_from(upstream, speech, received, cid, created):
  with Tracked() as tracked:
    async with client.stream("POST", upstream + "/v1/audio/speech", json=speech) as reply:
      if reply.status_code != 200:
        STATS["errors"] += 1
        error = (await reply.aread()).decode(errors="replace")[:1000]
        yield _chunk(cid, created, {"role": "assistant", "content": ""}, "error")
        yield "data: %s\n\n" % json.dumps({"error": {"message": error, "code": reply.status_code}})
        yield "data: [DONE]\n\n"
        return
      rate = _sample_rate(reply.headers)
      # opts lists the TTS options that reached this server, to check what the proxy passes on;
      # inflight shows how many requests the proxy lets into this container at once.
      header = "#audio/pcm;rate=%d;channels=1;format=s16le;opts=%s;inflight=%d\n" % (
        rate, "+".join(received), tracked.inflight)
      yield _chunk(cid, created, {"role": "assistant", "content": header})
      pending, total, guard = b"", 0, SilenceGuard(rate)
      async for data in reply.aiter_bytes():
        if guard.feed(data):  # the model went silent mid-utterance: end the stream here
          STATS["silence_cutoffs"] += 1
          break
        if not total and data:
          STATS["ttfa_sum"] += time.time() - tracked.start
          STATS["ttfa_count"] += 1
        pending += data
        # Send whole samples as soon as they arrive, in pieces of at most CHUNK_BYTES.
        while len(pending) >= 2:
          size = min(len(pending), CHUNK_BYTES) & ~1
          piece, pending = pending[:size], pending[size:]
          total += len(piece)
          yield _chunk(cid, created, {"content": base64.b64encode(piece).decode()})
    STATS["audio_seconds"] += total / 2 / rate
  audio_ms = int(total / 2 / rate * 1000)
  yield _chunk(cid, created, {}, "stop", _usage(speech["input"], audio_ms))
  yield "data: [DONE]\n\n"


async def _upstream_ready():
  try:
    for upstream in UPSTREAMS:
      if (await client.get(upstream + "/health", timeout=5)).status_code != 200:
        return False
    return True
  except httpx.HTTPError:
    return False


@app.get("/metrics")
async def metrics():
  lines = []
  for name, kind, value, help_text in [
    ("tts_requests_total", "counter", STATS["requests"], "Speech requests received"),
    ("tts_errors_total", "counter", STATS["errors"], "Speech requests that failed"),
    ("tts_inflight_requests", "gauge", STATS["inflight"], "Speech requests in progress"),
    ("tts_inflight_requests_max", "gauge", STATS["inflight_max"], "Most requests in progress at once"),
    ("tts_time_to_first_audio_seconds_sum", "counter", round(STATS["ttfa_sum"], 4), "Sum of time to first audio"),
    ("tts_time_to_first_audio_seconds_count", "counter", STATS["ttfa_count"], "Streams that produced audio"),
    ("tts_audio_seconds_total", "counter", round(STATS["audio_seconds"], 2), "Seconds of audio streamed"),
    ("tts_silence_cutoffs_total", "counter", STATS["silence_cutoffs"], "Streams ended early by the silence guard"),
    ("tts_silence_retries_total", "counter", STATS["silence_retries"], "Non-streaming requests regenerated by the silence guard"),
  ]:
    lines += ["# HELP %s %s" % (name, help_text), "# TYPE %s %s" % (name, kind), "%s %s" % (name, value)]
  return PlainTextResponse("\n".join(lines) + "\n")


@app.get("/health")
async def health():
  ok = await _upstream_ready()
  return JSONResponse({"status": "ok" if ok else "starting"}, status_code=200 if ok else 503)


# Model Serving checks readiness with GET /v1/models, so answer 503 until the engine is up;
# otherwise the endpoint reports READY while the engine is still loading and requests fail.
@app.get("/v1/models")
async def models():
  if not await _upstream_ready():
    return JSONResponse({"error": {"message": "the TTS engine is still starting"}}, status_code=503)
  return {"object": "list", "data": [{"id": MODEL_NAME, "object": "model", "owned_by": ENGINE}]}


@app.post("/v1/chat/completions")
@app.post("/invocations")
async def chat(request: Request):
  body = await request.json()
  try:
    speech, received = _speech_request(body)
  except ValueError as error:
    return JSONResponse({"error": {"message": str(error), "type": "invalid_request_error"}}, status_code=400)

  speech["model"] = await _upstream_model()
  if body.get("stream"):
    return StreamingResponse(_stream(speech, received), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

  speech.setdefault("response_format", "wav")
  upstream = _pick_upstream()
  LOAD[upstream] += 1
  try:
    with Tracked():
      reply = await client.post(upstream + "/v1/audio/speech", json=speech)
      if reply.status_code == 200 and speech["response_format"] == "wav" and _long_silence(reply.content):
        STATS["silence_retries"] += 1  # generate once more; a second take is almost always fine
        reply = await client.post(upstream + "/v1/audio/speech", json=speech)
  finally:
    LOAD[upstream] -= 1
  if reply.status_code != 200:
    STATS["errors"] += 1
    return JSONResponse({"error": {"message": reply.text[:1000]}}, status_code=reply.status_code)
  return {
    "id": "chatcmpl-" + uuid.uuid4().hex, "object": "chat.completion", "created": int(time.time()),
    "model": MODEL_NAME,
    "choices": [{"index": 0, "finish_reason": "stop", "message": {
      "role": "assistant", "content": base64.b64encode(reply.content).decode()}}],
    "usage": _usage(speech["input"], 0),
  }


def main():
  global UPSTREAMS, MODEL_NAME, DEFAULT_RATE, ENGINE, MAX_SILENCE_SECONDS, client
  parser = argparse.ArgumentParser()
  parser.add_argument("--port", type=int, default=8080)
  parser.add_argument("--upstream", default="http://127.0.0.1:8000", help="engine URL(s), comma-separated")
  parser.add_argument("--model-name", default="drama-3")
  parser.add_argument("--engine", default="sglang-omni", choices=["sglang-omni", "vllm-omni", "kokoro"])
  parser.add_argument("--default-sample-rate", type=int, default=int(os.environ.get("TTS_SAMPLE_RATE", 44100)))
  parser.add_argument("--max-silence-seconds", type=float, default=10.0,
                      help="silence guard; keep it above natural pauses (up to ~3 s); 0 turns it off")
  args = parser.parse_args()
  UPSTREAMS = [u.strip().rstrip("/") for u in args.upstream.split(",") if u.strip()]
  LOAD.update({u: 0 for u in UPSTREAMS})
  MODEL_NAME, DEFAULT_RATE = args.model_name, args.default_sample_rate
  ENGINE, MAX_SILENCE_SECONDS = args.engine, args.max_silence_seconds
  # Long scripts can take minutes to synthesize; Model Serving itself stops a request at ~597 s.
  client = httpx.AsyncClient(timeout=httpx.Timeout(600, connect=10))
  uvicorn.run(app, host="0.0.0.0", port=args.port, log_level="info")


if __name__ == "__main__":
  main()
