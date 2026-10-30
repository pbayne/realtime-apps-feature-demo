// Microphone capture: AudioWorklet -> 16 kHz mono Float32 frames of 20 ms. Ported from the reference app
// (getUserMedia with echoCancellation + noiseSuppression + autoGainControl, AudioContext at 16 kHz).
// If the browser refuses a 16 kHz context, the native-rate stream is resampled here.
import { FRAME_SAMPLES } from "./vad";
import { VOICE_RATE } from "./wav";

export interface Capture {
  setMuted(muted: boolean): void;
  /** Fill `out` with the current time-domain mic signal (bytes, 128 = silence). */
  level(out: Uint8Array<ArrayBuffer>): void;
  stop(): void;
}

const WORKLET = `class SidekickMic extends AudioWorkletProcessor {
  process(inputs) { const ch = inputs[0] && inputs[0][0]; if (ch) this.port.postMessage(ch.slice(0)); return true; }
}
registerProcessor("sidekick-mic", SidekickMic);`;

function concat(a: Float32Array, b: Float32Array): Float32Array<ArrayBuffer> {
  const out = new Float32Array(new ArrayBuffer((a.length + b.length) * 4));
  out.set(a, 0);
  out.set(b, a.length);
  return out;
}

/** Linear-interpolation resampler that carries state across blocks. Identity when the rates match. */
class Resampler {
  private readonly step: number;
  private pos = 0;
  private tail = new Float32Array(0);

  constructor(inRate: number, outRate: number) {
    this.step = inRate / outRate;
  }

  push(chunk: Float32Array): Float32Array {
    if (this.step === 1) return chunk;
    const buf = concat(this.tail, chunk);
    const out: number[] = [];
    let p = this.pos;
    while (p + 1 < buf.length) {
      const i = Math.floor(p);
      const f = p - i;
      out.push(buf[i] * (1 - f) + buf[i + 1] * f);
      p += this.step;
    }
    const start = Math.floor(p);
    this.tail = buf.slice(start);
    this.pos = p - start;
    return Float32Array.from(out);
  }
}

export async function startCapture(onFrame: (frame: Float32Array) => void): Promise<Capture> {
  if (!navigator.mediaDevices?.getUserMedia) throw new Error("This browser cannot open the microphone.");
  const stream = await navigator.mediaDevices.getUserMedia({
    audio: { channelCount: 1, echoCancellation: true, noiseSuppression: true, autoGainControl: true },
  });

  let ctx: AudioContext;
  try {
    ctx = new AudioContext({ sampleRate: VOICE_RATE });
  } catch {
    ctx = new AudioContext();
  }

  const url = URL.createObjectURL(new Blob([WORKLET], { type: "application/javascript" }));
  try {
    await ctx.audioWorklet.addModule(url);
  } finally {
    URL.revokeObjectURL(url);
  }

  const source = ctx.createMediaStreamSource(stream);
  const analyser = ctx.createAnalyser();
  analyser.fftSize = 1024;
  const node = new AudioWorkletNode(ctx, "sidekick-mic");
  const resampler = new Resampler(ctx.sampleRate, VOICE_RATE);

  let pending: Float32Array<ArrayBuffer> = new Float32Array(0);
  let muted = false;

  node.port.onmessage = (e: MessageEvent<Float32Array>) => {
    if (muted) return;
    pending = concat(pending, resampler.push(e.data));
    while (pending.length >= FRAME_SAMPLES) {
      onFrame(pending.slice(0, FRAME_SAMPLES));
      pending = pending.slice(FRAME_SAMPLES);
    }
  };

  source.connect(analyser);
  source.connect(node);
  void ctx.resume().catch(() => {});

  return {
    setMuted(m: boolean) {
      muted = m;
      if (m) pending = new Float32Array(0);
      stream.getAudioTracks().forEach((t) => (t.enabled = !m));
    },
    level(out: Uint8Array<ArrayBuffer>) {
      analyser.getByteTimeDomainData(out);
    },
    stop() {
      node.port.onmessage = null;
      source.disconnect();
      node.disconnect();
      stream.getTracks().forEach((t) => t.stop());
      void ctx.close();
    },
  };
}
