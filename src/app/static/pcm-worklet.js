// AudioWorklet: downsample the mic to 16 kHz mono and emit ~100 ms PCM16 frames.
// Runs in AudioWorkletGlobalScope; `sampleRate` is the context's native rate.
const TARGET_RATE = 16000;
const FRAME_SAMPLES = 1600; // 100 ms at 16 kHz

class PcmDownsampler extends AudioWorkletProcessor {
  constructor() {
    super();
    this.ratio = sampleRate / TARGET_RATE;
    this.pos = 0; // fractional read position in the incoming stream
    this.carry = []; // leftover input samples from the previous block
    this.out = new Float32Array(0);
  }

  process(inputs) {
    const input = inputs[0];
    if (!input || input.length === 0 || !input[0]) return true;
    const ch = input[0]; // mono: first channel only

    // Linear-interpolation resampling from native rate -> 16 kHz.
    const merged = new Float32Array(this.carry.length + ch.length);
    merged.set(this.carry, 0);
    merged.set(ch, this.carry.length);

    const resampled = [];
    let p = this.pos;
    while (p + 1 < merged.length) {
      const i = Math.floor(p);
      const frac = p - i;
      resampled.push(merged[i] * (1 - frac) + merged[i + 1] * frac);
      p += this.ratio;
    }
    // Keep the unread tail; rebase the read position.
    const consumed = Math.floor(p);
    this.carry = Array.from(merged.subarray(consumed));
    this.pos = p - consumed;

    // Accumulate and emit fixed-size frames.
    const next = new Float32Array(this.out.length + resampled.length);
    next.set(this.out, 0);
    next.set(resampled, this.out.length);
    this.out = next;

    while (this.out.length >= FRAME_SAMPLES) {
      const frame = this.out.subarray(0, FRAME_SAMPLES);
      const pcm = new Int16Array(FRAME_SAMPLES);
      for (let i = 0; i < FRAME_SAMPLES; i++) {
        const s = Math.max(-1, Math.min(1, frame[i]));
        pcm[i] = s < 0 ? s * 0x8000 : s * 0x7fff;
      }
      this.port.postMessage(pcm.buffer, [pcm.buffer]);
      this.out = this.out.slice(FRAME_SAMPLES);
    }
    return true;
  }
}

registerProcessor("pcm-downsampler", PcmDownsampler);
