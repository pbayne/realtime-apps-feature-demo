// Gapless playback of Sidekick audio: 24 kHz s16le mono chunks (SPEC §9.2 `audio` events), base64.
// Scheduling follows the reference app's play(): each chunk starts at max(nextTime, now + 50 ms).
// Chunks are played in seq order within a turn; a barge-in stops everything and drops late chunks
// until the next user turn starts.

export function decodePcm16(b64: string): Float32Array<ArrayBuffer> {
  const bin = atob(b64);
  const n = bin.length >> 1;
  const out = new Float32Array(new ArrayBuffer(n * 4));
  for (let i = 0; i < n; i++) {
    const lo = bin.charCodeAt(2 * i);
    const hi = bin.charCodeAt(2 * i + 1);
    let v = (hi << 8) | lo;
    if (v & 0x8000) v -= 0x10000;
    out[i] = v / 32768;
  }
  return out;
}

/** Inverse of decodePcm16. Used by the mock backend to fabricate audio. */
export function encodePcm16(samples: Float32Array): string {
  const bytes = new Uint8Array(samples.length * 2);
  for (let i = 0; i < samples.length; i++) {
    const s = Math.max(-1, Math.min(1, samples[i]));
    const v = s < 0 ? Math.round(s * 0x8000) : Math.round(s * 0x7fff);
    bytes[2 * i] = v & 0xff;
    bytes[2 * i + 1] = (v >> 8) & 0xff;
  }
  let bin = "";
  for (let i = 0; i < bytes.length; i += 0x8000) {
    bin += String.fromCharCode(...bytes.subarray(i, i + 0x8000));
  }
  return btoa(bin);
}

export class PcmPlayer {
  private readonly ctx: AudioContext;
  private readonly analyser: AnalyserNode;
  private readonly sources = new Set<AudioBufferSourceNode>();
  private nextTime = 0;
  private turn = -1;
  private expect = 0;
  private held = new Map<number, { rate: number; pcm: Float32Array<ArrayBuffer> }>();
  private dropping = false;

  constructor() {
    this.ctx = new AudioContext();
    this.analyser = this.ctx.createAnalyser();
    this.analyser.fftSize = 1024;
    this.analyser.connect(this.ctx.destination);
    void this.ctx.resume().catch(() => {});
  }

  /** A new user turn started: its audio is wanted again (lifts the barge-in drop). */
  beginTurn(turn: number): void {
    this.dropping = false;
    this.reset(turn);
  }

  enqueue(turn: number, seq: number, pcmB64: string, rate: number): void {
    if (this.dropping) return;
    if (turn !== this.turn) this.reset(turn);
    this.held.set(seq, { rate, pcm: decodePcm16(pcmB64) });
    let next = this.held.get(this.expect);
    while (next) {
      this.held.delete(this.expect);
      this.schedule(next.pcm, next.rate);
      this.expect += 1;
      next = this.held.get(this.expect);
    }
  }

  /** Barge-in: silence everything now and drop late audio until the next turn begins. */
  stop(): void {
    this.dropping = true;
    for (const s of this.sources) {
      try {
        s.stop();
      } catch {
        /* already stopped */
      }
    }
    this.sources.clear();
    this.held.clear();
    this.nextTime = 0;
  }

  isSpeaking(): boolean {
    return this.sources.size > 0;
  }

  level(out: Uint8Array<ArrayBuffer>): void {
    this.analyser.getByteTimeDomainData(out);
  }

  close(): void {
    this.stop();
    void this.ctx.close().catch(() => {});
  }

  private reset(turn: number): void {
    this.turn = turn;
    this.expect = 0;
    this.held.clear();
  }

  private schedule(pcm: Float32Array<ArrayBuffer>, rate: number): void {
    const buffer = this.ctx.createBuffer(1, pcm.length, rate);
    buffer.copyToChannel(pcm, 0);
    const src = this.ctx.createBufferSource();
    src.buffer = buffer;
    src.connect(this.analyser);
    this.nextTime = Math.max(this.nextTime, this.ctx.currentTime + 0.05);
    src.start(this.nextTime);
    this.nextTime += buffer.duration;
    this.sources.add(src);
    src.onended = () => {
      this.sources.delete(src);
    };
  }
}
