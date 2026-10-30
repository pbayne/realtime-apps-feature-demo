// Turn-taking on the microphone: capture + energy VAD (hands-free) or manual frames (push-to-talk).
// Two outputs. The VAD speech boundaries drive barge-in and the orb (the same local events the mock backend
// produces). When a stream sink is set, the mic also sends 100 ms PCM16 chunks while the gate is open (SPEC 9.2,
// streaming input): the server endpoints the turns, so the client builds no WAV in that mode.
import { startCapture, type Capture } from "./capture";
import { EnergyVad, FRAME_MS, type VadEnv } from "./vad";
import { pcm16, wav } from "./wav";

export type LocalEvent =
  | { kind: "speech_start" }
  | { kind: "speech_end"; wav?: ArrayBuffer; durationMs: number };

export type VoiceInputMode = "hands_free" | "ptt";

/** One streamed chunk: 100 ms at 16 kHz is 1600 samples. */
export const STREAM_CHUNK_SAMPLES = 1600;

export class MicEngine {
  private cap: Capture | null = null;
  private readonly vad = new EnergyVad();
  private manual: Float32Array[] | null = null;
  private mode: VoiceInputMode = "hands_free";
  private muted = false;
  private gate = false;
  private readonly chunk = new Float32Array(STREAM_CHUNK_SAMPLES);
  private fill = 0;

  constructor(
    private readonly env: () => VadEnv,
    private readonly emit: (ev: LocalEvent) => void,
    /** Set in streaming mode: receives each 100 ms PCM16 chunk while the gate is open. */
    private readonly sink?: (pcm: ArrayBuffer) => void,
  ) {}

  async start(): Promise<void> {
    if (this.cap) return;
    this.cap = await startCapture((frame) => this.onFrame(frame));
  }

  setMode(mode: VoiceInputMode): void {
    if (this.mode === mode) return;
    this.mode = mode;
    this.cancel();
  }

  setMuted(muted: boolean): void {
    this.muted = muted;
    this.cap?.setMuted(muted);
    if (muted) {
      this.setGate(false);
      this.cancel();
    }
  }

  /** Streaming: whether chunks reach the sink. Closing the gate drops any partial chunk. */
  setGate(open: boolean): void {
    this.gate = open && !!this.sink;
    if (!this.gate) this.fill = 0;
  }

  /** Push-to-talk: start a manual utterance. Ignored outside PTT mode. */
  pttDown(): void {
    if (this.mode !== "ptt" || this.muted || this.manual) return;
    this.manual = [];
    this.emit({ kind: "speech_start" });
  }

  /** Push-to-talk: finish the manual utterance and send it. */
  pttUp(): void {
    if (!this.manual) return;
    const frames = this.manual;
    this.manual = null;
    this.emitEnd(frames, frames.length * FRAME_MS);
  }

  level(out: Uint8Array<ArrayBuffer>): void {
    if (this.cap) this.cap.level(out);
    else out.fill(128);
  }

  stop(): void {
    this.cap?.stop();
    this.cap = null;
    this.manual = null;
    this.gate = false;
    this.fill = 0;
    this.vad.cancel();
  }

  private onFrame(frame: Float32Array): void {
    if (this.muted) return;
    if (this.gate) this.pump(frame);
    if (this.mode === "ptt") {
      if (this.manual && !this.sink) this.manual.push(frame);
      return;
    }
    const ev = this.vad.process(frame, this.env());
    if (!ev) return;
    if (ev.kind === "speech_start") this.emit({ kind: "speech_start" });
    else this.emitEnd(ev.frames, ev.durationMs);
  }

  private pump(frame: Float32Array): void {
    let i = 0;
    while (i < frame.length) {
      const n = Math.min(STREAM_CHUNK_SAMPLES - this.fill, frame.length - i);
      this.chunk.set(frame.subarray(i, i + n), this.fill);
      this.fill += n;
      i += n;
      if (this.fill === STREAM_CHUNK_SAMPLES) {
        this.sink?.(pcm16(this.chunk));
        this.fill = 0;
      }
    }
  }

  private emitEnd(frames: Float32Array[], durationMs: number): void {
    // In streaming mode the server already has the audio; no WAV is built.
    this.emit({ kind: "speech_end", wav: this.sink ? undefined : wav(frames), durationMs });
  }

  /** Abandon whatever utterance is open. Emits a zero-length end so the UI can clear "speaking". */
  private cancel(): void {
    const had = this.manual !== null || this.vad.cancel();
    this.manual = null;
    if (had) this.emit({ kind: "speech_end", wav: new ArrayBuffer(0), durationMs: 0 });
  }
}
