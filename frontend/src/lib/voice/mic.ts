// Turn-taking on the microphone: capture + energy VAD (hands-free) or manual frames (push-to-talk).
// Emits the same local events the mock backend produces, so the session hook has one code path.
import { startCapture, type Capture } from "./capture";
import { EnergyVad, FRAME_MS, type VadEnv } from "./vad";
import { wav } from "./wav";

export type LocalEvent =
  | { kind: "speech_start" }
  | { kind: "speech_end"; wav: ArrayBuffer; durationMs: number };

export type VoiceInputMode = "hands_free" | "ptt";

export class MicEngine {
  private cap: Capture | null = null;
  private readonly vad = new EnergyVad();
  private manual: Float32Array[] | null = null;
  private mode: VoiceInputMode = "hands_free";
  private muted = false;

  constructor(
    private readonly env: () => VadEnv,
    private readonly emit: (ev: LocalEvent) => void,
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
    if (muted) this.cancel();
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
    this.vad.cancel();
  }

  private onFrame(frame: Float32Array): void {
    if (this.muted) return;
    if (this.mode === "ptt") {
      this.manual?.push(frame);
      return;
    }
    const ev = this.vad.process(frame, this.env());
    if (!ev) return;
    if (ev.kind === "speech_start") this.emit({ kind: "speech_start" });
    else this.emitEnd(ev.frames, ev.durationMs);
  }

  private emitEnd(frames: Float32Array[], durationMs: number): void {
    this.emit({ kind: "speech_end", wav: wav(frames), durationMs });
  }

  /** Abandon whatever utterance is open. Emits a zero-length end so the UI can clear "speaking". */
  private cancel(): void {
    const had = this.manual !== null || this.vad.cancel();
    this.manual = null;
    if (had) this.emit({ kind: "speech_end", wav: new ArrayBuffer(0), durationMs: 0 });
  }
}
