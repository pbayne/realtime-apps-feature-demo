// Energy VAD with an adaptive noise floor. Ported from the reference voice-agent app:
//   threshold = max(0.012, noise * 3), doubled while agent audio plays
//   speech starts after 200 ms above threshold (300 ms at 2x while a turn is in flight: barge-in)
//   speech ends after 800 ms below threshold; 300 ms of audio is kept before the start (preroll)
// Frames are 20 ms at 16 kHz. The class is pure: no audio or DOM dependencies.

export const FRAME_MS = 20;
export const FRAME_SAMPLES = 320; // 20 ms at 16 kHz

export const START_MS = 200;
export const END_MS = 800;
export const PREROLL_MS = 300;
export const BARGE_IN_MS = 300;
/** Utterances shorter than this are dropped by the caller (clicks, coughs). */
export const MIN_UTTERANCE_MS = 300;
/** Hard cap so a stuck mic cannot grow an utterance forever. */
const MAX_UTTERANCE_MS = 30_000;

export interface VadEnv {
  /** Agent audio is audibly playing right now (raises the threshold 2x). */
  agentPlaying: boolean;
  /** A turn is in flight: thinking or speaking. Speech then needs BARGE_IN_MS and counts as a barge-in. */
  busy: boolean;
}

export type VadEvent =
  | { kind: "speech_start" }
  | { kind: "speech_end"; frames: Float32Array[]; durationMs: number };

export function rms(frame: Float32Array): number {
  let sum = 0;
  for (let i = 0; i < frame.length; i++) sum += frame[i] * frame[i];
  return Math.sqrt(sum / Math.max(1, frame.length));
}

export class EnergyVad {
  /** Background level estimate, adapted only while silent. */
  noise = 0.01;
  private voiced = 0;
  private silent = 0;
  private preroll: Float32Array[] = [];
  private utterance: Float32Array[] | null = null;

  get recording(): boolean {
    return this.utterance !== null;
  }

  process(frame: Float32Array, env: VadEnv): VadEvent | null {
    const level = rms(frame);
    const threshold = Math.max(0.012, this.noise * 3) * (env.agentPlaying ? 2 : 1);
    const loud = level > threshold;
    if (!loud && !this.utterance) this.noise = 0.95 * this.noise + 0.05 * level;
    this.voiced = loud ? this.voiced + FRAME_MS : 0;
    this.silent = loud ? 0 : this.silent + FRAME_MS;

    if (this.utterance) {
      this.utterance.push(frame);
      const durationMs = this.utterance.length * FRAME_MS;
      if (this.silent >= END_MS || durationMs >= MAX_UTTERANCE_MS) {
        const frames = this.utterance;
        this.utterance = null;
        this.silent = 0;
        this.voiced = 0;
        return { kind: "speech_end", frames, durationMs };
      }
      return null;
    }

    this.preroll.push(frame);
    if (this.preroll.length > PREROLL_MS / FRAME_MS) this.preroll.shift();

    const needed = env.busy ? BARGE_IN_MS : START_MS;
    if (this.voiced >= needed) {
      this.utterance = [...this.preroll];
      this.preroll = [];
      this.silent = 0;
      return { kind: "speech_start" };
    }
    return null;
  }

  /** Drop an in-progress utterance without emitting it. Returns true if one was in progress. */
  cancel(): boolean {
    const was = this.utterance !== null;
    this.utterance = null;
    this.preroll = [];
    this.voiced = 0;
    this.silent = 0;
    return was;
  }
}
