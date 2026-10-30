import { useEffect, useRef } from "react";
import "./orb.css";

export type OrbState =
  | "off"
  | "connecting"
  | "muted"
  | "you"
  | "sidekick"
  | "interrupted"
  | "thinking"
  | "ptt"
  | "listening";

const WAVE_COLOR: Record<OrbState, string> = {
  off: "#64748b",
  connecting: "#64748b",
  muted: "#64748b",
  you: "#2dd4bf",
  sidekick: "#ff6a4d",
  interrupted: "#fbbf24",
  thinking: "#a78bfa",
  ptt: "#38bdf8",
  listening: "#38bdf8",
};

interface Props {
  state: OrbState;
  label: string;
  /** The user's mic level (0..1 after RMS). Drives the orb's size while they talk. */
  readMic(out: Uint8Array<ArrayBuffer>): void;
  /** Sidekick's (or the customer's) playback level. Drives the glow while they talk. */
  readAgent(out: Uint8Array<ArrayBuffer>): void;
  /** Smaller orb beside the controls once the conversation has started, so the transcript gets the height. */
  compact?: boolean;
}

const rms = (data: Uint8Array<ArrayBuffer>) => {
  let sum = 0;
  for (let i = 0; i < data.length; i++) {
    const x = (data[i] - 128) / 128;
    sum += x * x;
  }
  return Math.min(1, Math.sqrt(sum / data.length) * 3.2);
};

/**
 * Central orb with a waveform strip. Two levels, two cues: the mic swells the orb while the user speaks, and
 * Sidekick's voice brightens its glow and rings while it talks. The waveform shows whichever is the active
 * speaker. Everything is written straight to CSS variables per frame; React does not re-render for audio.
 */
export function Orb({ state, label, readMic, readAgent, compact = false }: Props) {
  const orbRef = useRef<HTMLDivElement>(null);
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const stateRef = useRef(state);
  const micRef = useRef(readMic);
  const agentRef = useRef(readAgent);

  useEffect(() => {
    stateRef.current = state;
    micRef.current = readMic;
    agentRef.current = readAgent;
  }, [state, readMic, readAgent]);

  useEffect(() => {
    const canvas = canvasRef.current;
    const orb = orbRef.current;
    if (!canvas || !orb) return;
    const g = canvas.getContext("2d");
    if (!g) return;
    const mic = new Uint8Array(new ArrayBuffer(512));
    const agent = new Uint8Array(new ArrayBuffer(512));
    let raf = 0;
    let micLevel = 0;
    let outLevel = 0;

    const frame = () => {
      const dpr = window.devicePixelRatio || 1;
      const w = Math.round(canvas.clientWidth * dpr);
      const h = Math.round(canvas.clientHeight * dpr);
      if (canvas.width !== w || canvas.height !== h) {
        canvas.width = w;
        canvas.height = h;
      }
      micRef.current(mic);
      agentRef.current(agent);
      // Attack fast, release slow: the orb breathes with speech instead of flickering.
      const mNow = rms(mic);
      const oNow = rms(agent);
      micLevel = mNow > micLevel ? mNow : micLevel * 0.85 + mNow * 0.15;
      outLevel = oNow > outLevel ? oNow : outLevel * 0.9 + oNow * 0.1;
      orb.style.setProperty("--mic", micLevel.toFixed(3));
      orb.style.setProperty("--out", outLevel.toFixed(3));
      orb.style.setProperty("--lvl", Math.max(micLevel, outLevel).toFixed(3));

      const active = stateRef.current === "sidekick" ? agent : mic;
      g.clearRect(0, 0, canvas.width, canvas.height);
      g.lineWidth = 2 * dpr;
      g.lineJoin = "round";
      g.strokeStyle = WAVE_COLOR[stateRef.current];
      g.beginPath();
      for (let i = 0; i < active.length; i++) {
        const x = (i / (active.length - 1)) * canvas.width;
        const y = (active[i] / 255) * canvas.height;
        if (i === 0) g.moveTo(x, y);
        else g.lineTo(x, y);
      }
      g.stroke();
      raf = requestAnimationFrame(frame);
    };
    raf = requestAnimationFrame(frame);
    return () => cancelAnimationFrame(raf);
  }, []);

  return (
    <div className={compact ? "flex min-w-0 items-center gap-4" : "flex flex-col items-center"}>
      <div className={`grid shrink-0 place-items-center transition-all duration-300 ${compact ? "size-[84px]" : "size-[150px]"}`}>
        <div ref={orbRef} data-state={state} className={`orb ${compact ? "orb-compact" : ""}`} aria-hidden />
      </div>
      <div className={compact ? "min-w-0" : "contents"}>
        <p className={`font-semibold tracking-tight ${compact ? "text-[15px]" : "mt-1 text-[16px]"}`} aria-live="polite">
          {label}
        </p>
        <canvas
          ref={canvasRef}
          className={compact ? "mt-1 h-7 w-[220px] max-w-full" : "mt-1.5 h-10 w-full max-w-[460px]"}
          aria-hidden
        />
      </div>
    </div>
  );
}
