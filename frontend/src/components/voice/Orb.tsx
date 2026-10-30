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
  readLevel(out: Uint8Array<ArrayBuffer>): void;
}

/** Central orb (scaled by audio level) with a live waveform strip underneath. Drawn outside React state. */
export function Orb({ state, label, readLevel }: Props) {
  const orbRef = useRef<HTMLDivElement>(null);
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const stateRef = useRef(state);
  const readRef = useRef(readLevel);

  useEffect(() => {
    stateRef.current = state;
    readRef.current = readLevel;
  }, [state, readLevel]);

  useEffect(() => {
    const canvas = canvasRef.current;
    const orb = orbRef.current;
    if (!canvas || !orb) return;
    const g = canvas.getContext("2d");
    if (!g) return;
    const data = new Uint8Array(new ArrayBuffer(512));
    let raf = 0;

    const frame = () => {
      const dpr = window.devicePixelRatio || 1;
      const w = Math.round(canvas.clientWidth * dpr);
      const h = Math.round(canvas.clientHeight * dpr);
      if (canvas.width !== w || canvas.height !== h) {
        canvas.width = w;
        canvas.height = h;
      }
      readRef.current(data);

      let sum = 0;
      for (let i = 0; i < data.length; i++) {
        const x = (data[i] - 128) / 128;
        sum += x * x;
      }
      const level = Math.min(1, Math.sqrt(sum / data.length) * 3.2);
      orb.style.setProperty("--lvl", level.toFixed(3));

      g.clearRect(0, 0, canvas.width, canvas.height);
      g.lineWidth = 2 * dpr;
      g.lineJoin = "round";
      g.strokeStyle = WAVE_COLOR[stateRef.current];
      g.beginPath();
      for (let i = 0; i < data.length; i++) {
        const x = (i / (data.length - 1)) * canvas.width;
        const y = (data[i] / 255) * canvas.height;
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
    <div className="flex flex-col items-center">
      <div className="grid size-[150px] place-items-center">
        <div ref={orbRef} data-state={state} className="orb" aria-hidden />
      </div>
      <p className="mt-1 text-[16px] font-semibold tracking-tight" aria-live="polite">
        {label}
      </p>
      <canvas ref={canvasRef} className="mt-1.5 h-10 w-full max-w-[460px]" aria-hidden />
    </div>
  );
}
