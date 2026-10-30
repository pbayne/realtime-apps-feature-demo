import { useCallback, useEffect, useRef, useState } from "react";
import { Flame, Loader2, Zap } from "lucide-react";
import { MOCK } from "../../lib/useCallSession";

type State = "warm" | "warming" | "cold" | "unknown";
interface Endpoint {
  state: State;
  last_ms?: number | null;
}
interface Warmth {
  state: State;
  endpoints?: Record<string, Endpoint>;
}

const LABEL: Record<string, string> = { stt: "Ears (Parakeet)", tts: "Voice (Kokoro)", llm: "Brain (Claude)" };

/**
 * The GPU voice endpoints scale to zero; a cold one takes ~2 minutes to start (Kokoro measured 112 s). Serving
 * reports READY either way, so the app tracks when each endpoint last answered (GET /api/voice/warmup).
 * "Warm up voice" starts the pings (POST) and this card shows progress, so nobody waits on an unexplained pause.
 */
export function VoiceReadiness() {
  const [w, setW] = useState<Warmth>({ state: MOCK ? "warm" : "unknown" });
  const [since, setSince] = useState<number | null>(null);
  const [now, setNow] = useState(Date.now());
  const timer = useRef<number | null>(null);

  const read = useCallback(async (method: "GET" | "POST" = "GET") => {
    if (MOCK) return;
    try {
      const res = await fetch("/api/voice/warmup", { method });
      if (res.ok) setW((await res.json()) as Warmth);
    } catch {
      /* the start screen still works without the indicator */
    }
  }, []);

  useEffect(() => {
    void read();
  }, [read]);

  useEffect(() => {
    if (w.state !== "warming") {
      setSince(null);
      return;
    }
    setSince((s) => s ?? Date.now());
    setNow(Date.now());
    timer.current = window.setInterval(() => {
      setNow(Date.now());
      void read();
    }, 2500);
    return () => {
      if (timer.current) window.clearInterval(timer.current);
    };
  }, [w.state, read]);

  const elapsed = since ? Math.max(0, Math.round((now - since) / 1000)) : 0;
  const tone =
    w.state === "warm"
      ? "border-emerald-400/30 bg-emerald-400/[0.06] text-emerald-100"
      : w.state === "warming"
        ? "border-sky-400/30 bg-sky-400/[0.07] text-sky-100"
        : "border-amber-400/30 bg-amber-400/[0.06] text-amber-100";

  return (
    <div className={`rounded-xl border px-3.5 py-3 ${tone}`} role="status" aria-live="polite">
      <div className="flex items-center gap-2.5">
        {w.state === "warm" ? (
          <Zap className="size-4 shrink-0 text-emerald-300" />
        ) : w.state === "warming" ? (
          <Loader2 className="size-4 shrink-0 motion-safe:animate-spin text-sky-300" />
        ) : (
          <Flame className="size-4 shrink-0 text-amber-300" />
        )}
        <p className="min-w-0 flex-1 text-[13px] font-semibold">
          {w.state === "warm"
            ? "Voice is warm. Replies start in about a second."
            : w.state === "warming"
              ? `Warming up voice… ${elapsed}s (a cold GPU takes about 2 minutes)`
              : w.state === "cold"
                ? "Voice may be asleep. The first reply can take ~2 minutes."
                : "Checking voice…"}
        </p>
        {(w.state === "cold" || w.state === "unknown") && !MOCK && (
          <button type="button" onClick={() => void read("POST")} className="btn btn-primary shrink-0 py-1.5 text-[12.5px]">
            Warm up voice
          </button>
        )}
      </div>
      {w.endpoints && w.state !== "warm" && (
        <ul className="mt-2 flex flex-wrap gap-x-4 gap-y-1 pl-[26px] text-[11.5px] text-[var(--color-muted)]">
          {Object.entries(w.endpoints).map(([name, e]) => (
            <li key={name} className="flex items-center gap-1.5">
              <span
                className={`size-1.5 rounded-full ${
                  e.state === "warm" ? "bg-emerald-400" : e.state === "warming" ? "bg-sky-300" : "bg-amber-400"
                }`}
              />
              {LABEL[name] ?? name}: {e.state}
              {e.state === "warm" && e.last_ms != null ? ` (${(e.last_ms / 1000).toFixed(1)} s)` : ""}
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
