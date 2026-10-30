import { HEALTH_CLASS, HEALTH_DOT, fmtMs, health } from "../../lib/format";
import type { VoiceTiming } from "../../lib/types";

// SPEC §9.5 targets. The LLM first token has no target, so it is shown without a health colour.
// "Transcript" in streaming mode runs from the end of speech to the final text, so it includes the deliberate
// end-of-turn silence (700 ms); 250 ms is the raw Parakeet round trip, not this number.
const TARGET = { first_audio: 1200, stt: 1000, tts: 200 } as const;

function Tile({
  label,
  value,
  target,
  hint,
  strong = false,
  compact = false,
}: {
  label: string;
  value: number | null | undefined;
  target: number | null;
  hint: string;
  strong?: boolean;
  compact?: boolean;
}) {
  const h = target == null ? "none" : health(value ?? null, target);
  return (
    <div className={`min-w-0 flex-1 rounded-lg border border-[var(--color-line)] bg-[var(--color-ink)]/50 ${compact ? "px-2.5 py-1.5" : "px-3 py-2"}`}>
      <div className="flex items-center gap-1.5 text-[11px] text-slate-300">
        <span className={`size-1.5 shrink-0 rounded-full ${HEALTH_DOT[h]}`} />
        <span className="truncate">{label}</span>
      </div>
      <div className={`mt-0.5 font-mono font-semibold tabular-nums ${strong ? "text-[18px]" : "text-[14px]"} ${HEALTH_CLASS[h]}`}>
        {fmtMs(value ?? null)}
      </div>
      {!compact && <div className="truncate text-[11px] text-[var(--color-muted)]">{hint}</div>}
    </div>
  );
}

export function LatencyHud({
  timing,
  history,
  compact = false,
}: {
  timing: VoiceTiming | null;
  history: number[];
  compact?: boolean;
}) {
  const avg = history.length ? history.reduce((a, b) => a + b, 0) / history.length : null;
  return (
    <div className={`grid w-full grid-cols-2 gap-2 md:grid-cols-4 ${compact ? "max-w-none" : "max-w-[720px]"}`} aria-label="Latency">
      <Tile
        label="First audio"
        value={timing?.first_audio_ms}
        target={TARGET.first_audio}
        hint={`target 1.2 s${avg != null ? ` · avg ${fmtMs(avg)}` : ""}`}
        strong={!compact}
        compact={compact}
      />
      <Tile label="Transcript" value={timing?.stt_ms} target={TARGET.stt} hint="end of speech → text · ≤ 1 s" compact={compact} />
      <Tile label="Model first token" value={timing?.llm_first_token_ms} target={null} hint="voice model" compact={compact} />
      <Tile label="Voice first audio" value={timing?.tts_first_audio_ms} target={TARGET.tts} hint="Kokoro · ≤ 200 ms" compact={compact} />
    </div>
  );
}
