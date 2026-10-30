import type { CoverageItem } from "../../lib/types";
import type { LiveRead as LiveReadState } from "../../lib/voice/state";

interface Props {
  live: LiveReadState | null;
  series: number[];
  /** Assistant mode shows the methodology checklist; practice keeps coverage on the call pipeline. */
  coverage?: CoverageItem[] | null;
  compact?: boolean;
}

const W = 240;
const H = 40;
// Palette: the same tints as the existing chips (positive, tense, neutral).
const POSITIVE = "#6ee7b7";
const TENSE = "#fda4af";
const NEUTRAL = "#8ea0bd";
const SPARK = "#7dd3fc";

/** Tone word for the gauge. Thresholds are deliberately wide: the meter is a read, not a verdict. */
export function toneLabel(v: number): string {
  if (v >= 0.25) return "Positive";
  if (v <= -0.25) return "Tense";
  return "Neutral";
}

/** Signal keys from the rules (`objection:fees`, `risk:unsupported_claim`) as words a rep can scan. */
export function signalLabel(s: string): string {
  const [kind, key] = s.split(":");
  const words = (key ?? "").replace(/_/g, " ");
  if (kind === "objection") return `Objection · ${words}`;
  if (kind === "risk") return `Watch · ${words}`;
  if (kind === "method") return `Covers · ${words}`;
  if (s === "question") return "Question";
  return s.replace(/_/g, " ");
}

/** Sparkline geometry in the viewBox. Fewer than two readings draw a flat midline. */
export function sparkPoints(series: number[]): string {
  if (series.length < 2) return `0,${H / 2} ${W},${H / 2}`;
  const pad = 3;
  return series
    .map((v, i) => {
      const x = (i / (series.length - 1)) * W;
      const y = pad + (1 - (Math.max(-1, Math.min(1, v)) + 1) / 2) * (H - 2 * pad);
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join(" ");
}

export function LiveRead({ live, series, coverage, compact = false }: Props) {
  const v = live?.sentiment ?? 0;
  const tone = live ? toneLabel(v) : "Waiting";
  const marker = `${((v + 1) / 2) * 100}%`;
  const tint = !live ? NEUTRAL : v >= 0.25 ? POSITIVE : v <= -0.25 ? TENSE : NEUTRAL;
  const list = coverage ?? live?.coverage ?? null;
  const covered = list ? list.filter((c) => c.covered).length : 0;

  return (
    <section aria-label="Live read" className={`panel ${compact ? "px-3 py-2.5" : "px-4 py-3"}`}>
      <div className="flex items-baseline justify-between gap-2">
        <h2 className="kbd-label text-[11px]">Live read</h2>
        <p className="flex items-baseline gap-2 text-[12px]">
          <span className="font-semibold" style={{ color: tint }}>
            {tone}
          </span>
          <span className="font-mono tabular-nums text-[var(--color-muted)]">
            {live ? `${v >= 0 ? "+" : ""}${v.toFixed(2)}` : "—"}
          </span>
          {live?.partial && <span className="chip chip-neutral text-[11px]">while speaking</span>}
        </p>
      </div>

      {/* Gauge: -1 (tense) to +1 (positive), with a centre tick. The marker glides between readings. */}
      <div
        className="relative mt-2.5 h-2 rounded-full bg-[var(--color-ink)]"
        role="img"
        aria-label={`Tone ${tone.toLowerCase()}, ${v.toFixed(2)} on a scale from minus one to plus one`}
      >
        <div className="absolute inset-y-0 left-1/2 w-px bg-[var(--color-line)]" aria-hidden />
        <div
          aria-hidden
          className="gauge-marker absolute top-1/2 size-3.5 -translate-x-1/2 -translate-y-1/2 rounded-full border-2 border-[var(--color-panel)]"
          style={{ left: marker, background: tint, boxShadow: `0 0 12px ${tint}` }}
        />
      </div>

      <svg viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="none" className={`mt-2 w-full ${compact ? "h-8" : "h-10"}`} aria-hidden>
        <line x1="0" x2={W} y1={H / 2} y2={H / 2} stroke="var(--color-line)" strokeWidth="1" strokeDasharray="3 4" />
        <polygon points={`${sparkPoints(series.slice(-60))} ${W},${H} 0,${H}`} fill={SPARK} fillOpacity={0.12} />
        <polyline
          points={sparkPoints(series.slice(-60))}
          fill="none"
          stroke={SPARK}
          strokeWidth="1.75"
          strokeLinejoin="round"
          strokeLinecap="round"
          vectorEffect="non-scaling-stroke"
          className="spark-line"
        />
      </svg>

      {live ? (
        <div className="mt-2 flex flex-wrap gap-1.5">
          <span className="chip chip-intent">{live.intent === "question" ? "question" : "statement"}</span>
          {live.signals
            .filter((s) => !s.startsWith("method:") && s !== "question")
            .slice(0, compact ? 2 : 4)
            .map((s) => (
              <span
                key={s}
                className={`chip ${s.startsWith("objection") ? "chip-objection" : s.startsWith("risk") ? "chip-risk" : "chip-neutral"}`}
              >
                {signalLabel(s)}
              </span>
            ))}
        </div>
      ) : (
        <p className="mt-2 text-[12px] text-[var(--color-muted)]">Tone and signals appear as you speak.</p>
      )}

      {!compact && list && list.length > 0 && (
        <p className="mt-2 text-[11.5px] text-[var(--color-muted)]">
          Checklist <span className="font-mono tabular-nums text-slate-300">{covered}/{list.length}</span> covered
        </p>
      )}
    </section>
  );
}
