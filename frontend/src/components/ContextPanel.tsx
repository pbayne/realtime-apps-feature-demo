import type { ReactNode } from "react";
import { CheckCircle2, Circle, Gauge, ShieldAlert, Activity } from "lucide-react";
import { HEALTH_CLASS, HEALTH_DOT, TARGETS, fmtMs, health, type Health } from "../lib/format";
import { riskFlagCounts, sentimentSeries, type CallState, type MetricSample } from "../lib/store";

export function ContextPanel({ state }: { state: CallState }) {
  return (
    <section className="panel scroll-thin flex h-full min-h-0 flex-col gap-0 overflow-y-auto">
      <CoverageSection state={state} />
      <SentimentSection state={state} />
      <RiskSection state={state} />
      <LatencySection metrics={state.metrics} />
    </section>
  );
}

function Block({ title, icon, right, children }: { title: string; icon: ReactNode; right?: ReactNode; children: ReactNode }) {
  return (
    <div className="border-b border-[var(--color-line)] px-4 py-3.5 last:border-b-0">
      <div className="mb-2.5 flex items-center justify-between">
        <h2 className="flex items-center gap-2 text-[12px] font-semibold uppercase tracking-[0.08em] text-slate-300">
          <span className="text-[var(--color-muted)]">{icon}</span>
          {title}
        </h2>
        {right}
      </div>
      {children}
    </div>
  );
}

function CoverageSection({ state }: { state: CallState }) {
  const total = state.coverage.length;
  const covered = state.coverage.filter((c) => c.covered).length;
  const pct = total ? Math.round((covered / total) * 100) : 0;
  return (
    <Block
      title="Coverage"
      icon={<CheckCircle2 className="size-3.5" />}
      right={<span className="font-mono text-[12px] text-slate-300">{covered}/{total} · {pct}%</span>}
    >
      <div className="mb-3 h-1.5 overflow-hidden rounded-full bg-white/[0.07]">
        <div className="h-full rounded-full bg-gradient-to-r from-teal-400 to-sky-400 transition-[width] duration-700" style={{ width: `${pct}%` }} />
      </div>
      {total === 0 ? (
        <p className="text-[12.5px] text-[var(--color-muted)]">Methodology checklist loads with the playbook.</p>
      ) : (
        <ul className="space-y-1.5">
          {state.coverage.map((c) => (
            <li key={c.key} className={`flex items-center gap-2.5 text-[13px] transition-colors ${c.covered ? "text-slate-100" : "text-slate-400"}`}>
              {c.covered ? <CheckCircle2 className="size-4 shrink-0 text-teal-300" /> : <Circle className="size-4 shrink-0" />}
              <span className={c.covered ? "" : ""}>{c.label}</span>
            </li>
          ))}
        </ul>
      )}
    </Block>
  );
}

function SentimentSection({ state }: { state: CallState }) {
  const series = sentimentSeries(state);
  const last = series[series.length - 1]?.value;
  return (
    <Block
      title="Sentiment"
      icon={<Activity className="size-3.5" />}
      right={
        last != null ? (
          <span className={`font-mono text-[12px] ${last < -0.2 ? "text-rose-300" : last > 0.2 ? "text-emerald-300" : "text-slate-300"}`}>
            {last > 0 ? "+" : ""}
            {last.toFixed(2)}
          </span>
        ) : null
      }
    >
      <Sparkline values={series.map((p) => p.value)} />
      <div className="mt-1.5 flex justify-between text-[11px] text-[var(--color-muted)]">
        <span>call start</span>
        <span>{series.length} turns scored</span>
      </div>
    </Block>
  );
}

function Sparkline({ values }: { values: number[] }) {
  const W = 300;
  const H = 64;
  const pad = 4;
  const xs = (i: number) => (values.length <= 1 ? W / 2 : pad + (i * (W - pad * 2)) / (values.length - 1));
  const ys = (v: number) => pad + ((1 - (Math.max(-1, Math.min(1, v)) + 1) / 2) * (H - pad * 2));
  const points = values.map((v, i) => `${xs(i).toFixed(1)},${ys(v).toFixed(1)}`).join(" ");
  const area = values.length
    ? `${xs(0)},${H} ${points} ${xs(values.length - 1)},${H}`
    : "";
  return (
    <div className="relative h-16 rounded-lg border border-[var(--color-line)] bg-[var(--color-ink)]/60">
      <svg viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="none" className="absolute inset-0 size-full">
        <line x1="0" x2={W} y1={H / 2} y2={H / 2} stroke="#2a3c60" strokeDasharray="3 4" />
        {values.length > 1 && (
          <>
            <defs>
              <linearGradient id="spark-fill" x1="0" x2="0" y1="0" y2="1">
                <stop offset="0%" stopColor="#38bdf8" stopOpacity="0.28" />
                <stop offset="100%" stopColor="#38bdf8" stopOpacity="0" />
              </linearGradient>
            </defs>
            <polygon points={area} fill="url(#spark-fill)" />
            <polyline points={points} fill="none" stroke="#7dd3fc" strokeWidth="2" vectorEffect="non-scaling-stroke" strokeLinejoin="round" />
          </>
        )}
        {values.map((v, i) => (
          <circle key={i} cx={xs(i)} cy={ys(v)} r={i === values.length - 1 ? 3.2 : 1.8} fill={v < -0.2 ? "#fb7185" : v > 0.2 ? "#34d399" : "#94a3b8"} vectorEffect="non-scaling-stroke" />
        ))}
      </svg>
      {values.length === 0 && <span className="absolute inset-0 grid place-items-center text-[11.5px] text-[var(--color-muted)]">no scored turns yet</span>}
    </div>
  );
}

function RiskSection({ state }: { state: CallState }) {
  const flags = riskFlagCounts(state);
  const compliance = state.cardOrder.filter((id) => state.cards[id]?.card.kind === "compliance").length;
  return (
    <Block title="Risk flags" icon={<ShieldAlert className="size-3.5" />} right={compliance ? <span className="chip chip-risk">{compliance} compliance</span> : null}>
      {flags.length === 0 ? (
        <p className="text-[12.5px] text-[var(--color-muted)]">No risk flags raised.</p>
      ) : (
        <ul className="space-y-1.5">
          {flags.map((f) => (
            <li key={f.flag} className="flex items-center justify-between rounded-lg bg-rose-500/[0.06] px-2.5 py-1.5 text-[12.5px] text-rose-100 ring-1 ring-rose-400/15">
              <span>{f.flag.replace(/_/g, " ")}</span>
              <span className="font-mono text-[11px] text-rose-300">×{f.count}</span>
            </li>
          ))}
        </ul>
      )}
    </Block>
  );
}

const LATENCY_ROWS: { key: keyof Omit<MetricSample, "at">; label: string; target: number; hint: string }[] = [
  { key: "stt_first_word_ms", label: "STT first word", target: TARGETS.stt_first_word_ms, hint: "≤ 0.8 s" },
  { key: "stt_commit_ms", label: "STT commit", target: TARGETS.stt_commit_ms, hint: "≈ 1.5 s" },
  { key: "rtm_ms", label: "RTM enrich", target: TARGETS.rtm_ms, hint: "≤ 300 ms" },
  { key: "card_ms", label: "Card start", target: TARGETS.card_ms, hint: "≤ 1.5 s" },
  { key: "card_done_ms", label: "Card done", target: TARGETS.card_done_ms, hint: "≤ 3 s" },
];

function LatencySection({ metrics }: { metrics: MetricSample[] }) {
  // Each row shows its most recent real value: silent turns carry no card timings.
  const latestOf = (key: keyof Omit<MetricSample, "at">) =>
    [...metrics].reverse().find((m) => typeof m[key] === "number")?.[key] ?? null;
  return (
    <Block title="Latency" icon={<Gauge className="size-3.5" />} right={<span className="text-[11px] text-[var(--color-muted)]">latest · avg</span>}>
      <div className="space-y-2">
        {LATENCY_ROWS.map((row) => {
          const value = latestOf(row.key);
          const h: Health = health(value, row.target);
          const samples = metrics.map((m) => m[row.key]).filter((v): v is number => typeof v === "number");
          const avg = samples.length ? samples.reduce((a, b) => a + b, 0) / samples.length : null;
          return (
            <div key={row.key} className="rounded-lg border border-[var(--color-line)] bg-[var(--color-ink)]/50 px-3 py-2">
              <div className="flex items-center justify-between text-[12px]">
                <span className="flex items-center gap-2 text-slate-300">
                  <span className={`size-1.5 rounded-full ${HEALTH_DOT[h]}`} />
                  {row.label}
                </span>
                <span className="text-[11px] text-[var(--color-muted)]">target {row.hint}</span>
              </div>
              <div className="mt-1 flex items-baseline justify-between">
                <span className={`font-mono text-[18px] font-semibold tabular-nums ${HEALTH_CLASS[h]}`}>{fmtMs(value)}</span>
                <span className="font-mono text-[11.5px] text-[var(--color-muted)]">avg {fmtMs(avg)}</span>
              </div>
            </div>
          );
        })}
      </div>
      <p className="mt-2 text-[11px] leading-snug text-[var(--color-muted)]">
        Latencies run from the customer's final transcript. Green within target, amber up to 1.5x, red beyond.
      </p>
    </Block>
  );
}
