import { useEffect, useMemo, useState } from "react";
import { ChevronLeft, ChevronRight, Link2, Maximize2, Minimize2, Pause, Play } from "lucide-react";
import { NODE_BY_ID, NODES, STAGES, STEPS, type ArchNode, type Metric } from "../../lib/architecture";

/** Shareable state lives in the hash: #/admin/architecture?c=<node>&s=<step>&k=1&p=1 (c, s, k, p). */
interface ViewState {
  c: string | null;
  s: number; // 0 = overview, 1..STEPS.length = a step
  k: boolean; // scale path (Kafka + Spark Real-Time Mode) shown
  p: boolean; // presentation mode
}

function readView(): ViewState {
  const q = new URLSearchParams(window.location.hash.split("?")[1] ?? "");
  const c = q.get("c");
  const s = Number(q.get("s") ?? "0");
  return {
    c: c && NODE_BY_ID[c] ? c : null,
    s: Number.isInteger(s) && s >= 0 && s <= STEPS.length ? s : 0,
    k: q.get("k") === "1",
    p: q.get("p") === "1",
  };
}

function writeView(v: ViewState): void {
  const q = new URLSearchParams();
  if (v.c) q.set("c", v.c);
  if (v.s) q.set("s", String(v.s));
  if (v.k) q.set("k", "1");
  if (v.p) q.set("p", "1");
  const qs = q.toString();
  window.history.replaceState(null, "", `#/admin/architecture${qs ? `?${qs}` : ""}`);
}

export function ArchitecturePage() {
  const [view, setView] = useState<ViewState>(() => readView());
  const [playing, setPlaying] = useState(false);
  const reduceMotion = useMemo(() => window.matchMedia?.("(prefers-reduced-motion: reduce)").matches ?? false, []);

  // Keep the URL in step with the view, without adding history entries.
  useEffect(() => writeView(view), [view]);

  // Play the flow: one step every 2.4 s (every step is also reachable directly, so motion is optional).
  useEffect(() => {
    if (!playing) return;
    const id = window.setInterval(() => setView((v) => (v.s < STEPS.length ? { ...v, s: v.s + 1 } : v)), reduceMotion ? 3600 : 2400);
    return () => window.clearInterval(id);
  }, [playing, reduceMotion]);

  // The flow stops on its own after the last step.
  useEffect(() => {
    if (view.s >= STEPS.length) setPlaying(false);
  }, [view.s]);

  const step = view.s > 0 ? STEPS[view.s - 1] : null;
  const litNodes = new Set(step?.nodes ?? []);
  const selected = view.c ? NODE_BY_ID[view.c] : null;
  const panelNode: ArchNode | null = selected ?? null;

  const set = (patch: Partial<ViewState>) => setView((v) => ({ ...v, ...patch }));
  const pick = (id: string) => set({ c: view.c === id ? null : id });
  const togglePlay = () => {
    if (playing) return setPlaying(false);
    setPlaying(true);
    set({ s: view.s >= STEPS.length ? 1 : Math.max(1, view.s) });
  };

  const copyLink = () => {
    void navigator.clipboard?.writeText(window.location.href).catch(() => undefined);
  };

  return (
    <div className={view.p ? "fixed inset-0 z-50 overflow-auto bg-[var(--color-ink)] px-10 py-8 text-[18px]" : ""}>
      <section aria-labelledby="arch-h" className={view.p ? "mx-auto max-w-[1400px]" : ""}>
        <p className="kbd-label">How it's built</p>
        <div className="mt-2 flex flex-wrap items-end justify-between gap-4">
          <div>
            <h1 id="arch-h" className={`${view.p ? "text-[44px]" : "text-[28px]"} font-semibold leading-tight tracking-tight`}>
              Sidekick, on Databricks
            </h1>
            <p className={`${view.p ? "text-[22px]" : "text-[14px]"} mt-2 max-w-3xl leading-relaxed text-[var(--color-muted)]`}>
              Click any component for its role, why Databricks, and the measured numbers. Play the flow to follow one utterance end to end.
            </p>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <button type="button" className="btn btn-ghost py-1.5 text-[12.5px]" aria-pressed={view.k} onClick={() => set({ k: !view.k })}>
              {view.k ? "Hide" : "Show"} scale path (Kafka + RTM)
            </button>
            <button type="button" className="btn btn-ghost py-1.5 text-[12.5px]" onClick={copyLink}>
              <Link2 className="size-3.5" /> Copy link
            </button>
            <button type="button" className="btn btn-ghost py-1.5 text-[12.5px]" aria-pressed={view.p} onClick={() => set({ p: !view.p })}>
              {view.p ? <Minimize2 className="size-3.5" /> : <Maximize2 className="size-3.5" />}
              {view.p ? "Exit presentation" : "Presentation"}
            </button>
          </div>
        </div>

        {/* Play the flow */}
        <div className="panel mt-6 flex flex-wrap items-center gap-2 p-3" role="group" aria-label="Play the flow">
          <button type="button" className="btn btn-primary py-1.5 text-[12.5px]" onClick={togglePlay}>
            {playing ? <Pause className="size-3.5" /> : <Play className="size-3.5" />}
            {playing ? "Pause" : "Play the flow"}
          </button>
          <button type="button" className="btn btn-ghost py-1.5 text-[12.5px]" disabled={view.s === 0} onClick={() => { setPlaying(false); set({ s: view.s - 1 }); }} aria-label="Previous step">
            <ChevronLeft className="size-3.5" />
          </button>
          <button type="button" className="btn btn-ghost py-1.5 text-[12.5px]" disabled={view.s >= STEPS.length} onClick={() => { setPlaying(false); set({ s: view.s + 1 }); }} aria-label="Next step">
            <ChevronRight className="size-3.5" />
          </button>
          <ol className="ml-2 flex flex-wrap gap-1.5" aria-label="Steps">
            {STEPS.map((s, i) => (
              <li key={s.title}>
                <button
                  type="button"
                  aria-current={view.s === i + 1 ? "step" : undefined}
                  onClick={() => { setPlaying(false); set({ s: i + 1 }); }}
                  className={`rounded-full border px-2.5 py-0.5 text-[11.5px] font-semibold motion-safe:transition-colors ${
                    view.s === i + 1
                      ? "border-[var(--c-accent)] bg-[var(--c-accent)]/15 text-[var(--color-text)]"
                      : "border-[var(--color-line)] text-[var(--color-muted)] hover:text-[var(--color-text)]"
                  }`}
                >
                  {i + 1}. {s.title}
                </button>
              </li>
            ))}
          </ol>
        </div>

        {/* Stage columns: the flow reads left to right. */}
        <div className="mt-6 grid gap-3 lg:grid-cols-8">
          {STAGES.map((stage, idx) => {
            const nodes = NODES.filter((n) => n.stage === stage.id && (!n.scale || view.k));
            return (
              <div key={stage.id} className="relative flex min-w-0 flex-col gap-2">
                <div className="flex items-center gap-1.5 text-[11px] font-semibold uppercase tracking-wider text-[var(--color-muted)]">
                  <span className="grid size-4 place-items-center rounded-full border border-[var(--color-line)] text-[9.5px]">{idx + 1}</span>
                  {stage.label}
                </div>
                {nodes.length === 0 && <div className="rounded-[var(--radius-control)] border border-dashed border-[var(--color-line)] p-3 text-[11.5px] text-[var(--color-muted)]">none</div>}
                {nodes.map((n) => {
                  const lit = litNodes.has(n.id);
                  const on = view.c === n.id;
                  return (
                    <button
                      key={n.id}
                      type="button"
                      aria-pressed={on}
                      onClick={() => pick(n.id)}
                      className={`rounded-[var(--radius-control)] border p-2.5 text-left motion-safe:transition-all ${
                        n.scale ? "border-dashed" : ""
                      } ${
                        lit ? "border-[var(--c-accent)] bg-[var(--c-accent)]/10 shadow-[0_0_0_3px_color-mix(in_srgb,var(--c-accent)_25%,transparent)]" : "border-[var(--color-line)] bg-[var(--color-panel)] hover:border-[var(--color-muted)]"
                      } ${on ? "ring-2 ring-[var(--c-accent)]" : ""}`}
                    >
                      <div className="text-[12.5px] font-semibold leading-snug text-[var(--color-text)]">{n.title}</div>
                      {n.metrics[0] && (
                        <div className="mt-1 text-[11px] leading-snug text-[var(--color-muted)]">{n.metrics[0].value}</div>
                      )}
                    </button>
                  );
                })}
              </div>
            );
          })}
        </div>
        <p className="mt-3 text-[11.5px] text-[var(--color-muted)]">
          Browser, App, Model Serving, Live state, History, Jobs, Knowledge, Consume. Read left to right; the App, Live state and Model Serving boxes are
          where a turn spends its time.
        </p>

        {/* Detail: a step while playing or selected from the strip, otherwise the selected component. */}
        <div className="mt-6 grid gap-4 md:grid-cols-[1fr_1.2fr]">
          {step && (
            <div className="panel p-5" aria-live="polite">
              <p className="kbd-label">Step {view.s} of {STEPS.length}</p>
              <h2 className="mt-1 text-[20px] font-semibold">{step.title}</h2>
              <p className="mt-2 text-[14px] leading-relaxed text-[var(--color-muted)]">{step.text}</p>
              {step.timings.length > 0 && <MetricList rows={step.timings} />}
            </div>
          )}
          {panelNode && (
            <div className="panel p-5">
              <p className="kbd-label">{STAGES.find((s) => s.id === panelNode.stage)?.label}</p>
              <h2 className="mt-1 text-[20px] font-semibold">{panelNode.title}</h2>
              <dl className="mt-3 space-y-3 text-[13.5px] leading-relaxed">
                <div>
                  <dt className="font-semibold text-[var(--color-text)]">Role in Sidekick</dt>
                  <dd className="text-[var(--color-muted)]">{panelNode.role}</dd>
                </div>
                <div>
                  <dt className="font-semibold text-[var(--color-text)]">Why Databricks</dt>
                  <dd className="text-[var(--color-muted)]">{panelNode.why}</dd>
                </div>
              </dl>
              {panelNode.metrics.length > 0 && <MetricList rows={panelNode.metrics} />}
              <p className="mt-4 text-[12px] text-[var(--color-muted)]">Source: {panelNode.doc}</p>
            </div>
          )}
          {!step && !panelNode && (
            <div className="panel p-5 text-[14px] leading-relaxed text-[var(--color-muted)] md:col-span-2">
              Pick a component, or press Play the flow. Timings are the measured numbers from docs/ARCHITECTURE.md section 3.3 unless marked estimated.
            </div>
          )}
        </div>
      </section>
    </div>
  );
}

function MetricList({ rows }: { rows: Metric[] }) {
  return (
    <ul className="mt-4 divide-y divide-[var(--color-line)] rounded-[var(--radius-control)] border border-[var(--color-line)]">
      {rows.map((m) => (
        <li key={m.label} className="flex items-baseline justify-between gap-4 px-3 py-2 text-[13px]">
          <span className="text-[var(--color-muted)]">{m.label}</span>
          <span className="whitespace-nowrap text-right font-mono tabular-nums text-[var(--color-text)]">
            {m.value}
            {m.basis && (
              <span className={`ml-2 text-[10px] font-semibold tracking-wide ${m.basis === "MEASURED" ? "text-[var(--c-pos)]" : "text-[var(--c-warn)]"}`}>{m.basis}</span>
            )}
          </span>
        </li>
      ))}
    </ul>
  );
}
