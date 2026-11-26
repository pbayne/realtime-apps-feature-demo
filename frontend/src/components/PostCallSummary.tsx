import type { ReactNode } from "react";
import { CheckCircle2, Circle, Pencil, RotateCcw, X, Check, Inbox } from "lucide-react";
import { HEALTH_CLASS, TARGETS, fmtClock, fmtMs, health } from "../lib/format";
import { summarize, type CallState } from "../lib/store";
import { Brand } from "./Brand";
import { PageEyebrow } from "./PageEyebrow";

export function PostCallSummary({ state, onNewCall }: { state: CallState; onNewCall(): void }) {
  const s = summarize(state);
  const missing = state.coverage.filter((c) => !c.covered);
  // Genuinely-empty call: nothing to average or chart. Show an honest empty state instead of a
  // wall of "--" placeholders (fmtMs renders null as "--").
  const isEmpty = s.cardsShown === 0 && s.coverageTotal === 0 && state.metrics.length === 0;
  const latencyRows: { label: string; value: number | null; target: number; hint: string }[] = [
    { label: "STT first word", value: s.avg.stt_first_word_ms, target: TARGETS.stt_first_word_ms, hint: "≤ 0.8 s" },
    { label: "STT commit", value: s.avg.stt_commit_ms, target: TARGETS.stt_commit_ms, hint: "≈ 1.5 s" },
    { label: "RTM enrich", value: s.avg.rtm_ms, target: TARGETS.rtm_ms, hint: "≤ 300 ms" },
    { label: "Card start", value: s.avg.card_ms, target: TARGETS.card_ms, hint: "≤ 1.5 s" },
    { label: "Card done", value: s.avg.card_done_ms, target: TARGETS.card_done_ms, hint: "≤ 3 s" },
  ];

  return (
    <div className="flex min-h-full flex-col">
      <header className="flex items-center justify-between px-8 py-4">
        <Brand />
        <span className="chip border-[var(--color-line)] text-[var(--color-muted)]">Call ended · {state.playbook.replace(/_/g, " ")}</span>
      </header>

      <main className="mx-auto flex w-full max-w-5xl flex-1 flex-col justify-center px-6 pb-12 pt-2 md:justify-start md:pt-8">
        <PageEyebrow>Post-call summary</PageEyebrow>
        <h1 className="mt-2 text-[28px] font-semibold tracking-tight">
          {isEmpty
            ? `${fmtClock(s.durationMs)} call`
            : `${fmtClock(s.durationMs)} call, ${s.accepted + s.edited} of ${s.cardsShown} suggestions used`}
        </h1>

        {isEmpty ? (
          <div className="panel mt-7 flex flex-col items-center gap-3 px-6 py-12 text-center">
            <span className="grid size-11 place-items-center rounded-full text-[var(--color-muted)]" style={{ background: "color-mix(in srgb, var(--color-text) 7%, transparent)" }}>
              <Inbox className="size-5" aria-hidden />
            </span>
            <p className="text-[15px] font-semibold">No suggestions or metrics this call</p>
            <p className="max-w-sm text-[13px] leading-relaxed text-[var(--color-muted)]">
              Sidekick stayed quiet and nothing was measured — this happens on a very short call or when the
              pipeline produced no coaching cards. The transcript is still saved under Past calls.
            </p>
          </div>
        ) : (
          <>
        <div className="mt-7 grid gap-3 sm:grid-cols-4">
          <Stat icon={<Check className="size-4 text-emerald-300" />} label="Accepted" value={s.accepted} />
          <Stat icon={<Pencil className="size-4 text-sky-300" />} label="Edited & sent" value={s.edited} />
          <Stat icon={<X className="size-4 text-[var(--color-muted)]" />} label="Dismissed" value={s.rejected} />
          <Stat icon={<CheckCircle2 className="size-4 text-teal-300" />} label="Coverage" value={`${s.coveragePct}%`} sub={`${s.coveredCount}/${s.coverageTotal} items`} />
        </div>

        <div className="mt-4 grid gap-4 md:grid-cols-2">
          <section className="panel p-5">
            <h2 className="text-[13px] font-semibold uppercase tracking-[0.08em] text-slate-300">Coverage</h2>
            <div className="mt-3 h-2 overflow-hidden rounded-full" style={{ background: "color-mix(in srgb, var(--color-text) 10%, transparent)" }}>
              <div className="h-full rounded-full bg-gradient-to-r from-teal-400 to-sky-400" style={{ width: `${s.coveragePct}%` }} />
            </div>
            <ul className="mt-4 space-y-2 text-[13px]">
              {state.coverage.map((c) => (
                <li key={c.key} className={`flex items-center gap-2.5 ${c.covered ? "text-slate-100" : "text-slate-400"}`}>
                  {c.covered ? <CheckCircle2 className="size-4 text-teal-300" /> : <Circle className="size-4" />}
                  {c.label}
                </li>
              ))}
            </ul>
            {missing.length > 0 && (
              <p className="mt-4 rounded-lg border border-[var(--color-line)] bg-[color-mix(in_srgb,var(--c-warn-base)_8%,transparent)] px-3 py-2 text-[12.5px] text-[var(--color-muted)]">
                <span className="font-medium text-[var(--c-warn)]">Follow up</span> on {missing.map((m) => m.label.toLowerCase()).join(", ")}.
              </p>
            )}
          </section>

          <section className="panel p-5">
            <h2 className="text-[13px] font-semibold uppercase tracking-[0.08em] text-slate-300">Average latency</h2>
            <ul className="mt-3 divide-y divide-[var(--color-line)]">
              {latencyRows.map((r) => {
                const h = health(r.value, r.target);
                return (
                  <li key={r.label} className="flex items-center justify-between py-2.5 text-[13px]">
                    <span className="text-slate-300">
                      {r.label} <span className="ml-1 text-[11px] text-[var(--color-muted)]">{r.hint}</span>
                    </span>
                    <span className={`font-mono font-semibold tabular-nums ${HEALTH_CLASS[h]}`}>{fmtMs(r.value)}</span>
                  </li>
                );
              })}
            </ul>
            <p className="mt-3 text-[11.5px] text-[var(--color-muted)]">
              Averages over {state.metrics.length} metric samples. Final transcript latency is per utterance on the transcript.
            </p>
          </section>
        </div>
          </>
        )}

        {state.error && <p className="mt-8 text-center text-[14px] text-[var(--c-warn)]">{state.error}</p>}
        <div className="mt-8 flex justify-center">
          <button type="button" onClick={onNewCall} className="btn btn-primary px-5 py-2.5 text-[14px]">
            <RotateCcw className="size-4" /> Start another call
          </button>
        </div>
      </main>
    </div>
  );
}

function Stat({ icon, label, value, sub }: { icon: ReactNode; label: string; value: number | string; sub?: string }) {
  return (
    <div className="panel p-4">
      <div className="flex items-center gap-2 text-[12px] text-[var(--color-muted)]">
        {icon} {label}
      </div>
      <div className="mt-2 font-mono text-[28px] font-semibold tabular-nums">{value}</div>
      {sub && <div className="text-[11.5px] text-[var(--color-muted)]">{sub}</div>}
    </div>
  );
}
