// Past Calls (request E): the caller's own calls, with transcript, suggestion cards, actions and the
// post-call summary. Read-only; data comes from Lakebase (live) plus the Delta gold summary when available.
import { useEffect, useState } from "react";
import { ArrowLeft, Clock, FileText, ListChecks, MessageSquare, Sparkles } from "lucide-react";
import { Brand } from "./Brand";
import { type CallDetail, type CallSummaryRow, getCallDetail, listCalls } from "../lib/history";

function fmtWhen(ms: number | null): string {
  if (!ms) return "";
  return new Date(ms).toLocaleString(undefined, { day: "numeric", month: "short", hour: "2-digit", minute: "2-digit" });
}

function fmtDuration(a: number | null, b: number | null): string {
  if (!a || !b || b <= a) return "";
  const s = Math.round((b - a) / 1000);
  const m = Math.floor(s / 60);
  return m > 0 ? `${m}m ${s % 60}s` : `${s}s`;
}

export function PastCalls() {
  const [calls, setCalls] = useState<CallSummaryRow[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [selected, setSelected] = useState<string | null>(null);

  useEffect(() => {
    listCalls()
      .then(setCalls)
      .catch((e) => setError(e instanceof Error ? e.message : "Could not load calls"));
  }, []);

  return (
    <div className="flex min-h-full flex-col">
      <header className="flex items-center justify-between px-8 py-4">
        <Brand />
        <a href="#/" className="btn btn-ghost py-1.5 text-[12.5px]" onClick={() => setSelected(null)}>
          <ArrowLeft className="size-3.5" /> Back to start
        </a>
      </header>

      <main className="mx-auto w-full max-w-5xl flex-1 px-6 pb-10">
        <p className="kbd-label flex items-center gap-2">
          <span className="inline-block size-1.5 rounded-full bg-[var(--color-lava)]" aria-hidden /> History
        </p>
        <h1 className="mt-2 text-[28px] font-semibold tracking-tight">Past calls</h1>
        <p className="mt-2 max-w-xl text-[13.5px] leading-relaxed text-[var(--color-muted)]">
          Every call you ran — transcript, the cards Sidekick raised, the actions you approved, and the post-call
          summary. Persisted to Lakebase.
        </p>

        {error && (
          <p role="alert" className="mt-6 rounded-lg border border-rose-500/30 bg-rose-500/10 px-4 py-3 text-[13px] text-rose-200">
            {error}
          </p>
        )}

        {selected ? (
          <Detail callId={selected} onBack={() => setSelected(null)} />
        ) : (
          <CallList calls={calls} onOpen={setSelected} />
        )}
      </main>
    </div>
  );
}

function CallList({ calls, onOpen }: { calls: CallSummaryRow[] | null; onOpen(id: string): void }) {
  if (calls === null) return <p className="mt-8 text-[13px] text-[var(--color-muted)]">Loading…</p>;
  if (calls.length === 0)
    return (
      <div className="mt-10 grid place-items-center rounded-xl border border-[var(--color-line)] bg-[var(--color-panel)] p-10 text-center">
        <FileText className="size-6 text-[var(--color-muted)]" aria-hidden />
        <p className="mt-3 text-[14px] text-slate-300">No calls yet.</p>
        <p className="mt-1 text-[12.5px] text-[var(--color-muted)]">Run a call and it will show up here with its transcript and summary.</p>
      </div>
    );
  return (
    <ul className="mt-6 grid gap-2">
      {calls.map((c) => (
        <li key={c.call_id}>
          <button
            type="button"
            onClick={() => onOpen(c.call_id)}
            className="flex w-full items-center justify-between rounded-xl border border-[var(--color-line)] bg-[var(--color-panel)] px-4 py-3 text-left transition-colors hover:border-slate-400 hover:bg-[var(--color-raised)]"
          >
            <div className="min-w-0">
              <div className="flex items-center gap-2">
                <span className="text-[14px] font-semibold capitalize">{c.playbook.replace(/_/g, " ")}</span>
                <span className="chip chip-neutral capitalize">{c.mode}</span>
                <span className={`chip ${c.status === "ended" ? "chip-neutral" : "chip-pos"}`}>{c.status}</span>
              </div>
              <div className="mt-1 flex flex-wrap items-center gap-3 text-[12px] text-[var(--color-muted)]">
                <span className="flex items-center gap-1"><Clock className="size-3" /> {fmtWhen(c.started_at_ms)}</span>
                {fmtDuration(c.started_at_ms, c.ended_at_ms) && <span>· {fmtDuration(c.started_at_ms, c.ended_at_ms)}</span>}
                {c.utterance_count != null && <span className="flex items-center gap-1"><MessageSquare className="size-3" /> {c.utterance_count}</span>}
                {c.card_count != null && <span className="flex items-center gap-1"><Sparkles className="size-3" /> {c.card_count}</span>}
              </div>
            </div>
            <span className="text-[12px] text-sky-300">Open →</span>
          </button>
        </li>
      ))}
    </ul>
  );
}

function Detail({ callId, onBack }: { callId: string; onBack(): void }) {
  const [d, setD] = useState<CallDetail | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setD(null);
    getCallDetail(callId)
      .then(setD)
      .catch((e) => setError(e instanceof Error ? e.message : "Could not load this call"));
  }, [callId]);

  if (error) return <p role="alert" className="mt-6 text-[13px] text-rose-300">{error}</p>;
  if (!d) return <p className="mt-6 text-[13px] text-[var(--color-muted)]">Loading…</p>;

  const nextSteps = d.cards.filter((c) => c.kind === "next_best_action" || c.kind === "next_question");
  return (
    <div className="mt-6">
      <button type="button" onClick={onBack} className="btn btn-ghost py-1.5 text-[12.5px]">
        <ArrowLeft className="size-3.5" /> All calls
      </button>

      {d.summary?.summary && (
        <section className="mt-4 rounded-xl border border-[var(--color-line)] bg-[var(--color-raised)] p-4">
          <h2 className="flex items-center gap-2 text-[13px] font-semibold"><FileText className="size-4 text-sky-300" /> Summary</h2>
          <p className="mt-2 whitespace-pre-wrap text-[13.5px] leading-relaxed text-slate-200">{d.summary.summary}</p>
          {d.summary.overall_score != null && (
            <p className="mt-2 text-[12px] text-[var(--color-muted)]">Score: {d.summary.overall_score}</p>
          )}
        </section>
      )}

      <div className="mt-4 grid gap-4 md:grid-cols-[1.4fr_1fr]">
        <section className="rounded-xl border border-[var(--color-line)] bg-[var(--color-panel)] p-4">
          <h2 className="flex items-center gap-2 text-[13px] font-semibold"><MessageSquare className="size-4 text-sky-300" /> Transcript</h2>
          <div className="scroll-thin mt-3 max-h-[420px] space-y-2 overflow-y-auto pr-1">
            {d.transcript.length === 0 && <p className="text-[12.5px] text-[var(--color-muted)]">No transcript captured.</p>}
            {d.transcript.map((u) => (
              <div key={u.seq} className={`rounded-lg border px-3 py-2 text-[13px] ${u.speaker === "customer" ? "border-[var(--color-line)] bg-[var(--color-raised)]" : "border-sky-400/20 bg-sky-400/[0.06]"}`}>
                <div className="mb-0.5 text-[10.5px] font-semibold uppercase tracking-wider text-[var(--color-muted)]">{u.speaker}</div>
                <p className="leading-relaxed text-slate-200">{u.text}</p>
              </div>
            ))}
          </div>
        </section>

        <div className="space-y-4">
          <section className="rounded-xl border border-[var(--color-line)] bg-[var(--color-panel)] p-4">
            <h2 className="flex items-center gap-2 text-[13px] font-semibold"><ListChecks className="size-4 text-sky-300" /> Next steps &amp; actions</h2>
            <div className="mt-3 space-y-2">
              {nextSteps.length === 0 && d.actions.length === 0 && (
                <p className="text-[12.5px] text-[var(--color-muted)]">No next steps or actions recorded.</p>
              )}
              {nextSteps.map((c) => (
                <div key={c.card_id} className="rounded-lg border border-[var(--color-line)] bg-[var(--color-raised)] px-3 py-2">
                  <p className="text-[12.5px] font-medium text-slate-200">{c.title}</p>
                  {c.body && <p className="mt-0.5 text-[12px] text-[var(--color-muted)]">{c.body}</p>}
                </div>
              ))}
              {d.actions.map((a) => (
                <div key={a.action_id} className="rounded-lg border border-[var(--color-line)] bg-[var(--color-raised)] px-3 py-2">
                  <div className="flex items-center justify-between gap-2">
                    <p className="text-[12.5px] font-medium text-slate-200">{a.summary}</p>
                    <span className="chip chip-neutral">{a.status}</span>
                  </div>
                </div>
              ))}
            </div>
          </section>

          <section className="rounded-xl border border-[var(--color-line)] bg-[var(--color-panel)] p-4">
            <h2 className="flex items-center gap-2 text-[13px] font-semibold"><Sparkles className="size-4 text-sky-300" /> Cards raised</h2>
            <div className="mt-3 space-y-2">
              {d.cards.length === 0 && <p className="text-[12.5px] text-[var(--color-muted)]">No cards.</p>}
              {d.cards.map((c) => (
                <div key={c.card_id} className="rounded-lg border border-[var(--color-line)] bg-[var(--color-raised)] px-3 py-2">
                  <div className="flex items-center justify-between gap-2">
                    <p className="text-[12.5px] font-medium text-slate-200">{c.title}</p>
                    <span className="chip chip-neutral">{c.status}</span>
                  </div>
                </div>
              ))}
            </div>
          </section>
        </div>
      </div>
    </div>
  );
}
