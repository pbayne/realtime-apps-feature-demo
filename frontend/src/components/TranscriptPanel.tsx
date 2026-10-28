import { useEffect, useLayoutEffect, useRef, useState } from "react";
import { ArrowDown, Flag } from "lucide-react";
import { sentimentLabel } from "../lib/format";
import type { CallState, UtteranceView } from "../lib/store";
import type { Speaker } from "../lib/types";

const SPEAKER: Record<Speaker, { label: string; avatar: string; bubble: string; ring: string; text: string }> = {
  customer: {
    label: "Customer",
    avatar: "bg-teal-400/15 text-teal-300 ring-teal-400/30",
    bubble: "border-teal-400/15 bg-teal-400/[0.06]",
    ring: "bg-teal-300",
    text: "text-teal-200",
  },
  agent: {
    label: "Agent",
    avatar: "bg-sky-400/15 text-sky-300 ring-sky-400/30",
    bubble: "border-sky-400/15 bg-sky-400/[0.07]",
    ring: "bg-sky-300",
    text: "text-sky-200",
  },
};

export function TranscriptPanel({ state }: { state: CallState }) {
  const scroller = useRef<HTMLDivElement>(null);
  const [stick, setStick] = useState(true);
  const finals = state.utterances.filter((u) => u.final).length;

  // Keep pinned to the bottom unless the user scrolled up to read.
  useLayoutEffect(() => {
    const el = scroller.current;
    if (el && stick) el.scrollTop = el.scrollHeight;
  }, [state.utterances, stick]);

  useEffect(() => {
    setStick(true);
  }, [state.callId]);

  const onScroll = () => {
    const el = scroller.current;
    if (!el) return;
    setStick(el.scrollHeight - el.scrollTop - el.clientHeight < 48);
  };

  return (
    <section className="panel flex h-full min-h-0 flex-col overflow-hidden">
      <header className="flex items-center justify-between border-b border-[var(--color-line)] px-4 py-3">
        <div>
          <h2 className="text-[14px] font-semibold">Live transcript</h2>
          <p className="text-[11.5px] text-[var(--color-muted)]">{finals} utterances · speaker-labelled</p>
        </div>
        <div className="flex items-center gap-3 text-[11px] text-[var(--color-muted)]">
          <span className="flex items-center gap-1.5"><span className="size-2 rounded-full bg-teal-300" />Customer</span>
          <span className="flex items-center gap-1.5"><span className="size-2 rounded-full bg-sky-300" />Agent</span>
        </div>
      </header>

      <div className="relative min-h-0 flex-1">
        <div ref={scroller} onScroll={onScroll} className="scroll-thin absolute inset-0 space-y-3 overflow-y-auto px-4 py-4">
          {state.utterances.length === 0 && (
            <div className="grid h-full place-items-center text-center text-[13px] text-[var(--color-muted)]">
              <div>
                <div className="skeleton mx-auto mb-3 h-2 w-40" />
                <div className="skeleton mx-auto h-2 w-28" />
                <p className="mt-5">Waiting for the first words…</p>
              </div>
            </div>
          )}
          {state.utterances.map((u) => (
            <Bubble key={u.key} u={u} enrich={u.final ? state.enrich[u.seq] : undefined} />
          ))}
        </div>
        {!stick && (
          <button
            type="button"
            onClick={() => {
              setStick(true);
              const el = scroller.current;
              if (el) el.scrollTop = el.scrollHeight;
            }}
            className="btn btn-ghost absolute bottom-3 left-1/2 -translate-x-1/2 bg-[var(--color-raised)] py-1 text-[11.5px] shadow-lg"
          >
            <ArrowDown className="size-3.5" /> Latest
          </button>
        )}
      </div>
    </section>
  );
}

function Bubble({ u, enrich }: { u: UtteranceView; enrich?: CallState["enrich"][number] }) {
  const s = SPEAKER[u.speaker];
  const isAgent = u.speaker === "agent";
  const sent = sentimentLabel(enrich?.sentiment);
  const hasTags = !!(enrich && (enrich.intent || enrich.objection || enrich.risk_flags.length || enrich.sentiment != null));

  return (
    <div className={`anim-bubble-in flex gap-2.5 ${isAgent ? "flex-row-reverse" : ""}`}>
      <div
        className={`mt-0.5 grid size-7 shrink-0 place-items-center rounded-full text-[11px] font-bold ring-1 ${s.avatar}`}
        aria-hidden
      >
        {s.label[0]}
      </div>
      <div className={`max-w-[84%] ${isAgent ? "items-end text-right" : ""}`}>
        <div className={`mb-1 flex items-center gap-2 text-[11px] text-[var(--color-muted)] ${isAgent ? "justify-end" : ""}`}>
          <span className="font-semibold">{s.label}</span>
          {!u.final && <span className="font-medium uppercase tracking-wider text-[10px] text-slate-400">listening</span>}
        </div>
        <div
          className={`rounded-2xl border px-3.5 py-2.5 text-left text-[14px] leading-relaxed transition-colors ${
            u.final ? `${s.bubble} text-slate-100` : "border-dashed border-[var(--color-line)] bg-transparent text-slate-400"
          }`}
        >
          <span className={u.final ? "" : "italic"}>{u.text}</span>
          {!u.final && <span className={`anim-blink ml-1 inline-block h-3.5 w-[2px] translate-y-0.5 ${s.ring}`} aria-hidden />}
        </div>
        {u.final && hasTags && enrich && (
          <div className={`mt-1.5 flex flex-wrap gap-1.5 ${isAgent ? "justify-end" : ""}`}>
            {enrich.sentiment != null && (
              <span className={`chip ${sent.cls}`} title={`sentiment ${enrich.sentiment.toFixed(2)}`}>
                {sent.label} {enrich.sentiment > 0 ? "+" : ""}
                {enrich.sentiment.toFixed(2)}
              </span>
            )}
            {enrich.intent && <span className="chip chip-intent">{enrich.intent}</span>}
            {enrich.objection && <span className="chip chip-objection">objection: {enrich.objection}</span>}
            {enrich.risk_flags.map((f) => (
              <span key={f} className="chip chip-risk">
                <Flag className="size-2.5" /> {f.replace(/_/g, " ")}
              </span>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
