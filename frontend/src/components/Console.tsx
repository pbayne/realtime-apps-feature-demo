import { useEffect, useState } from "react";
import { Captions, Headphones, Mic, PhoneOff, Radio, Users } from "lucide-react";
import { fmtClock } from "../lib/format";
import type { CallState, CallStatus } from "../lib/store";
import type { Speaker } from "../lib/types";
import { Brand } from "./Brand";
import { ContextPanel } from "./ContextPanel";
import { SidekickFeed } from "./SidekickFeed";
import { ActionsPanel } from "./actions/ActionsPanel";
import { MOCK } from "../lib/useCallSession";
import type { ActionView } from "../lib/actions";
import { TranscriptPanel } from "./TranscriptPanel";

interface Props {
  state: CallState;
  channel: Speaker;
  sttBrowser: boolean;
  notice: string | null;
  onEnd(): void;
  onChannel(ch: Speaker): void;
  onBrowserStt(on: boolean): void;
  onResolve(cardId: string, action: "accept" | "reject" | "edit", text?: string): void;
  onActionUpdate(action: ActionView): void;
}

function useNow(active: boolean) {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    if (!active) return;
    const id = window.setInterval(() => setNow(Date.now()), 500);
    return () => window.clearInterval(id);
  }, [active]);
  return now;
}

const STATUS_STYLE: Record<CallStatus, { label: string; cls: string }> = {
  idle: { label: "Idle", cls: "text-slate-400 border-slate-600" },
  connecting: { label: "Connecting", cls: "text-sky-200 border-sky-400/40" },
  live: { label: "Live", cls: "text-emerald-200 border-emerald-400/40" },
  reconnecting: { label: "Reconnecting", cls: "text-amber-200 border-amber-400/40" },
  ended: { label: "Ended", cls: "text-slate-300 border-slate-500" },
  error: { label: "Error", cls: "text-rose-200 border-rose-400/40" },
};

export function Console({ state, channel, sttBrowser, notice, onEnd, onChannel, onBrowserStt, onResolve, onActionUpdate }: Props) {
  const running = state.status !== "ended" && state.status !== "error";
  const now = useNow(running);
  const elapsed = state.startedAt ? (state.endedAt ?? now) - state.startedAt : 0;
  const st = STATUS_STYLE[state.status];
  const isMic = state.mode === "mic";

  return (
    <div className="flex min-h-full flex-col md:h-full md:min-h-0">
      <header className="flex flex-wrap items-center gap-x-4 gap-y-2 border-b border-[var(--color-line)] bg-[var(--color-ink)]/80 px-5 py-3 backdrop-blur">
        <Brand />
        <div className="mx-1 hidden h-7 w-px bg-[var(--color-line)] md:block" />
        <div className="flex items-center gap-2 text-[13px]">
          <Users className="size-3.5 text-[var(--color-muted)]" />
          <span className="font-semibold">{playbookLabel(state.playbook)}</span>
          <span className="chip chip-neutral">{isMic ? <><Mic className="size-3" /> Live mic</> : <><Radio className="size-3" /> Replay</>}</span>
        </div>

        <span className={`chip border bg-transparent ${st.cls}`}>
          {state.status === "live" && <span className="anim-live-dot size-1.5 rounded-full bg-emerald-400" />}
          {st.label}
        </span>
        <span className="font-mono text-[13px] tabular-nums text-slate-300">{fmtClock(elapsed)}</span>

        <div className="ml-auto flex flex-wrap items-center gap-2">
          {isMic && running && (
            <>
              <div className="flex items-center gap-1 rounded-lg border border-[var(--color-line)] bg-[var(--color-panel)] p-0.5" role="group" aria-label="Mic channel">
                <span className="px-2 text-[11px] uppercase tracking-wider text-[var(--color-muted)]">Mic is</span>
                {(["customer", "agent"] as Speaker[]).map((ch) => (
                  <button
                    key={ch}
                    type="button"
                    aria-pressed={channel === ch}
                    onClick={() => onChannel(ch)}
                    className={`rounded-md px-2.5 py-1 text-[12px] font-semibold capitalize transition-colors ${
                      channel === ch ? "bg-[var(--color-raised)] text-white" : "text-[var(--color-muted)] hover:text-slate-200"
                    }`}
                  >
                    {ch}
                  </button>
                ))}
              </div>
              <button
                type="button"
                onClick={() => onBrowserStt(!sttBrowser)}
                className={`btn ${sttBrowser ? "border border-sky-400/50 bg-sky-400/15 text-sky-100" : "btn-ghost"} py-1.5 text-[12px]`}
                aria-pressed={sttBrowser}
                title="Use the browser Web Speech API instead of backend STT"
              >
                <Captions className="size-3.5" /> Browser STT {sttBrowser ? "on" : "off"}
              </button>
            </>
          )}
          {running && (
            <button type="button" onClick={onEnd} className="btn btn-danger py-1.5 text-[12.5px]">
              <PhoneOff className="size-3.5" /> End call
            </button>
          )}
        </div>
      </header>

      {state.status === "reconnecting" && (
        <div role="status" className="flex items-center gap-2 border-b border-amber-400/25 bg-amber-400/[0.08] px-5 py-2 text-[12.5px] text-amber-100">
          <span className="anim-live-dot size-1.5 shrink-0 rounded-full bg-amber-300" aria-hidden />
          <span className="min-w-0 flex-1">Connection dropped. Reconnecting… the call is still open.</span>
        </div>
      )}
      {notice && (
        <div role="alert" className="flex items-center gap-2 border-b border-amber-400/20 bg-amber-400/10 px-5 py-2 text-[12.5px] text-amber-100">
          <Headphones className="size-3.5" /> {notice}
        </div>
      )}

      <main className="grid min-h-0 flex-1 grid-cols-1 gap-3 p-3 md:grid-cols-2 md:grid-rows-[minmax(0,1fr)_17rem] xl:grid-cols-[minmax(0,1.1fr)_minmax(0,1fr)_minmax(0,0.78fr)] xl:grid-rows-[minmax(0,1fr)]">
        <div className="h-[420px] min-h-0 md:h-auto xl:h-auto">
          <TranscriptPanel state={state} />
        </div>
        <div className="h-[520px] min-h-0 md:h-auto xl:h-auto">
          <div className="flex h-full min-h-0 flex-col gap-3">
            <div className="flex min-h-0 flex-[3] flex-col">
              <SidekickFeed state={state} onResolve={onResolve} />
            </div>
            <ActionsPanel
              className="min-h-0 flex-[2]"
              actions={state.actions}
              scope={{ call_id: state.callId ?? undefined }}
              playbook={state.playbook}
              mock={MOCK}
              onUpdate={onActionUpdate}
            />
          </div>
        </div>
        <div className="h-[460px] min-h-0 md:col-span-2 md:h-auto xl:col-span-1 xl:h-auto">
          <ContextPanel state={state} />
        </div>
      </main>

      {state.status === "error" && state.error && (
        <div className="px-5 pb-3 text-[12.5px] text-rose-300">{state.error}</div>
      )}
    </div>
  );
}

function playbookLabel(name: string): string {
  return name ? name.replace(/_/g, " ") : "Playbook";
}
