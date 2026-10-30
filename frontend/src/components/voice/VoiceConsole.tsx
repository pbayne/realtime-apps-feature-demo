import { useEffect, useState } from "react";
import { Headphones, Mic, MicOff, PhoneOff, Radio, Sparkles, Users, X } from "lucide-react";
import { getVoices, MOCK_VOICES } from "../../lib/api";
import { fmtClock } from "../../lib/format";
import { MOCK, MOCK_SPEED } from "../../lib/useCallSession";
import { isThinking, type VoiceState } from "../../lib/voice/state";
import type { VoiceSession } from "../../lib/voice/useVoiceSession";
import type { VoiceOption } from "../../lib/types";
import { Brand } from "../Brand";
import { ContextPanel } from "../ContextPanel";
import { SidekickFeed } from "../SidekickFeed";
import { Composer, Conversation } from "./Conversation";
import { LatencyHud } from "./LatencyHud";
import { LiveCaption } from "./LiveCaption";
import { LiveLane } from "./LiveLane";
import { LiveRead } from "./LiveRead";
import { Orb, type OrbState } from "./Orb";
import { Outcomes } from "./Outcomes";

const HINT_KEY = "sidekick.voice.headphoneHint.dismissed";

function readHintDismissed(): boolean {
  try {
    return localStorage.getItem(HINT_KEY) === "1";
  } catch {
    return false;
  }
}

function dismissHint() {
  try {
    localStorage.setItem(HINT_KEY, "1");
  } catch {
    /* private mode: the hint simply shows again */
  }
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

/** What the orb shows. Precedence: connection, mute, who is talking, barge-in flash, thinking, idle. */
export function orbFor(s: VoiceState, thinking: boolean): { kind: OrbState; label: string } {
  const practice = s.role === "practice";
  if (s.status === "connecting") return { kind: "connecting", label: "Connecting" };
  if (s.status === "reconnecting") return { kind: "connecting", label: "Reconnecting" };
  if (s.status === "ended" || s.status === "error" || s.status === "idle") return { kind: "off", label: "Session ended" };
  if (s.muted) return { kind: "muted", label: "Muted" };
  if (s.userSpeaking) return { kind: "you", label: "You're speaking" };
  if (s.agentSpeaking) return { kind: "sidekick", label: practice ? "Customer speaking" : "Sidekick speaking" };
  if (s.flash === "interrupted") return { kind: "interrupted", label: "Interrupted" };
  if (thinking) return { kind: "thinking", label: "Thinking" };
  if (s.mode === "ptt") return { kind: "ptt", label: "Hold to talk" };
  return { kind: "listening", label: "Listening" };
}

const STATUS_STYLE: Record<VoiceState["status"], { label: string; cls: string }> = {
  idle: { label: "Idle", cls: "text-slate-400 border-slate-600" },
  connecting: { label: "Connecting", cls: "text-sky-200 border-sky-400/40" },
  live: { label: "Live", cls: "text-emerald-200 border-emerald-400/40" },
  reconnecting: { label: "Reconnecting", cls: "text-amber-200 border-amber-400/40" },
  ended: { label: "Ended", cls: "text-slate-300 border-slate-500" },
  error: { label: "Error", cls: "text-rose-200 border-rose-400/40" },
};

const inField = (t: EventTarget | null) =>
  t instanceof HTMLElement && (t.tagName === "INPUT" || t.tagName === "TEXTAREA" || t.tagName === "SELECT" || t.isContentEditable);

export function VoiceConsole({ api, onNew }: { api: VoiceSession; onNew(): void }) {
  const { state, callState } = api;
  const practice = state.role === "practice";
  const running = state.status === "live" || state.status === "reconnecting" || state.status === "connecting";
  const now = useNow(running || !!state.warming);
  const elapsed = state.startedAt ? (state.endedAt ?? now) - state.startedAt : 0;
  const warmingFor = state.warming ? now - state.warming.since : 0;
  const thinking = isThinking(state);
  const orb = orbFor(state, thinking);
  const st = STATUS_STYLE[state.status];
  const [hint, setHint] = useState(() => !readHintDismissed());
  const [voices, setVoices] = useState<VoiceOption[]>(MOCK ? MOCK_VOICES : []);

  useEffect(() => {
    if (MOCK) return; // the simulated backend has no /api/voices; MOCK_VOICES is the initial list
    let alive = true;
    void getVoices().then((list) => {
      if (alive) setVoices(list);
    });
    return () => {
      alive = false;
    };
  }, []);

  // Keyboard: Space holds to talk in push-to-talk mode; M mutes; Esc interrupts Sidekick. Typing in a field
  // keeps its keys, except Esc, which always interrupts.
  const { pttDown, pttUp, setMuted, interrupt } = api;
  const muted = state.muted;
  const ptt = state.mode === "ptt";
  useEffect(() => {
    if (!running) return;
    const down = (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        interrupt();
        return;
      }
      if (inField(e.target) || e.metaKey || e.ctrlKey || e.altKey) return;
      if ((e.key === "m" || e.key === "M") && !e.repeat) {
        e.preventDefault();
        setMuted(!muted);
        return;
      }
      if (e.code === "Space" && ptt && !e.repeat) {
        e.preventDefault();
        pttDown();
      }
    };
    const up = (e: KeyboardEvent) => {
      if (e.code !== "Space" || !ptt || inField(e.target)) return;
      e.preventDefault();
      pttUp();
    };
    window.addEventListener("keydown", down);
    window.addEventListener("keyup", up);
    return () => {
      window.removeEventListener("keydown", down);
      window.removeEventListener("keyup", up);
    };
  }, [running, ptt, muted, interrupt, setMuted, pttDown, pttUp]);

  const voiceList = voices.some((v) => v.id === state.voice)
    ? voices
    : [...voices, { id: state.voice, label: state.voice }];
  const scenarioTitle = state.scenario ? state.scenario.replace(/_/g, " ") : "free practice";
  const listening = running && !muted && !state.userSpeaking && !state.caption;

  const live = (
    <LiveRead live={state.live} series={state.series} coverage={practice ? null : undefined} compact={practice} />
  );
  const lane = <LiveLane chips={state.chips} onPin={api.pinChip} onDismiss={api.dismissChip} />;

  return (
    <div className="flex min-h-full flex-col md:h-full md:min-h-0">
      <header className="flex flex-wrap items-center gap-x-4 gap-y-2 border-b border-[var(--color-line)] bg-[var(--color-ink)]/80 px-5 py-3 backdrop-blur">
        <Brand />
        <div className="mx-1 hidden h-7 w-px bg-[var(--color-line)] md:block" />
        <div className="flex items-center gap-2 text-[13px]">
          {practice ? <Users className="size-3.5 text-[var(--color-muted)]" /> : <Sparkles className="size-3.5 text-[var(--color-muted)]" />}
          <span className="font-semibold">{practice ? `Practice · ${scenarioTitle}` : "Talk to Sidekick"}</span>
          <span className="chip chip-neutral">
            <Radio className="size-3" /> {state.playbook.replace(/_/g, " ")}
          </span>
        </div>

        <span className={`chip border bg-transparent ${st.cls}`}>
          {state.status === "live" && <span className="anim-live-dot size-1.5 rounded-full bg-emerald-400" />}
          {st.label}
        </span>
        <span className="font-mono text-[13px] tabular-nums text-slate-300">{fmtClock(elapsed)}</span>
        {MOCK && (
          <span className="chip border-sky-400/30 text-sky-200">
            Mock · simulated voice{MOCK_SPEED !== 1 ? ` · ${MOCK_SPEED}x` : ""}
          </span>
        )}

        <div className="ml-auto flex flex-wrap items-center gap-2">
          <label className="flex items-center gap-1.5 text-[11.5px] text-[var(--color-muted)]">
            Voice
            <select
              value={state.voice}
              onChange={(e) => api.setVoice(e.target.value)}
              disabled={!running}
              aria-label="Voice"
              className="rounded-md border border-[var(--color-line)] bg-[var(--color-panel)] px-2 py-1 text-[12px] text-slate-200 outline-none disabled:opacity-50"
            >
              {voiceList.map((v) => (
                <option key={v.id} value={v.id}>
                  {v.label}
                </option>
              ))}
            </select>
          </label>
          {running ? (
            <button type="button" onClick={() => void api.end()} className="btn btn-danger py-1.5 text-[12.5px]">
              <PhoneOff className="size-3.5" /> End session
            </button>
          ) : (
            <button type="button" onClick={onNew} className="btn btn-primary py-1.5 text-[12.5px]">
              New session
            </button>
          )}
        </div>
      </header>

      {state.warming && (
        <div role="status" className="flex items-center gap-2 border-b border-sky-400/20 bg-sky-400/[0.08] px-5 py-2 text-[12.5px] text-sky-100">
          <span className="anim-live-dot size-1.5 shrink-0 rounded-full bg-sky-300" aria-hidden />
          <span className="min-w-0 flex-1">Warming up voice (GPU starting)… the first reply can take a minute.</span>
          <span className="font-mono tabular-nums text-sky-200">{fmtClock(warmingFor)}</span>
        </div>
      )}
      {state.notice && (
        <div role="alert" className="flex items-center gap-2 border-b border-amber-400/20 bg-amber-400/10 px-5 py-2 text-[12.5px] text-amber-100">
          <Headphones className="size-3.5" /> {state.notice}
        </div>
      )}
      {hint && (
        <div className="flex items-center gap-2 border-b border-sky-400/20 bg-sky-400/[0.07] px-5 py-2 text-[12.5px] text-sky-100">
          <Headphones className="size-3.5 shrink-0" />
          <span className="min-w-0 flex-1">Use headphones to avoid Sidekick hearing itself.</span>
          <button
            type="button"
            onClick={() => {
              dismissHint();
              setHint(false);
            }}
            aria-label="Dismiss headphone hint"
            className="grid size-6 place-items-center rounded-md hover:bg-white/10"
          >
            <X className="size-3.5" />
          </button>
        </div>
      )}

      <main
        className={`grid min-h-0 flex-1 grid-cols-1 gap-3 p-3 ${
          practice ? "md:grid-cols-[minmax(0,1.1fr)_minmax(0,0.9fr)]" : "md:grid-cols-[minmax(0,1fr)_340px] xl:grid-cols-[minmax(0,1fr)_380px]"
        }`}
      >
        <section className="panel flex min-h-[560px] min-h-0 flex-col overflow-hidden md:min-h-0">
          <div className="flex flex-col items-center gap-3 border-b border-[var(--color-line)] px-5 pb-4 pt-5">
            <Orb state={orb.kind} label={orb.label} readMic={api.readMic} readAgent={api.readAgent} />
            <Controls api={api} running={running} />
            <LatencyHud timing={state.lastTiming} history={state.firstAudioHistory} />
          </div>

          <Conversation exchanges={state.exchanges} role={state.role} agentLabel={practice ? "Customer" : "Sidekick"} />

          {practice && (
            <details className="border-t border-[var(--color-line)]">
              <summary className="cursor-pointer px-4 py-2.5 text-[12.5px] font-medium text-slate-300 hover:bg-white/[0.02]">
                Outcomes · {state.items.length}
              </summary>
              <div className="max-h-[220px] overflow-y-auto">
                <Outcomes items={state.items} compact />
              </div>
            </details>
          )}

          <LiveCaption caption={state.caption} listening={listening} />
          <Composer disabled={!running} onSend={api.sendText} />
          <KeyHints ptt={ptt} running={running} />
        </section>

        {practice ? (
          <div className="flex min-h-[640px] flex-col gap-3 md:min-h-0">
            {live}
            {lane}
            <div className="grid min-h-0 flex-1 grid-rows-[minmax(0,1fr)_minmax(0,0.85fr)] gap-3">
              <SidekickFeed state={callState} onResolve={api.resolveCard} />
              <ContextPanel state={callState} />
            </div>
          </div>
        ) : (
          <div className="flex min-h-[360px] flex-col gap-3 md:min-h-0">
            {live}
            {lane}
            <div className="min-h-0 flex-1">
              <Outcomes items={state.items} />
            </div>
          </div>
        )}
      </main>
    </div>
  );
}

function KeyHints({ ptt, running }: { ptt: boolean; running: boolean }) {
  if (!running) return null;
  return (
    <p className="flex flex-wrap items-center gap-x-3 gap-y-1 border-t border-[var(--color-line)] px-4 py-2 text-[11px] text-[var(--color-muted)]">
      {ptt && (
        <span>
          <kbd className="key">Space</kbd> hold to talk
        </span>
      )}
      <span>
        <kbd className="key">M</kbd> mute
      </span>
      <span>
        <kbd className="key">Esc</kbd> interrupt
      </span>
    </p>
  );
}

function Controls({ api, running }: { api: VoiceSession; running: boolean }) {
  const { state } = api;
  const ptt = state.mode === "ptt";
  const seg = (on: boolean) =>
    `rounded-md px-3 py-1.5 text-[12.5px] font-semibold transition-colors ${
      on ? "bg-[var(--color-raised)] text-white" : "text-[var(--color-muted)] hover:text-slate-200"
    }`;
  return (
    <div className="flex flex-wrap items-center justify-center gap-2">
      <div role="group" aria-label="Input mode" className="flex rounded-lg border border-[var(--color-line)] bg-[var(--color-panel)] p-0.5">
        <button type="button" className={seg(!ptt)} aria-pressed={!ptt} onClick={() => api.setMode("hands_free")} disabled={!running}>
          Hands-free
        </button>
        <button type="button" className={seg(ptt)} aria-pressed={ptt} onClick={() => api.setMode("ptt")} disabled={!running}>
          Push to talk
        </button>
      </div>

      {ptt && (
        <button
          type="button"
          disabled={!running || state.muted}
          onPointerDown={(e) => {
            e.currentTarget.setPointerCapture?.(e.pointerId);
            api.pttDown();
          }}
          onPointerUp={() => api.pttUp()}
          onPointerCancel={() => api.pttUp()}
          className={`btn py-1.5 text-[13px] ${state.userSpeaking ? "border border-teal-400/50 bg-teal-400/20 text-teal-100" : "btn-ghost"}`}
        >
          <Mic className="size-3.5" /> Hold to talk <span className="font-mono text-[10.5px] text-[var(--color-muted)]">Space</span>
        </button>
      )}

      <button
        type="button"
        onClick={() => api.setMuted(!state.muted)}
        disabled={!running}
        aria-pressed={state.muted}
        className={`btn py-1.5 text-[13px] ${state.muted ? "border border-amber-400/40 bg-amber-400/10 text-amber-100" : "btn-ghost"}`}
      >
        {state.muted ? <MicOff className="size-3.5" /> : <Mic className="size-3.5" />}
        {state.muted ? "Unmute" : "Mute"} <span className="font-mono text-[10.5px] text-[var(--color-muted)]">M</span>
      </button>
    </div>
  );
}
