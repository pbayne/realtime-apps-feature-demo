import { useEffect, useState, type ReactNode } from "react";
import { Headphones, Mic, MicOff, PlayCircle, Radio, RotateCcw, Sparkles } from "lucide-react";
import { FALLBACK_PLAYBOOKS, FALLBACK_SCRIPTS, FALLBACK_VOICES, MOCK_VOICES, getPlaybooks, getScripts, getVoices } from "../lib/api";
import { MOCK, MOCK_SPEED, URL_PARAMS } from "../lib/useCallSession";
import type { Mode, PlaybookSummary, StartOptions, VoiceOption, VoiceRole, VoiceStartOptions } from "../lib/types";
import { Brand } from "./Brand";
import { VoiceStartPanel } from "./voice/VoiceStartPanel";
import { isMicBlocked, MIC_BLOCKED } from "../lib/micErrors";

type Source = Mode | "talk";

interface Props {
  error?: string;
  onStart(opts: StartOptions): void;
  onStartVoice(opts: VoiceStartOptions): void;
}

export function StartScreen({ error, onStart, onStartVoice }: Props) {
  const [playbooks, setPlaybooks] = useState<PlaybookSummary[]>(FALLBACK_PLAYBOOKS);
  const [scripts, setScripts] = useState<string[]>([]);
  const [backendNote, setBackendNote] = useState<string | null>(null);
  // ?playbook=&script= preselect the start form (used by demo links; see App.tsx for ?autostart=1).
  const [playbook, setPlaybook] = useState(() => URL_PARAMS.get("playbook") || FALLBACK_PLAYBOOKS[0].name);
  const [source, setSource] = useState<Source>(() => {
    const m = URL_PARAMS.get("mode");
    return m === "mic" || m === "talk" ? m : "replay";
  });
  const [script, setScript] = useState(() => URL_PARAMS.get("script") || "");
  const [busy, setBusy] = useState(false);
  // Voice form state.
  const [role, setRole] = useState<VoiceRole>("assistant");
  const [scenario, setScenario] = useState("");
  const [callId, setCallId] = useState("");
  const [voices, setVoices] = useState<VoiceOption[]>(MOCK ? MOCK_VOICES : FALLBACK_VOICES);
  const [voice, setVoice] = useState(FALLBACK_VOICES[0].id);

  // A failed start re-enables the form (the error is shown above it).
  useEffect(() => {
    if (error) setBusy(false);
  }, [error]);

  // Playbook catalogue: once on mount. Offline (or mock) it keeps the built-in list.
  useEffect(() => {
    if (MOCK) return;
    getPlaybooks()
      .then((p) => {
        if (!p.length) return;
        setPlaybooks(p);
        setPlaybook((cur) => (p.some((x) => x.name === cur) ? cur : p[0].name));
      })
      .catch(() => setBackendNote("Backend unreachable: showing built-in playbooks."));
  }, []);

  // Voice catalogue: once on mount. A failed call keeps the default voice only.
  useEffect(() => {
    if (MOCK) return;
    let alive = true;
    void getVoices().then((list) => {
      if (!alive || !list.length) return;
      setVoices(list);
      setVoice((cur) => (list.some((v) => v.id === cur) ? cur : list[0].id));
    });
    return () => {
      alive = false;
    };
  }, []);

  // Scripts follow the selected playbook. A response for a previous selection is ignored.
  useEffect(() => {
    let current = true;
    const load = MOCK ? Promise.resolve(FALLBACK_SCRIPTS[playbook] ?? []) : getScripts(playbook);
    void load.then((list) => {
      if (!current) return;
      setScripts(list);
      setScript((cur) => (list.includes(cur) ? cur : list[0] ?? ""));
    });
    return () => {
      current = false;
    };
  }, [playbook]);

  const selected = playbooks.find((p) => p.name === playbook);
  const scenarios = selected?.scenarios ?? [];
  const activeScenario = scenarios.some((s) => s.name === scenario) ? scenario : scenarios[0]?.name ?? "";

  const pickScenario = (name: string) => {
    setScenario(name);
    // A scenario can pin its own voice (playbook YAML `practice.voice`).
    const pinned = scenarios.find((s) => s.name === name)?.voice;
    if (pinned && voices.some((v) => v.id === pinned)) setVoice(pinned);
  };

  const submit = () => {
    setBusy(true);
    if (source === "talk") {
      onStartVoice({
        role,
        playbook,
        scenario: role === "practice" ? activeScenario || undefined : undefined,
        callId: role === "assistant" && callId.trim() ? callId.trim() : undefined,
        voice,
      });
      return;
    }
    onStart({ playbook, mode: source, script: source === "replay" ? script : undefined });
  };

  const label = source === "talk" ? (busy ? "Connecting…" : "Start voice session") : busy ? "Starting…" : "Start call";
  const disabled = busy || (source === "replay" && !script);

  return (
    <div className="flex min-h-full flex-col">
      <header className="flex items-center justify-between px-8 py-4">
        <Brand />
        {MOCK && (
          <span className="chip border-[var(--color-line)] text-[var(--color-muted)]">
            <Sparkles className="size-3" /> Mock mode · simulated stream{MOCK_SPEED !== 1 ? ` · ${MOCK_SPEED}x` : ""}
          </span>
        )}
      </header>

      <main className="mx-auto grid w-full max-w-5xl flex-1 items-center gap-8 px-6 pb-10 pt-2 md:grid-cols-[1.1fr_1fr]">
        <section>
          <p className="kbd-label">{source === "talk" ? "Voice" : "New call"}</p>
          <h1 className="mt-2 text-[34px] font-semibold leading-[1.1] tracking-tight">
            {source === "talk" ? "Talk to Sidekick" : "Start a sidekick session"}
          </h1>
          <p className="mt-3 max-w-md text-[14px] leading-relaxed text-[var(--color-muted)]">
            {source === "talk"
              ? "Speak with Sidekick out loud. Ask for next questions and summaries, or rehearse with an AI customer while coaching cards run alongside."
              : "Live transcription, enrichment and suggestion cards stream into one console. The sidekick stays quiet unless it has something grounded to say."}
          </p>

          {error && !isMicBlocked(error) && (
            <div role="alert" className="mt-6 rounded-lg border border-rose-500/30 bg-rose-500/10 px-4 py-3 text-[13px] text-rose-200">
              {error}
            </div>
          )}
          {backendNote && !error && (
            <p role="status" className="mt-6 flex gap-2 rounded-lg border border-amber-400/25 bg-amber-400/[0.07] px-4 py-3 text-[13px] text-amber-100">
              <Radio className="mt-0.5 size-4 shrink-0 text-amber-300" />
              <span>{backendNote} Scripted calls need the backend, but Live mic and Talk still work once it is up.</span>
            </p>
          )}

          <div className="mt-7">
            <p className="kbd-label mb-2">Playbook</p>
            <div className="grid gap-2">
              {playbooks.map((p) => {
                const active = p.name === playbook;
                return (
                  <button
                    key={p.name}
                    type="button"
                    aria-pressed={active}
                    onClick={() => setPlaybook(p.name)}
                    className={`flex items-center justify-between rounded-xl border px-4 py-3 text-left transition-colors ${
                      active
                        ? "border-sky-400/60 bg-sky-400/[0.07]"
                        : "border-[var(--color-line)] bg-[var(--color-panel)] hover:border-slate-400"
                    }`}
                  >
                    <div>
                      <div className="text-[14px] font-semibold">{p.label}</div>
                      <div className="text-[12px] text-[var(--color-muted)]">{p.persona}</div>
                    </div>
                    <span
                      className={`grid size-4 place-items-center rounded-full border ${
                        active ? "border-sky-300" : "border-slate-600"
                      }`}
                    >
                      {active && <span className="size-2 rounded-full bg-sky-300" />}
                    </span>
                  </button>
                );
              })}
            </div>
          </div>
        </section>

        <section className="panel p-5">
          <p className="kbd-label">Source</p>
          <div className="mt-3 grid grid-cols-3 gap-2 rounded-xl bg-[var(--color-ink)] p-1">
            <ModeButton active={source === "replay"} onClick={() => setSource("replay")} icon={<PlayCircle className="size-4" />} label="Replay" />
            <ModeButton active={source === "mic"} onClick={() => setSource("mic")} icon={<Mic className="size-4" />} label="Live mic" />
            <ModeButton active={source === "talk"} onClick={() => setSource("talk")} icon={<Headphones className="size-4" />} label="Talk" />
          </div>

          {source === "replay" && (
            <div className="mt-5">
              <label className="kbd-label mb-2 block" htmlFor="script">
                Scripted call
              </label>
              <select
                id="script"
                value={script}
                onChange={(e) => setScript(e.target.value)}
                className="w-full rounded-lg border border-[var(--color-line)] bg-[var(--color-ink)] px-3 py-2.5 text-[14px] focus:border-sky-400/60"
              >
                {scripts.length === 0 && <option value="">No scripts for this playbook</option>}
                {scripts.map((s) => (
                  <option key={s} value={s}>
                    {s.replace(/_/g, " ")}
                  </option>
                ))}
              </select>
              {scripts.length === 0 ? (
                <p className="mt-3 text-[12.5px] leading-relaxed text-amber-100/90">
                  No scripted calls for this playbook. They load from the backend, so start it or pick Live mic or Talk.
                </p>
              ) : (
                <p className="mt-3 text-[12.5px] leading-relaxed text-[var(--color-muted)]">
                  Replays a dual-channel recording at real speaking pace. No microphone or audio socket needed.
                </p>
              )}
            </div>
          )}

          {source === "mic" && (
            <div className="mt-5 space-y-3 text-[12.5px] leading-relaxed text-[var(--color-muted)]">
              <p className="flex gap-2">
                <Radio className="mt-0.5 size-4 shrink-0 text-sky-300" />
                Mic audio streams as 16 kHz PCM over the audio socket. Solo demos default to the customer channel.
              </p>
              <p>You can switch channels and enable the browser speech fallback from the console header.</p>
            </div>
          )}

          {source === "talk" && (
            <VoiceStartPanel
              playbook={selected}
              role={role}
              onRole={setRole}
              scenario={activeScenario}
              onScenario={pickScenario}
              callId={callId}
              onCallId={setCallId}
              voices={voices}
              voice={voice}
              onVoice={setVoice}
            />
          )}

          {error && isMicBlocked(error) && source !== "replay" && <MicBlocked onRetry={submit} disabled={busy} />}
          <button type="button" disabled={disabled} onClick={submit} className="btn btn-primary mt-6 w-full py-3 text-[15px]">
            {label}
          </button>
          {MOCK && <p className="mt-3 text-center text-[11.5px] text-[var(--color-muted)]">Mock mode ignores the backend and mic.</p>}
        </section>
      </main>
    </div>
  );
}

function ModeButton({ active, onClick, icon, label }: { active: boolean; onClick(): void; icon: ReactNode; label: string }) {
  return (
    <button
      type="button"
      aria-pressed={active}
      onClick={onClick}
      className={`flex items-center justify-center gap-2 rounded-lg px-2 py-2.5 text-[13.5px] font-semibold transition-colors ${
        active ? "bg-[var(--color-raised)] text-white shadow-sm" : "text-[var(--color-muted)] hover:text-slate-200"
      }`}
    >
      {icon}
      {label}
    </button>
  );
}

/** Microphone refused by the browser. Says what to change and retries the same start. */
function MicBlocked({ onRetry, disabled }: { onRetry(): void; disabled: boolean }) {
  return (
    <div role="alert" className="mt-6 rounded-xl border border-amber-400/35 bg-amber-400/[0.08] p-4 text-amber-50">
      <div className="flex gap-3">
        <MicOff className="mt-0.5 size-5 shrink-0 text-amber-300" aria-hidden />
        <div className="min-w-0 flex-1">
          <p className="text-[14px] font-semibold">{MIC_BLOCKED}</p>
          <p className="mt-1 text-[13px] leading-relaxed text-amber-100/90">
            Your browser stopped Sidekick from using the microphone. Click the lock or site-settings icon in the address bar,
            set Microphone to Allow, then try again.
          </p>
          <button type="button" onClick={onRetry} disabled={disabled} className="btn btn-ghost mt-3 py-1.5 text-[12.5px]">
            <RotateCcw className="size-3.5" /> Try again
          </button>
        </div>
      </div>
    </div>
  );
}
