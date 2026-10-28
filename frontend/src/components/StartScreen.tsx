import { useEffect, useState, type ReactNode } from "react";
import { Mic, PlayCircle, Radio, Sparkles } from "lucide-react";
import { FALLBACK_PLAYBOOKS, FALLBACK_SCRIPTS, getPlaybooks, getScripts } from "../lib/api";
import { MOCK, MOCK_SPEED, URL_PARAMS } from "../lib/useCallSession";
import type { Mode, PlaybookSummary, StartOptions } from "../lib/types";
import { Brand } from "./Brand";

interface Props {
  error?: string;
  onStart(opts: StartOptions): void;
}

export function StartScreen({ error, onStart }: Props) {
  const [playbooks, setPlaybooks] = useState<PlaybookSummary[]>(FALLBACK_PLAYBOOKS);
  const [scripts, setScripts] = useState<string[]>([]);
  const [backendNote, setBackendNote] = useState<string | null>(null);
  // ?playbook=&script= preselect the start form (used by demo links; see App.tsx for ?autostart=1).
  const [playbook, setPlaybook] = useState(() => URL_PARAMS.get("playbook") || FALLBACK_PLAYBOOKS[0].name);
  const [mode, setMode] = useState<Mode>(() => (URL_PARAMS.get("mode") === "mic" ? "mic" : "replay"));
  const [script, setScript] = useState(() => URL_PARAMS.get("script") || "");
  const [busy, setBusy] = useState(false);

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

  const submit = () => {
    setBusy(true);
    onStart({ playbook, mode, script: mode === "replay" ? script : undefined });
  };


  return (
    <div className="flex min-h-full flex-col">
      <header className="flex items-center justify-between px-8 py-5">
        <Brand />
        {MOCK && (
          <span className="chip border-[var(--color-line)] text-[var(--color-muted)]">
            <Sparkles className="size-3" /> Mock mode · simulated stream{MOCK_SPEED !== 1 ? ` · ${MOCK_SPEED}x` : ""}
          </span>
        )}
      </header>

      <main className="mx-auto grid w-full max-w-5xl flex-1 items-center gap-8 px-6 pb-16 pt-4 md:grid-cols-[1.1fr_1fr]">
        <section>
          <p className="kbd-label">New call</p>
          <h1 className="mt-2 text-[34px] font-semibold leading-[1.1] tracking-tight">
            Start a copilot session
          </h1>
          <p className="mt-3 max-w-md text-[14px] leading-relaxed text-[var(--color-muted)]">
            Live transcription, enrichment and suggestion cards stream into one console. The copilot
            stays quiet unless it has something grounded to say.
          </p>

          {(error || backendNote) && (
            <div className="mt-6 rounded-lg border border-rose-500/30 bg-rose-500/10 px-4 py-3 text-[13px] text-rose-200">
              {error ?? backendNote}
            </div>
          )}

          <div className="mt-8">
            <p className="kbd-label mb-2">Playbook</p>
            <div className="grid gap-2">
              {playbooks.map((p) => {
                const active = p.name === playbook;
                return (
                  <button
                    key={p.name}
                    type="button"
                    onClick={() => setPlaybook(p.name)}
                    className={`flex items-center justify-between rounded-xl border px-4 py-3 text-left transition-colors ${
                      active
                        ? "border-sky-400/60 bg-sky-400/[0.07]"
                        : "border-[var(--color-line)] bg-[var(--color-panel)] hover:border-slate-500"
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

        <section className="panel p-6">
          <p className="kbd-label">Source</p>
          <div className="mt-3 grid grid-cols-2 gap-2 rounded-xl bg-[var(--color-ink)] p-1">
            <ModeButton active={mode === "replay"} onClick={() => setMode("replay")} icon={<PlayCircle className="size-4" />} label="Replay script" />
            <ModeButton active={mode === "mic"} onClick={() => setMode("mic")} icon={<Mic className="size-4" />} label="Live mic" />
          </div>

          {mode === "replay" ? (
            <div className="mt-5">
              <label className="kbd-label mb-2 block" htmlFor="script">
                Scripted call
              </label>
              <select
                id="script"
                value={script}
                onChange={(e) => setScript(e.target.value)}
                className="w-full rounded-lg border border-[var(--color-line)] bg-[var(--color-ink)] px-3 py-2.5 text-[14px] outline-none focus:border-sky-400/60"
              >
                {scripts.length === 0 && <option value="">No scripts for this playbook</option>}
                {scripts.map((s) => (
                  <option key={s} value={s}>
                    {s.replace(/_/g, " ")}
                  </option>
                ))}
              </select>
              <p className="mt-3 text-[12.5px] leading-relaxed text-[var(--color-muted)]">
                Replays a dual-channel recording at real speaking pace. No microphone or audio socket needed.
              </p>
            </div>
          ) : (
            <div className="mt-5 space-y-3 text-[12.5px] leading-relaxed text-[var(--color-muted)]">
              <p className="flex gap-2">
                <Radio className="mt-0.5 size-4 shrink-0 text-sky-300" />
                Mic audio streams as 16 kHz PCM over the audio socket. Solo demos default to the customer channel.
              </p>
              <p>You can switch channels and enable the browser speech fallback from the console header.</p>
            </div>
          )}

          <button
            type="button"
            disabled={busy || (mode === "replay" && !script)}
            onClick={submit}
            className="btn btn-primary mt-7 w-full py-3 text-[15px]"
          >
            {busy ? "Starting…" : "Start call"}
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
      onClick={onClick}
      className={`flex items-center justify-center gap-2 rounded-lg px-3 py-2.5 text-[13.5px] font-semibold transition-colors ${
        active ? "bg-[var(--color-raised)] text-white shadow-sm" : "text-[var(--color-muted)] hover:text-slate-200"
      }`}
    >
      {icon}
      {label}
    </button>
  );
}
