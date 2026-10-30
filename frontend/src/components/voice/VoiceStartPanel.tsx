import type { ReactNode } from "react";
import { Headphones } from "lucide-react";
import type { PlaybookSummary, VoiceOption, VoiceRole } from "../../lib/types";

interface Props {
  playbook: PlaybookSummary | undefined;
  role: VoiceRole;
  onRole(role: VoiceRole): void;
  scenario: string;
  onScenario(name: string): void;
  callId: string;
  onCallId(v: string): void;
  voices: VoiceOption[];
  voice: string;
  onVoice(id: string): void;
}

const selectCls =
  "w-full rounded-lg border border-[var(--color-line)] bg-[var(--color-ink)] px-3 py-2.5 text-[14px] outline-none focus:border-sky-400/60";

export function VoiceStartPanel(p: Props) {
  const scenarios = playbookScenarios(p.playbook);
  const active = scenarios.find((s) => s.name === p.scenario);

  return (
    <div className="mt-5 space-y-5">
      <div>
        <p className="kbd-label mb-2">Role</p>
        <div className="grid grid-cols-2 gap-1 rounded-xl bg-[var(--color-ink)] p-1">
          <Seg active={p.role === "assistant"} onClick={() => p.onRole("assistant")}>
            Assistant
          </Seg>
          <Seg active={p.role === "practice"} onClick={() => p.onRole("practice")}>
            Practice customer
          </Seg>
        </div>
        <p className="mt-2 text-[12.5px] leading-relaxed text-[var(--color-muted)]">
          {p.role === "assistant"
            ? "Ask Sidekick before, during or after a call: next questions, summaries, follow-up drafts."
            : "Sidekick plays the customer from a playbook scenario. You talk, it answers aloud, and live coaching cards appear on the right."}
        </p>
      </div>

      {p.role === "assistant" ? (
        <div>
          <label className="kbd-label mb-2 block" htmlFor="call-link">
            Link to a call (optional)
          </label>
          <input
            id="call-link"
            value={p.callId}
            onChange={(e) => p.onCallId(e.target.value)}
            placeholder="e.g. c-123, or leave blank"
            className={`${selectCls} placeholder:text-slate-500`}
          />
          <p className="mt-2 text-[12px] leading-relaxed text-[var(--color-muted)]">
            A linked call adds its transcript, cards and coverage to Sidekick's context.
          </p>
        </div>
      ) : (
        <div>
          <label className="kbd-label mb-2 block" htmlFor="scenario">
            Scenario
          </label>
          {scenarios.length > 0 ? (
            <select id="scenario" value={p.scenario} onChange={(e) => p.onScenario(e.target.value)} className={selectCls}>
              {scenarios.map((s) => (
                <option key={s.name} value={s.name}>
                  {s.title}
                </option>
              ))}
            </select>
          ) : (
            <p className="rounded-lg border border-[var(--color-line)] bg-[var(--color-ink)] px-3 py-2.5 text-[13.5px] text-[var(--color-muted)]">
              No scenarios for this playbook. Free practice with a generic customer.
            </p>
          )}
          {active?.persona && <p className="mt-2 text-[12.5px] leading-relaxed text-slate-300">Persona: {active.persona}</p>}
        </div>
      )}

      <div>
        <label className="kbd-label mb-2 block" htmlFor="voice-pick">
          Voice
        </label>
        <select id="voice-pick" value={p.voice} onChange={(e) => p.onVoice(e.target.value)} className={selectCls}>
          {p.voices.map((v) => (
            <option key={v.id} value={v.id}>
              {v.label}
            </option>
          ))}
        </select>
      </div>

      <p className="flex gap-2 text-[12.5px] leading-relaxed text-[var(--color-muted)]">
        <Headphones className="mt-0.5 size-4 shrink-0 text-sky-300" />
        Hands-free by default. Use headphones so Sidekick does not hear itself. Your browser asks for the microphone.
      </p>
    </div>
  );
}

function playbookScenarios(pb: PlaybookSummary | undefined) {
  return pb?.scenarios ?? [];
}

function Seg({ active, onClick, children }: { active: boolean; onClick(): void; children: ReactNode }) {
  return (
    <button
      type="button"
      onClick={onClick}
      aria-pressed={active}
      className={`rounded-lg px-3 py-2 text-[13px] font-semibold transition-colors ${
        active ? "bg-[var(--color-raised)] text-white shadow-sm" : "text-[var(--color-muted)] hover:text-slate-200"
      }`}
    >
      {children}
    </button>
  );
}
