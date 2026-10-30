import { useState } from "react";
import { ListChecks, Plus, ShieldAlert, X } from "lucide-react";
import {
  type ActionKind,
  type ActionView,
  type Decision,
  EMPTY_DRAFT,
  KIND_LABEL,
  createAction,
  decideAction,
  mockDecide,
  mockProposal,
  usePrograms,
} from "../../lib/actions";
import { ActionCard, ActionForm, statusLabel } from "./ActionCard";

export const SAFE_RECIPIENT_FALLBACK = "patrick.bayne@databricks.com";

interface Props {
  actions: ActionView[];
  /** Live scope: a voice session or a call. Manual creates and decisions are checked against it on the server. */
  scope: { session_id?: string; call_id?: string };
  /** The playbook the conversation or call runs; its enrollment programs feed the enroll form. */
  playbook: string;
  /** Mock mode: proposals and execution are simulated locally. */
  mock: boolean;
  /** Called for every change to an action: a decision response, a manual create, or a mock step. */
  onUpdate(action: ActionView): void;
  /** Visual size: the call console gives the feed a fixed height, the voice console fills its column. */
  className?: string;
}

const CREATE_KINDS: ActionKind[] = ["send_email", "create_meeting", "update_contact", "enroll"];

export function ActionsPanel({ actions, scope, playbook, mock, onUpdate, className = "" }: Props) {
  const programs = usePrograms(playbook);
  const [creating, setCreating] = useState(false);
  const [kind, setKind] = useState<ActionKind>("send_email");
  const [createError, setCreateError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const newestFirst = [...actions].reverse();
  const safety = actions.find((a) => a.safety)?.safety ?? (mock ? { lock: true, safe_recipient: SAFE_RECIPIENT_FALLBACK, mode: "outbox" as const } : undefined);
  const pending = actions.filter((a) => a.status === "proposed" || a.status === "failed").length;
  const last = actions[actions.length - 1];
  // Polite live region: the latest change is announced without interrupting the conversation.
  const announcement = last ? `${last.summary}: ${statusLabel(last.status)}` : "";

  const decide = async (a: ActionView, decision: Decision, params?: Record<string, unknown>): Promise<string | null> => {
    if (mock) {
      mockDecide(params ? { ...a, params } : a, decision, onUpdate);
      return null;
    }
    try {
      onUpdate(await decideAction(a.action_id, decision, params));
      return null;
    } catch (e) {
      return e instanceof Error ? e.message : "That did not work. Try again.";
    }
  };

  const create = async (params: Record<string, unknown>) => {
    setBusy(true);
    setCreateError(null);
    try {
      const made = mock
        ? { ...mockProposal(kind, scope, programs), params, summary: summarizeLocal(kind, params), proposed_by: "user" as const }
        : await createAction(scope, kind, params);
      onUpdate(made);
      setCreating(false);
    } catch (e) {
      setCreateError(e instanceof Error ? e.message : "Could not create the action.");
    } finally {
      setBusy(false);
    }
  };

  return (
    <section aria-labelledby="actions-heading" className={`panel flex min-h-0 flex-col overflow-hidden ${className}`}>
      <header className="flex items-center justify-between gap-3 border-b border-[var(--color-line)] px-4 py-3">
        <div className="min-w-0">
          <h2 id="actions-heading" className="flex items-center gap-2 text-[14px] font-semibold">
            <ListChecks className="size-4 text-sky-300" aria-hidden /> Actions
          </h2>
          <p className="text-[11.5px] text-[var(--color-muted)]">
            {pending > 0 ? `${pending} waiting for your approval` : "Nothing runs until you approve it"}
          </p>
        </div>
        <button
          type="button"
          className="btn btn-ghost py-1 text-[12px]"
          onClick={() => {
            setCreating((v) => !v);
            setCreateError(null);
          }}
          aria-expanded={creating}
        >
          {creating ? <X className="size-3.5" aria-hidden /> : <Plus className="size-3.5" aria-hidden />}
          {creating ? "Close" : "Action"}
        </button>
      </header>

      {safety?.lock && (
        <div role="note" className="flex items-start gap-2 border-b border-amber-400/25 bg-amber-400/[0.08] px-4 py-2 text-[12px] text-amber-100">
          <ShieldAlert className="mt-0.5 size-3.5 shrink-0" aria-hidden />
          <span>Test mode: only sends to {safety.safe_recipient}. Recipients are rewritten and the subject is marked.</span>
        </div>
      )}
      {safety && safety.mode === "outbox" && (
        <p className="border-b border-[var(--color-line)] px-4 py-1.5 text-[11.5px] text-[var(--color-muted)]">
          No mail relay is configured: approved mail is rendered and kept here, not sent.
        </p>
      )}

      <p aria-live="polite" className="sr-only">
        {announcement}
      </p>

      <div className="scroll-thin min-h-0 flex-1 space-y-3 overflow-y-auto px-3.5 py-3.5">
        {creating && (
          <div className="space-y-2">
            <label className="block text-[11.5px] font-medium text-[var(--color-muted)]" htmlFor="new-action-kind">
              What should Sidekick do?
            </label>
            <select
              id="new-action-kind"
              className="w-full rounded-md border border-[var(--color-line)] bg-[var(--color-panel)] px-2.5 py-1.5 text-[13px] text-slate-200"
              value={kind}
              onChange={(e) => setKind(e.target.value as ActionKind)}
            >
              {CREATE_KINDS.map((k) => (
                <option key={k} value={k}>
                  {KIND_LABEL[k]}
                </option>
              ))}
            </select>
            <ActionForm
              key={kind}
              kind={kind}
              initial={EMPTY_DRAFT[kind]}
              programs={programs}
              submitLabel="Add for approval"
              onSubmit={(params) => void create(params)}
              onCancel={() => setCreating(false)}
              busy={busy}
            />
            {createError && (
              <p role="alert" className="text-[12.5px] text-rose-200">
                {createError}
              </p>
            )}
          </div>
        )}

        {actions.length === 0 && !creating && (
          <div className="rounded-xl border border-dashed border-[var(--color-line)] px-4 py-7 text-center text-[13px] text-[var(--color-muted)]">
            <ListChecks className="mx-auto mb-2 size-5 text-slate-400" aria-hidden />
            Ask Sidekick to send an email, book a meeting, update a contact or enroll someone. It appears here for your
            approval first.
          </div>
        )}

        {newestFirst.map((a) => (
          <ActionCard key={a.action_id} action={a} programs={programs} onDecide={decide} />
        ))}
      </div>
    </section>
  );
}

/** Summary for a mock create (the server builds the same text for real creates). */
function summarizeLocal(kind: ActionKind, params: Record<string, unknown>): string {
  return `${KIND_LABEL[kind]}: ${String(params.subject ?? params.title ?? params.program ?? params.email ?? "")}`.trim();
}
