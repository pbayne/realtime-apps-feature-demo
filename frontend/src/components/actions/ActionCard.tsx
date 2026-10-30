import { useEffect, useId, useRef, useState, type ChangeEvent, type FormEvent, type KeyboardEvent } from "react";
import { Check, CalendarDays, Download, Eye, GraduationCap, Loader2, Mail, Pencil, Undo2, UserRound, X } from "lucide-react";
import {
  type ActionKind,
  type ActionView,
  type Decision,
  type Draft,
  DEFAULT_REMINDERS,
  DURATIONS,
  EMPTY_DRAFT,
  KIND_LABEL,
  type Reminder,
  canDecide,
  canUndo,
  draftFromParams,
  fetchPreview,
  formatWhen,
  inviteUrl,
  isRendered,
  messageUrl,
  mockPreview,
  paramsFromDraft,
  reminderLine,
} from "../../lib/actions";

const KIND_ICON = { send_email: Mail, create_meeting: CalendarDays, update_contact: UserRound, enroll: GraduationCap };

/** Chip colours are the ones the rest of the app uses for these states (see VoiceConsole STATUS_STYLE). */
const STATUS: Record<ActionView["status"], { label: string; cls: string }> = {
  proposed: { label: "Needs approval", cls: "text-amber-200 border-amber-400/40" },
  executing: { label: "Working", cls: "text-sky-200 border-sky-400/40" },
  done: { label: "Sent", cls: "text-emerald-200 border-emerald-400/40" },
  outboxed: { label: "Ready to send", cls: "text-sky-200 border-sky-400/40" },
  failed: { label: "Failed", cls: "text-rose-200 border-rose-400/40" },
  rejected: { label: "Rejected", cls: "text-slate-300 border-slate-500" },
  undone: { label: "Undone", cls: "text-slate-300 border-slate-500" },
};

export function statusLabel(status: ActionView["status"]): string {
  return STATUS[status].label;
}

export const inputCls =
  "w-full rounded-md border border-[var(--color-line)] bg-[var(--color-panel)] px-2.5 py-1.5 text-[13px] text-slate-200 placeholder:text-[var(--color-muted)]";

interface Props {
  action: ActionView;
  /** Programs of the playbook, for enroll. */
  programs: string[];
  onDecide(action: ActionView, decision: Decision, params?: Record<string, unknown>): Promise<string | null>;
}

export function ActionCard({ action: a, programs, onDecide }: Props) {
  const Icon = KIND_ICON[a.kind];
  const status = STATUS[a.status];
  const [editing, setEditing] = useState(false);
  const [busy, setBusy] = useState<Decision | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [preview, setPreview] = useState<string | null>(null);
  const cardRef = useRef<HTMLElement>(null);
  const titleId = useId();
  const safety = a.safety;
  const testMode = !!safety?.lock || !!a.result?.safety?.lock;
  const originals = a.result?.safety?.original_to ?? a.result?.safety?.original_attendees;
  const mock = a.owner === "mock";

  // A decision changed the status: the working state ends, and a failed approval shows its reason.
  useEffect(() => {
    if (a.status !== "executing") setBusy(null);
  }, [a.status]);

  const decide = async (decision: Decision, params?: Record<string, unknown>) => {
    setBusy(decision);
    setError(null);
    const err = await onDecide(a, decision, params);
    setBusy(null);
    if (err) setError(err);
    else setEditing(false);
  };

  const onKey = (e: KeyboardEvent<HTMLElement>) => {
    // Enter approves the focused card (not a control inside it, which keeps its own Enter).
    if (e.key === "Enter" && e.target === e.currentTarget && canDecide(a.status) && !editing) {
      e.preventDefault();
      void decide("approve");
    }
  };

  const showPreview = async () => {
    if (preview !== null) {
      setPreview(null);
      return;
    }
    if (mock) {
      setPreview(mockPreview(a));
      return;
    }
    try {
      setPreview(await fetchPreview(a));
    } catch (e) {
      setError(e instanceof Error ? e.message : "Preview unavailable");
    }
  };

  return (
    <article
      ref={cardRef}
      tabIndex={0}
      onKeyDown={onKey}
      aria-labelledby={titleId}
      data-status={a.status}
      className="anim-card-in relative overflow-hidden rounded-xl border border-[var(--color-line)] bg-[var(--color-raised)] p-3.5 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-sky-300"
    >
      <div className="flex items-start gap-3">
        <div className="grid size-8 shrink-0 place-items-center rounded-lg bg-[var(--color-panel)] text-sky-200">
          <Icon className="size-4" aria-hidden />
        </div>
        <div className="min-w-0 flex-1">
          <div className="flex flex-wrap items-center gap-2">
            <span className="text-[11px] font-semibold uppercase tracking-wider text-[var(--color-muted)]">{KIND_LABEL[a.kind]}</span>
            <span className={`chip border bg-transparent ${status.cls}`}>
              {a.status === "executing" ? <Loader2 className="size-3 animate-spin" aria-hidden /> : null}
              {status.label}
            </span>
          </div>
          <p id={titleId} className="mt-0.5 text-[13.5px] font-medium leading-snug text-slate-100">
            {a.summary}
          </p>
          <Details action={a} />
          {testMode && (
            <p className="mt-1.5 text-[11.5px] text-amber-200">
              Test mode: only sends to {safety?.safe_recipient ?? "the safe recipient"}
              {originals && originals.length > 0 ? ` (was ${originals.join(", ")})` : ""}
            </p>
          )}
          {a.error && (
            <p role="alert" className="mt-1.5 text-[12.5px] text-rose-200">
              {a.error}
            </p>
          )}
        </div>
      </div>

      {editing ? (
        <div className="mt-3">
          <ActionForm
            kind={a.kind}
            initial={draftFromParams(a.kind, a.params)}
            programs={programs}
            submitLabel={a.status === "failed" ? "Save and retry" : "Save and approve"}
            onCancel={() => setEditing(false)}
            onSubmit={(params) => void decide("approve", params)}
            busy={busy === "approve"}
          />
        </div>
      ) : (
        <Actions a={a} busy={busy} mock={mock} onDecide={decide} onEdit={() => setEditing(true)} onPreview={showPreview} previewing={preview !== null} />
      )}

      {error && !a.error && (
        <p role="alert" className="mt-2 text-[12.5px] text-rose-200">
          {error}
        </p>
      )}
      {preview !== null && (
        <pre
          aria-label={`Preview of ${a.summary}`}
          className="scroll-thin mt-3 max-h-56 overflow-auto whitespace-pre-wrap rounded-lg border border-[var(--color-line)] bg-[var(--color-panel)] p-3 font-mono text-[11.5px] leading-relaxed text-slate-300"
        >
          {preview}
        </pre>
      )}
    </article>
  );
}

function Details({ action: a }: { action: ActionView }) {
  const p = a.params as Record<string, unknown>;
  if (a.kind === "create_meeting") {
    return (
      <p className="mt-1 text-[12px] text-[var(--color-muted)]">
        {formatWhen(p.start_iso)} · {String(p.duration_min ?? 45)} min · {reminderLine(p.reminders as Reminder[] | undefined)}
      </p>
    );
  }
  if (a.kind === "send_email") {
    return <p className="mt-1 line-clamp-2 text-[12px] text-[var(--color-muted)]">{String(p.body_text ?? "")}</p>;
  }
  return null;
}

function Actions({
  a,
  busy,
  mock,
  onDecide,
  onEdit,
  onPreview,
  previewing,
}: {
  a: ActionView;
  busy: Decision | null;
  mock: boolean;
  onDecide(d: Decision): void;
  onEdit(): void;
  onPreview(): void;
  previewing: boolean;
}) {
  const pending = canDecide(a.status);
  const rendered = isRendered(a);
  const ready = (a.status === "done" || a.status === "outboxed") && rendered;
  return (
    <div className="mt-3 flex flex-wrap items-center gap-2">
      {pending && (
        <>
          <button type="button" className="btn btn-primary py-1.5 text-[12.5px]" onClick={() => onDecide("approve")} disabled={!!busy}>
            {busy === "approve" ? <Loader2 className="size-3.5 animate-spin" aria-hidden /> : <Check className="size-3.5" aria-hidden />}
            {a.status === "failed" ? "Retry" : "Approve"}
          </button>
          <button type="button" className="btn btn-ghost py-1.5 text-[12.5px]" onClick={onEdit} disabled={!!busy}>
            <Pencil className="size-3.5" aria-hidden /> Edit
          </button>
          <button type="button" className="btn btn-ghost py-1.5 text-[12.5px]" onClick={() => onDecide("reject")} disabled={!!busy}>
            <X className="size-3.5" aria-hidden /> Reject
          </button>
        </>
      )}

      {rendered && (
        <button type="button" className="btn btn-ghost py-1.5 text-[12.5px]" onClick={onPreview} aria-expanded={previewing}>
          <Eye className="size-3.5" aria-hidden /> {previewing ? "Hide preview" : "Preview"}
        </button>
      )}
      {ready && !mock && (
        <a className="btn btn-ghost py-1.5 text-[12.5px]" href={messageUrl(a.action_id)} download={`${a.kind}-${a.action_id.slice(0, 8)}.eml`}>
          <Download className="size-3.5" aria-hidden /> .eml
        </a>
      )}
      {ready && a.kind === "create_meeting" && !mock && (
        <a className="btn btn-ghost py-1.5 text-[12.5px]" href={inviteUrl(a.action_id)} download={`invite-${a.action_id.slice(0, 8)}.ics`}>
          <Download className="size-3.5" aria-hidden /> .ics
        </a>
      )}
      {canUndo(a) && (
        <button type="button" className="btn btn-ghost py-1.5 text-[12.5px]" onClick={() => onDecide("undo")} disabled={!!busy}>
          <Undo2 className="size-3.5" aria-hidden /> Undo
        </button>
      )}
      {a.status === "outboxed" && (
        <span className="text-[11.5px] text-[var(--color-muted)]">ready to send (no SMTP configured)</span>
      )}
    </div>
  );
}

/** Inline editor per kind. Submitting sends params; the server validates them and says why when they are wrong. */
export function ActionForm({
  kind,
  initial,
  programs,
  submitLabel,
  onSubmit,
  onCancel,
  busy,
}: {
  kind: ActionKind;
  initial: Draft;
  programs: string[];
  submitLabel: string;
  onSubmit(params: Record<string, unknown>): void;
  onCancel(): void;
  busy: boolean;
}) {
  const [d, setD] = useState<Draft>({ ...EMPTY_DRAFT[kind], ...initial });
  const [err, setErr] = useState<string | null>(null);
  const set = (k: string) => (e: ChangeEvent<HTMLInputElement | HTMLTextAreaElement | HTMLSelectElement>) =>
    setD((cur) => ({ ...cur, [k]: e.target.value }));
  const id = useId();
  const field = (k: string, label: string, props: { type?: string; textarea?: boolean; placeholder?: string } = {}) => (
    <label className="block text-[11.5px] font-medium text-[var(--color-muted)]" htmlFor={`${id}-${k}`}>
      {label}
      {props.textarea ? (
        <textarea id={`${id}-${k}`} className={`${inputCls} mt-1 min-h-[64px]`} value={d[k]} onChange={set(k)} placeholder={props.placeholder} />
      ) : (
        <input id={`${id}-${k}`} type={props.type ?? "text"} className={`${inputCls} mt-1`} value={d[k]} onChange={set(k)} placeholder={props.placeholder} />
      )}
    </label>
  );

  const submit = (e: FormEvent) => {
    e.preventDefault();
    const res = paramsFromDraft(kind, d);
    if (!res.ok) {
      setErr(res.error);
      return;
    }
    setErr(null);
    onSubmit(res.params);
  };

  return (
    <form onSubmit={submit} className="grid gap-2.5 rounded-lg border border-[var(--color-line)] bg-[var(--color-panel)] p-3" aria-label={`${KIND_LABEL[kind]} details`}>
      {kind === "send_email" && (
        <>
          {field("to", "To (comma separated)", { placeholder: "dana.lee@example.com" })}
          {field("cc", "Cc (optional)")}
          {field("subject", "Subject")}
          {field("body_text", "Message", { textarea: true })}
        </>
      )}
      {kind === "create_meeting" && (
        <>
          {field("title", "Title")}
          {field("attendees", "Attendees (comma separated)", { placeholder: "dana.lee@example.com" })}
          <div className="grid grid-cols-2 gap-2.5">
            {field("date", "Date", { type: "date" })}
            {field("time", "Start time", { type: "time" })}
          </div>
          <label className="block text-[11.5px] font-medium text-[var(--color-muted)]" htmlFor={`${id}-duration`}>
            Duration
            <select id={`${id}-duration`} className={`${inputCls} mt-1`} value={d.duration} onChange={set("duration")}>
              {DURATIONS.map((m) => (
                <option key={m} value={m}>
                  {m} min
                </option>
              ))}
            </select>
          </label>
          {field("location", "Location (optional)")}
          <p className="text-[11.5px] text-[var(--color-muted)]">{reminderLine(DEFAULT_REMINDERS)}</p>
        </>
      )}
      {kind === "update_contact" && (
        <>
          {field("email", "Contact email (to find them)")}
          <div className="grid grid-cols-2 gap-2.5">
            {field("first_name", "First name")}
            {field("last_name", "Last name")}
          </div>
          {field("company", "Company")}
          {field("phone", "Phone")}
        </>
      )}
      {kind === "enroll" && (
        <>
          {field("email", "Contact email")}
          <label className="block text-[11.5px] font-medium text-[var(--color-muted)]" htmlFor={`${id}-program`}>
            Program
            <select id={`${id}-program`} className={`${inputCls} mt-1`} value={d.program} onChange={set("program")}>
              <option value="">Choose a program</option>
              {programs.map((p) => (
                <option key={p} value={p}>
                  {p}
                </option>
              ))}
            </select>
          </label>
          {field("note", "Note (optional)")}
        </>
      )}
      {err && (
        <p role="alert" className="text-[12.5px] text-rose-200">
          {err}
        </p>
      )}
      <div className="flex flex-wrap gap-2">
        <button type="submit" className="btn btn-primary py-1.5 text-[12.5px]" disabled={busy}>
          {busy && <Loader2 className="size-3.5 animate-spin" aria-hidden />} {submitLabel}
        </button>
        <button type="button" className="btn btn-ghost py-1.5 text-[12.5px]" onClick={onCancel}>
          Cancel
        </button>
      </div>
    </form>
  );
}
