// Human-in-the-loop actions (SPEC 12, v0.7): wire types, the form <-> params mapping, the REST client, and the
// ?mock=1 simulation. The server is the source of truth; the UI only proposes, edits, approves or rejects.
import { useEffect, useState } from "react";
import { MOCK } from "./useCallSession";

export type ActionKind = "send_email" | "create_meeting" | "update_contact" | "enroll" | "update_record";
export type ActionStatus = "proposed" | "executing" | "done" | "outboxed" | "failed" | "rejected" | "undone";
export type Decision = "approve" | "reject" | "undo";

export interface Reminder {
  method: "email" | "display";
  minutes: number;
}

export interface SafetyInfo {
  lock: boolean;
  safe_recipient: string;
  /** smtp: messages are sent. outbox: they are rendered and stored only. */
  mode: "smtp" | "outbox";
}

export interface ActionResult {
  mode?: "smtp" | "outbox";
  has_eml?: boolean;
  has_ics?: boolean;
  has_cancel_ics?: boolean;
  safety?: { lock: boolean; original_to?: string[]; original_cc?: string[]; original_attendees?: string[] };
  [key: string]: unknown;
}

/** One action as the server sends it (Action.to_wire plus `safety`). */
export interface ActionView {
  action_id: string;
  session_id: string | null;
  call_id: string | null;
  owner: string;
  kind: ActionKind;
  params: Record<string, unknown> & Partial<Record<"to" | "cc" | "attendees", string[]>>;
  status: ActionStatus;
  summary: string;
  proposed_by: "model" | "user";
  created_at_ms: number;
  updated_at_ms: number | null;
  decided_at_ms: number | null;
  error: string | null;
  result: ActionResult | null;
  safety?: SafetyInfo;
}

export const KIND_LABEL: Record<ActionKind, string> = {
  send_email: "Email",
  create_meeting: "Meeting",
  update_contact: "Update contact",
  enroll: "Enroll",
  update_record: "Update account",
};

export const DURATIONS = [15, 30, 45, 60, 90, 120] as const;
export const DEFAULT_REMINDERS: Reminder[] = [
  { method: "email", minutes: 1440 },
  { method: "display", minutes: 10 },
];

/** Upsert by action_id, keeping arrival order (the server orders by creation time). */
export function upsertAction(list: ActionView[], action: ActionView): ActionView[] {
  const i = list.findIndex((a) => a.action_id === action.action_id);
  if (i < 0) return [...list, action];
  const next = list.slice();
  next[i] = action;
  return next;
}

export function canDecide(status: ActionStatus): boolean {
  return status === "proposed" || status === "failed";
}

export function canUndo(a: ActionView): boolean {
  return a.kind !== "send_email" && (a.status === "done" || a.status === "outboxed");
}

export function isRendered(a: ActionView): boolean {
  return a.kind === "send_email" || a.kind === "create_meeting";
}

/** "Reminder: email 1 day before + alert 10 min" (the card line; the server sends the same two alarms). */
export function reminderLine(reminders: Reminder[] | undefined): string {
  const list = reminders ?? [];
  if (list.length === 0) return "No reminders";
  const parts = list.map((r) => `${r.method === "email" ? "email" : "alert"} ${fmtMinutes(r.minutes)} before`);
  return `Reminder: ${parts.join(" + ")}`;
}

function fmtMinutes(minutes: number): string {
  if (minutes > 0 && minutes % 1440 === 0) {
    const days = minutes / 1440;
    return `${days} day${days === 1 ? "" : "s"}`;
  }
  return `${minutes} min`;
}

/** Local date ("2026-11-03") and time ("14:00") to an ISO 8601 string with this machine's UTC offset. */
export function localDateTimeToIso(date: string, time: string): string {
  const naive = `${date}T${time}:00`;
  const d = new Date(naive);
  if (Number.isNaN(d.getTime())) return "";
  const offset = -d.getTimezoneOffset();
  const sign = offset >= 0 ? "+" : "-";
  const abs = Math.abs(offset);
  return `${naive}${sign}${pad2(Math.floor(abs / 60))}:${pad2(abs % 60)}`;
}

/** The inverse, for the edit form: the instant as this machine's local date and time. */
export function isoToLocalParts(iso: string): { date: string; time: string } {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return { date: "", time: "" };
  return {
    date: `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())}`,
    time: `${pad2(d.getHours())}:${pad2(d.getMinutes())}`,
  };
}

function pad2(n: number): string {
  return String(n).padStart(2, "0");
}

export function formatWhen(iso: unknown): string {
  if (typeof iso !== "string") return "";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toLocaleString(undefined, { weekday: "short", day: "numeric", month: "short", hour: "2-digit", minute: "2-digit" });
}

// ---------------------------------------------------------------- form draft <-> params

export type Draft = Record<string, string>;

/** Form field names per kind. Lists are comma separated in the form. */
export const EMPTY_DRAFT: Record<ActionKind, Draft> = {
  send_email: { to: "", cc: "", subject: "", body_text: "" },
  create_meeting: { title: "", attendees: "", date: "", time: "14:00", duration: "45", location: "", description: "" },
  update_contact: { email: "", first_name: "", last_name: "", company: "", phone: "" },
  enroll: { email: "", program: "", note: "" },
  update_record: { account_id: "", name: "", stage: "", arr: "", owner: "", next_step: "", notes: "" },
};

function list(value: string): string[] {
  return value.split(/[,;\s]+/).map((s) => s.trim()).filter(Boolean);
}

function str(v: unknown): string {
  return typeof v === "string" ? v : "";
}

/** Draft from existing params (edit form and manual create defaults). */
export function draftFromParams(kind: ActionKind, p: Record<string, unknown>): Draft {
  if (kind === "send_email") {
    return {
      to: (p.to as string[] | undefined ?? []).join(", "),
      cc: (p.cc as string[] | undefined ?? []).join(", "),
      subject: str(p.subject),
      body_text: str(p.body_text),
    };
  }
  if (kind === "create_meeting") {
    const parts = isoToLocalParts(str(p.start_iso));
    return {
      title: str(p.title),
      attendees: (p.attendees as string[] | undefined ?? []).join(", "),
      date: parts.date,
      time: parts.time,
      duration: String(p.duration_min ?? 45),
      location: str(p.location),
      description: str(p.description),
    };
  }
  const fields = (p.fields ?? {}) as Record<string, unknown>;
  if (kind === "update_contact") {
    return {
      email: str(p.email),
      first_name: str(fields.first_name),
      last_name: str(fields.last_name),
      company: str(fields.company),
      phone: str(fields.phone),
    };
  }
  if (kind === "update_record") {
    return {
      account_id: str(p.account_id),
      name: str(fields.name),
      stage: str(fields.stage),
      arr: fields.arr == null ? "" : String(fields.arr),
      owner: str(fields.owner),
      next_step: str(fields.next_step),
      notes: str(fields.notes),
    };
  }
  return { email: str(p.email), program: str(p.program), note: str(p.note) };
}

export type ParamsResult = { ok: true; params: Record<string, unknown> } | { ok: false; error: string };

/** Params for the API from a draft. Client checks are light; the server validates everything and says why. */
export function paramsFromDraft(kind: ActionKind, d: Draft, reminders: Reminder[] = DEFAULT_REMINDERS): ParamsResult {
  if (kind === "send_email") {
    const to = list(d.to);
    if (to.length === 0) return { ok: false, error: "Add at least one recipient." };
    if (!d.subject.trim()) return { ok: false, error: "Add a subject." };
    if (!d.body_text.trim()) return { ok: false, error: "Write the message." };
    return { ok: true, params: { to, cc: list(d.cc), subject: d.subject.trim(), body_text: d.body_text } };
  }
  if (kind === "create_meeting") {
    const attendees = list(d.attendees);
    if (!d.title.trim()) return { ok: false, error: "Add a title." };
    if (attendees.length === 0) return { ok: false, error: "Add at least one attendee." };
    if (!d.date || !d.time) return { ok: false, error: "Pick a date and time." };
    const start = localDateTimeToIso(d.date, d.time);
    if (!start) return { ok: false, error: "That date or time is not valid." };
    return {
      ok: true,
      params: {
        title: d.title.trim(),
        attendees,
        start_iso: start,
        duration_min: Number(d.duration) || 45,
        location: d.location.trim() || null,
        description: d.description,
        reminders,
      },
    };
  }
  if (kind === "update_contact") {
    if (!d.email.trim()) return { ok: false, error: "Give the contact's email to find them." };
    const fields: Record<string, string> = {};
    for (const k of ["first_name", "last_name", "company", "phone"] as const) {
      if (d[k].trim()) fields[k] = d[k].trim();
    }
    if (Object.keys(fields).length === 0) return { ok: false, error: "Change at least one field." };
    return { ok: true, params: { email: d.email.trim(), fields } };
  }
  if (kind === "update_record") {
    if (!d.account_id.trim()) return { ok: false, error: "Give the account id to update." };
    const fields: Record<string, unknown> = {};
    for (const k of ["name", "stage", "owner", "next_step", "notes"] as const) {
      if (d[k].trim()) fields[k] = d[k].trim();
    }
    if (d.arr.trim()) {
      const arr = Number(d.arr.replace(/[,$\s]/g, ""));
      if (Number.isNaN(arr) || arr < 0) return { ok: false, error: "ARR must be a number." };
      fields.arr = arr;
    }
    if (Object.keys(fields).length === 0) return { ok: false, error: "Change at least one field." };
    return { ok: true, params: { account_id: d.account_id.trim(), fields } };
  }
  if (!d.email.trim()) return { ok: false, error: "Give the contact's email." };
  if (!d.program) return { ok: false, error: "Pick a program." };
  return { ok: true, params: { email: d.email.trim(), program: d.program, note: d.note.trim() || null } };
}

/** The plain-language line, mirroring actions/schemas.py summarize(). */
export function summaryOf(kind: ActionKind, p: Record<string, unknown>): string {
  if (kind === "send_email") {
    const to = (p.to as string[] | undefined) ?? [];
    return `Email ${firstName(to[0] ?? "")}${to.length > 1 ? ` and ${to.length - 1} more` : ""}: ${str(p.subject)}`;
  }
  if (kind === "create_meeting") {
    const names = ((p.attendees as string[] | undefined) ?? []).slice(0, 3).map(firstName).join(", ");
    return `Meeting “${str(p.title)}” with ${names} (${p.duration_min ?? 45} min)`;
  }
  if (kind === "update_contact") {
    const fields = Object.keys((p.fields as Record<string, unknown>) ?? {}).map((k) => k.replace(/_/g, " "));
    return `Update ${firstName(str(p.email))}: ${fields.join(", ")}`;
  }
  if (kind === "update_record") {
    const fields = Object.keys((p.fields as Record<string, unknown>) ?? {}).map((k) => k.replace(/_/g, " "));
    return `Update account ${str(p.account_id)}: ${fields.join(", ")}`;
  }
  return `Enroll ${firstName(str(p.email))} in ${str(p.program)}`;
}

function firstName(address: string): string {
  const local = address.split("@")[0] ?? "";
  const word = local.split(/[._-]/)[0] || address;
  return word.charAt(0).toUpperCase() + word.slice(1);
}

// ---------------------------------------------------------------- programs and mock sequencing

export const MOCK_PROGRAMS = ["Retirement income review", "Tax-loss harvesting pilot", "Quarterly portfolio check-in"];
const MOCK_KINDS: ActionKind[] = ["send_email", "create_meeting", "update_record", "enroll"];
let mockKindIndex = 0;

/** Kinds the mock proposes in turn, so every card type shows up in a demo run. */
export function nextMockKind(): ActionKind {
  const kind = MOCK_KINDS[mockKindIndex % MOCK_KINDS.length] ?? "send_email";
  mockKindIndex += 1;
  return kind;
}

/** Enrollment programs of a playbook (GET /api/playbooks). Mock mode uses the advisor programs. */
export function usePrograms(playbook: string): string[] {
  const [programs, setPrograms] = useState<string[]>(MOCK ? MOCK_PROGRAMS : []);
  useEffect(() => {
    if (MOCK || !playbook) return;
    let alive = true;
    fetch("/api/playbooks")
      .then((r) => (r.ok ? (r.json() as Promise<{ playbooks?: { name: string; programs?: string[] }[] }>) : null))
      .then((data) => {
        const hit = data?.playbooks?.find((p) => p.name === playbook);
        if (alive && hit?.programs) setPrograms(hit.programs);
      })
      .catch(() => {
        /* the enroll form keeps its empty list; the server still refuses unknown programs */
      });
    return () => {
      alive = false;
    };
  }, [playbook]);
  return programs;
}

// ---------------------------------------------------------------- REST

async function failure(res: Response): Promise<Error> {
  let detail = "";
  try {
    detail = ((await res.json()) as { detail?: string }).detail ?? "";
  } catch {
    /* not JSON */
  }
  return new Error(detail || `request failed (${res.status})`);
}

export function messageUrl(actionId: string): string {
  return `/api/actions/${encodeURIComponent(actionId)}/message.eml`;
}

export function inviteUrl(actionId: string): string {
  return `/api/actions/${encodeURIComponent(actionId)}/invite.ics`;
}

export async function createAction(
  scope: { session_id?: string; call_id?: string },
  kind: ActionKind,
  params: Record<string, unknown>,
): Promise<ActionView> {
  const res = await fetch("/api/actions", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ ...scope, kind, params }),
  });
  if (!res.ok) throw await failure(res);
  return (await res.json()) as ActionView;
}

export async function decideAction(actionId: string, decision: Decision, params?: Record<string, unknown>): Promise<ActionView> {
  const path = decision === "undo" ? "undo" : decision === "approve" ? "approve" : "reject";
  const res = await fetch(`/api/actions/${encodeURIComponent(actionId)}/${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(params ? { params } : {}),
  });
  if (!res.ok) throw await failure(res);
  return (await res.json()) as ActionView;
}

/** Preview text for an action, from the server (the same bytes that are sent or downloaded). */
export async function fetchPreview(a: ActionView): Promise<string> {
  const res = await fetch(messageUrl(a.action_id));
  if (!res.ok) throw await failure(res);
  return res.text();
}

// ---------------------------------------------------------------- mock (?mock=1)

let mockSeq = 0;

/** A proposal the mock backend would make from a conversation turn. */
export function mockProposal(kind: ActionKind, scope: { session_id?: string; call_id?: string }, programs: string[]): ActionView {
  const now = Date.now();
  const tomorrow = new Date(now + 86_400_000);
  const pad = (n: number) => String(n).padStart(2, "0");
  const date = `${tomorrow.getFullYear()}-${pad(tomorrow.getMonth() + 1)}-${pad(tomorrow.getDate())}`;
  const params: Record<string, unknown> =
    kind === "send_email"
      ? { to: ["dana.lee@example.com"], cc: [], subject: "Your fee breakdown", body_text: "Hi Dana, here is the fee breakdown we discussed. Reply with any questions." }
      : kind === "create_meeting"
        ? {
            title: "Retirement income review",
            attendees: ["dana.lee@example.com"],
            start_iso: localDateTimeToIso(date, "14:00"),
            duration_min: 45,
            location: "Zoom",
            description: "Bring the latest statements.",
            reminders: DEFAULT_REMINDERS,
          }
        : kind === "enroll"
          ? { email: "dana.lee@example.com", program: programs[0] ?? "Quarterly portfolio check-in", note: null }
          : kind === "update_record"
            ? { account_id: "acct-northwind", fields: { stage: "Proposal", next_step: "Send fee breakdown" } }
            : { email: "dana.lee@example.com", fields: { phone: "+1 555 010 0101" } };
  const id = `mock-action-${++mockSeq}-${now.toString(36)}`;
  return {
    action_id: id,
    session_id: scope.session_id ?? null,
    call_id: scope.call_id ?? null,
    owner: "mock",
    kind,
    params,
    status: "proposed",
    summary: summaryOf(kind, params),
    proposed_by: "model",
    created_at_ms: now,
    updated_at_ms: now,
    decided_at_ms: null,
    error: null,
    result: null,
    safety: { lock: true, safe_recipient: "patrick.bayne@databricks.com", mode: "outbox" },
  };
}

/** The mock's preview text: a readable stand-in for the .eml or .ics the server renders. */
export function mockPreview(a: ActionView): string {
  if (a.kind === "create_meeting") {
    return ["BEGIN:VCALENDAR", "VERSION:2.0", "METHOD:REQUEST", "BEGIN:VEVENT", `SUMMARY:${a.summary}`,
      `DTSTART:${String(a.params.start_iso)}`, "BEGIN:VALARM", "TRIGGER:-P1D", "END:VALARM",
      "BEGIN:VALARM", "TRIGGER:-PT10M", "END:VALARM", "END:VEVENT", "END:VCALENDAR"].join("\n");
  }
  const p = a.params as { to?: string[]; subject?: string; body_text?: string };
  return [`To: ${(p.to ?? []).join(", ")}`, `Subject: ${p.subject ?? ""}`, "", p.body_text ?? ""].join("\n");
}

/**
 * Mock execution: approve goes executing -> outboxed (or done), undo goes to undone. `onUpdate` receives each step,
 * as the socket would. Nothing is sent.
 */
export function mockDecide(a: ActionView, decision: Decision, onUpdate: (next: ActionView) => void): void {
  const at = Date.now();
  if (decision === "reject") {
    onUpdate({ ...a, status: "rejected", updated_at_ms: at, decided_at_ms: at });
    return;
  }
  if (decision === "undo") {
    onUpdate({ ...a, status: "undone", updated_at_ms: at });
    return;
  }
  const finalStatus: ActionStatus = a.kind === "enroll" || a.kind === "update_contact" || a.kind === "update_record" ? "done" : "outboxed";
  onUpdate({ ...a, status: "executing", updated_at_ms: at, decided_at_ms: at, error: null });
  window.setTimeout(() => {
    onUpdate({
      ...a,
      status: finalStatus,
      updated_at_ms: Date.now(),
      decided_at_ms: at,
      error: null,
      result: isRendered(a)
        ? { mode: "outbox", has_eml: true, has_ics: a.kind === "create_meeting", safety: { lock: true, original_to: ["dana.lee@example.com"] } }
        : { mode: "outbox" },
    });
  }, 900);
}
