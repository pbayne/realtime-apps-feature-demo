import type {
  Mode,
  PlaybookApi,
  PlaybookSummary,
  PracticeScenario,
  VoiceItem,
  VoiceOption,
  VoiceRole,
  VoiceTiming,
} from "./types";

// Used when the backend is unreachable (and in ?mock=1 mode). Keep ids aligned with
// src/app/playbooks/*.yaml and src/app/data/replay/<playbook>__<script>.jsonl.
export const FALLBACK_PLAYBOOKS: PlaybookSummary[] = [
  {
    name: "advisor_coach",
    label: "Wealth advisor discovery coach",
    persona: "Wealth advisor discovery",
    scenarios: [
      { name: "fee_objection", title: "Fee objection", persona: "Price-sensitive prospect with a 1.8M household" },
      { name: "market_timing", title: "Market timing worry", persona: "Sold part of the portfolio in a downturn" },
    ],
  },
  {
    name: "contact_center",
    label: "Contact center",
    persona: "Claims & billing service",
    scenarios: [
      { name: "billing_dispute", title: "Billing dispute", persona: "Angry about a duplicate charge" },
      { name: "cancellation_save", title: "Cancellation save", persona: "Wants to cancel after a price rise" },
    ],
  },
];

export const FALLBACK_SCRIPTS: Record<string, string[]> = {
  advisor_coach: ["fee_objection", "market_timing"],
  contact_center: ["billing_dispute", "cancellation_save"],
};

/** Used when GET /api/voices fails. */
export const FALLBACK_VOICES: VoiceOption[] = [{ id: "af_heart", label: "af heart" }];

/** Richer list for ?mock=1 (the simulated backend has all four Kokoro voices). */
export const MOCK_VOICES: VoiceOption[] = [
  { id: "af_heart", label: "Heart (US, female)" },
  { id: "am_michael", label: "Michael (US, male)" },
  { id: "bf_emma", label: "Emma (UK, female)" },
  { id: "bm_george", label: "George (UK, male)" },
];

const VOICE_LABELS: Record<string, string> = Object.fromEntries(MOCK_VOICES.map((v) => [v.id, v.label]));

function str(v: unknown): string | undefined {
  return typeof v === "string" && v ? v : undefined;
}

function normalizeScenarios(raw: unknown): PracticeScenario[] {
  if (!Array.isArray(raw)) return [];
  const out: PracticeScenario[] = [];
  for (const s of raw) {
    if (typeof s === "string") {
      out.push({ name: s, title: s.replace(/_/g, " ") });
      continue;
    }
    if (!s || typeof s !== "object") continue;
    const o = s as Record<string, unknown>;
    const name = str(o.name) ?? str(o.id);
    if (!name) continue;
    out.push({
      name,
      title: str(o.title) ?? str(o.label) ?? name.replace(/_/g, " "),
      persona: str(o.persona),
      voice: str(o.voice),
    });
  }
  return out;
}

/**
 * Map GET /api/playbooks to UI summaries. The backend returns {"playbooks": [{name, title, persona, scenarios, ...}]};
 * a bare array with `label` is accepted as well. title -> label. Missing `scenarios` means none.
 */
export function normalizePlaybooks(data: unknown): PlaybookSummary[] {
  let list: unknown[] = [];
  if (Array.isArray(data)) list = data;
  else if (data && typeof data === "object" && Array.isArray((data as { playbooks?: unknown }).playbooks)) {
    list = (data as { playbooks: unknown[] }).playbooks;
  }
  const out: PlaybookSummary[] = [];
  for (const raw of list) {
    if (!raw || typeof raw !== "object" || typeof (raw as PlaybookApi).name !== "string") continue;
    const p = raw as PlaybookApi;
    out.push({
      name: p.name,
      label: p.label || p.title || p.name,
      persona: p.persona ?? "",
      scenarios: normalizeScenarios(p.scenarios),
    });
  }
  return out;
}

/**
 * Map GET /api/scripts responses. With ?playbook=<name> the backend returns a JSON array of names.
 * The mapping form {<playbook>: [...]} and {"scripts": [...]} are accepted too.
 */
export function normalizeScripts(data: unknown, playbook: string): string[] {
  if (Array.isArray(data)) return data.filter((s): s is string => typeof s === "string");
  if (data && typeof data === "object") {
    const obj = data as Record<string, unknown>;
    const pick = obj[playbook] ?? obj.scripts;
    if (Array.isArray(pick)) return pick.filter((s): s is string => typeof s === "string");
  }
  return [];
}

/**
 * Map GET /api/voices. Accepts a bare array or {"voices": [...]}; entries may be strings or
 * objects with id|name|voice and an optional label. The SPEC does not fix the shape yet.
 */
export function normalizeVoices(data: unknown): VoiceOption[] {
  const list: unknown[] = Array.isArray(data)
    ? data
    : data && typeof data === "object" && Array.isArray((data as { voices?: unknown }).voices)
      ? (data as { voices: unknown[] }).voices
      : [];
  const out: VoiceOption[] = [];
  for (const v of list) {
    if (typeof v === "string" && v) {
      out.push({ id: v, label: VOICE_LABELS[v] ?? v });
      continue;
    }
    if (!v || typeof v !== "object") continue;
    const o = v as Record<string, unknown>;
    const id = str(o.id) ?? str(o.name) ?? str(o.voice);
    if (!id) continue;
    out.push({ id, label: str(o.label) ?? str(o.title) ?? VOICE_LABELS[id] ?? id });
  }
  return out;
}

export async function getPlaybooks(): Promise<PlaybookSummary[]> {
  const res = await fetch("/api/playbooks");
  if (!res.ok) throw new Error(`GET /api/playbooks failed: ${res.status}`);
  return normalizePlaybooks(await res.json());
}

/** Voice list for the picker. Any failure falls back to the default voice only. */
export async function getVoices(): Promise<VoiceOption[]> {
  try {
    const res = await fetch("/api/voices");
    if (!res.ok) return FALLBACK_VOICES;
    const list = normalizeVoices(await res.json());
    return list.length ? list : FALLBACK_VOICES;
  } catch {
    return FALLBACK_VOICES;
  }
}

/** Scripts for one playbook (GET /api/scripts?playbook=<name>). Falls back to the built-in list offline. */
export async function getScripts(playbook: string): Promise<string[]> {
  try {
    const res = await fetch(`/api/scripts?playbook=${encodeURIComponent(playbook)}`);
    if (res.status === 404) return [];
    if (!res.ok) return FALLBACK_SCRIPTS[playbook] ?? [];
    return normalizeScripts(await res.json(), playbook);
  } catch {
    return FALLBACK_SCRIPTS[playbook] ?? [];
  }
}

export async function createCall(opts: {
  playbook: string;
  mode: Mode;
  script?: string;
}): Promise<{ call_id: string }> {
  const body: Record<string, string> = { playbook: opts.playbook, mode: opts.mode };
  if (opts.mode === "replay" && opts.script) body.script = opts.script;
  const res = await fetch("/api/calls", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    let detail = "";
    try {
      detail = ((await res.json()) as { detail?: string }).detail ?? "";
    } catch {
      /* not JSON */
    }
    throw new Error(`POST /api/calls failed: ${res.status}${detail ? ` (${detail})` : ""}`);
  }
  return (await res.json()) as { call_id: string };
}

export async function endCall(callId: string): Promise<void> {
  const res = await fetch(`/api/calls/${encodeURIComponent(callId)}/end`, { method: "POST" });
  if (!res.ok) throw new Error(`POST end failed: ${res.status}`);
}

/** SPEC §9.1 POST /api/voice/sessions. Practice always returns a call_id; assistant may return null. */
export async function createVoiceSession(opts: {
  role: VoiceRole;
  playbook: string;
  scenario?: string;
  callId?: string;
  voice?: string;
}): Promise<{ session_id: string; call_id: string | null }> {
  const body: Record<string, string> = { role: opts.role, playbook: opts.playbook };
  if (opts.scenario) body.scenario = opts.scenario;
  if (opts.callId) body.call_id = opts.callId;
  if (opts.voice) body.voice = opts.voice;
  const res = await fetch("/api/voice/sessions", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    let detail = "";
    try {
      detail = ((await res.json()) as { detail?: string }).detail ?? "";
    } catch {
      /* not JSON */
    }
    throw new Error(`POST /api/voice/sessions failed: ${res.status}${detail ? ` (${detail})` : ""}`);
  }
  const data = (await res.json()) as { session_id?: unknown; call_id?: unknown };
  if (typeof data.session_id !== "string") throw new Error("POST /api/voice/sessions returned no session_id");
  return { session_id: data.session_id, call_id: str(data.call_id) ?? null };
}

/** A turn as the snapshot reports it. Used to rebuild the conversation after a socket reconnect. */
export interface VoiceSnapshotTurn {
  turn: number;
  user_text: string;
  agent_text: string;
  interrupted: boolean;
  timing?: VoiceTiming;
}

export interface VoiceSnapshot {
  turns: VoiceSnapshotTurn[];
  items: VoiceItem[];
}

/** Tolerant reader for GET /api/voice/sessions/{id} (shape not fixed by SPEC §9.1 yet). */
export function normalizeSnapshot(data: unknown): VoiceSnapshot {
  const o = (data && typeof data === "object" ? data : {}) as Record<string, unknown>;
  const turns: VoiceSnapshotTurn[] = [];
  if (Array.isArray(o.turns)) {
    for (const t of o.turns) {
      if (!t || typeof t !== "object") continue;
      const r = t as Record<string, unknown>;
      if (typeof r.turn !== "number") continue;
      turns.push({
        turn: r.turn,
        user_text: str(r.user_text) ?? "",
        agent_text: str(r.agent_text) ?? "",
        interrupted: r.interrupted === true,
        timing: (r.timing as VoiceTiming | undefined) ?? undefined,
      });
    }
  }
  const items = Array.isArray(o.items)
    ? (o.items.filter((i) => i && typeof i === "object" && typeof (i as VoiceItem).item_id === "string") as VoiceItem[])
    : [];
  return { turns, items };
}

export async function getVoiceSnapshot(sessionId: string): Promise<VoiceSnapshot | null> {
  try {
    const res = await fetch(`/api/voice/sessions/${encodeURIComponent(sessionId)}`);
    if (!res.ok) return null;
    return normalizeSnapshot(await res.json());
  } catch {
    return null;
  }
}
