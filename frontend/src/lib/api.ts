import type { Mode, PlaybookApi, PlaybookSummary } from "./types";

// Used when the backend is unreachable (and in ?mock=1 mode). Keep ids aligned with
// src/app/playbooks/*.yaml and src/app/data/replay/<playbook>__<script>.jsonl.
export const FALLBACK_PLAYBOOKS: PlaybookSummary[] = [
  { name: "advisor_coach", label: "Wealth advisor discovery coach", persona: "Wealth advisor discovery" },
  { name: "contact_center", label: "Contact center", persona: "Claims & billing service" },
];

export const FALLBACK_SCRIPTS: Record<string, string[]> = {
  advisor_coach: ["fee_objection", "market_timing"],
  contact_center: ["billing_dispute", "cancellation_save"],
};

/**
 * Map GET /api/playbooks to UI summaries. The backend returns {"playbooks": [{name, title, persona, ...}]};
 * a bare array with `label` is accepted as well. title -> label.
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
    out.push({ name: p.name, label: p.label || p.title || p.name, persona: p.persona ?? "" });
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

export async function getPlaybooks(): Promise<PlaybookSummary[]> {
  const res = await fetch("/api/playbooks");
  if (!res.ok) throw new Error(`GET /api/playbooks failed: ${res.status}`);
  return normalizePlaybooks(await res.json());
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
