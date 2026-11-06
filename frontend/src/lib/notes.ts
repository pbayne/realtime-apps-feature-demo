// Freeform notes (Granola-style) for a live call or voice session: GET/PUT /api/notes.
// One note per scope; the scope id (call_id or session_id) keys it on the server. Owner-checked there.
export interface Note {
  text: string;
  updated_at_ms: number | null;
  call_id?: string | null;
  session_id?: string | null;
  degraded?: boolean;
}

export type NoteScope = { call_id: string } | { session_id: string };

function scopeQuery(scope: NoteScope): string {
  const p = new URLSearchParams(scope as Record<string, string>);
  return p.toString();
}

async function json<T>(path: string, init?: RequestInit): Promise<T> {
  const r = await fetch(path, { ...init, headers: { "content-type": "application/json", ...(init?.headers ?? {}) } });
  if (!r.ok) {
    const body = await r.json().catch(() => null);
    throw new Error(typeof body?.detail === "string" ? body.detail : `HTTP ${r.status}`);
  }
  return (await r.json()) as T;
}

export function getNote(scope: NoteScope): Promise<Note> {
  return json<Note>(`/api/notes?${scopeQuery(scope)}`);
}

export function saveNote(scope: NoteScope, text: string): Promise<Note> {
  return json<Note>(`/api/notes?${scopeQuery(scope)}`, { method: "PUT", body: JSON.stringify({ text }) });
}
