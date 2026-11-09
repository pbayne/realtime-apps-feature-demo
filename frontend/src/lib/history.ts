// Past Calls history (request E): the caller's own ended/active calls and one call's detail
// (transcript, cards, actions, and the post-call summary when the insights warehouse is configured).
import type { ActionView } from "./actions";

export interface CallSummaryRow {
  call_id: string;
  playbook: string;
  mode: string;
  status: string;
  started_at_ms: number | null;
  ended_at_ms: number | null;
  customer_id: string | null;
  utterance_count?: number;
  card_count?: number;
}

export interface TranscriptLine {
  call_id: string;
  seq: number;
  speaker: string;
  text: string;
  emitted_at_ms?: number;
}

export interface HistoryCard {
  card_id: string;
  kind: string;
  title: string;
  body: string;
  status: string;
}

export interface CallDetail {
  call: CallSummaryRow;
  transcript: TranscriptLine[];
  cards: HistoryCard[];
  actions: ActionView[];
  summary: { summary?: string; overall_score?: number | null } | null;
}

async function failure(res: Response): Promise<Error> {
  let detail = "";
  try {
    detail = ((await res.json()) as { detail?: string }).detail ?? "";
  } catch {
    /* not JSON */
  }
  return new Error(detail || `request failed (${res.status})`);
}

export async function listCalls(limit = 50): Promise<CallSummaryRow[]> {
  const res = await fetch(`/api/history/calls?limit=${limit}`);
  if (!res.ok) throw await failure(res);
  return ((await res.json()) as { calls: CallSummaryRow[] }).calls;
}

export async function getCallDetail(callId: string): Promise<CallDetail> {
  const res = await fetch(`/api/history/calls/${encodeURIComponent(callId)}`);
  if (!res.ok) throw await failure(res);
  return (await res.json()) as CallDetail;
}
