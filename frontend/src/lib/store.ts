// Call console state. Server events are folded in via `applyServerEvent`.
import type { Card, CoverageItem, Mode, ServerEvent, Speaker } from "./types";

import { upsertAction, type ActionView } from "./actions";

export type CallStatus = "idle" | "connecting" | "live" | "reconnecting" | "ended" | "error";
export type Resolution = "accepted" | "edited" | "rejected";

export interface UtteranceView {
  key: string;
  seq: number;
  speaker: Speaker;
  text: string;
  final: boolean;
  latencyMs?: number;
}

export interface Enrichment {
  intent?: string | null;
  sentiment?: number | null;
  risk_flags: string[];
  objection?: string | null;
}

export interface CardView {
  card: Omit<Card, "body">;
  body: string;
  streaming: boolean;
  resolution?: Resolution;
  editedText?: string;
}

export interface MetricSample {
  at: number;
  stt_first_word_ms?: number | null;
  stt_commit_ms?: number | null;
  rtm_ms?: number | null;
  card_ms?: number | null;
  card_done_ms?: number | null;
}

export interface CallState {
  status: CallStatus;
  error?: string;
  callId: string | null;
  playbook: string;
  mode: Mode;
  script?: string;
  startedAt: number | null;
  endedAt: number | null;
  utterances: UtteranceView[];
  enrich: Record<number, Enrichment>;
  cards: Record<string, CardView>;
  cardOrder: string[]; // chronological
  coverage: CoverageItem[];
  metrics: MetricSample[];
  actions: ActionView[];       // SPEC 12: proposed and executed actions for this call
}

export const initialCallState: CallState = {
  status: "idle",
  callId: null,
  playbook: "",
  mode: "replay",
  startedAt: null,
  endedAt: null,
  utterances: [],
  enrich: {},
  cards: {},
  cardOrder: [],
  coverage: [],
  metrics: [],
  actions: [],
};

export type Action =
  | { type: "begin"; callId: string | null; playbook: string; mode: Mode; script?: string; at: number }
  | { type: "status"; status: CallStatus }
  | { type: "event"; ev: ServerEvent; at: number }
  | { type: "resolve"; cardId: string; resolution: Resolution; text?: string }
  | { type: "ended"; at: number; message?: string }
  | { type: "error"; message: string }
  | { type: "reset" };

function applyUtterance(
  list: UtteranceView[],
  speaker: Speaker,
  seq: number,
  text: string,
  final: boolean,
  latencyMs?: number,
): UtteranceView[] {
  const item: UtteranceView = {
    key: `${final ? "f" : "p"}-${speaker}-${seq}`,
    seq,
    speaker,
    text,
    final,
    latencyMs,
  };
  if (final) {
    // Finals stay in order; the speaker's partial and the other channel's
    // in-flight partials sit at the bottom until they resolve.
    const finals = list.filter((u) => u.final && u.seq !== seq);
    const others = list.filter((u) => !u.final && u.speaker !== speaker);
    return [...finals, item, ...others];
  }
  const finals = list.filter((u) => u.final);
  const others = list.filter((u) => !u.final && u.speaker !== speaker);
  return [...finals, ...others, item];
}

export function applyServerEvent(state: CallState, ev: ServerEvent, at: number): CallState {
  switch (ev.type) {
    case "action":
      return { ...state, actions: upsertAction(state.actions, ev.action) };
    case "transcript.partial":
      return { ...state, utterances: applyUtterance(state.utterances, ev.speaker, ev.seq, ev.text, false) };

    case "transcript.final":
      return {
        ...state,
        utterances: applyUtterance(state.utterances, ev.speaker, ev.seq, ev.text, true, ev.latency_ms),
      };

    case "enrichment": {
      const prev = state.enrich[ev.seq];
      const next: Enrichment = {
        intent: ev.intent ?? prev?.intent ?? null,
        sentiment: ev.sentiment ?? prev?.sentiment ?? null,
        risk_flags: ev.risk_flags ?? prev?.risk_flags ?? [],
        objection: ev.objection ?? prev?.objection ?? null,
      };
      return { ...state, enrich: { ...state.enrich, [ev.seq]: next } };
    }

    case "card.start": {
      const { body: _ignored, ...card } = ev.card;
      void _ignored;
      if (state.cards[card.card_id]) return state;
      return {
        ...state,
        cards: { ...state.cards, [card.card_id]: { card, body: "", streaming: true } },
        cardOrder: [...state.cardOrder, card.card_id],
      };
    }

    case "card.delta": {
      const cur = state.cards[ev.card_id];
      if (!cur) return state;
      return {
        ...state,
        cards: { ...state.cards, [ev.card_id]: { ...cur, body: cur.body + ev.delta } },
      };
    }

    case "card.done": {
      const { body, ...card } = ev.card;
      const cur = state.cards[card.card_id];
      const merged: CardView = {
        card,
        body: body ?? cur?.body ?? "",
        streaming: false,
        resolution: cur?.resolution,
        editedText: cur?.editedText,
      };
      return {
        ...state,
        cards: { ...state.cards, [card.card_id]: merged },
        cardOrder: state.cardOrder.includes(card.card_id)
          ? state.cardOrder
          : [...state.cardOrder, card.card_id],
      };
    }

    case "coverage":
      return { ...state, coverage: ev.items };

    case "metrics":
      return {
        ...state,
        metrics: [
          ...state.metrics.slice(-199),
          {
            at,
            stt_first_word_ms: ev.stt_first_word_ms,
            stt_commit_ms: ev.stt_commit_ms,
            rtm_ms: ev.rtm_ms,
            card_ms: ev.card_ms,
            card_done_ms: ev.card_done_ms,
          },
        ],
      };

    case "call.ended":
      return { ...state, status: "ended", endedAt: state.endedAt ?? at };

    default:
      return state;
  }
}

export function reducer(state: CallState, action: Action): CallState {
  switch (action.type) {
    case "begin":
      return {
        ...initialCallState,
        status: "connecting",
        callId: action.callId,
        playbook: action.playbook,
        mode: action.mode,
        script: action.script,
        startedAt: action.at,
      };
    case "status":
      if (state.status === "ended" || state.status === "idle") return state;
      return { ...state, status: action.status };
    case "event":
      return applyServerEvent(state, action.ev, action.at);
    case "resolve": {
      const cur = state.cards[action.cardId];
      if (!cur) return state;
      return {
        ...state,
        cards: {
          ...state.cards,
          [action.cardId]: {
            ...cur,
            resolution: action.resolution,
            editedText: action.text ?? cur.editedText,
          },
        },
      };
    }
    case "ended":
      // Idempotent: a socket that gives up after call.ended must not replace the normal end with a notice.
      if (state.status === "ended") return state;
      return { ...state, status: "ended", endedAt: state.endedAt ?? action.at, error: action.message ?? state.error };
    case "error":
      return { ...state, status: "error", error: action.message };
    case "reset":
      return initialCallState;
    default:
      return state;
  }
}

// ---- Derived helpers -------------------------------------------------------

export function sentimentSeries(state: CallState): { seq: number; value: number }[] {
  return Object.entries(state.enrich)
    .map(([seq, e]) => ({ seq: Number(seq), value: e.sentiment }))
    .filter((p): p is { seq: number; value: number } => typeof p.value === "number")
    .sort((a, b) => a.seq - b.seq);
}

export function riskFlagCounts(state: CallState): { flag: string; count: number }[] {
  const counts = new Map<string, number>();
  for (const e of Object.values(state.enrich)) {
    for (const f of e.risk_flags) counts.set(f, (counts.get(f) ?? 0) + 1);
  }
  return [...counts.entries()]
    .map(([flag, count]) => ({ flag, count }))
    .sort((a, b) => b.count - a.count);
}

export function avg(xs: (number | null | undefined)[]): number | null {
  const v = xs.filter((x): x is number => typeof x === "number");
  return v.length ? v.reduce((a, b) => a + b, 0) / v.length : null;
}

export interface CallSummary {
  durationMs: number;
  cardsShown: number;
  accepted: number;
  edited: number;
  rejected: number;
  coveragePct: number;
  coveredCount: number;
  coverageTotal: number;
  avg: {
    stt_first_word_ms: number | null;
    stt_commit_ms: number | null;
    rtm_ms: number | null;
    card_ms: number | null;
    card_done_ms: number | null;
  };
}

export function summarize(state: CallState): CallSummary {
  const cards = state.cardOrder.map((id) => state.cards[id]).filter(Boolean);
  const covered = state.coverage.filter((c) => c.covered).length;
  const total = state.coverage.length;
  const end = state.endedAt ?? Date.now();
  return {
    durationMs: state.startedAt ? end - state.startedAt : 0,
    cardsShown: cards.length,
    accepted: cards.filter((c) => c.resolution === "accepted").length,
    edited: cards.filter((c) => c.resolution === "edited").length,
    rejected: cards.filter((c) => c.resolution === "rejected").length,
    coveredCount: covered,
    coverageTotal: total,
    coveragePct: total ? Math.round((covered / total) * 100) : 0,
    avg: {
      stt_first_word_ms: avg(state.metrics.map((m) => m.stt_first_word_ms)),
      stt_commit_ms: avg(state.metrics.map((m) => m.stt_commit_ms)),
      rtm_ms: avg(state.metrics.map((m) => m.rtm_ms)),
      card_ms: avg(state.metrics.map((m) => m.card_ms)),
      card_done_ms: avg(state.metrics.map((m) => m.card_done_ms)),
    },
  };
}
