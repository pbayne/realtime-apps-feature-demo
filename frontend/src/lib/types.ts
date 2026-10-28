// Wire contracts from docs/SPEC.md §3 and §3.1 (keep in sync with src/app/main.py and session.py).

export type Speaker = "agent" | "customer";

export type CardKind =
  | "next_question"
  | "suggested_response"
  | "objection_pickup"
  | "missing_info"
  | "compliance"
  | "example"
  | "next_best_action";

export type CardStatus = "shown" | "accepted" | "edited" | "rejected" | "expired";

export interface Card {
  card_id: string;
  call_id: string;
  seq: number;
  kind: CardKind;
  title: string;
  body?: string;
  confidence: number;
  source_doc: string | null;
  source_clause: string | null;
  trigger: string | null;
  status: CardStatus;
  created_at_ms: number;
}

export interface CoverageItem {
  key: string;
  label: string;
  covered: boolean;
}

// Server -> client
export type ServerEvent =
  | { type: "transcript.partial"; speaker: Speaker; text: string; seq: number }
  | { type: "transcript.final"; speaker: Speaker; text: string; seq: number; latency_ms?: number }
  | {
      type: "enrichment";
      seq: number;
      speaker?: Speaker;
      // "rtm" | "local_fallback" for the enrichment row; "perception" for the late/in-time model refinement.
      source?: string;
      intent?: string | null;
      sentiment?: number | null;
      risk_flags?: string[];
      objection?: string | null;
    }
  | { type: "card.start"; card: Omit<Card, "body"> & { body?: undefined } }
  | { type: "card.delta"; card_id: string; delta: string }
  | { type: "card.done"; card: Card }
  | { type: "coverage"; items: CoverageItem[] }
  | {
      type: "metrics";
      seq?: number;
      stt_first_word_ms?: number | null;
      stt_commit_ms?: number | null;
      rtm_ms?: number | null;
      card_ms?: number | null;
      card_done_ms?: number | null;
      card_path?: "in_time" | "late" | "compliance" | null;
      compose_mode?: "llm" | "template" | "verbatim" | null;
      enrichment_source?: string;
    }
  | { type: "error"; message?: string; scope?: string; speaker?: Speaker }
  | { type: "call.ended" };

// Client -> server
export type ClientMessage =
  | { type: "feedback"; card_id: string; action: "accept" | "reject" | "edit"; text?: string }
  // SPEC addition (requested for backend): browser Web Speech API fallback results.
  | { type: "browser_stt"; speaker: Speaker; text: string; is_final: boolean };

/** One playbook as the UI uses it. Built by normalizePlaybooks() in lib/api.ts. */
export type PlaybookSummary = { name: string; label: string; persona: string };

/** GET /api/playbooks returns {"playbooks": [{name, title, persona, card_weights, methodology, ...}]}.
 *  `label` and a bare array are accepted too, so an older backend still works. */
export interface PlaybookApi {
  name: string;
  title?: string;
  label?: string;
  persona?: string;
}

export type Mode = "replay" | "mic";

export interface StartOptions {
  playbook: string;
  mode: Mode;
  script?: string;
}
