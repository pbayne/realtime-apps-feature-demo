// Wire contracts from docs/SPEC.md §3 and §3.1 (keep in sync with src/app/main.py and session.py).
import type { ActionView } from "./actions";

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
  | { type: "call.ended" }
  | { type: "action"; action: ActionView };

// Client -> server
export type ClientMessage =
  | { type: "feedback"; card_id: string; action: "accept" | "reject" | "edit"; text?: string }
  // SPEC addition (requested for backend): browser Web Speech API fallback results.
  | { type: "browser_stt"; speaker: Speaker; text: string; is_final: boolean };

/** A practice scenario (SPEC §9.1: playbook YAML `practice:`). `persona` and `voice` are optional. */
export interface PracticeScenario {
  name: string;
  title: string;
  persona?: string;
  voice?: string;
}

/** One playbook as the UI uses it. Built by normalizePlaybooks() in lib/api.ts. */
export interface PlaybookSummary {
  name: string;
  label: string;
  persona: string;
  scenarios: PracticeScenario[];
}

/** GET /api/playbooks returns {"playbooks": [{name, title, persona, card_weights, methodology, scenarios, ...}]}.
 *  `label` and a bare array are accepted too, so an older backend still works. */
export interface PlaybookApi {
  name: string;
  title?: string;
  label?: string;
  persona?: string;
  scenarios?: unknown;
}

export type Mode = "replay" | "mic";

export interface StartOptions {
  playbook: string;
  mode: Mode;
  script?: string;
}

// ---- Voice mode (SPEC §9, v0.3) ----------------------------------------------------------------

export type VoiceRole = "assistant" | "practice";

export type OutcomeKind =
  | "action_item"
  | "follow_up_question"
  | "follow_up_response"
  | "next_step"
  | "summary"
  | "risk"
  | "note";

/** SPEC §9.3 Item: structured outcome of one turn. */
export interface VoiceItem {
  item_id: string;
  session_id?: string;
  turn: number;
  kind: OutcomeKind;
  text: string;
  owner?: string | null;
  due?: string | null;
  source_doc?: string | null;
  created_at_ms: number;
}

/** SPEC §9.2 turn_done.timing. Every field is optional so partial timings render. */
export interface VoiceTiming {
  stt_ms?: number | null;
  llm_first_token_ms?: number | null;
  tts_first_audio_ms?: number | null;
  first_audio_ms?: number | null;
  total_ms?: number | null;
}

/** A provisional suggestion chip (SPEC 9.7): an item that is not yet pinned. */
export type VoiceSuggestion = VoiceItem & { provisional: true };

/** Live analysis of the latest user (or rep) speech, from the `live` event (SPEC 9.7). */
export interface LiveEvent {
  type: "live";
  turn: number;
  speaker: string;
  kind: "partial" | "final";
  sentiment: number;
  intent: string;
  signals: string[];
  coverage: CoverageItem[] | null;
}

/** Server -> client voice events on /ws/voice/{session_id} (SPEC §9.2, streaming and live events §9.2a, §9.7). */
export type VoiceServerEvent =
  | { type: "user_text"; turn: number; text: string; stt_ms?: number | null }
  | { type: "user_partial"; turn: number; text: string; seq?: number; first_word_ms?: number | null }
  | { type: "agent_text"; turn: number; delta: string }
  | { type: "audio"; turn: number; seq: number; rate: number; pcm_b64: string }
  | { type: "item"; turn: number; item: VoiceItem }
  | { type: "suggestion"; turn: number; item: VoiceSuggestion }
  | { type: "suggestion_retire"; turn: number; item_ids: string[]; reason: string }
  | LiveEvent
  | { type: "turn_done"; turn: number; interrupted: boolean; timing?: VoiceTiming }
  | { type: "status"; turn?: number; stage: "stt" | "tts"; state: "warming" | "ready" | "failed"; message?: string; elapsed_ms?: number }
  | { type: "stream_started"; sample_rate: number; endpoint_silence_ms: number; frame_ms: number }
  | { type: "stream_stopped"; flush: boolean; dropped_frames: number }
  | { type: "session_ended"; reason: string }
  | { type: "action"; action: ActionView }
  | { type: "error"; turn: number; stage: "stt" | "llm" | "tts"; message: string };

/** Client -> server on /ws/voice/{session_id}. Binary frames are one WAV turn (RIFF header) or streamed PCM16
 *  frames while a stream is open. `feedback` is a SPEC gap: card resolutions in practice mode share this socket. */
export type VoiceClientMessage =
  | { type: "interrupt" }
  | { type: "text"; text: string }
  | { type: "config"; voice?: string; role?: VoiceRole }
  | { type: "stream_start"; sample_rate: 16000; endpoint_silence_ms?: number }
  | { type: "stream_stop"; flush: boolean }
  | { type: "pin"; item_id: string }
  | { type: "dismiss"; item_id: string }
  | { type: "feedback"; card_id: string; action: "accept" | "reject" | "edit"; text?: string };

/** Anything that can arrive on the voice socket: voice turn events plus the call events of practice mode. */
export type AnyServerEvent = ServerEvent | VoiceServerEvent;

export interface VoiceOption {
  id: string;
  label: string;
}

export interface VoiceStartOptions {
  role: VoiceRole;
  playbook: string;
  scenario?: string;
  /** Assistant only: link an existing call for context. */
  callId?: string;
  voice: string;
}
