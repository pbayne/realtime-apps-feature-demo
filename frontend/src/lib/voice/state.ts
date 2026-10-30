// Voice session state. Practice-mode call events (cards, coverage, enrichment) live in the existing
// call reducer (lib/store.ts); this file only holds the conversation, outcomes and turn timing.
import type { CoverageItem, VoiceItem, VoiceRole, VoiceServerEvent, VoiceSuggestion, VoiceTiming } from "../types";

import { upsertAction, type ActionView } from "../actions";

export type VoiceStatus = "idle" | "connecting" | "live" | "reconnecting" | "ended" | "error";
export type VoiceMode = "hands_free" | "ptt";
export type Flash = "interrupted" | null;

/** The user's words as they are being heard (streamed partial), before the endpoint makes them a turn. */
export interface Caption {
  turn: number;
  text: string;
}

/** Latest live reading: what the speech sounds like right now (SPEC 9.7). */
export interface LiveRead {
  sentiment: number;
  intent: string;
  signals: string[];
  coverage: CoverageItem[] | null;
  partial: boolean;
}

/** A cold STT or TTS endpoint is starting (SPEC 9.6). `since` is the wall clock of the first warming event. */
export interface Warming {
  stage: "stt" | "tts";
  since: number;
}

/** Sentiment history kept for the sparkline. */
export const SERIES_KEEP = 90;

/** One user turn and the reply to it. `turn` is null until the server numbers it. */
export interface Exchange {
  id: string;
  turn: number | null;
  via: "voice" | "text";
  user: string;
  agent: string;
  done: boolean;
  interrupted: boolean;
  error: string | null;
  timing: VoiceTiming | null;
}

export interface VoiceState {
  status: VoiceStatus;
  error: string | null;
  notice: string | null;
  sessionId: string | null;
  callId: string | null;
  role: VoiceRole;
  playbook: string;
  scenario: string | null;
  voice: string;
  mode: VoiceMode;
  muted: boolean;
  userSpeaking: boolean;
  agentSpeaking: boolean;
  flash: Flash;
  exchanges: Exchange[];
  items: VoiceItem[];
  lastTiming: VoiceTiming | null;
  firstAudioHistory: number[];
  startedAt: number | null;
  endedAt: number | null;
  caption: Caption | null;
  live: LiveRead | null;
  series: number[];
  chips: VoiceSuggestion[];
  warming: Warming | null;
  actions: ActionView[];   // SPEC 12: proposed and executed actions for this session
}

export const initialVoiceState: VoiceState = {
  status: "idle",
  error: null,
  notice: null,
  sessionId: null,
  callId: null,
  role: "assistant",
  playbook: "",
  scenario: null,
  voice: "af_heart",
  mode: "hands_free",
  muted: false,
  userSpeaking: false,
  agentSpeaking: false,
  flash: null,
  exchanges: [],
  items: [],
  lastTiming: null,
  firstAudioHistory: [],
  startedAt: null,
  endedAt: null,
  caption: null,
  live: null,
  series: [],
  chips: [],
  warming: null,
  actions: [],
};

export type VoiceAction =
  | {
      type: "begin";
      role: VoiceRole;
      playbook: string;
      scenario: string | null;
      voice: string;
      at: number;
    }
  | { type: "session"; sessionId: string; callId: string | null }
  | { type: "status"; status: VoiceStatus; error?: string | null }
  | { type: "notice"; message: string | null }
  | { type: "local_turn"; id: string; via: "voice" | "text"; text: string }
  | { type: "local_interrupt" }
  | { type: "speaking"; user?: boolean; agent?: boolean }
  | { type: "flash"; value: Flash }
  | { type: "event"; ev: VoiceServerEvent }
  | { type: "hydrate"; exchanges: Exchange[]; items: VoiceItem[] }
  | { type: "set"; patch: Partial<Pick<VoiceState, "mode" | "muted" | "voice">> }
  | { type: "chip_remove"; itemId: string }
  | { type: "caption_clear" }
  | { type: "ended"; at: number; message?: string }
  | { type: "reset" };

/** Finds the exchange for a server turn: exact turn match, else the oldest unnumbered one, else a new one. */
function upsert(list: Exchange[], turn: number, patch: (e: Exchange) => Exchange): Exchange[] {
  let idx = list.findIndex((e) => e.turn === turn);
  if (idx < 0) idx = list.findIndex((e) => e.turn === null);
  if (idx < 0) {
    const fresh: Exchange = {
      id: `srv-${turn}`,
      turn,
      via: "voice",
      user: "",
      agent: "",
      done: false,
      interrupted: false,
      error: null,
      timing: null,
    };
    return [...list, patch(fresh)];
  }
  const next = [...list];
  next[idx] = patch({ ...next[idx], turn: next[idx].turn ?? turn });
  return next;
}

export function isThinking(state: VoiceState): boolean {
  // A reply the user cut off is over for them, even before the server confirms the cancel. Counting it kept the
  // console "busy", so the next Space or typed turn was taken as another barge-in and the orb stuck on Interrupted.
  return state.exchanges.some((e) => !e.done && !e.interrupted);
}

/** Matches the voice socket's event types. `error` is shared with the call socket and is told apart by `stage`. */
const VOICE_ONLY = new Set([
  "user_text",
  "user_partial",
  "agent_text",
  "audio",
  "item",
  "suggestion",
  "suggestion_retire",
  "live",
  "turn_done",
  "status",
  "stream_started",
  "stream_stopped",
  "session_ended",
  "action",
]);

export function isVoiceEvent(ev: { type: string } & object): ev is VoiceServerEvent {
  if (VOICE_ONLY.has(ev.type)) return true;
  return ev.type === "error" && "stage" in ev;
}

const FIRST_AUDIO_KEEP = 20;

export function voiceReducer(state: VoiceState, action: VoiceAction): VoiceState {
  switch (action.type) {
    case "begin":
      return {
        ...initialVoiceState,
        status: "connecting",
        role: action.role,
        playbook: action.playbook,
        scenario: action.scenario,
        voice: action.voice,
        startedAt: action.at,
      };

    case "session":
      return { ...state, sessionId: action.sessionId, callId: action.callId };

    case "status": {
      if (state.status === "ended" || state.status === "idle") return state;
      return { ...state, status: action.status, error: action.error ?? state.error };
    }

    case "notice":
      return state.notice === action.message ? state : { ...state, notice: action.message };

    case "local_turn":
      return {
        ...state,
        exchanges: [
          ...state.exchanges,
          {
            id: action.id,
            turn: null,
            via: action.via,
            user: action.text,
            agent: "",
            done: false,
            interrupted: false,
            error: null,
            timing: null,
          },
        ],
      };

    case "local_interrupt": {
      // Barge-in cuts the reply in flight, as the server does. A turn whose text already finished is not
      // marked: the server records it as complete. Audio still playing is stopped either way.
      const list = [...state.exchanges];
      const last = list.length - 1;
      if (last >= 0 && !list[last].done && !list[last].interrupted) list[last] = { ...list[last], interrupted: true };
      return { ...state, exchanges: list, flash: "interrupted" };
    }

    case "speaking": {
      const user = action.user ?? state.userSpeaking;
      const agent = action.agent ?? state.agentSpeaking;
      if (user === state.userSpeaking && agent === state.agentSpeaking) return state;
      return { ...state, userSpeaking: user, agentSpeaking: agent };
    }

    case "flash":
      return state.flash === action.value ? state : { ...state, flash: action.value };

    case "event":
      return applyVoiceEvent(state, action.ev);

    case "hydrate":
      return { ...state, exchanges: action.exchanges, items: action.items };

    case "set":
      return { ...state, ...action.patch };

    case "ended":
      return {
        ...state,
        status: "ended",
        endedAt: state.endedAt ?? action.at,
        error: action.message ?? state.error,
        userSpeaking: false,
        agentSpeaking: false,
      };

    case "reset":
      return initialVoiceState;

    case "chip_remove":
      return { ...state, chips: state.chips.filter((c) => c.item_id !== action.itemId) };

    case "caption_clear":
      return state.caption ? { ...state, caption: null } : state;

    default:
      return state;
  }
}

function applyVoiceEvent(state: VoiceState, ev: VoiceServerEvent): VoiceState {
  switch (ev.type) {
    case "action":
      return { ...state, actions: upsertAction(state.actions, ev.action) };

    case "user_text":
      return {
        ...state,
        // The endpointed final replaces the streamed caption: the words solidify into the bubble.
        caption: state.caption && state.caption.turn !== ev.turn ? state.caption : null,
        exchanges: upsert(state.exchanges, ev.turn, (e) => ({ ...e, user: ev.text })),
      };

    case "user_partial":
      return { ...state, caption: { turn: ev.turn, text: ev.text } };

    case "live": {
      const series = [...state.series, ev.sentiment].slice(-SERIES_KEEP);
      const live: LiveRead = {
        sentiment: ev.sentiment,
        intent: ev.intent,
        signals: ev.signals,
        coverage: ev.coverage,
        partial: ev.kind === "partial",
      };
      return { ...state, live, series };
    }

    case "suggestion": {
      const chips = state.chips.filter((c) => c.item_id !== ev.item.item_id);
      return { ...state, chips: [...chips, ev.item] };
    }

    case "suggestion_retire":
      return { ...state, chips: state.chips.filter((c) => !ev.item_ids.includes(c.item_id)) };

    case "status":
      if (ev.state === "warming") {
        return { ...state, warming: { stage: ev.stage, since: state.warming?.since ?? Date.now() } };
      }
      return state.warming ? { ...state, warming: null } : state;

    case "session_ended":
      return { ...state, status: "ended", endedAt: state.endedAt ?? Date.now(), warming: null };

    case "agent_text":
      return {
        ...state,
        exchanges: upsert(state.exchanges, ev.turn, (e) => ({ ...e, agent: e.agent + ev.delta })),
      };

    case "item": {
      // A pinned chip becomes an outcome with the same id: it leaves the live lane.
      const items = state.items.filter((i) => i.item_id !== ev.item.item_id);
      const chips = state.chips.filter((c) => c.item_id !== ev.item.item_id);
      return { ...state, items: [...items, ev.item], chips };
    }

    case "turn_done": {
      const timing = ev.timing ?? null;
      const first = typeof timing?.first_audio_ms === "number" ? timing.first_audio_ms : null;
      return {
        ...state,
        exchanges: upsert(state.exchanges, ev.turn, (e) => ({
          ...e,
          done: true,
          interrupted: e.interrupted || ev.interrupted,
          timing,
        })),
        lastTiming: timing ?? state.lastTiming,
        firstAudioHistory:
          first == null ? state.firstAudioHistory : [...state.firstAudioHistory, first].slice(-FIRST_AUDIO_KEEP),
      };
    }

    case "error": {
      // stt and llm failures end the turn; a tts failure leaves the text reply standing.
      const terminal = ev.stage !== "tts";
      return {
        ...state,
        notice: ev.message,
        exchanges: upsert(state.exchanges, ev.turn, (e) => ({
          ...e,
          error: ev.message,
          done: e.done || terminal,
        })),
      };
    }

    default:
      return state;
  }
}

/** Outcome items newest first by created time, falling back to arrival order. */
export function newestItemId(items: VoiceItem[]): string | null {
  let best: VoiceItem | null = null;
  for (const it of items) if (!best || it.created_at_ms >= best.created_at_ms) best = it;
  return best?.item_id ?? null;
}
