// Simulated /ws/voice backend for ?mock=1. It speaks the SPEC §9.2 and §9.7 event order, so the real session
// hook runs unchanged. Scripted "user speech" is streamed as partials while it is spoken (user_partial, live,
// suggestion chips), then the endpoint starts the reply turn exactly as the server does. Replies are fabricated
// text, quiet tone audio (24 kHz s16le), items and timing.
import type {
  AnyServerEvent,
  CoverageItem,
  OutcomeKind,
  VoiceClientMessage,
  VoiceItem,
  VoiceRole,
  VoiceSuggestion,
  VoiceTiming,
} from "../types";
import type { LocalEvent } from "./mic";
import { encodePcm16 } from "./player";
import type { VoiceTransport } from "./voiceSocket";

const PCM_RATE = 24000;

interface MockItem {
  kind: OutcomeKind;
  text: string;
  owner?: string;
  due?: string;
  source_doc?: string;
}

interface MockChip {
  kind: OutcomeKind;
  text: string;
}

interface MockTurn {
  /** What the rep says (STT output). */
  user: string;
  /** What Sidekick (or the AI customer) says aloud. */
  reply: string;
  /** Where the rep's sentiment is heading as they speak, -1..1. Drives the live meter. */
  mood: number;
  /** Provisional chips that appear mid-sentence (SPEC 9.7). */
  chips?: MockChip[];
  items?: MockItem[];
  /** Autorun only: the rep starts talking this many ms after their turn ended, cutting the reply off. */
  bargeInMs?: number;
}

const ASSISTANT: MockTurn[] = [
  {
    user: "I'm worried the fee objection will sink the meeting, so what should I ask next?",
    mood: -0.45,
    reply:
      "Ask what the all-in cost is compared with what they pay today. Then ask what result would make the fee feel worth it. After that, stop and let them answer.",
    chips: [
      { kind: "next_step", text: "Ask what result would make the fee feel worth it" },
      { kind: "follow_up_question", text: "What is the all-in annual cost they pay today?" },
    ],
    items: [
      { kind: "next_step", text: "Ask what result would make the fee feel worth it" },
      {
        kind: "follow_up_question",
        text: "What is the all-in annual cost they pay today?",
        source_doc: "kb/advisor_coach/fee_objections.md",
      },
    ],
  },
  {
    user: "Okay, summarize where we are so far. They want to retire at 62 and the budget is tight.",
    mood: 0.25,
    reply:
      "The client wants to retire at 62 and stop checking the market every day. The household holds about 1.8 million across three accounts. Fees are the main concern, and they sold part of the portfolio last March.",
    items: [
      { kind: "summary", text: "Goal: retire at 62. About $1.8M across three accounts. Main concern: fees." },
      { kind: "risk", text: "Sold part of the portfolio in March. Watch for emotional decisions." },
    ],
    bargeInMs: 1500,
  },
  {
    user: "Sorry, one thing first. Did they mention the old 401k in their assets?",
    mood: 0.05,
    reply:
      "Yes, the old 401k at his former employer is still open. Confirm it can roll into the managed IRA, and check for vesting or cost-basis details.",
    chips: [{ kind: "note", text: "Check the 401k vesting schedule before the rollover." }],
    items: [
      {
        kind: "action_item",
        text: "Confirm the former-employer 401k can roll into the managed IRA",
        owner: "Rep",
        due: "This week",
      },
    ],
  },
  {
    user: "Great, thanks. Draft a follow-up email for them, I'm happy with how this went.",
    mood: 0.5,
    reply:
      "Here is a short draft. Thanks for talking today. You asked about fees and what the plan costs, so I will send a side by side comparison by Friday.",
    items: [
      {
        kind: "follow_up_response",
        text: "Thanks for talking today. I will send a side by side fee comparison by Friday.",
      },
      { kind: "action_item", text: "Send the fee comparison", owner: "Rep", due: "Fri" },
    ],
  },
];

const PRACTICE: MockTurn[] = [
  {
    user: "Thanks for making the time. What would you like this to achieve for you?",
    mood: 0.55,
    reply: "Honestly, we want to retire at 62 and stop checking the market every morning.",
    items: [{ kind: "note", text: "Customer goal: retire at 62 and stop watching the market daily." }],
  },
  {
    user: "Roughly how many years do you have until then?",
    mood: 0.1,
    reply: "About nine years for me, seven or eight for Mike. We also have a rollover IRA and an old 401k.",
  },
  {
    user: "Let me outline how we work together and what it costs.",
    mood: -0.3,
    chips: [{ kind: "risk", text: "Expect a fee push-back. Anchor on net outcome, not the rate." }],
    reply: "Honestly, the fees worry me. Eighty-odd thousand a year sounds like a lot for something I could probably do myself.",
    items: [{ kind: "risk", text: "Fee objection raised. Anchor on net outcome, not the rate." }],
  },
  {
    user: "Fair concern. Let's look at what the fee buys you, net of taxes and mistakes.",
    mood: 0.2,
    reply: "Okay, that makes sense. I still want to see the numbers before we decide anything.",
    items: [{ kind: "next_step", text: "Show a fee versus outcome comparison before the next meeting" }],
  },
];

const ADVISOR_COVERAGE: Array<{ key: string; label: string; words: RegExp }> = [
  { key: "goals", label: "Goals", words: /retire|goal|want/i },
  { key: "time_horizon", label: "Time horizon", words: /years|until|horizon/i },
  { key: "risk_tolerance", label: "Risk tolerance", words: /risk|nervous|comfortable/i },
  { key: "assets", label: "Assets", words: /assets|401k|IRA|portfolio|accounts/i },
  { key: "budget", label: "Income and savings", words: /budget|save|income|tight/i },
  { key: "decision_makers", label: "Decision makers", words: /decide|husband|wife|Mike|spouse/i },
];

const TYPED_REPLY =
  "Good question. Start with what they already do today, then compare it with the outcome they want. Keep it short and let them talk.";

const NEGATIVE = /\b(worried|concerned|worry|expensive|frustrated|too high|upset)\b/i;

export interface MockVoiceHandlers {
  onEvent(ev: AnyServerEvent): void;
  /** Local speech, as the mic would report it. */
  onLocal(ev: LocalEvent): void;
}

export interface MockVoiceOptions {
  /** Streamed input (the default). False: the client sends one WAV per turn, as before. */
  streaming: boolean;
  /** Demo: the first reply waits on a cold TTS endpoint, so the warming banner shows (SPEC 9.6). */
  warmFirst: boolean;
}

interface Reply {
  turn: number;
  userText: string;
  stt: number;
  heard: boolean;
  aborted: boolean;
  timers: number[];
}

const rnd = (lo: number, hi: number) => Math.round(lo + Math.random() * (hi - lo));

/** Soft 196 Hz tone with 20 ms fades, so the wave is visible and playback is audible but quiet. */
function tone(seconds: number): string {
  const n = Math.round(seconds * PCM_RATE);
  const out = new Float32Array(n);
  const fade = Math.round(0.02 * PCM_RATE);
  for (let i = 0; i < n; i++) {
    const env = Math.min(1, i / fade, (n - i) / fade);
    out[i] = 0.035 * env * Math.sin((2 * Math.PI * 196 * i) / PCM_RATE);
  }
  return encodePcm16(out);
}

function sentences(text: string): string[] {
  return text.split(/(?<=[.!?])\s+/).filter(Boolean);
}

/**
 * Opens a simulated voice session. With `autorun`, the scripted rep speaks on a timeline
 * (including one barge-in when the script asks for it). Otherwise the caller drives it.
 */
export function openMockVoice(
  role: VoiceRole,
  speed: number,
  h: MockVoiceHandlers,
  autorun: boolean,
  opts: MockVoiceOptions = { streaming: true, warmFirst: false },
): VoiceTransport {
  const script = role === "practice" ? PRACTICE : ASSISTANT;
  const t = (ms: number) => ms / speed;
  const startedAt = Date.now();
  let closed = false;
  let turnNo = 0;
  let sendIdx = 0;
  let itemNo = 0;
  let current: Reply | null = null;
  let streamOpen = false;
  let warmed = false;
  const chips = new Map<string, VoiceSuggestion>();
  const covered = new Set<string>();

  const emit = (ev: AnyServerEvent) => {
    if (!closed) h.onEvent(ev);
  };
  const later = (r: Reply, ms: number, fn: () => void) => {
    r.timers.push(
      window.setTimeout(() => {
        if (!r.aborted && !closed) fn();
      }, t(ms)),
    );
  };
  const sleep = (ms: number) =>
    new Promise<void>((resolve) => {
      window.setTimeout(resolve, t(ms));
    });

  const coverageItems = (): CoverageItem[] | null =>
    role === "assistant"
      ? ADVISOR_COVERAGE.map((c) => ({ key: c.key, label: c.label, covered: covered.has(c.key) }))
      : null;

  const finish = (r: Reply, interrupted: boolean, timing: VoiceTiming) => {
    if (current === r) current = null;
    retireTurn(r.turn);
    emit({ type: "turn_done", turn: r.turn, interrupted, timing });
  };

  const retireTurn = (turn: number) => {
    const ids = [...chips.values()].filter((c) => c.turn === turn).map((c) => c.item_id);
    ids.forEach((id) => chips.delete(id));
    if (ids.length) emit({ type: "suggestion_retire", turn, item_ids: ids, reason: "turn_done" });
  };

  const reply = (entry: MockTurn, userText: string, typed: boolean) => {
    // A new turn supersedes whatever is still speaking; the real server does the same on interrupt.
    cancel();
    turnNo += 1;
    const stt = typed ? 0 : rnd(180, 240);
    const llm = rnd(420, 640);
    let tts = rnd(120, 170);
    const r: Reply = { turn: turnNo, userText, stt, heard: false, aborted: false, timers: [] };
    current = r;

    later(r, stt, () => {
      r.heard = true;
      emit({ type: "user_text", turn: r.turn, text: userText, stt_ms: typed ? null : stt });
    });

    // Cold start demo: the first sentence waits on TTS, with the warming status and then ready.
    if (opts.warmFirst && !warmed) {
      warmed = true;
      later(r, stt + 60, () => emit({ type: "status", turn: r.turn, stage: "tts", state: "warming",
        message: "Warming up voice (GPU starting)…" }));
      tts += 2600;
      later(r, stt + llm + tts - 40, () => emit({ type: "status", turn: r.turn, stage: "tts", state: "ready",
        elapsed_ms: 2600 }));
    }

    const text = typed ? TYPED_REPLY : entry.reply;
    let at = stt + llm;
    let seq = 0;
    for (const sentence of sentences(text)) {
      const words = sentence.split(" ");
      const start = at;
      for (let i = 0; i < words.length; i += 3) {
        const chunk = `${words.slice(i, i + 3).join(" ")} `;
        const when = at;
        later(r, when, () => emit({ type: "agent_text", turn: r.turn, delta: chunk }));
        at += rnd(60, 95);
      }
      const audioSeq = seq;
      seq += 1;
      const dur = Math.max(0.6, words.length * 0.34);
      later(r, start + tts, () =>
        emit({ type: "audio", turn: r.turn, seq: audioSeq, rate: PCM_RATE, pcm_b64: tone(dur) }),
      );
    }

    const items = typed ? [] : entry.items ?? [];
    later(r, at + 60, () => {
      for (const it of items) {
        itemNo += 1;
        const item: VoiceItem = {
          item_id: `mock-item-${itemNo}`,
          session_id: "mock",
          turn: r.turn,
          kind: it.kind,
          text: it.text,
          owner: it.owner ?? null,
          due: it.due ?? null,
          source_doc: it.source_doc ?? null,
          created_at_ms: Date.now() - startedAt,
        };
        emit({ type: "item", turn: r.turn, item });
      }
    });
    later(r, at + 120 + items.length * 40, () =>
      finish(r, false, {
        stt_ms: stt,
        llm_first_token_ms: llm,
        tts_first_audio_ms: tts,
        first_audio_ms: stt + llm + tts,
        total_ms: at + 120,
      }),
    );
  };

  const cancel = () => {
    const r = current;
    if (!r) return;
    r.aborted = true;
    r.timers.forEach((id) => window.clearTimeout(id));
    if (!r.heard) emit({ type: "user_text", turn: r.turn, text: r.userText, stt_ms: r.stt });
    finish(r, true, { stt_ms: r.stt, total_ms: null, first_audio_ms: null });
  };

  // Scripted rep: speak (partials stream while the words are spoken), wait for the reply, pause, speak again.
  const speak = async (entry: MockTurn, durationMs: number) => {
    const upcoming = turnNo + 1;
    h.onLocal({ kind: "speech_start" });
    const words = entry.user.split(" ");
    const step = durationMs / words.length;
    let lastLiveAt = 0;
    let chipsShown = false;
    words.forEach((_, i) => {
      const at = (i + 1) * step;
      const prefix = words.slice(0, i + 1).join(" ");
      window.setTimeout(() => {
        if (closed || !streamOpen) return;
        if (i >= 1) emit({ type: "user_partial", turn: upcoming, text: prefix });
        const progress = (i + 1) / words.length;
        if (i + 1 - lastLiveAt >= 4 || i === words.length - 1) {
          lastLiveAt = i + 1;
          for (const c of ADVISOR_COVERAGE) if (c.words.test(prefix)) covered.add(c.key);
          const wobble = 0.08 * Math.sin(progress * 9);
          const sentiment = Math.max(-1, Math.min(1, entry.mood * (0.35 + 0.65 * progress) + wobble));
          const signals: string[] = [];
          if (/\?/.test(prefix)) signals.push("question");
          if (NEGATIVE.test(prefix)) signals.push("objection:fees");
          emit({
            type: "live", turn: upcoming, speaker: role === "assistant" ? "user" : "agent", kind: "partial",
            sentiment: Math.round(sentiment * 1000) / 1000,
            intent: /\?/.test(prefix) ? "question" : "statement", signals, coverage: coverageItems(),
          });
        }
        if (!chipsShown && progress >= 0.55 && entry.chips?.length) {
          chipsShown = true;
          for (const c of entry.chips) {
            itemNo += 1;
            const chip: VoiceSuggestion = {
              item_id: `mock-chip-${itemNo}`, session_id: "mock", turn: upcoming, kind: c.kind, text: c.text,
              owner: null, due: null, source_doc: null, created_at_ms: Date.now() - startedAt, provisional: true,
            };
            chips.set(chip.item_id, chip);
            emit({ type: "suggestion", turn: upcoming, item: chip });
          }
        }
      }, t(at));
    });
    await sleep(durationMs);
    if (closed) return;
    h.onLocal({ kind: "speech_end", wav: new ArrayBuffer(0), durationMs });
    if (opts.streaming) {
      // Streamed: the endpoint is the server's job. The mock plays the server's part and starts the reply turn.
      sendIdx += 1;
      reply(entry, entry.user, false);
    }
  };

  const run = async () => {
    await sleep(1400);
    for (let i = 0; i < script.length && !closed; i++) {
      await speak(script[i], 1500);
      if (closed) return;
      const entry = script[i];
      if (entry.bargeInMs) {
        await sleep(entry.bargeInMs);
        continue;
      }
      while (current && !closed) await sleep(150);
      await sleep(1100);
    }
  };

  if (autorun) void run();

  const transport: VoiceTransport = {
    sendAudio(): boolean {
      if (closed) return false;
      // Streamed frames are not replayed: the scripted rep drives the stream. WAV turns reply as before.
      if (opts.streaming) return true;
      const entry = script[sendIdx % script.length];
      sendIdx += 1;
      reply(entry, entry.user, false);
      return true;
    },
    sendJson(msg: VoiceClientMessage): boolean {
      if (closed) return false;
      switch (msg.type) {
        case "stream_start":
          streamOpen = true;
          emit({ type: "stream_started", sample_rate: 16000, endpoint_silence_ms: 700, frame_ms: 100 });
          return true;
        case "stream_stop":
          streamOpen = false;
          emit({ type: "stream_stopped", flush: msg.flush, dropped_frames: 0 });
          // Push-to-talk release: the flushed utterance is the next scripted turn.
          if (msg.flush && opts.streaming) {
            const entry = script[sendIdx % script.length];
            sendIdx += 1;
            reply(entry, entry.user, false);
          }
          return true;
        case "pin": {
          const chip = chips.get(msg.item_id);
          if (!chip) {
            // The server reports a stale pin without a stage (it is not a turn error): same shape here.
            emit({ type: "error", turn: turnNo, message: "unknown or superseded suggestion" } as unknown as AnyServerEvent);
            return true;
          }
          chips.delete(msg.item_id);
          const { provisional: _p, ...item } = chip;
          void _p;
          emit({ type: "item", turn: item.turn, item: { ...item, created_at_ms: Date.now() - startedAt } });
          return true;
        }
        case "dismiss":
          chips.delete(msg.item_id);
          return true;
        case "interrupt":
          cancel();
          return true;
        case "text": {
          const entry = script[sendIdx % script.length];
          sendIdx += 1;
          reply(entry, msg.text, true);
          return true;
        }
        default:
          return true;
      }
    },
    close() {
      closed = true;
      cancel();
    },
  };
  return transport;
}
