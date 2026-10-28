// Client-side simulator of the call WebSocket (`?mock=1`). Emits SPEC §3 events
// on a timeline so the console can be developed and demoed without the backend.
import type { Card, CardKind, ClientMessage, CoverageItem, ServerEvent, Speaker } from "./types";
import type { CallConnection, CallHandlers } from "./connection";

interface Enr {
  intent?: string;
  sentiment?: number;
  risk_flags?: string[];
  objection?: string | null;
}

interface Turn {
  speaker: Speaker;
  text: string;
  enr: Enr;
  /** Covers these coverage keys once the turn is finalised. */
  covers?: string[];
  /** Cards the copilot emits right after this customer turn. */
  cards?: MockCard[];
  pauseAfterMs?: number;
}

interface MockCard {
  kind: CardKind;
  title: string;
  body: string;
  confidence: number;
  source_doc: string | null;
  source_clause: string | null;
  trigger: string | null;
}

const COVERAGE: CoverageItem[] = [
  { key: "goals", label: "Goals & priorities", covered: false },
  { key: "horizon", label: "Time horizon", covered: false },
  { key: "risk", label: "Risk tolerance", covered: false },
  { key: "assets", label: "Assets & accounts", covered: false },
  { key: "decision_makers", label: "Decision makers", covered: false },
  { key: "fees", label: "Fees addressed", covered: false },
  { key: "next_steps", label: "Next steps agreed", covered: false },
];

const TURNS: Turn[] = [
  {
    speaker: "agent",
    text: "Thanks for making the time, Dana. Before we get into numbers, what would you like this relationship to achieve for you?",
    enr: { intent: "rapport", sentiment: 0.3 },
    pauseAfterMs: 900,
  },
  {
    speaker: "customer",
    text: "Honestly, we want to retire at 62 and stop checking the market every morning.",
    enr: { intent: "goal_statement", sentiment: 0.35 },
    covers: ["goals"],
    pauseAfterMs: 1400,
  },
  {
    speaker: "agent",
    text: "That is a clear goal. Roughly how many years do you have until that date?",
    enr: { intent: "question", sentiment: 0.4 },
    covers: ["horizon"],
    pauseAfterMs: 1000,
  },
  {
    speaker: "customer",
    text: "About nine years for me, seven or eight for Mike.",
    enr: { intent: "answer", sentiment: 0.1 },
    pauseAfterMs: 1600,
  },
  {
    speaker: "agent",
    text: "And how did you feel during the last downturn? Did you sell, or hold on?",
    enr: { intent: "question", sentiment: 0.2 },
    covers: ["risk"],
    pauseAfterMs: 1000,
  },
  {
    speaker: "customer",
    text: "Honestly I panicked a little. I sold part of it in March and regretted it almost immediately.",
    enr: { intent: "disclosure", sentiment: -0.45, risk_flags: ["emotional_decision"] },
    cards: [
      {
        kind: "example",
        title: "Reference: holding through the 2020 drawdown",
        body: "Clients who stayed invested through March 2020 were typically back near prior highs within months. Those who sold often re-entered late and locked in the loss.",
        confidence: 0.78,
        source_doc: "kb/behavioral_examples.md",
        source_clause: "2020 drawdown: stay-the-course cohort",
        trigger: "emotion:regret",
      },
    ],
    pauseAfterMs: 1800,
  },
  {
    speaker: "agent",
    text: "That is very common, and it is exactly why we build a plan you can hold through a bad quarter. What do the accounts look like today?",
    enr: { intent: "question", sentiment: 0.3 },
    pauseAfterMs: 1100,
  },
  {
    speaker: "customer",
    text: "A joint brokerage, Mike's rollover IRA, and an old 401k at his former employer. Maybe 1.8 million all together.",
    enr: { intent: "answer", sentiment: 0.15 },
    covers: ["assets"],
    cards: [
      {
        kind: "missing_info",
        title: "Confirm where the old 401k sits",
        body: "Ask whether the former-employer 401k is still held there or can be rolled into the managed IRA, and whether there are any vesting or cost-basis details.",
        confidence: 0.71,
        source_doc: "kb/account_consolidation.md",
        source_clause: "Rollover checklist, item 2",
        trigger: "missing:assets.401k_status",
      },
    ],
    pauseAfterMs: 1700,
  },
  {
    speaker: "agent",
    text: "Got it. Who else should be part of the decisions we make together?",
    enr: { intent: "question", sentiment: 0.3 },
    covers: ["decision_makers"],
    pauseAfterMs: 1000,
  },
  {
    speaker: "customer",
    text: "Just the two of us. My sister is our executor, but she does not weigh in on investments.",
    enr: { intent: "answer", sentiment: 0.1 },
    pauseAfterMs: 1500,
  },
  {
    speaker: "agent",
    text: "Great. Let me outline how we would work together and what it costs, so there are no surprises.",
    enr: { intent: "pitch", sentiment: 0.25 },
    pauseAfterMs: 1000,
  },
  {
    speaker: "customer",
    text: "Honestly, the fees worry me. Eighty-odd thousand a year sounds like a lot for something I could probably do myself.",
    enr: {
      intent: "objection",
      sentiment: -0.6,
      risk_flags: ["fee_objection"],
      objection: "fees",
    },
    cards: [
      {
        kind: "objection_pickup",
        title: "Fee objection: anchor on net outcome",
        body: "Acknowledge the concern, then reframe around net-of-fee outcomes and the cost of a March-style sell-off. Ask what they would expect the plan to protect.",
        confidence: 0.86,
        source_doc: "kb/fee_objections.md",
        source_clause: "2.1 Reframe fee as behavioural coaching",
        trigger: "objection:fees",
      },
    ],
    pauseAfterMs: 1600,
  },
  {
    speaker: "agent",
    text: "That is a fair question. The fee covers planning and the discipline that kept you from selling in March. Would it help to walk through the numbers net of fees?",
    enr: { intent: "response", sentiment: 0.35 },
    covers: ["fees"],
    pauseAfterMs: 1300,
  },
  {
    speaker: "customer",
    text: "Sure. But can you guarantee we will beat the market if we pay that?",
    enr: {
      intent: "question",
      sentiment: -0.2,
      risk_flags: ["performance_guarantee"],
    },
    cards: [
      {
        kind: "compliance",
        title: "Do not guarantee returns",
        body: "Avoid any return guarantee or market-beating promise. Say: no one can guarantee returns; here is how the plan manages risk against your specific goals.",
        confidence: 0.97,
        source_doc: "kb/advisor_disclosures.md",
        source_clause: "Section 4: performance claims",
        trigger: "compliance:performance_guarantee",
      },
    ],
    pauseAfterMs: 1800,
  },
  {
    speaker: "agent",
    text: "I cannot promise returns, and I would not want you to rely on that. What I can show you is how the plan manages risk toward your retirement date.",
    enr: { intent: "response", sentiment: 0.2 },
    pauseAfterMs: 1200,
  },
  {
    speaker: "customer",
    text: "Okay. I think I want to think about it, though, and maybe talk to Mike again.",
    enr: {
      intent: "stall",
      sentiment: -0.1,
      risk_flags: ["stall_signal"],
      objection: "think_about_it",
    },
    cards: [
      {
        kind: "next_best_action",
        title: "Book a joint follow-up with Mike",
        body: "Offer a 20-minute follow-up with both decision-makers this week, and send the net-of-fee comparison beforehand so the conversation starts from numbers.",
        confidence: 0.82,
        source_doc: "kb/next_steps.md",
        source_clause: "Stalled discovery: joint follow-up",
        trigger: "objection:think_about_it",
      },
    ],
    pauseAfterMs: 1400,
  },
  {
    speaker: "agent",
    text: "Of course. Shall we put a 20-minute call with you and Mike on the calendar for this week?",
    enr: { intent: "close", sentiment: 0.4 },
    covers: ["next_steps"],
    pauseAfterMs: 1200,
  },
];

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

function rnd(lo: number, hi: number) {
  return Math.round(lo + Math.random() * (hi - lo));
}

/**
 * Runs the scripted call. `speed` > 1 compresses time (e.g. speed=2 for a fast demo).
 * Returns a CallConnection-shaped object; `send` records feedback locally.
 */
export function openMockCall(callId: string, speed: number, h: CallHandlers): CallConnection {
  let cancelled = false;
  const t = (ms: number) => ms / speed;
  const emit = (ev: ServerEvent) => {
    if (!cancelled) h.onEvent(ev);
  };
  const wait = async (ms: number) => {
    await sleep(t(ms));
  };

  const coverage = COVERAGE.map((c) => ({ ...c }));
  let seq = 0;
  let cardSeq = 0;
  const startMs = Date.now();

  const pushCoverage = () => emit({ type: "coverage", items: coverage.map((c) => ({ ...c })) });

  const streamCard = async (c: MockCard, turnSeq: number) => {
    cardSeq += 1;
    const cardId = `mock-card-${cardSeq}`;
    const base = {
      card_id: cardId,
      call_id: callId,
      seq: turnSeq,
      kind: c.kind,
      title: c.title,
      confidence: c.confidence,
      source_doc: c.source_doc,
      source_clause: c.source_clause,
      trigger: c.trigger,
      status: "shown" as const,
      created_at_ms: Date.now() - startMs,
    };
    emit({ type: "card.start", card: base });
    // Stream the body in word-sized deltas, like the real compose step.
    const words = c.body.split(/(\s+)/);
    let acc = "";
    for (const w of words) {
      if (cancelled) return;
      await sleep(t(rnd(45, 90)));
      acc += w;
      emit({ type: "card.delta", card_id: cardId, delta: w });
    }
    const done: Card = { ...base, body: acc };
    emit({ type: "card.done", card: done });
  };

  const runTurn = async (turn: Turn) => {
    seq += 1;
    const mySeq = seq;
    const words = turn.text.split(" ");
    // Partials: reveal the sentence in growing chunks, like streaming STT.
    const chunk = Math.max(3, Math.ceil(words.length / 4));
    for (let n = chunk; n < words.length; n += chunk) {
      emit({
        type: "transcript.partial",
        speaker: turn.speaker,
        text: words.slice(0, n).join(" "),
        seq: mySeq,
      });
      await wait(rnd(260, 380));
      if (cancelled) return;
    }
    const latency = rnd(90, 160);
    await wait(rnd(380, 520));
    emit({
      type: "transcript.final",
      speaker: turn.speaker,
      text: turn.text,
      seq: mySeq,
      latency_ms: latency,
    });
    emit({ type: "enrichment", seq: mySeq, ...turn.enr });
    // Latency HUD samples. One deliberately slow commit so the amber state shows.
    const slow = mySeq === 6;
    emit({
      type: "metrics",
      stt_first_word_ms: rnd(560, 740),
      stt_commit_ms: slow ? rnd(1750, 1900) : rnd(1150, 1450),
      rtm_ms: rnd(110, 240),
      card_ms: rnd(900, 1450),
    });

    if (turn.covers && turn.covers.length) {
      for (const key of turn.covers) {
        const item = coverage.find((c) => c.key === key);
        if (item) item.covered = true;
      }
      pushCoverage();
    }

    if (turn.cards) {
      await wait(rnd(350, 600));
      for (const c of turn.cards) {
        if (cancelled) return;
        await streamCard(c, mySeq);
      }
    }
    if (turn.pauseAfterMs) await wait(turn.pauseAfterMs);
  };

  const run = async () => {
    await wait(500);
    pushCoverage();
    for (const turn of TURNS) {
      if (cancelled) return;
      await runTurn(turn);
    }
    await wait(800);
    emit({ type: "call.ended" });
  };

  h.onStatus("open");
  void run();

  return {
    send(msg: ClientMessage) {
      // Feedback is applied optimistically by the store; the mock just logs it.
      if (msg.type === "feedback") console.debug("[mock] feedback", msg);
    },
    close() {
      cancelled = true;
      h.onStatus("closed");
    },
  };
}
