import type { CardKind } from "./types";

export const TARGETS = {
  stt_first_word_ms: 800,
  stt_commit_ms: 1500,
  rtm_ms: 300,
  card_ms: 1500, // customer-final -> card.start (SPEC aim 1 s, hard target 1.5 s)
  card_done_ms: 3000, // customer-final -> card.done (SPEC 7)
} as const;

export type Health = "good" | "warn" | "bad" | "none";

export function health(value: number | null | undefined, target: number): Health {
  if (value == null) return "none";
  if (value <= target) return "good";
  if (value <= target * 1.5) return "warn";
  return "bad";
}

export const HEALTH_CLASS: Record<Health, string> = {
  good: "text-emerald-300",
  warn: "text-amber-300",
  bad: "text-rose-400",
  none: "text-[var(--color-muted)]",
};

export const HEALTH_DOT: Record<Health, string> = {
  good: "bg-emerald-400",
  warn: "bg-amber-400",
  bad: "bg-rose-500",
  none: "bg-slate-600",
};

export function fmtMs(ms: number | null | undefined): string {
  if (ms == null) return "--";
  return ms >= 1000 ? `${(ms / 1000).toFixed(2)} s` : `${Math.round(ms)} ms`;
}

export function fmtClock(ms: number): string {
  const total = Math.max(0, Math.floor(ms / 1000));
  const m = Math.floor(total / 60);
  const s = total % 60;
  return `${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}`;
}

export function sentimentLabel(v: number | null | undefined): { label: string; cls: string } {
  if (v == null) return { label: "n/a", cls: "chip-neutral" };
  if (v >= 0.2) return { label: "Positive", cls: "chip-pos" };
  if (v <= -0.2) return { label: "Negative", cls: "chip-neg" };
  return { label: "Neutral", cls: "chip-neutral" };
}

export const KIND_LABEL: Record<CardKind, string> = {
  next_question: "Next question",
  suggested_response: "Suggested response",
  objection_pickup: "Objection pickup",
  missing_info: "Missing info",
  compliance: "Compliance",
  example: "Example",
  next_best_action: "Next best action",
};
