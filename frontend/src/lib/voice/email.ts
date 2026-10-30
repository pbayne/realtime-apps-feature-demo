// "Copy all as follow-up email": plain text, sections only where there are items.
import type { OutcomeKind, VoiceItem } from "../types";

export const OUTCOME_ORDER: OutcomeKind[] = [
  "action_item",
  "follow_up_question",
  "follow_up_response",
  "next_step",
  "summary",
  "risk",
  "note",
];

export const OUTCOME_LABEL: Record<OutcomeKind, string> = {
  action_item: "Action items",
  follow_up_question: "Follow-up questions",
  follow_up_response: "Suggested reply",
  next_step: "Next steps",
  summary: "Summary",
  risk: "Things to watch",
  note: "Notes",
};

function meta(item: VoiceItem): string {
  const parts = [item.owner ? `owner: ${item.owner}` : "", item.due ? `due: ${item.due}` : ""].filter(Boolean);
  return parts.length ? ` (${parts.join(", ")})` : "";
}

export function outcomesToEmail(items: VoiceItem[]): string {
  const lines: string[] = ["Hi,", "", "Thank you for your time today. Here is a recap and what comes next.", ""];
  for (const kind of OUTCOME_ORDER) {
    const group = items.filter((i) => i.kind === kind);
    if (!group.length) continue;
    lines.push(OUTCOME_LABEL[kind]);
    for (const item of group) lines.push(`- ${item.text}${meta(item)}`);
    lines.push("");
  }
  lines.push("Best regards,");
  return lines.join("\n");
}
