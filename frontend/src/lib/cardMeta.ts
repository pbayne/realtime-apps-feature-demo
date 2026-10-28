import {
  CircleHelp,
  Lightbulb,
  MessageSquareText,
  Route,
  SearchX,
  ShieldAlert,
  Target,
  type LucideIcon,
} from "lucide-react";
import type { CardKind } from "./types";

export interface KindMeta {
  Icon: LucideIcon;
  /** Tailwind-free colour tokens used via inline style (hex). */
  color: string;
  /** Translucent tint for tile backgrounds. */
  tint: string;
}

export const KIND_META: Record<CardKind, KindMeta> = {
  next_question: { Icon: CircleHelp, color: "#60A5FA", tint: "rgba(96,165,250,0.14)" },
  suggested_response: { Icon: MessageSquareText, color: "#A78BFA", tint: "rgba(167,139,250,0.14)" },
  objection_pickup: { Icon: Target, color: "#F59E0B", tint: "rgba(245,158,11,0.14)" },
  missing_info: { Icon: SearchX, color: "#2DD4BF", tint: "rgba(45,212,191,0.14)" },
  compliance: { Icon: ShieldAlert, color: "#F43F5E", tint: "rgba(244,63,94,0.16)" },
  example: { Icon: Lightbulb, color: "#34D399", tint: "rgba(52,211,153,0.14)" },
  next_best_action: { Icon: Route, color: "#38BDF8", tint: "rgba(56,189,248,0.14)" },
};

export function confidenceColor(c: number): string {
  if (c >= 0.85) return "#34D399";
  if (c >= 0.7) return "#60A5FA";
  return "#F59E0B";
}
