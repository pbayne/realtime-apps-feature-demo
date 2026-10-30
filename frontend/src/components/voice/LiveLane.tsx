import { Pin, X } from "lucide-react";
import { OUTCOME_LABEL } from "../../lib/voice/email";
import type { VoiceSuggestion } from "../../lib/types";

interface Props {
  chips: VoiceSuggestion[];
  onPin(itemId: string): void;
  onDismiss(itemId: string): void;
}

/**
 * Provisional suggestions (SPEC 9.7). They are guesses made while the user talks: pinning one files it as an
 * outcome, and the end of the turn replaces whatever is left with the authoritative list below it.
 */
export function LiveLane({ chips, onPin, onDismiss }: Props) {
  return (
    <section aria-label="Live suggestions" className="rounded-xl border border-dashed border-sky-400/25 bg-sky-400/[0.03] px-3 py-2.5">
      <div className="mb-2 flex items-center gap-2">
        <span className="anim-live-dot size-1.5 rounded-full bg-sky-300" aria-hidden />
        <h2 className="text-[11px] font-semibold uppercase tracking-[0.09em] text-sky-200">Live</h2>
        <span className="text-[11px] text-[var(--color-muted)]">
          {chips.length ? "Pin to keep. Gone at the end of the turn." : "Suggestions appear while you talk."}
        </span>
      </div>
      {chips.length > 0 && (
        <ul className="space-y-1.5">
          {chips.map((chip) => (
            <li
              key={chip.item_id}
              className="anim-card-in flex items-start gap-2 rounded-lg border border-sky-400/20 bg-[var(--color-raised)]/70 py-1.5 pl-2.5 pr-1.5"
            >
              <div className="min-w-0 flex-1">
                <p className="text-[11px] font-semibold uppercase tracking-[0.08em] text-sky-200/80">
                  {OUTCOME_LABEL[chip.kind]}
                </p>
                <p className="text-[13px] leading-snug text-slate-100">{chip.text}</p>
              </div>
              <button
                type="button"
                onClick={() => onPin(chip.item_id)}
                aria-label={`Pin suggestion: ${chip.text}`}
                title="Pin"
                className="grid size-7 shrink-0 place-items-center rounded-md text-sky-200 hover:bg-sky-400/15 focus-visible:bg-sky-400/15"
              >
                <Pin className="size-3.5" />
              </button>
              <button
                type="button"
                onClick={() => onDismiss(chip.item_id)}
                aria-label={`Dismiss suggestion: ${chip.text}`}
                title="Dismiss"
                className="grid size-7 shrink-0 place-items-center rounded-md text-[var(--color-muted)] hover:bg-white/[0.06] hover:text-slate-100 focus-visible:bg-white/[0.06]"
              >
                <X className="size-3.5" />
              </button>
            </li>
          ))}
        </ul>
      )}
    </section>
  );
}
