import type { Caption } from "../../lib/voice/state";

interface Props {
  /** The streamed words of the user's current utterance, or null between utterances. */
  caption: Caption | null;
  /** The mic is open and nothing is being heard yet. */
  listening: boolean;
}

/**
 * What the user is saying, as it is heard. The streamed text shimmers; the endpointed final replaces it in the
 * conversation, where it settles (see the `solidify` animation on the user bubble). Announced politely: a live
 * region that changes on every partial would be unusable, so the text is the content, not a stream of events.
 */
/** The newest words are the ones a rep needs, so a long partial shows its tail, not its head. */
export function captionTail(text: string, words = 14): string {
  const parts = text.trim().split(/\s+/);
  return parts.length <= words ? text.trim() : `… ${parts.slice(-words).join(" ")}`;
}

export function LiveCaption({ caption, listening }: Props) {
  return (
    <div className="flex min-h-[44px] items-center gap-2.5 border-t border-[var(--color-line)] px-4 py-2">
      <span className="kbd-label shrink-0 text-[11px]">Hearing</span>
      <p aria-live="polite" className="min-w-0 flex-1 truncate text-[14px] leading-snug">
        {caption ? (
          <span className="caption-shimmer" data-testid="caption">
            {captionTail(caption.text)}
          </span>
        ) : listening ? (
          <span className="text-[var(--color-muted)]">Listening for your next words</span>
        ) : (
          <span className="text-[var(--color-muted)]">Mic is off</span>
        )}
      </p>
    </div>
  );
}
