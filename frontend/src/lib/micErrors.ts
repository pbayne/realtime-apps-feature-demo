// Friendly text for microphone start failures. The browser's own messages ("Permission denied", a DOMException
// string) do not tell a rep what to do, so both consoles map them here.

/** The browser refused the mic (permission denied or dismissed, or a non-secure page). Shown as a card on the start screen. */
export const MIC_BLOCKED = "Microphone access is blocked for this site.";
/** No usable input device. */
export const MIC_MISSING = "No microphone was found.";

/** Turns a failed start into the text a rep sees. Non-microphone errors keep their own message. */
export function describeStartError(err: unknown): string {
  const name = err instanceof Error ? err.name : "";
  if (name === "NotAllowedError" || name === "SecurityError") return MIC_BLOCKED;
  if (name === "NotFoundError" || name === "NotReadableError" || name === "OverconstrainedError") return MIC_MISSING;
  return err instanceof Error ? err.message : String(err);
}

/** The in-call notice when the mic cannot start. The session carries on as typed turns. */
export function micNotice(err: unknown): string {
  const text = describeStartError(err);
  if (text === MIC_BLOCKED) return `${MIC_BLOCKED} Allow it in the address bar to talk. You can type your turns below.`;
  if (text === MIC_MISSING) return `${MIC_MISSING} Connect one to talk. You can type your turns below.`;
  return `Microphone unavailable (${text}). You can type your turns below.`;
}

export const isMicBlocked = (text: string | null | undefined): boolean => !!text && text.startsWith(MIC_BLOCKED);
