// Reconnect policy shared by the call socket (connection.ts) and the voice socket (voice/voiceSocket.ts).

/** Reconnect attempts after a dropped socket. The next failure after this many gives up ("lost"). */
export const MAX_RECONNECT_ATTEMPTS = 6;

/** Why a socket stopped for good. `lost` is the reconnect cap; the rest map to server close codes or events. */
export type TerminalReason = "not_found" | "bad_request" | "superseded" | "session_ended" | "lost";

/** Shown to the rep when a socket stops for good. */
export const TERMINAL_MESSAGES: Record<TerminalReason, string> = {
  not_found: "Session ended. Start a new one.",
  bad_request: "This session could not continue. Start a new one.",
  superseded: "This session continued in another tab or window.",
  session_ended: "Session ended. Start a new one.",
  lost: "Connection lost. Start a new session.",
};

/** Every 4400-4499 close is permanent: the server has rejected the socket, so reconnecting cannot help. */
export function isPermanentClose(code: number): boolean {
  return code >= 4400 && code <= 4499;
}

/** Close code to terminal reason. Call only for codes where isPermanentClose is true. */
export function reasonForClose(code: number): TerminalReason {
  if (code === 4404) return "not_found";
  if (code === 4409) return "superseded";
  if (code === 4410) return "session_ended";
  return "bad_request";
}

/** Backoff before reconnect attempt `attempt` (1-based): 1 s, 2 s, 4 s, 8 s, then capped at 8 s. */
export function reconnectDelay(attempt: number): number {
  return Math.min(8000, 500 * 2 ** attempt);
}
