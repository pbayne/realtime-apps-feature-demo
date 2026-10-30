// /ws/voice/{session_id} (SPEC §9.2). Exponential-backoff reconnect like the call socket.
import { wsUrl, type ConnStatus } from "../connection";
import { MAX_RECONNECT_ATTEMPTS, reasonForClose, reconnectDelay, isPermanentClose, type TerminalReason } from "../reconnectPolicy";
import type { AnyServerEvent, VoiceClientMessage } from "../types";

/** Audio is dropped, not queued, once this much is unsent: a stalled link must not grow an unbounded backlog. */
export const MAX_BUFFERED_BYTES = 256 * 1024;

export interface VoiceTransport {
  /** A WAV turn, or one streamed PCM16 frame while a stream is open. False when not open or backed up. */
  sendAudio(wav: ArrayBuffer): boolean;
  sendJson(msg: VoiceClientMessage): boolean;
  close(): void;
}

export interface VoiceSocketHandlers {
  onEvent(ev: AnyServerEvent): void;
  onStatus(status: ConnStatus): void;
  /** Fired on every successful open. `reconnected` is true after the first open. */
  onOpen(reconnected: boolean): void;
  /** The socket has stopped for good and will not reconnect. Fired at most once. */
  onTerminal(reason: TerminalReason): void;
}

export function openVoiceSocket(sessionId: string, h: VoiceSocketHandlers): VoiceTransport {
  let ws: WebSocket | null = null;
  let closedByUs = false;
  let stopped = false;
  let attempt = 0;
  let opened = false;
  let timer: number | undefined;

  /** Stop for good: no reconnect, socket closed, and the owner told why. */
  const stop = (reason: TerminalReason) => {
    if (stopped) return;
    stopped = true;
    window.clearTimeout(timer);
    ws?.close();
    ws = null;
    h.onTerminal(reason);
  };

  const connect = () => {
    h.onStatus(attempt === 0 ? "connecting" : "reconnecting");
    const sock = new WebSocket(wsUrl(`/ws/voice/${encodeURIComponent(sessionId)}`));
    ws = sock;
    // Status stays connecting/reconnecting until the server speaks: an open socket alone is not "live".
    let proven = false;
    sock.onopen = () => {
      const again = opened;
      opened = true;
      h.onOpen(again);
    };
    sock.onmessage = (msg) => {
      let ev: AnyServerEvent;
      try {
        ev = JSON.parse(String(msg.data)) as AnyServerEvent;
      } catch {
        return; /* ignore malformed frames */
      }
      // The first frame of any type is proof of life. Only a proven connection resets the reconnect count,
      // so a server that accepts and then drops still runs into the cap.
      if (!proven) {
        proven = true;
        attempt = 0;
        h.onStatus("open");
      }
      // The server ends a session by sending session_ended and then closing with 4410. Stop here; no reconnect.
      if (ev?.type === "session_ended") {
        h.onEvent(ev);
        stop("session_ended");
        return;
      }
      h.onEvent(ev);
    };
    sock.onclose = (closed) => {
      if (ws === sock) ws = null;
      if (closedByUs) return h.onStatus("closed");
      if (stopped) return;
      if (isPermanentClose(closed.code)) return stop(reasonForClose(closed.code));
      attempt += 1;
      if (attempt > MAX_RECONNECT_ATTEMPTS) return stop("lost");
      h.onStatus("reconnecting");
      timer = window.setTimeout(connect, reconnectDelay(attempt));
    };
  };

  connect();

  const send = (data: string | ArrayBuffer): boolean => {
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(data);
      return true;
    }
    return false;
  };

  return {
    sendAudio: (wav) => (ws && ws.bufferedAmount > MAX_BUFFERED_BYTES ? false : send(wav)),
    sendJson: (msg) => send(JSON.stringify(msg)),
    close() {
      closedByUs = true;
      window.clearTimeout(timer);
      ws?.close();
      ws = null;
    },
  };
}
