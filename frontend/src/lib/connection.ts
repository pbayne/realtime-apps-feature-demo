import { MAX_RECONNECT_ATTEMPTS, isPermanentClose, reasonForClose, reconnectDelay, type TerminalReason } from "./reconnectPolicy";
import type { ClientMessage, ServerEvent } from "./types";

export type ConnStatus = "connecting" | "open" | "reconnecting" | "closed";

export interface CallConnection {
  send(msg: ClientMessage): void;
  close(): void;
}

export interface CallHandlers {
  onEvent(ev: ServerEvent): void;
  onStatus(status: ConnStatus): void;
  /** The socket has stopped for good and will not reconnect. Fired at most once. Optional for the mock. */
  onTerminal?(reason: TerminalReason): void;
}

export function wsUrl(path: string): string {
  const proto = location.protocol === "https:" ? "wss:" : "ws:";
  return `${proto}//${location.host}${path}`;
}

/** Call event socket with exponential-backoff reconnect, capped; permanent close codes stop it at once. */
export function openCallSocket(callId: string, h: CallHandlers): CallConnection {
  let ws: WebSocket | null = null;
  let closedByUs = false;
  let stopped = false;
  let attempt = 0;
  let timer: number | undefined;

  /** Stop for good: no reconnect, socket closed, and the owner told why. */
  const stop = (reason: TerminalReason) => {
    if (stopped) return;
    stopped = true;
    window.clearTimeout(timer);
    ws?.close();
    ws = null;
    h.onTerminal?.(reason);
  };

  const connect = () => {
    h.onStatus(attempt === 0 ? "connecting" : "reconnecting");
    const sock = new WebSocket(wsUrl(`/ws/call/${encodeURIComponent(callId)}`));
    ws = sock;
    // Status stays connecting/reconnecting until the server speaks: the first frame of any type proves the link.
    let proven = false;
    sock.onmessage = (msg) => {
      let ev: ServerEvent;
      try {
        ev = JSON.parse(String(msg.data)) as ServerEvent;
      } catch {
        return; /* ignore malformed frames */
      }
      if (!proven) {
        proven = true;
        attempt = 0;
        h.onStatus("open");
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

  return {
    send(msg) {
      if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(msg));
    },
    close() {
      closedByUs = true;
      window.clearTimeout(timer);
      ws?.close();
    },
  };
}

/** Binary PCM16 audio socket for one channel. */
export function openAudioSocket(callId: string, channel: "agent" | "customer") {
  const ws = new WebSocket(
    wsUrl(`/ws/audio/${encodeURIComponent(callId)}?channel=${channel}`),
  );
  ws.binaryType = "arraybuffer";
  return {
    send(frame: ArrayBuffer) {
      if (ws.readyState === WebSocket.OPEN) ws.send(frame);
    },
    close() {
      ws.close();
    },
  };
}
