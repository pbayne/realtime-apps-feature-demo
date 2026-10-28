import type { ClientMessage, ServerEvent } from "./types";

export type ConnStatus = "connecting" | "open" | "reconnecting" | "closed";

export interface CallConnection {
  send(msg: ClientMessage): void;
  close(): void;
}

export interface CallHandlers {
  onEvent(ev: ServerEvent): void;
  onStatus(status: ConnStatus): void;
}

function wsUrl(path: string): string {
  const proto = location.protocol === "https:" ? "wss:" : "ws:";
  return `${proto}//${location.host}${path}`;
}

/** Call event socket with simple exponential-backoff reconnect. */
export function openCallSocket(callId: string, h: CallHandlers): CallConnection {
  let ws: WebSocket | null = null;
  let closedByUs = false;
  let attempt = 0;
  let timer: number | undefined;

  const connect = () => {
    h.onStatus(attempt === 0 ? "connecting" : "reconnecting");
    ws = new WebSocket(wsUrl(`/ws/call/${encodeURIComponent(callId)}`));
    ws.onopen = () => {
      attempt = 0;
      h.onStatus("open");
    };
    ws.onmessage = (msg) => {
      try {
        h.onEvent(JSON.parse(String(msg.data)) as ServerEvent);
      } catch {
        /* ignore malformed frames */
      }
    };
    ws.onclose = () => {
      ws = null;
      if (closedByUs) return h.onStatus("closed");
      attempt += 1;
      h.onStatus("reconnecting");
      timer = window.setTimeout(connect, Math.min(8000, 500 * 2 ** attempt));
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
