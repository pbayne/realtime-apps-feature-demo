// /ws/voice/{session_id} (SPEC §9.2). Exponential-backoff reconnect like the call socket.
import { wsUrl, type ConnStatus } from "../connection";
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
}

export function openVoiceSocket(sessionId: string, h: VoiceSocketHandlers): VoiceTransport {
  let ws: WebSocket | null = null;
  let closedByUs = false;
  let attempt = 0;
  let opened = false;
  let timer: number | undefined;

  const connect = () => {
    h.onStatus(attempt === 0 ? "connecting" : "reconnecting");
    const sock = new WebSocket(wsUrl(`/ws/voice/${encodeURIComponent(sessionId)}`));
    ws = sock;
    sock.onopen = () => {
      const again = opened;
      opened = true;
      attempt = 0;
      h.onStatus("open");
      h.onOpen(again);
    };
    sock.onmessage = (msg) => {
      try {
        h.onEvent(JSON.parse(String(msg.data)) as AnyServerEvent);
      } catch {
        /* ignore malformed frames */
      }
    };
    sock.onclose = () => {
      if (ws === sock) ws = null;
      if (closedByUs) return h.onStatus("closed");
      attempt += 1;
      h.onStatus("reconnecting");
      timer = window.setTimeout(connect, Math.min(8000, 500 * 2 ** attempt));
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
