import { useCallback, useEffect, useReducer, useRef, useState } from "react";
import { createCall, endCall } from "./api";
import { startMicCapture, type MicCapture } from "./audio";
import { openAudioSocket } from "./connection";
import { openCallSocket, type CallConnection, type ConnStatus } from "./connection";
import { browserSttSupported, startBrowserStt, type BrowserStt } from "./browserStt";
import { openMockCall } from "./mock";
import { initialCallState, reducer, type Resolution } from "./store";
import type { ClientMessage, Mode, ServerEvent, Speaker } from "./types";

export const URL_PARAMS = new URLSearchParams(window.location.search);
export const MOCK = URL_PARAMS.get("mock") === "1";
export const MOCK_SPEED = Math.max(0.25, Number(URL_PARAMS.get("speed") ?? "1") || 1);

export interface SessionOptions {
  playbook: string;
  mode: Mode;
  script?: string;
}

const statusFromConn = (s: ConnStatus) =>
  s === "open" ? "live" : s === "reconnecting" ? "reconnecting" : s === "closed" ? "ended" : "connecting";

/** Owns the live call: transport, mic, browser STT fallback, and the state reducer. */
export function useCallSession() {
  const [state, dispatch] = useReducer(reducer, initialCallState);
  const [channel, setChannelState] = useState<Speaker>("customer");
  const [sttBrowser, setSttBrowser] = useState(false);
  const [notice, setNotice] = useState<string | null>(null);

  const connRef = useRef<CallConnection | null>(null);
  const micRef = useRef<MicCapture | null>(null);
  const audioRef = useRef<{ send(f: ArrayBuffer): void; close(): void } | null>(null);
  const sttRef = useRef<BrowserStt | null>(null);
  const channelRef = useRef<Speaker>("customer");
  const liveMicRef = useRef(false);
  const callIdRef = useRef<string | null>(null);
  const sttBrowserRef = useRef(false);

  const closeAudio = () => {
    audioRef.current?.close();
    audioRef.current = null;
  };
  const openAudioFor = (callId: string, ch: Speaker) => {
    closeAudio();
    audioRef.current = openAudioSocket(callId, ch);
  };
  const stopStt = () => {
    sttRef.current?.stop();
    sttRef.current = null;
  };
  const teardownMedia = useCallback(() => {
    liveMicRef.current = false;
    closeAudio();
    stopStt();
    micRef.current?.stop();
    micRef.current = null;
  }, []);

  const handleEvent = useCallback(
    (ev: ServerEvent) => {
      dispatch({ type: "event", ev, at: Date.now() });
      if (ev.type === "error" && ev.message) setNotice(ev.message);
      if (ev.type === "call.ended") teardownMedia();
    },
    [teardownMedia],
  );

  const handleStatus = useCallback((s: ConnStatus) => {
    dispatch({ type: "status", status: statusFromConn(s) });
  }, []);

  const start = useCallback(
    async (opts: SessionOptions) => {
      setNotice(null);
      teardownMedia();
      connRef.current?.close();
      dispatch({ type: "begin", callId: null, playbook: opts.playbook, mode: opts.mode, script: opts.script, at: Date.now() });
      try {
        const wantsMic = opts.mode === "mic" && !MOCK;
        // Ask for the mic first so a denied permission fails before a call is created.
        if (wantsMic) {
          const frames = (pcm: ArrayBuffer) => audioRef.current?.send(pcm);
          micRef.current = await startMicCapture(frames);
          liveMicRef.current = true;
        }

        if (MOCK) {
          const id = `mock-${Math.random().toString(36).slice(2, 8)}`;
          callIdRef.current = id;
          dispatch({ type: "begin", callId: id, playbook: opts.playbook, mode: opts.mode, script: opts.script, at: Date.now() });
          connRef.current = openMockCall(id, MOCK_SPEED, { onEvent: handleEvent, onStatus: handleStatus });
          return;
        }

        const { call_id } = await createCall({ playbook: opts.playbook, mode: opts.mode, script: opts.script });
        callIdRef.current = call_id;
        dispatch({ type: "begin", callId: call_id, playbook: opts.playbook, mode: opts.mode, script: opts.script, at: Date.now() });
        connRef.current = openCallSocket(call_id, { onEvent: handleEvent, onStatus: handleStatus });

        if (wantsMic) {
          openAudioFor(call_id, channelRef.current);
        }
      } catch (err) {
        teardownMedia();
        dispatch({ type: "error", message: err instanceof Error ? err.message : String(err) });
      }
    },
    [handleEvent, handleStatus, teardownMedia],
  );

  /** Switch which channel the mic represents (agent or customer). */
  const setChannel = useCallback((ch: Speaker) => {
    channelRef.current = ch;
    setChannelState(ch);
    const id = callIdRef.current;
    if (liveMicRef.current && id && !sttBrowserRef.current && !MOCK) openAudioFor(id, ch);
    sttRef.current?.setSpeaker(ch);
  }, []);

  /** Toggle the browser Web Speech fallback (mic mode only). */
  const setBrowserStt = useCallback(
    (on: boolean) => {
      const id = callIdRef.current;
      sttBrowserRef.current = on;
      setSttBrowser(on);
      if (!on) {
        stopStt();
        if (liveMicRef.current && id && !MOCK) openAudioFor(id, channelRef.current);
        return;
      }
      if (!browserSttSupported()) {
        setNotice("Browser speech recognition is not available in this browser.");
        sttBrowserRef.current = false;
        setSttBrowser(false);
        return;
      }
      // Browser STT replaces the backend audio stream to avoid double transcription.
      closeAudio();
      sttRef.current = startBrowserStt(
        channelRef.current,
        (speaker, text, isFinal) => {
          connRef.current?.send({ type: "browser_stt", speaker, text, is_final: isFinal });
        },
        (msg) => setNotice(msg),
      );
    },
    [],
  );

  const resolveCard = useCallback(
    (cardId: string, action: "accept" | "reject" | "edit", text?: string) => {
      const msg: ClientMessage =
        action === "edit"
          ? { type: "feedback", card_id: cardId, action, text }
          : { type: "feedback", card_id: cardId, action };
      connRef.current?.send(msg);
      const resolution: Resolution = action === "accept" ? "accepted" : action === "edit" ? "edited" : "rejected";
      dispatch({ type: "resolve", cardId, resolution, text });
    },
    [],
  );

  const end = useCallback(async () => {
    const id = callIdRef.current;
    teardownMedia();
    if (id && !MOCK) {
      try {
        await endCall(id);
      } catch (err) {
        setNotice(`End call request failed: ${err instanceof Error ? err.message : String(err)}`);
      }
    }
    connRef.current?.close();
    connRef.current = null;
    dispatch({ type: "ended", at: Date.now() });
  }, [teardownMedia]);

  const reset = useCallback(() => {
    teardownMedia();
    connRef.current?.close();
    connRef.current = null;
    callIdRef.current = null;
    setChannelState("customer");
    channelRef.current = "customer";
    setSttBrowser(false);
    sttBrowserRef.current = false;
    dispatch({ type: "reset" });
  }, [teardownMedia]);

  // Clean up on unmount.
  useEffect(() => () => {
    teardownMedia();
    connRef.current?.close();
  }, [teardownMedia]);

  return {
    state,
    channel,
    sttBrowser,
    notice,
    setNotice,
    start,
    end,
    reset,
    setChannel,
    setBrowserStt,
    resolveCard,
  };
}
