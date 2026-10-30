// Owns one voice session: the /ws/voice socket, the mic engine, the playback queue, and both reducers
// (voice conversation here; practice-mode call events in the existing call reducer).
//
// Input is streamed by default (SPEC 9.2, streaming input): `stream_start`, then 100 ms PCM16 frames while the
// mic is open. Hands-free keeps the stream open; push-to-talk opens it on key down and flushes on key up; mute
// discards the open utterance. `?stream=0` falls back to one WAV per turn (the client VAD decides the turn end).
import { useCallback, useEffect, useReducer, useRef } from "react";
import { beaconVoiceSessionEnd, createVoiceSession, endCall, endVoiceSession, getVoiceSnapshot } from "../api";
import type { ConnStatus } from "../connection";
import { openMockCall } from "../mock";
import { micNotice } from "../micErrors";
import { TERMINAL_MESSAGES, type TerminalReason } from "../reconnectPolicy";
import { initialCallState, reducer as callReducer, type CallStatus, type Resolution } from "../store";
import { MOCK, MOCK_SPEED } from "../useCallSession";
import type { AnyServerEvent, ServerEvent, VoiceClientMessage, VoiceStartOptions } from "../types";
import { MicEngine, type LocalEvent, type VoiceInputMode } from "./mic";
import { openMockVoice } from "./mockVoice";
import { PcmPlayer } from "./player";
import { initialVoiceState, isThinking, isVoiceEvent, voiceReducer, type Exchange } from "./state";
import { openVoiceSocket, type VoiceTransport } from "./voiceSocket";
import { MIN_UTTERANCE_MS, type VadEnv } from "./vad";

const FLASH_MS = 1400;

/** Streamed input unless the page is opened with ?stream=0 (the WAV-per-turn compatibility mode). */
const STREAMING =
  typeof window === "undefined" || new URLSearchParams(window.location.search).get("stream") !== "0";

const callStatusOf = (s: ConnStatus): CallStatus =>
  s === "open" ? "live" : s === "reconnecting" ? "reconnecting" : s === "closed" ? "ended" : "connecting";

export function useVoiceSession() {
  const [state, dispatch] = useReducer(voiceReducer, initialVoiceState);
  const [callState, callDispatch] = useReducer(callReducer, initialCallState);

  // Bumped on every start and teardown; callbacks from an older session compare against it and bail.
  const gen = useRef(0);
  const transportRef = useRef<VoiceTransport | null>(null);
  const playerRef = useRef<PcmPlayer | null>(null);
  const micRef = useRef<MicEngine | null>(null);
  const mockCallRef = useRef<{ close(): void } | null>(null);
  /** The live server session, or null once it has ended, been released, or was never created. */
  const sessionRef = useRef<{ role: string; sessionId: string; callId: string | null; mock: boolean } | null>(null);
  // Mirrors of React state for the non-React audio callbacks.
  const busyRef = useRef(false);
  const modeRef = useRef<VoiceInputMode>("hands_free");
  const mutedRef = useRef(false);
  const streamOpenRef = useRef(false);
  const userSpeakingRef = useRef(false);
  const pttAtRef = useRef(0);
  const flashTimer = useRef<number | undefined>(undefined);
  const idRef = useRef(0);

  useEffect(() => {
    busyRef.current = isThinking(state);
  }, [state]);

  const teardown = useCallback(() => {
    gen.current += 1;
    micRef.current?.stop();
    micRef.current = null;
    transportRef.current?.close();
    transportRef.current = null;
    streamOpenRef.current = false;
    mockCallRef.current?.close();
    mockCallRef.current = null;
    playerRef.current?.close();
    playerRef.current = null;
    window.clearTimeout(flashTimer.current);
  }, []);

  const flashInterrupted = useCallback(() => {
    window.clearTimeout(flashTimer.current);
    flashTimer.current = window.setTimeout(() => dispatch({ type: "flash", value: null }), FLASH_MS);
  }, []);

  /** Barge-in: silence Sidekick now and tell the server to cancel the turn in flight. */
  const interruptNow = useCallback(() => {
    playerRef.current?.stop();
    transportRef.current?.sendJson({ type: "interrupt" });
    dispatch({ type: "local_interrupt" });
    flashInterrupted();
  }, [flashInterrupted]);

  /** Open the stream (or re-arm the mic gate after a reconnect or a mic start). Never while muted. */
  const openStream = useCallback(() => {
    const t = transportRef.current;
    if (!STREAMING || !t || mutedRef.current) return;
    if (!streamOpenRef.current) {
      const msg: VoiceClientMessage = { type: "stream_start", sample_rate: 16000 };
      if (!t.sendJson(msg)) return;
      streamOpenRef.current = true;
    }
    micRef.current?.setGate(true);
  }, []);

  /** Close the stream. `flush` finalises the open utterance into a turn (push-to-talk release); otherwise it is dropped. */
  const closeStream = useCallback((flush: boolean) => {
    micRef.current?.setGate(false);
    if (!streamOpenRef.current) return;
    streamOpenRef.current = false;
    const msg: VoiceClientMessage = { type: "stream_stop", flush };
    transportRef.current?.sendJson(msg);
  }, []);

  const onServer = useCallback((ev: AnyServerEvent) => {
    if (isVoiceEvent(ev)) {
      if (ev.type === "audio") {
        playerRef.current?.enqueue(ev.turn, ev.seq, ev.pcm_b64, ev.rate);
        return;
      }
      if (ev.type === "user_text") playerRef.current?.beginTurn(ev.turn);
      dispatch({ type: "event", ev });
      return;
    }
    // Practice mode: call events (transcript, enrichment, cards, coverage) share this socket.
    callDispatch({ type: "event", ev: ev as ServerEvent, at: Date.now() });
  }, []);

  /** Mic or mock: a local speech boundary. Starting speech during a reply is a barge-in. */
  const onLocal = useCallback(
    (ev: LocalEvent) => {
      const t = transportRef.current;
      if (ev.kind === "speech_start") {
        userSpeakingRef.current = true;
        const busy = busyRef.current || (playerRef.current?.isSpeaking() ?? false);
        if (busy) interruptNow();
        dispatch({ type: "speaking", user: true });
        return;
      }
      userSpeakingRef.current = false;
      dispatch({ type: "speaking", user: false });
      // Streamed: the server endpoints the turn from the frames it already has.
      if (STREAMING) return;
      if (ev.durationMs < MIN_UTTERANCE_MS) return;
      dispatch({ type: "local_turn", id: `local-${++idRef.current}`, via: "voice", text: "" });
      if (!t?.sendAudio(ev.wav ?? new ArrayBuffer(0))) dispatch({ type: "notice", message: "Connection lost. That turn was not sent." });
    },
    [interruptNow],
  );

  const envNow = useCallback((): VadEnv => {
    const agentPlaying = playerRef.current?.isSpeaking() ?? false;
    return { agentPlaying, busy: busyRef.current || agentPlaying };
  }, []);

  const onSocketStatus = useCallback((g: number, s: ConnStatus) => {
    if (g !== gen.current || s === "closed") return;
    if (s !== "open") {
      // The server discards an open stream when its socket goes away; reopen after the reconnect.
      streamOpenRef.current = false;
      micRef.current?.setGate(false);
    }
    const status = callStatusOf(s);
    dispatch({ type: "status", status });
    callDispatch({ type: "status", status });
  }, []);

  /** The voice socket stopped for good: release the mic and playback, and say why. Ignored for a stale session. */
  const onSocketTerminal = useCallback(
    (g: number, reason: TerminalReason) => {
      if (g !== gen.current) return;
      const info = sessionRef.current;
      sessionRef.current = null;
      // A lost connection can leave the session open on the server: release it. A superseded socket is not ours
      // to release (another tab owns the session), and session_ended means the server has already closed it.
      if (reason === "lost" && info && !info.mock) void endVoiceSession(info.sessionId);
      teardown();
      const at = Date.now();
      const message = TERMINAL_MESSAGES[reason];
      dispatch({ type: "ended", at, message });
      callDispatch({ type: "ended", at, message });
    },
    [teardown],
  );

  const onSocketOpen = useCallback(async (g: number, sid: string, reconnected: boolean) => {
    if (g !== gen.current) return;
    // Not live yet: the status turns "live" when the server's first frame arrives (onSocketStatus "open").
    if (modeRef.current === "hands_free") openStream();
    if (!reconnected) return;
    // The socket dropped mid-session: rebuild the conversation from the server snapshot.
    const snap = await getVoiceSnapshot(sid);
    if (!snap || g !== gen.current) return;
    const exchanges: Exchange[] = snap.turns.map((t) => ({
      id: `snap-${t.turn}`,
      turn: t.turn,
      via: "voice",
      user: t.user_text,
      agent: t.agent_text,
      done: true,
      interrupted: t.interrupted,
      error: null,
      timing: t.timing ?? null,
    }));
    dispatch({ type: "hydrate", exchanges, items: snap.items });
  }, [openStream]);

  const start = useCallback(
    async (opts: VoiceStartOptions) => {
      teardown();
      const g = gen.current;
      const alive = () => g === gen.current;
      const scenario = opts.role === "practice" ? opts.scenario ?? null : null;

      // Create the player inside the click that started the session so the browser allows audio.
      playerRef.current = new PcmPlayer();
      modeRef.current = "hands_free";
      mutedRef.current = false;
      streamOpenRef.current = false;
      userSpeakingRef.current = false;
      dispatch({
        type: "begin",
        role: opts.role,
        playbook: opts.playbook,
        scenario,
        voice: opts.voice,
        at: Date.now(),
      });

      let sessionId: string;
      let callId: string | null;
      if (MOCK) {
        const rand = Math.random().toString(36).slice(2, 8);
        sessionId = `mock-${rand}`;
        callId = opts.role === "practice" ? `mock-call-${rand}` : opts.callId ?? null;
      } else {
        try {
          const res = await createVoiceSession({
            role: opts.role,
            playbook: opts.playbook,
            scenario: scenario ?? undefined,
            callId: opts.role === "assistant" ? opts.callId : undefined,
            voice: opts.voice,
          });
          sessionId = res.session_id;
          callId = res.call_id;
        } catch (err) {
          if (!alive()) return;
          teardown();
          dispatch({ type: "status", status: "error", error: err instanceof Error ? err.message : String(err) });
          return;
        }
      }
      if (!alive()) return;

      dispatch({ type: "session", sessionId, callId });
      sessionRef.current = { role: opts.role, sessionId, callId, mock: MOCK };
      if (opts.role === "practice") {
        callDispatch({ type: "begin", callId, playbook: opts.playbook, mode: "mic", at: Date.now() });
      }

      if (MOCK) {
        transportRef.current = openMockVoice(opts.role, MOCK_SPEED, { onEvent: onServer, onLocal }, true, {
          streaming: STREAMING,
          warmFirst: new URLSearchParams(window.location.search).get("warm") === "1",
        });
        if (opts.role === "practice" && callId) {
          // The mock call timeline supplies the practice-mode cards and coverage.
          mockCallRef.current = openMockCall(callId, MOCK_SPEED, { onEvent: onServer, onStatus: () => {} });
        }
        dispatch({ type: "status", status: "live" });
        if (opts.role === "practice") callDispatch({ type: "status", status: "live" });
        openStream();
        return;
      }

      transportRef.current = openVoiceSocket(sessionId, {
        onEvent: onServer,
        onStatus: (s) => onSocketStatus(g, s),
        onOpen: (again) => void onSocketOpen(g, sessionId, again),
        onTerminal: (reason) => onSocketTerminal(g, reason),
      });

      const sink = STREAMING ? (pcm: ArrayBuffer) => void transportRef.current?.sendAudio(pcm) : undefined;
      const mic = new MicEngine(envNow, onLocal, sink);
      micRef.current = mic;
      try {
        await mic.start();
      } catch (err) {
        if (!alive()) return;
        dispatch({ type: "notice", message: micNotice(err) });
        return;
      }
      if (alive() && modeRef.current === "hands_free") openStream();
    },
    [envNow, onLocal, onServer, onSocketOpen, onSocketStatus, onSocketTerminal, openStream, teardown],
  );

  const sendText = useCallback(
    (text: string) => {
      const clean = text.trim();
      const t = transportRef.current;
      if (!clean || !t) return;
      if (busyRef.current || (playerRef.current?.isSpeaking() ?? false)) interruptNow();
      dispatch({ type: "local_turn", id: `local-${++idRef.current}`, via: "text", text: clean });
      t.sendJson({ type: "text", text: clean });
    },
    [interruptNow],
  );

  const setMode = useCallback(
    (mode: VoiceInputMode) => {
      modeRef.current = mode;
      micRef.current?.setMode(mode);
      // Leaving hands-free drops any utterance still open; entering it re-opens the stream.
      if (mode === "ptt") closeStream(false);
      else openStream();
      dispatch({ type: "set", patch: { mode } });
    },
    [closeStream, openStream],
  );

  const setMuted = useCallback(
    (muted: boolean) => {
      mutedRef.current = muted;
      micRef.current?.setMuted(muted);
      if (muted) closeStream(false);
      else if (modeRef.current === "hands_free") openStream();
      dispatch({ type: "set", patch: { muted } });
    },
    [closeStream, openStream],
  );

  /** Push-to-talk. Without a mic (mock mode) the same speech events are synthesised here. */
  const pttDown = useCallback(() => {
    if (modeRef.current !== "ptt") return;
    if (micRef.current) {
      if (STREAMING) openStream();
      return micRef.current.pttDown();
    }
    if (!MOCK) return;
    pttAtRef.current = Date.now();
    if (STREAMING) openStream();
    onLocal({ kind: "speech_start" });
  }, [onLocal, openStream]);

  const pttUp = useCallback(() => {
    if (micRef.current) {
      micRef.current.pttUp();
      if (STREAMING) closeStream(true);
      return;
    }
    if (!MOCK || modeRef.current !== "ptt") return;
    onLocal({ kind: "speech_end", wav: new ArrayBuffer(0), durationMs: Math.max(0, Date.now() - pttAtRef.current) });
    if (STREAMING) closeStream(true);
  }, [closeStream, onLocal]);

  /** Barge-in from the keyboard (Esc): only while Sidekick is thinking or talking. */
  const interrupt = useCallback(() => {
    if (busyRef.current || (playerRef.current?.isSpeaking() ?? false)) interruptNow();
  }, [interruptNow]);

  const pinChip = useCallback((itemId: string) => {
    const msg: VoiceClientMessage = { type: "pin", item_id: itemId };
    transportRef.current?.sendJson(msg);
  }, []);

  const dismissChip = useCallback((itemId: string) => {
    dispatch({ type: "chip_remove", itemId });
    const msg: VoiceClientMessage = { type: "dismiss", item_id: itemId };
    transportRef.current?.sendJson(msg);
  }, []);

  const setVoice = useCallback((voice: string) => {
    dispatch({ type: "set", patch: { voice } });
    const msg: VoiceClientMessage = { type: "config", voice };
    transportRef.current?.sendJson(msg);
  }, []);

  const resolveCard = useCallback((cardId: string, action: "accept" | "reject" | "edit", text?: string) => {
    const msg: VoiceClientMessage =
      action === "edit"
        ? { type: "feedback", card_id: cardId, action, text }
        : { type: "feedback", card_id: cardId, action };
    transportRef.current?.sendJson(msg);
    const resolution: Resolution = action === "accept" ? "accepted" : action === "edit" ? "edited" : "rejected";
    callDispatch({ type: "resolve", cardId, resolution, text });
  }, []);

  const end = useCallback(async () => {
    const info = sessionRef.current;
    sessionRef.current = null;
    teardown();
    // Release the server session first; for practice, the DELETE also ends the call, and endCall below is kept as it was.
    if (info && !info.mock) void endVoiceSession(info.sessionId);
    if (info && !info.mock && info.role === "practice" && info.callId) {
      try {
        await endCall(info.callId);
      } catch {
        /* best effort: the session is closed either way */
      }
    }
    dispatch({ type: "ended", at: Date.now() });
    callDispatch({ type: "ended", at: Date.now() });
  }, [teardown]);

  const reset = useCallback(() => {
    // Leaving a session that is still open (the rep went back to the start screen): release it on the server.
    const info = sessionRef.current;
    sessionRef.current = null;
    if (info && !info.mock) void endVoiceSession(info.sessionId);
    teardown();
    dispatch({ type: "reset" });
    callDispatch({ type: "reset" });
  }, [teardown]);

  // Tab close or navigation away: the server session is released with a beacon while one is open.
  useEffect(() => {
    const onPageHide = () => {
      const info = sessionRef.current;
      if (info && !info.mock) beaconVoiceSessionEnd(info.sessionId);
    };
    window.addEventListener("pagehide", onPageHide);
    return () => window.removeEventListener("pagehide", onPageHide);
  }, []);

  /** Mic level for the orb: the real analyser, or a synthetic signal in mock mode while the rep is speaking. */
  const readMic = useCallback((out: Uint8Array<ArrayBuffer>) => {
    if (MOCK && userSpeakingRef.current) {
      synth(out, Date.now(), 70);
      return;
    }
    if (micRef.current && !mutedRef.current) micRef.current.level(out);
    else out.fill(128);
  }, []);

  /** Sidekick (or the practice customer) speech level, from the playback analyser. */
  const readAgent = useCallback((out: Uint8Array<ArrayBuffer>) => {
    const p = playerRef.current;
    if (p?.isSpeaking()) {
      if (MOCK) synth(out, Date.now(), 46);
      else p.level(out);
      return;
    }
    out.fill(128);
  }, []);

  // Agent-speaking is read from the playback queue, not from events, so it tracks gapless audio.
  const live = state.status === "live" || state.status === "reconnecting";
  useEffect(() => {
    if (!live) return;
    const id = window.setInterval(() => {
      dispatch({ type: "speaking", agent: playerRef.current?.isSpeaking() ?? false });
    }, 120);
    return () => window.clearInterval(id);
  }, [live]);

  useEffect(() => () => teardown(), [teardown]);

  return {
    state,
    callState,
    start,
    end,
    reset,
    sendText,
    setMode,
    setMuted,
    pttDown,
    pttUp,
    interrupt,
    setVoice,
    resolveCard,
    pinChip,
    dismissChip,
    readMic,
    readAgent,
  };
}

/** Fake waveform for mock mode: a breathing signal around the midpoint, amplitude `amp` out of 128. */
function synth(out: Uint8Array<ArrayBuffer>, now: number, amp: number): void {
  for (let i = 0; i < out.length; i++) {
    out[i] = 128 + Math.round(amp * Math.sin(i * 0.21 + now * 0.012) * (0.55 + 0.45 * Math.sin(i * 0.031 + now * 0.004)));
  }
}

export type VoiceSession = ReturnType<typeof useVoiceSession>;
