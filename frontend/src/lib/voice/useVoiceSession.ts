// Owns one voice session: the /ws/voice socket, the mic engine, the playback queue, and both reducers
// (voice conversation here; practice-mode call events in the existing call reducer).
import { useCallback, useEffect, useReducer, useRef } from "react";
import { createVoiceSession, endCall, getVoiceSnapshot } from "../api";
import type { ConnStatus } from "../connection";
import { openMockCall } from "../mock";
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
  const sessionRef = useRef<{ role: string; callId: string | null; mock: boolean } | null>(null);
  // Mirrors of React state for the non-React audio callbacks.
  const busyRef = useRef(false);
  const modeRef = useRef<VoiceInputMode>("hands_free");
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
      if (ev.durationMs < MIN_UTTERANCE_MS) return;
      dispatch({ type: "local_turn", id: `local-${++idRef.current}`, via: "voice", text: "" });
      if (!t?.sendAudio(ev.wav)) dispatch({ type: "notice", message: "Connection lost. That turn was not sent." });
    },
    [interruptNow],
  );

  const envNow = useCallback((): VadEnv => {
    const agentPlaying = playerRef.current?.isSpeaking() ?? false;
    return { agentPlaying, busy: busyRef.current || agentPlaying };
  }, []);

  const onSocketStatus = useCallback((g: number, s: ConnStatus) => {
    if (g !== gen.current || s === "closed") return;
    const status = callStatusOf(s);
    dispatch({ type: "status", status });
    callDispatch({ type: "status", status });
  }, []);

  const onSocketOpen = useCallback(async (g: number, sid: string, reconnected: boolean) => {
    if (g !== gen.current) return;
    dispatch({ type: "status", status: "live", error: null });
    callDispatch({ type: "status", status: "live" });
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
  }, []);

  const start = useCallback(
    async (opts: VoiceStartOptions) => {
      teardown();
      const g = gen.current;
      const alive = () => g === gen.current;
      const scenario = opts.role === "practice" ? opts.scenario ?? null : null;

      // Create the player inside the click that started the session so the browser allows audio.
      playerRef.current = new PcmPlayer();
      modeRef.current = "hands_free";
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
      sessionRef.current = { role: opts.role, callId, mock: MOCK };
      if (opts.role === "practice") {
        callDispatch({ type: "begin", callId, playbook: opts.playbook, mode: "mic", at: Date.now() });
      }

      if (MOCK) {
        transportRef.current = openMockVoice(opts.role, MOCK_SPEED, { onEvent: onServer, onLocal }, true);
        if (opts.role === "practice" && callId) {
          // The mock call timeline supplies the practice-mode cards and coverage.
          mockCallRef.current = openMockCall(callId, MOCK_SPEED, { onEvent: onServer, onStatus: () => {} });
        }
        dispatch({ type: "status", status: "live" });
        if (opts.role === "practice") callDispatch({ type: "status", status: "live" });
        return;
      }

      transportRef.current = openVoiceSocket(sessionId, {
        onEvent: onServer,
        onStatus: (s) => onSocketStatus(g, s),
        onOpen: (again) => void onSocketOpen(g, sessionId, again),
      });

      const mic = new MicEngine(envNow, onLocal);
      micRef.current = mic;
      try {
        await mic.start();
      } catch (err) {
        if (!alive()) return;
        const why = err instanceof Error ? err.message : String(err);
        dispatch({ type: "notice", message: `Microphone unavailable (${why}). Type your turns instead.` });
      }
    },
    [envNow, onLocal, onServer, onSocketOpen, onSocketStatus, teardown],
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

  const setMode = useCallback((mode: VoiceInputMode) => {
    modeRef.current = mode;
    micRef.current?.setMode(mode);
    dispatch({ type: "set", patch: { mode } });
  }, []);

  const setMuted = useCallback((muted: boolean) => {
    micRef.current?.setMuted(muted);
    dispatch({ type: "set", patch: { muted } });
  }, []);

  /** Push-to-talk. Without a mic (mock mode) the same speech events are synthesised here. */
  const pttDown = useCallback(() => {
    if (micRef.current) return micRef.current.pttDown();
    if (!MOCK || modeRef.current !== "ptt") return;
    pttAtRef.current = Date.now();
    onLocal({ kind: "speech_start" });
  }, [onLocal]);

  const pttUp = useCallback(() => {
    if (micRef.current) return micRef.current.pttUp();
    if (!MOCK || modeRef.current !== "ptt") return;
    onLocal({ kind: "speech_end", wav: new ArrayBuffer(0), durationMs: Math.max(0, Date.now() - pttAtRef.current) });
  }, [onLocal]);

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
    teardown();
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
    teardown();
    sessionRef.current = null;
    dispatch({ type: "reset" });
    callDispatch({ type: "reset" });
  }, [teardown]);

  /** Fill `out` with the signal to draw: agent audio while it plays, otherwise the mic. */
  const readLevel = useCallback((out: Uint8Array<ArrayBuffer>) => {
    const p = playerRef.current;
    if (p?.isSpeaking()) return p.level(out);
    if (MOCK && userSpeakingRef.current) {
      const now = Date.now();
      for (let i = 0; i < out.length; i++) {
        out[i] = 128 + Math.round(70 * Math.sin(i * 0.21 + now * 0.012) * (0.55 + 0.45 * Math.sin(i * 0.031 + now * 0.004)));
      }
      return;
    }
    if (micRef.current) return micRef.current.level(out);
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
    setVoice,
    resolveCard,
    readLevel,
  };
}

export type VoiceSession = ReturnType<typeof useVoiceSession>;
