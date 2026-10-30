import { useCallback, useEffect, useRef } from "react";
import { Console } from "./components/Console";
import { PostCallSummary } from "./components/PostCallSummary";
import { StartScreen } from "./components/StartScreen";
import { VoiceConsole } from "./components/voice/VoiceConsole";
import { MOCK, useCallSession } from "./lib/useCallSession";
import { useVoiceSession } from "./lib/voice/useVoiceSession";
import type { StartOptions, VoiceRole, VoiceStartOptions } from "./lib/types";

export default function App() {
  const session = useCallSession();
  const voice = useVoiceSession();
  const { state } = session;
  const vState = voice.state;
  // A voice session owns the screen from the moment it starts until the rep returns to the start screen.
  // A voice start that failed before a session existed falls through to the start screen with its error.
  const voiceActive = vState.status !== "idle" && !(vState.status === "error" && !vState.sessionId);

  // Demo convenience (?autostart=1): starts a call straight away.
  //   ?mode=replay|mic                       call console (real backend or ?mock=1)
  //   ?voice=assistant|practice              voice session (add ?mock=1 for a simulated one: no backend, no mic)
  //   &playbook=<name>&script=<name>|scenario=<name>
  const autostarted = useRef(false);
  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    if (autostarted.current || params.get("autostart") !== "1") return;
    autostarted.current = true;
    const playbook = params.get("playbook") || "advisor_coach";
    const voiceRole = params.get("voice");
    if (voiceRole) {
      // Real sessions too: the browser still asks for the mic, and audio starts after the first user gesture.
      const role: VoiceRole = voiceRole === "practice" ? "practice" : "assistant";
      const scenario =
        params.get("scenario") ||
        (role === "practice" ? (playbook === "contact_center" ? "billing_dispute" : "fee_objection") : undefined);
      void voice.start({ role, playbook, scenario, voice: "af_heart" });
      return;
    }
    const mode = params.get("mode") === "mic" ? "mic" : "replay";
    const script = params.get("script") || (playbook === "contact_center" ? "billing_dispute" : "fee_objection");
    void session.start({ playbook, mode, script: mode === "replay" ? script : undefined });
  }, [session.start, voice.start]);

  const onStart = useCallback(
    (opts: StartOptions) => {
      void session.start(opts);
    },
    [session.start],
  );

  const onStartVoice = useCallback(
    (opts: VoiceStartOptions) => {
      void voice.start(opts);
    },
    [voice.start],
  );

  if (voiceActive) {
    return <VoiceConsole api={voice} onNew={voice.reset} />;
  }

  const startError = vState.status === "error" ? vState.error ?? undefined : state.error;

  // Start screen before any call, or after a failed start (error shown there).
  if (state.status === "idle" || (state.status === "error" && !state.callId) || vState.status === "error") {
    return <StartScreen error={startError} onStart={onStart} onStartVoice={onStartVoice} />;
  }

  if (state.status === "ended") {
    return <PostCallSummary state={state} onNewCall={session.reset} />;
  }

  return (
    <div className="h-full">
      {MOCK && (
        <div className="bg-sky-400/10 px-5 py-1 text-center text-[11px] tracking-wide text-sky-200">
          MOCK MODE · simulated event stream, no backend
        </div>
      )}
      <Console
        state={state}
        channel={session.channel}
        sttBrowser={session.sttBrowser}
        notice={session.notice}
        onEnd={() => void session.end()}
        onChannel={session.setChannel}
        onBrowserStt={session.setBrowserStt}
        onResolve={session.resolveCard}
      />
    </div>
  );
}
