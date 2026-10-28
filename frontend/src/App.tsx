import { useCallback, useEffect, useRef } from "react";
import { Console } from "./components/Console";
import { PostCallSummary } from "./components/PostCallSummary";
import { StartScreen } from "./components/StartScreen";
import { MOCK, useCallSession } from "./lib/useCallSession";
import type { StartOptions } from "./lib/types";

export default function App() {
  const session = useCallSession();
  const { state } = session;

  // Demo convenience: ?autostart=1[&playbook=<name>][&script=<name>][&mode=replay|mic] starts a call
  // straight away, against the real backend or in ?mock=1 mode. Used for headless screenshots.
  const autostarted = useRef(false);
  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    if (autostarted.current || params.get("autostart") !== "1") return;
    autostarted.current = true;
    const mode = params.get("mode") === "mic" ? "mic" : "replay";
    const playbook = params.get("playbook") || "advisor_coach";
    const script = params.get("script") || (playbook === "contact_center" ? "billing_dispute" : "fee_objection");
    void session.start({ playbook, mode, script: mode === "replay" ? script : undefined });
  }, [session.start]);

  const onStart = useCallback(
    (opts: StartOptions) => {
      void session.start(opts);
    },
    [session.start],
  );

  // Start screen before any call, or after a failed start (error shown there).
  if (state.status === "idle" || (state.status === "error" && !state.callId)) {
    return <StartScreen error={state.error} onStart={onStart} />;
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
