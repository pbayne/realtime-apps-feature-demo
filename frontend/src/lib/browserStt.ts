// Browser Web Speech API fallback. Results are forwarded as `browser_stt` messages.

import type { Speaker } from "./types";

interface SpeechResultLike {
  isFinal: boolean;
  0: { transcript: string };
}
interface SpeechEventLike {
  resultIndex: number;
  results: ArrayLike<SpeechResultLike>;
}
interface RecognitionLike {
  continuous: boolean;
  interimResults: boolean;
  lang: string;
  onresult: ((e: SpeechEventLike) => void) | null;
  onerror: ((e: { error: string }) => void) | null;
  onend: (() => void) | null;
  start(): void;
  stop(): void;
  abort(): void;
}

type RecognitionCtor = new () => RecognitionLike;

function getCtor(): RecognitionCtor | null {
  const w = window as unknown as {
    SpeechRecognition?: RecognitionCtor;
    webkitSpeechRecognition?: RecognitionCtor;
  };
  return w.SpeechRecognition ?? w.webkitSpeechRecognition ?? null;
}

export const browserSttSupported = (): boolean => getCtor() !== null;

export interface BrowserStt {
  setSpeaker(s: Speaker): void;
  stop(): void;
}

export function startBrowserStt(
  initialSpeaker: Speaker,
  send: (speaker: Speaker, text: string, isFinal: boolean) => void,
  onError: (msg: string) => void,
): BrowserStt {
  const Ctor = getCtor();
  if (!Ctor) throw new Error("Web Speech API not available in this browser");
  let speaker = initialSpeaker;
  let stopped = false;
  const rec = new Ctor();
  rec.continuous = true;
  rec.interimResults = true;
  rec.lang = "en-US";

  rec.onresult = (e) => {
    for (let i = e.resultIndex; i < e.results.length; i++) {
      const r = e.results[i];
      const text = r[0].transcript.trim();
      if (text) send(speaker, text, r.isFinal);
    }
  };
  rec.onerror = (e) => {
    if (e.error !== "no-speech") onError(`Browser STT: ${e.error}`);
  };
  // Chrome ends continuous sessions periodically; restart until stopped.
  rec.onend = () => {
    if (!stopped) {
      try {
        rec.start();
      } catch {
        /* already started */
      }
    }
  };
  rec.start();

  return {
    setSpeaker(s) {
      speaker = s;
    },
    stop() {
      stopped = true;
      rec.onend = null;
      rec.abort();
    },
  };
}
