import { useEffect, useLayoutEffect, useRef, useState, type FormEvent } from "react";
import { ArrowDown, Send } from "lucide-react";
import { fmtMs } from "../../lib/format";
import type { Exchange } from "../../lib/voice/state";
import type { VoiceRole } from "../../lib/types";

interface Props {
  exchanges: Exchange[];
  role: VoiceRole;
  agentLabel: string;
}

export function Conversation({ exchanges, role, agentLabel }: Props) {
  const scroller = useRef<HTMLDivElement>(null);
  const [stick, setStick] = useState(true);

  // Keep pinned to the newest line unless the rep scrolled up to re-read.
  useLayoutEffect(() => {
    const el = scroller.current;
    if (el && stick) el.scrollTop = el.scrollHeight;
  }, [exchanges, stick]);

  const onScroll = () => {
    const el = scroller.current;
    if (!el) return;
    setStick(el.scrollHeight - el.scrollTop - el.clientHeight < 48);
  };

  return (
    <div className="relative min-h-0 flex-1">
      <div ref={scroller} onScroll={onScroll} className="scroll-thin absolute inset-0 space-y-3 overflow-y-auto px-4 py-4">
        {exchanges.length === 0 && (
          <div className="grid h-full place-items-center text-center text-[13px] text-[var(--color-muted)]">
            <div>
              <p className="text-slate-300">
                {role === "practice" ? "Say your opening line. The customer answers out loud." : "Ask anything about the playbook or the call."}
              </p>
              <p className="mt-1.5">Speak, or type below. Replies play aloud and are filed as outcomes.</p>
            </div>
          </div>
        )}

        {exchanges.map((e) => (
          <div key={e.id} className="space-y-2">
            <UserBubble ex={e} />
            <AgentBubble ex={e} label={agentLabel} />
          </div>
        ))}
      </div>

      {!stick && (
        <button
          type="button"
          onClick={() => {
            setStick(true);
            const el = scroller.current;
            if (el) el.scrollTop = el.scrollHeight;
          }}
          className="btn btn-ghost absolute bottom-3 left-1/2 -translate-x-1/2 py-1 text-[12px] shadow-lg"
        >
          <ArrowDown className="size-3.5" /> Latest
        </button>
      )}
    </div>
  );
}

function UserBubble({ ex }: { ex: Exchange }) {
  const pending = !ex.user && !ex.done;
  return (
    <div className="flex justify-end">
      <div className="max-w-[80%] rounded-2xl rounded-br-md border border-sky-400/20 bg-sky-400/[0.08] px-3.5 py-2">
        <div className="mb-0.5 text-[10px] font-semibold uppercase tracking-[0.08em] text-sky-200/80">You</div>
        {pending ? (
          <div className="flex items-center gap-1.5 py-1 text-[13px] text-[var(--color-muted)]">
            <span className="anim-blink size-1.5 rounded-full bg-sky-300" /> Transcribing…
          </div>
        ) : (
          <p className="anim-solidify text-[14px] leading-relaxed text-slate-100">{ex.user}</p>
        )}
      </div>
    </div>
  );
}

function AgentBubble({ ex, label }: { ex: Exchange; label: string }) {
  const waiting = !ex.agent && !ex.done;
  const cut = ex.interrupted;
  return (
    <div className="flex justify-start">
      <div
        className={`max-w-[84%] rounded-2xl rounded-bl-md border px-3.5 py-2 ${
          cut ? "border-amber-400/30 bg-amber-400/[0.06]" : "border-[var(--color-line)] bg-[var(--color-raised)]"
        }`}
      >
        <div className="mb-0.5 flex items-center gap-2 text-[10px] font-semibold uppercase tracking-[0.08em] text-[var(--color-muted)]">
          {label}
          {cut && <span className="chip border-amber-400/40 bg-amber-400/10 normal-case tracking-normal text-amber-200">cut off</span>}
        </div>
        {waiting ? (
          <div className="flex items-center gap-1.5 py-1 text-[13px] text-[var(--color-muted)]">
            <span className="anim-blink size-1.5 rounded-full bg-violet-300" /> Thinking…
          </div>
        ) : (
          <p className={`text-[14px] leading-relaxed ${cut ? "text-slate-300" : "text-slate-200"}`}>
            {ex.agent}
            {!ex.done && <span className="anim-blink ml-0.5 inline-block h-3.5 w-[2px] translate-y-0.5 bg-sky-300" aria-hidden />}
          </p>
        )}
        {ex.error && <p className="mt-1 text-[12px] text-rose-300">{ex.error}</p>}
        {ex.done && ex.timing?.first_audio_ms != null && (
          <p className="mt-1 font-mono text-[10.5px] text-[var(--color-muted)]">first audio {fmtMs(ex.timing.first_audio_ms)}</p>
        )}
      </div>
    </div>
  );
}

/** Typed turn. Enter sends. Typing while Sidekick talks is a new turn, same as speaking. */
export function Composer({ disabled, onSend }: { disabled: boolean; onSend(text: string): void }) {
  const [text, setText] = useState("");
  const ref = useRef<HTMLInputElement>(null);
  useEffect(() => {
    if (!disabled) ref.current?.focus({ preventScroll: true });
  }, [disabled]);

  const submit = (e: FormEvent) => {
    e.preventDefault();
    const clean = text.trim();
    if (!clean || disabled) return;
    onSend(clean);
    setText("");
  };

  return (
    <form onSubmit={submit} className="flex items-center gap-2 border-t border-[var(--color-line)] p-3">
      <input
        ref={ref}
        value={text}
        onChange={(e) => setText(e.target.value)}
        disabled={disabled}
        placeholder={disabled ? "Session is not live" : "Type a turn…"}
        aria-label="Type a turn"
        className="min-w-0 flex-1 rounded-lg border border-[var(--color-line)] bg-[var(--color-ink)] px-3 py-2.5 text-[14px] outline-none placeholder:text-slate-500 focus-visible:outline-none focus:border-sky-400/60 disabled:opacity-50"
      />
      <button type="submit" disabled={disabled || !text.trim()} className="btn btn-ghost py-2.5">
        <Send className="size-3.5" /> Send
      </button>
    </form>
  );
}
