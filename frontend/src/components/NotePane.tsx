// NotePane — freeform notes you type during a call or voice session (Granola/Humla-inspired).
// Autosaves on a debounce to /api/notes, scoped to the live call or voice session. In mock mode it keeps
// the text locally with no network. The model fuses these notes with the transcript for summaries.
import { useCallback, useEffect, useRef, useState } from "react";
import { Check, Loader2, NotebookPen, TriangleAlert } from "lucide-react";
import { getNote, type NoteScope, saveNote } from "../lib/notes";

type SaveState = "idle" | "saving" | "saved" | "error";

const SAVE_DEBOUNCE_MS = 700;

interface Props {
  /** The live scope the note belongs to. Undefined until a call/session id exists (textarea disabled). */
  scope: NoteScope | null;
  /** Mock mode: keep the note locally, no backend. */
  mock: boolean;
  className?: string;
}

/** A stable string key for a scope, so a changed scope reloads the note. */
function scopeKey(scope: NoteScope | null): string {
  if (!scope) return "";
  return "call_id" in scope ? `call:${scope.call_id}` : `session:${scope.session_id}`;
}

function wordCount(text: string): number {
  const t = text.trim();
  return t ? t.split(/\s+/).length : 0;
}

export function NotePane({ scope, mock, className = "" }: Props) {
  const [text, setText] = useState("");
  const [save, setSave] = useState<SaveState>("idle");
  const [loaded, setLoaded] = useState(false);
  const timer = useRef<number | null>(null);
  const key = scopeKey(scope);
  // The text last persisted, so an identical debounce flush is a no-op (and the initial load never resaves).
  const persisted = useRef<string>("");

  // Load the note when the scope appears or changes.
  useEffect(() => {
    setLoaded(false);
    setText("");
    setSave("idle");
    persisted.current = "";
    if (!scope) return;
    if (mock) {
      setLoaded(true);
      return;
    }
    let alive = true;
    getNote(scope)
      .then((n) => {
        if (!alive) return;
        setText(n.text);
        persisted.current = n.text;
        setLoaded(true);
      })
      .catch(() => alive && setLoaded(true));
    return () => {
      alive = false;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key, mock]);

  const flush = useCallback(
    (value: string) => {
      if (!scope || mock || value === persisted.current) return;
      setSave("saving");
      saveNote(scope, value)
        .then((n) => {
          persisted.current = n.text;
          setSave("saved");
        })
        .catch(() => setSave("error"));
    },
    [scope, mock],
  );

  const onChange = (value: string) => {
    setText(value);
    if (mock) {
      persisted.current = value;
      return;
    }
    setSave(value === persisted.current ? "idle" : "saving");
    if (timer.current) window.clearTimeout(timer.current);
    timer.current = window.setTimeout(() => flush(value), SAVE_DEBOUNCE_MS);
  };

  // Flush a pending edit on unmount (e.g. ending the call) so nothing is lost.
  useEffect(
    () => () => {
      if (timer.current) window.clearTimeout(timer.current);
    },
    [],
  );

  const onBlur = () => {
    if (timer.current) window.clearTimeout(timer.current);
    flush(text);
  };

  const words = wordCount(text);
  const active = !!scope && loaded;

  return (
    <section
      aria-labelledby="notes-heading"
      className={`panel group flex min-h-0 flex-col overflow-hidden transition-[box-shadow,border-color] duration-200 focus-within:border-teal-400/40 focus-within:shadow-[0_0_0_1px_var(--color-line),0_10px_30px_-18px_rgba(45,212,191,0.5)] ${className}`}
    >
      <header className="flex items-center justify-between gap-3 border-b border-[var(--color-line)] px-4 py-3">
        <div className="min-w-0">
          <h2 id="notes-heading" className="flex items-center gap-2 text-[14px] font-semibold">
            <NotebookPen className="size-4 text-teal-300 transition-colors group-focus-within:text-teal-200" aria-hidden /> Notes
          </h2>
          <p className="text-[11.5px] text-[var(--color-muted)]">Your notes join the transcript in the summary.</p>
        </div>
        <SaveBadge state={mock ? (words > 0 ? "saved" : "idle") : save} />
      </header>
      <textarea
        value={text}
        onChange={(e) => onChange(e.target.value)}
        onBlur={onBlur}
        disabled={!active}
        spellCheck
        placeholder={
          scope
            ? "Jot down what matters — key points, commitments, things to follow up. Autosaves as you type."
            : "Notes open once the session is live."
        }
        className="scroll-thin min-h-0 flex-1 resize-none bg-transparent px-4 py-3.5 text-[13.5px] leading-relaxed text-slate-100 placeholder:text-[var(--color-muted)] focus:outline-none disabled:opacity-50"
        aria-label="Session notes"
      />
      {active && (
        <footer className="flex items-center justify-between border-t border-[var(--color-line)] px-4 py-1.5 text-[11px] text-[var(--color-muted)]">
          <span className="tabular-nums">{words === 0 ? "No notes yet" : `${words} ${words === 1 ? "word" : "words"}`}</span>
          <span className="opacity-80">Fused into the summary</span>
        </footer>
      )}
    </section>
  );
}

function SaveBadge({ state }: { state: SaveState }) {
  if (state === "saving")
    return (
      <span className="flex items-center gap-1.5 text-[11.5px] text-[var(--color-muted)]">
        <Loader2 className="size-3.5 motion-safe:animate-spin" aria-hidden /> Saving…
      </span>
    );
  if (state === "saved")
    return (
      <span className="flex items-center gap-1.5 text-[11.5px] text-emerald-300">
        <Check className="size-3.5" aria-hidden /> Saved
      </span>
    );
  if (state === "error")
    return (
      <span className="flex items-center gap-1.5 text-[11.5px] text-amber-300">
        <TriangleAlert className="size-3.5" aria-hidden /> Not saved
      </span>
    );
  return null;
}
