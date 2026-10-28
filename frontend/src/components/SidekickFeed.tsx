import { useState } from "react";
import { Check, ChevronDown, Pencil, Sparkles, X, FileText, Pin } from "lucide-react";
import { KIND_META, confidenceColor } from "../lib/cardMeta";
import { KIND_LABEL } from "../lib/format";
import type { CallState, CardView } from "../lib/store";

const VISIBLE = 5;

interface Props {
  state: CallState;
  onResolve(cardId: string, action: "accept" | "reject" | "edit", text?: string): void;
}

export function SidekickFeed({ state, onResolve }: Props) {
  const [showOlder, setShowOlder] = useState(false);

  const all = state.cardOrder.map((id) => state.cards[id]).filter(Boolean) as CardView[];
  // Compliance is pinned until resolved; everything else is newest-first.
  const pinned = all.filter((c) => c.card.kind === "compliance" && !c.resolution).reverse();
  const rest = all.filter((c) => !(c.card.kind === "compliance" && !c.resolution)).reverse();
  const visible = rest.slice(0, VISIBLE);
  const older = rest.slice(VISIBLE);
  const live = state.status === "live" || state.status === "connecting" || state.status === "reconnecting";

  return (
    <section className="panel flex h-full min-h-0 flex-col overflow-hidden">
      <header className="flex items-center justify-between border-b border-[var(--color-line)] px-4 py-3">
        <div>
          <h2 className="flex items-center gap-2 text-[14px] font-semibold">
            <Sparkles className="size-4 text-sky-300" /> Sidekick feed
          </h2>
          <p className="text-[11.5px] text-[var(--color-muted)]">
            {all.length === 0 ? "Silent until there is something grounded to say" : `${all.length} suggestion${all.length === 1 ? "" : "s"} this call`}
          </p>
        </div>
        <span className="chip chip-neutral">
          {all.some((c) => c.streaming) ? (
            <>
              <span className="anim-blink size-1.5 rounded-full bg-sky-300" /> composing
            </>
          ) : live ? (
            "listening"
          ) : (
            "idle"
          )}
        </span>
      </header>

      <div className="scroll-thin min-h-0 flex-1 space-y-3 overflow-y-auto px-3.5 py-3.5">
        {all.length === 0 && (
          <div className="rounded-xl border border-dashed border-[var(--color-line)] px-4 py-8 text-center text-[13px] text-[var(--color-muted)]">
            <Sparkles className="mx-auto mb-2 size-5 text-slate-500" />
            Cards appear here after a customer turn, each with its source and a confidence score.
          </div>
        )}

        {pinned.map((c) => (
          <CardItem key={c.card.card_id} view={c} onResolve={onResolve} />
        ))}

        {visible.map((c) => (
          <CardItem key={c.card.card_id} view={c} onResolve={onResolve} />
        ))}

        {older.length > 0 && (
          <div>
            <button
              type="button"
              onClick={() => setShowOlder((v) => !v)}
              className="flex w-full items-center justify-center gap-1.5 rounded-lg py-2 text-[12px] font-medium text-[var(--color-muted)] hover:bg-white/[0.03] hover:text-slate-200"
            >
              <ChevronDown className={`size-3.5 transition-transform ${showOlder ? "rotate-180" : ""}`} />
              {showOlder ? "Hide" : "Show"} {older.length} earlier
            </button>
            {showOlder && (
              <div className="mt-2 space-y-2 opacity-80">
                {older.map((c) => (
                  <CardItem key={c.card.card_id} view={c} onResolve={onResolve} compact />
                ))}
              </div>
            )}
          </div>
        )}
      </div>
    </section>
  );
}

function CardItem({
  view,
  onResolve,
  compact = false,
}: {
  view: CardView;
  onResolve: Props["onResolve"];
  compact?: boolean;
}) {
  const { card, body, streaming, resolution, editedText } = view;
  const meta = KIND_META[card.kind];
  const Icon = meta.Icon;
  const isCompliance = card.kind === "compliance";
  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState("");
  const [collapsed, setCollapsed] = useState(false);
  const resolved = !!resolution;
  const displayBody = editedText ?? body;

  const startEdit = () => {
    setDraft(displayBody);
    setEditing(true);
  };
  const saveEdit = () => {
    const text = draft.trim();
    if (!text) return;
    onResolve(card.card_id, "edit", text);
    setEditing(false);
  };

  if (compact || (resolved && resolution === "rejected") || collapsed) {
    return (
      <article className="anim-card-in flex items-center gap-2.5 rounded-xl border border-[var(--color-line)] bg-[var(--color-raised)]/50 px-3 py-2">
        <Icon className="size-3.5 shrink-0" style={{ color: meta.color }} />
        <span className="min-w-0 flex-1 truncate text-[12.5px] text-slate-300">{card.title}</span>
        <span className="text-[10.5px] uppercase tracking-wider text-[var(--color-muted)]">
          {resolution ?? "shown"}
        </span>
        {!compact && (
          <button type="button" onClick={() => setCollapsed(false)} className="text-[11px] text-[var(--color-muted)] hover:text-slate-200">
            open
          </button>
        )}
      </article>
    );
  }

  return (
    <article
      className={`anim-card-in relative overflow-hidden rounded-xl border bg-[var(--color-raised)] p-3.5 transition-colors ${
        isCompliance && !resolved
          ? "anim-pinned border-rose-500/50 bg-rose-950/30"
          : "border-[var(--color-line)]"
      }`}
    >
      <span className="absolute inset-y-0 left-0 w-[3px]" style={{ background: meta.color }} aria-hidden />

      <div className="flex items-start gap-3">
        <div className="grid size-8 shrink-0 place-items-center rounded-lg" style={{ background: meta.tint, color: meta.color }}>
          <Icon className="size-4" />
        </div>
        <div className="min-w-0 flex-1">
          <div className="flex items-center gap-2">
            <span className="text-[10.5px] font-semibold uppercase tracking-[0.09em]" style={{ color: meta.color }}>
              {KIND_LABEL[card.kind]}
            </span>
            {isCompliance && !resolved && (
              <span className="chip border-rose-500/40 bg-rose-500/15 text-rose-200">
                <Pin className="size-2.5" /> pinned
              </span>
            )}
            <span className="ml-auto text-[10.5px] text-[var(--color-muted)]">seq {card.seq}</span>
          </div>
          <h3 className="mt-0.5 text-[14.5px] font-semibold leading-snug text-white">{card.title}</h3>
        </div>
      </div>

      {editing ? (
        <div className="mt-3">
          <textarea
            value={draft}
            onChange={(e) => setDraft(e.target.value)}
            rows={3}
            className="w-full resize-none rounded-lg border border-sky-400/40 bg-[var(--color-ink)] p-2.5 text-[13.5px] leading-relaxed outline-none focus:border-sky-300"
            autoFocus
          />
          <div className="mt-2 flex justify-end gap-2">
            <button type="button" className="btn btn-ghost py-1.5" onClick={() => setEditing(false)}>
              Cancel
            </button>
            <button type="button" className="btn btn-primary py-1.5" onClick={saveEdit} disabled={!draft.trim()}>
              <Check className="size-3.5" /> Save & send
            </button>
          </div>
        </div>
      ) : (
        <>
          <div className="mt-2.5 min-h-[20px] text-[13.5px] leading-relaxed text-slate-300">
            {streaming && !displayBody ? (
              <div className="space-y-1.5 py-1">
                <div className="skeleton h-2.5 w-11/12" />
                <div className="skeleton h-2.5 w-3/5" />
              </div>
            ) : (
              <p>
                {displayBody}
                {streaming && <span className="anim-blink ml-0.5 inline-block h-3.5 w-[2px] translate-y-0.5 bg-sky-300" aria-hidden />}
              </p>
            )}
          </div>

          <div className="mt-3 flex items-center gap-2">
            <ConfidenceBar value={card.confidence} />
            {card.trigger && <span className="chip chip-neutral truncate">{card.trigger}</span>}
          </div>

          {(card.source_doc || card.source_clause) && (
            <div className="mt-2.5 flex items-start gap-1.5 rounded-lg bg-[var(--color-ink)]/70 px-2.5 py-1.5 text-[11.5px] text-[var(--color-muted)]">
              <FileText className="mt-px size-3.5 shrink-0" />
              <span className="min-w-0 break-words">
                <span className="font-mono text-slate-300">{card.source_doc ?? "source"}</span>
                {card.source_clause && <span> · {card.source_clause}</span>}
              </span>
            </div>
          )}

          {resolved ? (
            <div className="mt-3 flex items-center gap-2 text-[12px]">
              <span
                className={`chip ${
                  resolution === "accepted" ? "chip-pos" : resolution === "edited" ? "chip-intent" : "chip-neutral"
                }`}
              >
                {resolution === "accepted" ? <Check className="size-3" /> : resolution === "edited" ? <Pencil className="size-3" /> : <X className="size-3" />}
                {resolution === "accepted" ? "Accepted" : resolution === "edited" ? "Edited & sent" : "Dismissed"}
              </span>
              <button type="button" onClick={() => setCollapsed(true)} className="text-[11.5px] text-[var(--color-muted)] hover:text-slate-200">
                collapse
              </button>
            </div>
          ) : (
            <div className="mt-3 grid grid-cols-3 gap-2">
              <button
                type="button"
                disabled={streaming}
                onClick={() => onResolve(card.card_id, "accept")}
                className="btn border border-emerald-400/30 bg-emerald-400/10 py-1.5 text-emerald-200 hover:bg-emerald-400/20"
              >
                <Check className="size-3.5" /> Accept
              </button>
              <button
                type="button"
                disabled={streaming}
                onClick={startEdit}
                className="btn btn-ghost py-1.5"
              >
                <Pencil className="size-3.5" /> Edit
              </button>
              <button
                type="button"
                disabled={streaming}
                onClick={() => onResolve(card.card_id, "reject")}
                className="btn btn-ghost py-1.5 text-[var(--color-muted)]"
              >
                <X className="size-3.5" /> Dismiss
              </button>
            </div>
          )}
        </>
      )}
    </article>
  );
}

function ConfidenceBar({ value }: { value: number }) {
  const pct = Math.round(value * 100);
  const color = confidenceColor(value);
  return (
    <div className="flex min-w-0 flex-1 items-center gap-2" title={`confidence ${pct}%`}>
      <span className="text-[10.5px] uppercase tracking-wider text-[var(--color-muted)]">conf</span>
      <div className="h-1.5 flex-1 overflow-hidden rounded-full bg-white/[0.07]">
        <div className="h-full rounded-full transition-[width] duration-500" style={{ width: `${pct}%`, background: color }} />
      </div>
      <span className="w-8 text-right font-mono text-[11px] text-slate-300">{pct}%</span>
    </div>
  );
}
