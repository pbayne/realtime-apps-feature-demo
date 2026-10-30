import { useState } from "react";
import { Check, Copy, Mail } from "lucide-react";
import { copyText } from "../../lib/clipboard";
import { OUTCOME_LABEL, OUTCOME_ORDER, outcomesToEmail } from "../../lib/voice/email";
import { newestItemId } from "../../lib/voice/state";
import type { VoiceItem } from "../../lib/types";

interface Props {
  items: VoiceItem[];
  /** Inside a collapsible in the conversation column: no outer panel. */
  compact?: boolean;
}

export function Outcomes({ items, compact = false }: Props) {
  const [copied, setCopied] = useState<string | null>(null);
  const newest = newestItemId(items);

  const flash = (key: string) => {
    setCopied(key);
    window.setTimeout(() => setCopied((cur) => (cur === key ? null : cur)), 1600);
  };

  const copyItem = async (item: VoiceItem) => {
    if (await copyText(item.text)) flash(item.item_id);
  };

  const copyAll = async () => {
    if (!items.length) return;
    if (await copyText(outcomesToEmail(items))) flash("__all");
  };

  const body = (
    <>
      <div className="flex items-center justify-between gap-2 border-b border-[var(--color-line)] px-4 py-3">
        <div className="min-w-0">
          <h2 className="text-[14px] font-semibold">Outcomes</h2>
          <p className="text-[11.5px] text-[var(--color-muted)]">
            {items.length ? `${items.length} item${items.length === 1 ? "" : "s"} from this session` : "Filed as Sidekick answers"}
          </p>
        </div>
        <button type="button" onClick={copyAll} disabled={!items.length} className="btn btn-ghost shrink-0 py-1.5 text-[12px]">
          {copied === "__all" ? <Check className="size-3.5 text-emerald-300" /> : <Mail className="size-3.5" />}
          {copied === "__all" ? "Copied" : "Copy all as follow-up email"}
        </button>
      </div>

      <div className="scroll-thin min-h-0 flex-1 space-y-4 overflow-y-auto px-4 py-3.5">
        {items.length === 0 && (
          <div className="rounded-xl border border-dashed border-[var(--color-line)] px-4 py-8 text-center text-[13px] text-[var(--color-muted)]">
            Action items, follow-up questions and summaries appear here after each turn.
          </div>
        )}
        {OUTCOME_ORDER.map((kind) => {
          const group = items.filter((i) => i.kind === kind);
          if (!group.length) return null;
          return (
            <section key={kind}>
              <h3 className="mb-1.5 flex items-center justify-between text-[10.5px] font-semibold uppercase tracking-[0.09em] text-sky-200/80">
                {OUTCOME_LABEL[kind]}
                <span className="font-mono text-[var(--color-muted)]">{group.length}</span>
              </h3>
              <ul className="space-y-1.5">
                {group.map((item) => {
                  const isNew = item.item_id === newest;
                  return (
                    <li
                      key={item.item_id}
                      className={`anim-card-in rounded-lg border px-3 py-2 ${
                        isNew ? "border-sky-400/50 bg-sky-400/[0.08] ring-1 ring-sky-400/25" : "border-[var(--color-line)] bg-[var(--color-raised)]/60"
                      }`}
                    >
                      <div className="flex items-start gap-2">
                        <p className="min-w-0 flex-1 text-[13.5px] leading-snug text-slate-100">{item.text}</p>
                        <button
                          type="button"
                          onClick={() => void copyItem(item)}
                          aria-label="Copy item"
                          title="Copy"
                          className="grid size-7 shrink-0 place-items-center rounded-md text-[var(--color-muted)] hover:bg-white/[0.06] hover:text-slate-100"
                        >
                          {copied === item.item_id ? <Check className="size-3.5 text-emerald-300" /> : <Copy className="size-3.5" />}
                        </button>
                      </div>
                      {(item.owner || item.due || item.source_doc || isNew) && (
                        <div className="mt-1.5 flex flex-wrap items-center gap-1.5">
                          {isNew && <span className="chip border-sky-400/40 bg-sky-400/15 text-sky-100">newest</span>}
                          {item.owner && <span className="chip chip-neutral">owner: {item.owner}</span>}
                          {item.due && <span className="chip chip-neutral">due: {item.due}</span>}
                          {item.source_doc && <span className="truncate font-mono text-[10.5px] text-[var(--color-muted)]">{item.source_doc}</span>}
                        </div>
                      )}
                    </li>
                  );
                })}
              </ul>
            </section>
          );
        })}
      </div>
    </>
  );

  if (compact) return <div className="flex flex-col">{body}</div>;
  return <section className="panel flex h-full min-h-0 flex-col overflow-hidden">{body}</section>;
}
