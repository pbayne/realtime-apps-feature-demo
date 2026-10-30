import { Check } from "lucide-react";
import { THEMES, type ThemeId } from "../../lib/theme";

interface Props {
  active: ThemeId;
  saving: boolean;
  note: string | null;
  onPick(id: ThemeId): void;
}

/** Picker with a live preview per theme. Each preview sets its own data-theme, so it shows that theme's tokens
 *  while the rest of the page keeps the active one. */
export function ThemesPage({ active, saving, note, onPick }: Props) {
  return (
    <section aria-labelledby="themes-h">
      <p className="kbd-label">Appearance</p>
      <h1 id="themes-h" className="mt-2 text-[28px] font-semibold leading-tight tracking-tight">
        Theme
      </h1>
      <p className="mt-2 max-w-xl text-[14px] leading-relaxed text-[var(--color-muted)]">
        Pick a look. It applies to the whole app at once and is saved to your profile, so it follows you to any browser.
      </p>
      {note && (
        <p role="status" className="mt-3 text-[12.5px] text-[var(--color-muted)]">
          {note}
        </p>
      )}

      <div role="radiogroup" aria-label="Theme" className="mt-6 grid gap-4 sm:grid-cols-2">
        {THEMES.map((t) => {
          const selected = t.id === active;
          return (
            <button
              key={t.id}
              type="button"
              role="radio"
              aria-checked={selected}
              disabled={saving && !selected}
              onClick={() => onPick(t.id)}
              className={`group rounded-[var(--radius-panel)] border p-3 text-left transition-colors focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--c-accent)] ${
                selected ? "border-[var(--c-accent)] bg-[var(--color-panel)]" : "border-[var(--color-line)] bg-[var(--color-panel)] hover:border-[var(--color-muted)]"
              }`}
            >
              <ThemePreview id={t.id} />
              <div className="mt-3 flex items-start justify-between gap-3 px-1">
                <div>
                  <div className="text-[15px] font-semibold text-[var(--color-text)]">{t.label}</div>
                  <div className="mt-0.5 text-[12.5px] leading-relaxed text-[var(--color-muted)]">{t.blurb}</div>
                </div>
                <span
                  className={`mt-0.5 grid size-5 shrink-0 place-items-center rounded-full border ${
                    selected ? "border-[var(--c-accent)] bg-[var(--c-accent)] text-[var(--color-ink)]" : "border-[var(--color-line)]"
                  }`}
                  aria-hidden
                >
                  {selected && <Check className="size-3.5" />}
                </span>
              </div>
            </button>
          );
        })}
      </div>
    </section>
  );
}

/** A miniature of the call console, drawn with that theme's own tokens. */
export function ThemePreview({ id }: { id: ThemeId }) {
  return (
    <div data-theme={id} className="relative overflow-hidden rounded-[calc(var(--radius-panel)-4px)] border border-[var(--color-line)] bg-[var(--color-ink)] p-3" aria-hidden>
      <div className="flex items-center gap-2">
        <span className="grid size-5 place-items-center rounded-md bg-[var(--color-lava)]">
          <span className="block size-1.5 rounded-sm bg-white/90" />
        </span>
        <span className="text-[12px] font-semibold text-[var(--color-text)]">Call Sidekick</span>
        <span className="chip chip-pos ml-auto">Positive</span>
      </div>
      <div className="panel mt-2.5 space-y-2 p-2.5">
        <p className="text-[11.5px] text-[var(--color-muted)]">Customer: &ldquo;Why did the fee go up?&rdquo;</p>
        <div className="flex items-center gap-2">
          <span className="chip chip-objection">Fee objection</span>
          <span className="chip chip-neutral">Methodology 2/4</span>
        </div>
        <div className="flex justify-end">
          <span className="btn btn-primary px-2.5 py-1 text-[11px]">Accept</span>
        </div>
      </div>
    </div>
  );
}
