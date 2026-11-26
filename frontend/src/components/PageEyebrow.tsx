import type { ReactNode } from "react";

/** The small uppercase eyebrow above every standalone page's h1 (Start, Past calls, summary, admin pages). */
export function PageEyebrow({ children }: { children: ReactNode }) {
  return (
    <p className="kbd-label flex items-center gap-2">
      <span className="inline-block size-1.5 rounded-full bg-[var(--color-lava)]" aria-hidden />
      {children}
    </p>
  );
}
