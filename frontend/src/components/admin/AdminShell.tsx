import type { ReactNode } from "react";
import { ArrowLeft, BarChart3, Layers, Palette } from "lucide-react";
import { Brand } from "../Brand";
import { adminHref, type AdminPage } from "../../lib/route";

const TABS: { page: AdminPage; label: string; icon: ReactNode; adminOnly?: boolean }[] = [
  { page: "themes", label: "Themes", icon: <Palette className="size-4" /> },
  { page: "architecture", label: "How it's built", icon: <Layers className="size-4" /> },
  { page: "insights", label: "Insights", icon: <BarChart3 className="size-4" />, adminOnly: true },
];

interface Props {
  page: AdminPage;
  isAdmin: boolean;
  children: ReactNode;
}

/** Shell for #/admin/*: brand, back link to the console, section tabs. Insights is hidden for non-admins. */
export function AdminShell({ page, isAdmin, children }: Props) {
  const tabs = TABS.filter((t) => isAdmin || !t.adminOnly);
  return (
    <div className="flex min-h-full flex-col">
      <header className="flex flex-wrap items-center gap-x-6 gap-y-2 border-b border-[var(--color-line)] bg-[var(--color-ink)]/80 px-5 py-3 backdrop-blur">
        <Brand />
        <nav aria-label="Admin sections" className="flex flex-wrap items-center gap-1">
          {tabs.map((t) => {
            const active = t.page === page;
            return (
              <a
                key={t.page}
                href={adminHref(t.page)}
                aria-current={active ? "page" : undefined}
                className={`inline-flex items-center gap-2 rounded-[var(--radius-control)] px-3 py-1.5 text-[13px] font-semibold transition-colors ${
                  active ? "bg-[var(--color-raised)] text-[var(--color-text)]" : "text-[var(--color-muted)] hover:text-[var(--color-text)]"
                }`}
              >
                {t.icon}
                {t.label}
              </a>
            );
          })}
        </nav>
        <a href="#" className="btn btn-ghost ml-auto py-1.5 text-[12.5px]">
          <ArrowLeft className="size-3.5" /> Back to Sidekick
        </a>
      </header>
      <main className="mx-auto w-full max-w-6xl flex-1 px-5 pb-12 pt-8">{children}</main>
    </div>
  );
}
