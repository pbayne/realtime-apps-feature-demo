import { useState } from "react";
import { applyTheme, cacheTheme, currentTheme, type ThemeId } from "../../lib/theme";
import type { AdminPage } from "../../lib/route";
import { AdminShell } from "./AdminShell";
import { ThemesPage } from "./ThemesPage";

interface Props {
  page: AdminPage;
}

/** The #/admin/* area. Every page is reachable for every user except Insights (admin only, see AdminShell). */
export function AdminRoute({ page }: Props) {
  const [theme, setTheme] = useState<ThemeId>(() => currentTheme());

  const pick = (id: ThemeId) => {
    applyTheme(id);
    cacheTheme(id);
    setTheme(id);
  };

  return (
    <AdminShell page={page} isAdmin={false}>
      {page === "themes" && <ThemesPage active={theme} saving={false} note={null} onPick={pick} />}
    </AdminShell>
  );
}
