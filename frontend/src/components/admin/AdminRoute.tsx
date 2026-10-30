import { useState } from "react";
import { saveTheme, useMe } from "../../lib/prefs";
import { applyTheme, cacheTheme, currentTheme, type ThemeId } from "../../lib/theme";
import type { AdminPage } from "../../lib/route";
import { AdminShell } from "./AdminShell";
import { ThemesPage } from "./ThemesPage";
import { ArchitecturePage } from "./ArchitecturePage";
import { InsightsPage } from "./InsightsPage";

interface Props {
  page: AdminPage;
}

/** The #/admin/* area. Themes and architecture are open to every user; Insights is admin only (AdminShell). */
export function AdminRoute({ page }: Props) {
  const me = useMe();
  const [theme, setTheme] = useState<ThemeId>(() => currentTheme());
  const [saving, setSaving] = useState(false);
  const [note, setNote] = useState<string | null>(null);

  const pick = (id: ThemeId) => {
    // Applied at once, so the choice is visible while the save is in flight. The cache keeps it across reloads
    // even if the save fails; the note says so.
    applyTheme(id);
    cacheTheme(id);
    setTheme(id);
    setSaving(true);
    setNote("Saving to your profile…");
    saveTheme(id)
      .then(() => setNote("Saved to your profile. It follows you to any browser."))
      .catch(() => setNote("Applied in this browser. Could not reach the server, so it is not saved to your profile yet."))
      .finally(() => setSaving(false));
  };

  return (
    <AdminShell page={page} isAdmin={me?.is_admin ?? false}>
      {page === "themes" && <ThemesPage active={theme} saving={saving} note={note} onPick={pick} />}
      {page === "architecture" && <ArchitecturePage />}
      {page === "insights" && me && (me.is_admin ? <InsightsPage /> : <p className="text-[var(--color-muted)]">Insights are for admins.</p>)}
    </AdminShell>
  );
}
