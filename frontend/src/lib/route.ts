// Hash routes for the admin area: #/admin/themes | #/admin/architecture | #/admin/insights.
// The call console and start screen have no route of their own (they render when this is not an admin route).
import { useEffect, useState } from "react";

export const ADMIN_PAGES = ["themes", "architecture", "insights"] as const;
export type AdminPage = (typeof ADMIN_PAGES)[number];

export function parseAdminRoute(hash: string): AdminPage | null {
  const m = /^#\/admin\/([a-z]+)/.exec(hash);
  if (!m) return null;
  const page = m[1] as AdminPage;
  return (ADMIN_PAGES as readonly string[]).includes(page) ? page : null;
}

export function adminHref(page: AdminPage): string {
  return `#/admin/${page}`;
}

/** The Past Calls history route (#/history). */
export function useHistoryRoute(): boolean {
  const [on, setOn] = useState<boolean>(() => window.location.hash.startsWith("#/history"));
  useEffect(() => {
    const onChange = () => setOn(window.location.hash.startsWith("#/history"));
    window.addEventListener("hashchange", onChange);
    return () => window.removeEventListener("hashchange", onChange);
  }, []);
  return on;
}

export const HISTORY_HREF = "#/history";

/** Current admin page from the URL hash, or null outside the admin area. Re-renders on hash changes. */
export function useAdminRoute(): AdminPage | null {
  const [page, setPage] = useState<AdminPage | null>(() => parseAdminRoute(window.location.hash));
  useEffect(() => {
    const onChange = () => setPage(parseAdminRoute(window.location.hash));
    window.addEventListener("hashchange", onChange);
    return () => window.removeEventListener("hashchange", onChange);
  }, []);
  return page;
}
