// Server-side preferences and identity: GET/PUT /api/prefs (theme), GET /api/me (email, is_admin).
// The theme is applied from the local cache at first paint (index.html), then reconciled with the saved copy.
import { useEffect, useState } from "react";
import { applyTheme, cacheTheme, cachedTheme, isThemeId, type ThemeId } from "./theme";
import { MOCK } from "./useCallSession";

export interface Me {
  email: string | null;
  is_admin: boolean;
  default_theme: ThemeId;
}

export interface SavedTheme {
  theme: ThemeId;
  is_default: boolean;
}

async function json<T>(path: string, init?: RequestInit): Promise<T> {
  const r = await fetch(path, { ...init, headers: { "content-type": "application/json", ...(init?.headers ?? {}) } });
  if (!r.ok) {
    const body = await r.json().catch(() => null);
    throw new Error(typeof body?.detail === "string" ? body.detail : `HTTP ${r.status}`);
  }
  return (await r.json()) as T;
}

export function getMe(): Promise<Me> {
  return json<Me>("/api/me");
}

export function getSavedTheme(): Promise<SavedTheme> {
  return json<SavedTheme>("/api/prefs");
}

export function saveTheme(theme: ThemeId): Promise<SavedTheme> {
  return json<SavedTheme>("/api/prefs", { method: "PUT", body: JSON.stringify({ theme }) });
}

/** Who the caller is. Offline or in mock mode: not an admin, default theme. */
export function useMe(): Me | null {
  const [me, setMe] = useState<Me | null>(null);
  useEffect(() => {
    if (MOCK) {
      setMe({ email: null, is_admin: false, default_theme: "midnight" });
      return;
    }
    let alive = true;
    getMe()
      .then((m) => alive && setMe(m))
      .catch(() => alive && setMe({ email: null, is_admin: false, default_theme: "midnight" }));
    return () => {
      alive = false;
    };
  }, []);
  return me;
}

/** Applies the saved theme once on load. A failed read keeps whatever the page already shows. */
export function useSavedTheme(): void {
  useEffect(() => {
    if (MOCK) return;
    let alive = true;
    getSavedTheme()
      .then((s) => {
        if (!alive || !isThemeId(s.theme)) return;
        if (s.theme !== cachedTheme() || s.theme !== document.documentElement.getAttribute("data-theme")) {
          applyTheme(s.theme);
          cacheTheme(s.theme);
        }
      })
      .catch(() => undefined);
    return () => {
      alive = false;
    };
  }, []);
}
