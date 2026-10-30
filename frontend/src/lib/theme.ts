// Themes: the catalogue, the page-level apply, and the local cache that index.html reads before first paint.
// The per-user choice is saved on the server (GET/PUT /api/prefs) and mirrored here.
// Token values live in index.css (one [data-theme] block per theme; docs/THEMES.md has the contrast table).

export const THEME_IDS = ["midnight", "aurora", "daylight", "databricks"] as const;
export type ThemeId = (typeof THEME_IDS)[number];

export interface ThemeInfo {
  id: ThemeId;
  label: string;
  blurb: string;
  scheme: "dark" | "light";
}

export const THEMES: ThemeInfo[] = [
  { id: "midnight", label: "Midnight", blurb: "The original dark navy with lava accents.", scheme: "dark" },
  { id: "aurora", label: "Aurora", blurb: "Glassy dark with violet and teal glows and blurred panels.", scheme: "dark" },
  { id: "daylight", label: "Daylight", blurb: "Clean, bright and modern. Easy to read in a lit room.", scheme: "light" },
  { id: "databricks", label: "Databricks", blurb: "Lava and navy on warm oat surfaces.", scheme: "light" },
];

export const DEFAULT_THEME: ThemeId = "midnight";
const CACHE_KEY = "sidekick.theme";

export function isThemeId(v: unknown): v is ThemeId {
  return typeof v === "string" && (THEME_IDS as readonly string[]).includes(v);
}

export function currentTheme(): ThemeId {
  const v = document.documentElement.getAttribute("data-theme");
  return isThemeId(v) ? v : DEFAULT_THEME;
}

/** Applies a theme to the whole page. */
export function applyTheme(id: ThemeId): void {
  document.documentElement.setAttribute("data-theme", id);
}

export function cacheTheme(id: ThemeId): void {
  try {
    localStorage.setItem(CACHE_KEY, id);
  } catch {
    /* storage disabled: the server copy still applies on the next load */
  }
}

export function cachedTheme(): ThemeId | null {
  try {
    const v = localStorage.getItem(CACHE_KEY);
    return isThemeId(v) ? v : null;
  } catch {
    return null;
  }
}
