# Themes

Four looks, chosen per user from the admin area (`#/admin/themes`) and saved to Lakebase (`user_prefs`, SPEC 13).

| Id | Look | Scheme |
|---|---|---|
| `midnight` | The original dark navy with lava accents. Default. | dark |
| `aurora` | Glassy dark, violet and teal glows, translucent blurred panels. | dark |
| `daylight` | Clean, bright, modern. | light |
| `databricks` | Lava and navy on warm oat surfaces. | light |

## How it is built

- Every colour, radius, shadow and glow is a CSS custom property in `frontend/src/index.css`.
  The `@theme` block holds the Midnight values, so Tailwind utilities keep working. Each other theme is a
  `[data-theme="<id>"]` block that overrides the same tokens.
- Light themes also remap the Tailwind palette families the components use for text (`slate`, `sky`, `amber`,
  `emerald`, `rose`, `teal`, `white`). Opacity-tinted backgrounds such as `bg-sky-400/10` are compiled to fixed
  values by Tailwind and are not remapped.
- The theme id is written to `<html data-theme>`. The pre-paint script in `frontend/index.html` reads
  `localStorage["sidekick.theme"]` before first paint. The server copy is applied after load (`GET /api/prefs`),
  so a first visit on a new browser can show the default for a moment.
- Server: `GET/PUT /api/prefs`, owner from `X-Forwarded-Email`, theme validated against `admin/prefs.py`
  `THEME_IDS`. Default for users with no saved choice: `APP_DEFAULT_THEME` (default `midnight`).
- A theme must define the same tokens as Midnight. `tests/app/test_themes.py` checks that, that the four ids
  match across the server, `lib/theme.ts` and `index.html`, and the contrast table below.

## Contrast (WCAG 2.x, text on surface)

Measured by `tests/app/test_themes.py` from the token values. AA for body text is 4.5:1. Every pair passes in
every theme.

| Pair | Midnight | Aurora | Daylight | Databricks |
|---|---|---|---|---|
| text on ink | 16.0 | 17.8 | 16.6 | 12.4 |
| text on panel | 14.8 | 15.9 | 17.9 | 13.6 |
| text on raised | 13.1 | 14.1 | 15.5 | 11.4 |
| muted on ink | 8.0 | 10.3 | 7.1 | 6.2 |
| muted on panel | 7.4 | 9.2 | 7.6 | 6.8 |
| muted on raised | 6.6 | 8.2 | 6.6 | 5.7 |
| positive chip text on panel | 11.3 | 11.7 | 5.5 | 6.4 |
| negative chip text on panel | 9.1 | 9.4 | 6.3 | 7.5 |
| warning chip text on panel | 12.0 | 12.8 | 7.1 | 7.5 |
| info chip text on panel | 9.6 | 9.5 | 6.7 | 7.9 |
| neutral chip text on panel | 7.4 | 9.2 | 7.6 | 6.8 |
| accent text on panel | 10.3 | 12.6 | 5.9 | 7.7 |
| white on primary button (both ends of the gradient) | 4.9 | 7.1 / 5.5 | 5.7 | 6.3 |

Aurora panels are translucent. The test composites them over the page ink before measuring.

## Pictures

Screenshots of the admin pages in each theme: `frontend/screenshots/admin-<theme>-<page>.png`
(themes, architecture, insights). Insights shows the non-admin message in the mock capture, because the mock
has no backend and no admin identity.

## Known gaps

- DM Sans is named first in the font stack but is not bundled. Without it the system sans-serif is used.
- Components that use hard-coded colours outside the token set (the voice orb, live-read chart colours) keep
  their dark-theme values.
