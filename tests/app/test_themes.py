"""Theme tokens: every theme must define the same tokens, keep text at WCAG AA (4.5:1) on its surfaces, and be
known to the server, the inline pre-paint script, and the picker. Reads the built source files, not a copy."""

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
CSS = (ROOT / "frontend/src/index.css").read_text(encoding="utf-8")
THEME_TS = (ROOT / "frontend/src/lib/theme.ts").read_text(encoding="utf-8")
INDEX_HTML = (ROOT / "frontend/index.html").read_text(encoding="utf-8")

THEMES = ["midnight", "aurora", "daylight", "databricks"]

# Text pairs that must reach AA (4.5:1). Surfaces are the page ink, the panel, and the raised bubble.
TEXT_ON_SURFACE = [
    ("--color-text", "--color-ink"),
    ("--color-text", "--color-panel"),
    ("--color-text", "--color-raised"),
    ("--color-muted", "--color-ink"),
    ("--color-muted", "--color-panel"),
    ("--color-muted", "--color-raised"),
    ("--c-pos", "--color-panel"),
    ("--c-neg", "--color-panel"),
    ("--c-warn", "--color-panel"),
    ("--c-info", "--color-panel"),
    ("--c-neutral", "--color-panel"),
    ("--c-accent", "--color-panel"),
]


def theme_block(theme: str) -> dict[str, str]:
    """Declarations of the theme's own block, plus the ones of blocks that list it as well (shared palettes)."""
    decls: dict[str, str] = {}
    css = re.sub(r"/\*.*?\*/", "", CSS, flags=re.S)
    for selectors, body in re.findall(r"([^{}]+)\{([^{}]*)\}", css):
        sels = [s.strip() for s in selectors.split(",")]
        sels = [s.split("\n")[-1].strip() for s in sels]
        if any(s == f'[data-theme="{theme}"]' or (theme == "midnight" and s in (":root", "@theme")) for s in sels):
            for name, value in re.findall(r"(--[\w-]+)\s*:\s*([^;]+);", body):
                decls[name] = value.strip()
    return decls


def parse_color(value: str) -> tuple[float, float, float, float]:
    value = value.strip()
    if value.startswith("#"):
        h = value[1:]
        if len(h) == 3:
            h = "".join(c * 2 for c in h)
        return (int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16), 1.0)
    m = re.fullmatch(r"rgba?\(\s*([\d.]+)\s*,\s*([\d.]+)\s*,\s*([\d.]+)\s*(?:,\s*([\d.]+)\s*)?\)", value)
    if not m:
        raise ValueError(f"unsupported colour {value!r}")
    r, g, b = (float(m.group(i)) for i in (1, 2, 3))
    a = float(m.group(4)) if m.group(4) else 1.0
    return (r, g, b, a)


def over(fg: tuple[float, ...], bg: tuple[float, float, float]) -> tuple[float, float, float]:
    a = fg[3]
    return tuple(fg[i] * a + bg[i] * (1 - a) for i in range(3))  # type: ignore[return-value]


def luminance(rgb: tuple[float, float, float]) -> float:
    def lin(c: float) -> float:
        c /= 255
        return c / 12.92 if c <= 0.03928 else ((c + 0.055) / 1.055) ** 2.4

    r, g, b = (lin(c) for c in rgb)
    return 0.2126 * r + 0.7152 * g + 0.0722 * b


def contrast(fg: tuple[float, float, float], bg: tuple[float, float, float]) -> float:
    hi, lo = sorted((luminance(fg), luminance(bg)), reverse=True)
    return (hi + 0.05) / (lo + 0.05)


def resolve(decls: dict[str, str], name: str, depth: int = 0) -> tuple[float, float, float, float]:
    value = decls[name]
    m = re.fullmatch(r"var\((--[\w-]+)\)", value)
    if m and depth < 5:
        return resolve(decls, m.group(1), depth + 1)
    return parse_color(value)


def test_known_themes_match_across_files():
    for theme in THEMES:
        assert f'"{theme}"' in THEME_TS, f"{theme} missing from lib/theme.ts"
        assert theme in INDEX_HTML, f"{theme} missing from the pre-paint script in index.html"
        assert f'[data-theme="{theme}"]' in CSS or theme == "midnight", theme


@pytest.mark.parametrize("theme", THEMES)
def test_every_theme_defines_the_same_tokens(theme):
    base = {k for k in theme_block("midnight") if not k.startswith("--font")}
    assert base - set(theme_block(theme)) == set(), f"{theme} is missing tokens"


@pytest.mark.parametrize("theme", THEMES)
def test_text_meets_wcag_aa(theme):
    decls = theme_block(theme)
    ink = over(resolve(decls, "--color-ink"), (0.0, 0.0, 0.0))[:3]
    failures = []
    for fg_name, bg_name in TEXT_ON_SURFACE:
        # A translucent surface (Aurora panels) is composited over the page ink first.
        surface = over(resolve(decls, bg_name), ink)
        fg = over(resolve(decls, fg_name), surface)
        ratio = contrast(fg, surface)
        if ratio < 4.5:
            failures.append(f"{fg_name} on {bg_name}: {ratio:.2f}")
    assert not failures, failures


@pytest.mark.parametrize("theme", THEMES)
def test_primary_button_text_meets_aa(theme):
    decls = theme_block(theme)
    white = (255.0, 255.0, 255.0)
    for name in ("--accent-btn-from", "--accent-btn-to"):
        assert contrast(white, resolve(decls, name)[:3]) >= 4.5, (theme, name)  # type: ignore[arg-type]
