"""Per-user preferences (theme). One row per owner in Lakebase `user_prefs`; the owner is the caller the Databricks
Apps proxy identifies (X-Forwarded-Email), never a value from the request body.

The theme list here is the server's source of truth. index.css, lib/theme.ts and the pre-paint script in
index.html must list the same ids (tests/app/test_themes.py checks that).
"""

from __future__ import annotations

from typing import Any, Optional, Protocol

THEME_IDS: tuple[str, ...] = ("midnight", "aurora", "daylight", "databricks")
DEFAULT_THEME = "midnight"


class PrefsStore(Protocol):
    async def get_prefs(self, owner: str) -> Optional[dict[str, Any]]: ...

    async def put_prefs(self, owner: str, theme: str, updated_at_ms: int) -> None: ...


def validate_theme(theme: Any) -> str:
    if not isinstance(theme, str) or theme not in THEME_IDS:
        raise ValueError(f"unknown theme {theme!r}; expected one of {', '.join(THEME_IDS)}")
    return theme


def resolve_default(configured: Optional[str]) -> str:
    """APP_DEFAULT_THEME, falling back to Midnight when unset or unknown (the setting is validated at startup)."""
    return configured if configured in THEME_IDS else DEFAULT_THEME


async def get_theme(store: PrefsStore, owner: str, default: str) -> dict[str, Any]:
    row = await store.get_prefs(owner)
    if row is None or row.get("theme") not in THEME_IDS:
        return {"theme": default, "updated_at_ms": None, "is_default": True}
    return {"theme": row["theme"], "updated_at_ms": row.get("updated_at_ms"), "is_default": False}


async def set_theme(store: PrefsStore, owner: str, theme: str, now_ms: int) -> dict[str, Any]:
    validate_theme(theme)
    await store.put_prefs(owner, theme, now_ms)
    return {"theme": theme, "updated_at_ms": now_ms, "is_default": False}


def is_admin(email: Optional[str], allowlist: frozenset[str]) -> bool:
    """ADMIN_EMAILS allowlist, compared case-insensitively. No caller is never an admin."""
    return bool(email) and email.strip().lower() in allowlist


def parse_admin_emails(raw: str) -> frozenset[str]:
    return frozenset(e.strip().lower() for e in raw.split(",") if e.strip())
