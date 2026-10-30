"""Preferences API (theme per user), /api/me and the admin allowlist (SPEC 13).

Owner scoping: the owner is always the proxy identity (X-Forwarded-Email), never a body field. Validation: only the
known theme ids are accepted. The store is memory here; the Lakebase DDL parity is in test_schema_parity.py.
"""

import asyncio

import pytest
from admin import prefs as P
from store.memory import MemoryStore
from test_hardening import build_client

ALICE = {"x-forwarded-email": "alice@example.com"}
BOB = {"x-forwarded-email": "bob@example.com"}


@pytest.fixture()
def deployed(monkeypatch):
    """databricks mode, so identity comes from the proxy headers; store and history stay in memory."""
    with build_client(monkeypatch, mode="databricks", store_backend="memory", history_backend="memory",
                      admin_emails="patrick.bayne@databricks.com") as c:
        yield c


# ----------------------------------------------------------------- pure functions

def test_only_known_themes_validate():
    for theme in P.THEME_IDS:
        assert P.validate_theme(theme) == theme
    for bad in ("", "Midnight", "dark", None, 3, ["aurora"]):
        with pytest.raises(ValueError):
            P.validate_theme(bad)


def test_unknown_configured_default_falls_back_to_midnight():
    assert P.resolve_default("aurora") == "aurora"
    assert P.resolve_default("neon") == P.DEFAULT_THEME
    assert P.resolve_default(None) == P.DEFAULT_THEME


def test_admin_allowlist_is_case_insensitive_and_never_admits_no_one():
    allow = P.parse_admin_emails(" Patrick.Bayne@databricks.com , ops@example.com ,")
    assert P.is_admin("patrick.bayne@DATABRICKS.com", allow)
    assert P.is_admin("ops@example.com", allow)
    assert not P.is_admin("someone@example.com", allow)
    assert not P.is_admin(None, allow)
    assert not P.is_admin("", frozenset())


# ----------------------------------------------------------------- store and owner scoping

def test_a_missing_preference_reads_as_the_default():
    store = MemoryStore()
    got = asyncio.run(P.get_theme(store, "alice@example.com", "aurora"))
    assert got == {"theme": "aurora", "updated_at_ms": None, "is_default": True}


def test_a_saved_preference_belongs_to_its_owner_only():
    store = MemoryStore()

    async def run():
        await P.set_theme(store, "alice@example.com", "daylight", 1_000)
        return (await P.get_theme(store, "alice@example.com", "midnight"),
                await P.get_theme(store, "bob@example.com", "midnight"))

    alice, bob = asyncio.run(run())
    assert alice["theme"] == "daylight" and alice["is_default"] is False
    assert bob["theme"] == "midnight" and bob["is_default"] is True


def test_set_theme_rejects_unknown_values_before_writing():
    store = MemoryStore()
    with pytest.raises(ValueError):
        asyncio.run(P.set_theme(store, "alice@example.com", "hot-pink", 1))
    assert store.prefs == {}


# ----------------------------------------------------------------- HTTP

def test_prefs_round_trip_for_the_caller(deployed):
    assert deployed.get("/api/prefs", headers=ALICE).json()["theme"] == "midnight"
    r = deployed.put("/api/prefs", json={"theme": "aurora"}, headers=ALICE)
    assert r.status_code == 200 and r.json()["theme"] == "aurora"
    assert deployed.get("/api/prefs", headers=ALICE).json()["theme"] == "aurora"
    # Another user does not see it.
    assert deployed.get("/api/prefs", headers=BOB).json()["theme"] == "midnight"


def test_an_unknown_theme_is_refused(deployed):
    r = deployed.put("/api/prefs", json={"theme": "hot-pink"}, headers=ALICE)
    assert r.status_code == 422
    assert deployed.get("/api/prefs", headers=ALICE).json()["is_default"] is True


def test_the_owner_comes_from_the_header_not_the_body(deployed):
    deployed.put("/api/prefs", json={"theme": "daylight", "owner": "bob@example.com"}, headers=ALICE)
    assert deployed.get("/api/prefs", headers=BOB).json()["theme"] == "midnight"
    assert deployed.get("/api/prefs", headers=ALICE).json()["theme"] == "daylight"


def test_no_identity_is_refused(deployed):
    assert deployed.get("/api/prefs").status_code == 403
    assert deployed.put("/api/prefs", json={"theme": "aurora"}).status_code == 403


def test_me_reports_admin_only_for_the_allowlist(deployed):
    admin = deployed.get("/api/me", headers={"x-forwarded-email": "Patrick.Bayne@databricks.com"}).json()
    assert admin == {"email": "Patrick.Bayne@databricks.com", "is_admin": True, "default_theme": "midnight"}
    assert deployed.get("/api/me", headers=ALICE).json()["is_admin"] is False
