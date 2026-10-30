"""The deployed app must serve the built UI at / (a hardening refactor once dropped the static mount)."""

import main
from fastapi.testclient import TestClient


def test_root_serves_the_built_ui():
    assert (main.STATIC_DIR / "index.html").exists(), "run `bun run build` in frontend/"
    with TestClient(main.app) as client:
        r = client.get("/")
        assert r.status_code == 200
        assert "<div id=\"root\"" in r.text or "id=root" in r.text or "<script" in r.text
        assert client.get("/pcm-worklet.js").status_code == 200
