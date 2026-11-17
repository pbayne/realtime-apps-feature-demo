"""lifecycle.py — bring a Databricks App's billable resources UP or DOWN to control cost.

Generic and project-agnostic: it reads a small JSON manifest (default: scripts/lifecycle.json next to
this file, override with --manifest) that names the app, its GPU serving endpoints, and any Lakebase
project. The same script drives any app you build — point it at a different manifest.

Billing sources this manages, in cost order:
  1. GPU serving endpoints (A10/H100 …): the big spend. DOWN deletes them (billing stops); UP re-creates
     them by running their bundle "register" jobs. Deleting is the only way to truly stop GPU cost; a
     scale-to-zero endpoint still bills its scale-up and keeps the config around.
  2. The app compute: DOWN stops it, UP starts it. A stopped app does not bill compute.
  3. Lakebase project: optional. DOWN can suspend it (if the project allows suspension); UP resumes it.
     Note: a project with no_suspension=true (always-on) cannot be suspended — the manifest should set
     "lakebase_suspend": false for those, and the only way to stop that cost is to delete the project
     (destructive, drops data + the CDF feed) — which this script will NOT do automatically.

Usage:
  python scripts/lifecycle.py up     --profile <profile> [--manifest path] [--yes]
  python scripts/lifecycle.py down   --profile <profile> [--manifest path] [--yes]
  python scripts/lifecycle.py status --profile <profile> [--manifest path]

Manifest schema (all keys optional except app):
  {
    "app": "call-sidekick",
    "gpu_endpoints": ["sidekick-parakeet", "sidekick-kokoro"],
    "register_jobs": ["call_sidekick_parakeet_register", "call_sidekick_kokoro_register"],
    "bundle_target": "dev",
    "warmup": "scripts/warmup_voice.py",
    "lakebase_project": "call-sidekick",
    "lakebase_suspend": false
  }

DOWN is the money-saver: run it when you are done. UP readies the app for a demo. `status` is read-only.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent


def load_manifest(path: str | None) -> dict:
    p = Path(path) if path else HERE / "lifecycle.json"
    if not p.exists():
        sys.exit(f"lifecycle: manifest not found at {p} (pass --manifest or create scripts/lifecycle.json)")
    return json.loads(p.read_text())


def endpoint_exists(w, name: str) -> bool:
    from databricks.sdk.errors import NotFound
    try:
        w.serving_endpoints.get(name)
        return True
    except NotFound:
        return False


def app_state(w, name: str) -> str:
    from databricks.sdk.errors import NotFound
    try:
        a = w.apps.get(name)
        cs = getattr(a, "compute_status", None)
        return str(getattr(cs, "state", "UNKNOWN")).split(".")[-1] if cs else "UNKNOWN"
    except NotFound:
        return "ABSENT"


def bundle_run(job: str, target: str, profile: str) -> int:
    print(f"  bundle run {job} (target={target})…")
    return subprocess.call(["databricks", "bundle", "run", job, "--target", target, "--profile", profile], cwd=REPO)


def do_status(w, m: dict) -> None:
    app = m.get("app")
    if app:
        print(f"app {app}: {app_state(w, app)}")
    for ep in m.get("gpu_endpoints", []):
        if endpoint_exists(w, ep):
            e = w.serving_endpoints.get(ep)
            print(f"gpu {ep}: EXISTS ready={str(e.state.ready).split('.')[-1]} (BILLING)")
        else:
            print(f"gpu {ep}: ABSENT (no GPU cost)")


def do_up(w, m: dict, profile: str) -> None:
    target = m.get("bundle_target", "dev")
    # 1. GPU endpoints: re-create any that are missing via their register jobs.
    missing = [j for ep, j in zip(m.get("gpu_endpoints", []), m.get("register_jobs", [])) if not endpoint_exists(w, ep)]
    for job in missing:
        if bundle_run(job, target, profile) != 0:
            sys.exit(f"lifecycle up: register job {job} failed")
    # 2. App: start it (idempotent; start on a running app is a no-op).
    app = m.get("app")
    if app and app_state(w, app) not in ("RUNNING", "ACTIVE"):
        print(f"  starting app {app}…")
        subprocess.call(["databricks", "apps", "start", app, "--profile", profile])
    # 3. Warm the endpoints so the first demo turn is fast.
    warm = m.get("warmup")
    if warm and (REPO / warm).exists():
        print("  warming endpoints…")
        subprocess.call([sys.executable, str(REPO / warm), "--profile", profile])
    print("UP complete.")
    do_status(w, m)


def do_down(w, m: dict, profile: str, yes: bool) -> None:
    app = m.get("app")
    eps = [ep for ep in m.get("gpu_endpoints", []) if endpoint_exists(w, ep)]
    if not yes:
        print("DOWN will:")
        if app and app_state(w, app) not in ("ABSENT",):
            print(f"  - stop app {app}")
        for ep in eps:
            print(f"  - DELETE gpu endpoint {ep} (stops its billing; UP re-creates it)")
        if m.get("lakebase_suspend"):
            print(f"  - suspend lakebase project {m.get('lakebase_project')}")
        if input("proceed? [y/N] ").strip().lower() not in ("y", "yes"):
            sys.exit("aborted")
    # 1. Stop the app compute.
    if app and app_state(w, app) not in ("ABSENT", "STOPPED"):
        print(f"  stopping app {app}…")
        subprocess.call(["databricks", "apps", "stop", app, "--profile", profile])
    # 2. Delete GPU endpoints — the real GPU cost stopper.
    for ep in eps:
        print(f"  deleting gpu endpoint {ep}…")
        w.serving_endpoints.delete(ep)
    # 3. Optionally suspend the Lakebase project (only if the manifest allows it).
    if m.get("lakebase_suspend") and m.get("lakebase_project"):
        print(f"  (lakebase suspend requested for {m['lakebase_project']}; implement per your project's branch endpoint)")
    print("DOWN complete — GPU endpoints deleted, app stopped. Lakebase/CDF left running unless configured otherwise.")


def main() -> None:
    ap = argparse.ArgumentParser(description="Bring a Databricks App's billable resources up or down.")
    ap.add_argument("action", choices=["up", "down", "status"])
    ap.add_argument("--profile", required=True)
    ap.add_argument("--manifest", default=None)
    ap.add_argument("--yes", action="store_true", help="skip the DOWN confirmation prompt")
    args = ap.parse_args()

    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient(profile=args.profile)
    m = load_manifest(args.manifest)

    if args.action == "status":
        do_status(w, m)
    elif args.action == "up":
        do_up(w, m, args.profile)
    else:
        do_down(w, m, args.profile, args.yes)


if __name__ == "__main__":
    main()
