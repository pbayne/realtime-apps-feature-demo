"""Demo mode: keep the voice GPU endpoints warm only while you demo.

The Parakeet (STT) and Kokoro (TTS) endpoints scale to zero by default, so an idle app's first voice turn waits
for a cold GPU (measured 2-6 minutes). `on` turns scale-to-zero off for both endpoints (they bill while on),
waits until each serves a request, and prints the time to ready. `off` turns scale-to-zero back on. `status`
shows the current setting and readiness. Only the scale_to_zero_enabled flag changes; the served model, version,
workload type and size stay as they are.

  .venv/bin/python scripts/demo_mode.py on     --profile realtime-call-sidekick
  .venv/bin/python scripts/demo_mode.py off    --profile realtime-call-sidekick
  .venv/bin/python scripts/demo_mode.py status --profile realtime-call-sidekick
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path

ENDPOINTS = ("sidekick-parakeet", "sidekick-kokoro")


def served(w, name: str):
    ep = w.serving_endpoints.get(name)
    return ep, list(ep.config.served_entities or [])


def set_scale_to_zero(w, name: str, enabled: bool) -> bool:
    """Update only scale_to_zero_enabled. Returns True if a config update was started."""
    from databricks.sdk.service.serving import ServedEntityInput

    ep, entities = served(w, name)
    if all(bool(e.scale_to_zero_enabled) == enabled for e in entities):
        return False
    inputs = [ServedEntityInput(entity_name=e.entity_name, entity_version=e.entity_version, name=e.name,
                                workload_size=e.workload_size, workload_type=e.workload_type,
                                scale_to_zero_enabled=enabled, environment_vars=e.environment_vars)
              for e in entities]
    w.serving_endpoints.update_config(name=name, served_entities=inputs,
                                      traffic_config=ep.config.traffic_config)
    return True


def wait_not_updating(w, name: str, timeout_s: float = 1800) -> None:
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout_s:
        st = w.serving_endpoints.get(name).state
        if str(st.config_update).endswith("NOT_UPDATING"):
            return
        time.sleep(10)
    raise TimeoutError(f"{name} still updating after {timeout_s:.0f} s")


def status(w) -> None:
    for name in ENDPOINTS:
        ep, entities = served(w, name)
        stz = ", ".join(f"{e.name}: scale_to_zero={'on' if e.scale_to_zero_enabled else 'off'}" for e in entities)
        print(f"{name}: ready={str(ep.state.ready).split('.')[-1]} update={str(ep.state.config_update).split('.')[-1]} | {stz}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["on", "off", "status"])
    ap.add_argument("--profile", required=True)
    args = ap.parse_args()

    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient(profile=args.profile)
    if args.mode == "status":
        status(w)
        return
    enabled = args.mode == "off"           # demo "off" = scale-to-zero back on
    started = [n for n in ENDPOINTS if set_scale_to_zero(w, n, enabled)]
    for name in ENDPOINTS:
        print(f"{name}: {'updating' if name in started else 'already'} scale_to_zero={'on' if enabled else 'off'}")
    for name in started:
        wait_not_updating(w, name)
        print(f"{name}: config applied")
    if args.mode == "on":
        # Prove both endpoints actually serve (and warm them) before the demo starts.
        warm = Path(__file__).with_name("warmup_voice.py")
        sys.exit(subprocess.call([sys.executable, str(warm), "--profile", args.profile]))
    status(w)


if __name__ == "__main__":
    main()
