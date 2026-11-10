# Sidekick App — Work Synopsis (handoff for next session)

_Last updated: end of this session. Repo: `/Users/patrick.bayne/Development/realtime-call-sidekick/app`_
_Branch: `feat/note-pane-and-record-actions` (fork remote `pbayne/realtime-apps-feature-demo`)_
_Live app: https://call-sidekick-7474650324322090.aws.databricksapps.com (Databricks profile `realtime-call-sidekick`)_

## DONE — committed + DEPLOYED + verified live
- **Note pane** (commit `0d28796`) — Granola-style notes per call/voice session, autosaved to Lakebase.
- **update_record action** (`6a26074`) — proposes a UC Delta MERGE via SQL warehouse (Statement Execution API);
  warehouse `d5e34905bf2cceaf` bound CAN_USE so it runs real MERGEs. Seeded `crm_accounts` (acct-northwind/contoso/fabrikam).
- **UI polish** (`14559dd`) — NotePane focus ring + word count, StartScreen gradient hero / accent / selection ring.
- **D — actions populate on agent finals** (`c8f30c3`) — was customer-only; now proposes on either speaker, prompt is
  speaker-aware (agent commitments like "I'll send the paperwork" surface actions). 5 tests.
- **E — Past Calls history** (`c8f30c3`) — `list_calls(owner)` store methods; `GET /api/history/calls` +
  `/api/history/calls/{id}` (owner-scoped: transcript, cards, actions, Delta summary); PastCalls view at `#/history`
  + "Past calls" link on StartScreen. Verified `/api/history/calls` returns 200 with real calls. 1 test.

## DONE — committed but NOT yet deployed
- **C — auto-accept suggestion when agent follows the talk track** (`fd93a7b`).
  - `sidekick/talktrack.py`: lexical matcher + light stemming, conservative threshold 0.6 + min-overlap 2.
  - `session.py`: tracks open cards (`_open_cards`), matches each AGENT final, auto-accepts (persists feedback
    source=talk_track, emits `card.matched`), drops on manual resolve.
  - `config.py`: `auto_accept_talk_track` (default True), `talk_track_threshold` (0.6).
  - frontend: `card.matched` → `CardView.autoAccepted` → "Auto · you said it" badge in SidekickFeed.
  - 6 tests. Suite 358 passed, ruff clean, frontend typecheck + build green.
  - **NEXT STEP: deploy C** (bundle deploy + app restart — see Deploy recipe below).

## NOT STARTED (remaining features)
- **A — Live mic: pick role once, Sidekick auto-plays the other side** (no manual agent/customer channel toggle).
  - Current: `useCallSession.ts` `setChannel` toggles one mic channel; `connection.ts openAudioSocket(channel)`.
  - Approach: let the rep choose their role at start; the counterpart is driven by the practice-mode style LLM+TTS.
  - Medium effort. Touches `useCallSession.ts`, `StartScreen`/`Console`, `session.py` channel handling.
- **B — "Other side talks back" in the LIVE-MIC call console** (not just Talk/Practice mode).
  - Exists already in voice Practice mode (`voice/session.py`, `voice/prompts.py practice_messages`). Needs bridging
    into the call console so the counterpart speaks during a mic call for either role. Largest piece; depends on A.

## KNOWN BLOCKER — voice GPUs
- `demo_mode.py on` (scale-to-zero OFF for `sidekick-parakeet` STT + `sidekick-kokoro` TTS) **wedged platform-side**:
  config update stuck `IN_PROGRESS` 30+ min, likely GPU capacity. Endpoints serve when scaled-to-zero but
  **cold-start latency exceeds app timeouts**, so voice turns fail (empty transcript, phantom "cut off" bubbles).
- This was the root cause of the originally-reported bugs (STT/LLM/TTS ReadTimeout / Unavailable).
- Options not yet done: (1) raise app STT/LLM/TTS timeouts + retry so cold starts WAIT instead of erroring
  (most reliable given stuck GPU); (2) keep retrying the always-on flip / escalate capacity.
- **Also: a real UX bug worth fixing** — when a turn genuinely produces no words, the UI shows a "cut off" chip on
  an empty agent bubble. Should show an error state ("couldn't reach the voice model") instead. (`Conversation.tsx`
  AgentBubble `cut = ex.interrupted`.)
- Cost: GPU endpoints may be billing. Run `sidekick off` (or `scripts/demo_mode.py off --profile realtime-call-sidekick`)
  when not testing voice.

## Deploy recipe (what works — the big deploy.sh dies on a DNS blip; do it in two steps)
```
cd /Users/patrick.bayne/Development/realtime-call-sidekick/app
export BUNDLE_VAR_catalog=realtime_call_sidekick_catalog BUNDLE_VAR_uc_schema=call_sidekick \
       BUNDLE_VAR_secret_scope=call-sidekick BUNDLE_VAR_warehouse_id=d5e34905bf2cceaf
# 1. build frontend if changed:  (cd frontend && bun run build)
databricks bundle deploy --target dev --profile realtime-call-sidekick
databricks bundle run call_sidekick_app --target dev --profile realtime-call-sidekick   # restarts app w/ new code
```
Verify: `GET /api/health` (status ok) and the feature's endpoint. Auth note: profile uses OAuth; if it expires run
`databricks auth login --profile realtime-call-sidekick` (interactive). GitHub pushes need active account `pbayne`
(`gh auth switch --user pbayne` — `patrick-bayne_data` lacks access to the fork).

## Quality gates (always run before commit/deploy)
- `uvx ruff check src/app tests/app`  ·  `.venv/bin/python -m pytest tests/app -q`  (currently 358 passed / 5 skipped)
- `cd frontend && bun run typecheck && bun run build`  (build writes src/app/static; CI checks for stale bundle)
- Pre-commit/pre-push secret scan runs automatically. `docs/DEMO_TALK_TRACK.md` is pre-existing untracked — leave it.

## Suggested order next session
1. Deploy C (committed, not live). 2. Fix the "cut off"-on-empty-bubble UX. 3. Address voice timeouts (cold-start
waiting/retry) OR get GPUs always-on. 4. Build A. 5. Build B. 6. Push branch to GitHub when ready for review.
