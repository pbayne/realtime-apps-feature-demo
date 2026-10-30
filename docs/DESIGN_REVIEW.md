# Sidekick design review

Scope: the start screen, the Talk (voice) start panel, the call console (replay and live mic), the voice console
(assistant and practice), and the post-call summary. Captured in mock mode at 1440x900 and 1280x720 (before:
`frontend/screenshots/` and `/tmp/sk-design/before`). Visual language kept: dark navy, lava `#FF3621` used
sparingly, Inter/system fonts.

Note on the brand reference: the design-critique skill's Databricks rules (DM Sans, warm light surface) do not
match the established Sidekick dark-navy language, so they were not applied.

## Overall verdict

The layout is sound: the voice console has a clear hierarchy (orb and controls, then the conversation, then the
live read and outcomes), the density suits a product tool, and the motion work already respects reduced motion.
The gaps are accessibility and states: the primary button and the muted and placeholder text fail WCAG AA, focus
rings are removed on the form controls, a denied microphone shows a raw browser error, and reconnecting and
normal session end have no visible message.

## Findings

### P0

1. **Primary button text fails AA.** White on `#FF3621` is 3.62:1; the 13-15px semibold label needs 4.5:1. The
   primary button is used for Start call, Start voice session, New session, Warm up voice and Start another call.
2. **Focus rings are removed on form controls.** `outline-none` on the start-screen selects, the voice and script
   selects, the scenario and voice pickers, the link-to-call field, the composer and the card edit box. Keyboard
   focus shows only a 1px border tint (`focus:border-sky-400/60`), which is easy to miss on navy.
3. **Microphone denied has no designed state.** The start screen shows the raw error text, and the voice console
   shows `Microphone unavailable (<browser message>). Type your turns instead.` There is no explanation of how to
   allow the mic and no retry.

### P1

4. **Informational text uses `slate-500`.** It measures 3.2-3.9:1 on the panels: unchecked coverage items (call
   and post-call), input placeholders, and the empty-feed icon.
5. **The muted token is borderline on tinted surfaces.** `#8ea0bd` passes on the bare surfaces (7.0 / 6.5 / 5.8:1)
   but drops to 4.78:1 on tinted chips and cards. It needs more margin, not just a pass.
6. **Reconnecting and normal session end are silent.** Reconnecting shows only a grey orb and an amber chip, so a
   rep cannot tell whether the call is still running. A normal end shows an "Ended" chip and "Session ended" on the
   orb, with no explanation that the transcript is kept.
7. **Talk form does not fit 1280x720.** The "Start voice session" button sits below the fold; the start screen
   needs a scroll to reach its primary action.
8. **Source, role, playbook and mode toggles have no `aria-pressed`.** The selected state is visual only, so a screen
   reader cannot tell which source, role or playbook is active.
9. **Backend-unreachable start state is confusing.** Without the backend, the replay source shows "No scripts for
   this playbook", a disabled Start call with no reason, and the backend note in the error (rose) colour.

### P2

10. **Type scale is loose.** Roughly 14 sizes between 10 and 16px, with 10px and 10.5px used for uppercase labels
    (below a legible 11px).
11. **Transcript region is not keyboard reachable.** The conversation scroller cannot be focused, so keyboard users
    cannot scroll it.
12. **The loading spinner ignores reduced motion.** `animate-spin` on the voice warm-up card keeps rotating.
13. **Post-call tile row is a mild template pattern.** Four equal stat tiles with icons and big numbers. Kept, since it
    reads as product UI, but the latency rows show "--" with no explanation.
14. **Call console and voice console vocabulary differs.** "End call" vs "End session", "Start call" vs "Start voice
    session", "Start another call" vs "New session". Not changed; noted for copy review.

### Already in good shape

- Reduced motion: orb pulse and breathing, caption shimmer, live dot, skeleton shimmer, card and bubble entry, and the
  gauge marker all stop under `prefers-reduced-motion` (`orb.css`, `live.css`, `index.css`).
- Empty states: the feed, outcomes, conversation and read panel each say what will appear and when.
- Warming banner: a polite status with an elapsed clock.

## Contrast (WCAG AA, normal text needs 4.5:1)

| Token / use | Surface | Before | After |
| --- | --- | --- | --- |
| Primary button label (white on lava) | `#FF3621` | 3.62 | 4.89 (`#D92A18`) |
| Primary button hover | `#D92A18` | 4.89 | 6.39 (`#B82311`) |
| `--color-muted` (`#8ea0bd` to `#9aabc4`) | ink / panel / raised | 7.01 / 6.50 / 5.76 | 7.97 / 7.38 / 6.55 |
| `--color-muted` worst case on tinted chip or card | tinted | 4.78 | 5.44 |
| `slate-500` text and placeholders, moved to `slate-400` | ink / raised | 3.91 / 3.21 | 7.25 / 5.97 |
| Focus ring (`#38bdf8`) | panel | 8.05 | 8.05 (unchanged) |

Other pairs checked and passing: body text, sky, rose, amber and emerald chip text, and the warning and notice
banners (all above 7:1).

## Priority list for edits

P0 1-3 and P1 4-9 are fixed in this pass. P2 10-12 are fixed (cheap). 13 and 14 are noted only.

## Before and after

Screenshots: before in `/tmp/sk-design/before/` (not committed); after in `frontend/screenshots/design-after-*.png`
(1440x900 and 1280x720 for start, talk, assistant, practice, replay and postcall, plus `micdenied` at both sizes).

- **Start screen and Talk (1280x720).** The Talk CTA was below the fold; now it sits inside the viewport
  (`design-after-talk-1280x720.png`). Start, source and playbook padding is tighter.
- **Mic denied.** Was a raw browser message; now a card with the reason, the fix (site permission), and Try again
  (`design-after-micdenied-1440x900.png`). Voice-console notice reads the same way.
- **Focus.** Form fields no longer suppress the focus ring; keyboard focus shows the sky-400 outline.
- **Primary button.** Lava-deep base (4.89:1 with white text, was 3.62:1), with the same red identity.
- **Reconnecting and session end.** Banners for both. A normal end keeps the transcript and offers a new session.
- **Backend unreachable.** Amber advisory, and an explanation under the replay source instead of a bare disabled button.
- **Type.** No text below 11px. The 10px and 10.5px labels moved to 11px.

Not changed: the stat-tile post-call layout (P2 13) and vocabulary alignment between the consoles (P2 14).

Verification: `bun run typecheck` and `bun run build` pass. The mic-denied state was captured with `getUserMedia`
rejecting `NotAllowedError` in a copy of the build (served on port 8131, not committed).
