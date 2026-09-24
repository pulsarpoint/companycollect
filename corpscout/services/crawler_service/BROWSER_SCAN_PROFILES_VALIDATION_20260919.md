# Saved browser profiles for domain scans — 2026-09-19

Implemented and deployed crawler-service **0.35.0** to the existing Linux crawler.
The service is healthy and reports two running saved-profile desktops.
Release: `edbf2439083903b0184122a9ab81ef1dadc904fc5723467649a599990585d4f8`.

## Behavior

- REST, JetStream and manual attempts lease one saved profile for the entire domain
  scan. Site navigation and Brave searches use separate tabs in that same profile.
- Concurrent scans cannot share a profile. Selection rotates through available
  profiles; waiting scans can be cancelled. Stopped or unhealthy profiles are not
  selected, and explicit Stop remains effective.
- Verification waits retain the profile. Brave still has one service-wide search
  lock and durable block; another profile cannot send searches around that block.
- Successful, failed and cancelled scans save session cookies, close Chromium
  gracefully to flush profile storage, and restart with one blank tab. Assignment
  is released only after cleanup. Shutdown saves/closes without relaunching.
- Save/restart failure quarantines the profile in an error state for manual restart.
- Saved profiles show assigned domain/request and recycling status. Start, Stop and
  opening tabs through the admin API reject busy profiles; the desktop remains
  accessible for verification. Server inventory links saved desktops to their crawl.
- The Auto-restart switch controls idle window-close recovery. End-of-domain
  recycling always applies. Pool-disabled and standalone behavior is preserved.

## Validation

- Backend discovery: **336 tests run, 38 skipped, no failures**. This includes seven
  new lifecycle tests for exclusivity, cancellation during cleanup, waiting scan
  cancellation, shutdown, quarantine and shared site/search assignment.
- Updated saved-profile API/inventory checks: **9 passed**.
- Linux Python 3.12 / real Chromium / Xvfb: **4 native tests passed**, using temporary
  profiles and local HTTP fixtures. No production login cookies were inspected.
  Three complete service scans rotated profile 1 → 2 → 1; login cookies and local
  storage survived, the unused profile's generation stayed unchanged, and each
  used profile restarted with one blank tab before completion was reported.
- Native search test verified site/search context sharing, a global search wait
  across two leased profiles, explicit verification activation, exact pending-query
  continuation, profile retention throughout the wait, and recycling afterward.
  The challenge was a local fixture, not a real CAPTCHA.
- Existing native profile/auto-restart and standalone Brave verification regressions
  also passed. The native run caught and fixed Python 3.12 eager annotation imports
  that local Python 3.14 had not exposed.
- Frontend: **18 tests passed** across five crawler/browser test files; TypeScript
  typecheck passed. Targeted Ruff, Python type checks and lockfile check passed.
- Built 0.35.0 wheel and deployed through the existing Ansible playbook. Health and
  authenticated status confirm 0.35.0, two available saved profiles, one tab each.
- Backoffice visual/AX check confirms the Assigned domain column and server view
  showing Online, 0.35.0 and two saved Xvfb sessions.

The operator explicitly approved deployment while a manual attempt was waiting
for assistance. The interrupted attempt is retained as failed with
`Interrupted before completion` and can be retried from Backoffice.
