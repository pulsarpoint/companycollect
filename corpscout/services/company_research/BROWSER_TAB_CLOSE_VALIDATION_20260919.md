# Respect closing a verification tab — 2026-09-19

Deployed version **0.36.2**, release
`a719b90c7bfae4fb3a60aba41eafa8b2de6f0e8e265b68f8ea0e5ff752a04b2e`.

The active Melexis verification loop treated closing its tab as a browser failure
and immediately recreated that tab. Recovery now distinguishes a closed named tab
in a running browser from Chromium itself exiting.

- Closing a verification tab leaves it closed and makes no further page requests.
  The crawl keeps its reservation and original deadline, and reports that the tab
  was closed.
- Selecting Resume explicitly reopens the pending verification page on the same
  browser. The operator must verify the page and select Resume again to continue.
- Closing Chromium still recovers the same profile and reservation when
  auto-restart is enabled. Existing login state, search blocking, timeout and
  cancellation behavior remain in effect.

## Validation

- **345 backend tests run, 38 skipped, no failures.**
- **9 focused HTTP API tests passed**, including leaving a closed tab untouched
  across repeated recovery checks, explicit reopening, unchanged reservation and
  browser generation, continued manual pause, and whole-browser recovery.
- **Real Linux / Chromium / Xvfb test passed** over a TCP browser API and local
  verification fixture: closing the pending search tab stopped page requests;
  explicit Resume reopened it without continuing the crawl; closing Chromium
  still restored the same lease/profile with saved login state and unchanged
  deadline. The shared search block remained set until a fresh confirmation.
- Ruff, type checking, lockfile validation and the Ansible wheel build passed.

The user authorized deployment and interruption of the active Melexis manual
attempt. Ansible completed with no failed tasks. Live checks confirmed healthy
**0.36.2**, both saved browsers running without errors, auto-restart enabled, one
tab each, and no active crawls. The interrupted attempt is retained as failed with
`Interrupted before completion` at `2026-09-19T07:16:05+00:00` and remains available
for an explicit manual retry.
