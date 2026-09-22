# Verification browser recovery — 2026-09-19

Version **0.36.1**, release
`d45c89aaba51f3bfd618aa90bb17d2cae6419fa9658a21d4366c7c726f43fce4`.

Browser-2 previously remained in error after Chromium exited during a manual
Melexis verification wait. Idle auto-restart deliberately skips leased profiles,
and the waiting crawl made no page requests to recover its assigned browser.

Active site and Brave verification now check their pending tab through the browser
HTTP API while waiting. A closed tab/browser is restored on the same exclusive
profile and reservation ID. The pending URL reopens, the desktop connection gets
the current generation, and a fresh operator Resume is required. Healthy tabs are
left untouched. Recovery does not extend the assistance deadline or activate a
Brave verification that the operator has not yet started.

Recovery honors the profile's auto-restart setting. A launch/recovery failure is
retained without an automatic restart loop. Cancellation, deadline expiry and
release retain normal reservation cleanup. Browser close/disconnect events now log
the profile ID and whether it was assigned, without URLs or credentials.

## Validation

- Backend suite: **344 tests run, 38 skipped, no failures**.
- Focused HTTP API tests: **8 passed**, including browser/tab restoration, stale
  Resume rejection, unchanged deadline, live operator page preservation, disabled
  auto-restart and failed recovery without repeated launches.
- Linux Python 3.12 / real Chromium / Xvfb: **2 native tests passed**. A browser
  was explicitly closed during an active local Brave verification fixture over a
  real TCP API. The same reservation/profile reopened the pending query with saved
  login state; both configured desktops remained present, the global search block
  remained set, and a fresh Resume was needed. Subsequent crawling and final
  profile recycling also passed.
- Native fixtures used temporary profiles and a local HTTP server; production
  profiles were not modified by tests. Temporary test sources were removed.
- Targeted Ruff, Python type checking, lockfile validation and Ansible wheel build
  passed.

The live crawl queue was idle before deploying the release.

Deployment completed through Ansible with no failed tasks. Authenticated live
checks confirmed healthy **0.36.1**, both saved browsers running without errors,
one tab per browser, auto-restart enabled, and no assigned/active crawls.
