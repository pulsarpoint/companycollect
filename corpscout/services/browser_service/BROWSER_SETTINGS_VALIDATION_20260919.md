# Browser settings validation — 2026-09-19

Deployed browser service 0.6.1 and crawler 0.39.0 to 192.168.88.132. Backoffice
provides `/admin/browsers/settings`, linked from the Browsers tabs.

## Configuration

- Startup defaults: zero headless, two non-headless browsers, 120-second idle expiry.
- Initial CLI flags: `--headless-count`, `--headed-count` (alias
  `--non-headless-count`), and `--idle-timeout-seconds`.
- Precedence: saved SQLite settings, CLI, environment/init file, defaults.
- SQLite `settings` contains `browser_pool` and optional `browser_runtime` entries.
- Pool resizing drains current leases before removal and keeps saved profile data.
- Timeout updates affect new leases and subsequent requests/heartbeats; existing
  deadlines are not shortened by saving settings.

## Checks

- Browser suite: 101 tests, 99 passed and two opt-in native tests skipped.
- Focused crawler integration, service, human-assistance, agent, API and JetStream
  suite: 64 passed.
- Backoffice browser/settings suites: 31 passed. Typecheck and production build passed.
- Ruff and whitespace checks passed.
- Real server: saved a mixed pool of one headless and one headed browser through
  Backoffice. Both reached running state. Only the headed process had Xvfb/VNC.
- Headless browser captured example.com successfully (HTTP 200). Removing it from
  desired capacity preserved its active lease and generation; capture continued.
  Release retired it, and adding it back reused the saved profile directory.
- A server restart retained the SQLite 1+1 override over startup 0+2 counts.
- Crawl `pool-headless-crawl-9d37487d58a2` completed through the deployed crawler,
  collected one page, and used `headless-1`.
- The dedicated Settings page saved a 180-second timeout. Direct SQLite inspection
  confirmed `browser_runtime` held that value. Reset restored the startup 120 seconds.
- Regression tests cover releases during pool growth, concurrent configuration
  changes, mode selection, active-lease protection, and restart precedence.

The user subsequently saved four headless and two non-headless browsers; that
SQLite configuration was preserved. Final timeout is the startup 120 seconds.
