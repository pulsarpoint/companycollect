# SQLite browser ownership validation — 2026-09-19

Browser service 0.3.0 and crawler 0.37.2 are deployed on `192.168.88.132`.
Both systemd services are active and healthy. Crawler consumption was paused
while activating the coordinated update; no crawl was active at deployment.

## Implementation

- Removed `browser_multiplexer.py`, the browser allocation queue, reservation
  holder tasks and duplicate ownership/deadline fields in memory.
- `SessionStore.claim()` uses `BEGIN IMMEDIATE` to check and claim a browser.
  A unique index covers unfinished assignments through preparation, use and
  release. Browser selection uses saved assignment history to rotate profiles.
- Busy explicit claims and first navigation requests return 503 with
  `Retry-After: 1`, without creating a session. The crawler retries the same claim
  once per second; cancellation stops waiting. Navigation is not retried by this
  mechanism.
- Browser handles, tabs, operation locks and cleanup tasks remain in the runtime.
  Release rejects new operations immediately and retains ownership until active
  operations and browser recycling finish. Caller disconnects cannot cancel
  cleanup. Expiration and restart retain terminal session history.
- Backoffice recognizes `starting` assignments and allows their termination.
  Removed the obsolete browser `BROWSER_MAX_PENDING` configuration.

## Automated verification

- Browser service suite: 69 tests, 68 passed and one opt-in native test skipped
  locally. Includes SQLite contention between independent connections, expiration,
  idempotency, conflicting owners, cleanup cancellation and profile quarantine.
- Crawler browser suite: 39 tests, 28 passed and 11 platform/opt-in tests skipped
  locally. Includes busy-claim retry/cancellation and human-assistance recovery.
- Backoffice assignment controls: seven tests passed; typecheck passed.
- Ruff and `git diff --check` passed.
- Four real Chromium/Xvfb tests passed on the server with temporary profiles:
  saved login persistence, profile rotation/recycling, simultaneous paused manual
  crawls and site/search verification recovery. Live saved profiles were not used
  for those native tests.

## Live capacity and crawl check

Held both live browsers with test sessions. An additional claim returned 503 and
its session ID returned 404, confirming no browser queue entry was created.

Submitted `sqlite-lease-novelic-20260919-1789818591` through the crawler REST API
for `https://www.novelic.com/contact/`. While capacity was occupied, the crawler
reported `running` with reason `Waiting for an available saved browser profile`;
its prospective browser session did not exist in SQLite. Releasing one held
browser allowed the same crawl to proceed and complete with one collected page.
The crawl's browser assignment ended as `released`.

The result and diagnostic artifacts uploaded to S3. Backoffice's JSON download
read the new object through ClickHouse and exactly matched the saved crawl JSON:

```text
crawls/company-crawls/sqlite-lease-novelic-20260919-1789818591/attempts/0001/result.json.gz
```

After cleanup, no browser assignments remained active and both browsers were
running. Detailed receipts are retained locally in
`../company_research/data/browser-sqlite-20260919/`.
