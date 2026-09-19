# Browser-service extraction validation — 2026-09-19

Deployed browser service **0.1.0** and crawler **0.37.0** on `192.168.88.132`.

- Browser release: `18d0569736cf6b5524171c252cfa20bc0d087b9049dc384729fde1c0fc1edb0b`.
- Crawler release: `2bf857e377ff66aced33c5adb18afe243bc330de557fbd800205e9aa5b8ce4cb`.
- Separate systemd services, users, state directories, locked Python environments and API credentials.
- Browser API: port 8081; crawler API: port 8080.

## Checks

- Browser-service suite: 32 tests, 31 passed, one opt-in Linux test skipped locally.
- Crawler suite: 337 tests, 300 passed, 37 environment-dependent tests skipped locally.
- Three real Linux/Xvfb integration tests passed (54.981 seconds), using temporary
  profiles and a local HTTP fixture: two simultaneous manual crawls with a queued third;
  profile rotation, cookie/localStorage retention and blank restart; site/search affinity,
  durable Brave pause, closed-tab behavior and whole-browser recovery.
- Ruff and Python 3.12 type checks passed for both services.
- Backoffice typecheck and 18 focused UI tests passed.
- Production crawler environment has no Playwright or CloakBrowser packages.
- Old crawler browser-management endpoint returns 404; its crawl API remains healthy.

## Migration and live verification

Both old profiles were copied while the crawler was stopped. All 1,096 files in
browser-1 and 1,382 files in browser-2 matched byte-for-byte before the new browsers
started. Originals remain under `/var/lib/company-research/results/.browser-sessions`
for rollback; active copies are under `/var/lib/browser-service/profiles`.

Crawler deployment left both running browser generations unchanged, demonstrating
independent process ownership. Backoffice `/admin/browsers` showed service 0.1.0 and
two running desktops. Connecting to browser-1 displayed the live Chromium desktop;
`/admin/browsers/profiles` showed both saved profiles available with auto-restart enabled.

Live fixture crawl `browser-service-smoke-653c9ef91d9f` completed. Chromium rendered its JavaScript
content and the crawler stored the result. SQLite retained session `6393c213eedc4c1cb8f1dde07f95ba9a`
with profile `browser-1` and state `released`. Both browsers then remained
running and available, each with one blank tab. No public-site search or real CAPTCHA
was used for validation.

Idle expiry is configured to 120 seconds. Lifecycle tests cover non-renewing GET and
management reads, heartbeat renewal, late-heartbeat rejection, in-flight protection,
exclusive allocation, terminal-ID reuse rejection and service-restart reconciliation.

Final client cleanup check: a rejected conflicting reservation cannot release the
existing owner’s session. Six client tests and 15 HTTP boundary tests passed after
this correction; the final crawler artifact above includes it. Both health endpoints
returned 200 after deployment and both saved browsers remained available.
