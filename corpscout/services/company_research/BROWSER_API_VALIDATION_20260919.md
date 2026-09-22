# Sticky browser API validation — 2026-09-19

Deployed version **0.36.0**, release
`d8da98c5af6b555233d63f3967a801790588aede7e37433022a3f7512af88c2a`.

The agreed policy is one domain crawl per browser. An opaque reservation ID binds
all site/search requests to one existing saved profile. The pool remains bounded
by its configured profile count. See [the API contract and examples](BROWSER_API.md).

## Checks

- Backend suite: **341 tests run, 38 skipped, no failures**. New HTTP tests cover
  two ready reservations and a queued third, idempotent reservation, profile/tab
  affinity, serialized requests, capture without navigation, header redaction,
  missing/unknown IDs, authentication, unavailable pool, expiration/heartbeat,
  and client cancellation cleanup.
- Targeted API tests rerun after adding the admin lease-ID field: **5 passed**.
- Real Linux Python 3.12 / Chromium / Xvfb: **4 native tests passed**. The domain
  crawler used the HTTP client, rotated existing profiles, preserved cookies/local
  storage and restarted blank after release. Site/search verification also ran
  through a real TCP HTTP server with the same session ID. A second profile's
  Brave request remained behind the shared search guard during verification.
- Native verification used local fixtures, not real CAPTCHAs. All test profiles
  were temporary; production cookies were not inspected.
- Frontend: **18 tests passed** across five crawler/browser files. TypeScript,
  targeted Ruff, Python type checking and lockfile checks passed.

## Live deployed API check

The crawler was idle before deployment. After Ansible completed, authenticated
status showed healthy 0.36.0 and exactly two saved desktops. Both profiles were
idle with blank tabs before the API smoke test.

1. Created three temporary API reservations. The first two became ready on
   browser-1/browser-2; the third stayed queued. Desktop count stayed two.
2. Opened site and search tabs using the first reservation ID and captured their
   blank documents. Both responses retained that ID and profile ID. No external
   website navigation, LLM calls, or crawl/S3 result jobs were created.
3. Verified Backoffice displays each active domain, request ID and session ID;
   Stop is disabled while reserved and desktop access remains available.
4. Released the first reservation. The queued reservation received that same
   profile with a new browser generation, proving recycling completed first.
5. An extract request with the released ID returned HTTP 404.
6. Released all test reservations. Final status: exactly two running, unassigned
   profiles, one blank tab each, and a successful health check.

Temporary validation source bundles were removed from the crawler host. Existing
saved profiles and login state remain private service data.
