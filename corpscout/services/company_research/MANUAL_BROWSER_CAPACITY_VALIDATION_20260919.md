# Manual queue browser capacity — 2026-09-19

Deployed version **0.36.3**, release
`5b3e4a8aa08c4566e7f9c8a15deac6c2dd747d13c817530bc657b7822418d430`.

The service started only one manual-crawl worker regardless of the saved-browser
count. A paused Melexis attempt occupied that worker and browser-1, leaving three
manual requests queued while browser-2 was free.

The manual queue now has one worker per configured saved browser, with one worker
when the pool is disabled. A paused manual attempt can therefore coexist with a
second attempt using the other profile. All inputs retain exclusive reservations
through the same bounded pool. Normal REST/JetStream concurrency remains configured
by `--concurrency`; no additional browser profiles are created.

## Validation

- **347 backend tests run, 39 skipped, no failures.** The service regression test
  uses normal concurrency 1 and two saved profiles: two manual attempts enter
  human pauses, a third remains queued, cancelling one releases/recycles its
  profile, and the third starts without affecting the other paused attempt.
- A **real Linux / Chromium / Xvfb integration test passed** with local HTTP
  fixture pages. Both saved browsers visibly held a page while both manual
  attempts were paused. The third waited, then used the released profile after
  cancellation and recycling. Exactly two desktops remained open.
- Targeted Ruff, Python type checking, lockfile checks and Ansible wheel build
  passed. Temporary native test source files and profiles were removed.

The user authorized deployment and interruption of the paused manual attempt.
Ansible completed without failed tasks. Authenticated checks confirmed healthy
**0.36.3** and exactly two running saved browsers with distinct reservations.
Two queued Melexis retries started at `2026-09-19T07:24:25+00:00`, each with a
Melexis page open in its assigned browser and waiting for operator verification.
The Oresund request remained queued because both profiles were occupied. The
interrupted previous attempt was retained as failed; queued requests were preserved.
