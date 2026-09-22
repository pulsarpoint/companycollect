# Brave browser-service validation — 2026-09-19

Browser service 0.7.1 is deployed on `192.168.88.132:8081`. Dagster’s Brave resource now calls its HTTP API. Backoffice browser assignments display the current Brave operation and completed CAPTCHA-agent runs.

## Live searches

These queries ran concurrently through the new Dagster HTTP client against the deployed browser service. Query execution excludes browser allocation, route switching and final profile recycling. No real CAPTCHA appeared in these four queries.

| Company | Route | Query execution | Result | Agent runs |
| --- | --- | ---: | --- | ---: |
| Novelic | direct | 15.659 s | success | 0 |
| Skanska AB | crawl_proxy3 | 13.903 s | success | 0 |
| Melexis | crawl_proxy1 | 14.266 s | success | 0 |
| Framework Computer | crawl_proxy2 | 14.312 s | success | 0 |

Saved request IDs (available through authenticated `GET /v1/brave/requests/<id>`):

- `dagster-f644e62470af48b78137c1b8cf8cafb9`
- `dagster-f115175f52ab4d369200f577d903c224`
- `dagster-86dc2e04c67f42f19dcd121e6e7d4fa6`
- `dagster-f848033a6c3a4b2eb86b0af6c6718620`

All four saved results were read again after the final service restart; answers and execution timestamps/durations were retained without repeating the searches.

## Verification

- Browser regression suite: 109 tests, 99 passed and 10 opt-in tests skipped.
- Native Brave suite: all 8 tests passed with real Chromium and CDP, intercepted fixture pages, and a deterministic model transport. Covers automatic agent invocation; verification returning to the search home page; CAPTCHA appearing during answer generation; false agent success and the three-run budget; preserving interrupted-agent evidence; query identity; Copy isolation; empty answers; deadlines; authentication; capacity; cancellation; idempotency; and persistent direct/proxy cookie isolation.
- Dagster Brave/publication/initialization/integration suites: 39 passed, including lazy refill, save-before-refill, adaptive timeouts, HTTP capacity retry and response recovery, S3/publication integration, and resource compatibility.
- `uv run --no-sync dg check defs`: passed.
- Backoffice TypeScript check and browser-server UI tests: passed (7 tests).
- Ruff and `git diff --check`: passed for the changes.

## Deployment

- Browser service deployed using its Ansible playbook; final release 0.7.1.
- Saved SQLite configuration remains four headless and two headed browsers, with no active test leases left behind.
- Dagster deployment was staged from the existing server release with only Brave files overlaid, preserving unrelated local work.
- Full restart correctly refused to interrupt the active Norway annual-account PDF import. The existing hot-sync playbook then validated and reloaded the code location without restarting the supervisor. Main PID and restart-count assertions passed; the Norway run remained STARTED with its original start time.
- The remote Dagster resource initialized with the browser API settings and successfully retrieved a saved Brave result. New resource environment variables are read by Dagster’s secrets loader when the code location/run worker starts.

## Limits

CAPTCHA handling was verified against controlled browser fixtures, including real CDP clicks and page checks. The live Brave queries did not trigger CAPTCHA, so this test does not establish success against every Brave challenge. Provider errors, unsupported challenges or exhausted budgets return a structured failure with local diagnostics. CAPTCHA-agent runs default to three and can be overridden per request; the overall request deadline still applies.

The historical standalone searcher prototype and the crawler’s ordinary Brave search-page parser are separate from the production Brave Ask workflow migrated here. Dagster still owns selection, retries, archiving and publication. Local Brave diagnostics currently have no automatic retention policy.
