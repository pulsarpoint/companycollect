# Brave verification validation — 18 September 2026

Version 0.33.0 is deployed to the regular `company-research.service` on
`192.168.88.132:8080`. Ansible finished with `failed=0`; REST and JetStream health
returned `ok`. The live OpenAPI schema exposes the authenticated
`POST /v1/crawls/{request_id}/verify-search` control. No crawl jobs were active
before deployment or at the final health check.

The service now serializes Brave navigation, retains a durable block on a detected
validator or HTTP 401/403/429, and waits for an explicit operator action. Backoffice
offers **Start verification** on the existing attempt. **Resume crawl** checks the
original query, results and status before letting queued Brave searches continue.
See [operator behavior and limits](HUMAN_ASSISTANCE.md#brave-search-verification).

Validation:

- Python suite: 292 tests ran successfully, with 34 optional-environment skips.
- Five focused Brave tests passed: HTTP 200 challenge detection, no concurrent
  search while paused, explicit activation, rejection of blocked/wrong-query
  confirmation, cancellation/restart persistence, verification timeout and the
  authenticated API continuing the same attempt.
- Existing human-assistance (4) and source-discovery (18) tests passed after the
  final integration changes.
- A real Linux browser fixture passed on the crawler host, using the installed
  Python 3.12 runtime with the new source. The fixture first returns 429 and sets
  a session cookie (without an expiry). No further request occurs until activation.
  The exact pending URL reopens in headed CloakBrowser under Xvfb with that cookie
  present. The VNC endpoint returns its RFB greeting. A controlled fixture button
  loads results; explicit resume captures them and the next query retains the
  verification cookie. The test does not visit Brave or solve a real CAPTCHA.
- Backoffice: eight targeted tests, TypeScript checking and a live browser render
  passed. The admin page displays the updated search-verification description.
- Python source type checking, Ruff, lockfile consistency, Ansible syntax and
  whitespace checks passed. No dependency versions were changed for this feature.

The normal service profile is private state under `.brave-search`, outside per-job
result archives. The pause is shared within this service instance; standalone CLI
processes and other crawler hosts do not participate in its lock. Service restart
retains the block, but interrupted crawl attempts follow existing recovery rules.
