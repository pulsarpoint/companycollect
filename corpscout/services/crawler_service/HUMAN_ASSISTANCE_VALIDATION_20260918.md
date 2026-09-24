# Human assistance validation — 18 September 2026

## Headless default — version 0.31.1

The subsequent deployment changes automatic REST/JetStream scans to headless
CloakBrowser. Only a manual retry of a saved failed attempt starts Xvfb and a headed
browser. The following checks ran against the regular service on `192.168.88.132`:

- `melexis-headless-default-20260918`: the systemd process group contained a
  headless Chrome browser and no Xvfb, x11vnc or Openbox processes. Status reported
  `browser_available: false`; browser-ticket and Resume requests returned 409.
- The CAPTCHA event was recorded at 12:40:43 UTC and failure at 12:40:53 UTC,
  preserving the exact 10-second notification window. SQLite retained the attempt
  and S3 delivery completed at 12:40:54 UTC.
- `melexis-headed-retry-20260918`: the manual retry started Xvfb, Openbox, x11vnc
  and non-headless Chrome. The ticket-protected WebSocket returned the VNC greeting
  `RFB 003.008`. The validation did not solve the actual CAPTCHA.
- Cancelling the test retry removed all of its browser/display processes and saved
  the cancelled attempt to S3. The service remained healthy.

The four focused human-assistance tests passed, including both automatic input
sources, headed manual retries and rejection of Resume for headless attempts.
Ruff, type checking of `src`, lockfile validation and whitespace checks passed.
An unscoped type check additionally reports existing diagnostics in tests and
benchmarks; those files are outside the passing source-only type check.
Evidence is saved in `data/human-assistance-20260918/headless-default-validation.json`.

## Initial version 0.31.0 validation

Version 0.31.0 was deployed with Ansible to `192.168.88.132`, running the regular
`crawler-service.service` on port 8080 with REST and JetStream enabled. Health
checks passed. Systemd retains `PrivateTmp=yes`, `ProtectSystem=strict` and
`NoNewPrivileges=yes`. Backoffice's local configuration now uses this service.

## Production Melexis failure

Request `melexis-human-production-20260918` fetched `https://www.melexis.com/` in
headed CloakBrowser on Xvfb with an explicit page list and no LLM calls.

| Event | UTC time |
| --- | --- |
| Queued / running | 12:20:04 |
| CAPTCHA detected and notification persisted | 12:20:12 |
| Failed with `human_assistance_timeout` | 12:20:22 |
| S3 upload recorded | 12:20:24 |

The SQLite event history confirms exactly **10 seconds** between CAPTCHA detection
and failure. The following S3 objects were downloaded and their SHA-256 hashes
checked against the service's delivery metadata:

```text
s3://crawls/company-crawls/melexis-human-production-20260918/attempts/0001/result.json.gz
s3://crawls/company-crawls/melexis-human-production-20260918/attempts/0001/artifacts.tar.gz
```

The result reports `failed`, zero collected pages and `human_assistance_timeout`.
The archive contains the crawl manifest, blocked HTML, screenshot and failure
metadata. Backoffice displays the failed attempt and `S3 saved` after a fresh load.

Clicking **Retry interactively** created
`manual-4add4a00-8873-4dc4-8cc1-b9bf0df4140d`, linked to attempt 1 of the original
request. The deployed noVNC stream connected with keyboard/mouse control and
displayed the real Melexis Cloudflare verification. The original failure remained
visible. Domain filtering preserved the selected live session. No real CAPTCHA was
solved during validation; this manual session is available until its 15-minute
deadline, after which another retry can be started.

## Successful human continuation

Before deployment, the same service implementation ran on a temporary preview
instance with real S3 storage. The controlled website in
`benchmarks/human_browser/fixture.py` initially returns HTTP 403. An ordinary
**Load test company** button sets a test cookie and opens its company page; the
subsequent jobs page requires that cookie.

Automatic request `browser-fixture-auto-20260918` failed and was archived. Its
manual retry `manual-d95af641-7dbf-4cf5-99e2-ada65ace151c` was opened in Backoffice.
The button was clicked through noVNC, then **Resume crawl** was clicked. The crawl
finished with both company and jobs pages, including an Embedded Software Engineer
job, and zero LLM calls. Its result was uploaded to S3. This verifies VNC input,
explicit Resume, extraction from the current document and cookie continuity on the
next page. It does not establish that Melexis will accept verification.

Saved local evidence is under `data/human-assistance-20260918/`, including
`production-attempt.json`, `interactive-fixture-attempt.json` and
`interactive-fixture-result.json`. Preview and fixture services were stopped after
validation; the regular service remains active.

## Automated checks

- Python suite: 280 tests run successfully, 32 skipped for optional environments.
- Real JetStream service suite with `NATS_SERVER`: all 35 tests passed.
- Final focused human-assistance suite: all 4 tests passed, including S3 outage and
  restart recovery without recrawling, invalid Resume rejection, evidence retention,
  cancellation and manual execution while the normal worker is busy.
- Backoffice targeted suites: 23 tests passed; TypeScript checking and production
  build passed.
- Python Ruff/type checks, Ansible syntax check and `git diff --check` passed.

Notifications currently mean persisted crawler SSE events and Backoffice toasts.
There is no email or external messaging integration in this change. See
[the operator/API guide](HUMAN_ASSISTANCE.md) for configuration and endpoint details.
