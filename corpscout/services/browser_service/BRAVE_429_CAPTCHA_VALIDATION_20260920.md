# Brave HTTP 429 verification fix — 2026-09-20

Browser service 0.7.2 is deployed on `192.168.88.132:8081`. The failed company query now succeeds, automatically invokes the CAPTCHA agent when verification appears, and succeeds again without verification on an immediate repeat.

## Original failure

- Dagster task: `0602f7d2-6158-48f1-9b31-0550d8383d4e`.
- Input: `SE:5560049529`, Masmästaren Näktergalen AB.
- Query: `Find the official website of Masmästaren Näktergalen AB.`
- Three saved attempts failed at access: `crawl_proxy3` (666 ms), `direct` (580 ms), and `crawl_proxy3` (856 ms). All had zero agent runs.
- The reported 856 ms attempt used browser request `dagster-c81a5bc0-6021-4710-b930-105e13789125` and profile `headless-3`.

These were access failures before answer generation. The configured 60-second answer timeout was not reached. The original results did not save response status, HTML, or screenshots, so the diagnosis required reproducing the query in the affected profile.

## Reproduction and change

Brave returned HTTP 429 at the original `/ask` URL, with title `Brave Search`, heading “Verifying you're not a bot”, and buttons “Verify” and “Switch to traditional CAPTCHA”. This proof-of-work screen had none of the previously recognized challenge widgets or URLs. The detector classified it as a generic block and did not invoke the agent.

The detector now recognizes these visible challenge controls. Rejected pages receive up to five seconds, bounded by the configured page timeout, to render verification controls before rejection. Actual CAPTCHA screens use the existing agent and page-verification loop. Plain access failures are classified as `Unauthorized`, `Forbidden`, or `RateLimited` according to HTTP status.

Results now include the last document HTTP status. Failed requests also attempt to save the title, `Retry-After`, HTML, and a screenshot; evidence capture is bounded and cannot replace the original error. Response headers other than `Retry-After` are not copied into failure evidence.

The pre-fix screenshot is available locally at `output/playwright/brave-429.png` (ignored by Git).

## Live validation

Both post-deployment calls used the exact original query, headless mode, route `crawl_proxy3`, and persistent profile `headless-1`.

| Request | Result | Query execution | Client wall time | Agent runs |
| --- | --- | ---: | ---: | ---: |
| `brave-429-retry-ebea4ad365394167836512f56ce5ae99` | success, HTTP 200 | 17.065 s | 20.81 s | 1 |
| `brave-429-repeat-35d4078050af40eb8bb16e619df91afc` | success, HTTP 200 | 10.763 s | 12.79 s | 0 |

The first call encountered the real verification screen. The `deepseek-flash` agent clicked Verify, waited, and proposed another wait while the page changed. Its trace ended as `interrupted` (“Browser session or page changed”); the outer Brave workflow independently checked the resulting page, confirmed access, and collected the requested answer. The agent used 5,984 prompt tokens and 129 completion tokens across three recorded steps, two executed and one proposed. A trace ending as interrupted is not itself proof that the overall request failed.

Both answers identified `www.masmastaren.se`. The repeat demonstrates reuse of verification state for this immediate request on the same profile and route; it does not establish how long Brave will retain that state. The calls exercised the browser API directly; they did not rerun or replace the historical Dagster task results.

Saved results can be inspected using authenticated `GET /v1/brave/requests/<request_id>`. Detailed artifacts are under `/var/lib/browser-service/brave-requests/<request_id>/`; the first agent run is `2ba7731a7a3d45c69201944cce116ced` under `/var/lib/browser-service/challenge-runs/`.

## Checks and deployment

- Native Brave suite: 10 tests passed, including immediate and delayed HTTP 429 verification screens, and plain rate limiting with saved evidence and no agent invocation.
- Browser regression suite: 111 tests, 99 passed and 12 opt-in tests skipped.
- Ruff and `git diff --check`: passed.
- Ansible deployment: 38 successful tasks, 10 changed, zero failures or unreachable hosts.
- Deployed version 0.7.2 reports healthy. Saved SQLite configuration remains four headless and two headed browsers, with no draining profiles.
- Both live test leases are released. The diagnostic tab and SSH tunnel were closed.

The proposed on-demand browser/session redesign is not part of this fix.
