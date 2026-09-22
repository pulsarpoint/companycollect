# Real-domain CAPTCHA agent validation — 2026-09-19

Tested the deployed browser service 0.4.0 on `192.168.88.132` with crawler
0.37.3 and the local Backoffice application. Both Melexis (`melexis.com`) and
Framework (`frame.work`) displayed real Cloudflare verification checkboxes.
After explicit user confirmation for both domains, the DeepSeek agent cleared
both challenges. Independent browser captures confirmed normal page content
and HTTP 200, replacing the initial HTTP 403 responses.

## Agent results

The configured model was `deepseek-flash` (DeepSeek V4.1 Flash). Each run used the
existing interactive browser session through CDP, retaining its profile and
browser generation. Both action sequences were **click → wait → finish**.

| Domain | Agent duration | Decisions | Prompt tokens | Completion tokens | Total tokens | HTTP before → after |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| melexis.com | 10.803 seconds | 3 | 5,969 | 127 | 6,096 | 403 → 200 |
| frame.work | 9.497 seconds | 3 | 5,979 | 126 | 6,105 | 403 → 200 |

Both returned `appears_clear`. These durations cover only the agent execution,
excluding the wait for user confirmation and subsequent crawling. The two runs
used 12,201 model tokens in total; crawler discovery calls are separate.

| Domain | Crawl request ID | Agent run ID |
| --- | --- | --- |
| melexis.com | `agent-real-melexis-ff389c86149a` | `acc7cb9ead6e476ba66475440d673052` |
| frame.work | `agent-real-framework-3bf7305fc7c0` | `ebbf1dc6bd6c49159dfc9b40a9269fb6` |

## Crawl continuation

The agent left each crawl paused. **Resume crawl** was selected separately in
Backoffice after checking access. Both requests used full discovery with a
five-page budget and web search disabled.

- **Framework:** collected five pages, each HTTP 200: `/`, `/contact-us`,
  `/about`, `/desktop`, and `/laptop12`. Result status was `partial`, with
  `stop_reason: page_budget` and no crawl errors.
- **Melexis:** collected its homepage with HTTP 200. The subsequent company
  classification call failed with `ValueError: Invalid JSON model output`.
  Result status was `needs_review`, with `stop_reason: run_error`. This is a
  separate discovery-model failure after the challenge had cleared.

Both attempts uploaded their result JSON and crawl artifacts to S3. Their
original JSON was successfully read back through Backoffice's ClickHouse/S3
mapping. A top-level job state of `completed` means a result was produced;
the result's crawl status above describes the actual collection outcome.
The final browser-service check showed zero active leases.

## Evidence

Local runtime evidence is stored in the ignored directory
`../company_research/data/challenge-real-20260919/`, including before/after
captures and screenshots, agent journals, final jobs, original crawl JSON,
`crawl-summary.json`, and combined metrics in `summary.json`.

Agent journals and screenshots also remain on the server under
`/var/lib/browser-service/challenge-runs/<agent-run-id>/`. These agent diagnostics
are separate from the crawler's S3 artifact archives.

Crawler objects are in bucket `crawls`:

- `company-crawls/agent-real-melexis-ff389c86149a/attempts/0001/result.json.gz`
- `company-crawls/agent-real-framework-3bf7305fc7c0/attempts/0001/result.json.gz`

Each attempt directory also contains `artifacts.tar.gz`.

This was one approved agent attempt per domain in an interactive browser. It
demonstrates success for these two checkbox challenges, not a measured general
success rate, image-puzzle capability, or headless-browser performance. The
Melexis classification failure remains an independent follow-up issue.

## Repeat requests after verification

At approximately 14:58–15:00 UTC, around 16–18 minutes after the agent runs,
tested two fresh browser-service sessions per domain and then one normal REST
crawler request per domain. Each direct session explicitly selected its previously
verified profile; normal crawler allocation independently selected those same
profiles. Every attempt used a new browser generation after normal recycling.

| Domain | Saved profile | Direct repeat 1 | Direct repeat 2 | Normal crawler request |
| --- | --- | --- | --- | --- |
| melexis.com | `browser-1` | HTTP 200 | HTTP 200 | `finished`, one page, no challenge |
| frame.work | `browser-2` | HTTP 403, challenge | HTTP 403, challenge | `failed`, `human_assistance_timeout`, zero pages |

Framework initially displayed a verification spinner. A later capture during the
normal crawler attempt showed the **Verify you are human** checkbox and HTTP 403,
confirming that the initial challenge did not simply resolve on its own. No agent
or human attempted the challenge during these repeat tests. The automatic attempt
failed after the configured 10-second assistance window.

Both saved profiles still contained an unexpired `cf_clearance` cookie for their
respective domain. Only cookie metadata was inspected; values were not logged.
This rules out a missing saved cookie as the immediate explanation, but does not
establish why Framework required another verification. Browser restart identity
and server-side validation remain possible follow-up areas to investigate.

Normal crawler request IDs:

- `repeat-validation-melexis-6d4e8037c59b`
- `repeat-validation-framework-bc0adab0f3c1`

Both results and artifacts were uploaded to S3, including the failed Framework
attempt. All test leases were released. Captures, screenshots, request payloads,
job/result JSON, and summaries are under
`../company_research/data/challenge-repeat-20260919/` (ignored runtime evidence).

These results concern the current saved, headed browser pool after recycling;
they do not measure headless behavior or how long verification remains accepted.
