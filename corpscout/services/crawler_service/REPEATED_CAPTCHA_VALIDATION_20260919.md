# Later-page CAPTCHA recovery — 2026-09-19

## Framework incident

Inspected JetStream request `backoffice-ecbd10e6-29dd-432a-a46a-5600fe510db4`, its
saved result and SQLite status history. The first CAPTCHA was solved by agent run
`4bd32abf5f3a4e318615d21af35eb307` in 9.834 seconds. The crawler independently
verified access and continued at 18:46:01 UTC.

It processed 52 URLs: 47 collected documents, four redirected duplicates and one
failed page. At 19:01:10 UTC it encountered a new HTTP 403 CAPTCHA at
`https://frame.work/laptop16?slug=laptop16-diy-amd-7040&tab=specs`.
Crawler 0.37.4 allowed only one automatic agent run per entire attempt, so no agent
ran on this later page. After ten seconds the attempt failed. The previous message
incorrectly implied that the agent had failed verification, and the generic final
status obscured the 47 documents already saved to S3.

The original result remains retained. Backoffice now displays its saved page count
and stopping URL using the existing S3 delivery metadata, even for older attempts.
Local incident evidence is under `data/framework-post-verification-20260919/`.

## Deployed correction

Crawler 0.37.5 is deployed on `192.168.88.132`:

- Automatic assistance can run on later requested URLs. The same URL gets at most
  one automatic run; the default total budget is three runs per crawl attempt.
  Configure `CRAWL_CHALLENGE_AGENT_MAX_RUNS` (1–10) or Ansible's
  `crawler_service_challenge_agent_max_runs`.
- Every run retains its page URL, actions, usage and independent access result.
  The job/API and portable JSON retain all runs as `challenge_agent_results`,
  alongside the latest result for existing consumers.
- A later unresolved challenge leaves portable crawl status `partial` when pages
  were collected. The job remains in the failed queue for filtering and retry.
  Backoffice shows **partial · stopped**, page count and stopping URL, and the
  final reason identifies exhausted budget or unsuccessful page verification.
- HTTP/content/domain checks, agent limits, manual controls and the ten-second
  fallback window remain enforced.

## Validation

Passed 14 automatic-assistance tests, including later-page recovery, the total
budget, repeated-URL protection and partial-result persistence. Also passed 42
service tests with real local JetStream, four human-assistance tests, five crawl
tests, and 18 Backoffice tests. TypeScript checking, production build, Ruff,
Ansible syntax and `git diff --check` passed.

Deployment completed with 35 OK, nine changed, zero failed and zero unreachable.
The live status API confirms automatic assistance enabled with a three-run budget.

Submitted `repeated-agent-pages-20260919` through Backoffice → JetStream
(`COMPANY_CRAWL`, sequence 12). The controlled fixture presents verification on
four distinct URLs, exercising the actual DeepSeek API and existing CDP browser.

| Page | Agent run | Duration | Tokens | Verified access |
| --- | --- | ---: | ---: | --- |
| `/one` | `2e1868adef1642328ba4abdbf0f2f179` | 2.440 s | 4,005 | Yes |
| `/two` | `2ee5f3ca48b04152ad81a67ac32b6956` | 2.747 s | 4,006 | Yes |
| `/three` | `baf468ba8c3b40568e0603336b56e30f` | 2.717 s | 4,005 | Yes |
| `/four` | Not started: budget exhausted | — | — | No |

The crawler collected three documents, then stopped at the fourth URL after the
fallback deadline. Its JSON reports `partial`; all three agent results and the
exact stopping reason survived S3 upload and a read through Backoffice's
ClickHouse mapping. The UI visibly showed all three per-page agent results and
**partial · stopped** with three pages saved.

Evidence is in `data/repeated-challenge-20260919/` (ignored runtime data). Objects
are under bucket `crawls`, prefix
`company-crawls/repeated-agent-pages-20260919/attempts/0001/`.
The temporary fixture server was stopped, its test tab closed, and browser leases
were released. This validates repeated automatic recovery and bounded failure on
a controlled fixture; a new full Framework crawl was not run during this fix.
