# Request budgets, progressive retries, and GLM validation — 2026-09-19

Deployed crawler 0.38.0 and browser service 0.5.1 to 192.168.88.132.
Backoffice changes are running in the local development application.

## Behavior

- Default automatic CAPTCHA budget: three runs per crawl attempt, at most one per requested URL.
- REST/JetStream field: `challenge_agent_max_runs` (integer 3–1000).
- A retry of an exhausted attempt doubles the previous effective budget: 3 → 6 → 12, up to 1000. Other failure reasons preserve it.
- An explicit budget in the retry body overrides escalation. Retry options and results are durable across service restarts. Conflicting reuse of a retry request ID returns 409.
- Backoffice adds **Retry with agent** and optional budget/model controls. It uses the separate manual queue with automatic assistance and the usual 10-second human fallback. **Retry interactively** keeps its existing operator flow.
- Retrying is explicitly requested; this change does not automatically enqueue repeated crawls or resume from saved pages.
- `challenge_agent_model`: `deepseek-flash` (default) or `z-ai/glm-5.3-flash`. This is separate from the discovery LLM.
- CLI flags: `--challenge-agent-max-runs` and `--challenge-agent-model`.
- Effective budget, model, exhaustion flag, and complete agent history are in live status, SQLite history and the S3 result JSON.

## Live controlled test

Used the existing local cross-origin iframe/button fixture on server port 18184.
This is a controlled verification UI, not a real third-party CAPTCHA.
The initial GLM request was submitted through Backoffice → JetStream (`COMPANY_CRAWL`, sequence 13).

OpenRouter rejected the initial attempt because GLM requires reasoning. Its error
was `Reasoning is mandatory for this endpoint and cannot be disabled.` We changed
GLM to low reasoning effort and a 4096-token response allowance. DeepSeek retains
disabled reasoning and a 1024-token response allowance. Both use the same visual
prompt/action schema, screenshots, 12-decision limit and 120-second run limit.
No alternate model fallback is used.

| Request | Budget | Outcome |
|---|---:|---|
| `glm-fixture-three-20260919` | 3 | Provider rejected disabled reasoning; zero pages saved |
| `manual-420c7e99-ab2c-4faa-84b0-66aab87fa374` | 3 | Backoffice agent retry after provider failure retained 3; three challenges solved, then exhausted at page four; partial result saved |
| `manual-d5a8b1b9-28a8-4114-85ec-078be2733c2a` | 6 | Backoffice agent retry doubled to 6; all four challenges solved and all four pages saved |
| `deepseek-fixture-compare-20260919` | 3 | All three matching fixture pages solved and saved |

Backoffice visibly showed “Agent retry queued with 6 runs”, then “Crawl completed”
and “4 / 6 automatic agent runs used”. All final JSON results were read back through
the Backoffice ClickHouse/S3 mapping and their agent histories matched live job state.

### Same three fixture pages

| Model | Runs verified | Mean elapsed/run | Median elapsed/run | Mean reported tokens/run |
|---|---:|---:|---:|---:|
| DeepSeek V4.1 Flash | 3/3 | 3.315 s | 2.781 s | 4009.7 |
| GLM-5.3 Flash, low reasoning | 3/3 | 5.130 s | 4.162 s | 5517.0 |

GLM's individual times were 7.379, 4.162 and 3.849 seconds; DeepSeek's were
4.554, 2.611 and 2.781 seconds. This is a small sequential sample with different
providers, tokenizers and reasoning settings, not a statistically reliable ranking.
Times include agent screenshots/actions/waits, not the complete crawl or S3 upload.
The subsequent four-page GLM retry also verified 4/4 (4.876, 6.229, 4.526, 7.635 s).

## Real Framework check

`glm-framework-low-20260919` retried the initial rejected GLM request with budget 3
and automatic assistance. This was a targeted two-page crawl, not another full
100-page discovery run:

- `https://frame.work/`
- `https://frame.work/laptop16?slug=laptop16-diy-amd-7040&tab=specs`

GLM run `195d20529493450e8084ea57058d7892` clicked the real Cloudflare checkbox and
waited five seconds. The third model response failed action-schema validation.
The journal therefore records `state: error`, but the crawler's fresh independent
capture verified access (`accessVerified: true`) and continued. The crawl completed,
saving both pages, including the exact URL that stopped the earlier full crawl.
Agent elapsed: 12.340 seconds; 8245 prompt + 121 completion = 8366 reported tokens.
This model-output issue remains visible in the saved agent evidence; it was not
changed into a model-reported success. The earlier DeepSeek homepage run took
9.834 seconds, but it was a separate session/time and is not a controlled comparison.

Browser sessions used the deployed native headed Xvfb service with saved profiles.
No cookies were cleared or copied and no human clicked either live test challenge.

## Validation and evidence

- 15 automatic-assistance tests, including real crawler execution through a mocked browser HTTP boundary, 3 → 6 → 12 escalation across restart, explicit overrides, and rejection of invalid budgets.
- 42 service tests with a real local NATS server; 4 human-assistance tests; 5 crawl tests.
- 7 browser-agent tests; 12 browser API tests, including GLM endpoint/authentication/payload/CDP behavior.
- 19 Backoffice tests, TypeScript check and production build.
- Ruff checks/formatting, Git whitespace check, and both Ansible syntax/deployment checks.

Ignored local evidence: `data/glm-agent-20260919/*-{job,result,summary}.json`.
S3 paths use `crawls/company-crawls/<request-id>/attempts/0001/result.json.gz` and
matching `artifacts.tar.gz`. Temporary fixture server stopped after validation.

Provider reference: [GLM-5.3 Flash on OpenRouter](https://openrouter.ai/z-ai/glm-5.3-flash).
