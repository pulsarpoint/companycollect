# Direct DeepSeek comparison — 10 September 2026

Direct `deepseek-flash` recovered more records under the current pipeline budgets,
but it was more expensive and still missed useful information. This is one paired
run plus one job-page extraction experiment, not a general model ranking.

DeepSeek identifies `deepseek-flash` as **DeepSeek-V4.1-Flash** in its
[model documentation](https://api-docs.deepseek.com/quick_start/pricing/).
The returned model label was `deepseek-flash`. The comparison endpoint returned
`deepseek/deepseek-v4-flash-0731`, routed through OpenRouter/Wafer. These labels and
response IDs are saved; neither supplies an independently verified weights hash.

## Setup

- Package 0.14.4, optional statement schema `page-statements/1.3`.
- Direct URL: `https://api.deepseek.com/chat/completions`.
- Direct credential: `DEEPSEEK` from `jobs_extraction_lab/.env`; the OpenRouter arm
  reads `OPENROUTER_API_KEY` from the same file. Keys were not copied into code or
  artifacts. All 312 generated source/JSON/HTML artifacts were checked for leaks.
- Same frozen native Crawl4AI HTML, catalog, current task prompts and validators.
  Both arms use JSON object mode and receive the identical schema in the system
  prompt. Application models validate the output. Direct DeepSeek documents JSON
  object output, rather than OpenRouter's strict JSON schema request format.
- Low reasoning, 65,536 maximum output tokens, 120-second deadline per model request,
  two review attempts and one correction. The ceiling is an experiment setting,
  not the model's maximum. No returned response ended with `finish_reason=length`.
- Direct requests use `thinking` and `reasoning_effort`. Tool continuations preserve
  returned `reasoning_content`, as required by DeepSeek's
  [tool-call documentation](https://api-docs.deepseek.com/guides/thinking_mode/).
  Direct thinking mode ignores temperature, so it is omitted for that arm.
- No recrawl, deployment, database submission or administrator proposal approval.

The endpoints, serving stacks and model versions differ. Later prompts naturally
diverge when prior answers produce different corrections or catalog searches. Source
hashes and initial prompts match between the arms. Expected controls were not sent
to either model. Normal crawler/CLI defaults still use OpenRouter; direct API use
is explicit in these benchmarks.

## Replay of the 31 saved records

This repeats the selected attribution failures from the preceding experiment using
the same current code for both arms, without another page extraction pass.

| Measure | Direct V4.1 Flash | OpenRouter pinned V4 Flash |
| --- | ---: | ---: |
| DMC descriptions accepted | 13/16 | 0/16 |
| DMC observations retained after existing identity hold | 11 | 0 |
| thoughtbot descriptions accepted | 10/10 | 10/10 |
| thoughtbot observations passing source/catalog checks | 12 | 7 |
| Earlier thoughtbot relationship controls retained | 9/12 | 5/12 |
| Plausible company/person records recovered | 4/4 | 4/4 |
| MCAP relationships retained | 2/2 | 2/2 |
| HTTP attempts / responses with usage | 47 / 43 | 33 / 29 |
| Deadline failures | 4 | 4 |
| Completed HTTP responses with invalid/truncated JSON | 0 | 0 |
| Reported input tokens | 370,709 | 229,042 |
| Reported completion tokens, including reasoning | 122,364 | 51,727 |
| Reasoning tokens | 105,041 | 43,500 |
| Median successful first-attempt response time | 9.97 s | 9.42 s |
| Whole arm elapsed time | 11.70 min | 10.13 min |
| Cost for responses with usage | **$0.21812 estimated** | **$0.034735 reported** |

Direct DMC output includes seven industrial-service observations, React and Next.js usage,
Arduino Cloud and Ignition expertise, and a neutral Zephyr RTOS mention before the
AVEVA hold is applied. AVEVA remains too broad a vendor/product-portfolio label and
is held, leaving 11 observations. **Zephyr is only a mention, not evidence of use.**
The React/Next.js excerpts explicitly describe DMC's usage. Portfolio project titles
support expertise, without proving internal production deployment.

Mitsubishi MELSEC Motion, Siemens SIMOTION and SharePoint descriptions still need
recovery. Neither endpoint's accepted outputs contained the previous wrong actors,
unsupported technology-sales signals or invented negative-use claims. OpenRouter's
zero DMC count reflects failed correction/validation under this budget, rather than
evidence that it cannot understand DMC in every configuration.

thoughtbot counts are observations per page and relationship, not unique technologies.
Both arms retain 9/12 earlier controls before catalog checks; direct retains all nine
afterward, versus five for OpenRouter. Its larger accepted count also contains new
expertise observations, so it does not establish complete relationship recall.
Plausible's four records remain source claims: operating since 2018, team of ten,
and Uku/Marko as cofounders; legal incorporation, payroll and full names are unverified.

The old cumulative 0.14.1–0.14.3 recovery results remain separate. Do not compare their
combined retries and older strict-schema prompts as if they were this paired run.

Artifacts: [paired summary](data/deepseek-direct-20260910/comparison.json),
[manual review](data/deepseek-direct-20260910/manual-audit.json), and each arm's
`manifest.json`, per-case `input.json`, `result.json`, HTML and `calls/` files.

## New extraction from the saved NOVELIC job page

Both models received the same 19,066-character native HTML of the Senior Data
Engineer/Data Architect advert, including its cookie banner. No selectors or manual
HTML cleanup were introduced. This runs page-statement extraction, description review,
normalization, source review and local catalog/proposal review.

| Measure | Direct V4.1 Flash | OpenRouter pinned V4 Flash |
| --- | ---: | ---: |
| Initial statements / quotations matched | 19 / 19 | No extraction response |
| Descriptions accepted | 17/19 | 0 |
| Source-reviewed technology observations | 13 | 0 |
| Observations passing catalog/proposal checks | 12 | 0 |
| Targeted source controls retained | 12/18 | Unmeasurable: extraction failed |
| HTTP attempts / responses with usage | 27 / 25 | 2 / 0 |
| Deadline failures | 2 | 2 |
| Reported input / completion tokens | 159,560 / 75,082 | Unknown |
| Cost for responses with usage | **$0.12276 estimated** | Unknown |

OpenRouter exceeded the 120-second deadline on both extraction attempts. Its output
correctly lists the page as pending extraction. The control JSON records zero matches,
but that is an execution failure, not an accuracy score for a completed answer.

Direct output preserves NOVELIC, the exact job title and role scope. It separates AWS
use from required experience, and MCAP use from preferred experience. Python/Linux/
FlatBuffers stay requirements; ROS2, rosbag2, Foxglove, DVC and LakeFS stay preferences.
The source's alternatives and examples remain in the context/evidence: these fields
must not mean every listed option is mandatory or already deployed company-wide.

The useful remaining failures are:

1. **S3, IAM, Lambda, Glue and Athena are not separate observations.** The first pass
   keeps them inside one AWS description. Downstream processing never recovers the
   individual identities. Page-level compression needs one named item per statement,
   including products inside parenthetical lists.
2. **Protobuf passes source review but fails proposal metadata review.** The reviewer
   rejects the expansion “Protocol Buffers” because the job page does not supply it.
   This illustrates the need to verify aliases/product identity separately from
   evidence that a company uses or asks for the technology.
3. **The general job-summary description is held unnecessarily.** Its nullable
   `source_name` conflicts with a reviewer-reconstructed job title, despite supported
   prose. The validation rules for generic company/job statements need separate handling.

Generic camera/IMU candidates appear during intermediate extraction/normalization,
but do not survive accepted specific-technology output. Neither cookie-banner vendors
nor a generic AWS/cloud-certification preference becomes an accepted company credential.
This experiment covers the optional statement workflow, not the normal crawler's
complete jobs-objective export or navigation.

Artifacts: [job comparison](data/deepseek-direct-20260910-job/comparison.json),
[manual review](data/deepseek-direct-20260910-job/manual-audit.json),
[source-read controls](data/deepseek-direct-20260910-job/controls.json), and each arm's
`extracted-statements.json`, `result.json`, saved HTML and request/response files.

## Cost interpretation and next experiment

All observed calls ran during the documented weekday peak window. Direct estimates
use $0.30/M uncached input, $0.006/M cached input and $1.20/M completion tokens from
the [10 September pricing page](https://api-docs.deepseek.com/quick_start/pricing/).
DeepSeek returns token usage, not billed cost. The artifacts retain both peak and
off-peak estimates; off-peak would be half as much at these rates.

Including the one-call connection smoke test, there were 110 HTTP attempts: 75 direct
and 35 OpenRouter. Direct observed usage estimates total **$0.34102** at peak rates;
OpenRouter reports **$0.034735**. Six deadline calls per endpoint have unknown usage
and additional unknown cost. A failed request is not free. Earlier console output
printed `$0.000000` for missing direct billing; that was an empty reported-cost sum,
not a charge. The harness now prints `unavailable` in that situation.

Direct made more progress and therefore performed more downstream work. Its high
reasoning-token share and different tariff also matter; this is not a cost comparison
for identical completed outputs. Successful-call medians exclude timeouts and do not
describe full workflow latency.

V4.1 Flash is a promising candidate for further tests. Before changing the crawler's
default, compare non-thinking versus low reasoning on the same saved pages, preserving
the current deadlines and all failures. Separately address list-item completeness,
identity metadata versus source attribution, and generic job-summary validation. Do
not silently patch these prompts inside the completed comparison.

## Reproduce

Run from `companycollect/corpscout/services/company_research`. Output directories must be new:

```sh
.venv/bin/python benchmarks/compare_deepseek_direct.py \
  --env-file .env \
  --output data/deepseek-direct-repeat

.venv/bin/python benchmarks/compare_deepseek_job_page.py \
  --env-file .env \
  --output data/deepseek-job-repeat
.venv/bin/python benchmarks/audit_deepseek_job_page.py \
  data/deepseek-job-repeat
```

For direct-only replay, use `replay_attribution_failures.py --api deepseek --model
deepseek-flash --json-mode json_object --output <new-directory>`. Both comparison
scripts contain frozen model choices; the reusable `ModelClient` accepts an explicit
API dialect and preserves the original OpenRouter defaults.

Validation: 134 regular tests pass; three existing browser tests were skipped because
this change does not touch browsing. Ruff, source/new-client/new-comparison type checks
and 0.14.4 wheel/source builds pass. No model requests remain running. Local `data/`
artifacts are Git-ignored and must be preserved when resuming.
