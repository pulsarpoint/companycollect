# Compact page selector — 17 September 2026

Version 0.22.0 separates custom-instruction selection from broad company research.
On NOVELIC, the final compact selector captured the same 18 URLs and all 16 jobs
listed on the fresh Careers page in **2m34s**, versus **8m41s** previously. Output
tokens fell **78.51%**, with no fetch or model errors. The model and reasoning
effort stayed direct DeepSeek `deepseek-flash` / high.

[Browse the final HTML captures](data/compact-selector-20260917/index.html),
[inspect the audit](data/compact-selector-20260917/audit.json), or
[read the frozen-input comparison](data/compact-selector-20260917/replay-final/comparison.json).

## Implementation

Custom instructions now use a standalone `RequestedContentAssessment` with only
candidate ID, requested-content potential/role, target relevance, follow scope
and one reason of at most 300 characters. The prompt no longer asks for ten broad
objective assessments or includes their definitions/coverage. Target scope is
required explicitly; it does not silently default when omitted. The default
general selector retains its existing prompt and full objective schema.

Page-list restrictions, external-navigation permissions, budgets and capture
format remain enforced by the queue. The schema does not extract company facts.
For job-description requests, the prompt explicitly excludes application forms,
referral/CV-check steps and candidate sign-in unless that content was requested.

## Controlled replay

Replayed all ten saved NOVELIC link-assessment inputs with identical candidate
metadata and site-profile hypotheses, including the repeated batch that previously
needed correction. No pages were fetched for this comparison. Both variants used
the same model and high reasoning; the custom prompt/schema changed.

| Measure | Previous broad schema | Final compact schema |
|---|---:|---:|
| Candidate occurrences | 187 | 187 |
| Model calls | 10 | 10 |
| Input tokens | 63,770 | 44,715 |
| Output tokens | 125,705 | 21,887 |
| Sum of model-call elapsed time | 467.230s | 87.048s |

All ten compact responses passed the strict schema and complete candidate-ID
checks. **187/187 queue-eligibility decisions agreed**, with **82.59% fewer output
tokens**. This is agreement with the baseline, not independent destination accuracy.
Seven potential/role assessments differed without changing eligibility. Two
irrelevant patent indexes changed from target navigation to single-page scope;
both remained excluded. Target-relevance classifications all agreed.

## Fresh NOVELIC crawl

Both crawls started from the homepage, without supplied careers/job URLs, using
the same jobs instructions and limits of 30 pages, 20 external pages and 60 calls.

| Measure | Previous run | Final compact run |
|---|---:|---:|
| Successful captures | 18 | 18 |
| Listed jobs captured | 16/16 | 16/16 |
| Model calls | 11 | 11 |
| Input tokens | 86,972 | 68,079 |
| Output tokens | 126,343 | 27,146 |
| Wall time | 8m41s | 2m34s |

Wall time decreased **70.44%**, input tokens **21.72%**, and output tokens **78.51%**.
The URL sets match exactly: homepage, Careers and 16 OneAssessment descriptions.
All cleaned job-content sections match their rendered counterparts; all capture
hashes and the saved-crawl loader validate. No application form was captured.

The frozen replay isolates selector inputs more tightly than a live run. Live
site content, provider scheduling and model output can vary; these are individual
measurements, not a universal latency guarantee. The earlier first-page classifier
also predates the v0.21 brief-description wording, so its small contribution is
included only in the whole-crawl figures, not the controlled replay.

## Broader smoke tests

The following four sites used the initial compact prompt, before the explicit
application-form refinement. Their results test the smaller schema and traversal
on different sites; they are not a full rerun of the final wording. B92 and Google
stop before selection and therefore do not exercise that wording.

| Site | Successful / attempted pages | Calls | Elapsed | Outcome |
|---|---:|---:|---:|---|
| Oxide | 16/16 | 19 | 5m58s | Finished; all 13 job links on its captured Careers page have descriptions |
| Vensas, German homepage | 4/4 | 15 | 2m22s | Partial: captured German, English and French careers pages, then hit the configured 15-call budget |
| RT-Labs | 9/11 | 12 | 3m40s | Partial: found its Swedish vacancy; two unresolved template links returned 404 |
| B92 | 1/1 | 1 | 12s | skip_crawling at the company-site gate |
| Google | 0/1 | 0 | 1s | needs_review: robots.txt denied the initial fetch |

Oxide's 16 pages include its homepage, Careers, 13 descriptions and a careers feed.
Vensas's captured German Careers page explicitly says there are no open positions;
the model budget prevents claiming exhaustive site coverage. Its 15-call limit is
lower than the package default of 100 and the NOVELIC benchmark limit of 60.

RT-Labs has unresolved `[#DSR_FORM_URL#]` links with matching template labels in
its rendered source. URL normalization leaves paths ending in `/[`, two of which
were selected and returned 404. The run also visited several general language
homepages as speculative gateways. Those extra fetches remain specificity issues;
the partial status preserves the actual failures. Do not present every site as a
successful complete crawl or interpret no matching candidates as proof of no jobs.

## Experiment history and cost accounting

The initial compact NOVELIC run finished in 3m10s using 34,311 output tokens. It
captured all jobs but added two employer-board pages and one application form.
That form motivated the final prompt refinement; original outputs remain under
`novelic/`, and the final rerun is under `novelic-final/`.

The first replay harness stopped after four completed calls because an old
correction request appended prose after its JSON input. The harness now reads
that first JSON object with `raw_decode`; the final ten-batch replay completed.
The incomplete replay's requests, responses and error log remain under `replay/`.

All experiments, including initial runs, retries of the experiment and both
replays, used **84 model calls**, with **632,187 input / 247,629 output tokens**.
Every call returned usage. No call returned a price, so known cost zero must not
be interpreted as free usage. No estimated billing total is supplied.

Initial and final runtime source snapshots are separate; the final snapshot
matches the delivered runtime. No production prompt was edited inside a running
process. Outputs, captures, call records and audits are local under
`data/compact-selector-20260917/`. No fact-extraction workflow, uploads, catalog
writes or company-database writes were run in this selector task.

## Reproduce and validate

```sh
.venv/bin/python benchmarks/compare_selector.py \
  data/novelic-jobs-instructions-20260917/crawl \
  --output-dir runs/selector-replay --env-file .env

.venv/bin/crawler-service-crawl https://www.novelic.com/ \
  --instructions "Collect current job listings and full job descriptions. Skip employee stories and other employers." \
  --output-dir runs/compact-novelic \
  --max-pages 30 --max-external-pages 20 --max-model-calls 60 \
  --env-file .env
```

Use new empty output folders. The package test suite contains 194 tests, with six
skipped. It covers both selector modes, scope and list restrictions, multilingual
gateways, external employer-board traversal, missing/invalid custom assessments,
required scope fields and bounded reasons. Ruff and runtime/benchmark type checks
pass. Live coverage here means links on the captured listings; whole-site coverage
remains `not_established`.
