# NOVELIC jobs crawl — 17 September 2026

The v0.20.0 crawler discovered Careers from the NOVELIC homepage and saved all
16 job-description URLs listed on the freshly captured Careers page. No careers
or job URLs were supplied to the discovery run. It finished in **8m41s**, saving
18 pages with no fetch/model errors and using 11 navigation model calls.

Browse the [local HTML index](data/novelic-jobs-instructions-20260917/index.html),
[machine-readable audit](data/novelic-jobs-instructions-20260917/audit.json), or
[crawl manifest](data/novelic-jobs-instructions-20260917/crawl/crawl-manifest.json).

## Live cases

| Case | Result | Captures | Model calls | Elapsed |
|---|---|---:|---:|---:|
| Homepage + jobs instructions; no supplied pages | Found Careers and all 16 linked job descriptions | 18 | 11 | 8m41s |
| Explicit list of two discovered job URLs; no instructions | Fetched exactly those two pages | 2 | 0 | 7s |
| Explicit list containing Careers and Contact + jobs instructions | Selected Careers only; did not follow its job links outside the list | 1 | 2 | 25s |

All three returned `status: finished`. Discovery and the filtered list stopped
with `no_matching_candidates`; the exact list stopped with
`supplied_pages_exhausted`. A supplied list is an allowlist. The supplementary
cases ran independently while discovery continued and did not feed URLs or
assessments back into it.

## Discovery configuration

```sh
.venv/bin/crawler-service-crawl https://www.novelic.com/ \
  --instructions-file data/novelic-jobs-instructions-20260917/instructions.txt \
  --output-dir data/novelic-jobs-instructions-20260917/crawl \
  --max-pages 30 \
  --max-external-pages 20 \
  --max-model-calls 60 \
  --env-file .env
```

Instructions: “Collect current job listings and full job descriptions. Skip
employee stories and other employers.” Repeating the command requires a new
empty output directory. The model was direct DeepSeek `deepseek-flash` with
high reasoning. The only task types were `site_classification` (one call) and
`link_assessment` (ten calls). Every link-assessment request contained the custom
instructions. No fact-extraction or final-classification stage was run.

The queue contained 168 candidates and finished with zero unassessed candidates
or assessment failures. About, Contact, Mechanical Engineering and Blog were
explicitly assessed as low relevance to the requested content and not fetched.
The 18 captures are the homepage, Careers and 16 NOVELIC OneAssessment job pages.

## Source and output checks

- All 16 job URLs from the fresh Careers capture have successful HTML captures.
- Every job capture contains its title, NOVELIC employer text and job description.
- All job-content sections have identical normalized text in the cleaned and
  rendered captures, including qualifications and benefits wherever published.
- The DSP internship puts its competencies inside Job Description rather than
  a separate Requirements section. That content was preserved.
- `load_crawl` successfully validated all 21 snapshots across the three cases,
  including source metadata, paths and cleaned/rendered HTML hashes.
- The source snapshot hashes match the runtime files after the experiment; no
  runtime code changes were needed during this test.

Captures are ordinary UTF-8 HTML with metadata alongside them. Native Crawl4AI
cleaning retains some menus, cookie text and employee stories embedded in the
Careers page; custom instructions select pages, not individual text sections.
The separate analysis module can consume these saved pages later.

## Usage and limits

| Case | Input tokens | Output tokens |
|---|---:|---:|
| Discovery | 86,972 | 126,343 |
| Exact list | 0 | 0 |
| Filtered list | 7,022 | 4,128 |

All 13 model calls returned token usage, but none returned a price. The manifest's
`known_cost_usd: 0` is accompanied by `unknown_cost_calls`; it does not mean the
calls were free. No billing estimate was added.

The 16/16 result measures coverage of links on the captured Careers page. It
does not establish whole-site coverage or independently verify that vacancies
remain open. The manifest retains `site_coverage: not_established`.

Selection worked, but it still generated 126k output tokens while ranking links
with the broad company-assessment schema. A smaller response schema for custom
instructions is a possible future performance improvement; it was not changed
or benchmarked in this test.

Requests, responses, source snapshots, experiment settings, stdout/stderr, raw
captures and the reproducible `audit.py` are retained under
`data/novelic-jobs-instructions-20260917/`. Everything is local; no uploads or
company-database writes occurred.
