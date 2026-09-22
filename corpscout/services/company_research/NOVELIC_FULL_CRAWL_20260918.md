# NOVELIC full-crawl benchmark — 18 September 2026

The live `--crawl full` run ended with **partial** and stop reason
`no_matching_candidates`. It collected **68 pages** in
**8m 30s**, using **198,482 reported tokens**.
This is automatic discovery from the homepage, with no supplied page list or custom
selection instructions. Technology inference and LLM job analysis were disabled.

```bash
company-research https://www.novelic.com/ --crawl full \
  --output-dir runs/novelic-full --env-file /path/to/crawler.env
```

## Run and usage

| Measurement | Result |
| --- | ---: |
| Model | deepseek-flash |
| Reasoning | high |
| Pages attempted / collected | 76 / 68 |
| Wall time | 510.199 s |
| LLM time, summed provider calls | 263.837 s |
| Fetch/render and link processing | 220.042 s |
| Additional static observations and capture writes | 23.173 s |
| LLM HTTP calls | 40 |
| Input tokens | 146,636 |
| Cached / uncached input tokens | 76,160 / 70,476 |
| Completion tokens, including reasoning | 51,846 |
| Reasoning tokens (part of completion) | 36,187 |
| Total tokens | 198,482 |
| Calls without reported usage | 0 |

Only these LLM tasks ran: `{'site_classification': 1, 'link_assessment': 39}`. Collection is sequential, so model
latency adds directly to elapsed time. Fetch timing includes browser rendering,
Crawl4AI cleanup and link processing. Static-capture timing covers additional
metadata/contacts/JSON-LD/text parsing, inventories, hashing and artifact writes.
The wall timer wraps the CLI invocation, excluding Python imports; browser startup,
sitemap work and final JSON writing are included. Summed provider timings can
overlap retry time when a request is retried; the per-call receipt records attempts.

## Collected source data

- JobPosting objects: **16**, with **16** full descriptions.
- Discovered company job-detail URLs: **16**; missing from captures: **0**.
- Distinct email strings: **7**; phone strings: **4**.
- Candidate HTML URLs: **205**; retained document-reference URLs: **78**.
- Unique captured page-link destinations: **2445**.
- Heuristic financial-information links: **20**.
- Cleaned HTML: **4.79 MB**; rendered HTML: **10.76 MB**.
- Full portable JSON: **31.13 MB** (uncompressed, includes metadata and duplicated source representations).
- Failed pages: **3**; crawl errors: **0**; captures with observation errors: **2**.

The 76 attempts produced 68 captures, five duplicate redirects and three HTTP
failures: `/systems-engineering/` and `/mechanical-enginering/` returned 404;
`/docs/` returned an anti-bot 403. These failures explain `status=partial` even
though the selector exhausted its useful candidates. The correctly spelled
`/mechanical-engineering/` page was captured. Job coverage compares discovered detail URLs with captured final
and requested URLs, accounting for redirects and excluding apply/referral forms.

All 20 financial-link candidates came from external NOVELIC partner-profile pages
on Analog Devices, Infineon and Lattice Semiconductor. They are navigation links
to those companies' investor information, **not established NOVELIC financial
reports**. No financial-link candidates were recognized on the captured novelic.com
pages. Five email strings use `@novelic.com`; partner-page strings include a raw
escaped-HTML regex artifact (`u003ewebmaster@latticesemi.com`). Contact strings
therefore require source-aware cleanup before treating them as company contacts.
Two external partner pages contained invalid JSON-LD; the raw blocks and parser
errors were retained alongside their otherwise usable captures.

Each `documents[]` entry includes `url`, simplified/cleaned `html`, `input.links`,
headings and deterministic `input.observations`, plus original rendered HTML.
Source descriptions and structured claims remain unverified; data is not merged
into inferred company facts. Missing structured job fields are not invented.
The absence of NOVELIC financial-link matches does not establish that the site
has no financial information outside these captures or this keyword heuristic.

The crawl used the full-mode defaults: 100 pages, 30 external pages and 100 model
calls. LLM decisions and bounded discovery do not prove every useful page was found.
This is one run on a local machine with the existing browser runtime and provider
prompt caching, not a controlled cold-start benchmark.

An earlier diagnostic attempt was stopped when `.pdf/` links bypassed the document
filter. The filter and regression tests were fixed before this measured run.
The diagnostic artifacts are preserved separately in
`data/novelic-full-crawl-20260918`; their time/tokens are excluded from this table.
That stopped diagnostic attempted 30 pages in
262.841 seconds and reported 132,394 tokens.

## Artifacts and validation

- [Machine-readable summary](data/novelic-full-crawl-20260918-verified/summary.json)
- [Portable crawl result](data/novelic-full-crawl-20260918-verified/crawl/result.json)
- [Raw job records](data/novelic-full-crawl-20260918-verified/jobs.json)
- [Reproducible benchmark runner](data/novelic-full-crawl-20260918-verified/run.py)
- [Recorded invocation](data/novelic-full-crawl-20260918-verified/benchmark-input.json)

Portable capture/hash replay and ClickHouse row mapping passed. No S3 upload,
database import or server deployment was performed for this benchmark. The package
suite passed 255 tests, with 14 optional/live checks skipped, including full-mode
CLI, REST and real JetStream coverage and the document-URL regression.
