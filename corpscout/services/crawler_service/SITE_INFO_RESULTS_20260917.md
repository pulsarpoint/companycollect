# Brief site information — 17 September 2026

Version 0.21.0 adds `crawler-service-crawl URL --site-info --output-dir PATH`.
Used alone, the flag reads only the input page and returns a short description.
Combined with `--instructions`, `--instructions-file`, `--pages` or `--crawl`, it
includes the description and continues the requested crawl after site admission.
The existing first-page classification call provides the description; no separate
summary or fact-extraction call is added.

Company and non-company sites share the same `site_info` object, also saved in
`site-info.json`. It contains description, operator, purpose, business activities,
site types, crawl decision, source URL, first-page scope and supporting evidence.
Skipped sites return it automatically even when the flag is absent. Unavailable
pages and unmatched evidence produce an explicit unknown brief with no asserted
operator or activities.

## Live validation

Both commands used `--site-info`, `--max-model-calls 3`, the existing local env
file and separate empty output directories. No page URLs or instructions were
supplied. Direct DeepSeek Flash/high was used.

| Site | Result | Pages fetched | Model calls | Duration | Description |
|---|---|---:|---:|---:|---|
| NOVELIC | finished / site_info_complete | 1 | 1 | 10s | 4 sentences, 100 words |
| B92 | skip_crawling / not_company_website | 1 | 1 | 12s | 3 sentences, 71 words |

NOVELIC's description covers mmWave radar products, embedded engineering,
product-development and semiconductor services, and the markets described on its
homepage. B92's description covers its Serbian-language news content and portal
functionality. The returned objects have identical field names. These are
descriptions of the captured pages, not independent verification of their claims.

Both runs had no errors. Each made exactly one `site_classification` call, no
link-assessment calls, no sitemap requests and no follow-up page fetches. Cleaned
and rendered capture hashes validate. The company capture also passes the normal
saved-crawl loader. B92 remains excluded from company analysis by its skip status.

| Site | Input tokens | Output tokens |
|---|---:|---:|
| NOVELIC | 23,362 | 713 |
| B92 | 116,573 | 737 |

Prices were not returned by the API; reported known cost of zero must not be
interpreted as free usage. B92's large homepage still produces a large input.
This change does not alter HTML cleaning or input compaction.

Artifacts are local under [site-info-20260917](data/site-info-20260917):

- [NOVELIC brief](data/site-info-20260917/novelic/site-info.json)
- [B92 brief](data/site-info-20260917/b92/site-info.json)
- [Audit](data/site-info-20260917/audit.json)
- [Experiment settings and source hashes](data/site-info-20260917/experiment.json)

The folder also retains complete captures, requests/responses, source snapshots
and stdout/stderr. Runtime source hashes stayed unchanged throughout the live runs.

## Automated checks

192 package tests ran successfully, with six skipped. Eight new tests cover information-only
mode, CLI JSON output, combined list/instruction/general discovery, skipped-site
output shape, redirect provenance, failed fetches and unmatched evidence, page
budgets and blank descriptions. Existing exact-list tests preserve zero model
calls when the new flag is absent. Runtime type and changed-file Ruff checks pass.

With a list, the flag explicitly adds the input page for the brief; it counts
toward the page budget. The first-page gate can stop the list, and subsequent
selection remains confined to the list. Information-only mode retains the
first-page HTML and `site_coverage: not_established`; it does not imply site-wide
coverage. No NACE lookup, detailed fact extraction, upload or database write ran.
