# Fresh company page-context benchmark

**Result: keep this workflow experimental.** The new multiple-relationship support
works, but the fresh benchmark exposed attribution errors, relationship mistakes and
processing failures that the recovered NOVELIC result did not reveal. Of 31 technology
observations accepted by automated gates, manual review held 19. The remaining 12
observations all came from thoughtbot. This is not a production accuracy claim.

Start with the [audited output index](data/fresh-company-statements-v1/README.md).
The original model outputs and automatic acceptance decisions remain unchanged.

## Frozen protocol — 9 September 2026

Package 0.13.0; optional statement schema `page-statements/1.2`. Test five websites
without NOVELIC-specific page selections or facts: DMC (engineering), Plausible
(software), thoughtbot (services), OnLogic (manufacturer), IC Resources (staffing).
Each starts with only its homepage. Crawl4AI returns native cleaned HTML; the existing
controller discovers sitemaps and links and selects the next pages. No site-specific
CSS selectors, manually supplied detail URLs, interactive logins or PDF parsing.

Each site has a ten-page budget, up to three external pages, 100 direct-crawl model
requests and 120 page-statement requests. Sites run two at a time. DeepSeek
`deepseek/deepseek-v4-flash-0731`, Wafer, low reasoning, 65,536 output-token ceiling,
120-second request deadline. The same frozen catalog and implementation are used
for all five. Preserve blocked or inaccessible sites in results; do not replace them
after seeing their scores. Do not tune prompts or replay failed sites during this run.

The direct crawler examines all existing objectives and saves its normal result.
Fresh page descriptions are then extracted from every successfully fetched page,
reviewed against that page, normalized, checked for source meaning, and matched to
the catalog or proposed as new identities. The final experimental JSON replaces only
technology/certification objectives with these statement results. Other objectives
retain direct extraction. The original direct result remains available as a baseline.
The two paths share the fetched HTML, not generated descriptions or review decisions.

One statement can now produce multiple distinct source-supported technology
relationships. Each has its own source review and record ID. Identical duplicates
and conflicting routes remain errors. This supports both role usage and preferred
experience without upgrading a requirement alone into usage.

## Evaluation

- Record attempted/fetched pages, selected URLs, candidate assessments, stop reasons
  and per-objective coverage. Distinguish a missing fetch, unexamined content,
  extraction omission, source-evidence failure, relationship error and catalog hold.
- Preserve all raw/accepted/held records, original and corrected descriptions,
  source hashes, model requests, response text, elapsed time and known/unknown costs.
- Manually inspect accepted observations against saved HTML, sampling technologies,
  job attribution, services, credentials, contacts and company relationships. Record
  checked IDs and source quotations. Inspect the homepage and relevant fetched job or
  service pages for omissions. This sample is not exhaustive ground truth.
- Pay particular attention to client versus recruiter attribution, offered technology
  versus internal use, optional versus required job skills, company versus staff or
  product certificates, and generic formats/methods misclassified as technology.
- Report objective counts as observations, not verified facts or unique technologies.
  No results is not proof of absence. Site blocking and budget exhaustion are outcomes.
- Costs include two extraction paths and therefore do not estimate a single optimized
  production crawl. No database proposals are submitted or automatically approved.

Reproduce in a new output directory:

```sh
.venv/bin/python benchmarks/fresh_company_statements.py \
  --output data/fresh-company-statements-v1 \
  --catalog data/novelic-page-statements-v2-validated/technology-catalog.json \
  --env-file .env
```

## MCAP regression and implementation checks

The isolated saved MCAP description produced both `stated_use / role` and
`preferred_experience / role`, and both passed a separate raw-source review. The
company, job and source URL were preserved. This used two successful model calls,
$0.00176125 reported, with no unknown-cost calls. It did not rerun NOVELIC or change
the historical 31/32 score. Artifacts: [MCAP regression](data/mcap-multiple-relationships-v013/result.json).
This checks normalization and source meaning; catalog metadata was not rerun.

Implementation checks: 123 regular tests plus three opt-in browser tests passed;
Ruff, source/focused type checking, `git diff --check`, wheel and source builds pass.
The broader historical benchmark/test type-check diagnostics described in the
previous report are outside the source-package check.

## Results

The run finished on 9 September 2026, 09:19–10:44 UTC, approximately **85 minutes**.
All five sites returned HTML. Only **11 pages** were fetched across the five sites,
because all direct crawls stopped under the crawler's `model_unavailable` rule after
repeated request errors. None reached the ten-page budget. No site was replaced or
replayed, and extraction prompts were not tuned during the run.

| Site | Pages fetched | Fresh descriptions | Descriptions passing LLM review | Automatically accepted technology observations | Remaining after manual holds |
| --- | ---: | ---: | ---: | ---: | ---: |
| DMC | 1 | 88 | 54 | 14 | 0 |
| Plausible | 4 | 0 | 0 | 0 | 0 |
| thoughtbot | 2 | 28 | 27 | 17 | 12 |
| OnLogic | 3 | 0 | 0 | 0 | 0 |
| IC Resources | 1 | 20 | 17 | 0 | 0 |

The zero-description results for Plausible and OnLogic are processing failures:
each statement path made three failed requests and then stopped making requests.
They are not evidence that the pages lack technologies or certifications. IC Resources
did produce descriptions; its generic recruiting-sector candidates were excluded.

The direct baseline accepted three technology mentions on Plausible (Google Workspace,
Okta and Microsoft Entra ID as SSO integrations) and one OnLogic ISO 9001:2015 claim.
The experimental statement paths failed to recover those records. They remain in the
original direct results; the experimental output deliberately does not silently fall
back to baseline records, because that would obscure the comparison.

Other objectives continue to use the direct crawler:

| Site | Company facts | Contacts | Locations | Products/services | Company relationships |
| --- | ---: | ---: | ---: | ---: | ---: |
| DMC | 2 | 0 | 0 | 0 | 0 |
| Plausible | 3 | 4 | 0 | 1 | 3 |
| thoughtbot | 1 | 0 | 0 | 0 | 0 |
| OnLogic | 10 | 6 | 1 | 15 | 5 |
| IC Resources | 3 | 0 | 0 | 1 | 0 |

These are automatic acceptance counts, not an exhaustive manual verification of every
record. No people, jobs or document links passed the existing gates in this run. No
job detail pages were fetched, so the intended recruiter-versus-client **job technology
attribution test remains untested**. Ownership and financial-document coverage are
also insufficient for a quality conclusion. The controller still uses direct findings
to choose pages; this benchmark does not test statement-driven navigation feedback.

## What worked

- Multiple relationships survive independently. The isolated MCAP regression retained
  both role usage and preferred experience. Fresh thoughtbot statements also retained
  combinations of usage, expertise and historical use with their separate sources.
- Thoughtbot's React, Elixir and React Native usage for application development survived
  the statement path, while its direct baseline accepted no technologies. Its 12
  observations remaining after manual holds cover eight source-named technologies;
  repeated observations and different relationships are not additional technologies.
- IC Resources' semiconductor, photonics, AI and other recruitment sectors were kept
  out of accepted technology observations. Recruitment remains a described service.
- OnLogic's direct extraction retained useful company facts, the named AX300 product
  family, headquarters, published email and described engineering services. The native
  HTML was sufficient for these facts when requests completed and evidence passed.
- Saved HTML, response logs, linked corrections and per-stage results made it possible
  to identify where information was lost. All 11 page hashes and the frozen source
  implementation are verifiable. No database submission or deployment occurred.

## What failed

**Structured company attribution was not reliably reviewed.** DMC statements often
put a tool/vendor name in `subject_name`, even when the prose says DMC provides the
service. Description review accepted some of that prose without rejecting the wrong
structured subject. Normalization copied the wrong subject, and later source review
and catalog checks accepted 14 observations under companies such as SharePoint or
Arduino Cloud. All 14 are held in the audited output. Do not silently rename their
companies: correction needs explicit source support and another review.

**Offering a service was confused with offering the technology itself.** Five thoughtbot
observations classified Rails/Hotwire development or Android app development as
`offers` of Rails, Hotwire or Android. Under the current definition, `offers` means
providing the named technology itself. These service claims belong in services and/or
technology expertise/use. The five `offers` records are held; separately supported
observations remain. ChatGPT was classified as advertised expertise, preserving the
consulting context rather than claiming thoughtbot internally deploys it.

**False negative-use claims appeared during normalization.** DMC service listings were
sometimes normalized to `explicitly_not_used` without a source denial. Later checks
rejected or corrected those versions; none appears in DMC's final automatically
accepted set. They remain important regression cases for the normalization prompt.

**Correct values frequently failed evidence binding.** Plausible's founded year, team
size and co-founders were extracted but held because their quotations did not include
the company anchor. Some DMC quotations also changed apostrophe characters. The source
contains useful facts, but incomplete or altered quotation fragments prevent acceptance.
This is distinct from a factual error or an absent fact.

**Requests were not reliable enough to cover the websites.** There were 51 request
deadlines and two HTTP 429 responses. The global repeated-error stop prevented further
page exploration. The current test cannot separate model capability from request-size,
response-size and provider-performance effects. DMC's native homepage HTML was about
128 KB despite only about 9.8k characters of readable text; its header alone occupied
about 62.6k characters. Navigation and testimonial-heavy pages consumed much of the run.

**Empty pending-ID lists concealed failed page extraction.** When no statements are
created, there are no statement IDs to mark pending. The frozen benchmark harness also
counted attempted page files as examined pages. The offline auditor corrects coverage
in a separate `audited-company.json`, marking these objectives `processing_incomplete`
and exposing incomplete page IDs. It preserves the original `accepted-company.json`
and all frozen model outputs.

## Manual audit and cost

All 31 automatically accepted technology observations were inspected against their
saved sources. Nineteen explicit holds are saved in per-site `manual-holds.json`; the
derived outputs remove those records and recompute technology summaries. The remaining
records are not promoted to independently verified company facts.

[manual-controls.json](data/fresh-company-statements-v1/manual-controls.json) records
26 source-derived checks across the five sites, with IDs, quotations, outcomes and
failure stages. These were never supplied to the LLM. They cover a sample of available
facts, not exhaustive website recall. [manual-findings.json](data/fresh-company-statements-v1/manual-findings.json)
documents the attribution and relationship failures. The [automatic audit](data/fresh-company-statements-v1/automatic-audit.json)
checks provenance, acceptance consistency and processing coverage; its zero integrity
errors must not be read as zero semantic errors.

| Stage | Calls | Reported cost | Unknown-cost calls |
| --- | ---: | ---: | ---: |
| Direct crawl and extraction | 64 | $0.09522325 | 29 |
| Fresh statements, review and normalization | 98 | $0.11497670 | 24 |
| Five-site total | 162 | **$0.21019995** | **53** |

Unknown costs cover 51 deadlines and two rate-limit responses. Reported costs are
incomplete; do not interpret them as the final charge. The separate two-call MCAP
regression cost $0.00176125. The benchmark runs both extraction paths and therefore
does not measure the cost of one optimized production workflow.

## Recommended next work

1. Bind the company/holder as an explicit verified actor and check that field separately
   from descriptive prose. Preserve a distinct client/employer when the source establishes
   it; unknown identity must stay unknown. Use the DMC failures as frozen regressions.
2. Tighten the distinction between offering technology, providing services using it,
   advertised expertise, usage and explicit non-use. Keep the multi-relationship support,
   but require evidence for every relationship. Use thoughtbot and MCAP together.
3. Reduce request complexity and improve evidence anchors. Test generic Crawl4AI content
   exclusions or its structured Markdown, with navigation links preserved for discovery;
   use smaller source windows with reliable company/section context. Do not impose an
   arbitrary small output-token cap or silently weaken evidence checks.
4. Expose failed/unfinished pages in the package result and isolate long-page failures
   so that one page does not prevent reaching important job/contact/quality pages.
5. Recheck those changes on the saved failure cases, then run a focused fresh job-detail
   and ownership/document-discovery test. Do not repeat the entire five-site crawl yet.
