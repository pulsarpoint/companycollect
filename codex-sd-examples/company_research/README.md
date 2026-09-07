# Company research

See the [research checkpoint](../RESEARCH_CHECKPOINT.md) for saved findings,
known NOVELIC extraction gaps and next work. PDF/OCR experimentation is paused.
The [NOVELIC recheck report](NOVELIC_RECHECK_RESULTS.md) contains the latest company,
Careers and engineering-page JSON, measured costs, fixes and remaining limitations.

One website URL in; JSON findings for company profile, contacts, locations,
products/services, people, company relationships, jobs, technology signals and
certifications/compliance and company-document links out.

The package uses Crawl4AI with the tested CloakBrowser rendering setup and native
cleaned HTML. DeepSeek assesses candidates and extracts all objectives on every
fetched page. A small Python queue balances objectives and enforces limits.
It has no dependency on the example applications, benchmark folders or Codex SDK.

The [live verification report](SMOKE_RESULTS.md) includes a five-page run with
102 records and the observed provider timeout/coverage limitations.

The [technology database implementation plan](TECHNOLOGY_DB_IMPLEMENTATION_PLAN.md)
describes company/domain attribution, catalog aliases, observation history, unified
summaries, and a staged ClickHouse rollout. Catalog lookup and administrator-reviewed
technology proposals are implemented; the broader company observation tables remain
planned. See [technology proposal setup](TECHNOLOGY_PROPOSALS.md).

The [site profile flow](SITE_PROFILE_FLOW.md) describes initial site classification,
profile-based objective priorities, company summaries, and certifications/compliance
extraction. It includes the request graph and inputs/outputs for each LLM stage.

The [first technology mapping audit](technology_mapping/README.md) checks saved real-job
findings against the current catalog using case-insensitive matching and provides
source-backed mapping decisions, an alias output file, and a replay script.

```sh
python -m pip install -e ./company_research
company-research https://www.example.com --env-file /path/to/crawler.env \
  --technology-catalog data/technology-catalog.json \
  --output-dir data/example > example.json
```

Requires Python 3.12 or newer. Configure `OPENROUTER_API_KEY` and ClickHouse connection
variables in the environment or the explicitly supplied env file. Each crawl refreshes
the catalog before making model calls; use `--offline-catalog` with an explicit snapshot
path for a reproducible offline lookup. The package never automatically
searches other projects for credentials. Browser binaries are downloaded by
CloakBrowser on first use; this needs internet access and a supported browser host.

Python API:

```python
import asyncio
from pathlib import Path
from company_research import ResearchConfig, research_company
from company_research.technology_catalog import TechnologyCatalog

result = asyncio.run(
    research_company(
        "https://www.example.com",
        output_dir=Path("data/example"),
        config=ResearchConfig(max_pages=20),
        technology_catalog=TechnologyCatalog.read(Path("data/technology-catalog.json")),
    )
)
print(result.model_dump_json(indent=2))
```

`result.records` contains the ten objective arrays plus scoped explicit negative
statements. Each finding includes its data, stable record ID, source URL(s), snapshot
hash, fetch time, source-window offsets, exact evidence fragments and evidence status.
The same data observed on several pages is merged while preserving its sources.
When only quotations fail, a focused repair request returns separate exact source
fragments for fixed record values. It cannot invent or rewrite the facts. Relationship
and technology claims also receive structured interpretation review: parties and
direction, technology specificity, and the source-supported usage signal. A reversed
pair or wrong signal can receive one narrowly scoped correction and another review;
the original record remains reviewable with a link to its correction. Review failures
and unavailable reviews stay `needs_review`. This is not independent verification.
Conflicting or differently enriched records remain separate; this is not global
entity resolution.

Each objective has one of these statuses:

- `found`: at least one record passed the source-presence checks.
- `needs_review`: records exist but their evidence or URLs need review.
- `explicit_negative_found`: the source makes a negative statement; its scope is preserved.
- `not_found`: no record found in the examined content, not proof of absence.
- `not_assessed`: no assessable content was returned for that objective.

`source_matched` verifies source presence, required identity anchors and URLs. It
does not certify the model's interpretation, ownership attribution or temporal
correctness. Failed source checks retain the record as `needs_review`.

Technology extraction is part of the same LLM request on every page. The selection
prompt also prioritizes full job descriptions for technology research after job
titles have been found. The `technology_signals` objective has its own coverage
status, so finding a list of openings does not complete this objective.

Technology signals are limited to specific named applications, tools, platforms,
libraries, languages and hardware products. Ansys HFSS, CST Studio Suite and AURIX
can qualify; XML/JSON formats, generic AI/radar/FPGA capabilities and architectures
such as RISC-V do not. Catalog presence does not override this scope. Prompts enforce
the semantic distinction, with validation rejecting known generic names in observations,
proposals and claimed canonical matches; that guard is not an exhaustive classifier.

`records.products_services` with `kind: service` captures what the company offers:
radar development, outsourced hardware design, custom embedded-device development,
or specialization in a named application. Each description should explain supported
work, deliverables and specialization. Formats and methods may remain description
details. A job requirement alone does not establish a commercial service, and offering
implementation of an application does not establish internal use of that application.

Each technology signal preserves the technology's source spelling, category, company,
job employer/title/URL, scope, statement type, alternatives, dates, context and evidence.
Statement types distinguish `stated_use`, `required_experience`, `preferred_experience`,
`planned_adoption`, `past_use`, `being_replaced`, `explicitly_not_used` and `mentioned`.
For example, a requirement for AWS or Azure does not establish that both are deployed;
a client's SAP installation is attributed to the client, not its staffing agency.

`result.technology_summary` groups attributed, source-matched observations by company,
technology, category, statement type, scope, alternative group and stated date. It
includes supporting record IDs, source URLs and distinct job URLs. Repeated windows
and tracking-URL variants do not inflate job URL counts. These are counts of observed
URLs, not independent vacancies, deployments or company-wide adoption. Requirements,
optional skills, historical claims and explicit non-use remain separate rows.
Unknown-company and reviewable observations remain in `records.technology_signals`
but do not enter this company summary. Accepted aliases share their canonical technology
identity. Proposed identities remain separate until review; product families, company
names and mirrored job ads are not resolved globally.
See [technology verification](TECHNOLOGY_RESULTS.md) for real-ad and controlled-prompt results.

Version 0.5.0 emits schema `1.4`, retaining a run ID, pinned catalog metadata and
`catalog_match` with either `matched` or `proposed` on technology findings. A local
`search_technologies` tool supplies canonical candidates in a separate identity-resolution
step after HTML extraction. Exact names and aliases resolve locally; the smaller model
request receives observed names, their contexts and initial local search results, and
can search alternative names with the tool. The HTML extraction request has no catalog
tools attached. Missing or
invalid model resolutions are recorded as processing errors, with the source finding
retained and excluded from the technology summary. Historical schema `1.0`–`1.3` artifacts are preserved and are not silently
upgraded or submitted as catalog-checked findings.

Before selecting further pages, the crawler classifies the first usable page into site
types and research profiles. A failed or unsupported classification can be retried on
the next fetched page. Service, product and manufacturing profiles prioritize credentials
and business relationships; all ten objectives remain available on every page to
capture secondary facts. Classification is provisional and appears as `site_profile`
with its evidence; automatic later reclassification is not yet implemented.

`records.certifications_compliance` preserves the holder, standard, scope, issuer,
dates and supporting document URLs. `verification_level=website_claim` and
`document_examined=false` make clear that the HTML crawler has not independently
verified credentials or opened linked PDFs. Source validation does not prove a claim.

`records.company_relationships` now distinguishes person/company/brand parties and
preserves stated ownership percentages, direct/indirect/unspecified scope and dates.
Founder or CEO roles do not establish ownership; commercial relationships cannot carry
ownership fields. Percentage and identity evidence must be present in quoted fragments.

`records.document_links` contains financial statements, annual reports, ownership
disclosures and certificates identified from links and surrounding HTML. Entity and
reporting period require explicit source evidence; a filename year is insufficient.
`content_examined=false` is set by code. `discovery.document_candidates` also retains
unclassified PDF/office-document URLs and their discovery sources, including sitemaps.
Document contents are never fetched or parsed by this package.

The final `company_overview` consolidates supported site/business facts and offerings.
Each summary statement cites input record IDs; source URLs are derived from those
records. Large inputs are summarized in bounded batches. All atomic findings remain
available if a summary fails, with the failure recorded in `errors`. Selection reserves
three model calls where possible for final consolidation. Classification and summary
calls count against the same model budget and have their own saved artifacts.

Defaults are defined once in `ResearchConfig`: 20 page targets, up to two fetch
attempts per target, three external pages, 100 model HTTP attempts, 500 sitemap URLs,
1,000 total candidates, 60,000-character HTML windows with 4,000-character overlap,
and up to one JSON/schema/evidence correction. Invalid findings remain reviewable;
corrected identical records merge with their original evidence attempts. Completely
empty output despite an explicit email link is flagged for correction, not marked as
successful absence. This guard is not a general recall check.
Output tokens are budgeted at 65,536, not 8K.
Link assessment uses batches of 20 candidates, normally one batch between page fetches;
additional batches are assessed if the queue has no useful or explorable next page.
All model requests including retries consume the request budget. API costs exclude
calls for which the provider did not return usage.

The default model/provider are the tested `deepseek/deepseek-v4-flash-0731` and
`baidu/fp8`, with low reasoning, strict JSON-schema requests and no provider fallback.
Other models/providers can be selected through `ResearchConfig`; compatibility is
not assumed and failures are reported.
Set `ResearchConfig(provider=None)` to allow OpenRouter to select available providers
for the same model, sorted by latency. This preserves parameter requirements and
records the actual provider on each response. Named providers remain pinned for
reproducible comparisons. HTTP 429 retries honor numeric Retry-After headers up to
60 seconds, otherwise waiting 30 seconds within the configured request deadline.
`reasoning_effort` is configurable; `none` disables reasoning for a controlled test.
Identical repeated JSON documents can be unwrapped; conflicting documents are rejected.

Discovery combines bounded sitemap/sitemap-index traversal and links from fetched
pages. Missing sitemap metadata stays uncertain. Each objective can nominate a
metadata-relevant candidate into an assessment batch before remaining slots are filled
by labeled/shallow URLs. These navigation hints affect ordering, not claims about
page contents. Failed assessments can be retried up to `max_assessment_attempts` (two
by default), and relevant unassessed internal links remain eligible for bounded
exploration. A finite budget cannot assess the whole web.
Crawl attempts are balanced across objectives before giving another turn to a still
empty objective. An objective's first suitable navigation page can precede more direct
pages, so one discovered job does not prevent following a Careers collection.
Directly linked external company/recruitment pages can be followed within the external
budget. Login/account/action and non-HTML document URLs are excluded from crawling;
their source links can still appear in extracted records. Social profiles and feeds
are retained as contacts but excluded from external page crawling; individual LinkedIn
job ads remain eligible. LinkedIn tracking and regional URL variants normalize without
collapsing distinct job IDs. Same-site subdomains are
recognized using the bundled public suffix list. Sitemap page entries are read directly;
nested image locations do not consume the page inventory. `robots.txt` is respected
by default. Sitemap failure does not prevent discovery from page links.

Finding one item does not complete a collection. The queue continues until its
budget or available useful/uncertain candidates are exhausted. Every fetched page
is examined for all objectives, regardless of why it was selected. Redirects to an
already examined final URL are recorded as duplicates. Related websites
and paginated lists can still be incomplete when the budget is reached.

The output directory must be empty/new. `result.json` is checkpointed after each
page and finalized even on recoverable failures. `html/`, `calls/`, `fetches/`,
`extractions/`, `evidence-repairs/`, `resolutions/`, `reviews/`,
`interpretation-corrections/`, `assessments/`, `queue.json` and `sitemaps.json` preserve provenance
and failure details. Resuming an interrupted directory is not implemented; use a
new directory for a new run. Credentials are omitted from saved requests/settings.

CLI progress goes to stderr; stdout contains JSON. Exit status is 0 for finished
or partial runs and 2 if no page could be assessed. Inspect the JSON's status,
stop reason, objective coverage and per-page errors before treating it as complete.
Failed link assessments appear in `errors` and `discovery.assessment_failed_count`,
and make the run `partial`. Page/model-budget stops are also `partial`. `finished`
means the configured run ended cleanly; it never establishes exhaustive website coverage.

The package does not click load-more controls, log in, or parse PDF documents.
Followable pagination links can enter the queue. Sites requiring interaction beyond
normal browser rendering may expose only a subset of their content.

Development checks, from the examples directory after installing this package:

```sh
python -m unittest discover -s company_research/tests -v
COMPANY_RESEARCH_BROWSER_TEST=1 python -m unittest discover -s company_research/tests -v
uvx ruff check company_research
uvx ty check company_research
uv build --wheel company_research
```

The opt-in integration test uses a real browser and local website, replacing only
OpenRouter's HTTP response. It checks sitemap discovery, JavaScript rendering,
selection, a failed page, continued discovery from a jobs list to a job description,
technology aggregation, extraction across objectives, scoped negative claims,
source hashes and the final JSON. It makes no paid model requests.
