# Company research

See the [research checkpoint](../RESEARCH_CHECKPOINT.md) for saved findings,
known NOVELIC extraction gaps and next work. PDF/OCR experimentation is paused.

Version 0.17.1 handles Crawl4AI's bare result on robots denial as well as its normal
result container. The [Informer/B92/Google test](SITE_GATE_REQUESTED_RESULTS.md)
skipped both news portals after one model call each; Google's robots-check rejection
now returns its actual reason and `needs_review`, without a model call.

Version 0.17.0 adds a mandatory [first-page eligibility gate](SITE_PROFILE_FLOW.md)
to `research_company` and the `company-research` URL command (result schema 1.11).
Only an identified company/brand's own business site proceeds to sitemap discovery,
link ranking and extraction. News/content sites, forums, search engines, directories,
multi-seller marketplaces and advertising portals return `status: "skip_crawling"`
with a factual `site_description` of at most 200 words. A named corporate owner does
not override the site's primary purpose. Blocked, unavailable or unclear pages stop
with `needs_review`. Both outcomes leave detailed objectives `not_assessed`.

The gate uses only the first fetched page, including redirects, with source-matched
quotations and bounded correction attempts. No sitemap or other page is fetched to
resolve uncertainty. Normal browser assets and robots checks still occur. Skipping
is a successful CLI outcome; `needs_review` exits with code 2 after emitting JSON.
See [the validation report](SITE_GATE_RESULTS.md) for measured behavior and limits.

Version 0.16.0 implements [saved-page research and S3 submission](PAGE_RESEARCH.md):
one page request collects all objectives, raw technology mentions and scored links;
a bounded final pass classifies the mentions, followed by read-only catalog lookup.
Complete JSON, original source text, exclusions and review items are stored in RustFS.
The new package command is `company-research-pages`. The existing URL crawler still
uses its earlier controller; integrating this page unit into that queue is next.

The standalone [page-agent lab](../page_agent_lab/README.md) now implements both
one-pass and routed-specialist variants. Its [seven-page results](../page_agent_lab/RESULTS.md)
retained the selected facts in both versions; routing increased cost/time and added
errors. One-pass is the user-approved integration baseline, with targeted specialists
still to be tested. Its common code now lives in the package; compatibility imports
keep the original benchmark behavior unchanged.

The [DSPy RLM experiment plan](DSPY_RLM_PLAN.md) is **postponed** and saved for
future reference. The RLM implementation and runs have not started. Current work
continues with the existing crawler and its discovery/validation gaps.

The [Handelsbanken company analysis](HANDELSBANKEN_ANALYSIS.md) combines the documented
autonomous diagnostic with source-guided follow-ups. Its [low/high reasoning comparison](HANDELSBANKEN_REASONING_COMPARISON.md)
retained 31/38 technology controls at low and 36/38 at high, with higher cost and
runtime and remaining taxonomy/scope failures. Link assessment did not show an overall
improvement. The experiment uses explicit reasoning settings; defaults are unchanged.

Version 0.15.3 fixes phone evidence checking: only the phone value uses numeric
matching, while its owner still requires quoted textual evidence. A saved-output
replay removed erroneous holds on 19 phone records without changing any facts,
quotations or IDs. See the [page-agent follow-up](../page_agent_lab/FOLLOW_UP.md).
The lab prompt/controls were also tightened; their model performance is untested.

Version 0.15.2 preserves technology observations when restricting job evidence to
role scope makes originally distinct company/team/role observations identical.
Only duplicates created by that conversion are coalesced; genuine duplicate model
decisions still need review. Usage and experience remain separate observations.
The Handelsbanken saved-decision replay recovered Microsoft 365 and Exchange
without new extraction or normalization requests.

Version 0.15.1 exposes direct DeepSeek through the public Python API with
`research_company(..., api="deepseek")`. With no explicit configuration it selects
`deepseek-flash`; the key defaults to `DEEPSEEK`. Explicit configurations must set
the desired direct model. The CLI retains its existing OpenRouter route.
This version also fixes numeric `tel:` links being mistaken for HTTP URLs during
external-link capture, a failure discovered on Handelsbanken.

Version 0.15.0 adds [external-link observations](EXTERNAL_LINKS.md): complete destination
URLs, source pages, rendered HTML provenance, header/footer/section context and separate
relationship assessments. Every observed external link survives crawl exclusions and
model failures. Research output uses schema 1.10; company extraction HTML is unchanged.

Version 0.14.4 adds an explicit direct DeepSeek API dialect to the shared model client
and paired benchmarks using `DEEPSEEK` from `jobs_extraction_lab/.env` with
`deepseek-flash`. See [direct API results](DEEPSEEK_DIRECT_RESULTS.md): greater recovery
in this bounded run, higher cost, remaining completeness gaps. The crawler CLI still
uses its existing OpenRouter configuration; direct selection is explicit in the tests.

The [three additional website tests](DEEPSEEK_MORE_WEBSITES.md) compare fresh native
Crawl4AI HTML from Memgraph, Oxide and RT-RK. Direct retained more observations without
API timeouts, but both routes expose relationship omissions, catalog/review failures
and overly broad technology identities. All six paired runs and manual review are saved.

Version 0.14.3 introduced separate checks for structured actors and descriptions, repairs to source
quotations before rechecking actor corrections, and a distinction between development services
and selling the technology. See the [saved-failure replay](ATTRIBUTION_REPLAY_RESULTS.md)
for measured improvements, manual holds and remaining omissions. Output uses
`page-statements/1.3`; regular research results retain schema 1.9. Failed page extraction
is reported separately from pending statement IDs. The workflow is still optional.
Multiple supported relationships from one description remain separate observations;
each retains its own signal, source review, company/job and page context. The
[fresh five-company benchmark](FRESH_COMPANY_BENCHMARK.md) compares
it with direct extraction on newly fetched pages, preserving all intermediate results.

The preceding [page statement review workflow](PAGE_STATEMENTS_REVIEW_RESULTS.md) in
0.12.3 checks descriptions against saved HTML, rechecks corrections and normalizes
statements with required decisions per ID. It keeps valid records when another fails,
preserves job-level scope and reuses saved reviews/results. That saved NOVELIC test
recovered 31/32 technology controls and 3/3 company credential controls. Its historical
output uses `page-statements/1.1`; shared catalog resolution also requires individual
decisions and retries incomplete items.

The [first page statement experiment](PAGE_STATEMENTS_EXPERIMENT.md) preserves the 0.11.1 results and the failures that motivated these changes.

The [technology metadata replay](NOVELIC_METADATA_REPLAY.md) documents 0.10.2 / schema 1.8, separate validation stages, advertised expertise, the saved-data results and remaining manual quality holds.

The [controller recheck](NOVELIC_CONTROLLER_RECHECK.md) documents the 0.9.2 URL binding, engineering coverage and saved-extraction retry changes, including which versions ran in each benchmark. The [scope and validation recheck](NOVELIC_SCOPE_RECHECK.md) preserves the preceding 0.8.1 benchmark.
The [earlier autonomous NOVELIC report](NOVELIC_AUTONOMOUS_RESULTS.md) preserves the
0.7.0 homepage-only run, measured costs and the errors that motivated these changes. The earlier
[recheck report](NOVELIC_RECHECK_RESULTS.md) preserves the guided and frozen-source comparisons.

One website URL in; JSON findings for company profile, contacts, locations,
products/services, people, company relationships, jobs, technology signals and
certifications/compliance and company-document links out.

The package uses Crawl4AI with the tested CloakBrowser rendering setup and native
cleaned HTML. DeepSeek assesses candidates and extracts all objectives on every
fetched page. A small Python queue balances objectives and enforces limits.
It has no dependency on the example applications, benchmark folders or Codex SDK.

The optional [technology catalog MCP server](TECHNOLOGY_MCP.md) lets agents search
the published database catalog, inspect technology/category metadata and prepare
new proposals with a required category and an LLM-written description.

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
and technology claims, company facts, credential holders and document types receive
structured interpretation review before catalog resolution. Technology proposals then
receive a separate identity/category audit. Review states distinguish accepted, rejected
and processing_failed; temporary failures receive bounded retries. A reversed
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

Technology extraction is part of the same LLM request on every page. The controller promotes source-matched target-employer job URLs into detail-page
follow-ups. `job_detail_reserve` prioritizes up to five descriptions by default, within
the overall and external-page budgets. `discovery.job_coverage` records discovered
postings, detail attempts, descriptions with target jobs and unvisited job URLs. The `technology_signals` objective has its own coverage
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

Version 0.7.0 emits schema `1.5` (the reader also accepts `1.4`), retaining a run ID, pinned catalog metadata and
`catalog_match` with either `matched` or `proposed` on technology findings. A local
`search_technologies` tool supplies canonical candidates in a separate identity-resolution
step after HTML extraction. Exact names and aliases resolve locally; the smaller model
request receives observed names, their contexts and initial local search results, and
can search alternative names with the tool. The HTML extraction request has no catalog
tools attached. Missing or
invalid model resolutions are recorded as processing errors, with the source finding
retained and excluded from the technology summary. Historical schema `1.0`–`1.3` artifacts are preserved and are not silently
upgraded or submitted as catalog-checked findings.

`result.entities.people`, `.jobs` and `.technologies` consolidate accepted findings
without deleting raw observations. Each entity carries its identity, all observed
field values, original record IDs and source URLs. People group by case-insensitive
name and company; conflicting profile URLs stay separate. Jobs group by published
job URL, actual redirects and single-opening detail-page links. Missing job URLs do
not trigger fuzzy title matching. Technology entities group by company and canonical
or proposed identity, preserving different dates, scopes and signals in the referenced
observations and `technology_summary`. These are conservative entity counts, not a
guarantee that every duplicate has been resolved.

Technology submissions require an accepted claim, company attribution, a completed
catalog match/proposal, and source-matched evidence. Rejected interpretation reviews
and catalog errors exclude a record even if a catalog match exists. The backoffice
also rejects unaccepted claims and sources before proposal insertion.

If the browser context closes, the run restarts it and retries the same page with
cumulative attempt numbers. `max_browser_restarts` bounds restarts across the entire
run (default two); exhaustion stops with `browser_unavailable`, preserving collected
findings. `discovery.browser_recoveries` records recovery events. A browser outage is
not evidence that a URL or company fact is absent.

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


### Target-company scope and overview validation (0.8.1)

Link assessments include `target_relevance` and `follow_scope`. External partner
profiles default to a single page. Further external navigation requires an explicitly
target-scoped navigation assessment and target-company facts observed after fetching.
Every fetched page is still examined for all objectives. Related companies remain
separate; only source-reviewed legal/trading-name mappings extend the target names. Different legal
names require an explicit same-entity basis; regional market-entry wording is insufficient.

The overview input contains only accepted facts attributed to the target (or explicit
relationships involving it). Relationship and credential statements must cite the
corresponding fact types. A final meaning check excludes unsupported statements and
saves them under `summaries/meaning-review.json`; no new ownership fact is inferred
from a name or location. `product_documentation` is separate from financial reports.
Document discovery does not download or interpret PDFs.

Summary requests use short internal citation IDs, expanded to canonical IDs only after
validation. Certification names must match their cited structured facts exactly.
Source/proposal reviews are propagated to duplicate records. `required_reviews` metadata
prevents missing reviews from entering accepted exports; rejected copies cannot be
restored by merging an unreviewed duplicate. Generic CI/CD is a method, not a technology
identity; named tools such as GitHub Actions remain valid candidates.

## Technology approval stages (0.10.2, schema 1.8)

A source-supported observation can have rejected catalog metadata. Its
`evidence_status` records the quotation/meaning outcome; `catalog_error` and
`proposal_review` record separate catalog outcomes. Use `accepted_finding`, not
`evidence_status` alone, when producing accepted summaries or submissions.
`discovery.pending_technology_metadata` lists source-supported records still waiting
on catalog stages, separately from unfinished HTML extraction chunks.

`advertised_expertise` represents company skills tables, tool experience and advertised
competence. It does not establish an installed technology stack. Explicit usage stays
`stated_use`; applicant requirements stay `required_experience` or
`preferred_experience`, with their original company/team/role scope.

Proposal review checks identity, category and description independently. A rejected
category or description can receive one bounded metadata correction
(`ResearchConfig.max_proposal_corrections`, default 1). This correction cannot change
the observed technology, company, signal, quotations, proposal name, website or
licensing fields. New descriptive category suggestions are allowed when no published
category fits. Unknown numeric category IDs are rejected.

Changed metadata is reviewed again. `proposal_review.metadata_sha256` binds approval
to the exact draft; before/after values and rejection reasons remain in
`proposal_metadata_repairs` and the saved call artifacts. Identity rejections remain
blocked. An LLM approval is still a draft-quality check, not independent verification
or administrator approval.

`process_technology_metadata(records, catalog, llm, root, task)` resumes only missing
catalog resolution and unreviewed/failed proposal metadata. It skips unchanged accepted
drafts, retries rejected/failed metadata review when explicitly resumed, clears stale
resolution errors after success, and does not extract HTML again. An unchanged repair
can be re-reviewed within the same correction budget; a mistaken rejection does not
require inventing a metadata edit.
Successful source extraction is not retried because of a catalog HTTP failure. Existing
source interpretation corrections retain the rejected original and link a newly
reviewed record through `correction_of`.

### Posting URLs and saved extraction attempts (0.9.2, schema 1.7)

Accepted listing links plus an unchanged fetched URL and unambiguous primary heading
establish `page.job_detail`. Matching job/technology titles receive that exact URL in
code, with provenance under `data.job_url_binding`. Listings, unrelated job titles and
redirects to a different page do not receive this binding. HTML evidence checks remain.

`engineering_page_reserve` defaults to three pages inside `max_pages`; it prioritizes
direct service/engineering sources with plausible technology evidence. `page_kind`
and observed new-record counts reduce repetitive news/navigation priority. A zero-yield
penalty requires completed extraction of that objective; failed or partially processed
pages do not count as evidence of low usefulness. These are discovery heuristics;
every fetched page still receives all extraction objectives.

`extract_saved_page` verifies stored native HTML and processes pending chunks without
fetching again. `max_extraction_attempts=2` bounds each chunk and
`max_saved_extraction_retries=5` bounds additional attempts across the run. Existing
HTTP and total model-call limits still apply. `page.extraction_attempts`,
`extraction-attempts/`, `discovery.saved_extraction_retries` and
`discovery.pending_extractions` retain progress and failures, including planned chunks
that have not started. Completed chunks and semantic rejections are not retried. This
recovery function does not refresh sources.

Source and proposal reviews use short request IDs, mapped exactly back to canonical
record IDs. Unknown or truncated IDs are rejected rather than guessed.

Native HTML, source neighborhoods, original claims, corrections, source reviews,
proposal reviews and overview exclusions remain inspectable. Proposal metadata is an
LLM-reviewed draft for administrator approval, not independently verified catalog data.
