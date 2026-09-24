# Crawler service

Website crawling and source-page collection, using the independent browser service.

Version 0.42.0 renames `company_research` to `crawler_service`. Run the REST worker
with `crawler-service`; run a one-off crawl with `crawler-service-crawl` or
`python -m crawler_service`. The API remains on port 8080. Existing saved-result
schemas, S3 prefixes, and deployment data directories are preserved. See the
[deployment rename notes](ansible/README.md#service-rename).

Version 0.37.0 moves all browser ownership into the independent
[browser service](../browser_service/README.md). Configure `BROWSER_API_URL` and
`BROWSER_API_TOKEN` for both the crawler and collection CLI. The crawler sends a
persisted UUID for each attempt and uses HTTP for all navigation, captures, heartbeats
and release. Backoffice management is now under **Browsers**.

Version 0.36.0 adds the [sticky browser API](BROWSER_API.md). Each domain reserves
one existing profile and receives a unique session ID for all site/search requests.
Busy browsers queue reservations; expired IDs fail instead of launching a browser.
The crawler now uses the same HTTP contract, with heartbeat and cancellation cleanup.

Version 0.35.0 assigns scans to the saved browser profiles. Each domain owns one
profile for site pages and Brave search, including human verification pauses.
On completion, failure or cancellation, cookies and profile storage are saved and
the browser restarts with a blank tab before another domain can use it. The admin
view shows the assigned domain, and Brave retains its service-wide verification block.


Version 0.34.2 adds a per-profile **Auto-restart** switch, enabled by default.
Closing the last tab or browser window opens a blank tab with the same profile;
the Backoffice viewer reconnects. **Stop and save** cancels pending recovery.

Version 0.34.1 adds **Crawler → Servers & sessions** in Backoffice. The connected
server reports every open Xvfb desktop it manages, including saved profiles,
interactive crawl attempts and the shared headed Brave browser. Operators can
connect to any listed desktop with a short-lived, single-use ticket.

The service lives at `companycollect/corpscout/services/crawler_service`.
Run setup and commands from this directory; see [the service guide](SERVICE.md).
The Python package name and command names are `crawler_service` and
`crawler-service-*`. Saved captures and benchmark data moved with the package.

Version 0.34.0 adds a configurable pool of [persistent browser sessions](HUMAN_ASSISTANCE.md#persistent-browser-sessions).
The Linux deployment starts two headed browsers under Xvfb, available in Backoffice
at `/admin/browser-sessions` for manual login and multiple tabs. Private profiles,
session cookies and tabs survive browser restarts. Since 0.35.0, crawl jobs lease these profiles for both site and search tabs.

Version 0.33.0 pauses all Brave searches in the service on a detected bot challenge.
Backoffice **Start verification** opens the exact pending query under Xvfb with the
same profile and cookies; **Resume crawl** validates the result and continues the
same attempt. The search block survives cancellation and service restart. See
[Brave verification](HUMAN_ASSISTANCE.md#brave-search-verification).

Version 0.32.0 removes Crawl4AI and uses CloakBrowser through Playwright directly.
The service retains native browser headers, checks robots.txt, captures rendered
HTML and produces deterministic simplified HTML using BeautifulSoup. Discovery,
static extraction, local/S3 results and interactive retries retain their existing flow.
Trafilatura continues extracting readable main text. Historical capture files remain
readable; newly collected simplified HTML can differ from old Crawl4AI captures.

Version 0.31.1 keeps automatic scans headless. Only a manual interactive retry of a
failed attempt starts Xvfb, a headed browser and noVNC access.

Version 0.31.0 adds [failed-attempt history and human assistance](HUMAN_ASSISTANCE.md).
Automatic blocks notify operators and fail after 10 seconds. SQLite and S3 preserve
each attempt; Backoffice provides live filters and independent interactive retries
with an embedded noVNC browser. S3 delivery now also covers REST and manual requests.

Version 0.30.0 expands source discovery: bounded navigation through evidenced
parent-company and filing sources, objective-driven Brave web search, and document
links with surrounding labels/headings. Full mode enables web search by default;
targeted requests can use `--web-search`. Search uses the existing browser without
an extra API key and consumes at most three queries by default. Queries, ordinary
results, source decisions and document references remain in `result.json`, including
when diagnostic files are disabled. PDF download/OCR and financial interpretation
remain separate from crawling. See [source discovery](CRAWL_AND_ANALYZE.md#source-discovery)
and the [NOVELIC gap audit](NOVELIC_FINANCIAL_DISCOVERY_AUDIT_20260918.md).
The [live discovery and focused verification](NOVELIC_SOURCE_DISCOVERY_20260918.md)
document the parent-company route, four labelled NOVELIC PDF references, observed
search rate limiting, and the final source-priority correction.

Version 0.29.0 adds `--crawl full` (REST/JetStream: `"crawl": "full"`). It uses LLM
navigation across contacts, jobs, company information and financial/report pages,
with deterministic extraction and bundled links/cleaned HTML. Full mode defaults
to 100 pages and 30 external pages, with explicit overrides; it reports elapsed
time and model usage. See [full-crawl usage](CRAWL_AND_ANALYZE.md#full-crawl).
The [live NOVELIC benchmark](NOVELIC_FULL_CRAWL_20260918.md) collected 68 unique
pages and all 16 discovered jobs in 8m30s, using 198,482 tokens across 40 DeepSeek
Flash/high calls. It ended `partial`: two URLs returned 404 and one returned 403.
Five redirects were deduplicated. The report separates model, fetching and static
processing time and records raw-data/financial-link attribution limitations.

Version 0.28.0 makes `crawler-service-crawl`,
`python -m crawler_service`, REST and JetStream use the same collection-only flow.
Default discovery looks for **contacts, job listings/descriptions, about-company
content (including activities/services), and financial-information links**.
Custom instructions and explicit page lists remain supported. LLM calls are limited
to site eligibility/optional brief descriptions and page selection. Job analysis,
technology inference, tracker/resource inventories and catalog lookup are disabled
in this flow; their implementations remain available for future offline processing.
Collected HTML, readable text and published JSON-LD/microdata are kept intact.
See [collection scope](CRAWL_AND_ANALYZE.md#current-collection-scope).

Version 0.27.0 added [deterministic page observations](PAGE_OBSERVATIONS.md): metadata,
readable text, JSON-LD/microdata, contacts, company identifiers, tracker IDs,
document/resource links and selected response headers. Every successful capture
keeps these in its JSON even when detailed artifacts are disabled, with no extra
fetches or LLM calls. Migration 422 exposes `page_observations` in ClickHouse.

Version 0.26.0 adds [ClickHouse website results and S3 queries](CLICKHOUSE.md).
The result table preserves history with jobs, services, contacts and other JSON
sections in separate columns. A latest view keeps crawl/analysis stages separate;
an external S3 view reads uploaded bundles directly. `crawler-service-clickhouse`
imports saved or S3 results without recrawling.

Version 0.25.0 adds [S3 delivery for JetStream requests](SERVICE.md#s3-results-and-completion-events).
The worker uploads compressed JSON with collected HTML, publishes a durable
completion event and then acknowledges the request. Delivery resumes from saved
state after failures; optional diagnostics upload alongside the result. Enable
with `CRAWL_S3_BUCKET` or `--s3-bucket`. REST and CLI keep their local output.

Version 0.24.0 makes separate crawl artifacts optional. They remain enabled by
default during development; use `--no-save-artifacts` or a REST/JetStream request
with `"save_artifacts": false` to retain only the bundled `result.json` in each
crawl attempt. The JSON includes complete inputs for later analysis, which accepts
`--crawl path/to/result.json`. See [artifact settings](CRAWL_AND_ANALYZE.md#artifact-retention).

Version 0.23.0 adds a [local crawl service](SERVICE.md) with REST, the existing
CLI, and NATS JetStream inputs. All use the same crawler and publish `result.json`
with the full manifest and collected HTML. REST/NATS share persisted jobs,
request-ID deduplication, restart recovery and bounded workers. JetStream is
acknowledged after a local result is saved. Install with `uv sync --extra service`
inside this package; its own lockfile and `.env.example` make setup independent
of the sibling labs. LLM fact analysis remains a separate stage.

Version 0.41.0 removes the NATS JetStream input and its result stream. The service
is REST-only, the same transport model as the browser service.

The [selector model comparison](SELECTOR_MODEL_COMPARISON_20260917.md) replays
identical NOVELIC inputs through DeepSeek Flash, GLM‑5.3 and Qwen3.8 Flash. All
agree on 187/187 eligibility decisions; measured times are 95s, 59s and 356s.
GLM is fastest in this run; Qwen has the lower reported OpenRouter cost
($0.0176 versus $0.0958). Runtime defaults are unchanged.

Version 0.22.0 uses a compact response schema for custom page-selection instructions.
The selector returns relevance, direct/navigation role, company scope and a short
reason, without scoring all ten general research objectives. The general selector
is unchanged. NOVELIC retained 16/16 listed jobs while runtime fell from 8m41s to
2m34s and output tokens from 126,343 to 27,146. The frozen-input replay agreed on
187/187 eligibility decisions with 82.59% fewer output tokens. See
[the benchmark and its limits](COMPACT_SELECTOR_RESULTS_20260917.md).

Version 0.21.0 adds `--site-info`: a few factual sentences describing the company's
activities, products and services, or a non-company site's purpose and content.
Alone it reads only the input page and stops; combine it with `--pages`,
`--instructions` or `--crawl` to also crawl. The first-page eligibility call supplies
the description, with no separate summary call. Requested information and skipped
sites share the same `site_info` output and local `site-info.json` file. See
[usage and output fields](CRAWL_AND_ANALYZE.md#get-a-brief-description-without-a-further-crawl).

Version 0.20.0 adds custom instructions to the page-selection module:
`crawler-service-crawl URL --instructions "Get current job listings and full job descriptions" --output-dir PATH`.
The same instructions guide every discovery pass. `--instructions-file PATH` loads
a saved request. `--pages URL1 URL2 ...` supplies an allowed URL list; without
instructions all listed pages are fetched, and with instructions the selector
chooses only within that list. See [the guide](CRAWL_AND_ANALYZE.md).

Version 0.19.0 separates [local HTML crawling and later LLM analysis](CRAWL_AND_ANALYZE.md).
Use `crawler-service-crawl URL --output-dir PATH` for automatic discovery, or add
repeatable `--page URL` options to fetch an exact list without LLM calls. Then run
`crawler-service-pages --crawl PATH` to analyze the saved folder independently.
Automatic discovery still uses the existing site gate and LLM link ranking;
extraction-dependent queue feedback is unavailable until the analysis stage.

See the [research checkpoint](../../../codex-sd-examples/RESEARCH_CHECKPOINT.md) for saved findings,
known NOVELIC extraction gaps and next work. PDF/OCR experimentation is paused.

Version 0.18.1 adds `--api`, `--model`, `--provider`, `--reasoning-effort` and
`--timeout` to the saved-page command for explicit model comparisons. Both APIs
receive the same prompt/schema messages in JSON-object mode. The default remains
direct DeepSeek Flash with high reasoning and a 300-second request deadline.

The [GLM comparison](NOVELIC_GLM_COMPARISON_20260917.md) reuses all 26 NOVELIC
captures. GLM low cost $0.623 and took 24m22s versus DeepSeek high's estimated
$2.812 and 54m56s. Both recover the checked jobs and management names, with different
technology and evidence failures; this is not a same-effort comparison. The
[JSON summary](NOVELIC_GLM_COMPARISON_20260917.json) records exact metrics and limits.

Version 0.18.0 removes RustFS/S3 output. The crawler returns complete JSON to its
caller; `crawler-service-pages` emits one JSON object on stdout and progress on
stderr. Local diagnostic files are optional with `--output`; temporary working
files are cleaned up otherwise. The upload command, bucket option and storage
dependency are removed. See [the JSON output guide](PAGE_RESEARCH.md).

The [September 17 NOVELIC scan](NOVELIC_SCAN_20260917.md) recovered all 16 checked
jobs and management names and 31/32 selected technologies on 26 guided pages.
The direct DeepSeek Flash/high run used 5.95M tokens and cost an estimated $2.81,
plus a separately documented $0.31 interrupted autonomous diagnostic. Cookie-vendor
attribution, format filtering, evidence holds and repeated context still block
unattended use. Its historical report and receipts preserve the run before storage
integration was removed. All remote experiment objects were subsequently deleted;
local results remain available. See [the deletion receipt](RUSTFS_CLEANUP_RECEIPT.json).

Version 0.17.1 handles Crawl4AI's bare result on robots denial as well as its normal
result container. The [Informer/B92/Google test](SITE_GATE_REQUESTED_RESULTS.md)
skipped both news portals after one model call each; Google's robots-check rejection
now returns its actual reason and `needs_review`, without a model call.

Version 0.17.0 adds a mandatory [first-page eligibility gate](SITE_PROFILE_FLOW.md)
to `research_company` and the `crawler-service` URL command (result schema 1.11).
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

Version 0.16.0 introduced [saved-page research](PAGE_RESEARCH.md):
one page request collects all objectives, raw technology mentions and scored links;
a bounded final pass classifies the mentions, followed by read-only catalog lookup.
Complete JSON includes original source text, exclusions and review items. The former
S3 upload path was removed in 0.18.0.
The new package command is `crawler-service-pages`. The existing URL crawler still
uses its earlier controller; integrating this page unit into that queue is next.

The standalone [page-agent lab](../../../codex-sd-examples/page_agent_lab/README.md) now implements both
one-pass and routed-specialist variants. Its [seven-page results](../../../codex-sd-examples/page_agent_lab/RESULTS.md)
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
quotations or IDs. See the [page-agent follow-up](../../../codex-sd-examples/page_agent_lab/FOLLOW_UP.md).
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

One website URL in; collected source pages, contacts, jobs, company/about content
and financial-information links out as JSON, with simplified HTML and raw evidence.

The package uses CloakBrowser through Playwright directly. DeepSeek assesses site
eligibility and navigation candidates; static code extracts page observations.
A Python queue balances objectives and enforces limits. Fact interpretation and
technology analysis remain separate from the collection flow.
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
python -m pip install -e .
crawler-service-crawl https://www.example.com --env-file /path/to/crawler.env \
  --output-dir data/example > example.json
```

Requires Python 3.12 or newer. Configure `DEEPSEEK` for automatic discovery, or
`OPENROUTER_API_KEY` with `--api openrouter`. Explicit page lists without site-info or
selection instructions require no model key. No technology catalog or database
connection is needed to crawl. The package never automatically
searches other projects for credentials. Browser binaries are downloaded by
CloakBrowser on first use; this needs internet access and a supported browser host.

Python API:

```python
import asyncio
from pathlib import Path
from crawler_service.crawl import crawl_company
from crawler_service.models import ResearchConfig

result = asyncio.run(
    crawl_company(
        "https://www.example.com",
        output_dir=Path("data/example"),
        config=ResearchConfig(max_pages=20),
    )
)
print(result["status"])  # Full collected HTML and observations are in data/example/result.json.
```

The sections below describe the preserved legacy analysis implementation, which
does not run as part of collection. Its old controller lives in
`crawler_service.research`; it is no longer the package-level Python API or CLI.
The saved-page analysis command remains an explicit, separate experimental tool.

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

Development checks, from `corpscout/services/crawler_service`:

```sh
uv sync --all-extras
uv run --all-extras python -m unittest discover -s tests -v
COMPANY_RESEARCH_BROWSER_TEST=1 uv run --all-extras python -m unittest discover -s tests -v
uvx ruff check src tests benchmarks
uvx ty check src --python .venv/bin/python
uv build --wheel
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
