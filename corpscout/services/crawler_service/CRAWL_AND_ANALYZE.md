# Crawl HTML, then analyze it

Collection runs through [REST, CLI and NATS JetStream](SERVICE.md).
Version 0.24.0 saves a self-contained `result.json` for later analysis, with
optional separate HTML captures and diagnostics.

## Full crawl

```bash
crawler-service-crawl https://www.novelic.com/ --crawl full \
  --output-dir runs/novelic-full --env-file .env
```

Version 0.29 starts at the homepage, checks company-site eligibility, discovers
sitemap/page links and uses the LLM to select relevant pages across all four areas
below. It follows useful contact/about/service/product pages, careers gateways,
company-scoped external boards and individual job descriptions, plus financial
information/report pages. Each fetched page is processed with deterministic parsers;
there are no LLM job-extraction, technology-inference or catalog calls.

Full mode defaults to **100 pages, 30 external pages and 100 model calls**.
`--max-pages`, `--max-external-pages` and `--max-model-calls` override these limits.
Robots policy and normal page/model timeouts still apply. “Full” describes the
requested content scope, not guaranteed completeness: inspect `status`,
`stop_reason`, errors and the saved queue for excluded, failed or unvisited pages.

The saved `result.json` contains `documents[]`, with each page's `url`, cleaned
(simplified) `html`, `input.links`, `input.headings`, and `input.observations`:
contacts, readable text, JSON-LD/microdata, company identifiers and financial/report
links. Rendered HTML is also retained for later offline interpretation. Raw JobPosting
objects keep all published fields, including full descriptions; company/about text
is preserved without inventing missing structured facts.

`crawl.mode` is `full`, `crawl.elapsed_seconds` records elapsed collection time
through browser cleanup (before final result serialization), and `crawl.usage`
records prompt/completion tokens and per-call timing. Provider completion counts
may include reasoning tokens; per-call provider usage retains the detailed split.

Add `--site-info` for the brief first-page company description; the eligibility
call supplies it. Bare `--crawl` keeps its previous behavior with `--site-info`.
Use `--pages` or custom `--instructions` separately for restricted/targeted requests;
they cannot be combined with `--crawl full`. REST/JetStream use `"crawl": "full"`,
and Python uses `crawl_company(..., crawl="full")` with the same defaults and limits.

The [live NOVELIC run](NOVELIC_FULL_CRAWL_20260918.md) collected 68 unique pages,
including all 16 discovered job descriptions, in 510.199 seconds. It used 146,636
input and 51,846 completion tokens; model calls took 263.837 seconds, fetching/link
processing 220.042 seconds, and additional static observations/capture writes
23.173 seconds. Three HTTP failures made the result partial. The report and linked
JSON retain exact limits, failures, provider usage and source provenance.

## Source discovery

Version 0.30 enables objective-driven web search with `--crawl full`. For a
targeted crawl, opt in explicitly:

```bash
crawler-service-crawl https://www.novelic.com/ \
  --instructions "Find company financial statements and acquisition disclosures" \
  --web-search --output-dir runs/novelic-financial --env-file .env
```

Use `--no-web-search` to restrict discovery to website/sitemap links. Page lists
remain strict, and `--site-info` alone never searches. Web search uses Brave's
ordinary result cards through the existing browser runtime; generated answers and
advertising are excluded. No additional API credential is required. Search markup,
rate limits or blocking can make it unavailable; failures are recorded explicitly
and make the result partial while ordinary page collection continues.

The navigation model proposes one initial query, can refine it after collecting
source pages, and can search again when eligible page links are exhausted. A
bounded excerpt from the three latest captures informs follow-up queries; it
remains unverified source text, not extracted financial facts. Queries include the quoted target name.
Results are unverified candidate links, subject to the usual model selection and
page limits. PDF search results are retained as document references, not fetched.
There is no form submission or interactive registry lookup in this version.

The selector can follow a parent's investor/subsidiary-report pages or a filing
source with `follow_scope=source_navigation`. New sources require an exact
quotation from supplied context; parent evidence must come from the target's own
site. These are source-matched navigation hypotheses, not independently verified
ownership. Traversal stays on that source's registrable domain, and every page is
still assessed against the caller's objective. Parent products, jobs and other
subsidiaries are excluded from target selection. Source owner and target subject
remain separate; financial amounts are not extracted or attributed during crawling.

| Limit | Default | CLI / REST and JetStream `config` |
| --- | ---: | --- |
| Search requests | 3 | `--max-search-queries` / `max_search_queries` |
| Results per query | 5 | `search_results_per_query` |
| Total source-text characters per search-planning pass | 12,000 | `search_context_chars` |
| Parent/filing source domains | 3 | `--max-source-domains` / `max_source_domains` |
| Pages per approved source domain | 4 | `--max-source-pages-per-domain` / `max_source_pages_per_domain` |
| Link depth beyond source entry page | 3 | `--max-source-depth` / `max_source_depth` |

Set `max_source_domains=0` to disable expanded source navigation. Source pages
also consume global/external page budgets. Search planning consumes the existing
model-call/token budget; search result pages have a separate query limit and are
not company captures. These bounds do not establish complete coverage.
The selector assigns an explicit 0-100 priority to break ties between equally
useful pages; target-specific report indexes precede broad investor gateways.
Useful navigation blocked by source page/domain limits produces a partial result
with `source_page_budget` or `source_domain_budget`, rather than reporting no matches.

`result.json` always preserves:

- `crawl.web_search`: queries, reasons, timestamps, result URLs/titles/snippets,
  result ranks, elapsed search times and failures.
- `crawl.discovery`: source approvals, source page counts, document references
  with labels/context and exclusion counters.
- Page `selection`: target relevance, follow scope, selection reason and source
  entry URL where applicable.
- Document observations: source URL, nearby row text, heading and page title.
  Financial links record whether the match came from the anchor/URL, surrounding
  context or the title of a document's source page. A match is not verification of
  a document's contents or subject.

Separate `searches/` HTML/JSON and queue/model diagnostics are retained only when
artifacts are enabled. Financial interpretation and PDF/OCR processing remain
offline; this change collects document references and source HTML.

## Current collection scope

Version 0.28.0 uses the collection flow for `crawler-service-crawl`, as well as `python -m crawler_service`, REST and JetStream.
Its default selection instructions request:

- Contact pages, office locations and published contact/profile details.
- Careers listings and full job descriptions, including company-scoped external boards.
- Company identity, activities, products and services that explain what it does.
- Financial-information pages, investor relations and report/filing links.

Page selection uses the compact requested-content schema. It does not score
technology objectives, prefer engineering jobs or use extracted job records to
control navigation. `selection_instructions` records the caller's override (null
for defaults); `effective_selection_instructions` records what the selector used.
Exact page lists without instructions still fetch every supplied page with no LLM.
`--site-info` alone still stops after the first page.

The `processing` object marks `stage=collection`, with `job_analysis` and
`technology_analysis` both `deferred`. Contacts and published structured data are
extracted deterministically. JobPosting descriptions/fields and organization data
stay as source claims in `documents[].input.observations.structured_data`; unstructured
content stays in readable text and HTML. No job skills, technology usage or employment
facts are inferred. Optional `site_info` remains a brief first-page description.
Financial-link candidates are recorded separately in `observations.financial_links`;
PDF/document bodies are not downloaded or interpreted.

The old combined controller and analysis modules are preserved but disconnected
from crawl entry points. The saved-page replay command documented below remains an
explicit experimental tool; no analysis runs automatically after collection,
S3 upload or ClickHouse import. A database-driven offline processor is future work.

The package separates collection and interpretation into two runnable modules.
Version 0.22.0 uses compact custom-instruction selection alongside the optional
brief site information introduced in v0.21.0:

- `crawler_service.crawl`: discover useful pages, render them with CloakBrowser, and
  save their cleaned HTML in a local folder. No company-fact extraction, technology
  classification, catalog lookup, full company-overview generation or uploads run
  here. The first-page eligibility call can also return a brief site description.
- `crawler_service.analysis`: read saved captures and run the existing mention-first
  page extraction and final technology classification. It never starts a browser or
  fetches website pages. `crawler_service.page_run` supplies its CLI settings.

The stages exchange a versioned `result.json`; saved capture folders are also
accepted. Analysis can be repeated with another model against the same content.

## Artifact retention

Detailed artifacts are **saved by default during development**. Use
`--save-artifacts` explicitly to retain HTML files, link/headings metadata,
fetch outcomes, queue decisions and model-call diagnostics. Disable them with:

```bash
crawler-service-crawl https://www.novelic.com/ \
  --pages https://www.novelic.com/careers/ \
  --no-save-artifacts --output-dir runs/novelic-json

crawler-service-pages --crawl runs/novelic-json/result.json \
  --output runs/novelic-analysis --env-file .env
```

The Python API accepts `save_artifacts=False`; REST and JetStream accept
`"save_artifacts": false` in each request. The default is `true` for all inputs.
This controls retention: the current crawler still uses temporary working files,
which are removed on normal exit, failure or cancellation. Only `result.json`
remains in the crawl output directory when disabled. A forced process kill can
leave OS temporary files for normal host cleanup.

The final JSON bundles cleaned HTML, rendered HTML when available, page metadata,
links and headings. It also preserves failures, site information and usage. The
`crawl.artifacts_saved` field records the choice; paths to omitted files are not
advertised. Analysis unpacks this JSON temporarily, validates identities/hashes,
and uses the same inputs without fetching pages again. Artifact retention does
not change which pages are selected or their HTML content.

## Get a brief description without a further crawl

```bash
crawler-service-crawl https://www.novelic.com/ \
  --site-info \
  --output-dir runs/novelic-info \
  --env-file .env
```

`--site-info` alone fetches and classifies the input URL, then stops. It does not
request a sitemap, follow page links or run detailed extraction. It uses the same
model call as the company-site eligibility check. Browser resources and robots
checks still occur; the description is based only on that first captured page.

The description normally has 2–4 sentences, with a hard limit of 200 words. For
a company it describes its activities, products and services, plus customers or
industries when stated on the page. For a non-company site it describes its purpose,
content or functionality. No NACE code is assigned. Unsupported details are omitted.

Both use the same `site_info` object in `crawl-manifest.json` and stdout. A copy is
saved as `site-info.json`. Its fields are:

| Field | Meaning |
|---|---|
| `site_description` | Brief factual prose |
| `operator_name` | Supported company/operator name, otherwise null |
| `site_types` | Primary purpose labels such as company or news_media |
| `purpose`, `business_activities` | Supported purpose and activities |
| `crawl_decision` | continue_crawling, skip_crawling or needs_review |
| `source_url`, `scope` | Final input URL and first_page_only scope |
| `evidence`, `evidence_status` | Supporting source fragments and presence-check status |

Skipped sites return this object even without the flag. For an unreadable page or
unmatched classification evidence, the brief returns `needs_review`, an explanation
that the purpose could not be determined, and no asserted company or activities.
Raw classification attempts remain in the diagnostics. Source-matched text is not
independent verification of a site's claims.

For an admitted company, information-only mode returns `status: finished`,
`stop_reason: site_info_complete`, and `mode: site_info`. A non-company site keeps
`status: skip_crawling`; uncertainty keeps `status: needs_review` and exit code 2.
The `continue_crawling` decision means the site is eligible, not that more pages
were requested. `crawl_requested` records that distinction.

## Include the description while crawling

Combine `--site-info` with `--instructions`, `--instructions-file` or `--pages`
to describe the site and perform that crawl:

```bash
crawler-service-crawl https://www.novelic.com/ \
  --site-info --instructions "Collect current job listings and full descriptions" \
  --output-dir runs/novelic-jobs-with-info --env-file .env
```

Use `--site-info --crawl` for a brief plus the default general discovery algorithm.
With a supplied list, `--site-info` explicitly requests the input URL first, even
if it is outside the list. That extra page counts toward `--max-pages`; subsequent
pages remain restricted to the list. No sitemap is requested for a list. A skipped
or uncertain first-page decision prevents all follow-up pages. Without `--site-info`,
list mode keeps its original behavior and does not add or classify the homepage.

The Python API follows the same rules: `crawl_company(..., site_info=True)` returns
only the brief and first-page capture; adding `pages`, `instructions`, or `crawl=True`
continues the crawl. `crawl=False` is accepted only for information-only mode.
All calls still return the full manifest and save captures under `output_dir`.

## Find pages using custom instructions

```bash
crawler-service-crawl https://www.novelic.com/ \
  --instructions "Collect current job listings and full job descriptions. Skip employee stories and other employers." \
  --output-dir runs/novelic-jobs \
  --max-pages 30 \
  --max-external-pages 20 \
  --env-file .env
```

You do not need to know the careers URL. The crawler starts at the target site,
runs the existing company eligibility gate, discovers links and sitemap candidates,
and asks the selector which pages contribute to your instructions. It applies the
same instructions to every later selection pass. `--instructions-file PATH` reads
a UTF-8 file instead of an inline string; the two options are mutually exclusive.

Instructions can request any page content, such as office contacts, annual reports,
leadership profiles or a specific product's documentation. They are not restricted
to predefined objective names. The selector returns `requested_content` relevance,
a direct-content or navigation role, and a reason for each candidate. The queue
uses that relevance rather than balancing unrelated crawler-service objectives.
Missing/invalid assessments are retried and retained as errors; they never silently
fall back to unrelated pages. Selection is still a hypothesis based on link metadata,
not proof of destination contents or complete coverage.

Custom selection returns only `candidate_id`, `requested_content`, `target_relevance`,
`follow_scope` and a reason of at most 300 characters. Scope fields are required.
It does not request the broad `objectives` or `page_kind` fields; the general
selector still uses those without custom instructions. For jobs, application
forms/referrals/CV checks are excluded unless explicitly requested. See the
[controlled replay and live benchmark](COMPACT_SELECTOR_RESULTS_20260917.md).

Navigation pages are useful intermediate steps: a careers gateway can lead to a
target employer's external board and then its individual job descriptions. Only
target-scoped external navigation is permitted, and external/page/model limits still
apply. Generic employer directories and unrelated companies' content are excluded.
The crawl folder retains the initial page and navigation captures as well as selected
content pages, preserving the route taken. Per-page `selection` entries distinguish
predicted direct content from gateways. No company records are extracted here.

## Collect useful pages automatically

After installing the package, run:

```bash
crawler-service-crawl https://www.novelic.com/ \
  --output-dir runs/novelic-crawl \
  --max-pages 20 \
  --env-file .env
```

Without custom instructions, automatic discovery retains the first-page company eligibility gate, sitemap and
page-link discovery, URL normalization and exclusions, bounded candidate inventory,
objective-balanced link ranking, engineering-page prioritization, exploration,
external-page limits, robots checks, retry limits and browser recovery. The shared
link-ranking function and prompts are also used by the older combined controller.
Direct DeepSeek is the default navigation API (`DEEPSEEK` in the environment);
`--api openrouter` uses `OPENROUTER_API_KEY`. `--max-model-calls` bounds navigation
requests. Site eligibility and link ranking still use an LLM.

Because extraction now runs later, the crawler has no verified fact counts, verified
job records, or per-page extraction yields. Those feedback signals are not simulated:
the queue balances navigation objectives with unknown fact coverage. Verified-job
reserves, yield-based repetition penalties and external traversal requiring an
extracted employer identity do not activate. Selected pages and order can therefore
differ from the older interleaved crawler. The manifest records this boundary and
does not claim complete site coverage. Job links remain candidates and can be
selected from their navigation assessments or supplied explicitly.

## Collect a supplied list of pages

```bash
crawler-service-crawl https://www.novelic.com/ \
  --pages /about-us/ /careers/ https://www.novelic.com/contact/ \
  --output-dir runs/novelic-selected
```

`--pages` accepts a list of URLs; `--page` is an alias, and repeated occurrences
extend the same list. Relative URLs resolve against the target URL. Without instructions or `--site-info` this mode
fetches only the supplied pages, including explicitly supplied external job pages.
It does not fetch the target homepage automatically, read sitemaps, follow discovered
links, run site eligibility, or call an LLM. It needs no model credentials or catalog.
Browser resources and robots checks still occur. Tracking/fragment duplicates are
normalized; redirects to an already captured page are deduplicated. The default
page limit is 20; increase `--max-pages` for a longer list. An oversized list is
rejected before crawling rather than silently truncated.

Combine a list with instructions to select only useful pages **within that list**:

```bash
crawler-service-crawl https://example.com/ \
  --pages /careers/ /jobs/engineer /products/ \
  --instructions "Get current vacancies and their full descriptions" \
  --output-dir runs/selected-jobs
```

Here the list defines the allowed URLs. The selector may skip unrelated listed
pages, but cannot add another destination; the homepage and sitemap are not fetched
automatically unless `--site-info` explicitly requests the input page first.
Browser resources and normal redirects are still allowed. The candidate
list may exceed `--max-pages`, which limits how many are fetched. This mode uses LLM
ranking and needs model credentials. No match is reported as `no_matching_candidates`,
not as proof that the company has no jobs. A request with no captured pages cannot
be passed to the saved-page analyzer.

## Local output

Every captured page includes [deterministic observations](PAGE_OBSERVATIONS.md)
from its rendered HTML: metadata, structured entities, contacts, identifiers,
tracker IDs, readable text, resource/document links and selected response headers.
They remain in the portable JSON when artifacts are disabled and require no extra
model calls. Observations remain distinct from LLM-derived company records.

With artifact saving enabled:

```text
runs/novelic-crawl/
  result.json            # complete portable input for later analysis
  crawl-manifest.json
  site-info.json        # when requested, or when the site gate stops the crawl
  pages/
    p0001/
      page.html          # deterministic simplified HTML, without LLM rewriting
      input.json         # source, hashes, headings, links and page observations
      link-page.html     # rendered source for additional evidence
    p0002/
      ...
  fetches/               # fetch outcomes and original link inventory
  html/                  # original cleaned capture artifacts
  link-html/             # original rendered captures
  external-links/        # source observations, including uncrawled links
  queue.json             # automatic-discovery queue, when used
  calls/                 # navigation model diagnostics, when used
```

`page.html` is the useful page content. `link-page.html` is separate rendered
evidence retained for link context and content missing from cleaned HTML; analysis
does not confuse the two representations. The HTML content is unchanged from the
existing fetch adapter. Document/PDF links are retained as links; PDF contents are
not downloaded or analyzed by this stage.

The manifest includes requested and final URLs, successful and failed fetches,
relative capture paths, SHA-256 hashes, crawl limits, stop reason, navigation usage
and site eligibility. Captures are saved before ranking can fail. The folder can
be moved as a unit. Existing nonempty output folders are refused. Failed pages
remain visible, and page/model limits produce a partial result.

## Analyze the saved folder

```bash
crawler-service-pages \
  --crawl runs/novelic-crawl \
  --output runs/novelic-analysis \
  --env-file .env
```

`--crawl` also accepts `result.json` (schema `company-crawl-result/1.1` or `/1.2`) or the
manifest file itself. Earlier saved capture folders remain supported; older
`company-crawl-result/1.0` JSON needs its original capture folder for analysis.
`--page PATH` remains available
for individual saved snapshot directories, and is mutually exclusive with `--crawl`.

The analysis API/model/provider/reasoning/deadline options are unchanged. Optional
`--catalog PATH --offline-catalog` supplies a pinned technology catalog; otherwise
the existing read-only catalog synchronization settings apply. Catalog failures
remain explicit partial outcomes. Collection never needs that catalog.

The analyzer verifies capture paths, target identity and hashes before model calls,
copies its inputs into its own output folder, and preserves the crawl manifest in
the research JSON. Failed captures remain visible; analyzing the successful subset
does not upgrade a partial crawl to a complete result. Rejected, uncertain and
unfinished discovery runs are refused. No site-wide absence claims are inferred
from missing records. JSON is emitted on stdout, progress on stderr. `--output` is
optional; without it, temporary analysis diagnostics are cleaned up.

## Python API

```python
from pathlib import Path

from crawler_service.crawl import crawl_company

manifest = await crawl_company(
    "https://example.com/",
    pages=["/about", "/careers"],  # omit for automatic discovery
    output_dir=Path("runs/example-crawl"),
)

# Let the selector find relevant pages without knowing their URLs:
manifest = await crawl_company(
    "https://example.com/",
    instructions="Collect current job listings and full job descriptions; skip employee stories.",
    output_dir=Path("runs/example-jobs"),
)
```

`crawler_service.analysis.analyze_pages` accepts a crawl folder/manifest `Path`
or a sequence of snapshot directories. Its explicit keyword arguments are
`output_dir`, `config` (`ResearchConfig`), `api_key`, `api`, `catalog` and
`catalog_info` (catalog availability/provenance metadata). It returns the complete
research dictionary. The CLI handles environment files and catalog loading.

`crawler-service-crawl URL` is now an alias for collection. The former interleaved
controller is retained in `crawler_service.research` for reference and is not
exposed by a crawl command. Catalog flags belong only to the separate analysis tool.

## Redirect provenance

Classification and discovery use the first page's final destination, including
redirects to another domain. The original input stays in `crawl.input_url`; the
destination is `crawl.site_url` and `crawl.site_info.source_url`. ClickHouse keeps
the original domain as the request/result key.

Each page retains `requested_url`, final `source_url`, HTTP `redirects`, and
`navigation_attempts` (including an HTTPS failure followed by an HTTP attempt
when the browser discovers a legacy redirect). These fields are included in
the local result, S3 JSON and the ClickHouse `pages` section. An HTTP fallback
is an attempted connection, not a server-issued redirect.
