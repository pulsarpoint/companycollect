# Crawl HTML, then analyze it

Version 0.23.0 exposes collection through [REST, CLI and NATS JetStream](SERVICE.md).
Every crawl additionally saves `result.json` with the complete manifest and cleaned
HTML content. The existing capture files remain the input to later analysis.

The package separates collection and interpretation into two runnable modules.
Version 0.22.0 uses compact custom-instruction selection alongside the optional
brief site information introduced in v0.21.0:

- `company_research.crawl`: discover useful pages, render them with Crawl4AI, and
  save their cleaned HTML in a local folder. No company-fact extraction, technology
  classification, catalog lookup, full company-overview generation or uploads run
  here. The first-page eligibility call can also return a brief site description.
- `company_research.analysis`: read saved captures and run the existing mention-first
  page extraction and final technology classification. It never starts a browser or
  fetches website pages. `company_research.page_run` supplies its CLI settings.

The boundary is a versioned `crawl-manifest.json` and ordinary UTF-8 HTML files.
Analysis can be repeated with another model against the same captures.

## Get a brief description without a further crawl

```bash
company-research-crawl https://www.novelic.com/ \
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
company-research-crawl https://www.novelic.com/ \
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
company-research-crawl https://www.novelic.com/ \
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
uses that relevance rather than balancing unrelated company-research objectives.
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
company-research-crawl https://www.novelic.com/ \
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
company-research-crawl https://www.novelic.com/ \
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
company-research-crawl https://example.com/ \
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

```text
runs/novelic-crawl/
  crawl-manifest.json
  site-info.json        # when requested, or when the site gate stops the crawl
  pages/
    p0001/
      page.html          # Crawl4AI cleaned HTML, without LLM rewriting
      input.json         # source URL, hashes, timestamp, headings and link inventory
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
company-research-pages \
  --crawl runs/novelic-crawl \
  --output runs/novelic-analysis \
  --env-file .env
```

`--crawl` also accepts the manifest file itself. `--page PATH` remains available
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

from company_research.crawl import crawl_company

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

`company_research.analysis.analyze_pages` accepts a crawl folder/manifest `Path`
or a sequence of snapshot directories. Its explicit keyword arguments are
`output_dir`, `config` (`ResearchConfig`), `api_key`, `api`, `catalog` and
`catalog_info` (catalog availability/provenance metadata). It returns the complete
research dictionary. The CLI handles environment files and catalog loading.

The historical `company-research URL` command still runs the older interleaved
controller. Use the two commands above for the separated workflow.
