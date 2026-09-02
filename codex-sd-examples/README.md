# Company website crawler examples

The repository contains three approaches:

- `ex1`: the application manages the crawl frontier and the LLM ranks links.
- `ex2`: Crawl4AI manages the complete breadth-first crawl and the LLM only
  extracts structured facts from each returned page.
- `ex3`: two independent phases connected by a durable crawl manifest. The
  first command selects English and stores a bounded crawl as Markdown; the
  second command can analyze those files repeatedly without crawling again.

## Example 1: LLM-assisted frontier

This example combines three responsibilities:

- CloakBrowser provides the Chromium process and CDP endpoint.
- Crawl4AI renders each page, produces Markdown, and discovers links.
- Codex extracts structured company data and ranks up to five promising links.

The crawler owns the frontier, visited set, depth limit, page limit, URL
normalization, and same-site policy. LLM recommendations can improve link
priority, but cannot inject arbitrary URLs into the frontier.

## Install

Python 3.12 and `uv` are required.

```bash
uv sync
```

The Codex SDK uses your existing Codex authentication. CloakBrowser downloads
its browser binary on first use.

## Run

```bash
uv run python -m ex1.main https://example.com \
  --max-pages 20 \
  --max-depth 2
```

The command writes:

- `company-report.json`: consolidated information plus page-level evidence.
- `company-crawl-state.json`: the pending frontier and all completed work.

Existing state or output files are not overwritten implicitly. Start over
explicitly with:

```bash
uv run python -m ex1.main https://example.com --restart
```

Resume an interrupted or page-limited crawl, optionally with a larger budget:

```bash
uv run python -m ex1.main https://example.com \
  --resume \
  --max-pages 40
```

External URLs are disabled by default. When enabled, only external links that
the LLM recommends from the supplied candidate list are scheduled:

```bash
uv run python -m ex1.main https://example.com \
  --allow-external \
  --max-pages 30
```

The crawler respects `robots.txt` by default. Run `--help` to see browser,
proxy, content-size, and crawl-budget options.

## Example 2: Crawl4AI breadth-first search

`ex2` gives navigation ownership to Crawl4AI. One `crawler.arun(...)` call uses
`BFSDeepCrawlStrategy`, which follows links level by level, tracks visited URLs,
and enforces both `max_depth` and `max_pages`. After crawling finishes, Codex
receives each page's URL and Markdown for extraction only. Its output schema has
no `next_links` field.

Run it with:

```bash
uv run python -m ex2.main https://example.com \
  --max-pages 20 \
  --max-depth 2
```

The default is same-site crawling. External traversal must be enabled
explicitly, and remains bounded by the page limit:

```bash
uv run python -m ex2.main https://example.com \
  --include-external \
  --max-pages 30
```

The command writes `company-bfs-report.json`. It records the Crawl4AI strategy,
page counts, maximum depth reached, crawl and extraction failures, consolidated
facts, page-level evidence, and full per-page and aggregate Codex token usage.
Use `--overwrite` to replace an existing report. Each extraction has a
three-minute timeout by default, configurable with `--analysis-timeout`.

## Example 3: Durable crawl and repeatable batch extraction

`ex3` exposes collection and analysis as separate commands with different
configuration, runtime, and scaling characteristics.

### Phase 1: crawl once

The crawl command probes the requested URL for an English document or English
`hreflang`, language selector, locale path, or locale query parameter. It then
runs a streaming Crawl4AI BFS, saves every successful page as Markdown, closes
the browser, and writes the durable manifest.

```bash
uv run python -m ex3.main crawl https://example.com \
  --markdown-dir company-markdown \
  --max-pages 20 \
  --max-depth 2
```

No LLM extraction runs during this command.

### Phase 2: analyze existing Markdown

The analysis command accepts the manifest created above. It validates all
referenced Markdown files before starting Codex, groups them by page and
character limits, and writes a consolidated report. It does not launch
CloakBrowser or Crawl4AI.

Every HTTP(S) link in the complete stored Markdown set is normalized and
deduplicated into top-level `discovered_urls`; this deterministic discovery does
not consume LLM tokens. A separate constrained LLM call then classifies the
external-domain candidates and may select official global sites, country sites,
parent companies, subsidiaries, or brands as `related_domains`. Model-selected
domains are validated against the discovered candidates, so the LLM cannot add
an unseen domain.

```bash
uv run python -m ex3.main analyze company-markdown/crawl-manifest.json \
  --output company-batched-report.json \
  --max-page-chars 30000 \
  --max-batch-pages 5 \
  --max-batch-chars 60000 \
  --analysis-timeout 300
```

To try a different prompt, model behavior, batch size, or timeout, run only the
analysis command again and choose a new output file (or pass `--overwrite`):

```bash
uv run python -m ex3.main analyze company-markdown/crawl-manifest.json \
  --output company-batched-report-v2.json \
  --max-batch-pages 3
```

The default artifacts are:

- `company-markdown/*.md`: page Markdown;
- `company-markdown/crawl-manifest.json`: the pre-analysis crawl manifest;
- `company-batched-report.json`: consolidated extraction, page provenance,
  batch outcomes, and per-batch plus aggregate token usage.

The LLM output has no navigation fields. Every batch must return one result per
supplied source URL; unknown and duplicate URLs are discarded, missing pages
are recorded as extraction failures, and token totals are counted once per
batch. Crawl failures live in the manifest and start with `crawl:`; LLM failures
in the report start with `analysis batch`. Each discovered URL retains its link
type, labels, occurrence count, and source pages. The top-level
`related_domain_analysis` records the classification status and token usage;
each accepted `related_domains` entry includes the LLM relationship and reason
plus deterministic URL provenance.

## Output

The consolidated `useful_information` object contains:

- contacts;
- company identity, description, industries, locations, and identifiers;
- products and services;
- jobs;
- other supported facts.

Each extracted item retains short evidence and the exact page URL. The
`pages` collection preserves each page-level LLM result. In `ex1` it also
contains ranked link recommendations; in `ex2` it contains extraction data and
analysis metadata only.

In `ex1` and `ex2`, each page contains `analysis_metadata.token_usage`. In
`ex3`, usage belongs to the corresponding item in `batches`, while each page's
`extraction_metadata` identifies its batch and Markdown file. All approaches
record last-turn and thread-total input, cached-input, cache-write, output,
reasoning-output, and total token counts, plus context-window size and duration.
The top-level `analysis_stats.token_totals` sums usage once per analysis call.

## Tests

```bash
uv run python -m unittest discover -v
uvx ruff check main.py ex1 ex2 ex3 tests
uvx ty check main.py ex1 ex2 ex3 tests
```
