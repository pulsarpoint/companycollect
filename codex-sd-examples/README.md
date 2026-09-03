# Company website crawler examples

The repository contains three approaches:

- `ex1`: the application manages the crawl frontier and the LLM ranks links.
- `ex2`: Crawl4AI manages the complete breadth-first crawl and the LLM only
  extracts structured facts from each returned page.
- `ex3`: crawl and analysis as independent commands connected by a durable
  crawl manifest, plus a `research` command that chains them with bounded,
  LLM-guided follow-up passes. The crawl command selects English, picks the
  most informative pages from the site's URL inventory, and stores a bounded
  crawl as Markdown; the analysis command can run against those files
  repeatedly without crawling again; `research` runs both once, then asks the
  LLM what is still missing and crawls and analyzes up to `--max-passes`
  rounds to fill it in.

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
`hreflang`, language selector, locale path, or locale query parameter. The
browser and every inventory request send `Accept-Language: en-US,en;q=0.9`
(configurable with `--accept-language`) so sites that negotiate language serve
English on their own.

Pages are then chosen before they are rendered:

1. **Inventory.** Crawl4AI's `AsyncUrlSeeder` reads the host's sitemap (or
   Common Crawl with `--seed-source sitemap+cc`) into a URL inventory, capped
   by `--seed-max-urls`.
2. **Candidates.** The scorer in `ex3/selection.py` removes external
   domains, binary files and duplicates, orders the rest structurally
   (homepage, depth, linked from the base page, vocabulary as tie-break) and
   caps the list at `--llm-candidates` (200). The base URL and the base page's
   links always join, because sitemaps often omit them. The `<head>` of each
   candidate is fetched for title and declared language.
3. **LLM selection.** One Codex call receives the candidates (path, title,
   anchor text, language) and the information requirements from
   `ex3/requirements.py`, and returns up to `--max-pages` URLs with a reason
   and the fields each should fill. Picks are validated against the candidate
   list. `--selector deterministic` uses the scorer instead, and the scorer is
   the automatic fallback when the call fails.
4. **No sitemap.** Crawl4AI's `BFSDeepCrawlStrategy` crawls from the base URL
   within `--max-pages` and `--max-depth` (`--discovery best_first` for the
   scored variant).
5. **Remaining budget.** If a sitemap exists but the budget is still not full
   after selection (crawl failures, a small inventory), the same discovery
   crawl fills what selection left open; already selected pages are skipped.
   `--no-seed` skips the inventory and LLM selection entirely, so discovery
   alone fills the whole budget.

```bash
uv run python -m ex3.main crawl https://example.com \
  --markdown-dir company-markdown \
  --max-pages 20 \
  --max-depth 2
```

No LLM extraction runs during this command. Each stored page records its
declared `<html lang>`, whether it came from the pre-crawl selection or link
discovery, and its selection score. The full scored inventory (selected,
eligible and excluded URLs with reasons) is written next to the manifest as
`url-inventory.json`, and the manifest's `url_seeding` block summarizes the
selection.

### Phase 2: analyze existing Markdown

The analysis command accepts the manifest created above. It validates all
referenced Markdown files before starting Codex, groups them by page and
character limits, and writes a consolidated report. It does not launch
CloakBrowser or Crawl4AI. Non-English pages are analyzed by default; the
extraction prompt asks the model to translate summaries and descriptions into
English while keeping names, values and identifiers verbatim. Pass
`--skip-non-english` to leave pages whose stored `<html lang>` is not English
out of extraction; they are then listed under `skipped_pages`, and link
discovery below still reads them.

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
- `company-markdown/url-inventory.json`: every scored inventory URL with its
  reasons and exclusion, for tuning the selector. Both selectors write the whole
  ranked inventory there: eligible URLs, excluded ones with their reason, and
  the selected pages. The LLM selector only saw the top `--llm-candidates` of
  the eligible list, refined with `<head>` metadata;
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

### Passes: let the LLM ask for more

`research` runs everything end to end and adds a configurable second pass:

```bash
uv run python -m ex3.main research https://example.com \
  --workdir company-research \
  --max-pages 20 \
  --pass-pages 10 \
  --max-passes 2
```

1. `crawl` and `analyze` produce `report-pass-1.json`.
2. `suggest` computes which requirements are still missing or weak
   (`ex3/requirements.py`) and shows the LLM every discovered or listed URL
   that was not processed; it returns up to `--pass-pages` URLs with the
   fields each should fill, validated against the candidates
   (`suggestions-pass-2.json`).
3. `extend` crawls those pages into the same manifest, tagged with the pass
   number and the reason.
4. `analyze --previous-report report-pass-1.json` extracts only the new pages,
   consolidates all rounds deterministically, then asks the LLM once to merge
   the previous result and the new round into one profile. Evidence URLs
   outside processed pages are dropped and counted in `merge_analysis`.

`--max-passes 1` disables the extra pass; the loop also stops when nothing is
missing or the LLM suggests nothing. A pass that fails is logged and the run
keeps the last completed report. Each command can be run on its own with the
artifacts above.

Token accounting follows the passes. `analysis_stats` and each
`passes[*].token_totals` cover one pass only: its extraction batches, the
related-domain call, the page-selection call in pass one, the suggestion call
of that pass (`analyze --suggestions suggestions-pass-N.json`, which `research`
supplies automatically), and its merge call. `run_token_totals` sums every pass
and is what the run cost end to end.

## Example 4: page-selection prompt lab

`ex4` compares page-selection prompts in isolation: no crawling, no
extraction. It reads each test site's sitemap, builds the same candidate list
the `ex3` selector sends to the model (URL, title, language, anchor text; capped
at 200), runs every prompt file under `experiments/page-selection/prompts/`
through Codex, and scores the picks against a hand-corrected gold set of
must-have pages per site.

```bash
uv run python -m ex4.main sites verify
uv run python -m ex4.main candidates build
uv run python -m ex4.main gold draft            # then edit experiments/page-selection/gold/*.json
uv run python -m ex4.main run --dry-run         # how many Codex calls
uv run python -m ex4.main run --run-id 20260903-1
uv run python -m ex4.main score --run-id 20260903-1
uv run python -m ex4.main report --run-id 20260903-1
```

Results are cached per prompt text, candidate list and limit under
`experiments/page-selection/runs/<run-id>/`; re-running a run id only calls
Codex for missing or failed cells (`--retry-failed`). Scores rank prompts by
mean gold coverage, then junk rate, then tokens; `--repeats 2` adds a
run-to-run stability column.

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
