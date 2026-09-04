# Page-selection prompt lab (`ex4`) — design

**Status:** approved in discussion 2026-09-03; spec for review before planning.

## Goal

Find the best prompt for the pass-one page selector by running several prompt
variants through the Codex SDK against the sitemaps of several real company
websites, in isolation from crawling and extraction, and scoring each run
against a hand-corrected gold set. The winning prompt replaces
`create_page_selection_prompt` in `ex3` verbatim.

## Non-goals

No page rendering, no extraction, no second pass. No changes to `ex3`
behaviour except one additive parameter (see "Reuse of ex3"). Not a
general eval framework: one task (page selection), one output schema.

## Decisions taken in discussion

- Judge = gold set per site (primary). An LLM-as-judge is not part of this
  iteration.
- Sites are proposed by the implementer and verified for a sitemap by the
  tool; the user swaps any they dislike.
- The model sees the production input: URL, title, declared language and
  anchor text for a capped candidate list built by the `ex3` code.
- Budget ≈ 1M Codex tokens: 8 sites × 4 prompts × 1 run (32 calls), then the
  two best prompts repeated once on every site (16 calls) for stability.
- The tool is a new package `ex4/` in `codex-sd-examples`, reusing `ex3` as a
  library; artifacts live under `experiments/page-selection/`.

## Layout

```
ex4/
  __init__.py
  main.py          click group: sites, candidates, gold, run, score, report
  sites.py         site list model, sitemap verification
  candidates.py    candidate building via ex3 (seed, rank, head fetch, cap)
  prompts.py       prompt template loading and rendering
  gold.py          gold model, deterministic draft, matching
  runner.py        matrix execution, cache, concurrency, resume
  scoring.py       metrics and ranking
  report.py        results JSON + Markdown
experiments/page-selection/
  sites.json
  candidates/<site>.json
  gold/<site>.json
  prompts/<name>.md
  runs/<run-id>/manifest.json
  runs/<run-id>/<site>/<prompt>/<repeat>.json
  results/<run-id>.json, results/<run-id>.md
tests/test_ex4_*.py
```

`<site>` is the canonical domain (e.g. `handelsbanken.se`). `<prompt>` is the
prompt file stem. `<run-id>` is `YYYYMMDD-HHMMSS`.

## Reuse of ex3

- `ex3.seeding.seed_sitemap_urls`, `fetch_head_metadata`
- `ex3.selection.rank_urls`, `dedupe_by_url_key`
- `ex3.candidates.build_selection_candidates`, `PageCandidate`
- `ex3.crawler._preferred_languages` (English plus the site language; the
  site language is detected from the base page's `<html lang>` via the head
  fetch of the base URL, no browser)
- `ex3.requirements.requirements_text`, `TARGET_FIELD_KEYS`
- `ex3.llm.run_structured_turn`, `ex3.models.PageSelectionResponse`
- `ex3.llm_selection.select_pages_with_llm` gains one optional keyword
  parameter `prompt: str | None = None`; when given, it is used instead of
  `create_page_selection_prompt`. Default behaviour unchanged, covered by a
  test. Pick validation (unknown, duplicate, beyond-limit → warnings; picks
  echo the candidate's URL) is therefore identical to production.

Base-page links are not available without a browser; `base_page_links` is
empty in the lab. The lab's candidate list is the sitemap-derived part of the
production input, which is the part the prompt is meant to rank.

## Workflow (each step a subcommand with a durable artifact)

1. `sites verify` — for every entry in `sites.json`, seed the sitemap
   (`--seed-max-urls 5000`), record `sitemap_found`, `inventory_urls`, base
   page language; fail the entry (not the command) when no sitemap exists.
2. `candidates build [--limit 200]` — per site: rank the inventory, dedupe by
   `url_key`, cap, fetch heads for the shortlist, write
   `candidates/<site>.json` (`PageCandidate` list plus `preferred_languages`,
   `base_url`, `built_at`). Skips sites that already have a file unless
   `--overwrite`.
3. `gold draft` — writes `gold/<site>.json` when absent: for each gold field,
   the candidates whose URL slug or title matches the field's vocabulary in
   `ex3.selection` (about, contact, people, careers, offering) plus
   `group_structure` (subsidiar, group, koncern, dotterbolag) and `home` (the
   base URL). Junk = candidates hitting `ex3.selection.NEGATIVE_CATEGORIES`.
   The draft is a starting point; the user edits the file by hand.
4. `run --run-id <id> [--prompts a,b] [--sites x,y] [--repeats 1] [--limit 20] [--concurrency 2] [--timeout 300] [--dry-run]`
   — executes the matrix. `--dry-run` prints the number of calls that would
   be made and the cached ones. Each call renders the prompt template with
   `{requirements}`, `{limit}`, `{candidates}` (the same JSON shape as
   production: url, title, language, anchor_text, source), calls
   `select_pages_with_llm(..., prompt=rendered)`, and writes the result file
   before moving on. Two calls in flight; a failed call is recorded with its
   error and does not stop the run; re-running the same run-id resumes.
5. `score --run-id <id>` — computes metrics from result files and gold files;
   never calls Codex.
6. `report --run-id <id>` — writes `results/<id>.json` and `.md`.

## Artifact formats

`sites.json`: list of `{ "domain", "start_url", "country", "size": "small|mid|large", "note" }`.

`gold/<site>.json`:

```json
{
  "domain": "handelsbanken.se",
  "base_url": "https://www.handelsbanken.se/en/",
  "must_have": {
    "home": ["https://www.handelsbanken.se/en/"],
    "about": ["https://www.handelsbanken.se/sv/om-oss"],
    "contact": ["https://www.handelsbanken.se/en/personal/contact-and-support"],
    "management": [],
    "careers": ["https://www.handelsbanken.se/sv/om-oss/jobba-hos-oss/lediga-jobb"],
    "products_services": ["https://www.handelsbanken.se/en/personal", "https://www.handelsbanken.se/en/corporate"],
    "group_structure": ["https://www.handelsbanken.se/en/about-us/swedish-subsidiaries"],
    "legal_identity": ["https://www.handelsbanken.se/en/about-us/legal-documents/lei"]
  },
  "junk": ["https://www.handelsbanken.se/en/about-us/legal-documents/cookies"],
  "notes": "management lives on handelsbanken.com, not on this site"
}
```

Gold fields: `home`, `about`, `contact`, `management`, `careers`,
`products_services`, `group_structure`, `legal_identity`. A field with an empty
list is "not on this site" and is excluded from that site's denominator. Any
one of a field's URLs satisfies it. URLs are matched by `ex3.urls.url_key`.

Result file `runs/<id>/<site>/<prompt>/<n>.json`: `prompt_name`, `prompt_hash`,
`candidates_hash`, `limit`, `picks` (url, reason, expected_fields),
`llm` (`LlmCallStatus`: warnings, token_usage, error), `latency_ms`,
`raw_response` (the validated `PageSelectionResponse` as returned).

Cache key = sha256(prompt text ∥ candidates JSON ∥ limit) plus the repeat
index; a result file whose key matches is reused.

## Prompt variants (files under `prompts/`)

All variants share the SECURITY paragraph, "Never invent a URL; return each
selected url exactly as supplied", the output schema, and the `{limit}` cap.

- `p0-production.md` — the current `create_page_selection_prompt` text,
  unchanged (baseline).
- `p1-minimal.md` — requirements list and the hard rules only; no guidance
  about page types, languages or duplicates.
- `p2-classify-then-select.md` — first tag every candidate with the
  requirement keys it likely serves (reasoning is internal; output is still
  the schema), then choose the smallest set of pages that covers every
  requirement, listing the keys in `expected_fields`.
- `p3-coverage-quota.md` — for every requirement, select at least one page
  when any candidate plausibly serves it; once a requirement is covered,
  prefer breadth over depth; never select two languages of the same page;
  legal, cookie, login, search and locator pages only when nothing else
  covers a requirement.

The exact wording of p1–p3 is written in the implementation plan.

## Scoring

Per result file:

- `coverage` = covered gold fields ÷ applicable gold fields.
- `missed_fields` = the applicable fields with no matching pick.
- `junk_rate` = picks in the gold junk list ÷ picks.
- `other_language_rate` = picks whose candidate `language` (or URL locale
  marker) is outside the site's preferred languages ÷ picks.
- `picks`, `warnings` (count by kind), `input_tokens`, `output_tokens`,
  `total_tokens`, `latency_ms`, `error`.

Per prompt (across sites): mean and min `coverage`, mean `junk_rate`, mean
`total_tokens`, mean `latency_ms`, failure count. Stability per prompt and
site = Jaccard overlap of pick sets between repeats (reported only where
repeats exist).

Ranking: mean coverage desc, then junk rate asc, then total tokens asc. The
Markdown report shows the ranking table, a per-site coverage matrix
(sites × prompts), and for each prompt the fields it missed per site.

## Sites (initial proposal; verified by `sites verify`, swapped on request)

| domain | country | size | why |
|---|---|---|---|
| handelsbanken.se | SE | large | known baseline from ex3 |
| tobii.com | SE | mid | tech, English-first |
| pricer.com | SE | mid | industrial |
| polarbrod.se (fallbacks: lofbergs.se, kavli.se) | SE | small/mid | Swedish-first, few English pages |
| kongsberg.com | NO | large | industrial group |
| novozymes.com | DK | large | biotech |
| vaisala.com | FI | mid | instruments |
| trumpf.com | DE | large | Mittelstand, German-first |

Any entry without a sitemap is replaced before the matrix runs.

## Run plan and budget

1. `sites verify`, `candidates build`, `gold draft`; the user corrects the gold files.
2. Live smoke: one site, `p0-production`, one call.
3. Matrix: 8 sites × 4 prompts × 1 repeat = 32 calls.
4. `score` + `report`; pick the top two prompts.
5. Stability: those two prompts × 8 sites, repeat index 2 = 16 calls.
6. Final report; the winner's text is proposed as the new production prompt
   (a separate change to `ex3/prompty.py`).

Estimated ≈ 48 × 25–35k ≈ 1.2–1.7M tokens worst case; `--dry-run` shows the
count before each step.

## Error handling

Sitemap missing or unreachable → site marked failed, others continue. Head
fetch failures → candidate without title/language. Codex error, timeout or
invalid output → result file with `error`, scored as coverage 0 and counted
under failures. Gold file missing for a site → that site is skipped by `score`
with a warning. Nothing in the lab writes outside `experiments/page-selection/`.

## Testing

Unit tests with fakes at the Codex and HTTP boundaries: template rendering
(placeholders present, unknown placeholder rejected), cache key stability,
resume (existing result file not re-run), gold matching by `url_key`
(trailing slash, `www`), coverage with empty gold fields excluded,
junk/other-language rates, ranking order and tie-breaks, report tables,
`select_pages_with_llm(prompt=...)` override, `gold draft` on a fixed
candidate list, `--dry-run` counts. One live smoke run before the matrix.

## Revision 2026-09-04: language-agnostic candidates (approved)

Decision (user): do not rely on slug vocabularies. Describe the intent, give the
model the URL list, and let it return the useful URLs. Companies publish in
their local language and not every page has an English twin.

Changes to the design above:

- **Structural filter replaces vocabulary ranking.** From the sitemap
  inventory keep same-site HTTP(S) URLs without query strings, without file
  extensions, with path depth ≤ 3 and without numeric or date-like segments;
  dedupe by `url_key`; the base URL always stays. No slug vocabulary is used
  on the selection path.
- **Map-reduce above 200 URLs.** If the filtered list holds at most 200 URLs,
  all of them are candidates. Otherwise the URLs are sorted by path, split
  into chunks of about 400, and one triage call per chunk (URLs only, shared
  intent prompt) returns every URL that plausibly holds any requirement with
  the requirement keys it likely serves. The union is deduped; while it still
  exceeds 200 another round runs (at most three rounds; afterwards the URLs
  with the most requirement votes are kept). Heads (title, language) are
  fetched for the resulting pool only. Every triage pick is validated against
  its chunk.
- **The prompt variants under test apply to the final selection call only.**
  The triage prompt is one shared prompt, so all variants see the same pool.
- **Gold drafts come from the model.** Per site, the triage output (URL →
  requirement keys) plus one labelling call over the pool propose up to three
  URLs per gold field; the user reviews the proposal. The vocabulary drafter
  stays as `--deterministic` fallback.
- **Fallbacks.** If Codex is unavailable during triage, the vocabulary ranking
  caps the pool and the candidate set records the fallback.
- **Artifacts.** `candidates/<domain>.json` gains a `funnel` block (input,
  filtered, rounds with per-chunk token usage, pool size, fallback flag).
- **Cost.** Triage costs about 26k tokens per 400-URL chunk and only triggers
  on large sites (here tobii.com and vaisala.com, roughly 8–12 calls each);
  gold drafting adds one call per site. The matrix budget is unchanged.
- **Follow-ups outside this lab:** apply the same filter + map-reduce to the
  `ex3` production shortlist, and replace the regex gap check in pass two
  with an intent-based LLM judgement.
