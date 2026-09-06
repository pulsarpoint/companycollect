# Status: company-site extraction PoC (ex3) and page-selection lab (ex4)

Last updated: 2026-09-04. Living document; newest state first, then the backlog.

## Where things are

| Piece | Branch | State |
|---|---|---|
| ex3 LLM-guided passes (sitemap → LLM page selection → crawl → analyze → suggest → extend → merge) | `main` (merge `2c9b388e`) | Done, live-tested on handelsbanken.se |
| ex4 prompt lab v1 (sitemaps of 8 sites, prompts, gold sets, runner, scoring, report) | `main` (merge `45082661`) | Done; matrix never run |
| ex4 map-reduce (structural filter, intent-based triage, LLM gold drafting, live rebuild) | `ex4-map-reduce` (head `3c8be832`, NOT merged) | Code done and reviewed; paused at the gold-review checkpoint |
| Map-reduce plan | `main` (`d1c92a05`) | `docs/superpowers/plans/2026-09-04-page-selection-map-reduce.md` |

Note: `ex4-map-reduce` also contains a merge of `se-basic-info` (`01aec2b2`, dagster SE basic-info work) made on 2026-09-04. Merging the branch into `main` brings that work along; cherry-pick the nine `ex4` commits instead if that is not wanted.

## What is done

### ex3 (on main)

- Sitemap inventory with deterministic guardrails (external, binary, duplicates by `url_key`), a 200-candidate shortlist with `<head>` title/language and anchor text, one Codex call that picks `--max-pages` pages against the 15 target fields in `ex3/requirements.py`, BFS fallback when there is no sitemap.
- Analysis → `suggest` (deterministic gap check + one LLM call over unprocessed URLs) → `extend` → `analyze --previous-report --suggestions` (incremental + LLM merge). `research --max-passes N` orchestrates; pass two is configurable.
- Rulings: English is best effort, never mandatory; non-English pages are analyzed by default; beyond-limit LLM picks are recorded as warnings; the overwrite guard runs before any LLM call.
- Live: handelsbanken.se, 20 pages, ~197k tokens; 30 jobs, founded 1871, 12,000 employees, CEO and vice CEO, four subsidiaries. All 15 gaps closed in pass one, so pass two has only unit coverage.

### ex4 prompt lab v1 (on main)

- `ex4/` compares page-selection prompts against real sitemaps with gold sets: `sites verify`, `candidates build`, `gold draft`, `run`, `score`, `report`. Data in `experiments/page-selection/` (sites, candidates, gold, prompts p0–p3, runs, results).
- Eight verified sites: handelsbanken.se, tobii.com, pricer.com, polarbrod.se, kongsberg.com, vestas.com, vaisala.com, trumpf.com.

### ex4 map-reduce (branch `ex4-map-reduce`)

User ruling that drove it: do not search pages by name or English slug vocabulary; describe the intent, give the model the list, and let it return the useful URLs, because many sites have local-language-only URLs. Above 200 URLs, map-reduce.

- `ex4/filtering.py`: structural filter (invalid, external, query, file, depth > 3, numeric path segments, duplicates by `url_key`); base URL first, then sorted.
- `ex4/mapreduce.py`: chunks of 400 path-sorted URLs, concurrency 2, up to 3 rounds, 300 s per call; the model returns URLs with the requirement fields it expects; votes are validated against the chunk (unknown URLs dropped with warnings) and restricted to the 15 requirement keys; a soft per-chunk keep target (`ceil(threshold × chunk / round_input)`) keeps the pool near 200; a vote cut applies after the last round; `TriageUnavailable` only when every call in a round fails, with a recorded `rank_urls` fallback.
- `ex4/candidates.py`: seed → filter → pass-through or triage → heads → `PageCandidate(score = vote count, reasons = votes)`; `CandidateSet.funnel` records every round and call, including tokens.
- `ex4/gold.py`: one Codex call per site proposes up to 3 URLs per gold field (home, about, contact, management, careers, products_services, group_structure, legal_identity) plus junk, from the pool only; `--deterministic` keeps the old vocabulary draft.
- Tests: 151 on the branch, ruff/ty clean. Each task had a spec-and-quality review; minors are parked in the SDD ledger.

Live rebuild (committed as `3c8be832`):

| Site | Sitemap | After filter | Pool | Rounds | Triage tokens |
|---|---|---|---|---|---|
| handelsbanken.se | 1,099 | 300 | 200 | 1 | 34k |
| tobii.com | 4,930 | 2,394 | 171 | 1 | 146k |
| pricer.com | 2,135 | 1,986 | 200 | 2 | 198k |
| polarbrod.se | 629 | 626 | 200 | 2 | 94k |
| kongsberg.com | 2,878 | 336 | 200 | 1 | 36k |
| vestas.com | 1,863 | 120 | 120 | 0 | 0 |
| vaisala.com | 5,001 | 1,564 | 200 | 3 | 187k |
| trumpf.com | 1,222 | 675 | 200 | 2 | 99k |

Gold drafts: eight calls, ~261k tokens, zero validation warnings. Observations: tobii's fifth chunk timed out at 300 s, so ~400 tobii URLs were never triaged and its management/careers gold is empty; vestas' pool is small because the numeric rule dropped 1,710 dated news URLs and the depth rule dropped depth-4 product-family pages; handelsbanken's 171 junk entries are branch-locator pages (correct); pricer's group_structure and legal_identity point at press releases and the annual report. Before the keep target the pools collapsed to 19 (handelsbanken) and 120 (tobii); that run was aborted after ~300k tokens.

## What should be done

### Immediate (page-selection lab)

1. Review `experiments/page-selection/gold/*.json` and edit or approve them. Decide whether to re-triage tobii.com (~150k tokens) first.
2. Task 6 of the map-reduce plan: `ex4.main run` smoke → 32-call matrix → 16 stability calls (~1M tokens) → `score` → `report` → prompt recommendation. Split live runs per site or per prompt; background runs are stopped by the harness after ~35 minutes.
3. Final whole-branch review of `ex4-map-reduce` (most capable model), one fix dispatch for the parked minors (README "Example 4" paragraph still says the pool is the ex3 selector's 200 vocabulary-ranked list; `chunk_urls` docstring; `max_rounds <= 0` guard; a CLI test for the `llm failed:` echo; `heads.get` style), then the merge decision.

### Next design question: persistence of chosen links per domain

The user's direction (2026-09-04): before continuing, decide where the chosen links for a domain are stored so the next pass reuses them instead of re-running seeding, filtering and triage.

What exists today: everything is per-run files (`crawl-manifest.json`, `ResearchReport`, `experiments/page-selection/candidates/<domain>.json`). The platform keeps domain-level facts in ClickHouse `corpscout.commoncrawl_domain_*` tables (`ReplacingMergeTree(resolved_at)`, `ORDER BY (root_domain, crawl_id)`), and the backoffice review queue in PostgreSQL. Nothing stores a per-domain page inventory or selection.

Open decisions (not made): whether the store is a PoC-local cache (SQLite or JSON keyed by `canonical_domain`) or a platform table in ClickHouse; the record shape (canonical domain, page URL by `url_key`, source sitemap or BFS, filter reason, triage votes, selected flag, prompt hash, model, run id, seen/selected timestamps); invalidation (sitemap `lastmod`, age, a failed crawl of a stored URL); how the second pass and `suggest` consume it; and whether analysis results are cached per page as well. Initial leaning: a domain-level ClickHouse table in the `commoncrawl_domain_*` style with one row per (root_domain, url_key) and a companion per-run selection row, because the eventual consumer is the field-candidate pipeline keyed by org number. Brainstorm before building.

### Follow-ups (noted, not scheduled)

- Apply filter + map-reduce to the ex3 production shortlist (it is still vocabulary-ranked).
- Replace the regex gap check in `ex3/requirements.py` with intent-based LLM judgement.
- Per-chunk retry or a smaller chunk on triage timeout; reconsider the depth-3 rule for product pages.
- Fix `<html lang>` detection (crawl4ai's lxml head parser never reads it; fall back to `og:locale` / `content-language` in `ex3/seeding.py`), then rebuild candidates.
- Split `ex3/crawler.py` (~1,750 lines); run pass two live on a thinner site; crawl4ai render failures under concurrency 5.
- Earlier ideas: HTML/JSON-LD sidecars, deterministic fact extraction (phones, org numbers, LEI), boilerplate stripping, typed schema, concurrent batches, eval fixtures.

## How to run

```bash
cd codex-sd-examples
.venv/bin/python -m unittest discover
uvx ruff check --fix ex3 ex4 tests && uvx ruff format ex3 ex4 tests && uvx ty check ex3 ex4 tests

# ex4 lab (branch ex4-map-reduce)
.venv/bin/python -m ex4.main candidates build --sites trumpf.com --overwrite   # filter → triage → heads
.venv/bin/python -m ex4.main gold draft --sites trumpf.com --overwrite         # LLM proposal; --deterministic for the old draft
.venv/bin/python -m ex4.main run --help
```

Gotchas: run tests with `unittest`, never pytest; crawl4ai logs "Task was destroyed but it is pending" at the 5,000-URL seeding cap, which is benign; a triage chunk that times out is recorded in `funnel.rounds[*].calls` but not retried.

## Pointers

- Specs: `docs/superpowers/specs/2026-09-02-ex3-llm-guided-passes.md`, `docs/superpowers/specs/2026-09-03-page-selection-prompt-lab-design.md` (with the 2026-09-04 language-agnostic revision).
- Plans: `docs/superpowers/plans/2026-09-02-ex3-llm-guided-passes.md`, `docs/superpowers/plans/2026-09-03-page-selection-prompt-lab.md`, `docs/superpowers/plans/2026-09-04-page-selection-map-reduce.md`.
- Execution ledgers (git-ignored, local): `.superpowers/sdd/2026-09-04-page-selection-map-reduce/progress.md`, `.superpowers/sdd/2026-09-03-page-selection-prompt-lab/progress.md`.
