# Translator redesign: split BuildQueue / Translate, per-source packages, bulk seed, heartbeat liveness

**Date:** 2026-06-27
**Status:** Design approved, pending implementation plan
**Area:** `corpscout/dagster_v3/translator/` (+ the `norway_brreg` Dagster trigger asset)

## Goal

Split the monolithic `TranslateSourceWorkflow` into two independent Temporal workflows — one that
**builds the translation queue** (bulk ClickHouse → DuckDB) and one that **translates** (drains the
queue via the LLM) — organize the translator as **per-source packages** with a shared core, replace the
Python row-by-row seed with a **bulk Arrow load**, and replace fixed activity duration timeouts with
**heartbeat-based liveness**.

## Motivation (problems with the current design)

1. **One workflow does two jobs.** `TranslateSourceWorkflow` does data movement (`scan_and_seed`) *and*
   the LLM loop *and* the flush. These have different failure modes, throughput, and scaling; coupling
   them is wrong.
2. **The seed is Python row-by-row.** `scan_untranslated_terms` pulls every untranslated row out of
   ClickHouse into Python, and `enqueue_items` computes a `sha256` per item in Python and `executemany`s
   into DuckDB. For ~377K terms this takes many minutes — and violates this repo's own rule (never load
   bulk row-by-row in Python; use DuckDB/Arrow). The ClickHouse → DuckDB transfer should be set-based.
3. **Fixed activity timeouts are the wrong knob.** `scan_timeout_seconds=300` killed a legitimate long
   seed with `CancelledError`. A duration guess can't be right across source sizes. Liveness should be
   proven by heartbeats, not bounded by a guessed wall-clock.
4. **No clear "done" signal / no observability** were symptoms of the same tangle (now partly addressed
   by logging; fully resolved here: "done" = the Translate workflow reaches `COMPLETED`).

## Decisions (settled)

- **Per-source packages.** Each translated source gets its own folder under `translator/`
  (e.g. `translator/norway_brreg/`) with its two workflows, its seed, its dump, and its config.
- **Two workflows per source:** `BuildQueueWorkflow` and `TranslateWorkflow`.
- **Activation:** `BuildQueueWorkflow`'s final step starts `TranslateWorkflow` as a **separate top-level
  workflow** (`start_workflow`, fixed id `translate-<source>`, `WorkflowIDConflictPolicy.USE_EXISTING`),
  then BuildQueue completes. Atomic handoff inside Temporal; no external poller.
- **Seed = Arrow + set-based SQL.** `clickhouse-connect` `query_arrow()` for the per-field anti-join →
  register the Arrow table in DuckDB → one `INSERT … SELECT` into the queue tables, with the item hash
  computed in **DuckDB SQL** (`sha256`). No per-row Python.
- **Dump = fully self-contained per source** (`norway_brreg/dump.py` owns the whole queue → ClickHouse
  write; no shared flush core).
- **Shared LLM function.** `translator/llm_batch.py` holds the common "translate N entries via the LLM"
  function used by every source's `TranslateWorkflow`.
- **Liveness = heartbeat only.** Long activities call `activity.heartbeat()` (~every 30s / N items);
  `heartbeat_timeout ≈ 150s` (≈5 missed → fail). `start_to_close_timeout` is set to a loose 24h backstop
  (Temporal requires *a* value; it should never realistically fire). The `scan_timeout`/`flush_timeout`
  knobs are removed.
- **Done** = `TranslateWorkflow` reaches `COMPLETED` (queue drained + dumped); visible in the Temporal UI
  with its output counts, and reflected by `corpscout.no_companies_translated._en` coverage.

## Package structure

```
translator/
  queue.py        # shared: DuckDB queue schema + status ops + a bulk-insert (Arrow→queue) helper
  llm_batch.py    # shared: translate-N-entries-via-LLM (the common function)
  smoke.py        # shared: OpenAI-compatible LLM provider (unchanged)
  clickhouse.py   # shared: CH client, query_arrow helper, cityHash64/sha helpers
  types.py        # shared dataclasses (ScannedTerm/FlushTranslationRow/etc.)
  worker.py       # shared: registers EVERY source's BuildQueue + Translate workflows + activities
  norway_brreg/
    __init__.py
    config.py     # SourceConfig: ch_table, source_lang, fields (column + static_map + static_key_col)
    seed.py       # ClickHouse → DuckDB queue (Arrow + set-based SQL); static resolution
    dump.py       # queue → corpscout.text_translations (self-contained, batched)
    workflows.py  # BuildQueueWorkflow + TranslateWorkflow (+ their activities)
```

A second source later adds a sibling folder (`translator/finland_ytj/…`) and registers its two workflows
in `worker.py`. Nothing source-specific lives in the shared core.

## Components

- **`norway_brreg/config.py`** — the source's translation config: `ch_table` (`corpscout.no_companies`),
  `source_lang` (`no`), and the field list (each: `original_col`, optional `static_map` + `static_key_col`).
  Replaces the old global `registry.py` entry.
- **`norway_brreg/seed.py`** — `build_queue(...)`:
  1. For each **dynamic** field: `query_arrow()` the `LEFT ANTI JOIN` scan → Arrow table of distinct
     untranslated `source_text`.
  2. Register each Arrow table in the per-source DuckDB queue connection and `INSERT … SELECT` into
     `translation_items` / `translation_locations`, computing `item_id = sha256(source_text || lang)` in
     SQL. Heartbeat between fields / row-chunks.
  3. For **static** fields: resolve via the dict (tiny, ~40 values) and write them straight to
     `text_translations` (`provider='static'`) — they never enter the LLM queue.
- **`translator/llm_batch.py`** — `translate_batch(items, *, provider, model, max_tokens, extra_body,
  timeout) -> results`: the shared LLM call (JSON-in/JSON-out, temperature 0), returning per-item
  translations. Used by every source's Translate loop.
- **`norway_brreg/dump.py`** — `dump_to_clickhouse(...)`: reads completed queue results and writes them
  to `corpscout.text_translations` (`source_table`/`source_column`, `cityHash64` in SQL), **batched**
  (chunked staging inserts like the import CLI). Self-contained.
- **`norway_brreg/workflows.py`**
  - `BuildQueueWorkflow`: one heartbeating activity wrapping `seed.build_queue`; final activity calls
    `start_workflow(TranslateWorkflow, id="translate-norway_brreg", USE_EXISTING)`.
  - `TranslateWorkflow`: loop { claim batch from the queue → `llm_batch.translate_batch` → mark completed,
    heartbeat }, tolerating up to `max_batch_failures` failed batches; then `dump.dump_to_clickhouse`;
    then summarize. Returns counts.

## Data flow

```
Dagster trigger (fire-and-forget)
  → BuildQueueWorkflow(norway_brreg)
       scan (LEFT ANTI JOIN, per field) ──query_arrow──► DuckDB queue (set-based INSERT, sha256 in SQL)
       static fields ─► text_translations (provider='static')
       (heartbeat throughout)
       └─ final step: start_workflow(TranslateWorkflow, USE_EXISTING) ─► COMPLETED
  → TranslateWorkflow(norway_brreg)
       loop: claim batch ─► llm_batch.translate_batch ─► mark completed (heartbeat per batch)
       dump_to_clickhouse (batched) ─► text_translations (provider=model)
       └─ COMPLETED  ◄── "translation is done"
no_companies_translated view joins text_translations → *_en
```

## Liveness / heartbeat

- Every long activity (`seed`, `translate_batch` loop, `dump`) calls `activity.heartbeat(progress)` at a
  bounded cadence (~30s or every N rows/items).
- Activity options: `heartbeat_timeout ≈ 150s` (≈5 missed heartbeats → Temporal fails/retries),
  `start_to_close_timeout = 24h` backstop, retry policy `maximum_attempts` small (e.g. 3).
- No `scan_timeout_seconds` / `flush_timeout_seconds` in the workflow input — removed.

## Error handling

- LLM batch failures → items marked `failed_retryable` (as today); the Translate loop tolerates up to
  `max_batch_failures` then completes (leftovers retried on the next run). `llm_batch` categorizes errors.
- A seed failure fails `BuildQueueWorkflow` (and therefore does not start Translate); retried via the
  Dagster trigger. Heartbeat death (worker crash) → Temporal retries the activity.
- Refuse-to-replace / empty guards as per the repo standard where applicable.

## Reused unchanged

- `corpscout.text_translations` cache + `corpscout.no_companies_translated` view + all migrations
  (000056, 000059–000070). Keying stays `(source_table, source_column, source_text_hash)`.
- The OpenAI-compatible LLM provider (`smoke.py`).
- The DuckDB queue schema (`translation_items` / `translation_locations` / `translation_results` /
  `translation_batch_attempts`) — but populated by a bulk Arrow insert instead of Python `executemany`.

## Replaced / removed

- `translator/workflow.py` (`TranslateSourceWorkflow`, combined) and `translator/activities.py` (the
  combined activities) are refactored into the per-source `workflows.py` + the shared core.
- `translator/registry.py` → per-source `config.py`.
- `scan_timeout_seconds` / `flush_timeout_seconds` workflow inputs removed.
- The Norway Dagster trigger asset now fires `BuildQueueWorkflow` (id `build-queue-norway_brreg`) instead
  of the combined workflow.

## Queue location & cutover

- Queue stays at `data/translator/<source_slug>.duckdb` (Norway: `data/translator/norway_brreg.duckdb`).
- On cutover, **delete the existing `data/translator/norway_brreg.duckdb` + `.wal`** so the new bulk seed
  builds a fresh queue (the current file holds a half-seeded state from the old workflow).
- The legacy `data/norway_brreg_translation_queue.duckdb` (scheduler era) is unrelated and can be deleted
  later (its translations already live in `text_translations` as `legacy-import`).

## Testing

- **Unit:** `seed` (Arrow → queue, `sha256` in SQL, static resolution); `dump` (batched write,
  `source_table`/`source_column`, empty-skip); `llm_batch` (provider call + result mapping, error
  categorization); `config` (fields, static maps).
- **Workflow (Temporal test env):** BuildQueue seeds + starts Translate (assert the `start_workflow`
  handoff with `USE_EXISTING`); Translate drains a seeded queue + dumps; heartbeat is emitted; failure
  tolerance honored.
- **Integration (real ClickHouse + DuckDB), gated by an env flag:** the seed's anti-join + Arrow load
  actually returns untranslated terms and populates the queue — the class of test that would have caught
  the `join_use_nulls` anti-join bug (text-only unit tests cannot).
- `uv run dg check defs` + full suite green.

## Non-goals / follow-ups

- Not migrating other sources now — only Norway gets the per-source package; the shared core is built so a
  second source is a small addition.
- Resumable seed via heartbeat `details` (record progress so a retry resumes mid-seed) — optional later.
- Moving the queue out of DuckDB into ClickHouse — out of scope; the bulk Arrow seed addresses the speed.
