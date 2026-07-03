# Translator Loader/Service Split — Design

**Date:** 2026-07-03
**Status:** Approved design, pending implementation plan
**Scope:** `corpscout/translator` (Go service) + `corpscout/dagster_v3` (Python loaders)
**Supersedes:** the definition-driven scan engine from
`2026-07-03-translator-generic-engine-design.md` (merged earlier the same day).
The queue, LLM provider, batch-recovery ladder, and `text_translations` uploader
from that work are retained; the per-source scan/definition machinery is removed.

## Problem

The merged engine couples extraction to translation: the translator owns
per-source definitions, generates scan SQL, and runs a signal-driven action
state machine (`load-and-run` / `run` / `load-queue`) precisely because loading
and translating live in one workflow. Extraction is a data-pipeline concern
that belongs with the rest of the (Python/dagster) pipelines; entangling it
makes the translator harder to reason about, puts source knowledge in the
wrong repo, and leaves translated output sitting in the local queue until the
queue fully drains.

## Decision summary

1. **One shared queue** — single DuckDB file, single Temporal workflow. The
   translator has zero per-source configuration; sources exist only as loader
   scripts.
2. **Bulk HTTP enqueue with auto-start** — loaders POST item batches; a
   successful enqueue signal-with-starts the processing workflow.
3. **Temporal stays** — one simplified workflow (loop, flush, continue-as-new);
   the action state machine is deleted.
4. **Loaders are dagster assets** in `corpscout/dagster_v3`, one per source.
   The Norway loader ships with this project and the old scan path is deleted
   (no fallback).

## Architecture

```
dagster asset (per source, Python)
  ├── anti-join scan in ClickHouse (distinct untranslated texts, cityHash64 in SQL)
  ├── static-map columns → INSERT directly into corpscout.text_translations (provider='static')
  └── LLM columns → chunked POST /v1/queue/items ──► translator API (Go)
                                                        │ upsert into shared DuckDB queue
                                                        │ signal-with-start workflow
                                                        ▼
                                          TranslationWorkflow (Temporal, one instance)
                                            loop: GetBatch (one lang pair) → LLM → output/failed
                                            flush every N batches AND at queue-empty:
                                              insert corpscout.text_translations,
                                              delete flushed output+input rows
```

## Component 1: Enqueue API (translator, Go)

### `POST /v1/queue/items`

Language pair and human-readable language names are declared once per request
(a loader naturally loads one pair); items carry only per-text fields:

```json
{
  "source_lang": "no",
  "target_lang": "en",
  "source_language_name": "Norwegian",
  "target_language_name": "English",
  "items": [
    {
      "source_table": "corpscout.no_companies",
      "source_column": "activity_text_original",
      "source_text": "Utvikling av programvare",
      "source_text_hash": "1234567890123456789"
    }
  ]
}
```

Rules:

- `source_text_hash` is a **decimal string** (uint64 exceeds JSON's safe
  integer range). Loaders compute it in ClickHouse SQL (`cityHash64(col)`) so
  hashes always agree with the loader's anti-join; the translator parses and
  stores it as uint64 and never recomputes it.
- Validation (reject the whole request with 400 and a per-field error):
  request-level fields non-empty; ≤ 10,000 items per request; every item's
  four fields non-empty; hash parses as uint64.
- Behavior: upsert into `input_items` keyed by
  `(source_table, source_column, source_text_hash, source_lang, target_lang)`
  — `ON CONFLICT DO NOTHING`, same as today. Re-running a loader is free.
- Response `202`: `{"received": N, "inserted": M, "workflow_id": "...", "run_id": "..."}`
  (`inserted` < `received` means duplicates were skipped — normal).
- After a successful upsert of ≥ 1 item, the handler signal-with-starts the
  processing workflow. Enqueue succeeds even if the signal fails (queue is
  durable; boot-resume and the manual kick cover it) — the response then
  carries a warning field instead of workflow IDs.

### `GET /v1/queue/stats`

`200`: `{"input": n, "pending": n, "output": n, "failed": n}` — same counting
queries the runtime uses today. Dagster asset checks and humans use this.

### `POST /v1/queue/process`

Manual kick: signal-with-start the workflow with no enqueue. Replaces the
per-source action endpoints. The trigger CLI shrinks to this single action.

### Removed endpoints

`POST /v1/sources/{source}/{action}` is deleted (sources no longer exist in
the translator). `/healthz` unchanged.

## Component 2: Queue (translator, Go)

Same three tables, **new file** (default `data/translator/queue.duckdb`,
configurable). `input_items` gains two columns:

```
source_language_name text not null
target_language_name text not null
```

Primary keys unchanged. The old per-source file
(`data/translator/norway_brreg.duckdb`) is abandoned, not migrated: anything
already uploaded is in `text_translations`; anything untranslated is
re-enqueued exactly by the loader's first anti-join run.

Queue lifecycle change: after a successful flush (see workflow), the flushed
`output_items` rows **and their matching `input_items` rows are deleted**. The
queue is transient working state; `text_translations` is the source of truth
that keeps loaders from re-enqueueing done work. `failed_items` is never
auto-deleted (operator inspects and clears).

## Component 3: Processing workflow (translator, Go + Temporal)

One workflow, fixed identity: workflow ID `translator/process`, task queue
`translator-process`, workflow type `TranslationWorkflow` (rewritten body).
No per-source identities remain.

Behavior:

1. Started (or signal-with-started) with input
   `{BatchSize, TimeoutSeconds, BatchesPerRun, FlushEveryBatches}` — all
   defaulted from config (50 / 120 / 500 / 10).
2. Loop up to `BatchesPerRun` times:
   - `ProcessOneBatch` activity: `GetBatch` selects up to `BatchSize` pending
     items **from a single language pair** (pick the pair with the oldest
     pending item, then batch within it) — prompts require homogeneous
     batches. Translate via provider, save output/failed. Returns counts.
   - Every `FlushEveryBatches` batches, and whenever `PendingCount == 0`:
     `FlushOutput` activity — read `output_items`, batched insert into
     `text_translations` (existing inserter), then delete flushed output +
     input rows in DuckDB. Returns rows flushed.
   - `PendingCount == 0` after flush → workflow completes.
3. Batch budget exhausted → continue-as-new (same input).
4. New enqueue signals during a run are absorbed by the loop re-checking
   pending counts; signals to a completed workflow start a fresh run
   (signal-with-start semantics).

Crash window note: a crash between the ClickHouse insert and the DuckDB
delete re-flushes the same rows on resume, producing duplicate
`text_translations` rows with identical content. Readers already pick the
latest version per key; this is the same idempotent-read property the current
system has. Documented, accepted.

Boot resume: `translator-api` main, after starting the worker, checks queue
pending count and signal-with-starts the workflow if > 0. A restart never
strands a half-full queue.

## Component 4: Provider change (translator, Go)

Prompt language names move from provider construction to per-call:

```go
type PromptData struct { SourceLanguage, TargetLanguage string } // exists today

Translate(ctx context.Context, items []TranslationInput, timeoutSeconds int,
          promptData PromptData) ([]TranslationResult, error)
```

`translation.Config.PromptData` is removed; one provider instance serves all
language pairs. `TranslateItems` (batch recovery: retry-shuffled → split →
quarantine single item to `failed_items`) threads `promptData` through
unchanged. Batch homogeneity is guaranteed by `GetBatch`; `promptData` comes
from the batch's queue columns.

## Component 5: Loaders (dagster_v3, Python)

New asset group (e.g. `translator_load`) in `corpscout/dagster_v3`. One asset
per source, downstream of that source's ingest assets. The Norway asset ships
with this project:

- **LLM columns** (`articles_purpose_original`, `activity_text_original` on
  `corpscout.no_companies`): run the canonical anti-join scan (the exact SQL
  shape the deleted Go templates generated — distinct text + `cityHash64` +
  LEFT ANTI JOIN `text_translations` on hash filtered by table/column, text
  `<> ''`), chunk ≤ 10k, POST to `/v1/queue/items` with `no`/`en` +
  `Norwegian`/`English`.
- **Static column** (`legal_form_description_original` keyed by
  `legal_form_code`): anti-join scan selecting text + hash + code, join the
  40-entry code→English map in Python (moved verbatim from
  `config/sources/norway_brreg.json`, which is deleted), insert directly into
  `text_translations` with `provider='static'`, `model='static'`,
  `version=unix_now`. Unknown codes are skipped (same as today).
- Config via env: `TRANSLATOR_API_URL`; ClickHouse connection reuses
  dagster_v3's existing resources. Asset fails loudly on any non-2xx enqueue
  response; asset check asserts `/v1/queue/stats` reachable and reports counts.
- The anti-join + hash-in-SQL + chunked-POST pattern is documented once (in
  the asset group's module docstring and the translator README) as the
  template for future countries.

Trust boundary (carried over from the previous spec): loader-authored
table/column names flow into loader SQL and into `text_translations` metadata;
loaders are trusted developer-authored code, same as before.

## Deletions and migration

Deleted from the translator:

- `internal/engine/definition.go`, `scan.go` (+ their tests, including golden
  SQL tests — the canonical anti-join SQL moves into this spec and the loader
  docs, since it no longer lives in Go).
- The scan/static half of `internal/engine/load.go` (`LoadInput`,
  `loadInputWithDB`, `flushStaticColumn`, `QueryStaticInput`, `StaticInput`);
  queue DDL, `upsertInputItems` (reused by the API), `countRows` stay.
- `LoadNewInput` activity, the load/run/load-queue action machinery, resume
  actions, per-source activity-name/workflow-ID/task-queue helpers, per-source
  workers.
- `config/sources/` directory, `SourceConfig.DefinitionPath`, the `sources`
  map (replaced by a single `queue` config block: path, flush cadence),
  per-source Makefile SOURCE variable and script SOURCE parameterization
  (script now triggers `translator/process`).
- README sections describing per-source workflows; replaced by API contract +
  loader pattern docs.

Temporal migration (one-time, deploy day): terminate
`translator/norway_brreg` (already required by the previous cutover if not yet
done). The old identity and the `translator-norway-brreg` task queue are
retired; nothing re-creates them.

Kept intact: DuckDB queue tables/upsert, ClickHouse `text_translations`
inserter and batching, the LLM provider HTTP client and response validation,
`TranslateItems` recovery ladder, `failed_items` isolation, router/server
skeleton, trigger CLI (reduced), Temporal worker wiring.

## Error handling

- Enqueue: full-request validation before any write; structured 400s.
- Workflow activities keep the existing retry policy (10 attempts, 10 min
  start-to-close).
- Model-output failures: unchanged ladder; poisoned items land in
  `failed_items` and are visible via `/v1/queue/stats`.
- Flush failure: activity retry; queue rows are only deleted after a
  successful insert.
- Loader failure: dagster surfaces it; re-running the asset is always safe
  (anti-join + upsert).

## Testing

- **Go unit:** enqueue validation matrix and hash-string round-trip (max
  uint64); upsert dedup; `GetBatch` single-language-pair grouping (mixed-pair
  queue yields homogeneous batches); flush deletes exactly the flushed rows;
  workflow loop/flush-cadence/continue-as-new via Temporal test suite;
  provider per-call promptData.
- **Go integration (real ClickHouse + DuckDB, existing env-guarded pattern):**
  enqueue → process (fake translator) → flush lands rows in
  `text_translations`; re-enqueue of flushed items is prevented only by the
  loader anti-join (verify a re-POST does re-insert into the queue — this
  boundary is the loader's job, and the test documents that).
- **Python (real ClickHouse, stub HTTP translator):** Norway loader produces
  the expected item set for a seeded table, chunks correctly, static rows land
  in `text_translations` with `provider='static'`.
- **End-to-end smoke** on the lab stack: dagster asset run → API →
  workflow → translated rows in ClickHouse.

## Out of scope

- Multiple LLM endpoints / per-language-pair endpoint routing (one endpoint
  serves all pairs, as today).
- Authentication on the translator API (internal service, unchanged).
- Backfilling or rewriting existing `text_translations` rows.
- Retry/requeue tooling for `failed_items` (operator clears manually).
- Watermark-based incremental scans in loaders (anti-join already bounds
  cost; revisit if a source table gets huge).
