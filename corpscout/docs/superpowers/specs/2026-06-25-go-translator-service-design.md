# Go Translator Service — Design Spec

**Date:** 2026-06-25
**Status:** Approved (brainstorming) — pending implementation plan
**Topic:** Extract company-data translation out of Dagster into a standalone Go service

---

## Problem

Translation of company free-text fields (`*_original` → `*_en`) currently runs **inside Dagster** via a
Python Temporal workflow (`corpscout/dagster_v3/temporal/translations/` + the
`norway_brreg_translation_*` assets/sensor/job). This coupling is the source of recurring operational pain:

- The translation workflow is gated by a Dagster **completion sensor**. When the Temporal workflow fails
  mid-run (observed: `status=FAILED` at 64.8%, 551 failed batches), the sensor skips forever and downstream
  assets never run.
- The translation queue asset holds a **single-writer DuckDB pool slot** (`norway_brreg_duckdb`). When a run
  crashes ungracefully, Dagster leaks the pool slot and **every later Norway run blocks indefinitely**
  (diagnosed 2026-06-25; required `dagster-health-check.py --fix` to free the slot).
- Translations live in the per-source DuckDB and are re-exported to ClickHouse. Because the ClickHouse export
  **wipes-and-replaces** the table on every refresh, the design re-couples translation to ingestion timing.

The broader direction: ingestion has **moved to Dagster**; the legacy Go `scheduler` service is being removed.
Translation is the one piece we want to pull *out* of Dagster into a dedicated service so its failures can no
longer block ingestion.

## Goal

A standalone Go service, **`corpscout/translator`**, that owns translation end-to-end: scan ClickHouse for
untranslated text, translate it via an LLM behind a durable Temporal workflow, and write results to a dedicated
ClickHouse table — fully decoupled from Dagster's asset graph.

## Non-Goals

- Translating sources other than `norway_brreg` in this iteration. The engine is **generic/config-driven**, but
  only Norway's columns are registered initially (adding a source later = a config entry, no code change).
- Replacing the LLM provider or prompt strategy — we salvage the existing OpenAI-compatible approach.
- Self-scheduling. Translation is **triggered by Dagster** after its ClickHouse export (not a Temporal cron).
- Migrating the NATS-based translation path (`scheduler/translationclient`) — it dies with `scheduler`.

---

## Key Decisions (resolved during brainstorming)

| # | Decision | Choice |
|---|----------|--------|
| 1 | Build approach | Reuse proven logic, SQLite worksets (not DuckDB) |
| 2 | Scope | Generic config-driven engine, **Norway-only config** initially |
| 3 | Where the code lives | **New standalone service** `corpscout/translator` (NOT in `scheduler`, which is being deleted; salvage its code first) |
| 4 | Trigger | **Dagster triggers** the workflow after its ClickHouse export |
| 5 | Wait semantics | **Fire-and-forget** — Dagster does not gate on completion |
| 6 | Write-back model | **Separate `company_translations` table + join view**; survives Dagster's wipe-and-replace |
| 7 | Dedup granularity | **Term-level**, hash-keyed — translate each distinct string once |
| 8 | Service name | `corpscout/translator` |
| 9 | `_en` exposure | **New join-view**; drop `_en` columns from the base companies table |

**Salvage, don't depend:** `scheduler` is being removed. Before deletion, copy/adapt its proven building blocks
into `corpscout/translator`:
- `scheduler/internal/sourcetranslation/` (SQLite workset: build → claim → save → apply, dedup, leasing, retry)
- `scheduler/internal/llmproviders/` (OpenAI-compatible LLM client + provider store)
- `scheduler/internal/clickhouse/` (native reader/writer)
- `scheduler/internal/app/temporal.go` (Temporal client/worker bootstrap pattern)

---

## Architecture

```
Dagster (corpscout/dagster_v3)                         corpscout/translator (NEW Go service)
─────────────────────────────────                      ──────────────────────────────────────
ingest norway_brreg                                     cmd/worker  ── Temporal worker (task queue:
   │                                                                    "translator")
   ▼
export corpscout.companies   (NO _en columns)
   │
   ▼
fire Temporal StartWorkflow ───────────────────────────►  TranslateSourceWorkflow{source_slug}
   (fire-and-forget;                                          id = "translate-<source_slug>"
    workflow-id dedup)                                        (WorkflowIDConflictPolicy.USE_EXISTING)
   │                                                          │
   ▼                                                          ▼
asset completes immediately                              1. scan: distinct untranslated terms from ClickHouse
                                                         2. seed SQLite workset (term-level, deduped)
                                                         3. loop: claim batch → LLM translate → save batch
                                                         4. flush results → corpscout.company_translations
                                                              │
queries read view  ◄──────────────────────────────────────────┘
corpscout.norway_companies_translated
   = companies LEFT JOIN company_translations (by source_text_hash per field)
```

Next refresh re-exports `companies` (wiping it) but **not** `company_translations`; the scan then finds only
new/changed terms → minimal re-translation.

---

## Components (`corpscout/translator/`)

```
corpscout/translator/
├── cmd/worker/main.go        # Temporal worker entrypoint — registers workflow + activities, runs forever
├── internal/registry/        # Config: per source_slug → {source_lang, table, [(original_col, en_col, field)]}
│                             #   Norway-only initially; pure data, no per-source code.
├── internal/scan/            # ClickHouse SELECT of distinct untranslated terms (LEFT JOIN company_translations)
├── internal/workset/         # SQLite queue (salvaged from scheduler/sourcetranslation)
├── internal/llm/             # OpenAI-compatible translation client (salvaged from scheduler/llmproviders)
├── internal/chwrite/         # ClickHouse insert into company_translations
├── internal/temporal/        # TranslateSourceWorkflow + activities (scan/seed/translate-batch/flush)
├── go.mod                    # NEW module; deps: go.temporal.io/sdk, clickhouse-go/v2, modernc.org/sqlite
└── Dockerfile                # + a `translator` service entry in corpscout/docker-compose.yml
```

### Component responsibilities

- **registry** — answers "for `source_slug`, what is the source language, which ClickHouse table, and which
  `(original_col → en_col)` field pairs need translating?" One entry for `norway_brreg`:
  `(articles_purpose, activity_text, company_description)` and the reference-style fields as applicable,
  `source_lang = "no"`.
- **scan** — for each field, `SELECT DISTINCT <original_col>` from the source table where the original is
  non-empty **and** no matching row exists in `company_translations` (LEFT JOIN on `cityHash64(normalized
  original)`). Returns distinct terms (text + hash + field).
- **workset** — SQLite-backed durable queue. Term-level: one row per distinct `(field, source_text_hash)`.
  Supports build/enqueue, atomic `claim_batch` (leasing with worker id), `save_batch` (results +
  success/`failed_retryable` status), retry. One workset DB file per workflow run, under the service's
  worksets volume.
- **llm** — OpenAI-compatible chat client (temperature 0, JSON-in/JSON-out prompt, configurable max tokens and
  `extra_body`). Translates a batch of terms `source_lang → en`.
- **chwrite** — bulk-insert deduped results into `company_translations` (ReplacingMergeTree; idempotent by
  ORDER BY key + version).
- **temporal** — `TranslateSourceWorkflow(source_slug)`:
  1. `ScanActivity` → distinct untranslated terms.
  2. `SeedActivity` → build the SQLite workset.
  3. loop: `ClaimBatchActivity` → `TranslateBatchActivity` (LLM) → `SaveBatchActivity`, until the queue drains.
  4. `FlushActivity` → write completed results to `company_translations`.
  Resilient: a failed batch leaves its items `failed_retryable` and the workflow **completes** after draining
  what it can — it never hard-fails the whole run on batch failures.

---

## Data Model

### `corpscout.company_translations` (new ClickHouse table)

Term-level, hash-keyed. Identical source strings across many companies are translated once.

```sql
CREATE TABLE IF NOT EXISTS corpscout.company_translations
(
    source_slug       LowCardinality(String),   -- 'norway_brreg'
    field             LowCardinality(String),   -- 'company_description'
    source_text_hash  UInt64,                    -- cityHash64(normalized original text)
    source_lang       LowCardinality(String),   -- 'no'
    target_lang       LowCardinality(String),   -- 'en'
    translated_text   String,
    provider          LowCardinality(String),
    model             LowCardinality(String),
    version           UInt64                     -- translated_at epoch seconds (ReplacingMergeTree version)
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (source_slug, field, source_text_hash);   -- all non-nullable (allow_nullable_key is off)
```

- **Hashing:** `cityHash64` over the *normalized* original text. Normalization (trim/collapse whitespace) must
  be **byte-identical** between the Go scanner (when it computes the hash to store) and the ClickHouse view
  (when it computes the hash to join). This is a contract; it gets a dedicated test.
- **Dagster never touches this table.** It survives wipe-and-replace of `companies`.
- Optional `target_lang` retained for future multi-language; `'en'` only for now.

### View: `corpscout.norway_companies_translated`

Exposes `_en` columns by joining the base table to `company_translations` per field. The base `companies`
table **no longer carries `_en` columns** (Decision 9). Consumers point at the view.

```sql
CREATE VIEW corpscout.norway_companies_translated AS
SELECT
    c.*,
    t_desc.translated_text   AS company_description_en,
    t_act.translated_text    AS activity_text_en,
    t_pur.translated_text    AS articles_purpose_en
    -- ...one LEFT JOIN per translated field...
FROM corpscout.companies AS c
LEFT JOIN corpscout.company_translations AS t_desc
       ON t_desc.source_slug = 'norway_brreg' AND t_desc.field = 'company_description'
      AND t_desc.source_text_hash = cityHash64(<normalize>(c.company_description_original))
-- ... t_act, t_pur similarly ...
;
```

(Exact `FINAL`/dedup handling on the ReplacingMergeTree join to be settled in the plan — likely
`argMax(translated_text, version)` in a small subquery to avoid `FINAL` cost.)

---

## Data Flow (end to end)

1. Dagster ingests `norway_brreg`, exports `corpscout.companies` **with no `_en` columns**.
2. Dagster's final step fires `StartWorkflow(TranslateSourceWorkflow, source_slug="norway_brreg")` with
   `id="translate-norway_brreg"` and `WorkflowIDConflictPolicy.USE_EXISTING`, then the asset completes
   (fire-and-forget).
3. The `translator` worker runs the workflow: scan → seed workset → claim/translate/save loop → flush to
   `company_translations`.
4. Queries read `corpscout.norway_companies_translated`; `_en` is filled for translated terms, empty otherwise.
5. On the next refresh, `companies` is wiped and re-exported; `company_translations` persists, so the scan
   only enqueues genuinely new/changed terms.

---

## Error Handling

- **Fire-and-forget eliminates the original failure class:** no Dagster completion sensor, no gating, and no
  DuckDB pool slot held by translation → the leaked-slot/stuck-QUEUED failure debugged on 2026-06-25 cannot
  recur from translation.
- **Batch failure isolation:** a failed LLM batch leaves its items `failed_retryable`; the workflow drains the
  rest and **completes**. Leftovers are retried on the next trigger (or a future Temporal continue-as-new).
- **LLM/transport errors:** Temporal activity retry with backoff; persistent failure leaves items retryable,
  not lost.
- **Duplicate triggers:** Temporal workflow-id dedup (`USE_EXISTING`) — at most one in-flight run per source.
- **Empty scan:** workflow no-ops fast.
- **Refuse to corrupt:** `chwrite` only inserts rows with non-empty `translated_text`; never writes a row that
  would shadow a good translation with an empty one.

---

## Changes to Dagster (`corpscout/dagster_v3`)

**Remove:**
- Assets `norway_brreg_translation_queue`, `norway_brreg_translation_workflow_status`,
  `norway_brreg_translations_applied`.
- Sensor `norway_brreg_translation_completion_sensor` and job `norway_brreg_translation_completion_job`.
- `src/dagster_v3/defs/translations/` module.
- `temporal/translations/` Python package (`queue.py`, `queue_cli.py`, `smoke.py`) and the
  `*_translation_queue.duckdb` artifact.
- The `norway_brreg_duckdb` pool reservation for translation steps (the pool remains for ingestion steps).

**Add:**
- One small trigger op/asset (e.g. `norway_brreg_translation_trigger`) downstream of the companies export that
  calls Temporal `StartWorkflow` against the `translator` task queue. Fire-and-forget; mocked in tests.

**Modify:**
- Companies ClickHouse export + table contract: drop `_en` columns from `corpscout.companies` (they move to
  the view). Update `COMPANIES_EXPORT_COLUMNS` and the migration-column contract test accordingly.

---

## ClickHouse Migrations (`corpscout/clickhouse/migrations`)

- New forward migration `000056_corpscout_company_translations.up.sql` (+ `.down.sql`) creating
  `company_translations`.
- New migration creating `norway_companies_translated` view and dropping `_en` columns from `companies`
  (forward-only; the ledger is shared/forward-only per dagster CLAUDE.md).
- Add both migration names to `EXPECTED_MIGRATIONS` in the migrations contract test.

---

## Testing Strategy

**Go (`corpscout/translator`):**
- `registry`: table-driven test of the Norway config (fields, language).
- `scan`: SQL builds the correct untranslated-term query; integration test against a local ClickHouse with a
  seeded `companies` + partial `company_translations` returns only the missing terms.
- `workset`: salvaged unit tests for build/claim/save/retry; term-level dedup verified.
- `llm`: client against a mock OpenAI-compatible server; JSON parse + error paths.
- `temporal`: workflow under the Temporal test environment — happy path drains the queue; a failing batch
  leaves items retryable and the workflow still completes.
- `chwrite`: integration insert + ReplacingMergeTree dedup by key/version.
- **Hash contract test:** Go normalization+`cityHash64` equals ClickHouse `cityHash64(<normalize>(...))` for a
  fixture set (the join correctness hinge).

**Dagster:**
- Trigger op calls `StartWorkflow` with the expected id/policy (mocked Temporal client).
- Companies export no longer depends on any translation asset; `_en` absent from export columns (contract test
  greps the migration).

**E2E:** adapt `e2e/translation_e2e` to drive the new Temporal workflow against a local ClickHouse + LLM stub,
asserting `norway_companies_translated` surfaces `_en`.

---

## Open Implementation Details (for the plan, not blockers)

- Exact ReplacingMergeTree dedup in the view (`argMax(version)` subquery vs `FINAL`).
- Workset file lifecycle/location under the service worksets volume; cleanup after a successful flush.
- Batch sizing / token budget defaults (carry over current Norway values as a baseline).
- Whether the trigger passes only `source_slug` (worker reads registry) — **yes**, keep the payload minimal.
- Continue-as-new vs next-trigger for very large retryable backlogs.
