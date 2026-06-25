# Python Translator Service — Design Spec

**Date:** 2026-06-26
**Status:** Approved (brainstorming) — pending implementation plan
**Topic:** Extract company-data translation out of the Dagster asset graph into a standalone Python Temporal worker

---

## Problem

Translation of company free-text fields (`*_original` → `*_en`) currently runs **inside the Dagster asset
graph** via `norway_brreg_translation_*` assets, a completion sensor, and a job, with a Python Temporal
workflow (`corpscout/dagster_v3/temporal/translations/`) doing the actual work. This coupling is the source of
recurring operational pain:

- The translation step is gated by a Dagster **completion sensor**. When the Temporal workflow fails mid-run
  (observed 2026-06-25: `status=FAILED` at 64.8%, 551 failed batches), the sensor skips forever and downstream
  assets never run.
- The translation queue asset declares the single-writer **`norway_brreg_duckdb` pool**. When a run crashes
  ungracefully, Dagster leaks the pool slot and **every later Norway run blocks indefinitely** (diagnosed
  2026-06-25; required `dagster-health-check.py --fix` to free the slot).
- Translations are applied back into the per-source DuckDB and re-exported to ClickHouse. Because the ClickHouse
  export **wipes-and-replaces** the table on every refresh, translation is re-coupled to ingestion timing.

The translation logic itself is **not** broken — the workflow reached 64.8%; the failures were operational (the
gating + pool glue), not the translation code. The fix is to **decouple translation from Dagster's run model**,
not to rewrite it.

## Goal

Run translation as a **standalone Python Temporal worker service**, `corpscout/translator`, that reuses the
existing translation workflow/queue/provider. Dagster's only involvement is firing the workflow
(fire-and-forget) after its ClickHouse export. Translations are written to a dedicated ClickHouse table and
surfaced through a join view, so they survive Dagster's wipe-and-replace and never gate ingestion.

## Non-Goals

- Rewriting translation in Go. (Considered and rejected: we just consolidated *off* the Go `scheduler`; a new
  Go service reintroduces a second language and a fragile cross-language hash contract for no throughput win —
  the bottleneck is the LLM, not the orchestrator.)
- Translating sources other than `norway_brreg` in this iteration. The engine is generic/config-driven but
  only Norway's columns are registered initially (adding a source later = a config entry).
- Self-scheduling. Translation is **triggered by Dagster** after its ClickHouse export (not a Temporal cron).

---

## Key Decisions (resolved during brainstorming)

| # | Decision | Choice |
|---|----------|--------|
| 1 | Substrate | **Standalone Python Temporal worker** — reuse existing `temporal/translations` workflow, DuckDB queue, and OpenAI-compatible provider |
| 2 | Queue store | **Reuse the existing DuckDB queue** (`translations/queue.py`); it is already term-level deduped |
| 3 | Scope | Generic config-driven engine, **Norway-only config** initially |
| 4 | Trigger | **Dagster triggers** the workflow after its ClickHouse export, **by workflow name** (thin op; no heavy import) |
| 5 | Wait semantics | **Fire-and-forget** — Dagster does not gate on completion |
| 6 | Write-back model | **Separate `company_translations` table + join view**; survives Dagster's wipe-and-replace |
| 7 | Dedup granularity | **Term-level** — translate each distinct string once (existing `item_id = hash(text+lang)`) |
| 8 | Service name | `corpscout/translator` (Python package + worker entrypoint) |
| 9 | `_en` exposure | **New join-view**; drop `_en` columns from the base companies table |
| 10 | Hash join contract | **ClickHouse computes the join hash on both write and read** — no cross-language hash contract |

---

## Architecture

```
Dagster (corpscout/dagster_v3)                      corpscout/translator (NEW standalone Python service)
─────────────────────────────────                   ─────────────────────────────────────────────────────
ingest norway_brreg                                  worker (Temporal) on task queue "translation-local-llm"
   │                                                    reuses temporal/translations workflow + DuckDB queue
   ▼                                                       + OpenAI-compatible provider
export corpscout.companies   (NO _en columns)
   │
   ▼
fire Temporal StartWorkflow ────────────────────────►  TranslationQueueWorkflow / TranslateSourceWorkflow
   (by name; fire-and-forget;                              id = "translate-<source_slug>"
    WorkflowIDConflictPolicy.USE_EXISTING)                 (USE_EXISTING dedup)
   │                                                        │
   ▼                                                        ▼
asset completes immediately                          1. scan: distinct untranslated terms FROM ClickHouse
                                                     2. seed DuckDB queue (term-level, deduped)  [existing]
                                                     3. loop: claim batch → LLM translate → save  [existing]
                                                     4. flush results → corpscout.company_translations  [NEW]
                                                            │
queries read view  ◄──────────────────────────────────────┘
corpscout.norway_companies_translated
   = companies LEFT JOIN company_translations (ClickHouse computes hash on both sides)
```

Next refresh re-exports `companies` (wiping it) but **not** `company_translations`; the scan then finds only
new/changed terms → minimal re-translation.

---

## What We Reuse vs. Build

**Reuse as-is (move into `corpscout/translator`):**
- `temporal/translations/queue.py` — the Temporal workflow + activities (initialize, process batch, summarize).
- `temporal/translations/queue_cli.py` — `worker_main()` already runs a standalone worker; `start_main()` /
  `status_main()` for ops.
- `translations/queue.py` — the DuckDB queue (items/results/batches), term-level dedup, leasing, retry.
- The OpenAI-compatible translation provider (temperature 0, JSON-in/JSON-out, configurable max tokens /
  `extra_body`).

**Build (the genuinely new parts):**
1. **ClickHouse scan** — replace the current "seed from source DuckDB" with "seed from ClickHouse": for each
   registered field, `SELECT DISTINCT <original_col>` where the original is non-empty **and** not already in
   `company_translations` (LEFT JOIN). Returns distinct terms.
2. **ClickHouse flush** — a new final workflow step that writes completed results to `company_translations`
   (replaces the current DuckDB apply-back + re-export).
3. **Generic registry** — per `source_slug`: `{source_lang, ch_table, [(original_col, field)]}`. Norway-only
   initially.
4. **Dagster trigger op** — a thin fire-and-forget `StartWorkflow` call (by name) after the companies export.
5. **ClickHouse migrations** — `company_translations` table + the join view; drop `_en` from `companies`.

**Simplification:** the existing `translation_locations` table tracks `source_table/source_pk/source_field`
for *patching source rows*. The new model writes **term-level** results keyed by hash, so per-row locations are
no longer needed — a term only needs its `field` and `source_lang`. Locations may be dropped or reduced to
`field` carriage.

---

## Components (`corpscout/translator/`)

```
corpscout/translator/
├── pyproject.toml             # own package; deps: temporalio, duckdb, openai/httpx, clickhouse-connect
├── translator/
│   ├── worker.py              # entrypoint — runs the Temporal worker (reuses queue_cli.worker_main)
│   ├── registry.py            # per source_slug → {source_lang, ch_table, [(original_col, field)]}
│   ├── scan.py                # ClickHouse: distinct untranslated terms (LEFT JOIN company_translations)
│   ├── flush.py               # ClickHouse: insert completed results into company_translations
│   ├── workflow.py            # workflow wiring scan → seed (existing queue) → translate loop → flush
│   ├── queue.py               # MOVED from translations/queue.py (DuckDB queue)
│   └── provider.py            # MOVED OpenAI-compatible translation provider
└── Dockerfile                 # + a `translator` service entry in corpscout/docker-compose.yml
```

The Temporal **task queue name stays** `translation-local-llm` (existing constant) so the worker and the
Dagster trigger agree without new config.

---

## Data Model

### `corpscout.company_translations` (new ClickHouse table)

Term-level, hash-keyed. Identical source strings across many companies are translated once.

```sql
CREATE TABLE IF NOT EXISTS corpscout.company_translations
(
    source_slug       LowCardinality(String),   -- 'norway_brreg'
    field             LowCardinality(String),   -- 'company_description'
    source_text_hash  UInt64,                    -- cityHash64(normalized original) — computed by ClickHouse
    source_lang       LowCardinality(String),   -- 'no'
    target_lang       LowCardinality(String),   -- 'en'
    translated_text   String,
    provider          LowCardinality(String),
    model             LowCardinality(String),
    version           UInt64                     -- translated_at epoch seconds (ReplacingMergeTree version)
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (source_slug, field, source_text_hash);   -- non-nullable keys (allow_nullable_key is off)
```

### Killing the hash contract — ClickHouse computes the hash on both sides

The fragile part of any hash-join design is keeping the producer's hash byte-identical to the view's hash.
We avoid it: the Python worker **never computes `cityHash64`**. The flush step inserts into a **staging table**
carrying the raw `source_text` + `translated_text`, then:

```sql
INSERT INTO corpscout.company_translations
SELECT 'norway_brreg', field, cityHash64(<normalize>(source_text)), 'no', 'en',
       translated_text, provider, model, <version>
FROM <staging>;
```

The view joins using the **same** `cityHash64(<normalize>(...))` expression. The hash is thus produced by
ClickHouse on both write and read — there is exactly **one** definition of the normalization+hash, in SQL.
(`<normalize>` = a fixed trim/lower/collapse-whitespace expression defined once and reused.)

### View: `corpscout.norway_companies_translated`

Exposes `_en` by joining the base table to `company_translations` per field. The base `companies` table
**no longer carries `_en` columns** (Decision 9); consumers point at the view.

```sql
CREATE VIEW corpscout.norway_companies_translated AS
SELECT
    c.*,
    t_desc.translated_text AS company_description_en,
    t_act.translated_text  AS activity_text_en,
    t_pur.translated_text  AS articles_purpose_en
    -- ...one LEFT JOIN per translated field...
FROM corpscout.companies AS c
LEFT JOIN (
    SELECT source_text_hash, argMax(translated_text, version) AS translated_text
    FROM corpscout.company_translations
    WHERE source_slug='norway_brreg' AND field='company_description'
    GROUP BY source_text_hash
) AS t_desc ON t_desc.source_text_hash = cityHash64(<normalize>(c.company_description_original))
-- ... t_act, t_pur similarly ...
;
```

`argMax(translated_text, version)` resolves ReplacingMergeTree duplicates without `FINAL`.

---

## Data Flow (end to end)

1. Dagster ingests `norway_brreg`, exports `corpscout.companies` **with no `_en` columns**.
2. Dagster's final step fires `StartWorkflow` by name with `id="translate-norway_brreg"` and
   `WorkflowIDConflictPolicy.USE_EXISTING`, then the asset completes (fire-and-forget).
3. The `translator` worker runs: scan ClickHouse → seed the DuckDB queue → claim/translate/save loop → flush
   results to `company_translations`.
4. Queries read `corpscout.norway_companies_translated`; `_en` is filled for translated terms, empty otherwise.
5. On the next refresh, `companies` is wiped and re-exported; `company_translations` persists, so the scan only
   enqueues genuinely new/changed terms.

---

## Error Handling

- **Fire-and-forget eliminates the original failure class:** no completion sensor, no gating, no DuckDB pool
  slot held by translation → the leaked-slot / stuck-QUEUED failure debugged on 2026-06-25 cannot recur from
  translation.
- **Batch failure isolation:** a failed LLM batch leaves its items `failed_retryable` in the queue; the
  workflow drains the rest and **completes** rather than hard-failing the whole run (the current
  `max_batch_failures=0` already allows this — make the workflow's terminal state success-on-partial). Leftover
  retryable items are picked up on the next trigger.
- **LLM/transport errors:** Temporal activity retry with backoff; persistent failure leaves items retryable,
  not lost.
- **Duplicate triggers:** Temporal workflow-id dedup (`USE_EXISTING`) — at most one in-flight run per source.
- **Empty scan:** workflow no-ops fast.
- **Refuse to corrupt:** flush only inserts rows with non-empty `translated_text`.

---

## Changes to Dagster (`corpscout/dagster_v3`)

**Remove:**
- Assets `norway_brreg_translation_queue`, `norway_brreg_translation_workflow_status`,
  `norway_brreg_translations_applied`.
- Sensor `norway_brreg_translation_completion_sensor` and job `norway_brreg_translation_completion_job`.
- `src/dagster_v3/defs/translations/` module.
- The translation `pool` reservation on translation steps (the `norway_brreg_duckdb` pool remains for ingestion).

**Move out (into `corpscout/translator`):**
- `temporal/translations/` (the workflow/worker/provider) and `translations/queue.py` (the DuckDB queue).

**Add:**
- One thin trigger op/asset (`norway_brreg_translation_trigger`) downstream of the companies export that calls
  Temporal `StartWorkflow` by name on task queue `translation-local-llm`. Fire-and-forget; mocked in tests.

**Modify:**
- Companies ClickHouse export + table contract: drop `_en` columns from `corpscout.companies`. Update
  `COMPANIES_EXPORT_COLUMNS` and the migration-column contract test.

---

## ClickHouse Migrations (`corpscout/clickhouse/migrations`)

- New forward migration creating `company_translations` (ReplacingMergeTree) + a `*_staging` insert path.
- New migration creating `norway_companies_translated` view and dropping `_en` columns from `companies`
  (forward-only; the ledger is shared/forward-only).
- Add both migration names to `EXPECTED_MIGRATIONS` in the migrations contract test.

---

## Testing Strategy

**`corpscout/translator` (Python):**
- `registry`: table-driven test of the Norway config (fields, language).
- `scan`: against a local ClickHouse with seeded `companies` + partial `company_translations`, returns only
  the missing distinct terms.
- `queue`: existing DuckDB queue tests carried over (build/claim/save/retry, term-level dedup).
- `provider`: against a mock OpenAI-compatible server; JSON parse + error paths (existing smoke test adapted).
- `workflow`: under the Temporal test environment — happy path drains the queue; a failing batch leaves items
  retryable and the workflow still completes; empty scan no-ops.
- `flush`: insert via staging → `company_translations` dedup by key/version; only non-empty translations land.
- **Hash/view test:** seed `company_translations` via the staging insert, then assert the view's
  `cityHash64(<normalize>(...))` join surfaces the right `_en` (proves the single SQL hash definition works on
  both sides).

**Dagster:**
- Trigger op calls `StartWorkflow` with the expected id/name/policy (mocked Temporal client).
- Companies export no longer depends on any translation asset; `_en` absent from export columns (contract test
  greps the migration).

**E2E:** adapt `e2e/translation_e2e` to drive the workflow against a local ClickHouse + LLM stub, asserting
`norway_companies_translated` surfaces `_en`.

---

## Open Implementation Details (for the plan, not blockers)

- Exact `<normalize>` SQL expression (trim/lower/collapse whitespace) — defined once, referenced by flush and
  view. Must match how "untranslated" is judged in the scan.
- Whether the worker imports the moved workflow directly vs. Dagster starting it purely by string name (keep
  Dagster's dependency surface minimal).
- DuckDB queue file lifecycle/location under the service volume; cleanup after a successful flush.
- Batch sizing / token budget defaults (carry over current Norway values).
- Continue-as-new vs. next-trigger for very large retryable backlogs.
- Packaging: separate `pyproject.toml`/venv vs. a worker entrypoint within the existing dagster_v3 package
  (leaning separate package so it deploys/restarts independently of Dagster).
