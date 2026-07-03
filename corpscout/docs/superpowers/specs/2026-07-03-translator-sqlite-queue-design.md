# Translator Queue: DuckDB → SQLite — Design

**Date:** 2026-07-03
**Status:** Approved design, pending implementation plan
**Scope:** `corpscout/translator` only (queue storage internals)
**Builds on:** `2026-07-03-translator-loader-service-split-design.md` (merged). The
enqueue API, Temporal workflow, flush semantics, ClickHouse insert, and Python
loaders are all untouched by this change — they have zero knowledge of queue
storage.

## Problem / motivation

The shared work queue is an OLTP workload (row-wise upserts, point lookups,
small batches) running on DuckDB, an OLAP engine — workable but the wrong
shape, and `go-duckdb` is the module's **only remaining CGO dependency**
(clickhouse-go and the Temporal SDK are pure Go). Switching to a pure-Go
SQLite driver removes CGO entirely: `CGO_ENABLED=0`, no C toolchain, trivial
cross-compilation, faster builds.

**Timing:** the queue became fully transient in the loader/service split
(flush deletes flushed rows; pending items are re-derivable from the loaders'
anti-join against `text_translations`). There is **no data migration** — the
`.duckdb` file is abandoned and a fresh `.sqlite` file starts empty. This
window closes once large backlogs routinely sit in the queue.

## Decisions

1. **Driver:** `modernc.org/sqlite` (pure-Go, `database/sql`, driver name
   `"sqlite"`). Rejected: `mattn/go-sqlite3` (keeps CGO — defeats half the
   point), `zombiezen.com/go/sqlite` (non-database/sql API → churn at every
   call site for no functional gain).
2. **No sqlc** (user decision after discussion): ~11 static queries, stable
   schema, behavior covered by tests against real database files; a
   generated layer would still need the same hand-written façade for hash
   conversion and type mapping. Revisit only if the query surface grows.
   Two ideas from that discussion ARE adopted:
   - **`schema.sql` as single DDL source of truth**, `go:embed`-ed and
     executed at startup — makes later sqlc adoption trivial if ever wanted.
   - **A `pending_items` view** that defines the
     not-in-output/not-in-failed predicate once, replacing the same
     predicate copy-pasted in three queries (a flagged review wart).

## Connection setup

Open once per Runtime (unchanged ownership: the Runtime is the single owner;
the HTTP API and Temporal activities share it in-process):

- DSN: `file:<path>?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)`
- `db.SetMaxOpenConns(1)` — one connection serializes the HTTP enqueue
  handler and activity goroutines, eliminating `SQLITE_BUSY` outright at
  throughput levels this queue will never approach.
- Default queue path changes to `data/translator/queue.sqlite`
  (`internal/config` default + `config/translator.json`).

## Schema (`internal/engine/schema.sql`, embedded)

Same three tables and primary keys as today, with two dialect changes:

1. **`source_text_hash` becomes `TEXT`** (decimal string). SQLite integers
   are signed 64-bit; half of all cityHash64 values exceed 2^63. The Go
   layer already round-trips hashes as strings (`FormatUint` /
   `ParseUint`), so the `cast(? as ubigint)` and `::varchar` casts are
   simply deleted. Hash ordering in queries becomes lexicographic — fine:
   it exists only for deterministic batch ordering, not numeric order.
   `queue.Item.SourceTextHash` and all Go types remain `uint64`; the
   string conversion stays at the SQL boundary where it already lives.
2. **`created_at` becomes `TEXT NOT NULL`** populated with
   `CURRENT_TIMESTAMP` (SQLite's `YYYY-MM-DD HH:MM:SS` UTC format sorts
   lexicographically = chronologically, which oldest-pair picking relies
   on).

Plus the new view:

```sql
CREATE VIEW IF NOT EXISTS pending_items AS
SELECT i.*
FROM input_items AS i
WHERE NOT EXISTS (
    SELECT 1 FROM output_items AS o
    WHERE o.source_table = i.source_table AND o.source_column = i.source_column
      AND o.source_text_hash = i.source_text_hash
      AND o.source_lang = i.source_lang AND o.target_lang = i.target_lang
)
AND NOT EXISTS (
    SELECT 1 FROM failed_items AS f
    WHERE f.source_table = i.source_table AND f.source_column = i.source_column
      AND f.source_text_hash = i.source_text_hash
      AND f.source_lang = i.source_lang AND f.target_lang = i.target_lang
);
```

`createQueueTables` executes the embedded file (idempotent: everything is
`IF NOT EXISTS`). The pair-pick and batch queries become
`SELECT ... FROM pending_items`, deleting the triplicated predicate; the
pending COUNT does not use the view (see Performance at scale).

## Performance at scale (millions of pending rows)

The `NOT EXISTS` probes are always cheap (`output_items` ≤
flush_every_batches × batch_size ≈ 500 rows by construction; `failed_items`
tiny). The risk is unindexed scans over `input_items`, which run per batch.
Three measures keep per-batch queue cost flat regardless of backlog size:

1. **Batch-query index:** `CREATE INDEX IF NOT EXISTS
   idx_input_pair_created ON input_items (source_lang, target_lang,
   created_at);` and the batch query orders by `created_at` (with the
   primary-key columns as tiebreak) instead of the 5-column key. SQLite
   seeks to the language pair, walks oldest-first, probes each row, and
   stops at the LIMIT; skipped rows are only translated-but-unflushed ones
   (≤ ~500). Deterministic ordering is preserved — determinism, not any
   specific order, is the requirement.
2. **Pair-pick index:** `CREATE INDEX IF NOT EXISTS idx_input_created ON
   input_items (created_at);` `ORDER BY created_at LIMIT 1` over the view
   walks the index and stops at the first pending row — near-instant, since
   the oldest rows are almost always pending (flushed rows are deleted).
3. **Pending count by arithmetic identity**, not a view scan:
   `pending = count(input_items) − count(output_items) − count(failed_items)`.
   This is exact under the structural invariant that every output/failed row
   has a matching input row (outputs/faileds are only written for items
   pulled from input; flush deletes inputs and outputs together; failed
   inputs are never deleted). It stays correct even if an operator clears
   `failed_items` (those inputs genuinely become pending again, and the
   arithmetic agrees). Used by both `queueCounts` (per batch) and `Stats`.

Without these, a 3M-row backlog would cost ~3 full scans (~2–10s each) per
~45s batch; with them, per-batch queue overhead is sub-second at any
realistic backlog. A test pins the arithmetic-count identity against the
view definition (both computed on a seeded queue, must agree).

## Query dialect changes

- Placeholders stay `?`; `ON CONFLICT ... DO NOTHING / DO UPDATE` and
  `CREATE TABLE IF NOT EXISTS` are supported SQLite syntax as-is.
- The flush delete's row-value `(cols) IN (SELECT ...)` is rewritten to the
  equivalent correlated-`EXISTS` form (clearer and dialect-safe).
- `internal/queue.validateSchema` swaps `information_schema.columns` for
  `SELECT name FROM pragma_table_info('<table>')` and keeps its role as a
  startup sanity check that an existing file matches the embedded schema.

## What does not change

Public APIs and behavior: `queue.Queue` (GetBatch/SaveBatch/SaveFailed),
`Runtime.Enqueue/Stats/FlushOutput/ProcessOneBatch`, the workflow, the HTTP
contract, ClickHouse writes, dedup semantics, single-language-pair batching,
flush-with-delete ordering (inputs joined against still-present outputs
first, then outputs), `failed_items` never auto-deleted. The Python loaders
are untouched.

## Cutover / ops

- Fresh `data/translator/queue.sqlite`; the old `queue.duckdb` (and the
  long-abandoned `norway_brreg.duckdb`) can be deleted. Anything pending in
  the old file is re-enqueued exactly by the next loader run.
- WAL mode leaves `-wal`/`-shm` sidecar files next to the database — README
  ops note.
- `Makefile`: `CGO_ENABLED := 1` becomes `0` (and the race-detector test
  target keeps working — `go test -race` requires CGO only on some
  platforms; on darwin/arm64 and linux/amd64 race works with CGO_ENABLED=0
  for pure-Go code since Go 1.20; verify in the plan and keep
  `CGO_ENABLED=1` for the race target only if needed).
- `go.mod`: drop `github.com/marcboeker/go-duckdb/v2` (+ duckdb bindings
  indirects), add `modernc.org/sqlite`.

## Testing

- Existing behavior tests carry over with driver/DDL fixture swaps only:
  upsert dedup, validation, max-uint64 hash round-trip (now trivially TEXT),
  single-pair oldest-first batching, empty-queue GetBatch, flush
  insert-then-delete with failed_items untouched, second-flush no-op,
  enqueue/stats, trimmed-language storage.
- Test helpers: DuckDB-specific `to_seconds()` timestamp arithmetic becomes
  SQLite `datetime('2026-01-01 00:00:00', '+' || ? || ' seconds')` (or
  explicit timestamp literals).
- One new test: `pending_items` view semantics (item leaves the view when a
  matching output OR failed row exists).
- A concurrency smoke: parallel Enqueue calls + ProcessOneBatch against one
  Runtime (`-race`), proving the MaxOpenConns(1)/busy_timeout setup holds.
- Real-ClickHouse integration test (enqueue → process → flush) re-run live.

## Out of scope

- sqlc adoption (explicitly deferred).
- Any change to the enqueue API, workflow, flush cadence, or loaders.
- Migrating data from existing DuckDB files.
