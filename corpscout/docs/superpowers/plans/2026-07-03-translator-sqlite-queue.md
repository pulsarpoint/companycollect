# Translator SQLite Queue Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move the translator's shared work queue from DuckDB to SQLite (pure-Go driver), eliminating the module's last CGO dependency, with an embedded schema, a `pending_items` view, indexes, and an O(1)-per-batch pending count.

**Architecture:** A new `internal/queuedb` package owns the embedded `schema.sql` (3 tables with TEXT hashes, `pending_items` view, 2 indexes) and the configured connection (`modernc.org/sqlite`, WAL, busy_timeout, `MaxOpenConns(1)`). `internal/queue` and `internal/engine` keep their public APIs and swap SQL dialect internals: casts removed (hash is TEXT), batch queries read the view ordered by `created_at` (index-backed early termination), the flush delete becomes correlated-EXISTS, and `queueCounts` becomes the arithmetic identity `pending = input − output − failed`. No data migration: fresh `queue.sqlite`, old `.duckdb` files abandoned.

**Tech Stack:** Go 1.24+, modernc.org/sqlite (database/sql driver name `"sqlite"`), go:embed.

**Spec:** `corpscout/docs/superpowers/specs/2026-07-03-translator-sqlite-queue-design.md`

## Global Constraints

- Work dir: `corpscout/translator/`. Public APIs unchanged: `queue.Queue` (GetBatch/SaveBatch/SaveFailed), `Runtime.Enqueue/Stats/FlushOutput/ProcessOneBatch`, HTTP contract, workflow, ClickHouse writes. Python loaders untouched.
- Driver exactly `modernc.org/sqlite`; DSN pragmas exactly `journal_mode(WAL)`, `busy_timeout(5000)`, `synchronous(NORMAL)`; `db.SetMaxOpenConns(1)`.
- `source_text_hash` stored as `TEXT` (decimal string; Go types stay uint64, conversion at SQL boundary as today). `created_at`/`completed_at`/`failed_at` are `TEXT NOT NULL` with `CURRENT_TIMESTAMP` (second resolution — all orderings must carry primary-key tiebreaks for determinism).
- Indexes exactly: `idx_input_created ON input_items (created_at)` and `idx_input_pair_created ON input_items (source_lang, target_lang, created_at)`.
- Pending count is the arithmetic identity `input − output − failed` (exact under the structural invariant; a test pins it against the view).
- Default queue path becomes `data/translator/queue.sqlite`.
- `CGO_ENABLED=0` end state (Makefile); `go.mod` loses go-duckdb entirely.
- Every task leaves `go build ./... && go test ./...` green. Conventional Commits; `go fmt ./... && go vet ./...` before each commit.

---

### Task 1: `internal/queuedb` — embedded schema and configured connection

**Files:**
- Create: `internal/queuedb/schema.sql`
- Create: `internal/queuedb/queuedb.go`
- Test: `internal/queuedb/queuedb_test.go`

**Interfaces:**
- Produces:

```go
package queuedb

// Open opens (creating if missing) the queue database at path with the
// translator's required pragmas and a single pooled connection.
func Open(path string) (*sql.DB, error)

// CreateTables applies the embedded schema; idempotent.
func CreateTables(ctx context.Context, db *sql.DB) error

// SchemaSQL returns the embedded schema text (for tests/tooling).
func SchemaSQL() string
```

- Consumes: nothing. Adds the `modernc.org/sqlite` dependency (`go get modernc.org/sqlite` — do NOT remove go-duckdb yet; internal/queue and internal/engine still use it until Tasks 2–3).

- [ ] **Step 1: Write schema.sql**

Create `internal/queuedb/schema.sql`:

```sql
-- Translator queue schema. Single source of truth: embedded via go:embed and
-- executed by CreateTables; everything is IF NOT EXISTS (idempotent).
-- source_text_hash is a decimal-string uint64 (SQLite integers are signed
-- 64-bit; half of all cityHash64 values exceed 2^63).

CREATE TABLE IF NOT EXISTS input_items (
    source_table TEXT NOT NULL,
    source_column TEXT NOT NULL,
    source_text TEXT NOT NULL,
    source_text_hash TEXT NOT NULL,
    source_lang TEXT NOT NULL,
    target_lang TEXT NOT NULL,
    source_language_name TEXT NOT NULL,
    target_language_name TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (source_table, source_column, source_text_hash, source_lang, target_lang)
);

CREATE TABLE IF NOT EXISTS output_items (
    source_table TEXT NOT NULL,
    source_column TEXT NOT NULL,
    source_text TEXT NOT NULL,
    source_text_hash TEXT NOT NULL,
    source_lang TEXT NOT NULL,
    target_lang TEXT NOT NULL,
    translated_text TEXT NOT NULL,
    provider TEXT NOT NULL,
    model TEXT NOT NULL,
    completed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (source_table, source_column, source_text_hash, source_lang, target_lang)
);

CREATE TABLE IF NOT EXISTS failed_items (
    source_table TEXT NOT NULL,
    source_column TEXT NOT NULL,
    source_text TEXT NOT NULL,
    source_text_hash TEXT NOT NULL,
    source_lang TEXT NOT NULL,
    target_lang TEXT NOT NULL,
    error_message TEXT NOT NULL,
    failed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (source_table, source_column, source_text_hash, source_lang, target_lang)
);

-- Early-termination indexes: batch queries walk created_at order and stop at
-- their LIMIT; without these, every batch pays a full scan of input_items.
CREATE INDEX IF NOT EXISTS idx_input_created ON input_items (created_at);
CREATE INDEX IF NOT EXISTS idx_input_pair_created ON input_items (source_lang, target_lang, created_at);

-- The single definition of "pending": in input, not yet translated, not failed.
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

- [ ] **Step 2: Write the failing tests**

Create `internal/queuedb/queuedb_test.go`:

```go
package queuedb

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
)

func openTestDB(t *testing.T) (*sql.DB, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "queue.sqlite")
	db, err := Open(path)
	if err != nil {
		t.Fatalf("open queue db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := CreateTables(context.Background(), db); err != nil {
		t.Fatalf("create tables: %v", err)
	}
	return db, path
}

func TestOpenAppliesPragmas(t *testing.T) {
	db, _ := openTestDB(t)

	var journalMode string
	if err := db.QueryRow("PRAGMA journal_mode").Scan(&journalMode); err != nil {
		t.Fatalf("read journal_mode: %v", err)
	}
	if journalMode != "wal" {
		t.Fatalf("expected WAL journal mode, got %q", journalMode)
	}

	var busyTimeout int
	if err := db.QueryRow("PRAGMA busy_timeout").Scan(&busyTimeout); err != nil {
		t.Fatalf("read busy_timeout: %v", err)
	}
	if busyTimeout != 5000 {
		t.Fatalf("expected busy_timeout 5000, got %d", busyTimeout)
	}
}

func TestCreateTablesIsIdempotentAndCreatesAllObjects(t *testing.T) {
	db, _ := openTestDB(t)
	if err := CreateTables(context.Background(), db); err != nil {
		t.Fatalf("second create tables must be idempotent: %v", err)
	}

	for _, object := range []struct{ kind, name string }{
		{"table", "input_items"},
		{"table", "output_items"},
		{"table", "failed_items"},
		{"view", "pending_items"},
		{"index", "idx_input_created"},
		{"index", "idx_input_pair_created"},
	} {
		var count int
		if err := db.QueryRow(
			"SELECT count(*) FROM sqlite_master WHERE type = ? AND name = ?",
			object.kind, object.name,
		).Scan(&count); err != nil {
			t.Fatalf("check %s %s: %v", object.kind, object.name, err)
		}
		if count != 1 {
			t.Fatalf("expected %s %s to exist", object.kind, object.name)
		}
	}
}

func TestPendingItemsViewSemantics(t *testing.T) {
	db, _ := openTestDB(t)
	ctx := context.Background()

	insert := func(table, hash string) {
		t.Helper()
		var query string
		switch table {
		case "input_items":
			query = `insert into input_items (source_table, source_column, source_text, source_text_hash,
				source_lang, target_lang, source_language_name, target_language_name)
				values ('corpscout.no_companies', 'activity_text_original', 'tekst', ?, 'no', 'en', 'Norwegian', 'English')`
		case "output_items":
			query = `insert into output_items (source_table, source_column, source_text, source_text_hash,
				source_lang, target_lang, translated_text, provider, model)
				values ('corpscout.no_companies', 'activity_text_original', 'tekst', ?, 'no', 'en', 'text', 'local_llm', 'm')`
		case "failed_items":
			query = `insert into failed_items (source_table, source_column, source_text, source_text_hash,
				source_lang, target_lang, error_message)
				values ('corpscout.no_companies', 'activity_text_original', 'tekst', ?, 'no', 'en', 'boom')`
		}
		if _, err := db.ExecContext(ctx, query, hash); err != nil {
			t.Fatalf("insert into %s: %v", table, err)
		}
	}
	pendingCount := func() int {
		t.Helper()
		var n int
		if err := db.QueryRow("SELECT count(*) FROM pending_items").Scan(&n); err != nil {
			t.Fatalf("count pending: %v", err)
		}
		return n
	}

	insert("input_items", "1")
	insert("input_items", "2")
	insert("input_items", "3")
	if got := pendingCount(); got != 3 {
		t.Fatalf("expected 3 pending, got %d", got)
	}

	insert("output_items", "1") // translated → leaves the view
	if got := pendingCount(); got != 2 {
		t.Fatalf("expected 2 pending after output, got %d", got)
	}

	insert("failed_items", "2") // failed → leaves the view
	if got := pendingCount(); got != 1 {
		t.Fatalf("expected 1 pending after failure, got %d", got)
	}
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `go test ./internal/queuedb/ -v 2>&1 | head -5`
Expected: FAIL to compile — package does not exist yet (only schema.sql and the test file).

- [ ] **Step 4: Implement queuedb.go**

```bash
go get modernc.org/sqlite@latest
```

Create `internal/queuedb/queuedb.go`:

```go
// Package queuedb owns the translator queue's SQLite storage: the embedded
// schema (tables, pending_items view, indexes) and the configured
// connection. The queue is transient working state — flushed rows are
// deleted and pending work is re-derivable from the loaders' anti-join —
// so there is no schema migration machinery, only idempotent creation.
package queuedb

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"net/url"

	_ "modernc.org/sqlite"
)

//go:embed schema.sql
var schemaSQL string

// Open opens (creating if missing) the queue database at path with WAL
// journaling, a 5s busy timeout, NORMAL synchronous mode, and a single
// pooled connection. One connection serializes all writers (HTTP enqueue
// handler and Temporal activity goroutines share the pool), which
// eliminates SQLITE_BUSY contention outright at queue throughput.
func Open(path string) (*sql.DB, error) {
	if path == "" {
		return nil, fmt.Errorf("queue database path is required")
	}

	dsn := "file:" + url.PathEscape(path) +
		"?_pragma=journal_mode(WAL)" +
		"&_pragma=busy_timeout(5000)" +
		"&_pragma=synchronous(NORMAL)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open queue sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)

	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping queue sqlite: %w", err)
	}
	return db, nil
}

// CreateTables applies the embedded schema; every statement is
// IF NOT EXISTS, so repeated calls are no-ops.
func CreateTables(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, schemaSQL); err != nil {
		return fmt.Errorf("apply queue schema: %w", err)
	}
	return nil
}

// SchemaSQL returns the embedded schema text.
func SchemaSQL() string {
	return schemaSQL
}
```

Implementation note: `url.PathEscape` on an absolute path escapes `/` — that breaks the DSN. Use this instead and verify with the tests: pass the path raw (`"file:" + path + "?..."`); modernc accepts unescaped paths without query-relevant characters. If a test with a `t.TempDir()` path fails to open, fall back to `url.QueryEscape` on the path with `file:`+escaped. The tests (which use real temp paths) are the arbiter — whichever form makes them pass on this machine is correct; leave a comment stating the choice.

Also verify multi-statement `ExecContext`: modernc's driver executes multi-statement strings sequentially. If `CreateTables` errors with a prepared-statement complaint, split `schemaSQL` on `";\n"` boundaries and execute statements in a loop (keep the split dumb — the schema contains no embedded semicolons in strings).

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./internal/queuedb/ -v`
Expected: PASS — pragmas, idempotency, all six schema objects, view semantics.

- [ ] **Step 6: Format, vet, commit**

```bash
go fmt ./... && go vet ./...
git add internal/queuedb/ go.mod go.sum
git commit -m "feat(translator): queuedb package — embedded sqlite schema and configured connection"
```

---

### Task 2: `internal/queue` on SQLite

**Files:**
- Modify: `internal/queue/queue.go`
- Modify: `internal/queue/queue_test.go`

**Interfaces:**
- Consumes: `queuedb.Open`, `queuedb.CreateTables` (Task 1).
- Produces: unchanged public API (`Init`, `New`, `GetBatch`, `SaveBatch`, `SaveFailed`, `Close`, types `Item`/`TranslatedItem`/`FailedItem`); internals now SQLite. `GetBatch` ordering becomes `created_at` + primary-key tiebreak (both queries via `pending_items`).

- [ ] **Step 1: Update the tests first**

In `internal/queue/queue_test.go` (read the whole file first):

1. Replace every `sql.Open("duckdb", ...)` / DuckDB DDL fixture with `queuedb.Open(path)` + `queuedb.CreateTables(ctx, db)` (import `github.com/pulsarpoint/corpscout/translator/internal/queuedb`; drop the go-duckdb blank import). The existing `createEmptyQueueFixture`/`openTestQueue` helpers collapse to thin wrappers over queuedb.
2. `insertTestInput` helper: replace the DuckDB-only timestamp arithmetic (`timestamp '2026-01-01 00:00:00' + to_seconds(?)`) with SQLite: `datetime('2026-01-01 00:00:00', '+' || ? || ' seconds')`, and drop the `cast(? as ubigint)` (hash param is the plain decimal string now).
3. All other fixture inserts: `cast(? as ubigint)` → `?`.
4. Add the empty-queue regression test (a known coverage gap):

```go
func TestGetBatchReturnsEmptySliceOnEmptyQueue(t *testing.T) {
	db := openTestQueue(t)
	q, err := New(db)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}

	batch, err := q.GetBatch(context.Background(), 10)
	if err != nil {
		t.Fatalf("get batch on empty queue: %v", err)
	}
	if len(batch) != 0 {
		t.Fatalf("expected empty batch, got %d items", len(batch))
	}
}
```

5. `TestGetBatchReturnsSingleLanguagePairOldestFirst` keeps its behavior assertions unchanged (Latvian oldest → only Latvian returned, names populated).

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/queue/ 2>&1 | head -8`
Expected: FAIL — production code still opens/validates DuckDB dialect against a SQLite fixture.

- [ ] **Step 3: Update queue.go**

1. Imports: drop `_ "github.com/marcboeker/go-duckdb/v2"`, add `github.com/pulsarpoint/corpscout/translator/internal/queuedb`.
2. `Init(path)`: keep the stat/IsDir checks; replace `sql.Open("duckdb", path)` with `queuedb.Open(path)`.
3. `GetBatch` — both queries collapse onto the view. Pair-pick:

```go
	err := q.db.QueryRowContext(ctx, `
		select source_lang, target_lang
		from pending_items
		order by created_at, source_lang, target_lang
		limit 1
	`).Scan(&srcLang, &dstLang)
```

Batch query:

```go
	rows, err := q.db.QueryContext(ctx, `
		select
			source_table,
			source_column,
			source_text,
			source_text_hash,
			source_lang,
			target_lang,
			source_language_name,
			target_language_name
		from pending_items
		where source_lang = ? and target_lang = ?
		order by created_at, source_table, source_column, source_text_hash
		limit ?
	`, srcLang, dstLang, limit)
```

(The scan loop is unchanged: `source_text_hash` arrives as a string and goes through the existing `ParseUint`.)
4. `SaveBatch`/`SaveFailed`: `cast(? as ubigint)` → `?` (hash params stay `strconv.FormatUint(...)` strings). `current_timestamp` keyword stays (valid SQLite).
5. `validateSchema`/`validateTable`: replace the `information_schema.columns` query with:

```go
	rows, err := db.QueryContext(ctx, "select name from pragma_table_info(?)", table)
```

If parameter binding into the table-valued function errors on this driver, fall back to `fmt.Sprintf("select name from pragma_table_info(%q)", table)` — `table` values are package-internal constants, never user input; leave a comment saying so.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/queue/ -v 2>&1 | tail -6`
Expected: PASS including the single-pair test and the new empty-queue test. (`go build ./...` still green — engine still on DuckDB until Task 3, both drivers coexist.)

- [ ] **Step 5: Format, vet, commit**

```bash
go fmt ./... && go vet ./...
git add internal/queue/
git commit -m "feat(translator): queue package on sqlite via pending_items view"
```

---

### Task 3: `internal/engine` on SQLite — arithmetic counts, EXISTS flush, identity + concurrency tests

**Files:**
- Modify: `internal/engine/runtime.go`
- Modify: `internal/engine/load.go`
- Modify: `internal/engine/enqueue.go`
- Modify: `internal/engine/load_test.go`, `internal/engine/runtime_test.go`, `internal/engine/enqueue_test.go`, `internal/engine/integration_test.go` (fixture/driver fallout)
- Test (new tests in): `internal/engine/enqueue_test.go` (identity), `internal/engine/runtime_test.go` (concurrency smoke)

**Interfaces:**
- Consumes: `queuedb.Open`, `queuedb.CreateTables`.
- Produces: unchanged public API. `queueCounts` struct becomes `{input, output, failed, pending int}` with `pending = input − output − failed` (clamped at 0 with a comment naming the invariant); `Stats` maps it directly (its separate `countRows(failed_items)` call is deleted).

- [ ] **Step 1: Write the two new failing tests**

Add to `internal/engine/enqueue_test.go` (uses the package's existing runtime/fake helpers — read them first and reuse):

```go
// TestPendingCountMatchesViewDefinition pins the arithmetic identity
// (pending = input − output − failed) against the pending_items view: both
// must agree on a queue seeded with pending, translated, and failed rows.
func TestPendingCountMatchesViewDefinition(t *testing.T) {
	ctx := context.Background()
	rt := newEnqueueTestRuntime(t) // existing helper; translator fake must succeed

	req := validEnqueueRequest(1)
	req.Items = []EnqueueItem{
		{SourceTable: "corpscout.no_companies", SourceColumn: "activity_text_original", SourceText: "en", SourceTextHash: "1"},
		{SourceTable: "corpscout.no_companies", SourceColumn: "activity_text_original", SourceText: "to", SourceTextHash: "2"},
		{SourceTable: "corpscout.no_companies", SourceColumn: "activity_text_original", SourceText: "tre", SourceTextHash: "3"},
	}
	if _, err := rt.Enqueue(ctx, req); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Translate one item (batch size 1) → 1 output row; fail one directly.
	if _, err := rt.ProcessOneBatch(ctx, ProcessInput{BatchSize: 1, TimeoutSeconds: 5}); err != nil {
		t.Fatalf("process one batch: %v", err)
	}
	if _, err := rt.db.ExecContext(ctx, `
		insert into failed_items (source_table, source_column, source_text, source_text_hash,
			source_lang, target_lang, error_message)
		select source_table, source_column, source_text, source_text_hash,
			source_lang, target_lang, 'seeded failure'
		from pending_items limit 1
	`); err != nil {
		t.Fatalf("seed failed item: %v", err)
	}

	counts, err := rt.queueCounts(ctx)
	if err != nil {
		t.Fatalf("queue counts: %v", err)
	}
	var viewPending int
	if err := rt.db.QueryRowContext(ctx, "select count(*) from pending_items").Scan(&viewPending); err != nil {
		t.Fatalf("count view: %v", err)
	}
	if counts.pending != viewPending {
		t.Fatalf("arithmetic pending %d != view pending %d", counts.pending, viewPending)
	}
	if counts.pending != 1 {
		t.Fatalf("expected exactly 1 pending (3 − 1 translated − 1 failed), got %d", counts.pending)
	}
}
```

Add to `internal/engine/runtime_test.go`:

```go
// TestConcurrentEnqueueAndProcess is the MaxOpenConns(1)/busy_timeout smoke:
// parallel enqueues racing the batch loop must produce no SQLITE_BUSY errors
// and a consistent final state. Run under -race in CI.
func TestConcurrentEnqueueAndProcess(t *testing.T) {
	ctx := context.Background()
	rt := newEnqueueTestRuntime(t) // translator fake must succeed

	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			req := validEnqueueRequest(1)
			req.Items = nil
			for i := 0; i < 25; i++ {
				req.Items = append(req.Items, EnqueueItem{
					SourceTable:    "corpscout.no_companies",
					SourceColumn:   "activity_text_original",
					SourceText:     fmt.Sprintf("tekst %d-%d", worker, i),
					SourceTextHash: fmt.Sprintf("%d", worker*1000+i+1),
				})
			}
			if _, err := rt.Enqueue(ctx, req); err != nil {
				errs <- err
			}
		}(worker)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 5; i++ {
			if _, err := rt.ProcessOneBatch(ctx, ProcessInput{BatchSize: 10, TimeoutSeconds: 5}); err != nil {
				errs <- err
			}
		}
	}()
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent operation failed: %v", err)
	}

	stats, err := rt.Stats(ctx)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.Input != 100 {
		t.Fatalf("expected 100 input rows, got %d", stats.Input)
	}
	if stats.Pending+stats.Output != 100 || stats.Failed != 0 {
		t.Fatalf("inconsistent final state: %+v", stats)
	}
}
```

(Add missing imports — `sync`, `fmt` — as needed.)

- [ ] **Step 2: Run to verify failure state**

Run: `go test ./internal/engine/ 2>&1 | head -8`
Expected: FAIL — engine still opens DuckDB; new tests reference the SQLite-era `queueCounts` shape.

- [ ] **Step 3: Implement the engine changes**

1. `runtime.go` `NewRuntime`: replace `sql.Open("duckdb", config.QueuePath)` with `queuedb.Open(config.QueuePath)`; drop the go-duckdb blank import; import queuedb. (Keep the `os.MkdirAll` directory creation that precedes it.)
2. `load.go` `createQueueTables`: delete the three inline `CREATE TABLE` statements; the function body becomes a delegation:

```go
func createQueueTables(ctx context.Context, db *sql.DB) error {
	return queuedb.CreateTables(ctx, db)
}
```

(If after this `load.go`'s remaining content is only the upsert/validate/count helpers, keep the file — no restructuring.)
3. `load.go` `upsertInputItems`: `cast(? as ubigint)` → `?` (hash already passed as `strconv.FormatUint` string).
4. `runtime.go` `outputTranslations`: `source_text_hash::varchar` → `source_text_hash` (already scanned into a string).
5. `runtime.go` `FlushOutput` input-delete becomes correlated EXISTS (the output-delete stays `delete from output_items`):

```go
	if _, err := r.db.ExecContext(ctx, `
		delete from input_items
		where exists (
			select 1 from output_items as o
			where o.source_table = input_items.source_table
				and o.source_column = input_items.source_column
				and o.source_text_hash = input_items.source_text_hash
				and o.source_lang = input_items.source_lang
				and o.target_lang = input_items.target_lang
		)
	`); err != nil {
```

6. `runtime.go` `queueCounts` — arithmetic identity, no view scan:

```go
type queueCounts struct {
	input   int
	output  int
	failed  int
	pending int
}

func (r *Runtime) queueCounts(ctx context.Context) (queueCounts, error) {
	var counts queueCounts
	var err error
	if counts.input, err = countRows(ctx, r.db, "input_items"); err != nil {
		return queueCounts{}, err
	}
	if counts.output, err = countRows(ctx, r.db, "output_items"); err != nil {
		return queueCounts{}, err
	}
	if counts.failed, err = countRows(ctx, r.db, "failed_items"); err != nil {
		return queueCounts{}, err
	}
	// Exact under the structural invariant: every output/failed row has a
	// matching input row (outputs/faileds are only written for items pulled
	// from input; flush deletes inputs and outputs together; failed inputs
	// are never auto-deleted). Clamped defensively — going negative would
	// mean the invariant broke, and a stuck-negative pending must not wedge
	// the workflow's queue-empty detection.
	counts.pending = counts.input - counts.output - counts.failed
	if counts.pending < 0 {
		counts.pending = 0
	}
	return counts, nil
}
```

7. `enqueue.go` `Stats`: use `counts.failed` from `queueCounts`; delete its separate `countRows(ctx, r.db, "failed_items")` call.
8. Test fixture fallout: in the four engine test files, replace `sql.Open("duckdb", ...)` with `queuedb.Open(...)`, drop go-duckdb imports, remove `cast(? as ubigint)` / `::varchar` from raw-SQL test helpers, and rename queue-path fixtures `*.duckdb` → `*.sqlite` (including the lingering `norway_brreg.duckdb` names and the `TestRuntimeLoadsProcessesAndUploadsBRREGQueue` test name → `TestRuntimeProcessesAndFlushesQueue`, both flagged in earlier reviews).

- [ ] **Step 4: Run the full suite including race**

Run: `go build ./... && go test ./internal/engine/ -v 2>&1 | tail -8 && go test -race ./internal/engine/ -run TestConcurrent -v`
Expected: all PASS; the concurrency smoke passes under `-race` with zero SQLITE_BUSY errors.

- [ ] **Step 5: Format, vet, commit**

```bash
go fmt ./... && go vet ./...
git add internal/engine/
git commit -m "feat(translator): engine on sqlite — arithmetic pending count, EXISTS flush delete"
```

---

### Task 4: Config, Makefile, dependency removal, README, live verification

**Files:**
- Modify: `internal/config/config.go` (default path), `internal/config/config_test.go`
- Modify: `config/translator.json`
- Modify: `Makefile`
- Modify: `go.mod`/`go.sum` (tidy)
- Modify: `README.md`

**Interfaces:**
- Consumes: everything above. Produces: CGO-free module.

- [ ] **Step 1: Config default**

`internal/config/config.go` `applyDefaults`: `data/translator/queue.duckdb` → `data/translator/queue.sqlite`. Update the corresponding assertion in `config_test.go` (`TestLoadAppliesQueueDefaultsAndSoleEndpointFallback`) and the `queue.path` value in `config/translator.json`.

- [ ] **Step 2: Dependency removal and Makefile**

```bash
go mod tidy   # go-duckdb and the duckdb-go-bindings indirects must disappear
rg -n 'duckdb' go.mod go.sum internal/ cmd/ && echo "STRAGGLERS FOUND" || echo "clean"
```

Expected: clean (no Go-code or module references remain).

`Makefile`: change `CGO_ENABLED := 1` to `CGO_ENABLED := 0`. Then verify the race detector works without CGO on this platform (supported since Go 1.20 on darwin/arm64 and linux/amd64):

```bash
CGO_ENABLED=0 go test -race ./internal/engine/ -run TestConcurrent 2>&1 | tail -2
```

Expected: PASS. If instead it errors with "-race requires cgo", add a dedicated Makefile variable for the test target only (`test: CGO_ENABLED=1`-style override) and note it in the README — do not silently drop `-race`.

- [ ] **Step 3: README**

Update `README.md`: every DuckDB queue mention becomes SQLite (`data/translator/queue.sqlite`); note the WAL `-wal`/`-shm` sidecar files; extend the deploy/ops note that old `queue.duckdb` / `norway_brreg.duckdb` files can be deleted (queue is transient; pending work is re-discovered by the loaders' anti-join); note the pure-Go build (`CGO_ENABLED=0`). Read the whole README after editing and fix any statement the storage swap made false.

- [ ] **Step 4: Full verification + live integration**

```bash
go build ./... && go fmt ./... && go vet ./... && go test ./... && go test -race ./internal/...
TRANSLATOR_INTEGRATION_TESTS=true TRANSLATOR_CONFIG_FILE=$PWD/config/translator.json go test ./internal/engine/ -run WithExistingClickHouse -v -count=1
```

For the integration run, ClickHouse credentials come from the environment (see `corpscout/dagster_v3/.env` in the main checkout for `CLICKHOUSE_*`). Expected: enqueue→process→flush passes against real ClickHouse with the SQLite queue. A skip is acceptable ONLY if ClickHouse is genuinely unreachable — report which happened.

- [ ] **Step 5: Commit**

```bash
git add internal/config/ config/ Makefile go.mod go.sum README.md
git commit -m "feat(translator): sqlite queue cutover — CGO-free build, config and docs"
```

---

## Deployment note (not a code task)

Restart translator-api after deploy; it creates `data/translator/queue.sqlite` on boot. Any pending items in the abandoned `queue.duckdb` are re-enqueued by the next loader run (Latvia has ~46k pending from the E2E — run `latvia_ur_translation_load` once after deploy to re-seed and let the workflow drain it). Old `.duckdb` files can then be deleted.
