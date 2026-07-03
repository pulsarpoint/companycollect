# Translator Loader/Service Split Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split extraction from translation — per-source Python loader assets in dagster_v3 feed a source-agnostic Go translator service (shared queue → LLM → periodic flush), and the legacy Python translator package is retired.

**Architecture:** The Go service (`corpscout/translator`) loses all per-source knowledge: one shared DuckDB queue, a bulk-enqueue HTTP API that signal-with-starts one Temporal workflow (`translator/process`), batches grouped by language pair, flush to `text_translations` every N batches and at queue-empty (flush deletes flushed rows). Loaders are dagster assets in `corpscout/dagster_v3` (Norway + Latvia) that anti-join scan ClickHouse and POST item batches; static-map columns are inserted to `text_translations` directly by the loader. The legacy Python translator package (`corpscout/dagster_v3/translator/`) is deleted after an end-to-end smoke passes.

**Tech Stack:** Go 1.24+ (Temporal SDK, clickhouse-go/v2, go-duckdb/v2), Python 3.14 (dagster, clickhouse-connect via `ClickhouseResource`, dlt requests helper), uv, pytest.

**Spec:** `corpscout/docs/superpowers/specs/2026-07-03-translator-loader-service-split-design.md` (including the retirement addendum)

## Global Constraints

- Go commands run from `corpscout/translator/`; Python commands run from `corpscout/dagster_v3/` with `uv run` (never bare `pytest`/`dg`).
- New Temporal identity (fixed, source-independent): workflow ID `translator/process`, task queue `translator-process`, workflow type `TranslationWorkflow`, activities `translator.ProcessOneBatch` and `translator.FlushOutput`, signal `new-items`.
- Enqueue dedup key everywhere: `(source_table, source_column, source_text_hash, source_lang, target_lang)`. `source_text_hash` is a decimal **string** in all JSON (uint64 exceeds JSON safe integers) and is computed only in ClickHouse SQL (`cityHash64`).
- Enqueue limit: 10,000 items per request. Flush cadence default: every 10 batches and at queue-empty. Queue file default: `data/translator/queue.duckdb` (new file; the old per-source file is abandoned, never migrated).
- Activity retry policy (ported Python lesson): unlimited attempts (`MaximumAttempts` unset = 0), `InitialInterval` 1s, `BackoffCoefficient` 2, `MaximumInterval` 5m. Deterministic bad model output still terminates into `failed_items` via the existing recovery ladder — never via activity failure.
- `corpscout.text_translations` insert format unchanged. `output_items`/`failed_items` schemas unchanged; `input_items` gains `source_language_name`/`target_language_name` (both `text not null`).
- Python HTTP calls use `dlt.sources.helpers.requests` (repo convention — NOT plain `requests`).
- Conventional Commits; Go: `go fmt ./... && go vet ./...` before each commit; Python: `uv run ruff check` clean.
- Python deletion (Task 11) happens only after the E2E smoke in Task 11 Step 1 passes.
- Intentionally-red window: none — each task leaves `go build ./... && go test ./...` (translator) and `uv run pytest` (dagster_v3) green. Tasks are ordered so replacements land before deletions.

---

### Task 1: Config — queue block replaces sources

**Files:**
- Modify: `internal/config/config.go`
- Modify: `internal/config/config_test.go`
- Modify: `config/translator.json`

**Interfaces:**
- Produces: `type QueueConfig struct {Path string \`json:"path"\`; FlushEveryBatches int \`json:"flush_every_batches"\`}`; `Config.Queue QueueConfig`; `Config.EndpointID string \`json:"endpoint_id"\``; `SourceConfig` type and `Config.Sources` field DELETED. Defaults: `Queue.Path` = `data/translator/queue.duckdb`, `Queue.FlushEveryBatches` = 10; `EndpointID` defaults to the sole key of `Endpoints` when exactly one is configured.
- Consumes: nothing.

- [ ] **Step 1: Write the failing tests**

Add to `internal/config/config_test.go` (delete `TestLoadParsesSourceDefinitionPath` in the same edit — `SourceConfig` is going away):

```go
func TestLoadParsesQueueConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "translator.json")
	content := `{
  "queue": {"path": "data/translator/custom.duckdb", "flush_every_batches": 5},
  "endpoint_id": "local_llm"
}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Queue.Path != "data/translator/custom.duckdb" {
		t.Fatalf("expected queue path, got %q", cfg.Queue.Path)
	}
	if cfg.Queue.FlushEveryBatches != 5 {
		t.Fatalf("expected flush_every_batches 5, got %d", cfg.Queue.FlushEveryBatches)
	}
	if cfg.EndpointID != "local_llm" {
		t.Fatalf("expected endpoint_id local_llm, got %q", cfg.EndpointID)
	}
}

func TestLoadAppliesQueueDefaultsAndSoleEndpointFallback(t *testing.T) {
	path := filepath.Join(t.TempDir(), "translator.json")
	content := `{
  "endpoints": {"local_llm": {"model": "qwen3:6b", "base_url": "http://x/v1"}}
}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Queue.Path != "data/translator/queue.duckdb" {
		t.Fatalf("expected default queue path, got %q", cfg.Queue.Path)
	}
	if cfg.Queue.FlushEveryBatches != 10 {
		t.Fatalf("expected default flush_every_batches 10, got %d", cfg.Queue.FlushEveryBatches)
	}
	if cfg.EndpointID != "local_llm" {
		t.Fatalf("expected sole-endpoint fallback, got %q", cfg.EndpointID)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/config/ -run 'TestLoadParsesQueueConfig|TestLoadAppliesQueueDefaults' -v`
Expected: FAIL to compile — `cfg.Queue undefined`.

- [ ] **Step 3: Modify config.go**

In `internal/config/config.go`:

1. Replace the `Sources map[string]SourceConfig \`json:"sources"\`` field on `Config` with:

```go
	Queue      QueueConfig `json:"queue"`
	EndpointID string      `json:"endpoint_id"`
```

2. Delete the `SourceConfig` type. Add:

```go
// QueueConfig locates the shared translation queue and sets the flush cadence.
type QueueConfig struct {
	Path              string `json:"path"`
	FlushEveryBatches int    `json:"flush_every_batches"`
}
```

3. In `applyDefaults`, delete the `cfg.Sources == nil` block and add:

```go
	if cfg.Queue.Path == "" {
		cfg.Queue.Path = "data/translator/queue.duckdb"
	}
	if cfg.Queue.FlushEveryBatches <= 0 {
		cfg.Queue.FlushEveryBatches = 10
	}
	if cfg.EndpointID == "" && len(cfg.Endpoints) == 1 {
		for name := range cfg.Endpoints {
			cfg.EndpointID = name
		}
	}
```

(Order note: `applyDefaults` already runs after unmarshal; the endpoint fallback must come AFTER the `cfg.Endpoints == nil` default block.)

4. Update `config/translator.json`: delete the whole `"sources"` block, add at top level:

```json
  "queue": {
    "path": "data/translator/queue.duckdb",
    "flush_every_batches": 10
  },
  "endpoint_id": "local_llm",
```

Everything else (server/clickhouse/temporal/endpoints) unchanged.

- [ ] **Step 4: Bridge the cmd binaries, run tests, verify**

`cmd/translator-api/main.go` and `cmd/translator-trigger/main.go` reference `cfg.Sources`, which no longer exists. To keep `go build ./...` green (Global Constraint) without pulling Task 8's full rewiring into this task, apply a minimal compile bridge now — Task 8 replaces it wholesale, so the bridge only needs to compile and refuse to start, not to work:

In `cmd/translator-api/main.go`, replace the `for name, sourceConfig := range cfg.Sources { ... }` loop and the `sourceSetup` function with:

```go
	if cfg.EndpointID == "" {
		logger.Error("endpoint_id is required")
		os.Exit(1)
	}
	endpointConfig, ok := cfg.Endpoints[cfg.EndpointID]
	if !ok {
		logger.Error("endpoint not found", "endpoint_id", cfg.EndpointID)
		os.Exit(1)
	}
	_ = endpointConfig
	logger.Error("translator-api is mid-migration to the shared-queue architecture (plan task 8); refusing to start")
	os.Exit(1)
```

Delete now-unused imports/helpers flagged by the compiler (`sourceSetup`, per-source worker/runtime/provider wiring, `sourceNames` router/starter construction — replace the router/starter construction with `_ = temporalClient` as needed to keep compilation; the whole main is rewritten in Task 8). `cmd/translator-trigger` does not reference `cfg.Sources`... it does — `newTemporalStarter` iterates `cfg.Sources` for source names. Bridge it the same way: pass `nil` sources (`orchestration.NewTemporalWorkflowStarter(temporalClient, nil, ...)`); the trigger becomes temporarily unable to start anything real (Task 8 rewrites it). Update `cmd/translator-trigger/main_test.go` only if compilation requires it.

Run: `go build ./... && go test ./internal/... ./cmd/... 2>&1 | tail -12`
Expected: builds green; config tests PASS; trigger tests may need `-source` default assertions untouched (behavior unchanged: flag parsing and action validation still work).

- [ ] **Step 5: Format, vet, commit**

```bash
go fmt ./... && go vet ./...
git add internal/config/ config/translator.json cmd/
git commit -m "feat(translator): queue config block replaces per-source config"
```

---

### Task 2: Queue — language-name columns and single-pair batches

**Files:**
- Modify: `internal/engine/load.go` (queue DDL + upsert)
- Modify: `internal/queue/queue.go`
- Modify: `internal/queue/queue_test.go`
- Modify: `internal/engine/load_test.go` (fixture fallout)

**Interfaces:**
- Consumes: existing `queue.Item`, `engine.InputItem`.
- Produces: `engine.InputItem` gains `SourceLanguageName, TargetLanguageName string`; `input_items` DDL gains `source_language_name text not null` and `target_language_name text not null` (after `target_lang`, before `created_at`); `upsertInputItems` writes them; `queue.Item` gains the same two fields; `queue.Queue.GetBatch` returns items from exactly ONE language pair per call (the pair containing the oldest `created_at` among pending items) including the name fields; `queue.validateSchema` requires the new columns.

- [ ] **Step 1: Write the failing tests**

Add to `internal/queue/queue_test.go` (reuse the file's existing helpers for creating a DuckDB with the queue schema — read the file first; it creates tables itself, so extend its DDL helper with the two new columns):

```go
func TestGetBatchReturnsSingleLanguagePairOldestFirst(t *testing.T) {
	ctx := context.Background()
	db := openTestQueue(t) // existing helper; update its input_items DDL with the two name columns
	q, err := New(db)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}

	// Latvian item inserted FIRST (oldest created_at), then Norwegian items.
	insertTestInput(t, db, "corpscout.lv_companies", "activity_text_original", "Latviešu teksts", 1, "lv", "en", "Latvian", "English")
	insertTestInput(t, db, "corpscout.no_companies", "activity_text_original", "Norsk tekst A", 2, "no", "en", "Norwegian", "English")
	insertTestInput(t, db, "corpscout.no_companies", "activity_text_original", "Norsk tekst B", 3, "no", "en", "Norwegian", "English")

	batch, err := q.GetBatch(ctx, 10)
	if err != nil {
		t.Fatalf("get batch: %v", err)
	}
	if len(batch) != 1 {
		t.Fatalf("expected only the oldest pair's items (1 Latvian), got %d items", len(batch))
	}
	if batch[0].SourceLang != "lv" || batch[0].SourceLanguageName != "Latvian" || batch[0].TargetLanguageName != "English" {
		t.Fatalf("expected Latvian item with names, got %+v", batch[0])
	}
}
```

`insertTestInput` (add as a helper; created_at ordering via explicit timestamps):

```go
var testInputSeq int

func insertTestInput(t *testing.T, db *sql.DB, table, column, text string, hash uint64, srcLang, dstLang, srcName, dstName string) {
	t.Helper()
	testInputSeq++
	if _, err := db.Exec(`
		insert into input_items (
			source_table, source_column, source_text, source_text_hash,
			source_lang, target_lang, source_language_name, target_language_name, created_at
		) values (?, ?, ?, cast(? as ubigint), ?, ?, ?, ?, timestamp '2026-01-01 00:00:00' + to_seconds(?))
	`, table, column, text, strconv.FormatUint(hash, 10), srcLang, dstLang, srcName, dstName, testInputSeq); err != nil {
		t.Fatalf("insert test input: %v", err)
	}
}
```

Also update every existing insert/DDL in `queue_test.go` to include the two new not-null columns (values `'Norwegian','English'` where the tests use no→en), otherwise the schema validation and inserts fail.

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/queue/ -v 2>&1 | head -20`
Expected: FAIL — schema validation rejects missing columns / `SourceLanguageName` undefined.

- [ ] **Step 3: Implement**

1. `internal/engine/load.go` — in `createQueueTables`, add to the `input_items` DDL after `target_lang text not null,`:

```
				source_language_name text not null,
				target_language_name text not null,
```

(primary key unchanged; `output_items`/`failed_items` untouched.)

2. `internal/engine/load.go` — `InputItem` (in `clickhouse.go`, where the type lives) gains:

```go
	SourceLanguageName string
	TargetLanguageName string
```

and `upsertInputItems`'s INSERT adds the two columns/placeholders (values `row.SourceLanguageName, row.TargetLanguageName`) with `validateInput` requiring both non-empty:

```go
	case row.SourceLanguageName == "":
		return errors.New("source_language_name is required")
	case row.TargetLanguageName == "":
		return errors.New("target_language_name is required")
```

3. `internal/queue/queue.go`:
   - `Item` gains `SourceLanguageName, TargetLanguageName string`.
   - `validateSchema` `input_items` required columns gain the two names.
   - `GetBatch` becomes two queries. First pick the pair:

```go
	var srcLang, dstLang string
	err := q.db.QueryRowContext(ctx, `
		select i.source_lang, i.target_lang
		from input_items as i
		where not exists (
			select 1 from output_items as o
			where o.source_table = i.source_table and o.source_column = i.source_column
				and o.source_text_hash = i.source_text_hash
				and o.source_lang = i.source_lang and o.target_lang = i.target_lang
		) and not exists (
			select 1 from failed_items as f
			where f.source_table = i.source_table and f.source_column = i.source_column
				and f.source_text_hash = i.source_text_hash
				and f.source_lang = i.source_lang and f.target_lang = i.target_lang
		)
		order by i.created_at, i.source_lang, i.target_lang
		limit 1
	`).Scan(&srcLang, &dstLang)
	if errors.Is(err, sql.ErrNoRows) {
		return []Item{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("pick queue language pair: %w", err)
	}
```

   Then the existing batch query gains `and i.source_lang = ? and i.target_lang = ?` in its WHERE, selects the two extra name columns, and scans them into the item. Existing ORDER BY and `itemID` unchanged.

4. `internal/engine/load_test.go` fallout: `fixtureInputItem` gains `SourceLanguageName: "Norwegian", TargetLanguageName: "English"`; the "malformed rows" duckDB assertion in the integration test is unaffected (names aren't part of the key). `internal/engine/integration_test.go` compiles against `LoadInput` still — untouched in this task (deleted in Task 9).

Wait — `LoadInput`'s scan path produces `InputItem`s WITHOUT names (the ClickHouse scan returns 6 columns), which now fail `validateInput`. Bridge: in `loadInputWithDB`, after `rows, err := source.QueryTranslationInput(...)`, set the names from the definition before upserting:

```go
		for i := range rows {
			rows[i].SourceLanguageName = def.SourceLanguageName
			rows[i].TargetLanguageName = def.TargetLanguageName
		}
```

(The whole scan path is deleted in Task 9; this keeps it green until then.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `go build ./... && go test ./internal/queue/ ./internal/engine/ -v 2>&1 | tail -8`
Expected: PASS (engine tests green with updated fixtures; new pair-grouping test green).

- [ ] **Step 5: Format, vet, commit**

```bash
go fmt ./... && go vet ./...
git add internal/queue/ internal/engine/
git commit -m "feat(translator): language-name columns and single-pair queue batches"
```

---

### Task 3: Translation provider — per-call prompt languages

**Files:**
- Modify: `internal/translation/provider.go`
- Modify: `internal/translation/types.go`
- Modify: `internal/translation/batch.go`
- Modify: `internal/translation/provider_test.go`, `internal/translation/batch_test.go`, `internal/translation/provider_integration_test.go`
- Modify: `internal/engine/runtime.go` (call-site fallout)

**Interfaces:**
- Produces: `Translator` interface becomes `Translate(ctx context.Context, items []TranslationInput, timeoutSeconds int, promptData PromptData) ([]TranslationResult, error)`; `translation.Config` loses `PromptData`; `Init` no longer validates prompt languages (validated per call: both names required, error otherwise); `TranslateItems(ctx, translator, items, timeoutSeconds, provider, model string, promptData PromptData)` threads it through (including the recursive split calls).
- Consumes: nothing new.

- [ ] **Step 1: Update the tests first**

In `internal/translation/provider_test.go` and `batch_test.go` (read them first): every `Translate(` call and every fake-translator method gains the `promptData PromptData` parameter; every `translation.Config{...}` literal loses `PromptData`; add one new test:

```go
func TestTranslateRequiresPromptLanguages(t *testing.T) {
	provider, err := Init(Config{BaseURL: "http://localhost:1", Model: "m", APIKey: "k"})
	if err != nil {
		t.Fatalf("init provider: %v", err)
	}
	_, err = provider.Translate(context.Background(), []TranslationInput{{ItemID: "1", SourceText: "x", SourceLang: "no", TargetLang: "en"}}, 1, PromptData{})
	if err == nil {
		t.Fatal("expected error for empty prompt languages")
	}
}
```

In `provider_integration_test.go`, move the `"Norwegian"/"English"` literals from the `Config` literal into the `Translate(...)` call's `PromptData` argument.

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/translation/ 2>&1 | head -8`
Expected: FAIL to compile.

- [ ] **Step 3: Implement**

1. `types.go`: `Translator` interface gains the 4th parameter as above.
2. `provider.go`:
   - Delete `PromptData` from `Config` (keep the `PromptData` type itself — it's now the per-call argument).
   - Delete the two prompt-language validations from `Init` and the `promptData` field from `LocalOpenAICompatibleProvider`; the logger `.With` drops the two language attributes.
   - `renderPrompt(items []TranslationInput, promptData PromptData)` uses the argument; `Translate` gains the parameter, validates first:

```go
	if strings.TrimSpace(promptData.SourceLanguage) == "" || strings.TrimSpace(promptData.TargetLanguage) == "" {
		return nil, errors.New("prompt source and target languages are required")
	}
```

   and logs `"source_language", promptData.SourceLanguage, "target_language", promptData.TargetLanguage` on the request-started line.
3. `batch.go`: `TranslateItems` and `translateItemsOnce` gain `promptData PromptData` (last parameter) and pass it to `translator.Translate` and both recursive `TranslateItems` calls.
4. `internal/engine/runtime.go` fallout: `ProcessOneBatch` currently calls `translation.TranslateItems(ctx, r.translator, items, input.TimeoutSeconds, r.providerName, r.model)`. Bridge with the batch's names (batch is single-pair as of Task 2):

```go
			promptData := translation.PromptData{
				SourceLanguage: items[0].SourceLanguageName,
				TargetLanguage: items[0].TargetLanguageName,
			}
			output, failed, err := translation.TranslateItems(
				ctx, r.translator, items, input.TimeoutSeconds, r.providerName, r.model, promptData,
			)
```

   `internal/engine/runtime_test.go` fakes implementing `translation.Translator` gain the parameter. `cmd/translator-api` bridge from Task 1 already doesn't construct a provider (it exits early), but if the bridge still contains `translation.Init(... PromptData ...)`, delete that field there too.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go build ./... && go test ./internal/translation/ ./internal/engine/ -v 2>&1 | tail -6`
Expected: PASS.

- [ ] **Step 5: Format, vet, commit**

```bash
go fmt ./... && go vet ./...
git add internal/translation/ internal/engine/ cmd/
git commit -m "feat(translator): per-call prompt languages in translation provider"
```

---

### Task 4: Engine — enqueue, stats, and flush on the runtime

**Files:**
- Create: `internal/engine/enqueue.go`
- Test: `internal/engine/enqueue_test.go`
- Modify: `internal/engine/runtime.go`
- Modify: `internal/engine/runtime_test.go`

**Interfaces:**
- Produces:

```go
const MaxEnqueueItems = 10000

type EnqueueItem struct {
	SourceTable    string `json:"source_table"`
	SourceColumn   string `json:"source_column"`
	SourceText     string `json:"source_text"`
	SourceTextHash string `json:"source_text_hash"` // decimal uint64
}

type EnqueueRequest struct {
	SourceLang         string        `json:"source_lang"`
	TargetLang         string        `json:"target_lang"`
	SourceLanguageName string        `json:"source_language_name"`
	TargetLanguageName string        `json:"target_language_name"`
	Items              []EnqueueItem `json:"items"`
}

type EnqueueResult struct {
	Received int `json:"received"`
	Inserted int `json:"inserted"`
}

type QueueStats struct {
	Input   int `json:"input"`
	Pending int `json:"pending"`
	Output  int `json:"output"`
	Failed  int `json:"failed"`
}

func (r EnqueueRequest) Validate() error                       // structured errors: "items[3]: source_text is required" etc.
func (rt *Runtime) Enqueue(ctx context.Context, req EnqueueRequest) (EnqueueResult, error)
func (rt *Runtime) Stats(ctx context.Context) (QueueStats, error)
func (rt *Runtime) FlushOutput(ctx context.Context) (UploadResult, error) // replaces UploadOutput: insert to ClickHouse, then delete flushed output+input rows
```

- Consumes: Task 2's `InputItem` name fields and `upsertInputItems`; existing `outputTranslations`, `queueCounts`, `InsertTextTranslations`.

- [ ] **Step 1: Write the failing tests**

Create `internal/engine/enqueue_test.go`:

```go
package engine

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
)

func validEnqueueRequest(n int) EnqueueRequest {
	items := make([]EnqueueItem, 0, n)
	for i := 0; i < n; i++ {
		items = append(items, EnqueueItem{
			SourceTable:    "corpscout.no_companies",
			SourceColumn:   "activity_text_original",
			SourceText:     "tekst",
			SourceTextHash: "18446744073709551615",
		})
	}
	return EnqueueRequest{
		SourceLang: "no", TargetLang: "en",
		SourceLanguageName: "Norwegian", TargetLanguageName: "English",
		Items: items,
	}
}

func TestEnqueueRequestValidate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*EnqueueRequest)
		wantErr string
	}{
		{"missing source_lang", func(r *EnqueueRequest) { r.SourceLang = "" }, "source_lang is required"},
		{"missing target_lang", func(r *EnqueueRequest) { r.TargetLang = "" }, "target_lang is required"},
		{"missing source_language_name", func(r *EnqueueRequest) { r.SourceLanguageName = "" }, "source_language_name is required"},
		{"missing target_language_name", func(r *EnqueueRequest) { r.TargetLanguageName = "" }, "target_language_name is required"},
		{"no items", func(r *EnqueueRequest) { r.Items = nil }, "at least one item is required"},
		{"too many items", func(r *EnqueueRequest) { r.Items = validEnqueueRequest(MaxEnqueueItems + 1).Items }, "at most 10000 items"},
		{"item missing table", func(r *EnqueueRequest) { r.Items[0].SourceTable = "" }, "items[0]: source_table is required"},
		{"item missing column", func(r *EnqueueRequest) { r.Items[0].SourceColumn = "" }, "items[0]: source_column is required"},
		{"item missing text", func(r *EnqueueRequest) { r.Items[0].SourceText = "" }, "items[0]: source_text is required"},
		{"item bad hash", func(r *EnqueueRequest) { r.Items[0].SourceTextHash = "not-a-number" }, "items[0]: source_text_hash"},
		{"item hash overflow", func(r *EnqueueRequest) { r.Items[0].SourceTextHash = "18446744073709551616" }, "items[0]: source_text_hash"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := validEnqueueRequest(1)
			tt.mutate(&req)
			err := req.Validate()
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("expected error containing %q, got %v", tt.wantErr, err)
			}
		})
	}
	if err := validEnqueueRequest(2).Validate(); err != nil {
		t.Fatalf("valid request must pass: %v", err)
	}
}

func TestEnqueueUpsertsAndCountsInserted(t *testing.T) {
	ctx := context.Background()
	rt := newEnqueueTestRuntime(t)

	req := validEnqueueRequest(1)
	req.Items = append(req.Items, EnqueueItem{
		SourceTable: "corpscout.no_companies", SourceColumn: "activity_text_original",
		SourceText: "annen tekst", SourceTextHash: "42",
	})

	first, err := rt.Enqueue(ctx, req)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if first.Received != 2 || first.Inserted != 2 {
		t.Fatalf("expected 2/2, got %+v", first)
	}

	second, err := rt.Enqueue(ctx, req)
	if err != nil {
		t.Fatalf("re-enqueue: %v", err)
	}
	if second.Received != 2 || second.Inserted != 0 {
		t.Fatalf("expected duplicate enqueue 2/0, got %+v", second)
	}

	stats, err := rt.Stats(ctx)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.Input != 2 || stats.Pending != 2 || stats.Output != 0 || stats.Failed != 0 {
		t.Fatalf("unexpected stats %+v", stats)
	}
}
```

`newEnqueueTestRuntime` builds a Runtime against a temp queue file with fakes (mirror `runtime_test.go`'s construction — reuse its fake ClickHouse source and translator; read that file and follow its helper pattern; if it has a `newTestRuntime`-style helper, reuse it instead of writing a new one).

Add a flush test to `internal/engine/runtime_test.go`:

```go
func TestFlushOutputInsertsAndDeletesFlushedRows(t *testing.T) {
	// Arrange: runtime with fake ClickHouse source; enqueue 2 items; run ProcessOneBatch
	// with a fake translator that succeeds → 2 output rows.
	// Act: FlushOutput.
	// Assert: fake source received 2 TextTranslations; output_items count == 0;
	// input_items count == 0 (matched inputs deleted); failed_items untouched;
	// second FlushOutput returns zero rows and no error.
}
```

Write it fully against the existing fakes (the fake ClickHouse source already records `insertedTranslations`; counts via direct `select count(*)` on the runtime's db — expose via the test being in package engine with access to `r.db`).

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/engine/ -run 'TestEnqueue|TestFlushOutput' -v 2>&1 | head -8`
Expected: FAIL to compile — `EnqueueRequest` undefined, `FlushOutput` undefined.

- [ ] **Step 3: Implement**

Create `internal/engine/enqueue.go`:

```go
package engine

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// MaxEnqueueItems caps one enqueue request; loaders chunk larger loads.
const MaxEnqueueItems = 10000

type EnqueueItem struct {
	SourceTable    string `json:"source_table"`
	SourceColumn   string `json:"source_column"`
	SourceText     string `json:"source_text"`
	SourceTextHash string `json:"source_text_hash"`
}

type EnqueueRequest struct {
	SourceLang         string        `json:"source_lang"`
	TargetLang         string        `json:"target_lang"`
	SourceLanguageName string        `json:"source_language_name"`
	TargetLanguageName string        `json:"target_language_name"`
	Items              []EnqueueItem `json:"items"`
}

type EnqueueResult struct {
	Received int `json:"received"`
	Inserted int `json:"inserted"`
}

type QueueStats struct {
	Input   int `json:"input"`
	Pending int `json:"pending"`
	Output  int `json:"output"`
	Failed  int `json:"failed"`
}

// Validate checks the whole request before any write; the request is
// all-or-nothing.
func (r EnqueueRequest) Validate() error {
	switch {
	case strings.TrimSpace(r.SourceLang) == "":
		return errors.New("source_lang is required")
	case strings.TrimSpace(r.TargetLang) == "":
		return errors.New("target_lang is required")
	case strings.TrimSpace(r.SourceLanguageName) == "":
		return errors.New("source_language_name is required")
	case strings.TrimSpace(r.TargetLanguageName) == "":
		return errors.New("target_language_name is required")
	case len(r.Items) == 0:
		return errors.New("at least one item is required")
	case len(r.Items) > MaxEnqueueItems:
		return fmt.Errorf("at most %d items per request, got %d", MaxEnqueueItems, len(r.Items))
	}

	for i, item := range r.Items {
		if strings.TrimSpace(item.SourceTable) == "" {
			return fmt.Errorf("items[%d]: source_table is required", i)
		}
		if strings.TrimSpace(item.SourceColumn) == "" {
			return fmt.Errorf("items[%d]: source_column is required", i)
		}
		if item.SourceText == "" {
			return fmt.Errorf("items[%d]: source_text is required", i)
		}
		if _, err := strconv.ParseUint(item.SourceTextHash, 10, 64); err != nil {
			return fmt.Errorf("items[%d]: source_text_hash must be a decimal uint64: %v", i, err)
		}
	}
	return nil
}

// Enqueue validates and upserts one loader batch into the queue. Inserted
// reports rows actually added (duplicates by dedup key are skipped).
func (r *Runtime) Enqueue(ctx context.Context, req EnqueueRequest) (EnqueueResult, error) {
	if r.closed {
		return EnqueueResult{}, errors.New("translator runtime is closed")
	}
	if err := req.Validate(); err != nil {
		return EnqueueResult{}, err
	}

	rows := make([]InputItem, 0, len(req.Items))
	for _, item := range req.Items {
		hash, err := strconv.ParseUint(item.SourceTextHash, 10, 64)
		if err != nil {
			return EnqueueResult{}, fmt.Errorf("parse source_text_hash %q: %w", item.SourceTextHash, err)
		}
		rows = append(rows, InputItem{
			SourceTable:        item.SourceTable,
			SourceColumn:       item.SourceColumn,
			SourceText:         item.SourceText,
			SourceTextHash:     hash,
			SourceLang:         req.SourceLang,
			TargetLang:         req.TargetLang,
			SourceLanguageName: req.SourceLanguageName,
			TargetLanguageName: req.TargetLanguageName,
		})
	}

	before, err := countRows(ctx, r.db, "input_items")
	if err != nil {
		return EnqueueResult{}, err
	}
	if err := upsertInputItems(ctx, r.db, rows); err != nil {
		return EnqueueResult{}, err
	}
	after, err := countRows(ctx, r.db, "input_items")
	if err != nil {
		return EnqueueResult{}, err
	}
	return EnqueueResult{Received: len(req.Items), Inserted: after - before}, nil
}

// Stats reports queue table counts; Pending uses the same definition the
// batch loop uses.
func (r *Runtime) Stats(ctx context.Context) (QueueStats, error) {
	if r.closed {
		return QueueStats{}, errors.New("translator runtime is closed")
	}
	counts, err := r.queueCounts(ctx)
	if err != nil {
		return QueueStats{}, err
	}
	failed, err := countRows(ctx, r.db, "failed_items")
	if err != nil {
		return QueueStats{}, err
	}
	return QueueStats{Input: counts.input, Pending: counts.pending, Output: counts.output, Failed: failed}, nil
}
```

Modify `internal/engine/runtime.go`: rename `UploadOutput` to `FlushOutput` and, after a successful `InsertTextTranslations`, delete the flushed rows (the workflow is the only output writer, so deleting all outputs after a successful insert of all outputs is exact):

```go
	if _, err := r.db.ExecContext(ctx, `
		delete from input_items
		where (source_table, source_column, source_text_hash, source_lang, target_lang) in (
			select source_table, source_column, source_text_hash, source_lang, target_lang
			from output_items
		)
	`); err != nil {
		return UploadResult{RowsSeen: len(translations), RowsInserted: inserted},
			fmt.Errorf("delete flushed input rows: %w", err)
	}
	if _, err := r.db.ExecContext(ctx, `delete from output_items`); err != nil {
		return UploadResult{RowsSeen: len(translations), RowsInserted: inserted},
			fmt.Errorf("delete flushed output rows: %w", err)
	}
```

(Delete inputs FIRST while outputs still exist to join against, then outputs.) Log messages: "upload output" → "flush output". Update `runtime_test.go` call sites (`UploadOutput` → `FlushOutput`).

- [ ] **Step 4: Run tests to verify they pass**

Run: `go build ./... && go test ./internal/engine/ -v 2>&1 | tail -6`
Expected: PASS, including the new flush-deletes test and duplicate-enqueue test.

- [ ] **Step 5: Format, vet, commit**

```bash
go fmt ./... && go vet ./...
git add internal/engine/
git commit -m "feat(translator): runtime enqueue, stats, and flush-with-delete"
```

---

### Task 5: Workflow — single process loop with flush cadence

**Files:**
- Rewrite: `internal/engine/workflow.go`
- Rewrite: `internal/engine/workflow_test.go`

**Interfaces:**
- Produces:

```go
const (
	ProcessWorkflowID = "translator/process"
	ProcessTaskQueue  = "translator-process"
	SignalNewItems    = "new-items"

	ActivityProcessOneBatch = "translator.ProcessOneBatch"
	ActivityFlushOutput     = "translator.FlushOutput"
)

const DefaultBatchesPerRun = 500

type WorkflowInput struct {
	BatchSize         int
	TimeoutSeconds    int
	BatchesPerRun     int
	FlushEveryBatches int
}

func TranslationWorkflow(ctx workflow.Context, input WorkflowInput) error
```

- Consumes: `ProcessInput`, `ProcessResult`, `UploadResult` (unchanged shapes from runtime.go).
- Deleted: `WorkflowID(source)`, `TaskQueue(source)`, `ActivityLoadNewInput/ProcessOneBatch/UploadOutput(source)`, `SourceActionSignal`, `SignalSourceAction`, action constants, `LoadResult`-typed activity references in the workflow.

- [ ] **Step 1: Rewrite workflow.go**

Replace `internal/engine/workflow.go` in full:

```go
package engine

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	// ProcessWorkflowID is the fixed ID of the single translation workflow.
	ProcessWorkflowID = "translator/process"
	// ProcessTaskQueue is the fixed Temporal task queue for the translator.
	ProcessTaskQueue = "translator-process"
	// SignalNewItems wakes (or signal-with-starts) the workflow after an enqueue.
	SignalNewItems = "new-items"

	ActivityProcessOneBatch = "translator.ProcessOneBatch"
	ActivityFlushOutput     = "translator.FlushOutput"
)

const DefaultBatchesPerRun = 500

type WorkflowInput struct {
	BatchSize         int
	TimeoutSeconds    int
	BatchesPerRun     int
	FlushEveryBatches int
}

// TranslationWorkflow drains the shared queue: translate batches (each batch
// is a single language pair), flush translated output to ClickHouse every
// FlushEveryBatches batches and at queue-empty, and continue-as-new after
// BatchesPerRun batches. Transient activity failures retry without an
// attempt cap (deterministic bad model output never fails the activity — it
// lands in failed_items inside ProcessOneBatch).
func TranslationWorkflow(ctx workflow.Context, input WorkflowInput) error {
	if input.BatchSize <= 0 {
		input.BatchSize = 50
	}
	if input.TimeoutSeconds <= 0 {
		input.TimeoutSeconds = 120
	}
	if input.BatchesPerRun <= 0 {
		input.BatchesPerRun = DefaultBatchesPerRun
	}
	if input.FlushEveryBatches <= 0 {
		input.FlushEveryBatches = 10
	}
	logger := workflow.GetLogger(ctx)
	logger.Info(
		"translator workflow started",
		"batch_size", input.BatchSize,
		"timeout_seconds", input.TimeoutSeconds,
		"batches_per_run", input.BatchesPerRun,
		"flush_every_batches", input.FlushEveryBatches,
	)

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    5 * time.Minute,
		},
	})

	signalChannel := workflow.GetSignalChannel(ctx, SignalNewItems)

	flush := func() error {
		var flushResult UploadResult
		logger.Info("translator workflow flushing output")
		if err := workflow.ExecuteActivity(ctx, ActivityFlushOutput).Get(ctx, &flushResult); err != nil {
			return err
		}
		logger.Info(
			"translator workflow flushed output",
			"rows_seen", flushResult.RowsSeen,
			"rows_inserted", flushResult.RowsInserted,
		)
		return nil
	}

	processInput := ProcessInput{
		BatchSize:      input.BatchSize,
		TimeoutSeconds: input.TimeoutSeconds,
	}
	batchesSinceFlush := 0

	for batch := 0; batch < input.BatchesPerRun; batch++ {
		var processResult ProcessResult
		if err := workflow.ExecuteActivity(ctx, ActivityProcessOneBatch, processInput).Get(ctx, &processResult); err != nil {
			return err
		}
		if processResult.TranslatedCount > 0 {
			batchesSinceFlush++
		}
		logger.Info(
			"translator workflow batch processed",
			"batch_index", batch+1,
			"translated_count", processResult.TranslatedCount,
			"pending_count", processResult.PendingCount,
			"output_count", processResult.OutputCount,
		)

		if processResult.PendingCount == 0 {
			if processResult.OutputCount > 0 {
				if err := flush(); err != nil {
					return err
				}
			}
			// The queue is drained and flushed. Any signal received since the
			// last check means an enqueue may have added items after our
			// batch — loop again to re-derive pending from the queue;
			// otherwise complete. Drain extra buffered signals so one more
			// round covers any number of racing enqueues (pending is
			// re-derived from the DB, signals are only wakeups).
			if !signalChannel.ReceiveAsync(nil) {
				logger.Info("translator workflow queue is empty")
				return nil
			}
			for signalChannel.ReceiveAsync(nil) {
			}
			batchesSinceFlush = 0
			continue
		}

		if batchesSinceFlush >= input.FlushEveryBatches {
			if err := flush(); err != nil {
				return err
			}
			batchesSinceFlush = 0
		}
	}

	logger.Info("translator workflow continuing as new", "batches_per_run", input.BatchesPerRun)
	return workflow.NewContinueAsNewError(ctx, TranslationWorkflow, input)
}
```

- [ ] **Step 2: Rewrite workflow_test.go**

Replace `internal/engine/workflow_test.go` in full (testify + Temporal test suite, matching the existing style):

```go
package engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func TestTemporalIdentityConstants(t *testing.T) {
	if ProcessWorkflowID != "translator/process" {
		t.Fatalf("ProcessWorkflowID = %q", ProcessWorkflowID)
	}
	if ProcessTaskQueue != "translator-process" {
		t.Fatalf("ProcessTaskQueue = %q", ProcessTaskQueue)
	}
	if ActivityProcessOneBatch != "translator.ProcessOneBatch" {
		t.Fatalf("ActivityProcessOneBatch = %q", ActivityProcessOneBatch)
	}
	if ActivityFlushOutput != "translator.FlushOutput" {
		t.Fatalf("ActivityFlushOutput = %q", ActivityFlushOutput)
	}
	if SignalNewItems != "new-items" {
		t.Fatalf("SignalNewItems = %q", SignalNewItems)
	}
}

func newWorkflowTestEnv(t *testing.T) *testsuite.TestWorkflowEnvironment {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(TranslationWorkflow)
	env.RegisterActivityWithOptions(
		func(context.Context, ProcessInput) (ProcessResult, error) { return ProcessResult{}, nil },
		activity.RegisterOptions{Name: ActivityProcessOneBatch},
	)
	env.RegisterActivityWithOptions(
		func(context.Context) (UploadResult, error) { return UploadResult{}, nil },
		activity.RegisterOptions{Name: ActivityFlushOutput},
	)
	return env
}

func TestTranslationWorkflowProcessesUntilEmptyThenFlushes(t *testing.T) {
	env := newWorkflowTestEnv(t)
	processInput := ProcessInput{BatchSize: 3, TimeoutSeconds: 30}

	env.OnActivity(ActivityProcessOneBatch, mock.Anything, processInput).
		Return(ProcessResult{TranslatedCount: 3, PendingCount: 2, OutputCount: 3}, nil).Once()
	env.OnActivity(ActivityProcessOneBatch, mock.Anything, processInput).
		Return(ProcessResult{TranslatedCount: 2, PendingCount: 0, OutputCount: 5}, nil).Once()
	env.OnActivity(ActivityFlushOutput, mock.Anything).
		Return(UploadResult{RowsSeen: 5, RowsInserted: 5}, nil).Once()

	env.ExecuteWorkflow(TranslationWorkflow, WorkflowInput{BatchSize: 3, TimeoutSeconds: 30})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func TestTranslationWorkflowCompletesWithoutFlushWhenQueueEmpty(t *testing.T) {
	env := newWorkflowTestEnv(t)
	env.OnActivity(ActivityProcessOneBatch, mock.Anything, mock.Anything).
		Return(ProcessResult{TranslatedCount: 0, PendingCount: 0, OutputCount: 0}, nil).Once()

	env.ExecuteWorkflow(TranslationWorkflow, WorkflowInput{BatchSize: 3, TimeoutSeconds: 30})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func TestTranslationWorkflowFlushesEveryNBatches(t *testing.T) {
	env := newWorkflowTestEnv(t)
	processInput := ProcessInput{BatchSize: 3, TimeoutSeconds: 30}

	// 3 translated batches with FlushEveryBatches=2: flush after batch 2,
	// then queue empties on batch 3 → final flush.
	env.OnActivity(ActivityProcessOneBatch, mock.Anything, processInput).
		Return(ProcessResult{TranslatedCount: 3, PendingCount: 9, OutputCount: 3}, nil).Once()
	env.OnActivity(ActivityProcessOneBatch, mock.Anything, processInput).
		Return(ProcessResult{TranslatedCount: 3, PendingCount: 6, OutputCount: 6}, nil).Once()
	env.OnActivity(ActivityFlushOutput, mock.Anything).
		Return(UploadResult{RowsSeen: 6, RowsInserted: 6}, nil).Once()
	env.OnActivity(ActivityProcessOneBatch, mock.Anything, processInput).
		Return(ProcessResult{TranslatedCount: 3, PendingCount: 0, OutputCount: 3}, nil).Once()
	env.OnActivity(ActivityFlushOutput, mock.Anything).
		Return(UploadResult{RowsSeen: 3, RowsInserted: 3}, nil).Once()

	env.ExecuteWorkflow(TranslationWorkflow, WorkflowInput{BatchSize: 3, TimeoutSeconds: 30, FlushEveryBatches: 2})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func TestTranslationWorkflowContinuesAsNewAfterBatchLimit(t *testing.T) {
	env := newWorkflowTestEnv(t)
	processInput := ProcessInput{BatchSize: 3, TimeoutSeconds: 30}

	env.OnActivity(ActivityProcessOneBatch, mock.Anything, processInput).
		Return(ProcessResult{TranslatedCount: 3, PendingCount: 9, OutputCount: 3}, nil).Twice()

	env.ExecuteWorkflow(TranslationWorkflow, WorkflowInput{
		BatchSize: 3, TimeoutSeconds: 30, BatchesPerRun: 2, FlushEveryBatches: 10,
	})

	require.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	require.Error(t, err)
	require.True(t, workflow.IsContinueAsNewError(err), "expected continue-as-new, got %v", err)
	env.AssertExpectations(t)
}

func TestTranslationWorkflowSignalTriggersAnotherRoundAfterEmpty(t *testing.T) {
	env := newWorkflowTestEnv(t)
	processInput := ProcessInput{BatchSize: 3, TimeoutSeconds: 30}

	env.OnActivity(ActivityProcessOneBatch, mock.Anything, processInput).
		Return(ProcessResult{TranslatedCount: 1, PendingCount: 0, OutputCount: 1}, nil).Once()
	env.OnActivity(ActivityFlushOutput, mock.Anything).
		Return(UploadResult{RowsSeen: 1, RowsInserted: 1}, nil).Once()
	// Signal delivered before the first batch completes → after flush the
	// workflow loops once more instead of completing.
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalNewItems, nil)
	}, 0)
	env.OnActivity(ActivityProcessOneBatch, mock.Anything, processInput).
		Return(ProcessResult{TranslatedCount: 0, PendingCount: 0, OutputCount: 0}, nil).Once()

	env.ExecuteWorkflow(TranslationWorkflow, WorkflowInput{BatchSize: 3, TimeoutSeconds: 30})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}
```

Note for the implementer: the mock framework matches `OnActivity` expectations in registration order for identical arguments; if the signal-race test proves flaky against the drain logic, assert instead on total call counts (`.Times(2)`) — behavior, not ordering, is the requirement. The signal-drain semantics (drain at loop top; re-check after empty+flush) must not change.

- [ ] **Step 3: Run tests**

Run: `go build ./... && go test ./internal/engine/ -run TestTranslation -v`
Expected: PASS (5 workflow tests + identity constants). Note `go build ./...` still green because orchestration/api still reference the OLD helpers... they DO (`engine.WorkflowID` etc. deleted). Fix in the same task: this task's rewrite deletes symbols used by `internal/orchestration`, `internal/api`, and cmd bridges. To stay green, Task 5 and Task 6 land as ONE commit series executed together — Task 6's rewrite is part of this task's verification. THEREFORE: do not run the full build until Task 6's files are also rewritten; run only `go test ./internal/engine/ -run TestTranslation` here. (Controller note: dispatch Tasks 5+6 to the same implementer as one unit.)

- [ ] **Step 4: Continue directly into Task 6 before committing.**

---

### Task 6: Orchestration + API rewiring (lands with Task 5 as one unit)

**Files:**
- Rewrite: `internal/orchestration/registration.go`, `internal/orchestration/starter.go`
- Rewrite: `internal/orchestration/registration_test.go`, `internal/orchestration/starter_test.go`
- Rewrite: `internal/api/router.go`, `internal/api/router_test.go`

**Interfaces:**
- Produces:

```go
// orchestration
type ProcessRuntime interface {
	ProcessOneBatch(ctx context.Context, input engine.ProcessInput) (engine.ProcessResult, error)
	FlushOutput(ctx context.Context) (engine.UploadResult, error)
}
func RegisterProcess(registry processRegistry, runtime ProcessRuntime) error // registers TranslationWorkflow + the two activities under the fixed names
var ErrProcessRuntimeRequired = errors.New("process runtime is required")

func NewTemporalWorkflowStarter(client temporalSignalStarter, batchSize, timeoutSeconds, batchesPerRun, flushEveryBatches int) *TemporalWorkflowStarter
func (s *TemporalWorkflowStarter) StartProcess(ctx context.Context) (WorkflowActionResult, error)
// signal-with-start: workflowID engine.ProcessWorkflowID, signal engine.SignalNewItems (nil arg),
// options{ID: engine.ProcessWorkflowID, TaskQueue: engine.ProcessTaskQueue},
// engine.TranslationWorkflow, engine.WorkflowInput{BatchSize, TimeoutSeconds, BatchesPerRun, FlushEveryBatches}

// api
type QueueAPI interface {
	Enqueue(ctx context.Context, req engine.EnqueueRequest) (engine.EnqueueResult, error)
	Stats(ctx context.Context) (engine.QueueStats, error)
}
type ProcessStarter interface {
	StartProcess(ctx context.Context) (orchestration.WorkflowActionResult, error)
}
func NewRouter(queue QueueAPI, starter ProcessStarter) *Router
func NewRouterWithLogger(queue QueueAPI, starter ProcessStarter, logger *slog.Logger) *Router
// Routes: GET /healthz (unchanged); POST /v1/queue/items; GET /v1/queue/stats; POST /v1/queue/process.
// /v1/sources/... deleted.
```

- Consumes: Task 4's `Enqueue/Stats/FlushOutput`, Task 5's constants/workflow.

- [ ] **Step 1: Rewrite orchestration tests, then implementations**

`registration_test.go`: keep the `fakeSourceRegistry`→rename `fakeProcessRegistry` and `functionName` helper; assertions become: one workflow registration ending `.TranslationWorkflow`; activity names exactly `["translator.ProcessOneBatch", "translator.FlushOutput"]`; nil runtime → `ErrProcessRuntimeRequired`. The fake runtime implements the two-method `ProcessRuntime`.

`starter_test.go`: keep `fakeTemporalClient`/`fakeWorkflowRun` verbatim. Tests: `StartProcess` uses workflow ID `translator/process`, task queue `translator-process`, signal name `new-items`, nil signal arg, and workflow input `engine.WorkflowInput{BatchSize: 25, TimeoutSeconds: 90, BatchesPerRun: 400, FlushEveryBatches: 7}` from `NewTemporalWorkflowStarter(client, 25, 90, 400, 7)`; nil client errors.

`registration.go` (full):

```go
package orchestration

import (
	"context"
	"errors"

	"github.com/pulsarpoint/corpscout/translator/internal/engine"
	"go.temporal.io/sdk/activity"
)

var ErrProcessRuntimeRequired = errors.New("process runtime is required")

// ProcessRuntime is the activity implementation; *engine.Runtime satisfies it.
type ProcessRuntime interface {
	ProcessOneBatch(ctx context.Context, input engine.ProcessInput) (engine.ProcessResult, error)
	FlushOutput(ctx context.Context) (engine.UploadResult, error)
}

type processRegistry interface {
	RegisterWorkflow(workflow interface{})
	RegisterActivityWithOptions(activity interface{}, options activity.RegisterOptions)
}

// RegisterProcess registers the translation workflow and its activities on
// the translator's single Temporal worker.
func RegisterProcess(registry processRegistry, runtime ProcessRuntime) error {
	if runtime == nil {
		return ErrProcessRuntimeRequired
	}
	registry.RegisterWorkflow(engine.TranslationWorkflow)
	registry.RegisterActivityWithOptions(
		runtime.ProcessOneBatch,
		activity.RegisterOptions{Name: engine.ActivityProcessOneBatch},
	)
	registry.RegisterActivityWithOptions(
		runtime.FlushOutput,
		activity.RegisterOptions{Name: engine.ActivityFlushOutput},
	)
	return nil
}
```

`starter.go`: keep `temporalSignalStarter` and `WorkflowActionResult` verbatim; struct fields become `{client; batchSize; timeoutSeconds; batchesPerRun; flushEveryBatches int}`; `StartProcess`:

```go
func (s *TemporalWorkflowStarter) StartProcess(ctx context.Context) (WorkflowActionResult, error) {
	if s.client == nil {
		return WorkflowActionResult{}, fmt.Errorf("temporal client is required")
	}
	run, err := s.client.SignalWithStartWorkflow(
		ctx,
		engine.ProcessWorkflowID,
		engine.SignalNewItems,
		nil,
		client.StartWorkflowOptions{
			ID:        engine.ProcessWorkflowID,
			TaskQueue: engine.ProcessTaskQueue,
		},
		engine.TranslationWorkflow,
		engine.WorkflowInput{
			BatchSize:         s.batchSize,
			TimeoutSeconds:    s.timeoutSeconds,
			BatchesPerRun:     s.batchesPerRun,
			FlushEveryBatches: s.flushEveryBatches,
		},
	)
	if err != nil {
		return WorkflowActionResult{}, fmt.Errorf("signal/start workflow: %w", err)
	}
	return WorkflowActionResult{WorkflowID: run.GetID(), RunID: run.GetRunID()}, nil
}
```

(`normalizeWorkflowAction` deleted.)

- [ ] **Step 2: Rewrite the API router and tests**

`router.go` (full rewrite; keep `writeJSON`/`writeError` helpers):

```go
package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/pulsarpoint/corpscout/translator/internal/engine"
	"github.com/pulsarpoint/corpscout/translator/internal/orchestration"
)

// QueueAPI is the queue surface the router exposes; *engine.Runtime satisfies it.
type QueueAPI interface {
	Enqueue(ctx context.Context, req engine.EnqueueRequest) (engine.EnqueueResult, error)
	Stats(ctx context.Context) (engine.QueueStats, error)
}

// ProcessStarter starts (or wakes) the translation workflow.
type ProcessStarter interface {
	StartProcess(ctx context.Context) (orchestration.WorkflowActionResult, error)
}

type Router struct {
	startedAt time.Time
	queue     QueueAPI
	starter   ProcessStarter
	logger    *slog.Logger
}

func NewRouter(queue QueueAPI, starter ProcessStarter) *Router {
	return NewRouterWithLogger(queue, starter, nil)
}

func NewRouterWithLogger(queue QueueAPI, starter ProcessStarter, logger *slog.Logger) *Router {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Router{
		startedAt: time.Now().UTC(),
		queue:     queue,
		starter:   starter,
		logger:    logger.With("component", "api"),
	}
}

func (r *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	switch {
	case req.URL.Path == "/healthz":
		r.healthz(w, req)
	case req.URL.Path == "/v1/queue/items":
		r.enqueue(w, req)
	case req.URL.Path == "/v1/queue/stats":
		r.stats(w, req)
	case req.URL.Path == "/v1/queue/process":
		r.process(w, req)
	default:
		writeError(w, http.StatusNotFound, "not found")
	}
}

func (r *Router) healthz(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"service":    "translator",
		"status":     "ok",
		"started_at": r.startedAt,
	})
}

func (r *Router) enqueue(w http.ResponseWriter, req *http.Request) {
	start := time.Now()
	if req.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if r.queue == nil {
		writeError(w, http.StatusServiceUnavailable, "queue is not configured")
		return
	}

	var enqueueRequest engine.EnqueueRequest
	decoder := json.NewDecoder(req.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&enqueueRequest); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if err := enqueueRequest.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	result, err := r.queue.Enqueue(req.Context(), enqueueRequest)
	if err != nil {
		r.logger.Error("enqueue failed", "err", err, "received", len(enqueueRequest.Items), "duration_ms", time.Since(start).Milliseconds())
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	response := map[string]any{
		"received": result.Received,
		"inserted": result.Inserted,
	}
	if result.Inserted > 0 && r.starter != nil {
		workflowResult, err := r.starter.StartProcess(req.Context())
		if err != nil {
			r.logger.Error("enqueue accepted but workflow start failed", "err", err)
			response["warning"] = "items queued but workflow start failed: " + err.Error()
		} else {
			response["workflow_id"] = workflowResult.WorkflowID
			response["run_id"] = workflowResult.RunID
		}
	}
	r.logger.Info(
		"enqueue accepted",
		"received", result.Received,
		"inserted", result.Inserted,
		"source_lang", enqueueRequest.SourceLang,
		"target_lang", enqueueRequest.TargetLang,
		"duration_ms", time.Since(start).Milliseconds(),
	)
	writeJSON(w, http.StatusAccepted, response)
}

func (r *Router) stats(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if r.queue == nil {
		writeError(w, http.StatusServiceUnavailable, "queue is not configured")
		return
	}
	stats, err := r.queue.Stats(req.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, stats)
}

func (r *Router) process(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if r.starter == nil {
		writeError(w, http.StatusServiceUnavailable, "workflow starter is not configured")
		return
	}
	result, err := r.starter.StartProcess(req.Context())
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{
		"workflow_id": result.WorkflowID,
		"run_id":      result.RunID,
		"status":      "accepted",
	})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
```

`router_test.go` (full rewrite): fakes `fakeQueueAPI` (records the request, returns configurable `EnqueueResult`/`QueueStats`/error) and `fakeProcessStarter` (records calls, returns `WorkflowActionResult{WorkflowID: "translator/process", RunID: "run-1"}`). Tests:

1. `TestEnqueueAcceptsValidRequestAndStartsWorkflow` — POST valid JSON body (2 items) → 202; body has `received:2, inserted:2, workflow_id, run_id`; starter called once.
2. `TestEnqueueSkipsWorkflowStartWhenNothingInserted` — fake returns `Inserted: 0` → 202, no `workflow_id` key, starter NOT called.
3. `TestEnqueueRejectsInvalidRequest` — missing `source_lang` → 400 containing `source_lang is required`; starter not called.
4. `TestEnqueueReportsWarningWhenWorkflowStartFails` — starter errors → 202 with `warning` key.
5. `TestStatsReturnsCounts` — GET → 200 `{"input":5,"pending":3,"output":1,"failed":1}`.
6. `TestProcessTriggersWorkflow` — POST /v1/queue/process → 202 with workflow_id.
7. `TestOldSourceRoutesAreGone` — POST /v1/sources/norway_brreg/run → 404.

Write each fully with `httptest` (mirror the existing file's style).

- [ ] **Step 3: Run all tests (Tasks 5+6 together)**

Run: `go build ./internal/... && go test ./internal/... 2>&1 | tail -8`
Expected: all internal packages PASS. (`cmd/` bridge from Task 1 may now fail to compile against renamed orchestration/api symbols — if so, extend the bridge minimally: construct nothing, keep the refuse-to-start exit; Task 7 rewrites cmd for real. `go build ./...` must be green before commit.)

- [ ] **Step 4: Format, vet, commit (one commit for Tasks 5+6)**

```bash
go fmt ./... && go vet ./...
git add internal/ cmd/
git commit -m "feat(translator): single process workflow, queue API endpoints, process starter"
```

---

### Task 7: Runtime cleanup — drop Definition and the scan path

**Files:**
- Modify: `internal/engine/runtime.go` (remove `Definition` from RuntimeConfig, remove `LoadNewInput`)
- Modify: `internal/engine/load.go` (delete `LoadInput`, `loadInputWithDB`, `flushStaticColumn`, `Options`, `LoadResult`, `ClickHouseSource.QueryTranslationInput`/`QueryStaticInput` usage — see below)
- Delete: `internal/engine/definition.go`, `internal/engine/definition_test.go`, `internal/engine/scan.go`, `internal/engine/scan_test.go`
- Modify: `internal/engine/clickhouse.go` (delete `QueryTranslationInput`, `QueryStaticInput`, `StaticInput`; keep `InsertTextTranslations`, `InputItem`, `TextTranslation`)
- Modify: `internal/engine/load_test.go` (reduce to upsert/DDL tests), `internal/engine/runtime_test.go` (drop Definition fixtures), `internal/engine/integration_test.go` (rewrite — see below)
- Delete: `config/sources/` directory

**Interfaces:**
- Produces: `RuntimeConfig{QueuePath string; Source ClickHouseSource; Translator translation.Translator; ProviderName, Model string; Logger *slog.Logger}` (no Definition); `ClickHouseSource` interface shrinks to `InsertTextTranslations(ctx, rows []TextTranslation) (int, error)`; logger `source` attribute dropped (component stays `translator_runtime`).
- Deleted symbols later tasks must not reference: `Definition`, `ColumnSpec`, `StaticSpec`, `LoadDefinition`, `ScanSQL`, `LoadInput`, `LoadResult`, `Options`, `StaticInput`, `norwayDefinition()`.

- [ ] **Step 1: Delete and prune**

```bash
git rm internal/engine/definition.go internal/engine/definition_test.go internal/engine/scan.go internal/engine/scan_test.go
git rm -r config/sources
```

Then prune per the interface list above. `load_test.go` keeps: queue-DDL creation, `upsertInputItems` dedup/validation tests, max-uint64 hash round-trip (rewrite call sites to use `upsertInputItems` + `createQueueTables` directly on a temp DuckDB instead of `LoadInput`; drop `fixtureSource`'s scan/static methods, keep `InsertTextTranslations` recording for flush tests). `runtime_test.go`: remove `Definition:` fields and any `norwayDefinition()` references (delete the helper along with scan_test.go — move any still-needed constants into the test files that use them).

`integration_test.go` rewrite: the two tests become (a) `TestEnqueueProcessFlushWithExistingClickHouse` — guarded by `TRANSLATOR_INTEGRATION_TESTS=true`: build a Runtime with the real ClickHouse and a fake translator (uppercase echo), `rt.Enqueue` two synthetic items under `source_table='corpscout.integration_test'`, `ProcessOneBatch`, `FlushOutput`, assert the two rows are in `corpscout.text_translations` (then ALTER DELETE cleanup like the existing test) and that `output_items`/matched `input_items` are empty; (b) keep `TestInsertTextTranslationsWithExistingClickHouse` as is. Delete the old Norway-scan integration test (its scan SQL now belongs to the Python loaders).

- [ ] **Step 2: Verify**

Run: `go build ./... && go test ./... 2>&1 | tail -10 && rg -n 'LoadDefinition|ScanSQL|LoadInput|norwayDefinition' internal/ cmd/ | head`
Expected: green; rg finds nothing.

- [ ] **Step 3: Format, vet, commit**

```bash
go fmt ./... && go vet ./...
git add -A internal/engine/ config/
git commit -m "refactor(translator): drop definition/scan machinery from engine"
```

---

### Task 8: Commands, script, Makefile — final wiring

**Files:**
- Rewrite: `cmd/translator-api/main.go`
- Rewrite: `cmd/translator-trigger/main.go`, `cmd/translator-trigger/main_test.go`
- Modify: `scripts/trigger-translator-workflow.sh`
- Modify: `Makefile`

**Interfaces:**
- Consumes: everything from Tasks 1–7.
- Produces: working binaries. `translator-api`: one runtime (from `cfg.Queue.Path` + `cfg.EndpointID`), one worker on `engine.ProcessTaskQueue`, `orchestration.RegisterProcess`, starter with `(client, cfg.Temporal.BatchSize, cfg.Temporal.TimeoutSeconds, cfg.Temporal.BatchesPerRun, cfg.Queue.FlushEveryBatches)`, router `api.NewRouterWithLogger(runtime, starter, logger)`, and boot-resume: after worker start, `runtime.Stats(ctx)`; if `Pending > 0` call `starter.StartProcess` (log, don't exit on error). `translator-trigger`: flags reduce to `-config`; it calls `StartProcess` and prints `workflow_id=… run_id=…`.

- [ ] **Step 1: Rewrite translator-api main.go**

Full structure (write it out; error handling like the current file — log + exit):

```go
func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, configPath, err := config.LoadFromEnvironment()
	// exit on: err; cfg.ClickHouse.NativeURL == ""; cfg.EndpointID == "";
	// endpoint missing/base_url/model/api_key empty (same checks as before, sans definition).

	clickHouse, err := engine.OpenClickHouse(ctx, cfg.ClickHouse.NativeURL)
	// defer close

	provider, err := translation.Init(translation.Config{
		BaseURL: endpointConfig.BaseURL, Model: endpointConfig.Model,
		APIKey: endpointConfig.APIKey, MaxTokens: endpointConfig.MaxTokens,
		ExtraBody: endpointConfig.ExtraBody, Logger: logger,
	})

	runtime, err := engine.NewRuntime(ctx, engine.RuntimeConfig{
		QueuePath: cfg.Queue.Path, Source: clickHouse, Translator: provider,
		ProviderName: cfg.EndpointID, Model: endpointConfig.Model, Logger: logger,
	})
	// defer close

	temporalClient, err := client.Dial(...)
	temporalWorker := worker.New(temporalClient, engine.ProcessTaskQueue, worker.Options{})
	orchestration.RegisterProcess(temporalWorker, runtime)
	temporalWorker.Start(); defer temporalWorker.Stop()

	starter := orchestration.NewTemporalWorkflowStarter(temporalClient,
		cfg.Temporal.BatchSize, cfg.Temporal.TimeoutSeconds, cfg.Temporal.BatchesPerRun,
		cfg.Queue.FlushEveryBatches)

	// Boot resume: never strand a half-full queue.
	if stats, err := runtime.Stats(ctx); err != nil {
		logger.Error("boot queue stats failed", "err", err)
	} else if stats.Pending > 0 {
		if result, err := starter.StartProcess(ctx); err != nil {
			logger.Error("boot resume failed", "err", err, "pending", stats.Pending)
		} else {
			logger.Info("boot resume started workflow", "pending", stats.Pending, "workflow_id", result.WorkflowID, "run_id", result.RunID)
		}
	}

	server := &http.Server{Addr: cfg.Server.ListenAddress,
		Handler: api.NewRouterWithLogger(runtime, starter, logger), ReadHeaderTimeout: 5 * time.Second}
	// serve + shutdown exactly as the current file
}
```

- [ ] **Step 2: Rewrite translator-trigger**

`main.go`: drop `-source`/`-action` flags entirely (keep `-config`); `run()` loads config, builds the starter via `newTemporalStarter`, calls `StartProcess`, prints `workflow_id=%s run_id=%s\n`. `newTemporalStarter` passes the four numeric knobs as in Step 1. `main_test.go`: rewrite the four tests — config-defaults test asserts the fake starter was called and stdout contains the workflow id; the "rejects unsupported action" test is deleted (no actions exist); help test unchanged; the fake starter interface becomes `StartProcess(ctx) (orchestration.WorkflowActionResult, error)`.

- [ ] **Step 3: Script and Makefile**

`scripts/trigger-translator-workflow.sh`: drop `SOURCE` and the action argument; workflow_id `translator/process`, task_queue `translator-process`, type `TranslationWorkflow`, signal `new-items` with `--signal-input 'null'`, workflow input `{"BatchSize":…,"TimeoutSeconds":…,"BatchesPerRun":…,"FlushEveryBatches":${TRANSLATOR_FLUSH_EVERY_BATCHES:-10}}`; update usage text.

`Makefile`: the three `trigger-*` targets collapse to one:

```make
trigger:
	CGO_ENABLED=$(CGO_ENABLED) $(GO) run $(TRIGGER_CMD)
```

(`SOURCE ?=` variable deleted; `.PHONY` updated.)

- [ ] **Step 4: Verify everything**

Run: `go build ./... && go fmt ./... && go vet ./... && go test ./... 2>&1 | tail -10 && go test -race ./internal/... 2>&1 | tail -3 && go run ./cmd/translator-trigger -h`
Expected: all green; usage prints.

- [ ] **Step 5: Commit**

```bash
git add cmd/ scripts/ Makefile
git commit -m "feat(translator): wire shared-queue service into binaries, script, Makefile"
```

---

### Task 9: Go README + integration verification

**Files:**
- Modify: `README.md` (translator)
- Verify: `internal/engine/integration_test.go` against real ClickHouse

- [ ] **Step 1: Rewrite the README's architecture sections**

Replace the per-source/definition/migration sections with: (a) the service contract (three endpoints with example bodies — copy the enqueue example from the spec §Component 1); (b) the loader pattern section, including the canonical anti-join scan SQL (copy verbatim from the spec §Component 5 / the deleted golden queries) and the note that loaders own dedup; (c) the SQL trust-boundary sentence (loaders are trusted developer-authored code); (d) deploy migration: terminate legacy workflows `translator/norway_brreg` (Go), `build-queue-norway_brreg` and `translate-norway_brreg` (Python), then simply restart — the new workflow self-starts on enqueue or boot-resume; (e) flush semantics (every N batches + at empty; crash-window duplicate note).

- [ ] **Step 2: Run integration tests against the real stack**

```bash
TRANSLATOR_INTEGRATION_TESTS=true go test ./internal/engine/ -run WithExistingClickHouse -v -count=1
```

Expected: enqueue→process→flush test PASS against real ClickHouse (skip acceptable ONLY if ClickHouse is unreachable — record which).

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs(translator): shared-queue service contract and loader pattern"
```

---

### Task 10: Dagster loader assets (Norway + Latvia)

**Files:**
- Create: `corpscout/dagster_v3/src/dagster_v3/defs/translator_load/__init__.py`
- Create: `corpscout/dagster_v3/src/dagster_v3/defs/translator_load/loader.py`
- Create: `corpscout/dagster_v3/src/dagster_v3/defs/translator_load/assets.py`
- Test: `corpscout/dagster_v3/tests/test_translator_load.py`

**Interfaces:**
- Consumes: `ClickhouseResource` (resource key `clickhouse`, `with clickhouse.get_connection() as client: client.execute(sql)` — native protocol, returns list of tuples); `dlt.sources.helpers.requests` for HTTP; env `TRANSLATOR_API_URL` (default `http://localhost:8080`); Go API from Task 6.
- Produces: assets `norway_brreg_translation_load` (deps: `norway_brreg_entities_snapshot_clickhouse`, group `norway_brreg`) and `latvia_ur_translation_load` (deps: `latvia_ur_clickhouse_companies`, group `latvia_ur`); module-level `defs = dg.Definitions(assets=[...])` (auto-discovered); shared helpers in `loader.py`:

```python
@dataclass(frozen=True)
class LoaderField:
    table: str
    column: str

@dataclass(frozen=True)
class LoaderSource:
    source_lang: str
    target_lang: str
    source_language_name: str
    target_language_name: str
    fields: tuple[LoaderField, ...]

def build_scan_sql(table: str, column: str) -> str
def build_static_scan_sql(table: str, column: str, key_column: str) -> str
def scan_rows(client, sql) -> list[tuple[str, int]]            # (text, hash) — llm scan returns extra cols positionally
def enqueue_items(session, api_url, source: LoaderSource, field: LoaderField, rows, chunk_size=10_000) -> dict  # sums {"received","inserted"}
def insert_static_translations(client, table, column, source_lang, target_lang, rows, mapping: dict[str, str]) -> int
```

- [ ] **Step 1: Write the failing tests**

Create `corpscout/dagster_v3/tests/test_translator_load.py` following the repo's fake-client style (see `tests/test_norway_brreg_entity_clickhouse.py` for fakes with `execute`). Cover:

```python
"""Tests for the translator_load loader helpers and assets."""

from dagster_v3.defs.translator_load.loader import (
    LoaderField,
    LoaderSource,
    build_scan_sql,
    build_static_scan_sql,
    enqueue_items,
    insert_static_translations,
)


def test_build_scan_sql_is_anti_join_with_cityhash():
    sql = build_scan_sql("corpscout.no_companies", "activity_text_original")
    for fragment in (
        "SELECT DISTINCT",
        "c.activity_text_original AS source_text",
        "cityHash64(c.activity_text_original) AS source_text_hash",
        "FROM corpscout.no_companies AS c",
        "LEFT ANTI JOIN",
        "FROM corpscout.text_translations",
        "WHERE source_table = 'corpscout.no_companies' AND source_column = 'activity_text_original'",
        "WHERE c.activity_text_original <> ''",
    ):
        assert fragment in sql, f"missing {fragment!r} in:\n{sql}"


def test_build_static_scan_sql_selects_key_column():
    sql = build_static_scan_sql(
        "corpscout.no_companies", "legal_form_description_original", "legal_form_code"
    )
    assert "c.legal_form_code AS legal_form_code" in sql
    assert "cityHash64(c.legal_form_description_original)" in sql


class _FakeSession:
    def __init__(self, inserted_per_call: int = 1) -> None:
        self.posts: list[tuple[str, dict]] = []
        self._inserted = inserted_per_call

    def post(self, url: str, json: dict, timeout: int = 60):
        self.posts.append((url, json))
        received = len(json["items"])

        class _Resp:
            status_code = 202

            def __init__(self, payload):
                self._payload = payload

            def raise_for_status(self):
                return None

            def json(self):
                return self._payload

        return _Resp({"received": received, "inserted": self._inserted})


def _norway_source() -> LoaderSource:
    return LoaderSource(
        source_lang="no",
        target_lang="en",
        source_language_name="Norwegian",
        target_language_name="English",
        fields=(LoaderField("corpscout.no_companies", "activity_text_original"),),
    )


def test_enqueue_items_chunks_and_sums():
    session = _FakeSession(inserted_per_call=2)
    rows = [(f"text {i}", i) for i in range(25)]

    totals = enqueue_items(
        session,
        "http://translator:8080",
        _norway_source(),
        _norway_source().fields[0],
        rows,
        chunk_size=10,
    )

    assert len(session.posts) == 3  # 10 + 10 + 5
    assert totals == {"received": 25, "inserted": 6}
    url, payload = session.posts[0]
    assert url == "http://translator:8080/v1/queue/items"
    assert payload["source_lang"] == "no"
    assert payload["source_language_name"] == "Norwegian"
    item = payload["items"][0]
    assert item["source_table"] == "corpscout.no_companies"
    assert item["source_column"] == "activity_text_original"
    assert isinstance(item["source_text_hash"], str)  # decimal string, never int


class _FakeInsertClient:
    def __init__(self) -> None:
        self.executed: list[tuple[str, object]] = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        return []


def test_insert_static_translations_maps_and_skips_unknown_keys():
    client = _FakeInsertClient()
    rows = [("Aksjeselskap", 9001, "AS"), ("Ukjent", 9002, "NOPE")]

    inserted = insert_static_translations(
        client,
        "corpscout.no_companies",
        "legal_form_description_original",
        "no",
        "en",
        rows,
        {"AS": "Private limited company"},
    )

    assert inserted == 1
    sql, params = client.executed[-1]
    assert "text_translations" in sql
    (row,) = params
    assert row[:3] == ("corpscout.no_companies", "legal_form_description_original", 9001)
```

(Adjust the params-shape assertions to the actual insert implementation — the test pins behavior: one inserted row, unknown key skipped, correct table/column/hash.)

Also add an asset wiring test:

```python
def test_assets_are_defined_with_expected_deps():
    from dagster_v3.defs.translator_load import assets as translator_assets

    import dagster as dg

    norway = translator_assets.norway_brreg_translation_load
    assert dg.AssetKey("norway_brreg_entities_snapshot_clickhouse") in {
        dep.asset_key for dep in norway.specs_by_key[norway.key].deps
    }
```

(If `specs_by_key` isn't the right accessor for the dagster version in use, assert via `norway.asset_deps` — the implementer picks the accessor that works and keeps the assertion: the dep on the ingest asset exists.)

Also add one env-guarded REAL-ClickHouse integration test (spec §Testing requires the Python side to execute against real ClickHouse; uses the `integration` marker already declared in `pytest.ini` and the `RUN_TRANSLATION_INTEGRATION_TESTS` var already reserved in `.env.example`):

```python
import os

import pytest


@pytest.mark.integration
@pytest.mark.skipif(
    os.environ.get("RUN_TRANSLATION_INTEGRATION_TESTS") != "1",
    reason="set RUN_TRANSLATION_INTEGRATION_TESTS=1 and CLICKHOUSE_* env vars to run",
)
def test_scan_sql_executes_against_real_clickhouse():
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_HTTP_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
        database=os.environ.get("CLICKHOUSE_DATABASE", "corpscout"),
    )
    for table, column in (
        ("corpscout.no_companies", "articles_purpose_original"),
        ("corpscout.no_companies", "activity_text_original"),
        ("corpscout.lv_companies", "activity_text_original"),
    ):
        sql = build_scan_sql(table, column)
        count = client.query(f"SELECT count() FROM ({sql})").result_rows[0][0]
        assert count >= 0  # proves the anti-join shape is valid against the real schema
    static_sql = build_static_scan_sql(
        "corpscout.no_companies", "legal_form_description_original", "legal_form_code"
    )
    assert client.query(f"SELECT count() FROM ({static_sql})").result_rows[0][0] >= 0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd ../dagster_v3 && uv run pytest tests/test_translator_load.py -v 2>&1 | head -8`
Expected: FAIL — `ModuleNotFoundError: dagster_v3.defs.translator_load`.

- [ ] **Step 3: Implement**

`loader.py` — the two SQL builders reproduce the canonical anti-join shape (LLM scan needs only `(source_text, source_text_hash)` since the enqueue request carries table/column/langs):

```python
"""Shared loader helpers: anti-join scans + bulk enqueue to the Go translator.

Loader contract (the system's dedup economics live here):
- Scan ONLY distinct texts not yet in corpscout.text_translations (anti-join).
- Compute cityHash64 in ClickHouse SQL — never in Python — so hashes always
  agree with past runs.
- POST chunks of at most 10k items; hashes are decimal STRINGS (uint64 does
  not fit JSON numbers).
- Static-map columns never touch the LLM: insert straight into
  text_translations with provider='static'.
Table/column values are interpolated into SQL — loader configs are trusted,
developer-authored code.
"""

from __future__ import annotations

import time
from dataclasses import dataclass

MAX_ITEMS_PER_REQUEST = 10_000


@dataclass(frozen=True)
class LoaderField:
    table: str
    column: str


@dataclass(frozen=True)
class LoaderSource:
    source_lang: str
    target_lang: str
    source_language_name: str
    target_language_name: str
    fields: tuple[LoaderField, ...]


def build_scan_sql(table: str, column: str) -> str:
    return f"""
SELECT DISTINCT
    c.{column} AS source_text,
    cityHash64(c.{column}) AS source_text_hash
FROM {table} AS c
LEFT ANTI JOIN (
    SELECT source_text_hash
    FROM corpscout.text_translations
    WHERE source_table = '{table}' AND source_column = '{column}'
    GROUP BY source_text_hash
) AS t ON t.source_text_hash = cityHash64(c.{column})
WHERE c.{column} <> ''"""


def build_static_scan_sql(table: str, column: str, key_column: str) -> str:
    return f"""
SELECT DISTINCT
    c.{column} AS source_text,
    cityHash64(c.{column}) AS source_text_hash,
    c.{key_column} AS {key_column}
FROM {table} AS c
LEFT ANTI JOIN (
    SELECT source_text_hash
    FROM corpscout.text_translations
    WHERE source_table = '{table}' AND source_column = '{column}'
    GROUP BY source_text_hash
) AS t ON t.source_text_hash = cityHash64(c.{column})
WHERE c.{column} <> ''"""


def enqueue_items(session, api_url, source, field, rows, chunk_size=MAX_ITEMS_PER_REQUEST):
    """POST (text, hash) rows in chunks; returns summed {'received','inserted'}."""
    totals = {"received": 0, "inserted": 0}
    for start in range(0, len(rows), chunk_size):
        chunk = rows[start : start + chunk_size]
        payload = {
            "source_lang": source.source_lang,
            "target_lang": source.target_lang,
            "source_language_name": source.source_language_name,
            "target_language_name": source.target_language_name,
            "items": [
                {
                    "source_table": field.table,
                    "source_column": field.column,
                    "source_text": text,
                    "source_text_hash": str(hash_),
                }
                for text, hash_ in chunk
            ],
        }
        response = session.post(f"{api_url}/v1/queue/items", json=payload, timeout=60)
        response.raise_for_status()
        body = response.json()
        totals["received"] += body["received"]
        totals["inserted"] += body["inserted"]
    return totals


def insert_static_translations(client, table, column, source_lang, target_lang, rows, mapping):
    """Insert map-translated rows straight into text_translations; unknown keys skipped."""
    version = int(time.time())
    values = []
    for text, hash_, key in rows:
        translated = mapping.get(key, "")
        if not text or not translated:
            continue
        values.append(
            (table, column, hash_, source_lang, target_lang, translated, "static", "static", version)
        )
    if not values:
        return 0
    client.execute(
        """
        INSERT INTO corpscout.text_translations (
            source_table, source_column, source_text_hash,
            source_lang, target_lang, translated_text, provider, model, version
        ) VALUES
        """,
        values,
    )
    return len(values)
```

`assets.py` — two assets following the `czech_ares` module-level `defs` pattern, `dlt` requests session, `ClickhouseResource`:

```python
"""Loader assets: scan ClickHouse for untranslated texts and enqueue them."""

from __future__ import annotations

import os

import dagster as dg
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource
from dlt.sources.helpers import requests as dlt_requests

from dagster_v3.defs.translator_load.loader import (
    LoaderField,
    LoaderSource,
    build_scan_sql,
    build_static_scan_sql,
    enqueue_items,
    insert_static_translations,
)

# Moved verbatim from the retired translator configs (Go config/sources JSON
# and Python translator.static_maps).
LEGAL_FORM_DESCRIPTION_EN_BY_CODE: dict[str, str] = {
    "ADOS": "Administrative unit - public sector",
    "ANNA": "Other legal entity",
    "ANS": "General partnership",
    "AS": "Private limited company",
    "ASA": "Public limited company",
    "BA": "Company with limited liability",
    "BBL": "Housing cooperative building association",
    "BO": "Other estate",
    "BRL": "Housing cooperative",
    "DA": "General partnership with shared liability",
    "ENK": "Sole proprietorship",
    "ESEK": "Condominium (owner-section co-ownership)",
    "FKF": "County municipal enterprise",
    "FLI": "Association/club/institution",
    "FYLK": "County authority",
    "GFS": "Mutual insurance company",
    "IKS": "Inter-municipal company",
    "KF": "Municipal enterprise",
    "KBO": "Bankruptcy estate",
    "KIRK": "Church of Norway",
    "KOMM": "Municipality",
    "KS": "Limited partnership",
    "KTRF": "Office-sharing arrangement",
    "NUF": "Norwegian-registered foreign company",
    "OPMV": "Separately divided unit (VAT Act section 2-2)",
    "ORGL": "Organisational subdivision",
    "PERS": "Other registered individuals",
    "PK": "Pension fund",
    "PRE": "Shipping partnership",
    "SA": "Cooperative",
    "SAM": "Co-ownership under property law",
    "SE": "European company (SE)",
    "SF": "State enterprise",
    "SPA": "Savings bank",
    "STAT": "The State",
    "STI": "Foundation",
    "SÆR": "Other enterprise under special legislation",
    "TVAM": "Compulsorily registered for VAT",
    "UTLA": "Foreign entity",
    "VPFO": "Securities fund",
}

_NORWAY = LoaderSource(
    source_lang="no",
    target_lang="en",
    source_language_name="Norwegian",
    target_language_name="English",
    fields=(
        LoaderField("corpscout.no_companies", "articles_purpose_original"),
        LoaderField("corpscout.no_companies", "activity_text_original"),
    ),
)
_NORWAY_STATIC = LoaderField("corpscout.no_companies", "legal_form_description_original")

_LATVIA = LoaderSource(
    source_lang="lv",
    target_lang="en",
    source_language_name="Latvian",
    target_language_name="English",
    fields=(LoaderField("corpscout.lv_companies", "activity_text_original"),),
)


def _api_url() -> str:
    return os.environ.get("TRANSLATOR_API_URL", "http://localhost:8080").rstrip("/")


def _load_source(context: AssetExecutionContext, clickhouse: ClickhouseResource, source: LoaderSource) -> dict:
    session = dlt_requests.Session()
    totals = {"received": 0, "inserted": 0}
    with clickhouse.get_connection() as client:
        for field in source.fields:
            rows = client.execute(build_scan_sql(field.table, field.column))
            context.log.info("scanned %d untranslated texts for %s.%s", len(rows), field.table, field.column)
            field_totals = enqueue_items(session, _api_url(), source, field, rows)
            totals["received"] += field_totals["received"]
            totals["inserted"] += field_totals["inserted"]
    return totals


@dg.asset(
    deps=[dg.AssetKey("norway_brreg_entities_snapshot_clickhouse")],
    group_name="norway_brreg",
    kinds={"python", "clickhouse"},
    description=(
        "Scan corpscout.no_companies for untranslated texts (anti-join vs "
        "text_translations), enqueue them to the translator service, and insert "
        "static legal-form translations directly."
    ),
)
def norway_brreg_translation_load(
    context: AssetExecutionContext, clickhouse: ClickhouseResource
) -> dg.MaterializeResult:
    totals = _load_source(context, clickhouse, _NORWAY)
    with clickhouse.get_connection() as client:
        static_rows = client.execute(
            build_static_scan_sql(_NORWAY_STATIC.table, _NORWAY_STATIC.column, "legal_form_code")
        )
        static_inserted = insert_static_translations(
            client,
            _NORWAY_STATIC.table,
            _NORWAY_STATIC.column,
            _NORWAY.source_lang,
            _NORWAY.target_lang,
            static_rows,
            LEGAL_FORM_DESCRIPTION_EN_BY_CODE,
        )
    context.log.info("static legal forms inserted: %d", static_inserted)
    return dg.MaterializeResult(
        metadata={
            "enqueued_received": totals["received"],
            "enqueued_inserted": totals["inserted"],
            "static_inserted": static_inserted,
        }
    )


@dg.asset(
    deps=[dg.AssetKey("latvia_ur_clickhouse_companies")],
    group_name="latvia_ur",
    kinds={"python", "clickhouse"},
    description=(
        "Scan corpscout.lv_companies for untranslated texts and enqueue them "
        "to the translator service."
    ),
)
def latvia_ur_translation_load(
    context: AssetExecutionContext, clickhouse: ClickhouseResource
) -> dg.MaterializeResult:
    totals = _load_source(context, clickhouse, _LATVIA)
    return dg.MaterializeResult(
        metadata={
            "enqueued_received": totals["received"],
            "enqueued_inserted": totals["inserted"],
        }
    )


defs = dg.Definitions(assets=[norway_brreg_translation_load, latvia_ur_translation_load])
```

`__init__.py`: module docstring naming the loader pattern (one paragraph pointing at `loader.py`'s contract docstring).

Adaptation note for the implementer: `ClickhouseResource.get_connection()` client method shape must be confirmed against an existing asset (`src/dagster_v3/defs/norway_brreg/assets/entity_clickhouse.py:49`) — if `client.execute` differs (e.g. `client.query(...).result_rows`), adapt `_load_source`/`insert_static_translations` and the fakes accordingly; the tests pin behavior (chunking, string hashes, skip-unknown-keys), not the client API.

- [ ] **Step 4: Run tests + dagster load check**

Run: `uv run pytest tests/test_translator_load.py -v && uv run dg check defs 2>&1 | tail -5`
Expected: tests PASS; definitions load without errors (both new assets listed).

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/defs/translator_load/ tests/test_translator_load.py
git commit -m "feat(dagster): translator_load loader assets for Norway and Latvia"
```

---

### Task 11: E2E smoke, then retire the Python translator package

**Files:**
- Delete: `corpscout/dagster_v3/translator/` (entire package)
- Delete: `corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/assets/translation.py`
- Modify: whichever `norway_brreg` file registers `norway_brreg_translation_trigger` / `norway_brreg_entities_full_snapshot_job` (find with `rg -n 'translation_trigger|full_snapshot_job' src/`) — the job's selection re-targets `norway_brreg_translation_load`
- Delete: `tests/test_translator_*.py` (12 files) and any other test importing `translator` (`rg -l '^from translator|^import translator' tests/`)
- Modify: `pyproject.toml` (remove `"translator"` from `[tool.hatch.build.targets.wheel].packages`)
- Modify: `README.md` (dagster_v3) and `docs/data-source-guidelines.md` §8 — rewrite the translation sections to describe the Go service + `translator_load` loader pattern

**Interfaces:**
- Consumes: everything; this is the cutover.

- [ ] **Step 1: E2E smoke (GATE — do not delete anything until this passes)**

On the lab stack (translator-api running with the new build, Temporal + ClickHouse + LLM endpoint up):

```bash
# 1. Start the new translator-api (from corpscout/translator): make run  (or the deployed instance)
# 2. Materialize the Norway loader:
cd corpscout/dagster_v3 && uv run dg launch --assets norway_brreg_translation_load
# 3. Watch: curl -s http://localhost:8080/v1/queue/stats   → pending drains to 0
# 4. Verify new rows: SELECT count() FROM corpscout.text_translations WHERE version >= <today's epoch>
```

Record the numbers in the commit message of Step 3. If the loader enqueues 0 items because everything is already translated, verify the path with the Latvia asset instead (Latvia has never been translated — expect real volume), or delete a handful of `text_translations` test rows first.

- [ ] **Step 2: Delete the Python package and fix registrations**

```bash
cd corpscout/dagster_v3
git rm -r translator/
git rm src/dagster_v3/defs/norway_brreg/assets/translation.py
git rm tests/test_translator_*.py
rg -l '^from translator|^import translator' tests/ src/ | xargs -r git rm   # remaining importers (inspect each before removal; test_norway_brreg_workflows.py and test_norway_brreg_dump.py tests of deleted code get deleted, files with incidental imports get edited instead)
```

Fix the `norway_brreg` group registration: remove the deleted asset/job imports; re-create `norway_brreg_entities_full_snapshot_job` where the group's other jobs live, with its selection updated from `norway_brreg_translation_trigger` to `norway_brreg_translation_load` (preserve the job name and any schedule attached to it — check `rg -n 'full_snapshot_job' src/`). Remove `"translator"` from `pyproject.toml` wheel packages.

- [ ] **Step 3: Docs, verify, commit**

Rewrite the "# Translation" section of `corpscout/dagster_v3/README.md` and §8 of `docs/data-source-guidelines.md`: translation is now "loader asset (this repo) → Go translator service (`corpscout/translator`) → `text_translations`"; the loader pattern contract (anti-join, hash-in-SQL, chunked POST, static maps direct-insert); link to the Go README for the API contract.

```bash
uv run pytest 2>&1 | tail -5          # whole suite green without the deleted tests
uv run ruff check . 2>&1 | tail -3
uv run dg check defs 2>&1 | tail -3   # definitions still load
git add -A
git commit -m "feat(dagster)!: retire legacy Python translator, loaders feed the Go service

E2E verified: <numbers from step 1>. Deploy note: terminate Temporal workflows
build-queue-norway_brreg, translate-norway_brreg (Python) and
translator/norway_brreg (old Go); the new translator/process workflow
self-starts on enqueue or translator-api boot."
```

---

## Deployment note (not a code task)

After deploying the new translator-api and the dagster code:

```bash
temporal workflow terminate --workflow-id "build-queue-norway_brreg" --reason "python translator retired" || true
temporal workflow terminate --workflow-id "translate-norway_brreg" --reason "python translator retired" || true
temporal workflow terminate --workflow-id "translator/norway_brreg" --reason "shared-queue cutover" || true
```

The old Python translator worker deployment (its Dockerfile is deleted) must be removed from wherever it runs. The old queue file `data/translator/norway_brreg.duckdb` can be archived or deleted — anything untranslated in it is re-discovered by the loaders' anti-join.
