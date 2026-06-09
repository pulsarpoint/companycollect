# Corpscout Source ClickHouse Ingestion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move the first company source ingestion path into Corpscout so Finland PRH YTJ raw source snapshots are parsed in Go and inserted directly into ClickHouse with manual source-specific migrations.

**Architecture:** Corpscout owns source modules, manual ClickHouse migrations, run manifests, run import state, and direct ClickHouse imports. `companies/companysource` remains untouched until the Finland path works, then its ClickHouse-specific commands and Finland ClickHouse glue are removed. The first slice uses `finland/prhytj` only; United States sources follow after this pattern is verified.

**Tech Stack:** Go 1.26, `log/slog`, `github.com/cockroachdb/errors`, `golang-migrate`, ClickHouse native port through `clickhouse-client` Docker container, JSONEachRow batch inserts, existing Corpscout Makefile.

---

## Scope

This plan implements the approved first slice:

- remove the unused `corpscout_projection` ClickHouse database
- keep manual ClickHouse migration SQL as the schema authority
- add shared Corpscout source run manifest and run index helpers
- add shared ClickHouse JSONEachRow batch insert helper
- add reusable Go import orchestration that CLI and Temporal activities can call
- add `corpscout-source` CLI inside the scheduler module
- move Finland PRH YTJ raw parsing and direct import into Corpscout
- import an existing raw run from `companies/data/finland/sources/prhytj/runs/20260608T201348Z-prhytj/source.ndjson`
- remove obsolete companysource ClickHouse generation/import wiring for Finland

This plan does not migrate all existing United States sources. It leaves them as a follow-up after Finland proves the path.

The CLI must remain a wrapper around `internal/companysources` orchestration. Temporal activities should call `companysources.ImportRun` or `companysources.ImportChangedRuns` directly instead of shelling out to `corpscout-source`.

## Small Execution Task Breakdown

Execute the implementation in these smaller commits. The larger task sections
below contain the concrete code snippets and test commands; this section is the
authoritative execution order.

### Task 1: Add ClickHouse Migration Shape Test

**Files:**
- Create: `corpscout/scheduler/internal/db/clickhouse_migrations_test.go`

**Verify:**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/db -run TestClickHouseInitialMigrationOnlyCreatesSourcesDatabase -count=1 -v
```

Expected before Task 2: FAIL because `corpscout_projection` still exists.

**Commit:** Do not commit yet; commit with Task 2.

### Task 2: Remove Projection Database From Initial ClickHouse Migration

**Files:**
- Modify: `corpscout/clickhouse/migrations/000001_create_databases.up.sql`
- Modify: `corpscout/clickhouse/migrations/000001_create_databases.down.sql`

**Verify:**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/db -run TestClickHouseInitialMigrationOnlyCreatesSourcesDatabase -count=1 -v
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
make -n clickhouse-migrate-up
```

Expected: test passes; dry-run still targets remote `companycollect`.

**Commit:** `fix: remove unused clickhouse projection database`

### Task 3: Add Run Manifest Read/Write/Hash

**Files:**
- Create: `corpscout/scheduler/internal/companysources/runmanifest/manifest.go`
- Create: `corpscout/scheduler/internal/companysources/runmanifest/manifest_test.go`

**Scope:** Only `Manifest`, `File`, `Write`, `Read`, and `Hash`.

**Verify:**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/companysources/runmanifest -run 'TestWriteReadAndHashManifest' -count=1 -v
```

**Commit:** `feat: add source run manifest IO`

### Task 4: Add Latest Run Discovery

**Files:**
- Modify: `corpscout/scheduler/internal/companysources/runmanifest/manifest.go`
- Modify: `corpscout/scheduler/internal/companysources/runmanifest/manifest_test.go`

**Scope:** Add `LatestCompletedRun` only.

**Verify:**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/companysources/runmanifest -run 'TestLatestCompletedRunChoosesNewestManifest' -count=1 -v
```

**Commit:** `feat: discover latest source run manifests`

### Task 5: Add Run Index Model And Import Decision

**Files:**
- Create: `corpscout/scheduler/internal/companysources/runindex/index.go`
- Create: `corpscout/scheduler/internal/companysources/runindex/index_test.go`

**Scope:** Add `Index`, `Entry`, `ShouldImport`, and `MarkImported`.

**Verify:**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/companysources/runindex -run 'TestShouldImportWhenRunIsMissingOrChanged' -count=1 -v
```

**Commit:** `feat: decide changed source imports`

### Task 6: Add Run Index Persistence

**Files:**
- Modify: `corpscout/scheduler/internal/companysources/runindex/index.go`
- Modify: `corpscout/scheduler/internal/companysources/runindex/index_test.go`

**Scope:** Add `Load` and `Save`.

**Verify:**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/companysources/runindex -run 'TestSaveAndLoadIndex' -count=1 -v
```

**Commit:** `feat: persist source import run index`

### Task 7: Add ClickHouse Native URL Parsing

**Files:**
- Create: `corpscout/scheduler/internal/clickhouseclient/url.go`
- Create: `corpscout/scheduler/internal/clickhouseclient/json_each_row_test.go`

**Scope:** Add `Target` and `ParseNativeURL`.

**Verify:**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/clickhouseclient -run 'TestParseNativeURL' -count=1 -v
```

**Commit:** `feat: parse clickhouse native URLs`

### Task 8: Add ClickHouse Insert Query And JSONEachRow Encoding

**Files:**
- Create: `corpscout/scheduler/internal/clickhouseclient/json_each_row.go`
- Modify: `corpscout/scheduler/internal/clickhouseclient/json_each_row_test.go`

**Scope:** Add `BuildInsertQuery`, `EncodeJSONEachRow`, `Insert`, and identifier quoting.

**Verify:**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/clickhouseclient -run 'TestBuildInsertQuery|TestEncodeJSONEachRow' -count=1 -v
```

**Commit:** `feat: encode clickhouse JSONEachRow inserts`

### Task 9: Add ClickHouse Docker Client Execution

**Files:**
- Modify: `corpscout/scheduler/internal/clickhouseclient/json_each_row.go`
- Modify: `corpscout/scheduler/internal/clickhouseclient/json_each_row_test.go`

**Scope:** Add `ExecuteInsert` and remote `companycollect` host mapping.

**Verify:**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/clickhouseclient -count=1 -v
```

**Commit:** `feat: run clickhouse insert client`

### Task 10: Add Company Source Interface And Registry

**Files:**
- Create: `corpscout/scheduler/internal/companysources/source.go`
- Create: `corpscout/scheduler/internal/companysources/registry.go`
- Create: `corpscout/scheduler/internal/companysources/registry_test.go`

**Scope:** Add `Source`, `Key`, import request/result structs, `Registry`.

**Verify:**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/companysources -run 'TestRegistryLookupAndKeys' -count=1 -v
```

**Commit:** `feat: add company source registry`

### Task 11: Add Single-Run Go Import Orchestration

**Files:**
- Create: `corpscout/scheduler/internal/companysources/importer.go`
- Create: `corpscout/scheduler/internal/companysources/importer_test.go`

**Scope:** Add `ImportRun` only.

**Verify:**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/companysources -run 'TestImportRunCallsRegisteredSource' -count=1 -v
```

**Commit:** `feat: add source import orchestration`

### Task 12: Add Changed-Runs Go Import Orchestration

**Files:**
- Modify: `corpscout/scheduler/internal/companysources/source.go`
- Modify: `corpscout/scheduler/internal/companysources/importer.go`
- Modify: `corpscout/scheduler/internal/companysources/importer_test.go`

**Scope:** Add `ImportChangedRuns` and changed-only skipping through `runindex`.

**Verify:**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/companysources -run 'TestImportChangedRunsSkipsUnchangedRun' -count=1 -v
```

**Commit:** `feat: orchestrate changed source imports`

### Task 13: Add `corpscout-source list-sources`

**Files:**
- Create: `corpscout/scheduler/cmd/corpscout-source/main.go`
- Create: `corpscout/scheduler/cmd/corpscout-source/main_test.go`

**Scope:** CLI skeleton with `list-sources` only and a temporary Finland registry entry.

**Verify:**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./cmd/corpscout-source -run 'TestListSourcesCommand|TestUnknownCommandFails' -count=1 -v
```

**Commit:** `feat: add corpscout source CLI skeleton`

### Task 14: Copy Finland PRH YTJ Types And Fixtures

**Files:**
- Create: `corpscout/scheduler/internal/companysources/finland/prhytj/types.go`
- Create: `corpscout/scheduler/internal/companysources/finland/prhytj/testdata/prh_snapshot_mixed.ndjson`

**Scope:** Copy raw input structs and one small fixture only.

**Verify:**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/companysources/finland/prhytj -run '^$' -count=1
```

**Commit:** `feat: add Finland PRH YTJ source types`

### Task 15: Add Finland PRH YTJ Snapshot Parser

**Files:**
- Create: `corpscout/scheduler/internal/companysources/finland/prhytj/parser.go`
- Create: `corpscout/scheduler/internal/companysources/finland/prhytj/parser_test.go`

**Scope:** Stream `source.ndjson`, decode records, preserve raw payload and payload hash.

**Verify:**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/companysources/finland/prhytj -run 'TestParseSnapshotPreservesRawPayloadAndHash' -count=1 -v
```

**Commit:** `feat: parse Finland PRH YTJ snapshots`

### Task 16: Add Finland Raw Record Row Mapping

**Files:**
- Create: `corpscout/scheduler/internal/companysources/finland/prhytj/import.go`
- Create: `corpscout/scheduler/internal/companysources/finland/prhytj/import_test.go`

**Scope:** Add `rawRecordColumns` and `rawRecordRow`; do not insert yet.

**Verify:**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/companysources/finland/prhytj -run 'TestRawRecord' -count=1 -v
```

**Commit:** `feat: map Finland PRH raw records`

### Task 17: Add Finland Company Row Mapping

**Files:**
- Modify: `corpscout/scheduler/internal/companysources/finland/prhytj/import.go`
- Modify: `corpscout/scheduler/internal/companysources/finland/prhytj/import_test.go`

**Scope:** Add `companyColumns`, `companyRow`, and mapping helpers; do not insert yet.

**Verify:**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/companysources/finland/prhytj -run 'TestCompany' -count=1 -v
```

**Commit:** `feat: map Finland PRH company rows`

### Task 18: Add Finland Direct Import Implementation

**Files:**
- Modify: `corpscout/scheduler/internal/companysources/finland/prhytj/import.go`
- Modify: `corpscout/scheduler/internal/companysources/finland/prhytj/import_test.go`

**Scope:** Add `Source` methods, batching, `flushRows`, and direct ClickHouse inserts.

**Verify:**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/companysources/finland/prhytj -count=1 -v
```

**Commit:** `feat: import Finland PRH rows into clickhouse`

### Task 19: Wire Finland Source Into Registry

**Files:**
- Modify: `corpscout/scheduler/cmd/corpscout-source/main.go`
- Modify: `corpscout/scheduler/cmd/corpscout-source/main_test.go`

**Scope:** Replace temporary registry entry with `prhytj.Source{}`.

**Verify:**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./cmd/corpscout-source ./internal/companysources/finland/prhytj -count=1 -v
```

**Commit:** `feat: register Finland PRH source in corpscout`

### Task 20: Add `corpscout-source import-run`

**Files:**
- Modify: `corpscout/scheduler/cmd/corpscout-source/main.go`
- Modify: `corpscout/scheduler/cmd/corpscout-source/main_test.go`

**Scope:** Parse flags and call `companysources.ImportRun`.

**Verify:**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./cmd/corpscout-source -run 'TestImportRunRequiresClickHouseURL' -count=1 -v
```

**Commit:** `feat: add source import-run CLI`

### Task 21: Add `corpscout-source import-runs`

**Files:**
- Modify: `corpscout/scheduler/cmd/corpscout-source/main.go`
- Modify: `corpscout/scheduler/cmd/corpscout-source/main_test.go`

**Scope:** Parse flags and call `companysources.ImportChangedRuns`.

**Verify:**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./cmd/corpscout-source -run 'TestImportRunsRequiresRunsRoot' -count=1 -v
```

**Commit:** `feat: add source import-runs CLI`

### Task 22: Add Make Targets For Corpscout Source Imports

**Files:**
- Modify: `corpscout/Makefile`

**Scope:** Add `source-list`, `source-import-run`, `source-import-changed`; remove obsolete Finland Parquet ClickHouse Make targets.

**Verify:**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
make -n source-list
make -n source-import-run
make source-list
```

**Commit:** `chore: route source imports through corpscout`

### Task 23: Verify Remote Limited Finland Import

**Files:**
- No planned source edits.

**Scope:** Run `make source-import-run SOURCE_LIMIT=100 SOURCE_BATCH_SIZE=50` against remote ClickHouse and verify counts.

**Verify:**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
make clickhouse-migrate-up
make source-import-run SOURCE_LIMIT=100 SOURCE_BATCH_SIZE=50
```

Then query remote counts as described in the detailed Task 10 section.

**Commit:** Only commit if verification requires code changes.

### Task 24: Remove Companysource ClickHouse CLI Commands

**Files:**
- Modify: `companies/companysource/internal/cli/config.go`
- Modify: `companies/companysource/internal/cli/config_test.go`
- Modify: `companies/companysource/internal/cli/run.go`
- Modify: `companies/companysource/internal/source/source.go`

**Scope:** Remove `generate-clickhouse-migration` and `import-clickhouse` from the companysource CLI and adapter contract.

**Verify:**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/companies/companysource
GOWORK=off go test ./internal/cli ./internal/registry -count=1 -v
```

**Commit:** `refactor: remove companysource clickhouse CLI commands`

### Task 25: Remove Finland Companysource ClickHouse Glue

**Files:**
- Modify: `companies/companysource/sources/finland/prhytj/source.go`
- Delete: `companies/companysource/sources/finland/prhytj/clickhouse.yaml`

**Scope:** Remove embedded ClickHouse YAML and Finland `GenerateClickHouseMigration` / `ImportClickHouse` methods.

**Verify:**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/companies/companysource
GOWORK=off go test ./sources/finland/prhytj ./... -count=1
```

**Commit:** `refactor: remove Finland companysource clickhouse glue`

### Task 26: Final Verification

**Files:**
- No planned source edits.

**Verify:**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./cmd/corpscout-source ./internal/clickhouseclient ./internal/companysources/... ./internal/db -count=1
cd /Users/graovic/pulsarpoint/ppoint/companycollect/companies/companysource
GOWORK=off go test ./... -count=1
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
make -n clickhouse-migrate-up
make -n source-list
make -n source-import-run
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git status --short -uall
```

Expected: tests pass, Makefile dry-runs are correct, worktree is clean.

## File Structure

Create or modify these files:

- Modify: `corpscout/clickhouse/migrations/000001_create_databases.up.sql`
  - Remove `corpscout_projection`.
- Modify: `corpscout/clickhouse/migrations/000001_create_databases.down.sql`
  - Stop dropping `corpscout_projection`.
- Create: `corpscout/scheduler/internal/db/clickhouse_migrations_test.go`
  - Test migration 000001 does not create the projection database.
- Create: `corpscout/scheduler/internal/companysources/runmanifest/manifest.go`
  - Read/write raw source run manifests and hash manifest files.
- Create: `corpscout/scheduler/internal/companysources/runmanifest/manifest_test.go`
  - Unit tests for manifest IO and hashing.
- Create: `corpscout/scheduler/internal/companysources/runindex/index.go`
  - Track imported run manifests and skip unchanged imports.
- Create: `corpscout/scheduler/internal/companysources/runindex/index_test.go`
  - Unit tests for changed/unchanged import decisions.
- Create: `corpscout/scheduler/internal/clickhouseclient/url.go`
  - Parse ClickHouse native URLs.
- Create: `corpscout/scheduler/internal/clickhouseclient/json_each_row.go`
  - Build insert queries, serialize row batches, and run `clickhouse-client`.
- Create: `corpscout/scheduler/internal/clickhouseclient/json_each_row_test.go`
  - Unit tests for insert query rendering and JSONEachRow encoding.
- Create: `corpscout/scheduler/internal/companysources/source.go`
  - Source key and import option/result structs.
- Create: `corpscout/scheduler/internal/companysources/registry.go`
  - Concrete source registry.
- Create: `corpscout/scheduler/internal/companysources/registry_test.go`
  - Registry lookup tests.
- Create: `corpscout/scheduler/internal/companysources/importer.go`
  - Reusable import-run and import-runs orchestration for CLI and Temporal.
- Create: `corpscout/scheduler/internal/companysources/importer_test.go`
  - Unit tests for orchestration and changed-only skipping.
- Create: `corpscout/scheduler/internal/companysources/finland/prhytj/types.go`
  - Raw PRH YTJ input structs copied/adapted from the current companysource package.
- Create: `corpscout/scheduler/internal/companysources/finland/prhytj/parser.go`
  - Stream `source.ndjson` records and preserve raw payload hashes.
- Create: `corpscout/scheduler/internal/companysources/finland/prhytj/parser_test.go`
  - Parser tests using copied small fixtures.
- Create: `corpscout/scheduler/internal/companysources/finland/prhytj/import.go`
  - Map parsed records to ClickHouse JSONEachRow batches.
- Create: `corpscout/scheduler/internal/companysources/finland/prhytj/import_test.go`
  - Mapper and insert-column tests.
- Create: `corpscout/scheduler/cmd/corpscout-source/main.go`
  - CLI for listing, importing, and migration up.
- Create: `corpscout/scheduler/cmd/corpscout-source/main_test.go`
  - CLI config parsing tests.
- Modify: `corpscout/Makefile`
  - Add `source-list`, `source-import-run`, `source-import-changed`; remove obsolete Finland Parquet ClickHouse targets.
- Modify: `companies/companysource/internal/cli/config.go`
  - Remove ClickHouse command modes after Corpscout Finland import works.
- Modify: `companies/companysource/internal/cli/config_test.go`
  - Update expected commands.
- Modify: `companies/companysource/internal/cli/run.go`
  - Remove ClickHouse generation/import dispatch.
- Modify: `companies/companysource/sources/finland/prhytj/source.go`
  - Remove embedded ClickHouse config methods after Corpscout path is verified.
- Delete: `companies/companysource/sources/finland/prhytj/clickhouse.yaml`
  - Finland ClickHouse schema lives only in manual Corpscout migrations.

## Task 1: Remove `corpscout_projection`

**Files:**
- Modify: `corpscout/clickhouse/migrations/000001_create_databases.up.sql`
- Modify: `corpscout/clickhouse/migrations/000001_create_databases.down.sql`
- Create: `corpscout/scheduler/internal/db/clickhouse_migrations_test.go`

- [ ] **Step 1: Write the failing migration shape test**

Create `corpscout/scheduler/internal/db/clickhouse_migrations_test.go`:

```go
package db

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClickHouseInitialMigrationOnlyCreatesSourcesDatabase(t *testing.T) {
	upPath := filepath.Join("..", "..", "..", "clickhouse", "migrations", "000001_create_databases.up.sql")
	downPath := filepath.Join("..", "..", "..", "clickhouse", "migrations", "000001_create_databases.down.sql")

	up, err := os.ReadFile(upPath)
	require.NoError(t, err)
	down, err := os.ReadFile(downPath)
	require.NoError(t, err)

	require.Contains(t, string(up), "CREATE DATABASE IF NOT EXISTS corpscout_sources")
	require.NotContains(t, string(up), "corpscout_projection")
	require.Equal(t, 1, strings.Count(string(up), "CREATE DATABASE"))
	require.Contains(t, string(down), "DROP DATABASE IF EXISTS corpscout_sources")
	require.NotContains(t, string(down), "corpscout_projection")
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/db -run TestClickHouseInitialMigrationOnlyCreatesSourcesDatabase -count=1 -v
```

Expected: FAIL because `000001_create_databases.up.sql` still creates `corpscout_projection`.

- [ ] **Step 3: Update the ClickHouse initial migrations**

Change `corpscout/clickhouse/migrations/000001_create_databases.up.sql` to:

```sql
CREATE DATABASE IF NOT EXISTS corpscout_sources;
```

Change `corpscout/clickhouse/migrations/000001_create_databases.down.sql` to:

```sql
DROP DATABASE IF EXISTS corpscout_sources;
```

- [ ] **Step 4: Run the test to verify it passes**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/db -run TestClickHouseInitialMigrationOnlyCreatesSourcesDatabase -count=1 -v
```

Expected: PASS.

- [ ] **Step 5: Verify remote migration state is not changed by this task**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
make -n clickhouse-migrate-up
```

Expected: printed command still targets `clickhouse://companycollect:9002` and includes `--add-host companycollect:100.85.212.113`. Do not apply migration in this task because remote version `1` has already run; changing an applied migration requires manual remote migration cleanup later.

- [ ] **Step 6: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/clickhouse/migrations/000001_create_databases.up.sql \
        corpscout/clickhouse/migrations/000001_create_databases.down.sql \
        corpscout/scheduler/internal/db/clickhouse_migrations_test.go
git commit -m "fix: remove unused clickhouse projection database"
```

## Task 2: Add Raw Run Manifest Helpers

**Files:**
- Create: `corpscout/scheduler/internal/companysources/runmanifest/manifest.go`
- Create: `corpscout/scheduler/internal/companysources/runmanifest/manifest_test.go`

- [ ] **Step 1: Write failing manifest tests**

Create `corpscout/scheduler/internal/companysources/runmanifest/manifest_test.go`:

```go
package runmanifest

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWriteReadAndHashManifest(t *testing.T) {
	runDir := t.TempDir()
	downloadedAt := time.Date(2026, 6, 9, 12, 0, 0, 0, time.UTC)
	manifest := Manifest{
		Country:      "finland",
		Source:       "prhytj",
		RunID:        "20260609T120000Z-prhytj",
		DownloadedAt: downloadedAt,
		Files: []File{
			{Path: "raw/source.ndjson", Kind: "ndjson", Rows: 2, SHA256: "abc123"},
		},
	}

	require.NoError(t, Write(runDir, manifest))

	loaded, err := Read(runDir)
	require.NoError(t, err)
	require.Equal(t, manifest, loaded)

	hash1, err := Hash(runDir)
	require.NoError(t, err)
	require.Len(t, hash1, 64)

	require.NoError(t, os.WriteFile(filepath.Join(runDir, "manifest.json"), []byte(`{"country":"finland"}`), 0o644))
	hash2, err := Hash(runDir)
	require.NoError(t, err)
	require.NotEqual(t, hash1, hash2)
}

func TestLatestCompletedRunChoosesNewestManifest(t *testing.T) {
	root := t.TempDir()
	first := filepath.Join(root, "finland", "prhytj", "runs", "20260608T201348Z-prhytj")
	second := filepath.Join(root, "finland", "prhytj", "runs", "20260609T120000Z-prhytj")
	require.NoError(t, Write(first, Manifest{Country: "finland", Source: "prhytj", RunID: filepath.Base(first)}))
	require.NoError(t, Write(second, Manifest{Country: "finland", Source: "prhytj", RunID: filepath.Base(second)}))

	runDir, manifest, err := LatestCompletedRun(root, "finland", "prhytj")
	require.NoError(t, err)
	require.Equal(t, second, runDir)
	require.Equal(t, "20260609T120000Z-prhytj", manifest.RunID)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/companysources/runmanifest -count=1 -v
```

Expected: FAIL because package `runmanifest` does not exist.

- [ ] **Step 3: Implement manifest helpers**

Create `corpscout/scheduler/internal/companysources/runmanifest/manifest.go`:

```go
package runmanifest

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/cockroachdb/errors"
)

const FileName = "manifest.json"

type Manifest struct {
	Country      string    `json:"country"`
	Source       string    `json:"source"`
	RunID        string    `json:"run_id"`
	DownloadedAt time.Time `json:"downloaded_at"`
	Files        []File    `json:"files"`
}

type File struct {
	Path   string `json:"path"`
	Kind   string `json:"kind"`
	Rows   int64  `json:"rows"`
	SHA256 string `json:"sha256"`
}

func Write(runDir string, manifest Manifest) error {
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		return errors.Wrap(err, "create run directory")
	}
	body, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return errors.Wrap(err, "marshal manifest")
	}
	body = append(body, '\n')
	if err := os.WriteFile(filepath.Join(runDir, FileName), body, 0o644); err != nil {
		return errors.Wrap(err, "write manifest")
	}
	return nil
}

func Read(runDir string) (Manifest, error) {
	body, err := os.ReadFile(filepath.Join(runDir, FileName))
	if err != nil {
		return Manifest{}, errors.Wrap(err, "read manifest")
	}
	var manifest Manifest
	if err := json.Unmarshal(body, &manifest); err != nil {
		return Manifest{}, errors.Wrap(err, "decode manifest")
	}
	return manifest, nil
}

func Hash(runDir string) (string, error) {
	body, err := os.ReadFile(filepath.Join(runDir, FileName))
	if err != nil {
		return "", errors.Wrap(err, "read manifest for hash")
	}
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:]), nil
}

func LatestCompletedRun(root string, country string, source string) (string, Manifest, error) {
	runsDir := filepath.Join(root, country, source, "runs")
	entries, err := os.ReadDir(runsDir)
	if err != nil {
		return "", Manifest{}, errors.Wrap(err, "read source runs directory")
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			names = append(names, entry.Name())
		}
	}
	sort.Strings(names)
	for i := len(names) - 1; i >= 0; i-- {
		runDir := filepath.Join(runsDir, names[i])
		manifest, err := Read(runDir)
		if err == nil {
			return runDir, manifest, nil
		}
		if !os.IsNotExist(errors.Cause(err)) {
			return "", Manifest{}, err
		}
	}
	return "", Manifest{}, errors.Errorf("no completed run for %s/%s under %s", country, source, root)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/companysources/runmanifest -count=1 -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/scheduler/internal/companysources/runmanifest
git commit -m "feat: add source run manifest helpers"
```

## Task 3: Add Run Import Index

**Files:**
- Create: `corpscout/scheduler/internal/companysources/runindex/index.go`
- Create: `corpscout/scheduler/internal/companysources/runindex/index_test.go`

- [ ] **Step 1: Write failing run-index tests**

Create `corpscout/scheduler/internal/companysources/runindex/index_test.go`:

```go
package runindex

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestShouldImportWhenRunIsMissingOrChanged(t *testing.T) {
	index := Index{}
	require.True(t, index.ShouldImport("finland", "prhytj", "run-1", "manifest-a", []string{"file-a"}))

	index.MarkImported(Entry{
		Country:        "finland",
		Source:         "prhytj",
		RunID:          "run-1",
		ManifestHash:   "manifest-a",
		RawFileHashes:  []string{"file-a"},
		SourceExportID: "export-1",
		ImportedAt:     time.Date(2026, 6, 9, 12, 0, 0, 0, time.UTC),
		Status:         "imported",
	})

	require.False(t, index.ShouldImport("finland", "prhytj", "run-1", "manifest-a", []string{"file-a"}))
	require.True(t, index.ShouldImport("finland", "prhytj", "run-1", "manifest-a", []string{"file-b"}))
	require.True(t, index.ShouldImport("finland", "prhytj", "run-2", "manifest-b", []string{"file-a"}))
}

func TestSaveAndLoadIndex(t *testing.T) {
	path := filepath.Join(t.TempDir(), "run-index.lock.yaml")
	index := Index{}
	index.MarkImported(Entry{
		Country:       "finland",
		Source:        "prhytj",
		RunID:         "run-1",
		ManifestHash:  "manifest-a",
		RawFileHashes: []string{"file-a", "file-b"},
		Status:        "imported",
	})

	require.NoError(t, Save(path, index))
	loaded, err := Load(path)
	require.NoError(t, err)
	require.False(t, loaded.ShouldImport("finland", "prhytj", "run-1", "manifest-a", []string{"file-a", "file-b"}))
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/companysources/runindex -count=1 -v
```

Expected: FAIL because package `runindex` does not exist.

- [ ] **Step 3: Implement run index**

Create `corpscout/scheduler/internal/companysources/runindex/index.go`:

```go
package runindex

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"gopkg.in/yaml.v3"
)

type Index struct {
	Entries []Entry `yaml:"entries"`
}

type Entry struct {
	Country        string    `yaml:"country"`
	Source         string    `yaml:"source"`
	RunID          string    `yaml:"run_id"`
	ManifestHash   string    `yaml:"manifest_hash"`
	RawFileHashes  []string  `yaml:"raw_file_hashes"`
	SourceExportID string    `yaml:"source_export_id"`
	ImportedAt     time.Time `yaml:"imported_at"`
	Status         string    `yaml:"status"`
}

func Load(path string) (Index, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return Index{}, nil
		}
		return Index{}, errors.Wrap(err, "read run index")
	}
	var index Index
	if err := yaml.Unmarshal(body, &index); err != nil {
		return Index{}, errors.Wrap(err, "decode run index")
	}
	return index, nil
}

func Save(path string, index Index) error {
	body, err := yaml.Marshal(index)
	if err != nil {
		return errors.Wrap(err, "encode run index")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return errors.Wrap(err, "create run index directory")
	}
	if err := os.WriteFile(path, body, 0o644); err != nil {
		return errors.Wrap(err, "write run index")
	}
	return nil
}

func (i Index) ShouldImport(country string, source string, runID string, manifestHash string, rawFileHashes []string) bool {
	entry, ok := i.find(country, source)
	if !ok {
		return true
	}
	return entry.RunID != runID ||
		entry.ManifestHash != manifestHash ||
		joinHashes(entry.RawFileHashes) != joinHashes(rawFileHashes) ||
		entry.Status != "imported"
}

func (i *Index) MarkImported(entry Entry) {
	for idx := range i.Entries {
		if i.Entries[idx].Country == entry.Country && i.Entries[idx].Source == entry.Source {
			i.Entries[idx] = normalized(entry)
			return
		}
	}
	i.Entries = append(i.Entries, normalized(entry))
	sort.Slice(i.Entries, func(a, b int) bool {
		left := i.Entries[a].Country + "/" + i.Entries[a].Source
		right := i.Entries[b].Country + "/" + i.Entries[b].Source
		return left < right
	})
}

func (i Index) find(country string, source string) (Entry, bool) {
	for _, entry := range i.Entries {
		if entry.Country == country && entry.Source == source {
			return entry, true
		}
	}
	return Entry{}, false
}

func normalized(entry Entry) Entry {
	sort.Strings(entry.RawFileHashes)
	return entry
}

func joinHashes(values []string) string {
	copyValues := append([]string(nil), values...)
	sort.Strings(copyValues)
	return strings.Join(copyValues, "\n")
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/companysources/runindex -count=1 -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/scheduler/internal/companysources/runindex
git commit -m "feat: track source clickhouse import runs"
```

## Task 4: Add ClickHouse JSONEachRow Writer

**Files:**
- Create: `corpscout/scheduler/internal/clickhouseclient/url.go`
- Create: `corpscout/scheduler/internal/clickhouseclient/json_each_row.go`
- Create: `corpscout/scheduler/internal/clickhouseclient/json_each_row_test.go`

- [ ] **Step 1: Write failing writer tests**

Create `corpscout/scheduler/internal/clickhouseclient/json_each_row_test.go`:

```go
package clickhouseclient

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseNativeURL(t *testing.T) {
	target, err := ParseNativeURL("clickhouse://companycollect:9002?username=default&password=change-me&database=corpscout_sources")
	require.NoError(t, err)
	require.Equal(t, Target{
		Host:     "companycollect",
		Port:     "9002",
		Username: "default",
		Password: "change-me",
		Database: "corpscout_sources",
	}, target)
}

func TestBuildInsertQuery(t *testing.T) {
	query := BuildInsertQuery("corpscout_sources", "fi_prhytj_companies", []string{"business_id", "legal_name"})
	require.Equal(t, "INSERT INTO `corpscout_sources`.`fi_prhytj_companies` (`business_id`, `legal_name`) FORMAT JSONEachRow", query)
}

func TestEncodeJSONEachRow(t *testing.T) {
	body, err := EncodeJSONEachRow([]map[string]any{
		{"business_id": "0100130-4", "legal_name": "Dynava Oy"},
		{"business_id": "0112038-9", "legal_name": "Example"},
	})
	require.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(string(body)), "\n")
	require.Len(t, lines, 2)
	require.JSONEq(t, `{"business_id":"0100130-4","legal_name":"Dynava Oy"}`, lines[0])
	require.JSONEq(t, `{"business_id":"0112038-9","legal_name":"Example"}`, lines[1])
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/clickhouseclient -count=1 -v
```

Expected: FAIL because package `clickhouseclient` does not exist.

- [ ] **Step 3: Implement URL parsing and JSONEachRow helpers**

Create `corpscout/scheduler/internal/clickhouseclient/url.go`:

```go
package clickhouseclient

import (
	"net/url"
	"strings"

	"github.com/cockroachdb/errors"
)

type Target struct {
	Host     string
	Port     string
	Username string
	Password string
	Database string
}

func ParseNativeURL(rawURL string) (Target, error) {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return Target{}, errors.Wrap(err, "parse clickhouse native url")
	}
	if parsed.Scheme != "clickhouse" {
		return Target{}, errors.Errorf("clickhouse native url must use clickhouse scheme, got %q", parsed.Scheme)
	}
	target := Target{
		Host:     parsed.Hostname(),
		Port:     parsed.Port(),
		Username: parsed.Query().Get("username"),
		Password: parsed.Query().Get("password"),
		Database: parsed.Query().Get("database"),
	}
	if parsed.User != nil {
		if target.Username == "" {
			target.Username = parsed.User.Username()
		}
		if password, ok := parsed.User.Password(); ok && target.Password == "" {
			target.Password = password
		}
	}
	if target.Port == "" {
		target.Port = "9000"
	}
	if target.Username == "" {
		target.Username = "default"
	}
	if target.Database == "" {
		target.Database = strings.TrimPrefix(parsed.EscapedPath(), "/")
	}
	if target.Host == "" {
		return Target{}, errors.New("clickhouse native url host is required")
	}
	if target.Database == "" {
		return Target{}, errors.New("clickhouse native url database is required")
	}
	return target, nil
}
```

Create `corpscout/scheduler/internal/clickhouseclient/json_each_row.go`:

```go
package clickhouseclient

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"strings"

	"github.com/cockroachdb/errors"
)

const DefaultClickHouseImage = "clickhouse/clickhouse-server:26.5"
const DefaultCompanycollectHostIP = "100.85.212.113"

type Insert struct {
	Database string
	Table    string
	Columns  []string
	Rows     []map[string]any
}

func BuildInsertQuery(database string, table string, columns []string) string {
	quotedColumns := make([]string, 0, len(columns))
	for _, column := range columns {
		quotedColumns = append(quotedColumns, quoteIdent(column))
	}
	return "INSERT INTO " + quoteIdent(database) + "." + quoteIdent(table) + " (" + strings.Join(quotedColumns, ", ") + ") FORMAT JSONEachRow"
}

func EncodeJSONEachRow(rows []map[string]any) ([]byte, error) {
	var body bytes.Buffer
	for _, row := range rows {
		encoded, err := json.Marshal(row)
		if err != nil {
			return nil, errors.Wrap(err, "encode clickhouse JSONEachRow")
		}
		body.Write(encoded)
		body.WriteByte('\n')
	}
	return body.Bytes(), nil
}

func ExecuteInsert(ctx context.Context, nativeURL string, image string, insert Insert) error {
	target, err := ParseNativeURL(nativeURL)
	if err != nil {
		return err
	}
	if strings.TrimSpace(image) == "" {
		image = DefaultClickHouseImage
	}
	body, err := EncodeJSONEachRow(insert.Rows)
	if err != nil {
		return err
	}
	args := clickHouseClientDockerArgs(image, target, BuildInsertQuery(insert.Database, insert.Table, insert.Columns))
	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Stdin = bytes.NewReader(body)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return errors.Wrapf(err, "run clickhouse insert stderr=%s", strings.TrimSpace(stderr.String()))
	}
	return nil
}

func clickHouseClientDockerArgs(image string, target Target, query string) []string {
	args := []string{"run", "--rm", "-i", "--add-host", "host.docker.internal:host-gateway"}
	if target.Host == "companycollect" {
		hostIP := strings.TrimSpace(os.Getenv("COMPANYCOLLECT_HOST_IP"))
		if hostIP == "" {
			hostIP = DefaultCompanycollectHostIP
		}
		args = append(args, "--add-host", "companycollect:"+hostIP)
	}
	args = append(args,
		image,
		"clickhouse-client",
		"--host", target.Host,
		"--port", target.Port,
		"--user", target.Username,
		"--database", target.Database,
	)
	if target.Password != "" {
		args = append(args, "--password", target.Password)
	}
	args = append(args, "--query", query)
	return args
}

func quoteIdent(value string) string {
	escaped := strings.NewReplacer(`\`, `\\`, "`", "\\`").Replace(value)
	return "`" + escaped + "`"
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/clickhouseclient -count=1 -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/scheduler/internal/clickhouseclient
git commit -m "feat: add clickhouse JSONEachRow writer"
```

## Task 5: Add Source Registry And CLI Skeleton

**Files:**
- Create: `corpscout/scheduler/internal/companysources/source.go`
- Create: `corpscout/scheduler/internal/companysources/registry.go`
- Create: `corpscout/scheduler/internal/companysources/registry_test.go`
- Create: `corpscout/scheduler/internal/companysources/importer.go`
- Create: `corpscout/scheduler/internal/companysources/importer_test.go`
- Create: `corpscout/scheduler/cmd/corpscout-source/main.go`
- Create: `corpscout/scheduler/cmd/corpscout-source/main_test.go`

- [ ] **Step 1: Write failing registry tests**

Create `corpscout/scheduler/internal/companysources/registry_test.go`:

```go
package companysources

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

type fakeSource struct {
	key Key
}

func (s fakeSource) Key() Key { return s.key }
func (s fakeSource) DisplayName() string { return "Fake Source" }
func (s fakeSource) Import(context.Context, ImportOptions) (ImportResult, error) {
	return ImportResult{}, nil
}

func TestRegistryLookupAndKeys(t *testing.T) {
	registry := NewRegistry(fakeSource{key: Key{Country: "finland", Source: "prhytj"}})

	source, err := registry.Get("finland", "prhytj")
	require.NoError(t, err)
	require.Equal(t, Key{Country: "finland", Source: "prhytj"}, source.Key())
	require.Equal(t, []string{"finland/prhytj"}, registry.Keys())

	_, err = registry.Get("norway", "brreg")
	require.EqualError(t, err, "unknown company source norway/brreg")
}
```

- [ ] **Step 2: Implement registry**

Create `corpscout/scheduler/internal/companysources/source.go`:

```go
package companysources

import (
	"context"
)

type Key struct {
	Country string
	Source  string
}

type Source interface {
	Key() Key
	DisplayName() string
	Import(ctx context.Context, opts ImportOptions) (ImportResult, error)
}

type ImportOptions struct {
	RunDir              string
	ClickHouseNativeURL string
	ClickHouseImage     string
	BatchSize           int
	Limit               int64
	Truncate            bool
}

type ImportResult struct {
	RunDir         string
	ImportedTables []string
	ImportedRows   int64
}

type ImportRunRequest struct {
	Country             string
	Source              string
	RunDir              string
	ClickHouseNativeURL string
	ClickHouseImage     string
	BatchSize           int
	Limit               int64
}

type ImportChangedRunsRequest struct {
	RunsRoot            string
	RunIndexPath        string
	ClickHouseNativeURL string
	ClickHouseImage     string
	BatchSize           int
	Limit               int64
	ChangedOnly         bool
}

type ImportChangedRunsResult struct {
	Sources []ImportChangedSourceResult
}

type ImportChangedSourceResult struct {
	Source       string
	RunID        string
	Status       string
	ImportedRows int64
}
```

Create `corpscout/scheduler/internal/companysources/registry.go`:

```go
package companysources

import (
	"sort"

	"github.com/cockroachdb/errors"
)

type Registry struct {
	sources map[string]Source
}

func NewRegistry(sources ...Source) Registry {
	byKey := make(map[string]Source, len(sources))
	for _, source := range sources {
		key := source.Key()
		byKey[key.Country+"/"+key.Source] = source
	}
	return Registry{sources: byKey}
}

func (r Registry) Get(country string, source string) (Source, error) {
	key := country + "/" + source
	value, ok := r.sources[key]
	if !ok {
		return nil, errors.Errorf("unknown company source %s", key)
	}
	return value, nil
}

func (r Registry) Keys() []string {
	keys := make([]string, 0, len(r.sources))
	for key := range r.sources {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
```

- [ ] **Step 3: Add reusable import orchestration tests**

Create `corpscout/scheduler/internal/companysources/importer_test.go`:

```go
package companysources

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/pulsarpoint/corpscout/scheduler/internal/companysources/runindex"
	"github.com/pulsarpoint/corpscout/scheduler/internal/companysources/runmanifest"
	"github.com/stretchr/testify/require"
)

type importingSource struct {
	key Key
}

func (s importingSource) Key() Key { return s.key }
func (s importingSource) DisplayName() string { return "Importing Source" }
func (s importingSource) Import(ctx context.Context, opts ImportOptions) (ImportResult, error) {
	return ImportResult{RunDir: opts.RunDir, ImportedTables: []string{"table"}, ImportedRows: 12}, nil
}

func TestImportRunCallsRegisteredSource(t *testing.T) {
	registry := NewRegistry(importingSource{key: Key{Country: "finland", Source: "prhytj"}})

	result, err := ImportRun(context.Background(), registry, ImportRunRequest{
		Country:             "finland",
		Source:              "prhytj",
		RunDir:              "/tmp/run",
		ClickHouseNativeURL: "clickhouse://companycollect:9002?username=default&database=corpscout_sources",
	})

	require.NoError(t, err)
	require.Equal(t, int64(12), result.ImportedRows)
}

func TestImportChangedRunsSkipsUnchangedRun(t *testing.T) {
	root := t.TempDir()
	runDir := filepath.Join(root, "finland", "prhytj", "runs", "20260609T120000Z-prhytj")
	manifest := runmanifest.Manifest{
		Country: "finland",
		Source:  "prhytj",
		RunID:   "20260609T120000Z-prhytj",
		Files:   []runmanifest.File{{Path: "raw/source.ndjson", Kind: "ndjson", Rows: 1, SHA256: "file-a"}},
	}
	require.NoError(t, runmanifest.Write(runDir, manifest))
	manifestHash, err := runmanifest.Hash(runDir)
	require.NoError(t, err)
	indexPath := filepath.Join(root, "run-index.lock.yaml")
	index := runindex.Index{}
	index.MarkImported(runindex.Entry{
		Country:       "finland",
		Source:        "prhytj",
		RunID:         manifest.RunID,
		ManifestHash:  manifestHash,
		RawFileHashes: []string{"file-a"},
		ImportedAt:    time.Date(2026, 6, 9, 12, 0, 0, 0, time.UTC),
		Status:        "imported",
	})
	require.NoError(t, runindex.Save(indexPath, index))

	registry := NewRegistry(importingSource{key: Key{Country: "finland", Source: "prhytj"}})
	result, err := ImportChangedRuns(context.Background(), registry, ImportChangedRunsRequest{
		RunsRoot:            root,
		RunIndexPath:        indexPath,
		ClickHouseNativeURL: "clickhouse://companycollect:9002?username=default&database=corpscout_sources",
		ChangedOnly:         true,
	})

	require.NoError(t, err)
	require.Equal(t, []ImportChangedSourceResult{{Source: "finland/prhytj", RunID: manifest.RunID, Status: "skipped"}}, result.Sources)
}
```

- [ ] **Step 4: Implement reusable import orchestration**

Create `corpscout/scheduler/internal/companysources/importer.go`:

```go
package companysources

import (
	"context"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/pulsarpoint/corpscout/scheduler/internal/companysources/runindex"
	"github.com/pulsarpoint/corpscout/scheduler/internal/companysources/runmanifest"
)

func ImportRun(ctx context.Context, registry Registry, req ImportRunRequest) (ImportResult, error) {
	source, err := registry.Get(req.Country, req.Source)
	if err != nil {
		return ImportResult{}, err
	}
	return source.Import(ctx, ImportOptions{
		RunDir:              req.RunDir,
		ClickHouseNativeURL: req.ClickHouseNativeURL,
		ClickHouseImage:     req.ClickHouseImage,
		BatchSize:           req.BatchSize,
		Limit:               req.Limit,
	})
}

func ImportChangedRuns(ctx context.Context, registry Registry, req ImportChangedRunsRequest) (ImportChangedRunsResult, error) {
	index, err := runindex.Load(req.RunIndexPath)
	if err != nil {
		return ImportChangedRunsResult{}, err
	}
	var summaries []ImportChangedSourceResult
	for _, key := range registry.Keys() {
		country, sourceKey, ok := strings.Cut(key, "/")
		if !ok {
			return ImportChangedRunsResult{}, errors.Errorf("invalid source key %s", key)
		}
		runDir, manifest, err := runmanifest.LatestCompletedRun(req.RunsRoot, country, sourceKey)
		if err != nil {
			return ImportChangedRunsResult{}, err
		}
		manifestHash, err := runmanifest.Hash(runDir)
		if err != nil {
			return ImportChangedRunsResult{}, err
		}
		rawHashes := make([]string, 0, len(manifest.Files))
		for _, file := range manifest.Files {
			rawHashes = append(rawHashes, file.SHA256)
		}
		if req.ChangedOnly && !index.ShouldImport(country, sourceKey, manifest.RunID, manifestHash, rawHashes) {
			summaries = append(summaries, ImportChangedSourceResult{Source: key, RunID: manifest.RunID, Status: "skipped"})
			continue
		}
		result, err := ImportRun(ctx, registry, ImportRunRequest{
			Country:             country,
			Source:              sourceKey,
			RunDir:              runDir,
			ClickHouseNativeURL: req.ClickHouseNativeURL,
			ClickHouseImage:     req.ClickHouseImage,
			BatchSize:           req.BatchSize,
			Limit:               req.Limit,
		})
		if err != nil {
			return ImportChangedRunsResult{}, err
		}
		index.MarkImported(runindex.Entry{
			Country:       country,
			Source:        sourceKey,
			RunID:         manifest.RunID,
			ManifestHash:  manifestHash,
			RawFileHashes: rawHashes,
			ImportedAt:    time.Now().UTC(),
			Status:        "imported",
		})
		summaries = append(summaries, ImportChangedSourceResult{Source: key, RunID: manifest.RunID, Status: "imported", ImportedRows: result.ImportedRows})
	}
	if err := runindex.Save(req.RunIndexPath, index); err != nil {
		return ImportChangedRunsResult{}, err
	}
	return ImportChangedRunsResult{Sources: summaries}, nil
}
```

- [ ] **Step 5: Run registry and orchestration tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/companysources -count=1 -v
```

Expected: PASS.

- [ ] **Step 6: Add CLI skeleton tests**

Create `corpscout/scheduler/cmd/corpscout-source/main_test.go`:

```go
package main

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestListSourcesCommand(t *testing.T) {
	var output bytes.Buffer
	err := run([]string{"list-sources"}, &output)
	require.NoError(t, err)
	require.Contains(t, output.String(), "finland/prhytj")
}

func TestUnknownCommandFails(t *testing.T) {
	var output bytes.Buffer
	err := run([]string{"unknown"}, &output)
	require.EqualError(t, err, "unknown command unknown")
}
```

- [ ] **Step 7: Implement CLI skeleton**

Create `corpscout/scheduler/cmd/corpscout-source/main.go`:

```go
package main

import (
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/cockroachdb/errors"
	"github.com/pulsarpoint/corpscout/scheduler/internal/companysources"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(os.Args[1:], os.Stdout); err != nil {
		logger.Error("corpscout source command failed", "error", err)
		os.Exit(1)
	}
}

func run(args []string, output io.Writer) error {
	if len(args) == 0 {
		return errors.New("command is required")
	}
	registry := defaultRegistry()
	switch args[0] {
	case "list-sources":
		for _, key := range registry.Keys() {
			if _, err := fmt.Fprintln(output, key); err != nil {
				return errors.Wrap(err, "write source list")
			}
		}
		return nil
	default:
		return errors.Errorf("unknown command %s", args[0])
	}
}

func defaultRegistry() companysources.Registry {
	return companysources.NewRegistry(finlandPRHYTJTemporary{})
}

type finlandPRHYTJTemporary struct{}

func (finlandPRHYTJTemporary) Key() companysources.Key {
	return companysources.Key{Country: "finland", Source: "prhytj"}
}

func (finlandPRHYTJTemporary) DisplayName() string { return "PRH Open Data YTJ API v3 companies" }
func (finlandPRHYTJTemporary) Import(ctx context.Context, opts companysources.ImportOptions) (companysources.ImportResult, error) {
	return companysources.ImportResult{}, errors.New("finland/prhytj import is not wired yet")
}
```

Add the missing import to `main.go`:

```go
import "context"
```

- [ ] **Step 8: Run CLI tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./cmd/corpscout-source ./internal/companysources -count=1 -v
```

Expected: PASS.

- [ ] **Step 9: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/scheduler/internal/companysources \
        corpscout/scheduler/cmd/corpscout-source
git commit -m "feat: add corpscout source registry CLI"
```

## Task 6: Move Finland PRH YTJ Parser Into Corpscout

**Files:**
- Create: `corpscout/scheduler/internal/companysources/finland/prhytj/types.go`
- Create: `corpscout/scheduler/internal/companysources/finland/prhytj/parser.go`
- Create: `corpscout/scheduler/internal/companysources/finland/prhytj/parser_test.go`
- Copy fixtures from: `companies/companysource/sources/finland/prhytj/testdata/`

- [ ] **Step 1: Copy Finland PRH fixtures**

Run:

```bash
mkdir -p /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler/internal/companysources/finland/prhytj/testdata
cp /Users/graovic/pulsarpoint/ppoint/companycollect/companies/companysource/sources/finland/prhytj/testdata/prh_snapshot_mixed.ndjson \
   /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler/internal/companysources/finland/prhytj/testdata/prh_snapshot_mixed.ndjson
```

- [ ] **Step 2: Write failing parser tests**

Create `corpscout/scheduler/internal/companysources/finland/prhytj/parser_test.go`:

```go
package prhytj

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseSnapshotPreservesRawPayloadAndHash(t *testing.T) {
	var records []CompanyRecord
	err := ParseSnapshot(context.Background(), "testdata/prh_snapshot_mixed.ndjson", func(record CompanyRecord) error {
		records = append(records, record)
		return nil
	})
	require.NoError(t, err)
	require.NotEmpty(t, records)
	require.NotEmpty(t, records[0].BusinessID.Value)
	require.NotEmpty(t, records[0].RawPayload)
	require.Len(t, records[0].PayloadHash, 64)
}
```

- [ ] **Step 3: Copy/adapt PRH input types**

Create `corpscout/scheduler/internal/companysources/finland/prhytj/types.go` by copying the current structs from `companies/companysource/sources/finland/prhytj/types.go`, changing only the package line:

```go
package prhytj
```

Keep these constants:

```go
const (
	SourceKey      = "prhytj"
	SourceSlug     = "finland_prh_ytj_v3"
	SourceName     = "PRH Open Data YTJ API v3 companies"
	DefaultBaseURL = "https://avoindata.prh.fi/opendata-ytj-api/v3/companies"
)
```

- [ ] **Step 4: Implement streaming parser**

Create `corpscout/scheduler/internal/companysources/finland/prhytj/parser.go`:

```go
package prhytj

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"

	"github.com/cockroachdb/errors"
)

func ParseSnapshot(ctx context.Context, path string, handle func(CompanyRecord) error) error {
	file, err := os.Open(path)
	if err != nil {
		return errors.Wrap(err, "open PRH YTJ snapshot")
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return err
		}
		raw := append([]byte(nil), scanner.Bytes()...)
		var record CompanyRecord
		if err := json.Unmarshal(raw, &record); err != nil {
			return errors.Wrap(err, "decode PRH YTJ record")
		}
		sum := sha256.Sum256(raw)
		record.RawPayload = raw
		record.PayloadHash = hex.EncodeToString(sum[:])
		if err := handle(record); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return errors.Wrap(err, "scan PRH YTJ snapshot")
	}
	return nil
}
```

- [ ] **Step 5: Run parser tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/companysources/finland/prhytj -run TestParseSnapshot -count=1 -v
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/scheduler/internal/companysources/finland/prhytj
git commit -m "feat: parse Finland PRH YTJ raw snapshots in corpscout"
```

## Task 7: Add Finland PRH Direct ClickHouse Import

**Files:**
- Create: `corpscout/scheduler/internal/companysources/finland/prhytj/import.go`
- Create: `corpscout/scheduler/internal/companysources/finland/prhytj/import_test.go`

- [ ] **Step 1: Write failing mapper tests**

Create `corpscout/scheduler/internal/companysources/finland/prhytj/import_test.go`:

```go
package prhytj

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompanyRowMapping(t *testing.T) {
	raw := []byte(`{
		"businessId":{"type":"BusinessId","value":"0100130-4"},
		"euId":{"type":"EUID","value":"FIFPRO.0100130-4"},
		"names":[{"name":"Dynava Oy","type":"1","registrationDate":"2021-01-01"}],
		"mainBusinessLine":{"type":"82200","typeCodeSet":"TOL2008","descriptions":[{"languageCode":"3","description":"Activities of call centres"}]},
		"website":{"url":"https://www.dynava.fi"},
		"companyForms":[{"type":"16","descriptions":[{"languageCode":"3","description":"Limited company"}]}],
		"tradeRegisterStatus":"1",
		"status":"2",
		"lastModified":"2026-01-01T00:00:00Z"
	}`)
	var record CompanyRecord
	require.NoError(t, json.Unmarshal(raw, &record))
	record.RawPayload = raw
	record.PayloadHash = "hash"

	row := companyRow("run-1", record)
	require.Equal(t, "FI", row["country_iso2"])
	require.Equal(t, "prhytj", row["source_slug"])
	require.Equal(t, "run-1", row["source_run_id"])
	require.Equal(t, "0100130-4", row["business_id"])
	require.Equal(t, "Dynava Oy", row["legal_name"])
	require.Equal(t, "FIFPRO.0100130-4", row["euid"])
	require.Equal(t, "https://www.dynava.fi", row["website_url"])
}

func TestCompanyColumnsMatchMigrationNames(t *testing.T) {
	require.Equal(t, []string{
		"country_iso2",
		"source_slug",
		"source_run_id",
		"source_record_id",
		"source_payload_hash",
		"source_updated_at",
		"business_id",
		"vat_id",
		"euid",
		"legal_name",
		"legal_name_normalized",
		"lifecycle_status",
		"is_active",
		"legal_form_code",
		"legal_form_label",
		"legal_form_label_en",
		"primary_industry_code",
		"primary_industry_code_set",
		"primary_industry_label",
		"primary_industry_label_en",
		"primary_nace_code",
		"primary_nace_revision",
		"website_url",
		"website_normalized_url",
		"website_host",
		"source_export_id",
		"ingested_at",
	}, companyColumns)
}

func TestRawRecordRowPreservesFullPayload(t *testing.T) {
	record := CompanyRecord{
		BusinessID: Identifier{Value: "0100130-4"},
		RawPayload: []byte(`{"businessId":{"value":"0100130-4"},"names":[{"name":"Dynava Oy"}]}`),
		PayloadHash: "hash",
	}

	row := rawRecordRow("run-1", "snapshot.ndjson", "snapshot-sha", 7, record)
	require.Equal(t, "0100130-4", row["business_id"])
	require.Equal(t, "hash", row["source_payload_hash"])
	require.Equal(t, `{"businessId":{"value":"0100130-4"},"names":[{"name":"Dynava Oy"}]}`, row["raw_payload_json"])
	require.Equal(t, int64(7), row["snapshot_line_number"])
}

func TestRawRecordColumnsMatchMigrationNames(t *testing.T) {
	require.Equal(t, []string{
		"country_iso2",
		"source_slug",
		"source_run_id",
		"source_record_id",
		"business_id",
		"source_payload_hash",
		"snapshot_path",
		"snapshot_sha256",
		"snapshot_line_number",
		"raw_payload_json",
		"schema_version",
		"exported_at",
		"source_export_id",
		"ingested_at",
	}, rawRecordColumns)
}
```

- [ ] **Step 2: Implement row mapping**

Create `corpscout/scheduler/internal/companysources/finland/prhytj/import.go`:

```go
package prhytj

import (
	"context"
	"net/url"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"github.com/pulsarpoint/corpscout/scheduler/internal/clickhouseclient"
	"github.com/pulsarpoint/corpscout/scheduler/internal/companysources"
)

const database = "corpscout_sources"
const rawRecordsTable = "fi_prhytj_raw_records"
const companiesTable = "fi_prhytj_companies"
const sourceExportSchemaVersion = "v1"

var rawRecordColumns = []string{
	"country_iso2",
	"source_slug",
	"source_run_id",
	"source_record_id",
	"business_id",
	"source_payload_hash",
	"snapshot_path",
	"snapshot_sha256",
	"snapshot_line_number",
	"raw_payload_json",
	"schema_version",
	"exported_at",
	"source_export_id",
	"ingested_at",
}

var companyColumns = []string{
	"country_iso2",
	"source_slug",
	"source_run_id",
	"source_record_id",
	"source_payload_hash",
	"source_updated_at",
	"business_id",
	"vat_id",
	"euid",
	"legal_name",
	"legal_name_normalized",
	"lifecycle_status",
	"is_active",
	"legal_form_code",
	"legal_form_label",
	"legal_form_label_en",
	"primary_industry_code",
	"primary_industry_code_set",
	"primary_industry_label",
	"primary_industry_label_en",
	"primary_nace_code",
	"primary_nace_revision",
	"website_url",
	"website_normalized_url",
	"website_host",
	"source_export_id",
	"ingested_at",
}

type Source struct{}

func (s Source) Key() companysources.Key {
	return companysources.Key{Country: "finland", Source: SourceKey}
}

func (s Source) DisplayName() string {
	return SourceName
}

func (s Source) Import(ctx context.Context, opts companysources.ImportOptions) (companysources.ImportResult, error) {
	if opts.RunDir == "" {
		return companysources.ImportResult{}, errors.New("run dir is required")
	}
	if opts.ClickHouseNativeURL == "" {
		return companysources.ImportResult{}, errors.New("clickhouse native url is required")
	}
	batchSize := opts.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}
	sourceExportID := uuid.NewString()
	snapshotPath := opts.RunDir + "/source.ndjson"
	rawRows := make([]map[string]any, 0, batchSize)
	companyRows := make([]map[string]any, 0, batchSize)
	var imported int64
	var lineNumber int64
	err := ParseSnapshot(ctx, snapshotPath, func(record CompanyRecord) error {
		if opts.Limit > 0 && imported >= opts.Limit {
			return nil
		}
		lineNumber++
		ingestedAt := time.Now().UTC().Format("2006-01-02 15:04:05.000")
		rawRow := rawRecordRow(opts.RunDir, snapshotPath, "", lineNumber, record)
		rawRow["source_export_id"] = sourceExportID
		rawRow["ingested_at"] = ingestedAt
		companyRow := companyRow(opts.RunDir, record)
		companyRow["source_export_id"] = sourceExportID
		companyRow["ingested_at"] = ingestedAt
		rawRows = append(rawRows, rawRow)
		companyRows = append(companyRows, companyRow)
		imported++
		if len(companyRows) < batchSize {
			return nil
		}
		if err := flushRows(ctx, opts, rawRecordsTable, rawRecordColumns, rawRows); err != nil {
			return err
		}
		if err := flushRows(ctx, opts, companiesTable, companyColumns, companyRows); err != nil {
			return err
		}
		rawRows = rawRows[:0]
		companyRows = companyRows[:0]
		return nil
	})
	if err != nil {
		return companysources.ImportResult{}, err
	}
	if len(companyRows) > 0 {
		if err := flushRows(ctx, opts, rawRecordsTable, rawRecordColumns, rawRows); err != nil {
			return companysources.ImportResult{}, err
		}
		if err := flushRows(ctx, opts, companiesTable, companyColumns, companyRows); err != nil {
			return companysources.ImportResult{}, err
		}
	}
	return companysources.ImportResult{
		RunDir:         opts.RunDir,
		ImportedTables: []string{rawRecordsTable, companiesTable},
		ImportedRows:   imported,
	}, nil
}

func flushRows(ctx context.Context, opts companysources.ImportOptions, table string, columns []string, rows []map[string]any) error {
	return clickhouseclient.ExecuteInsert(ctx, opts.ClickHouseNativeURL, opts.ClickHouseImage, clickhouseclient.Insert{
		Database: database,
		Table:    table,
		Columns:  columns,
		Rows:     rows,
	})
}

func rawRecordRow(runID string, snapshotPath string, snapshotSHA256 string, lineNumber int64, record CompanyRecord) map[string]any {
	return map[string]any{
		"country_iso2":          "FI",
		"source_slug":           SourceKey,
		"source_run_id":         runID,
		"source_record_id":      record.BusinessID.Value,
		"business_id":           record.BusinessID.Value,
		"source_payload_hash":   record.PayloadHash,
		"snapshot_path":         snapshotPath,
		"snapshot_sha256":       snapshotSHA256,
		"snapshot_line_number":  lineNumber,
		"raw_payload_json":      string(record.RawPayload),
		"schema_version":        sourceExportSchemaVersion,
		"exported_at":           time.Now().UTC().Format("2006-01-02 15:04:05.000"),
	}
}

func companyRow(runID string, record CompanyRecord) map[string]any {
	legalName := primaryName(record)
	websiteURL := strings.TrimSpace(record.Website.URL)
	return map[string]any{
		"country_iso2":               "FI",
		"source_slug":                SourceKey,
		"source_run_id":              runID,
		"source_record_id":           record.BusinessID.Value,
		"source_payload_hash":        record.PayloadHash,
		"source_updated_at":          record.LastModified,
		"business_id":                record.BusinessID.Value,
		"vat_id":                     identifierValue(record.Identifiers, "VAT"),
		"euid":                       identifierPointerValue(record.EUID),
		"legal_name":                 legalName,
		"legal_name_normalized":      strings.ToLower(legalName),
		"lifecycle_status":           lifecycleStatus(record),
		"is_active":                  lifecycleStatus(record) == "active",
		"legal_form_code":            firstCompanyFormCode(record),
		"legal_form_label":           firstCompanyFormLabel(record, "1"),
		"legal_form_label_en":        firstCompanyFormLabel(record, "3"),
		"primary_industry_code":      record.MainBusinessLine.Type,
		"primary_industry_code_set":  record.MainBusinessLine.TypeCodeSet,
		"primary_industry_label":     description(record.MainBusinessLine.Descriptions, "1"),
		"primary_industry_label_en":  description(record.MainBusinessLine.Descriptions, "3"),
		"primary_nace_code":          "",
		"primary_nace_revision":      "",
		"website_url":                websiteURL,
		"website_normalized_url":     websiteURL,
		"website_host":               websiteHost(websiteURL),
	}
}

func primaryName(record CompanyRecord) string {
	if len(record.Names) == 0 {
		return ""
	}
	return record.Names[0].Name
}

func identifierValue(values []Identifier, idType string) string {
	for _, value := range values {
		if strings.EqualFold(value.Type, idType) {
			return value.Value
		}
	}
	return ""
}

func identifierPointerValue(value *Identifier) string {
	if value == nil {
		return ""
	}
	return value.Value
}

func lifecycleStatus(record CompanyRecord) string {
	if record.EndDate != "" || record.TradeRegisterStatus == "3" {
		return "ceased"
	}
	return "active"
}

func firstCompanyFormCode(record CompanyRecord) string {
	if len(record.CompanyForms) == 0 {
		return ""
	}
	return record.CompanyForms[0].Type
}

func firstCompanyFormLabel(record CompanyRecord, language string) string {
	if len(record.CompanyForms) == 0 {
		return ""
	}
	return description(record.CompanyForms[0].Descriptions, language)
}

func description(values []Description, language string) string {
	for _, value := range values {
		if value.LanguageCode == language {
			return value.Description
		}
	}
	return ""
}

func websiteHost(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	return parsed.Hostname()
}
```

- [ ] **Step 3: Run mapper tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/companysources/finland/prhytj -run 'TestCompany' -count=1 -v
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/scheduler/internal/companysources/finland/prhytj/import.go \
        corpscout/scheduler/internal/companysources/finland/prhytj/import_test.go
git commit -m "feat: map Finland PRH YTJ records to clickhouse rows"
```

## Task 8: Wire Finland Source Into CLI

**Files:**
- Modify: `corpscout/scheduler/cmd/corpscout-source/main.go`
- Modify: `corpscout/scheduler/cmd/corpscout-source/main_test.go`

- [ ] **Step 1: Add CLI import config tests**

Extend `corpscout/scheduler/cmd/corpscout-source/main_test.go`:

```go
func TestImportRunRequiresClickHouseURL(t *testing.T) {
	var output bytes.Buffer
	err := run([]string{"import-run", "--country", "finland", "--source", "prhytj", "--run-dir", "/tmp/run"}, &output)
	require.EqualError(t, err, "clickhouse native url is required")
}

func TestImportRunsRequiresRunsRoot(t *testing.T) {
	var output bytes.Buffer
	err := run([]string{"import-runs", "--clickhouse-native-url", "clickhouse://companycollect:9002?username=default&database=corpscout_sources"}, &output)
	require.EqualError(t, err, "runs root is required")
}
```

- [ ] **Step 2: Replace temporary registry entry with Finland source**

Modify `defaultRegistry()` in `corpscout/scheduler/cmd/corpscout-source/main.go`:

```go
func defaultRegistry() companysources.Registry {
	return companysources.NewRegistry(prhytj.Source{})
}
```

Add import:

```go
import "github.com/pulsarpoint/corpscout/scheduler/internal/companysources/finland/prhytj"
```

Remove the `finlandPRHYTJTemporary` type.

- [ ] **Step 3: Implement `import-run` command parsing**

Add this branch to the `switch` in `run`:

```go
case "import-run":
	cfg, err := parseImportRun(args[1:])
	if err != nil {
		return err
	}
	result, err := companysources.ImportRun(context.Background(), registry, companysources.ImportRunRequest{
		Country:             cfg.Country,
		Source:              cfg.Source,
		RunDir:              cfg.RunDir,
		ClickHouseNativeURL: cfg.ClickHouseNativeURL,
		ClickHouseImage:     cfg.ClickHouseImage,
		BatchSize:           cfg.BatchSize,
		Limit:               cfg.Limit,
	})
	if err != nil {
		return err
	}
	return json.NewEncoder(output).Encode(result)
case "import-runs":
	cfg, err := parseImportRuns(args[1:])
	if err != nil {
		return err
	}
	result, err := companysources.ImportChangedRuns(context.Background(), registry, companysources.ImportChangedRunsRequest{
		RunsRoot:            cfg.RunsRoot,
		RunIndexPath:        cfg.RunIndexPath,
		ClickHouseNativeURL: cfg.ClickHouseNativeURL,
		ClickHouseImage:     cfg.ClickHouseImage,
		BatchSize:           cfg.BatchSize,
		Limit:               cfg.Limit,
		ChangedOnly:         cfg.ChangedOnly,
	})
	if err != nil {
		return err
	}
	return json.NewEncoder(output).Encode(result)
```

Add helper types/functions:

```go
type importRunConfig struct {
	Country             string
	Source              string
	RunDir              string
	ClickHouseNativeURL string
	ClickHouseImage     string
	BatchSize           int
	Limit               int64
}

type importRunsConfig struct {
	RunsRoot            string
	RunIndexPath        string
	ClickHouseNativeURL string
	ClickHouseImage     string
	BatchSize           int
	ChangedOnly         bool
	Limit               int64
}

func parseImportRun(args []string) (importRunConfig, error) {
	var cfg importRunConfig
	fs := flag.NewFlagSet("import-run", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&cfg.Country, "country", "", "source country")
	fs.StringVar(&cfg.Source, "source", "", "source key")
	fs.StringVar(&cfg.RunDir, "run-dir", "", "raw source run directory")
	fs.StringVar(&cfg.ClickHouseNativeURL, "clickhouse-native-url", os.Getenv("CLICKHOUSE_NATIVE_URL"), "ClickHouse native URL")
	fs.StringVar(&cfg.ClickHouseImage, "clickhouse-image", "", "ClickHouse Docker image")
	fs.IntVar(&cfg.BatchSize, "batch-size", 1000, "rows per ClickHouse insert batch")
	fs.Int64Var(&cfg.Limit, "limit", 0, "maximum records to import")
	if err := fs.Parse(args); err != nil {
		return importRunConfig{}, err
	}
	if cfg.Country == "" {
		return importRunConfig{}, errors.New("country is required")
	}
	if cfg.Source == "" {
		return importRunConfig{}, errors.New("source is required")
	}
	if cfg.RunDir == "" {
		return importRunConfig{}, errors.New("run dir is required")
	}
	if cfg.ClickHouseNativeURL == "" {
		return importRunConfig{}, errors.New("clickhouse native url is required")
	}
	return cfg, nil
}

func parseImportRuns(args []string) (importRunsConfig, error) {
	var cfg importRunsConfig
	fs := flag.NewFlagSet("import-runs", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&cfg.RunsRoot, "runs-root", "", "root directory containing raw source runs")
	fs.StringVar(&cfg.RunIndexPath, "run-index", "../clickhouse/run-index.lock.yaml", "run import index path")
	fs.StringVar(&cfg.ClickHouseNativeURL, "clickhouse-native-url", os.Getenv("CLICKHOUSE_NATIVE_URL"), "ClickHouse native URL")
	fs.StringVar(&cfg.ClickHouseImage, "clickhouse-image", "", "ClickHouse Docker image")
	fs.IntVar(&cfg.BatchSize, "batch-size", 1000, "rows per ClickHouse insert batch")
	fs.BoolVar(&cfg.ChangedOnly, "changed-only", false, "skip runs already imported with the same manifest and file hashes")
	fs.Int64Var(&cfg.Limit, "limit", 0, "maximum records to import per source")
	if err := fs.Parse(args); err != nil {
		return importRunsConfig{}, err
	}
	if cfg.RunsRoot == "" {
		return importRunsConfig{}, errors.New("runs root is required")
	}
	if cfg.ClickHouseNativeURL == "" {
		return importRunsConfig{}, errors.New("clickhouse native url is required")
	}
	return cfg, nil
}
```

Add imports:

```go
import (
	"context"
	"encoding/json"
	"flag"
)
```

- [ ] **Step 4: Run CLI tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./cmd/corpscout-source ./internal/companysources/finland/prhytj -count=1 -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/scheduler/cmd/corpscout-source \
        corpscout/scheduler/internal/companysources/finland/prhytj
git commit -m "feat: wire Finland PRH YTJ clickhouse import CLI"
```

## Task 9: Update Make Targets And Remove Obsolete Parquet ClickHouse Targets

**Files:**
- Modify: `corpscout/Makefile`

- [ ] **Step 1: Update Makefile targets**

Modify `.PHONY` in `corpscout/Makefile` so it includes:

```make
.PHONY: up down logs rebuild rebuild-local scheduler-local-bin migrate-up migrate-down migrate-test-up migrate-test-down clickhouse-migrate-up clickhouse-migrate-down source-list source-import-run source-import-changed sqlc-generate test test-db
```

Remove targets:

```make
clickhouse-generate-finland-prhytj:
clickhouse-import-finland-prhytj:
```

Add targets:

```make
SOURCE_COUNTRY ?= finland
SOURCE_KEY ?= prhytj
SOURCE_RUN_DIR ?= $(abspath ../companies/data/finland/sources/prhytj/runs/20260608T201348Z-prhytj)
SOURCE_BATCH_SIZE ?= 1000
SOURCE_LIMIT ?= 0

source-list:
	cd scheduler && GOWORK=off go run ./cmd/corpscout-source list-sources

source-import-run:
	cd scheduler && GOWORK=off go run ./cmd/corpscout-source import-run \
		--country "$(SOURCE_COUNTRY)" \
		--source "$(SOURCE_KEY)" \
		--run-dir "$(SOURCE_RUN_DIR)" \
		--clickhouse-native-url "$(CLICKHOUSE_NATIVE_URL)" \
		--batch-size "$(SOURCE_BATCH_SIZE)" \
		--limit "$(SOURCE_LIMIT)"

source-import-changed:
	cd scheduler && GOWORK=off go run ./cmd/corpscout-source import-runs \
		--runs-root "$(abspath data/sources)" \
		--clickhouse-native-url "$(CLICKHOUSE_NATIVE_URL)" \
		--changed-only
```

- [ ] **Step 2: Verify dry-run output**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
make -n source-list
make -n source-import-run
```

Expected: commands call `go run ./cmd/corpscout-source` inside `corpscout/scheduler` and pass `CLICKHOUSE_NATIVE_URL`.

- [ ] **Step 3: Run list command**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
make source-list
```

Expected: output contains `finland/prhytj`.

- [ ] **Step 4: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/Makefile
git commit -m "chore: route source clickhouse imports through corpscout"
```

## Task 10: Verify Direct Import Against Remote ClickHouse

**Files:**
- No source edits expected.

- [ ] **Step 1: Confirm remote ClickHouse connectivity**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
docker run --rm --add-host companycollect:100.85.212.113 clickhouse/clickhouse-server:26.5 clickhouse-client --host companycollect --port 9002 --user default --password change-me --database corpscout_sources --query "SELECT version()"
```

Expected: prints `26.5.1.882` or another `26.5.x` version from the remote server.

- [ ] **Step 2: Confirm migration state**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
make clickhouse-migrate-up
```

Expected: `no change` if remote migrations are already applied.

- [ ] **Step 3: Import a small limit from existing raw Finland run**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
make source-import-run SOURCE_LIMIT=100 SOURCE_BATCH_SIZE=50
```

Expected: JSON summary with `ImportedTables` containing `fi_prhytj_raw_records` and `fi_prhytj_companies`, and `ImportedRows` equal to `100`.

- [ ] **Step 4: Query remote row count**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
docker run --rm --add-host companycollect:100.85.212.113 clickhouse/clickhouse-server:26.5 clickhouse-client --host companycollect --port 9002 --user default --password change-me --database corpscout_sources --query "SELECT (SELECT count() FROM fi_prhytj_raw_records) AS raw_count, (SELECT count() FROM fi_prhytj_companies) AS company_count"
```

Expected: both counts are at least `100`.

- [ ] **Step 5: Run focused tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./cmd/corpscout-source ./internal/clickhouseclient ./internal/companysources/... -count=1
```

Expected: PASS.

- [ ] **Step 6: Commit verification-only code changes if any were needed**

If Step 3 or Step 5 required code fixes, commit them:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git status --short
git add corpscout/scheduler corpscout/Makefile
git commit -m "fix: complete Finland PRH YTJ clickhouse import"
```

If no files changed, do not create an empty commit.

## Task 11: Remove Obsolete Companysource ClickHouse Wiring

**Files:**
- Modify: `companies/companysource/internal/cli/config.go`
- Modify: `companies/companysource/internal/cli/config_test.go`
- Modify: `companies/companysource/internal/cli/run.go`
- Modify: `companies/companysource/internal/source/source.go`
- Modify: `companies/companysource/sources/finland/prhytj/source.go`
- Delete: `companies/companysource/sources/finland/prhytj/clickhouse.yaml`

- [ ] **Step 1: Write/update tests for supported companysource commands**

Update `companies/companysource/internal/cli/config_test.go` so supported commands are:

```go
commands := []string{
	"download",
	"export-parquet",
	"list-sources",
	"status",
}
```

Add this assertion:

```go
_, err := Parse([]string{"generate-clickhouse-migration", "--country", "finland", "--source", "prhytj"})
require.EqualError(t, err, "unknown command generate-clickhouse-migration")
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/companies/companysource
GOWORK=off go test ./internal/cli -count=1 -v
```

Expected: FAIL because ClickHouse commands still exist.

- [ ] **Step 3: Remove ClickHouse CLI command support**

In `companies/companysource/internal/cli/config.go`, change the command validation switch to:

```go
case "download", "export-parquet", "list-sources", "status":
```

Remove config fields that are only used by `generate-clickhouse-migration` or `import-clickhouse`:

```go
Database
Out
DownOut
ClickHouseNativeURL
SourceExportID
ClickHouseImage
DockerMount
```

In `companies/companysource/internal/cli/run.go`, remove cases:

```go
case "generate-clickhouse-migration":
case "import-clickhouse":
```

In `companies/companysource/internal/source/source.go`, remove methods from `Adapter`:

```go
GenerateClickHouseMigration(ctx context.Context, opts ClickHouseMigrationOptions) (ClickHouseMigrationResult, error)
ImportClickHouse(ctx context.Context, opts ClickHouseImportOptions) (ClickHouseImportResult, error)
```

Remove the now-unused option/result structs:

```go
ClickHouseMigrationOptions
ClickHouseImportOptions
```

- [ ] **Step 4: Remove Finland source ClickHouse methods**

In `companies/companysource/sources/finland/prhytj/source.go`, remove:

```go
//go:embed clickhouse.yaml
var clickHouseConfigYAML []byte

func (s *Source) GenerateClickHouseMigration(...)
func (s *Source) ImportClickHouse(...)
```

Remove imports that become unused:

```go
_ "embed"
chimport "github.com/pulsarpoint/companycollect/companies/companysource/internal/clickhouse"
```

Delete:

```bash
rm /Users/graovic/pulsarpoint/ppoint/companycollect/companies/companysource/sources/finland/prhytj/clickhouse.yaml
```

- [ ] **Step 5: Run companysource tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/companies/companysource
GOWORK=off go test ./... -count=1
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add companies/companysource
git commit -m "refactor: remove companysource clickhouse commands"
```

## Task 12: Final Verification

**Files:**
- No planned edits.

- [ ] **Step 1: Run Corpscout scheduler focused tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./cmd/corpscout-source ./internal/clickhouseclient ./internal/companysources/... ./internal/db -count=1
```

Expected: PASS.

- [ ] **Step 2: Run companysource full tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/companies/companysource
GOWORK=off go test ./... -count=1
```

Expected: PASS.

- [ ] **Step 3: Verify Makefile dry-runs**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
make -n clickhouse-migrate-up
make -n source-list
make -n source-import-run
```

Expected:

- `clickhouse-migrate-up` uses `--add-host companycollect:100.85.212.113`
- `source-list` runs `go run ./cmd/corpscout-source list-sources`
- `source-import-run` passes `--clickhouse-native-url`

- [ ] **Step 4: Verify git state**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git status --short -uall
```

Expected: clean worktree.

## Execution Notes

- Do not reset or drop remote ClickHouse state without explicit approval.
- Do not remove historical Parquet data in this implementation; it is no longer part of the primary path, but it can remain as historical data.
- Use `log/slog` only at command/worker boundaries. Lower-level packages should wrap errors with `github.com/cockroachdb/errors` and return them.
- Keep source packages concrete. The `companysources.Source` registry interface is justified because the CLI and future Temporal worker dispatch multiple real source implementations by country/source key.
