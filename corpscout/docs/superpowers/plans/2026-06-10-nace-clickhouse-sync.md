# NACE ClickHouse Sync Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a Temporal-backed `Sync to CH` path that mirrors Postgres NACE taxonomy into ClickHouse reference tables.

**Architecture:** Postgres remains the authoritative NACE store. A new workflow reads Postgres NACE rows through sqlc queries and writes a full snapshot into `corpscout_reference` ClickHouse tables. The existing NACE taxonomy workflow also starts this sync after successful or skipped Postgres imports.

**Tech Stack:** Go, Temporal SDK, sqlc, pgx, ClickHouse native driver, React Router, shadcn/ui, TypeScript.

---

## File Structure

- Create `clickhouse/migrations/000007_create_nace_reference_tables.up.sql`: ClickHouse database and table definitions.
- Create `clickhouse/migrations/000007_create_nace_reference_tables.down.sql`: Drops the NACE ClickHouse tables.
- Modify `database/queries/nace_taxonomy.sql`: Adds export queries for classifications, codes, and aliases.
- Modify generated sqlc files with `make sqlc-generate`.
- Modify `scheduler/internal/clickhouse/writer.go`: Adds database-specific insert/truncate helpers.
- Modify `scheduler/internal/clickhouse/writer_test.go`: Tests new helpers.
- Create `scheduler/internal/nacetaxonomy/clickhouse_sync.go`: ClickHouse sync activity logic and hierarchy derivation.
- Create `scheduler/internal/nacetaxonomy/clickhouse_sync_test.go`: Unit tests for hierarchy derivation and row building.
- Modify `scheduler/internal/nacetaxonomy/workflow.go`: Adds workflow names/types and automatic child workflow trigger.
- Modify `scheduler/internal/nacetaxonomy/workflow_test.go`: Tests standalone workflow and child workflow trigger from taxonomy sync.
- Modify `scheduler/internal/nacetaxonomy/actions.go`: Adds ClickHouse URL to `Actions`.
- Modify `scheduler/internal/app/temporal.go`: Passes ClickHouse URL to NACE actions.
- Modify `scheduler/internal/app/nace_taxonomy_temporal.go`: Registers the new workflow/activity directly.
- Modify `scheduler/internal/httpapi/workflow_triggers.go`: Adds manual start/list handlers.
- Modify `scheduler/internal/httpapi/handlers.go`: Registers the new routes.
- Modify `scheduler/internal/httpapi/workflow_triggers_test.go`: Tests manual NACE ClickHouse workflow start.
- Modify `ui/app/types/api.ts`: Adds NACE ClickHouse sync types.
- Modify `ui/app/lib/api.ts`: Adds API client methods.
- Modify `ui/app/components/app/NACETaxonomySyncManagement.tsx`: Adds `Sync to CH` button and recent run list or message.
- Create `scheduler/internal/db/clickhouse_nace_reference_migration_test.go`: Tests ClickHouse NACE migration shape.

---

### Task 1: Add ClickHouse NACE Reference Tables

**Files:**
- Create: `clickhouse/migrations/000007_create_nace_reference_tables.up.sql`
- Create: `clickhouse/migrations/000007_create_nace_reference_tables.down.sql`
- Create: `scheduler/internal/db/clickhouse_nace_reference_migration_test.go`

- [ ] **Step 1: Write the failing migration shape test**

Create `scheduler/internal/db/clickhouse_nace_reference_migration_test.go`:

```go
package db_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClickHouseNACEReferenceMigrationShape(t *testing.T) {
	body, err := os.ReadFile(filepath.Join("..", "..", "..", "clickhouse", "migrations", "000007_create_nace_reference_tables.up.sql"))
	require.NoError(t, err)
	sql := string(body)
	require.Contains(t, sql, "CREATE DATABASE IF NOT EXISTS `corpscout_reference`")
	require.Contains(t, sql, "CREATE TABLE IF NOT EXISTS `corpscout_reference`.`nace_classifications`")
	require.Contains(t, sql, "CREATE TABLE IF NOT EXISTS `corpscout_reference`.`nace_codes`")
	require.Contains(t, sql, "CREATE TABLE IF NOT EXISTS `corpscout_reference`.`nace_code_aliases`")
	require.Contains(t, sql, "section_code")
	require.Contains(t, sql, "division_code")
	require.Contains(t, sql, "ReplacingMergeTree")
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/db -run TestClickHouseNACEReferenceMigrationShape -count=1
```

Expected: fail because migration file does not exist.

- [ ] **Step 3: Add the ClickHouse migration**

Create `clickhouse/migrations/000007_create_nace_reference_tables.up.sql`:

```sql
CREATE DATABASE IF NOT EXISTS `corpscout_reference`;

CREATE TABLE IF NOT EXISTS `corpscout_reference`.`nace_classifications` (
  `revision` String,
  `name` String,
  `valid_from` Nullable(Date),
  `valid_to` Nullable(Date),
  `source_url` Nullable(String),
  `active_codes` UInt64,
  `inactive_codes` UInt64,
  `synced_at` DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(`synced_at`)
ORDER BY (`revision`);

CREATE TABLE IF NOT EXISTS `corpscout_reference`.`nace_codes` (
  `revision` String,
  `code` String,
  `normalized_code` String,
  `level` UInt8,
  `level_name` LowCardinality(String),
  `parent_code` Nullable(String),
  `parent_normalized_code` Nullable(String),
  `title` String,
  `description` Nullable(String),
  `active` Bool,
  `section_code` Nullable(String),
  `division_code` Nullable(String),
  `group_code` Nullable(String),
  `class_code` Nullable(String),
  `synced_at` DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(`synced_at`)
ORDER BY (`revision`, `level`, `normalized_code`);

CREATE TABLE IF NOT EXISTS `corpscout_reference`.`nace_code_aliases` (
  `revision` String,
  `code` String,
  `alias_type` LowCardinality(String),
  `alias_code` String,
  `normalized_alias_code` String,
  `synced_at` DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(`synced_at`)
ORDER BY (`revision`, `alias_type`, `normalized_alias_code`, `code`);
```

Create `clickhouse/migrations/000007_create_nace_reference_tables.down.sql`:

```sql
DROP TABLE IF EXISTS `corpscout_reference`.`nace_code_aliases`;
DROP TABLE IF EXISTS `corpscout_reference`.`nace_codes`;
DROP TABLE IF EXISTS `corpscout_reference`.`nace_classifications`;
```

- [ ] **Step 4: Run the migration shape test**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/db -run TestClickHouseNACEReferenceMigrationShape -count=1
```

Expected: pass.

- [ ] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
git add clickhouse/migrations/000007_create_nace_reference_tables.* scheduler/internal/db/clickhouse_nace_reference_migration_test.go
git commit -m "feat: add clickhouse nace reference tables"
```

---

### Task 2: Add Postgres Export Queries for NACE

**Files:**
- Modify: `database/queries/nace_taxonomy.sql`
- Regenerate: `scheduler/internal/db/gen/*.go`
- Test: `scheduler/internal/db/nace_clickhouse_export_query_shape_test.go`

- [ ] **Step 1: Write the failing query shape test**

Create `scheduler/internal/db/nace_clickhouse_export_query_shape_test.go`:

```go
package db_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNACEClickHouseExportQueriesExist(t *testing.T) {
	body, err := os.ReadFile(filepath.Join("..", "..", "..", "database", "queries", "nace_taxonomy.sql"))
	require.NoError(t, err)
	sql := string(body)
	require.Contains(t, sql, "-- name: ListNACEClassificationsForClickHouse :many")
	require.Contains(t, sql, "-- name: ListNACECodesForClickHouse :many")
	require.Contains(t, sql, "-- name: ListNACECodeAliasesForClickHouse :many")
	require.Contains(t, sql, "JOIN nace_classifications")
	require.Contains(t, sql, "JOIN nace_code_aliases")
}
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/db -run TestNACEClickHouseExportQueriesExist -count=1
```

Expected: fail because the query names are missing.

- [ ] **Step 3: Add sqlc export queries**

Append to `database/queries/nace_taxonomy.sql`:

```sql
-- name: ListNACEClassificationsForClickHouse :many
SELECT
  nclass.revision,
  nclass.name,
  nclass.valid_from,
  nclass.valid_to,
  nclass.source_url,
  count(ncodes.id) FILTER (WHERE ncodes.active) AS active_codes,
  count(ncodes.id) FILTER (WHERE NOT ncodes.active) AS inactive_codes
FROM nace_classifications nclass
LEFT JOIN nace_codes ncodes ON ncodes.classification_id = nclass.id
WHERE nclass.code_system = 'NACE'
GROUP BY nclass.id
ORDER BY nclass.revision;

-- name: ListNACECodesForClickHouse :many
SELECT
  nclass.revision,
  ncodes.id,
  ncodes.code,
  ncodes.normalized_code,
  ncodes.level,
  ncodes.level_name,
  ncodes.parent_code,
  parent.normalized_code AS parent_normalized_code,
  ncodes.parent_id,
  ncodes.title,
  ncodes.description,
  ncodes.active
FROM nace_codes ncodes
JOIN nace_classifications nclass ON nclass.id = ncodes.classification_id
LEFT JOIN nace_codes parent ON parent.id = ncodes.parent_id
WHERE nclass.code_system = 'NACE'
ORDER BY nclass.revision, ncodes.level, ncodes.code;

-- name: ListNACECodeAliasesForClickHouse :many
SELECT
  nclass.revision,
  ncodes.code,
  aliases.alias_type,
  aliases.alias_code,
  aliases.normalized_alias_code
FROM nace_code_aliases aliases
JOIN nace_codes ncodes ON ncodes.id = aliases.nace_code_id
JOIN nace_classifications nclass ON nclass.id = ncodes.classification_id
WHERE nclass.code_system = 'NACE'
ORDER BY nclass.revision, aliases.alias_type, aliases.normalized_alias_code, ncodes.code;
```

- [ ] **Step 4: Regenerate sqlc**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
make sqlc-generate
```

Expected: generated methods appear in `scheduler/internal/db/gen/nace_taxonomy.sql.go` and `scheduler/internal/db/gen/querier.go`.

- [ ] **Step 5: Run the query shape test**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/db -run TestNACEClickHouseExportQueriesExist -count=1
```

Expected: pass.

- [ ] **Step 6: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
git add database/queries/nace_taxonomy.sql scheduler/internal/db/gen scheduler/internal/db/nace_clickhouse_export_query_shape_test.go
git commit -m "feat: add nace clickhouse export queries"
```

---

### Task 3: Extend ClickHouse Writer for Reference Database Writes

**Files:**
- Modify: `scheduler/internal/clickhouse/writer.go`
- Modify: `scheduler/internal/clickhouse/writer_test.go`

- [ ] **Step 1: Write failing tests for database-specific helpers**

Add to `scheduler/internal/clickhouse/writer_test.go`:

```go
func TestBuildInsertQuerySupportsReferenceDatabase(t *testing.T) {
	query := BuildInsertQuery("corpscout_reference", "nace_codes", []string{"revision", "code"})
	require.Equal(t, "INSERT INTO `corpscout_reference`.`nace_codes` (`revision`, `code`)", query)
}

func TestBuildTruncateQuerySupportsReferenceDatabase(t *testing.T) {
	query := BuildTruncateQuery("corpscout_reference", "nace_codes")
	require.Equal(t, "TRUNCATE TABLE IF EXISTS `corpscout_reference`.`nace_codes`", query)
}
```

- [ ] **Step 2: Run tests**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/clickhouse -count=1
```

Expected: existing builders may already pass; this protects the reference database behavior.

- [ ] **Step 3: Add database-specific writer methods**

In `scheduler/internal/clickhouse/writer.go`, change `Insert` to delegate to `InsertInto`, and add `TruncateTablesIn`:

```go
func (w *Writer) Insert(ctx context.Context, insert Insert) error {
	return w.InsertInto(ctx, w.database, insert)
}

func (w *Writer) InsertInto(ctx context.Context, database string, insert Insert) error {
	if len(insert.Rows) == 0 {
		return nil
	}
	query := BuildInsertQuery(database, insert.Table, insert.Columns)
	batch, err := w.conn.PrepareBatch(ctx, query)
	if err != nil {
		return errors.Wrap(err, "prepare clickhouse insert batch")
	}
	sent := false
	defer func() {
		if !sent {
			_ = batch.Close()
		}
	}()
	for _, row := range insert.Rows {
		if err := batch.Append(insertValues(insert.Columns, row)...); err != nil {
			return errors.Wrap(err, "append clickhouse insert row")
		}
	}
	if err := batch.Send(); err != nil {
		return errors.Wrap(err, "send clickhouse insert batch")
	}
	sent = true
	return nil
}

func (w *Writer) TruncateTables(ctx context.Context, tables []string) error {
	return w.TruncateTablesIn(ctx, w.database, tables)
}

func (w *Writer) TruncateTablesIn(ctx context.Context, database string, tables []string) error {
	for _, table := range tables {
		if strings.TrimSpace(table) == "" {
			continue
		}
		if err := w.conn.Exec(ctx, BuildTruncateQuery(database, table)); err != nil {
			return errors.Wrapf(err, "truncate clickhouse table %s.%s", database, table)
		}
	}
	return nil
}
```

- [ ] **Step 4: Run tests**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
gofmt -w internal/clickhouse/writer.go internal/clickhouse/writer_test.go
GOWORK=off go test ./internal/clickhouse -count=1
```

Expected: pass.

- [ ] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
git add scheduler/internal/clickhouse/writer.go scheduler/internal/clickhouse/writer_test.go
git commit -m "feat: support clickhouse reference database writes"
```

---

### Task 4: Implement NACE ClickHouse Sync Activity Logic

**Files:**
- Modify: `scheduler/internal/nacetaxonomy/actions.go`
- Create: `scheduler/internal/nacetaxonomy/clickhouse_sync.go`
- Create: `scheduler/internal/nacetaxonomy/clickhouse_sync_test.go`

- [ ] **Step 1: Write failing hierarchy derivation test**

Create `scheduler/internal/nacetaxonomy/clickhouse_sync_test.go`:

```go
package nacetaxonomy

import (
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

func TestNACEClickHouseCodeRowsDeriveHierarchy(t *testing.T) {
	sectionID := uuid.New()
	divisionID := uuid.New()
	groupID := uuid.New()
	classID := uuid.New()
	rows := []db.ListNACECodesForClickHouseRow{
		{Revision: "2.1", ID: sectionID, Code: "N", NormalizedCode: "N", Level: 1, LevelName: "section", Title: "Administrative and support service activities", Active: true},
		{Revision: "2.1", ID: divisionID, Code: "82", NormalizedCode: "82", Level: 2, LevelName: "division", ParentCode: stringPtr("N"), ParentID: pgtype.UUID{Bytes: sectionID, Valid: true}, Title: "Office administrative, office support and other business support activities", Active: true},
		{Revision: "2.1", ID: groupID, Code: "82.2", NormalizedCode: "822", Level: 3, LevelName: "group", ParentCode: stringPtr("82"), ParentID: pgtype.UUID{Bytes: divisionID, Valid: true}, Title: "Activities of call centres", Active: true},
		{Revision: "2.1", ID: classID, Code: "82.20", NormalizedCode: "8220", Level: 4, LevelName: "class", ParentCode: stringPtr("82.2"), ParentID: pgtype.UUID{Bytes: groupID, Valid: true}, Title: "Activities of call centres", Active: true},
	}
	got := buildNACECodeClickHouseRows(rows, testSyncTime())
	require.Len(t, got, 4)
	classRow := got[3]
	require.Equal(t, "N", classRow["section_code"])
	require.Equal(t, "82", classRow["division_code"])
	require.Equal(t, "82.2", classRow["group_code"])
	require.Equal(t, "82.20", classRow["class_code"])
}
```

Add these helpers in the test file:

```go
func stringPtr(value string) *string { return &value }

func testSyncTime() time.Time {
	return time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
}
```

Include `time` in imports after adding `testSyncTime`.

- [ ] **Step 2: Run test to verify it fails**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/nacetaxonomy -run TestNACEClickHouseCodeRowsDeriveHierarchy -count=1
```

Expected: fail because `buildNACECodeClickHouseRows` is undefined.

- [ ] **Step 3: Extend `Actions` constructor**

In `scheduler/internal/nacetaxonomy/actions.go`, change the struct and constructor:

```go
type Actions struct {
	pool                *pgxpool.Pool
	httpClient          *http.Client
	clickHouseNativeURL string
}

func NewActions(pool *pgxpool.Pool, httpClient *http.Client, clickHouseNativeURL string) *Actions {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &Actions{pool: pool, httpClient: httpClient, clickHouseNativeURL: strings.TrimSpace(clickHouseNativeURL)}
}
```

This file already imports `strings`, so keep the import.

- [ ] **Step 4: Add sync types and activity**

Create `scheduler/internal/nacetaxonomy/clickhouse_sync.go`:

```go
package nacetaxonomy

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	chwriter "github.com/pulsarpoint/corpscout/scheduler/internal/clickhouse"
	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

const clickHouseReferenceDatabase = "corpscout_reference"

type SyncNACEToClickHouseActivityInput struct {
	TemporalWorkflowID string `json:"temporal_workflow_id"`
	Trigger            string `json:"trigger"`
}

type SyncNACEToClickHouseActivityResult struct {
	Status                string `json:"status"`
	ClassificationsSynced int    `json:"classifications_synced"`
	CodesSynced           int    `json:"codes_synced"`
	AliasesSynced         int    `json:"aliases_synced"`
	Message               string `json:"message"`
}

func (a *Actions) SyncNACEToClickHouseActivity(ctx context.Context, input SyncNACEToClickHouseActivityInput) (SyncNACEToClickHouseActivityResult, error) {
	if a == nil || a.pool == nil {
		return SyncNACEToClickHouseActivityResult{}, errors.New("nace taxonomy database is not available")
	}
	if a.clickHouseNativeURL == "" {
		return SyncNACEToClickHouseActivityResult{}, errors.New("clickhouse native url is required")
	}

	queries := db.New(a.pool)
	classifications, err := queries.ListNACEClassificationsForClickHouse(ctx)
	if err != nil {
		return SyncNACEToClickHouseActivityResult{}, errors.Wrap(err, "list nace classifications for clickhouse")
	}
	codes, err := queries.ListNACECodesForClickHouse(ctx)
	if err != nil {
		return SyncNACEToClickHouseActivityResult{}, errors.Wrap(err, "list nace codes for clickhouse")
	}
	aliases, err := queries.ListNACECodeAliasesForClickHouse(ctx)
	if err != nil {
		return SyncNACEToClickHouseActivityResult{}, errors.Wrap(err, "list nace aliases for clickhouse")
	}

	writer, err := chwriter.Open(ctx, a.clickHouseNativeURL)
	if err != nil {
		return SyncNACEToClickHouseActivityResult{}, errors.Wrap(err, "open clickhouse writer")
	}
	defer writer.Close()

	syncedAt := time.Now().UTC()
	if err := writer.TruncateTablesIn(ctx, clickHouseReferenceDatabase, []string{"nace_code_aliases", "nace_codes", "nace_classifications"}); err != nil {
		return SyncNACEToClickHouseActivityResult{}, err
	}
	if err := writer.InsertInto(ctx, clickHouseReferenceDatabase, chwriter.Insert{
		Table:   "nace_classifications",
		Columns: []string{"revision", "name", "valid_from", "valid_to", "source_url", "active_codes", "inactive_codes", "synced_at"},
		Rows:    buildNACEClassificationClickHouseRows(classifications, syncedAt),
	}); err != nil {
		return SyncNACEToClickHouseActivityResult{}, errors.Wrap(err, "insert clickhouse nace classifications")
	}
	if err := writer.InsertInto(ctx, clickHouseReferenceDatabase, chwriter.Insert{
		Table:   "nace_codes",
		Columns: []string{"revision", "code", "normalized_code", "level", "level_name", "parent_code", "parent_normalized_code", "title", "description", "active", "section_code", "division_code", "group_code", "class_code", "synced_at"},
		Rows:    buildNACECodeClickHouseRows(codes, syncedAt),
	}); err != nil {
		return SyncNACEToClickHouseActivityResult{}, errors.Wrap(err, "insert clickhouse nace codes")
	}
	if err := writer.InsertInto(ctx, clickHouseReferenceDatabase, chwriter.Insert{
		Table:   "nace_code_aliases",
		Columns: []string{"revision", "code", "alias_type", "alias_code", "normalized_alias_code", "synced_at"},
		Rows:    buildNACEAliasClickHouseRows(aliases, syncedAt),
	}); err != nil {
		return SyncNACEToClickHouseActivityResult{}, errors.Wrap(err, "insert clickhouse nace aliases")
	}

	return SyncNACEToClickHouseActivityResult{
		Status:                SyncStatusSucceeded,
		ClassificationsSynced: len(classifications),
		CodesSynced:           len(codes),
		AliasesSynced:         len(aliases),
		Message:               "nace taxonomy synced to clickhouse",
	}, nil
}
```

- [ ] **Step 5: Add row builders**

In the same file, add:

```go
func buildNACEClassificationClickHouseRows(rows []db.ListNACEClassificationsForClickHouseRow, syncedAt time.Time) []map[string]any {
	out := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		out = append(out, map[string]any{
			"revision":       row.Revision,
			"name":           row.Name,
			"valid_from":     pgDateValue(row.ValidFrom),
			"valid_to":       pgDateValue(row.ValidTo),
			"source_url":     row.SourceUrl,
			"active_codes":   uint64(row.ActiveCodes),
			"inactive_codes": uint64(row.InactiveCodes),
			"synced_at":      syncedAt,
		})
	}
	return out
}

func buildNACECodeClickHouseRows(rows []db.ListNACECodesForClickHouseRow, syncedAt time.Time) []map[string]any {
	byID := make(map[uuid.UUID]db.ListNACECodesForClickHouseRow, len(rows))
	for _, row := range rows {
		byID[row.ID] = row
	}
	out := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		hierarchy := naceHierarchy(row, byID)
		out = append(out, map[string]any{
			"revision":               row.Revision,
			"code":                   row.Code,
			"normalized_code":        row.NormalizedCode,
			"level":                  uint8(row.Level),
			"level_name":             row.LevelName,
			"parent_code":            row.ParentCode,
			"parent_normalized_code": row.ParentNormalizedCode,
			"title":                  row.Title,
			"description":            row.Description,
			"active":                 row.Active,
			"section_code":           hierarchy.SectionCode,
			"division_code":          hierarchy.DivisionCode,
			"group_code":             hierarchy.GroupCode,
			"class_code":             hierarchy.ClassCode,
			"synced_at":              syncedAt,
		})
	}
	return out
}

func buildNACEAliasClickHouseRows(rows []db.ListNACECodeAliasesForClickHouseRow, syncedAt time.Time) []map[string]any {
	out := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		out = append(out, map[string]any{
			"revision":              row.Revision,
			"code":                  row.Code,
			"alias_type":            row.AliasType,
			"alias_code":            row.AliasCode,
			"normalized_alias_code": row.NormalizedAliasCode,
			"synced_at":             syncedAt,
		})
	}
	return out
}
```

Add hierarchy helpers:

```go
type naceCodeHierarchy struct {
	SectionCode  *string
	DivisionCode *string
	GroupCode    *string
	ClassCode    *string
}

func naceHierarchy(row db.ListNACECodesForClickHouseRow, byID map[uuid.UUID]db.ListNACECodesForClickHouseRow) naceCodeHierarchy {
	var hierarchy naceCodeHierarchy
	seen := map[uuid.UUID]bool{}
	current := row
	for {
		assignNACEHierarchyCode(&hierarchy, current)
		if !current.ParentID.Valid {
			break
		}
		parentID := uuid.UUID(current.ParentID.Bytes)
		if seen[parentID] {
			break
		}
		seen[parentID] = true
		parent, ok := byID[parentID]
		if !ok {
			break
		}
		current = parent
	}
	return hierarchy
}

func assignNACEHierarchyCode(h *naceCodeHierarchy, row db.ListNACECodesForClickHouseRow) {
	code := row.Code
	switch row.LevelName {
	case "section":
		h.SectionCode = &code
	case "division":
		h.DivisionCode = &code
	case "group":
		h.GroupCode = &code
	case "class":
		h.ClassCode = &code
	}
}

func pgDateValue(value pgtype.Date) any {
	if !value.Valid {
		return nil
	}
	return value.Time
}
```

- [ ] **Step 6: Run tests**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
gofmt -w internal/nacetaxonomy/actions.go internal/nacetaxonomy/clickhouse_sync.go internal/nacetaxonomy/clickhouse_sync_test.go
GOWORK=off go test ./internal/nacetaxonomy -run TestNACEClickHouseCodeRowsDeriveHierarchy -count=1
```

Expected: pass.

- [ ] **Step 7: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
git add scheduler/internal/nacetaxonomy/actions.go scheduler/internal/nacetaxonomy/clickhouse_sync.go scheduler/internal/nacetaxonomy/clickhouse_sync_test.go
git commit -m "feat: add nace clickhouse sync activity"
```

---

### Task 5: Add and Register NACE ClickHouse Temporal Workflow

**Files:**
- Modify: `scheduler/internal/nacetaxonomy/workflow.go`
- Modify: `scheduler/internal/nacetaxonomy/workflow_test.go`
- Modify: `scheduler/internal/app/temporal.go`
- Modify: `scheduler/internal/app/nace_taxonomy_temporal.go`

- [ ] **Step 1: Write failing workflow tests**

In `scheduler/internal/nacetaxonomy/workflow_test.go`, add this standalone workflow test:

```go
func TestSyncNACEToClickHouseWorkflowRunsActivity(t *testing.T) {
	env := testsuite.WorkflowTestSuite{}.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(SyncNACEToClickHouse, workflow.RegisterOptions{Name: SyncToClickHouseWorkflowName})
	env.RegisterActivityWithOptions(func(context.Context, SyncNACEToClickHouseActivityInput) (SyncNACEToClickHouseActivityResult, error) {
		return SyncNACEToClickHouseActivityResult{Status: SyncStatusSucceeded, CodesSynced: 4}, nil
	}, activity.RegisterOptions{Name: syncNACEToClickHouseActivity})
	env.ExecuteWorkflow(SyncNACEToClickHouse, SyncNACEToClickHouseInput{Trigger: "manual"})
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var result SyncNACEToClickHouseResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, SyncStatusSucceeded, result.Status)
	require.Equal(t, 4, result.CodesSynced)
}
```

Add this child-workflow test in the same file:

```go
func TestSyncNACETaxonomyStartsClickHouseSyncAfterSuccessfulImport(t *testing.T) {
	env := testsuite.WorkflowTestSuite{}.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(SyncNACETaxonomy, workflow.RegisterOptions{Name: SyncWorkflowName})
	env.RegisterWorkflowWithOptions(SyncNACEToClickHouse, workflow.RegisterOptions{Name: SyncToClickHouseWorkflowName})
	env.RegisterActivityWithOptions(func(context.Context, SyncNACETaxonomyActivityInput) (SyncNACETaxonomyActivityResult, error) {
		return SyncNACETaxonomyActivityResult{
			Status:          SyncStatusSucceeded,
			ImportRunID:     "import-run-1",
			SourceFileID:    "source-file-1",
			ContentSHA256:   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			RecordsSeen:     4,
			RecordsImported: 4,
			Message:         "nace taxonomy imported",
		}, nil
	}, activity.RegisterOptions{Name: syncNACETaxonomyActivity})
	env.OnWorkflow(SyncToClickHouseWorkflowName, mock.Anything, SyncNACEToClickHouseInput{
		Trigger: "nace_taxonomy_sync",
	}).Return(SyncNACEToClickHouseResult{
		Status:      SyncStatusSucceeded,
		CodesSynced: 4,
	}, nil)

	env.ExecuteWorkflow(SyncNACETaxonomy, SyncNACETaxonomyInput{
		Revision:  "2.1",
		SourceURL: "https://example.test/nace.rdf",
		Trigger:   "manual",
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}
```

The test file needs these imports if they are not already present:

```go
import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)
```

- [ ] **Step 2: Add workflow constants and types**

In `scheduler/internal/nacetaxonomy/workflow.go`, add:

```go
const (
	SyncToClickHouseWorkflowName = "SyncNACEToClickHouse"
	syncNACEToClickHouseActivity = "SyncNACEToClickHouseActivity"
)

type SyncNACEToClickHouseInput struct {
	Trigger string `json:"trigger,omitempty"`
}

type SyncNACEToClickHouseResult = SyncNACEToClickHouseActivityResult
```

- [ ] **Step 3: Add standalone workflow**

```go
func SyncNACEToClickHouse(ctx temporalworkflow.Context, input SyncNACEToClickHouseInput) (SyncNACEToClickHouseResult, error) {
	if input.Trigger == "" {
		input.Trigger = "manual"
	}
	info := temporalworkflow.GetInfo(ctx)
	ctx = temporalworkflow.WithActivityOptions(ctx, temporalworkflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    10 * time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    2 * time.Minute,
			MaximumAttempts:    3,
		},
	})
	var result SyncNACEToClickHouseActivityResult
	if err := temporalworkflow.ExecuteActivity(ctx, syncNACEToClickHouseActivity, SyncNACEToClickHouseActivityInput{
		TemporalWorkflowID: info.WorkflowExecution.ID,
		Trigger:            input.Trigger,
	}).Get(ctx, &result); err != nil {
		return SyncNACEToClickHouseResult{}, errors.Wrap(err, "sync nace taxonomy to clickhouse activity")
	}
	return result, nil
}
```

- [ ] **Step 4: Extend existing taxonomy workflow**

After `SyncNACETaxonomyActivity` returns, if `result.Status` is `succeeded` or `skipped`, execute a child workflow:

```go
if result.Status == SyncStatusSucceeded || result.Status == SyncStatusSkipped {
	childOptions := temporalworkflow.ChildWorkflowOptions{
		WorkflowID: "nace-clickhouse-sync-" + info.WorkflowExecution.ID,
		TaskQueue: SyncTaskQueue,
	}
	childCtx := temporalworkflow.WithChildOptions(ctx, childOptions)
	var clickHouseResult SyncNACEToClickHouseResult
	if err := temporalworkflow.ExecuteChildWorkflow(childCtx, SyncToClickHouseWorkflowName, SyncNACEToClickHouseInput{
		Trigger: "nace_taxonomy_sync",
	}).Get(childCtx, &clickHouseResult); err != nil {
		return SyncNACETaxonomyResult{}, errors.Wrap(err, "sync nace taxonomy to clickhouse child workflow")
	}
}
```

Keep the public result compatible unless tests intentionally update it. The child workflow can complete without being embedded in the HTTP start response.

- [ ] **Step 5: Register workflow and activity**

In `scheduler/internal/app/nace_taxonomy_temporal.go`:

```go
worker.RegisterWorkflow(nacetaxonomy.SyncNACEToClickHouse)
worker.RegisterActivityWithOptions(
	resources.naceTaxonomyActions.SyncNACEToClickHouseActivity,
	activity.RegisterOptions{Name: "SyncNACEToClickHouseActivity"},
)
```

In `scheduler/internal/app/temporal.go`, update construction:

```go
naceTaxonomyActions: nacetaxonomy.NewActions(pool, http.DefaultClient, cfg.ClickHouseNativeURL),
```

- [ ] **Step 6: Run workflow/app tests**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
gofmt -w internal/nacetaxonomy/workflow.go internal/nacetaxonomy/workflow_test.go internal/app/temporal.go internal/app/nace_taxonomy_temporal.go
GOWORK=off go test ./internal/nacetaxonomy ./internal/app -count=1
```

Expected: pass.

- [ ] **Step 7: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
git add scheduler/internal/nacetaxonomy/workflow.go scheduler/internal/nacetaxonomy/workflow_test.go scheduler/internal/app/temporal.go scheduler/internal/app/nace_taxonomy_temporal.go
git commit -m "feat: add nace clickhouse sync workflow"
```

---

### Task 6: Add Manual HTTP API for Sync to ClickHouse

**Files:**
- Modify: `scheduler/internal/httpapi/workflow_triggers.go`
- Modify: `scheduler/internal/httpapi/handlers.go`
- Modify: `scheduler/internal/httpapi/workflow_triggers_test.go`

- [ ] **Step 1: Write failing HTTP handler test**

Add this test to `scheduler/internal/httpapi/workflow_triggers_test.go`:

```go
func TestStartNACEClickHouseSyncWorkflow(t *testing.T) {
	tc := &temporalExecuteRecorder{}
	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, nil, nil, "", tc, ""))
	body := strings.NewReader(`{"trigger":"manual"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/workflows/nace/clickhouse-sync", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusAccepted, w.Code)
	require.Equal(t, nacetaxonomy.SyncTaskQueue, tc.options.TaskQueue)
	require.Equal(t, nacetaxonomy.SyncNACEToClickHouse, tc.workflow)
	require.Equal(t, []interface{}{nacetaxonomy.SyncNACEToClickHouseInput{
		Trigger: "manual",
	}}, tc.args)
	require.Contains(t, w.Body.String(), nacetaxonomy.SyncToClickHouseWorkflowName)
}
```

Add imports if missing:

```go
import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pulsarpoint/corpscout/scheduler/internal/httpapi"
	"github.com/pulsarpoint/corpscout/scheduler/internal/nacetaxonomy"
)
```

- [ ] **Step 2: Add response/run types**

In `workflow_triggers.go`, add:

```go
const defaultNACEClickHouseWorkflowRunsLimit = 10
const maxNACEClickHouseWorkflowRunsLimit = 50

type startNACEClickHouseSyncWorkflowRequest struct {
	Trigger string `json:"trigger,omitempty"`
}

type naceClickHouseWorkflowRunListResponse struct {
	Items []naceTaxonomyWorkflowRunResponse `json:"items"`
}
```

- [ ] **Step 3: Add start handler**

```go
func (h *Handlers) handleStartNACEClickHouseSyncWorkflow(w http.ResponseWriter, r *http.Request) {
	if h.temporal == nil {
		writeError(w, http.StatusServiceUnavailable, "temporal client not available")
		return
	}
	req := startNACEClickHouseSyncWorkflowRequest{Trigger: "manual"}
	if r.Body != nil {
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&req); err != nil && !errors.Is(err, io.EOF) {
			writeError(w, http.StatusBadRequest, "invalid request body")
			return
		}
	}
	req.Trigger = strings.TrimSpace(req.Trigger)
	if req.Trigger == "" {
		req.Trigger = "manual"
	}
	workflowID := newWorkflowID("nace-clickhouse-sync")
	run, err := h.temporal.ExecuteWorkflow(
		r.Context(),
		client.StartWorkflowOptions{ID: workflowID, TaskQueue: nacetaxonomy.SyncTaskQueue},
		nacetaxonomy.SyncNACEToClickHouse,
		nacetaxonomy.SyncNACEToClickHouseInput{Trigger: req.Trigger},
	)
	if err != nil {
		slog.Error("start nace clickhouse sync workflow", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to start workflow")
		return
	}
	writeJSON(w, http.StatusAccepted, startWorkflowResponse{
		Status:        "started",
		Workflow:      nacetaxonomy.SyncToClickHouseWorkflowName,
		TaskQueue:     nacetaxonomy.SyncTaskQueue,
		WorkflowID:    workflowID,
		WorkflowRunID: run.GetRunID(),
	})
}
```

- [ ] **Step 4: Add list handler**

Use the same shape as `handleListNACETaxonomySyncWorkflowRuns`, but query:

```go
Query: "WorkflowType = 'SyncNACEToClickHouse'"
```

- [ ] **Step 5: Register routes**

In `scheduler/internal/httpapi/handlers.go`:

```go
r.Post("/workflows/nace/clickhouse-sync", h.handleStartNACEClickHouseSyncWorkflow)
r.Get("/workflows/nace/clickhouse-sync/runs", h.handleListNACEClickHouseSyncWorkflowRuns)
```

- [ ] **Step 6: Run HTTP tests**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
gofmt -w internal/httpapi/workflow_triggers.go internal/httpapi/handlers.go internal/httpapi/workflow_triggers_test.go
GOWORK=off go test ./internal/httpapi -run 'Test.*NACE.*ClickHouse' -count=1
```

Expected: pass.

- [ ] **Step 7: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
git add scheduler/internal/httpapi/workflow_triggers.go scheduler/internal/httpapi/handlers.go scheduler/internal/httpapi/workflow_triggers_test.go
git commit -m "feat: expose nace clickhouse sync workflow"
```

---

### Task 7: Add Settings UI Button

**Files:**
- Modify: `ui/app/types/api.ts`
- Modify: `ui/app/lib/api.ts`
- Modify: `ui/app/components/app/NACETaxonomySyncManagement.tsx`

- [ ] **Step 1: Add TypeScript API types**

In `ui/app/types/api.ts`, add:

```ts
export interface NACEClickHouseSyncRequest {
  trigger?: "manual" | "nace_taxonomy_sync";
}

export type NACEClickHouseWorkflowRun = NACETaxonomyWorkflowRun;

export interface NACEClickHouseWorkflowRunListResponse {
  items: NACEClickHouseWorkflowRun[];
}
```

- [ ] **Step 2: Add API client methods**

In `ui/app/lib/api.ts`, import the new types and add:

```ts
startNACEClickHouseSync: (body: NACEClickHouseSyncRequest = {}) =>
  post<StartWorkflowResponse>("/workflows/nace/clickhouse-sync", body),

getNACEClickHouseSyncRuns: (limit = 10) =>
  get<NACEClickHouseWorkflowRunListResponse>(
    `/workflows/nace/clickhouse-sync/runs?limit=${limit}`,
  ),
```

- [ ] **Step 3: Add UI state and button**

In `NACETaxonomySyncManagement.tsx`, add `syncingClickHouse`, `clickHouseMessage`, `clickHouseError`, and a handler:

```tsx
async function syncToClickHouse() {
  setSyncingClickHouse(true);
  setClickHouseError(null);
  setClickHouseMessage(null);
  try {
    const response = await api.startNACEClickHouseSync({ trigger: "manual" });
    setClickHouseMessage(`Started ${response.workflow_id}`);
  } catch (err) {
    setClickHouseError(errorMessage(err, "Failed to start NACE ClickHouse sync"));
  } finally {
    setSyncingClickHouse(false);
  }
}
```

Add a header button next to “Browse taxonomy” and “Schedules”:

```tsx
<Button variant="outline" onClick={() => void syncToClickHouse()} disabled={syncingClickHouse}>
  <RefreshCw data-icon="inline-start" />
  {syncingClickHouse ? "Starting..." : "Sync to CH"}
</Button>
```

Use `Alert` for `clickHouseError` and `clickHouseMessage`.

- [ ] **Step 4: Run UI typecheck**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/ui
npm run typecheck
```

Expected: pass.

- [ ] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
git add ui/app/types/api.ts ui/app/lib/api.ts ui/app/components/app/NACETaxonomySyncManagement.tsx
git commit -m "feat: add nace sync to clickhouse button"
```

---

### Task 8: End-to-End Verification

**Files:**
- No code changes expected.

- [ ] **Step 1: Run full scheduler tests**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./... -count=1
```

Expected: all tests pass.

- [ ] **Step 2: Run UI typecheck**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/ui
npm run typecheck
```

Expected: TypeScript passes.

- [ ] **Step 3: Apply ClickHouse migration**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
make clickhouse-migrate-up
```

Expected: migration `7/u create_nace_reference_tables` applies.

- [ ] **Step 4: Rebuild scheduler**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
make rebuild-local
```

Expected: scheduler starts and registers the new workflow/activity.

- [ ] **Step 5: Trigger manual sync API**

```bash
curl -sS -X POST http://127.0.0.1:5173/api/v1/workflows/nace/clickhouse-sync \
  -H 'Content-Type: application/json' \
  -d '{"trigger":"manual"}' | jq .
```

Expected: HTTP 202-style JSON with `workflow` equal to `SyncNACEToClickHouse`.

- [ ] **Step 6: Verify ClickHouse rows**

Use the ClickHouse client available for the remote server:

```sql
SELECT count() FROM corpscout_reference.nace_codes;
SELECT revision, count() FROM corpscout_reference.nace_codes GROUP BY revision ORDER BY revision;
SELECT code, section_code, division_code, group_code, class_code
FROM corpscout_reference.nace_codes
WHERE normalized_code IN ('8220', '82200')
LIMIT 10;
```

Expected: `nace_codes` has rows and hierarchy columns are populated for child codes.

- [ ] **Step 7: Browser verify settings page**

Open:

```text
http://127.0.0.1:5173/settings/nace-taxonomy
```

Expected: `Sync to CH` button is visible, starts the workflow, and shows a started workflow message.

---

## Self-Review

- Spec coverage: The plan covers ClickHouse migrations, Postgres export queries, ClickHouse writer support, Temporal workflow/activity, automatic child workflow trigger, manual API, UI button, and verification.
- Scope check: The plan intentionally excludes Finland explorer cache and NACE mapping to source industries.
- Placeholder scan: No task relies on unresolved behavior. Where tests depend on existing helpers, the plan names the existing patterns to reuse.
- Type consistency: Workflow names, API paths, and ClickHouse table names are consistent across tasks.
