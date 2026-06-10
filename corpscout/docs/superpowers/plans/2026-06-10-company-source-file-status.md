# Company Source File Status Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add first-class expected source files, per-file download history, deterministic Temporal workflow IDs based on Postgres run IDs, and UI controls to inspect and retry each source file.

**Architecture:** Postgres owns durable product state in `data_source_files`, `data_source_file_runs`, and `data_source_action_runs`. HTTP trigger handlers create run rows before starting Temporal and derive workflow IDs from those row IDs. Temporal executes source-specific file downloads and imports selected successful file runs into ClickHouse.

**Tech Stack:** Go, pgx/sqlc, PostgreSQL migrations, Temporal Go SDK, React Router, TypeScript, shadcn UI components, ClickHouse native writer.

---

## Scope Check

This plan implements one feature across existing layers:

```text
source catalog -> Postgres -> Temporal -> HTTP API -> UI
```

The ClickHouse import schema itself does not change. Source-specific importers only change how they receive raw source file paths.

## File Structure

### Database

- Create `database/migrations/000115_source_file_status.up.sql`
  - Adds `data_source_files`
  - Adds `data_source_file_runs`
  - Adds indexes
- Create `database/migrations/000115_source_file_status.down.sql`
  - Drops file-run and file-definition tables
- Modify `database/queries/sources.sql`
  - Adds source-file catalog sync queries
  - Adds file run create/update/list queries
  - Adds deterministic action-run creation/update queries

### Generated DB

- Modify generated files through sqlc:
  - `scheduler/internal/db/gen/models.go`
  - `scheduler/internal/db/gen/querier.go`
  - `scheduler/internal/db/gen/sources.sql.go`

### Catalog

- Modify `scheduler/internal/companysources/sourcecatalog/spec.go`
  - Adds `FileSpec`
  - Validates file definitions
- Modify `scheduler/internal/companysources/sourcecatalog/sync.go`
  - Upserts and disables file definitions
- Modify embedded JSON:
  - `scheduler/internal/companysources/sourcecatalog/sources/finland_prhytj.json`
  - `scheduler/internal/companysources/sourcecatalog/sources/united_states_coloradoentities.json`
  - `scheduler/internal/companysources/sourcecatalog/sources/united_states_irseobmf.json`
  - `scheduler/internal/companysources/sourcecatalog/sources/united_states_secedgar.json`
- Modify tests:
  - `scheduler/internal/companysources/sourcecatalog/catalog_test.go`
  - `scheduler/internal/companysources/sourcecatalog/sync_test.go`

### Source Runtime

- Modify `scheduler/internal/companysources/source.go`
  - Replaces whole-source download contract with file-specific download contract
  - Adds selected-file import support
- Modify `scheduler/internal/companysources/download.go`
  - Calls `DownloadFile`
- Modify `scheduler/internal/companysources/importer.go`
  - Passes selected file paths to source imports
- Modify source downloads:
  - `scheduler/internal/companysources/finland/prhytj/download.go`
  - `scheduler/internal/companysources/unitedstates/coloradoentities/download.go`
  - `scheduler/internal/companysources/unitedstates/irseobmf/download.go`
  - `scheduler/internal/companysources/unitedstates/secedgar/download.go`
- Modify source imports:
  - `scheduler/internal/companysources/finland/prhytj/import.go`
  - `scheduler/internal/companysources/unitedstates/coloradoentities/import.go`
  - `scheduler/internal/companysources/unitedstates/irseobmf/import.go`
  - `scheduler/internal/companysources/unitedstates/secedgar/import.go`

### Temporal

- Modify `scheduler/internal/temporal/workflow/companysources/workflow.go`
  - Adds file workflow
  - Adds deterministic workflow ID helpers
  - Changes action workflow inputs to include existing run IDs
- Modify `scheduler/internal/temporal/actions/companysources/actions.go`
  - Adds prepare, file download, finish, and import-selection activities
  - Removes action-run creation from activities that are started by HTTP
- Modify `scheduler/internal/app/temporal.go`
  - Registers new file workflow and activities directly

### HTTP API

- Modify `scheduler/internal/httpapi/source_actions.go`
  - Creates action runs before `ExecuteWorkflow`
  - Uses deterministic workflow IDs
  - Adds Temporal status handlers
- Create `scheduler/internal/httpapi/source_files.go`
  - Lists files
  - Lists file runs
  - Triggers one-file download
- Modify `scheduler/internal/httpapi/handlers.go`
  - Registers file and status routes
- Modify `scheduler/internal/httpapi/testhelpers_test.go`
  - Adds stub query methods
- Add tests:
  - `scheduler/internal/httpapi/source_files_test.go`
  - Extend `scheduler/internal/httpapi/source_actions_test.go`

### UI

- Modify `ui/app/types/api.ts`
  - Adds `SourceFile`, `SourceFileRun`, and status response types
- Modify `ui/app/lib/api.ts`
  - Adds source file API methods
- Modify `ui/app/components/app/source-detail/ActionsTab.tsx`
  - Loads file status
  - Shows file table
  - Adds per-file download action
  - Polls refresh after trigger

---

## Task 1: Add Source File Status Migration Tests

**Files:**
- Create: `scheduler/internal/db/source_file_status_migration_test.go`
- Create: `scheduler/internal/db/source_file_queries_shape_test.go`

- [ ] **Step 1: Write migration shape test**

Create `scheduler/internal/db/source_file_status_migration_test.go`:

```go
package db_test

import (
	"os"
	"strings"
	"testing"
)

func TestSourceFileStatusMigrationDefinesFileCatalogAndRuns(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000115_source_file_status.up.sql")
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	sql := string(body)

	required := []string{
		"CREATE TABLE data_source_files",
		"CREATE TABLE data_source_file_runs",
		"UNIQUE (source_id, file_key)",
		"kind IN ('source_snapshot', 'code_list', 'reference_data', 'archive')",
		"status IN ('running', 'succeeded', 'failed', 'missing', 'skipped', 'cancelled')",
		"idx_data_source_file_runs_file_started",
		"idx_data_source_file_runs_source_status",
		"idx_data_source_file_runs_parent_action",
	}
	for _, needle := range required {
		if !strings.Contains(sql, needle) {
			t.Fatalf("migration missing %q", needle)
		}
	}
}
```

- [ ] **Step 2: Write query shape test**

Create `scheduler/internal/db/source_file_queries_shape_test.go`:

```go
package db_test

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSourceFileQueriesExist(t *testing.T) {
	body, err := os.ReadFile("../../../database/queries/sources.sql")
	require.NoError(t, err)
	sql := string(body)

	required := []string{
		"-- name: UpsertDataSourceFileFromCatalog :exec",
		"-- name: DisableDataSourceFilesNotInCatalog :exec",
		"-- name: ListSourceFilesWithLatestRun :many",
		"-- name: GetSourceFileBySourceNameAndKey :one",
		"-- name: CreateSourceFileRun :one",
		"-- name: UpdateSourceFileRunTemporalRunID :exec",
		"-- name: FinishSourceFileRun :one",
		"-- name: ListSourceFileRuns :many",
		"-- name: ListSuccessfulSourceFileRunsForAction :many",
		"-- name: ListLatestSuccessfulRequiredSourceFileRuns :many",
		"-- name: GetSourceFileRunWithDefinition :one",
	}
	for _, needle := range required {
		require.Contains(t, sql, needle)
	}
}

func TestCreateSourceActionRunAcceptsDeterministicID(t *testing.T) {
	body, err := os.ReadFile("../../../database/queries/sources.sql")
	require.NoError(t, err)
	sql := string(body)

	start := strings.Index(sql, "-- name: CreateSourceActionRun :one")
	require.NotEqual(t, -1, start)
	createQuery := sql[start:]
	end := strings.Index(createQuery, "-- name: GetSourceActionRun :one")
	require.NotEqual(t, -1, end)
	createQuery = createQuery[:end]

	require.Contains(t, createQuery, "sqlc.arg(id)")
	require.Contains(t, createQuery, "sqlc.arg(temporal_workflow_id)")
	require.Contains(t, createQuery, "sqlc.arg(temporal_run_id)")
}
```

- [ ] **Step 3: Run tests and verify failure**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/db -run 'TestSourceFileStatusMigrationDefinesFileCatalogAndRuns|TestSourceFileQueriesExist|TestCreateSourceActionRunAcceptsDeterministicID' -count=1
```

Expected: FAIL because migration `000115_source_file_status.up.sql` and new queries do not exist.

- [ ] **Step 4: Commit failing tests**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
git add scheduler/internal/db/source_file_status_migration_test.go scheduler/internal/db/source_file_queries_shape_test.go
git commit -m "test: describe company source file status storage"
```

---

## Task 2: Add Source File Tables And Queries

**Files:**
- Create: `database/migrations/000115_source_file_status.up.sql`
- Create: `database/migrations/000115_source_file_status.down.sql`
- Modify: `database/queries/sources.sql`
- Generated: `scheduler/internal/db/gen/models.go`
- Generated: `scheduler/internal/db/gen/querier.go`
- Generated: `scheduler/internal/db/gen/sources.sql.go`

- [ ] **Step 1: Add migration up file**

Create `database/migrations/000115_source_file_status.up.sql`:

```sql
CREATE TABLE data_source_files (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source_id UUID NOT NULL REFERENCES data_sources(id) ON DELETE CASCADE,
  file_key TEXT NOT NULL,
  display_name TEXT NOT NULL,
  description TEXT,
  kind TEXT NOT NULL,
  required BOOLEAN NOT NULL DEFAULT true,
  relative_path TEXT NOT NULL,
  enabled BOOLEAN NOT NULL DEFAULT true,
  sort_order INTEGER NOT NULL DEFAULT 0,
  config JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (source_id, file_key),
  CONSTRAINT chk_data_source_files_file_key CHECK (btrim(file_key) <> ''),
  CONSTRAINT chk_data_source_files_display_name CHECK (btrim(display_name) <> ''),
  CONSTRAINT chk_data_source_files_relative_path CHECK (btrim(relative_path) <> ''),
  CONSTRAINT chk_data_source_files_kind CHECK (
    kind IN ('source_snapshot', 'code_list', 'reference_data', 'archive')
  ),
  CONSTRAINT chk_data_source_files_config_object CHECK (jsonb_typeof(config) = 'object')
);

CREATE TABLE data_source_file_runs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source_id UUID NOT NULL REFERENCES data_sources(id) ON DELETE CASCADE,
  source_file_id UUID NOT NULL REFERENCES data_source_files(id) ON DELETE CASCADE,
  parent_action_run_id UUID REFERENCES data_source_action_runs(id) ON DELETE SET NULL,
  status TEXT NOT NULL,
  temporal_workflow_id TEXT,
  temporal_run_id TEXT,
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at TIMESTAMPTZ,
  path TEXT,
  content_sha256 TEXT,
  content_length_bytes BIGINT,
  records_written BIGINT,
  error_message TEXT,
  log JSONB NOT NULL DEFAULT '[]'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_data_source_file_runs_status CHECK (
    status IN ('running', 'succeeded', 'failed', 'missing', 'skipped', 'cancelled')
  ),
  CONSTRAINT chk_data_source_file_runs_log_array CHECK (jsonb_typeof(log) = 'array'),
  CONSTRAINT chk_data_source_file_runs_finished_at CHECK (
    (status = 'running' AND finished_at IS NULL)
    OR (status <> 'running' AND finished_at IS NOT NULL)
  )
);

CREATE INDEX idx_data_source_files_source_sort
  ON data_source_files (source_id, sort_order, file_key);

CREATE INDEX idx_data_source_file_runs_file_started
  ON data_source_file_runs (source_file_id, started_at DESC);

CREATE INDEX idx_data_source_file_runs_source_status
  ON data_source_file_runs (source_id, status, started_at DESC);

CREATE INDEX idx_data_source_file_runs_parent_action
  ON data_source_file_runs (parent_action_run_id)
  WHERE parent_action_run_id IS NOT NULL;
```

- [ ] **Step 2: Add migration down file**

Create `database/migrations/000115_source_file_status.down.sql`:

```sql
DROP TABLE IF EXISTS data_source_file_runs;
DROP TABLE IF EXISTS data_source_files;
```

- [ ] **Step 3: Update action-run creation query for deterministic IDs**

In `database/queries/sources.sql`, replace the `CreateSourceActionRun` insert with:

```sql
-- name: CreateSourceActionRun :one
INSERT INTO data_source_action_runs (
  id,
  source_id,
  action_id,
  action,
  status,
  temporal_workflow_id,
  temporal_run_id,
  input,
  result
)
SELECT
  sqlc.arg(id),
  a.source_id,
  a.id,
  a.action,
  'running',
  sqlc.arg(temporal_workflow_id),
  sqlc.arg(temporal_run_id),
  sqlc.arg(input),
  '{}'::jsonb
FROM data_source_actions a
WHERE a.id = sqlc.arg(action_id)
RETURNING *;
```

- [ ] **Step 4: Add action-run Temporal run update query**

Add after `GetSourceActionRun`:

```sql
-- name: UpdateSourceActionRunTemporalRunID :exec
UPDATE data_source_action_runs
SET temporal_run_id = sqlc.arg(temporal_run_id)
WHERE id = sqlc.arg(id);
```

- [ ] **Step 5: Add source file catalog sync queries**

Add near `UpsertDataSourceFromCatalog`:

```sql
-- name: UpsertDataSourceFileFromCatalog :exec
INSERT INTO data_source_files (
  source_id,
  file_key,
  display_name,
  description,
  kind,
  required,
  relative_path,
  enabled,
  sort_order,
  config
)
SELECT
  s.id,
  sqlc.arg(file_key),
  sqlc.arg(display_name),
  NULLIF(sqlc.arg(description)::text, ''),
  sqlc.arg(kind),
  sqlc.arg(required),
  sqlc.arg(relative_path),
  sqlc.arg(enabled),
  sqlc.arg(sort_order),
  sqlc.arg(config)
FROM data_sources s
WHERE s.registry_key = sqlc.arg(registry_key)
ON CONFLICT (source_id, file_key) DO UPDATE SET
  display_name = EXCLUDED.display_name,
  description = EXCLUDED.description,
  kind = EXCLUDED.kind,
  required = EXCLUDED.required,
  relative_path = EXCLUDED.relative_path,
  enabled = EXCLUDED.enabled,
  sort_order = EXCLUDED.sort_order,
  config = EXCLUDED.config,
  updated_at = now();

-- name: DisableDataSourceFilesNotInCatalog :exec
UPDATE data_source_files f
SET enabled = false, updated_at = now()
FROM data_sources s
WHERE s.id = f.source_id
  AND s.registry_key = sqlc.arg(registry_key)
  AND NOT (f.file_key = ANY(sqlc.arg(file_keys)::text[]));
```

- [ ] **Step 6: Add source file read queries**

Add:

```sql
-- name: ListSourceFilesWithLatestRun :many
WITH latest AS (
  SELECT DISTINCT ON (r.source_file_id)
    r.*
  FROM data_source_file_runs r
  ORDER BY r.source_file_id, r.started_at DESC
),
latest_success AS (
  SELECT DISTINCT ON (r.source_file_id)
    r.*
  FROM data_source_file_runs r
  WHERE r.status = 'succeeded'
  ORDER BY r.source_file_id, r.finished_at DESC NULLS LAST, r.started_at DESC
)
SELECT
  f.id,
  f.source_id,
  s.name AS source_name,
  f.file_key,
  f.display_name,
  f.description,
  f.kind,
  f.required,
  f.relative_path,
  f.enabled,
  f.sort_order,
  f.config,
  f.created_at,
  f.updated_at,
  latest.id AS latest_run_id,
  latest.status AS latest_status,
  latest.started_at AS latest_started_at,
  latest.finished_at AS latest_finished_at,
  latest.path AS latest_path,
  latest.content_sha256 AS latest_content_sha256,
  latest.content_length_bytes AS latest_content_length_bytes,
  latest.records_written AS latest_records_written,
  latest.error_message AS latest_error_message,
  latest_success.id AS latest_successful_run_id,
  latest_success.path AS latest_successful_path
FROM data_source_files f
JOIN data_sources s ON s.id = f.source_id
LEFT JOIN latest ON latest.source_file_id = f.id
LEFT JOIN latest_success ON latest_success.source_file_id = f.id
WHERE s.name = $1
ORDER BY f.sort_order, f.file_key;

-- name: GetSourceFileBySourceNameAndKey :one
SELECT
  f.*,
  s.name AS source_name,
  s.country,
  s.source,
  s.registry_key,
  COALESCE(s.source_url, '') AS source_url,
  s.user_agent_required
FROM data_source_files f
JOIN data_sources s ON s.id = f.source_id
WHERE s.name = $1
  AND f.file_key = $2
  AND f.enabled = true;
```

- [ ] **Step 7: Add source file run write and selection queries**

Add:

```sql
-- name: CreateSourceFileRun :one
INSERT INTO data_source_file_runs (
  id,
  source_id,
  source_file_id,
  parent_action_run_id,
  status,
  temporal_workflow_id,
  temporal_run_id,
  log
)
SELECT
  sqlc.arg(id),
  f.source_id,
  f.id,
  sqlc.narg(parent_action_run_id),
  'running',
  sqlc.arg(temporal_workflow_id),
  sqlc.narg(temporal_run_id),
  '[]'::jsonb
FROM data_source_files f
WHERE f.id = sqlc.arg(source_file_id)
RETURNING *;

-- name: UpdateSourceFileRunTemporalRunID :exec
UPDATE data_source_file_runs
SET temporal_run_id = sqlc.arg(temporal_run_id)
WHERE id = sqlc.arg(id);

-- name: FinishSourceFileRun :one
UPDATE data_source_file_runs
SET
  status = sqlc.arg(status),
  finished_at = now(),
  path = NULLIF(sqlc.arg(path)::text, ''),
  content_sha256 = NULLIF(sqlc.arg(content_sha256)::text, ''),
  content_length_bytes = sqlc.narg(content_length_bytes),
  records_written = sqlc.narg(records_written),
  error_message = NULLIF(sqlc.arg(error_message)::text, ''),
  log = sqlc.arg(log)
WHERE id = sqlc.arg(id)
RETURNING *;

-- name: ListSourceFileRuns :many
SELECT
  r.*,
  f.file_key,
  f.display_name,
  s.name AS source_name
FROM data_source_file_runs r
JOIN data_source_files f ON f.id = r.source_file_id
JOIN data_sources s ON s.id = r.source_id
WHERE s.name = sqlc.arg(source_name)
  AND f.file_key = sqlc.arg(file_key)
ORDER BY r.started_at DESC
LIMIT sqlc.arg(limit);

-- name: ListSuccessfulSourceFileRunsForAction :many
SELECT
  r.*,
  f.file_key,
  f.relative_path,
  f.required
FROM data_source_file_runs r
JOIN data_source_files f ON f.id = r.source_file_id
WHERE r.parent_action_run_id = $1
  AND r.status = 'succeeded'
ORDER BY f.sort_order, f.file_key;

-- name: ListLatestSuccessfulRequiredSourceFileRuns :many
WITH latest AS (
  SELECT DISTINCT ON (f.id)
    r.*,
    f.file_key,
    f.relative_path,
    f.required
  FROM data_source_files f
  JOIN data_sources s ON s.id = f.source_id
  LEFT JOIN data_source_file_runs r
    ON r.source_file_id = f.id
   AND r.status = 'succeeded'
  WHERE s.name = $1
    AND f.enabled = true
    AND f.required = true
  ORDER BY f.id, r.finished_at DESC NULLS LAST, r.started_at DESC
)
SELECT *
FROM latest
ORDER BY file_key;

-- name: GetSourceFileRunWithDefinition :one
SELECT
  r.*,
  f.file_key,
  f.kind,
  f.relative_path,
  f.required,
  f.config,
  s.name AS source_name,
  s.country,
  s.source,
  COALESCE(s.source_url, '') AS source_url,
  s.user_agent_required
FROM data_source_file_runs r
JOIN data_source_files f ON f.id = r.source_file_id
JOIN data_sources s ON s.id = r.source_id
WHERE r.id = $1;
```

- [ ] **Step 8: Generate sqlc code**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
make sqlc-generate
```

Expected: sqlc completes and updates generated files under `scheduler/internal/db/gen`.

- [ ] **Step 9: Run DB tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/db -run 'TestSourceFileStatusMigrationDefinesFileCatalogAndRuns|TestSourceFileQueriesExist|TestCreateSourceActionRunAcceptsDeterministicID' -count=1
```

Expected: PASS.

- [ ] **Step 10: Commit database schema and queries**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
git add database/migrations/000115_source_file_status.up.sql database/migrations/000115_source_file_status.down.sql database/queries/sources.sql scheduler/internal/db/gen scheduler/internal/db/source_file_status_migration_test.go scheduler/internal/db/source_file_queries_shape_test.go
git commit -m "feat: add company source file status storage"
```

---

## Task 3: Extend Source Catalog With File Definitions

**Files:**
- Modify: `scheduler/internal/companysources/sourcecatalog/spec.go`
- Modify: `scheduler/internal/companysources/sourcecatalog/sync.go`
- Modify: `scheduler/internal/companysources/sourcecatalog/catalog_test.go`
- Modify: `scheduler/internal/companysources/sourcecatalog/sync_test.go`
- Modify JSON files under `scheduler/internal/companysources/sourcecatalog/sources/`

- [ ] **Step 1: Write catalog tests for file definitions**

Update `scheduler/internal/companysources/sourcecatalog/catalog_test.go`:

```go
func TestLoadEmbeddedSpecsIncludesSourceFiles(t *testing.T) {
	specs, err := LoadEmbedded()
	require.NoError(t, err)

	byRegistryKey := map[string]Spec{}
	for _, spec := range specs {
		byRegistryKey[spec.RegistryKey] = spec
	}

	finland := byRegistryKey["finland/prhytj"]
	require.NotEmpty(t, finland.Files)
	requireFileKeys(t, finland.Files, []string{
		"source",
		"codelist_REK_en",
		"codelist_REK_KDI_en",
		"codelist_VIRANOM_en",
		"codelist_TLAJI_en",
		"codelist_YRMU_en",
		"codelist_STATUS3_en",
		"codelist_KIELI_en",
	})

	sec := byRegistryKey["united_states/secedgar"]
	requireFileKeys(t, sec.Files, []string{"source"})
}

func requireFileKeys(t *testing.T, files []FileSpec, expected []string) {
	t.Helper()
	got := make([]string, 0, len(files))
	for _, file := range files {
		got = append(got, file.FileKey)
	}
	require.ElementsMatch(t, expected, got)
}
```

- [ ] **Step 2: Update sync test fake store**

In `scheduler/internal/companysources/sourcecatalog/sync_test.go`, extend `fakeStore`:

```go
type fakeStore struct {
	upserts     []db.UpsertDataSourceFromCatalogParams
	fileUpserts []db.UpsertDataSourceFileFromCatalogParams
	disabled    map[string][]string
	pruned      []string
}

func (s *fakeStore) UpsertDataSourceFileFromCatalog(ctx context.Context, arg db.UpsertDataSourceFileFromCatalogParams) error {
	s.fileUpserts = append(s.fileUpserts, arg)
	return nil
}

func (s *fakeStore) DisableDataSourceFilesNotInCatalog(ctx context.Context, arg db.DisableDataSourceFilesNotInCatalogParams) error {
	if s.disabled == nil {
		s.disabled = map[string][]string{}
	}
	s.disabled[arg.RegistryKey] = append([]string(nil), arg.FileKeys...)
	return nil
}
```

Add assertions to `TestSyncUpsertsAndPrunesCatalogSources`:

```go
require.NotEmpty(t, store.fileUpserts)
require.Contains(t, store.disabled, "finland/prhytj")
require.Contains(t, store.disabled["finland/prhytj"], "source")
```

- [ ] **Step 3: Run catalog tests and verify failure**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/companysources/sourcecatalog -count=1
```

Expected: FAIL because `FileSpec` and store methods do not exist.

- [ ] **Step 4: Add file spec model and validation**

Update `scheduler/internal/companysources/sourcecatalog/spec.go`:

```go
type Spec struct {
	Name                  string     `json:"name"`
	Country               string     `json:"country"`
	Source                string     `json:"source"`
	RegistryKey           string     `json:"registry_key"`
	DisplayName           string     `json:"display_name"`
	Description           string     `json:"description"`
	SourceGroup           string     `json:"source_group"`
	InputTableName        string     `json:"input_table_name"`
	Enabled               bool       `json:"enabled"`
	AuthRequired          bool       `json:"auth_required"`
	StorageKind           string     `json:"storage_kind"`
	ClickHouseDatabase    string     `json:"clickhouse_database"`
	ClickHouseTablePrefix string     `json:"clickhouse_table_prefix"`
	SourceURL             string     `json:"source_url"`
	DocsURL               string     `json:"docs_url"`
	RawSourceRetention    string     `json:"raw_source_retention"`
	SourceFileName        string     `json:"source_file_name"`
	UserAgentRequired     bool       `json:"user_agent_required"`
	Capabilities          []string   `json:"capabilities"`
	RequiresTranslation   bool       `json:"requires_translation"`
	Files                 []FileSpec `json:"files"`
}

type FileSpec struct {
	FileKey      string         `json:"file_key"`
	DisplayName  string         `json:"display_name"`
	Description  string         `json:"description"`
	Kind         string         `json:"kind"`
	Required     bool           `json:"required"`
	RelativePath string         `json:"relative_path"`
	Enabled      bool           `json:"enabled"`
	SortOrder    int32          `json:"sort_order"`
	Config       map[string]any `json:"config"`
}
```

Add validation helpers:

```go
func (s Spec) validateFiles() error {
	if len(s.Files) == 0 {
		return errors.Errorf("source spec %s must define at least one file", s.RegistryKey)
	}
	seen := map[string]struct{}{}
	for _, file := range s.Files {
		if err := file.Validate(); err != nil {
			return errors.Wrapf(err, "validate source file %s/%s", s.RegistryKey, file.FileKey)
		}
		if _, ok := seen[file.FileKey]; ok {
			return errors.Errorf("source spec %s has duplicate file key %q", s.RegistryKey, file.FileKey)
		}
		seen[file.FileKey] = struct{}{}
	}
	return nil
}

func (f FileSpec) Validate() error {
	required := map[string]string{
		"file_key":      f.FileKey,
		"display_name":  f.DisplayName,
		"kind":          f.Kind,
		"relative_path": f.RelativePath,
	}
	for field, value := range required {
		if strings.TrimSpace(value) == "" {
			return errors.Errorf("source file spec %s is required", field)
		}
	}
	switch f.Kind {
	case "source_snapshot", "code_list", "reference_data", "archive":
		return nil
	default:
		return errors.Errorf("source file spec kind %q is not supported", f.Kind)
	}
}
```

Call `validateFiles` at the end of `Spec.Validate`:

```go
return s.validateFiles()
```

- [ ] **Step 5: Extend source catalog sync**

Update `scheduler/internal/companysources/sourcecatalog/sync.go` `Store`:

```go
type Store interface {
	UpsertDataSourceFromCatalog(ctx context.Context, arg db.UpsertDataSourceFromCatalogParams) error
	UpsertDataSourceFileFromCatalog(ctx context.Context, arg db.UpsertDataSourceFileFromCatalogParams) error
	DisableDataSourceFilesNotInCatalog(ctx context.Context, arg db.DisableDataSourceFilesNotInCatalogParams) error
	PruneDataSourcesNotInCatalog(ctx context.Context, registryKeys []string) error
}
```

After each source upsert, sync files:

```go
fileKeys := make([]string, 0, len(spec.Files))
for _, file := range spec.Files {
	fileKeys = append(fileKeys, file.FileKey)
	config := file.Config
	if config == nil {
		config = map[string]any{}
	}
	configData, err := json.Marshal(config)
	if err != nil {
		return errors.Wrapf(err, "marshal source file config %s/%s", spec.RegistryKey, file.FileKey)
	}
	if err := store.UpsertDataSourceFileFromCatalog(ctx, db.UpsertDataSourceFileFromCatalogParams{
		RegistryKey:  spec.RegistryKey,
		FileKey:      file.FileKey,
		DisplayName:  file.DisplayName,
		Description:  file.Description,
		Kind:         file.Kind,
		Required:     file.Required,
		RelativePath: file.RelativePath,
		Enabled:      file.Enabled,
		SortOrder:    file.SortOrder,
		Config:       configData,
	}); err != nil {
		return errors.Wrapf(err, "upsert source file catalog spec %s/%s", spec.RegistryKey, file.FileKey)
	}
}
if err := store.DisableDataSourceFilesNotInCatalog(ctx, db.DisableDataSourceFilesNotInCatalogParams{
	RegistryKey: spec.RegistryKey,
	FileKeys:    fileKeys,
}); err != nil {
	return errors.Wrapf(err, "disable stale source file catalog specs %s", spec.RegistryKey)
}
```

- [ ] **Step 6: Add file definitions to embedded JSON**

For `scheduler/internal/companysources/sourcecatalog/sources/finland_prhytj.json`, add:

```json
"files": [
  {
    "file_key": "source",
    "display_name": "Company source snapshot",
    "description": "Raw PRH YTJ company snapshot preserved as NDJSON.",
    "kind": "source_snapshot",
    "required": true,
    "relative_path": "source.ndjson",
    "enabled": true,
    "sort_order": 10
  },
  {
    "file_key": "codelist_REK_en",
    "display_name": "Register code list",
    "description": "English labels for PRH register codes.",
    "kind": "code_list",
    "required": true,
    "relative_path": "codelists/REK.en.tsv",
    "enabled": true,
    "sort_order": 20,
    "config": { "code": "REK", "lang": "en" }
  },
  {
    "file_key": "codelist_REK_KDI_en",
    "display_name": "Register entry status code list",
    "description": "English labels for PRH register entry status codes.",
    "kind": "code_list",
    "required": true,
    "relative_path": "codelists/REK_KDI.en.tsv",
    "enabled": true,
    "sort_order": 30,
    "config": { "code": "REK_KDI", "lang": "en" }
  },
  {
    "file_key": "codelist_VIRANOM_en",
    "display_name": "Authority code list",
    "description": "English labels for PRH authority codes.",
    "kind": "code_list",
    "required": true,
    "relative_path": "codelists/VIRANOM.en.tsv",
    "enabled": true,
    "sort_order": 40,
    "config": { "code": "VIRANOM", "lang": "en" }
  },
  {
    "file_key": "codelist_TLAJI_en",
    "display_name": "Business name type code list",
    "description": "English labels for PRH business name type codes.",
    "kind": "code_list",
    "required": true,
    "relative_path": "codelists/TLAJI.en.tsv",
    "enabled": true,
    "sort_order": 50,
    "config": { "code": "TLAJI", "lang": "en" }
  },
  {
    "file_key": "codelist_YRMU_en",
    "display_name": "Company form code list",
    "description": "English labels for PRH company form codes.",
    "kind": "code_list",
    "required": true,
    "relative_path": "codelists/YRMU.en.tsv",
    "enabled": true,
    "sort_order": 60,
    "config": { "code": "YRMU", "lang": "en" }
  },
  {
    "file_key": "codelist_STATUS3_en",
    "display_name": "Business ID status code list",
    "description": "English labels for PRH Business ID status codes.",
    "kind": "code_list",
    "required": true,
    "relative_path": "codelists/STATUS3.en.tsv",
    "enabled": true,
    "sort_order": 70,
    "config": { "code": "STATUS3", "lang": "en" }
  },
  {
    "file_key": "codelist_KIELI_en",
    "display_name": "Language code list",
    "description": "English labels for PRH language codes.",
    "kind": "code_list",
    "required": true,
    "relative_path": "codelists/KIELI.en.tsv",
    "enabled": true,
    "sort_order": 80,
    "config": { "code": "KIELI", "lang": "en" }
  }
]
```

For each United States JSON file, add:

```json
"files": [
  {
    "file_key": "source",
    "display_name": "Source snapshot",
    "description": "Raw source snapshot preserved for ClickHouse import.",
    "kind": "source_snapshot",
    "required": true,
    "relative_path": "source.ndjson",
    "enabled": true,
    "sort_order": 10
  }
]
```

For SEC EDGAR use `"relative_path": "source.json"`.

- [ ] **Step 7: Run catalog tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/companysources/sourcecatalog -count=1
```

Expected: PASS.

- [ ] **Step 8: Commit catalog changes**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
git add scheduler/internal/companysources/sourcecatalog
git commit -m "feat: sync source file catalog definitions"
```

---

## Task 4: Refactor Source Download Contract To File Downloads

**Files:**
- Modify: `scheduler/internal/companysources/source.go`
- Modify: `scheduler/internal/companysources/download.go`
- Modify: `scheduler/internal/companysources/download_http.go`
- Modify source-specific download files and tests

- [ ] **Step 1: Write core download contract test**

Update `scheduler/internal/companysources/download_test.go`:

```go
type downloadingSource struct {
	got companysources.DownloadFileOptions
}

func (s *downloadingSource) Key() companysources.Key {
	return companysources.Key{Country: "finland", Source: "prhytj"}
}

func (s *downloadingSource) DisplayName() string { return "Finland PRH YTJ" }

func (s *downloadingSource) DownloadFile(ctx context.Context, opts companysources.DownloadFileOptions) (companysources.DownloadedFile, error) {
	s.got = opts
	return companysources.DownloadedFile{
		FileKey:            opts.FileKey,
		Kind:               opts.FileKind,
		RunDir:             opts.RunDir,
		Path:               filepath.Join(opts.RunDir, opts.RelativePath),
		RelativePath:       opts.RelativePath,
		ContentSHA256:      "abc123",
		ContentLengthBytes: 10,
		RecordsWritten:     2,
	}, nil
}

func (s *downloadingSource) Import(context.Context, companysources.ImportOptions) (companysources.ImportResult, error) {
	return companysources.ImportResult{}, nil
}

func TestDownloadFilePassesFileDefinitionToSource(t *testing.T) {
	source := &downloadingSource{}
	registry := companysources.NewRegistry(source)

	result, err := companysources.DownloadFile(context.Background(), registry, companysources.DownloadFileRequest{
		Country:           "finland",
		Source:            "prhytj",
		FileKey:           "codelist_REK_en",
		FileKind:          "code_list",
		RunDir:            t.TempDir(),
		RelativePath:      "codelists/REK.en.tsv",
		SourceURL:         "https://avoindata.prh.fi/opendata-ytj-api/v3/description?code=REK&lang=en",
		UserAgentRequired: false,
		Config:            map[string]any{"code": "REK", "lang": "en"},
	})
	require.NoError(t, err)
	require.Equal(t, "codelist_REK_en", result.FileKey)
	require.Equal(t, "code_list", result.Kind)
	require.Equal(t, "codelists/REK.en.tsv", source.got.RelativePath)
}
```

- [ ] **Step 2: Run test and verify failure**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/companysources -run TestDownloadFilePassesFileDefinitionToSource -count=1
```

Expected: FAIL because `DownloadFile` contract does not exist.

- [ ] **Step 3: Replace source download types**

Update `scheduler/internal/companysources/source.go`:

```go
type Source interface {
	Key() Key
	DisplayName() string
	DownloadFile(ctx context.Context, opts DownloadFileOptions) (DownloadedFile, error)
	Import(ctx context.Context, opts ImportOptions) (ImportResult, error)
}

type DownloadFileOptions struct {
	FileKey           string
	FileKind          string
	RunDir            string
	RelativePath      string
	SourceURL         string
	UserAgentRequired bool
	Config            map[string]any
}

type DownloadedFile struct {
	FileKey            string `json:"file_key"`
	Kind               string `json:"kind"`
	RunDir             string `json:"run_dir"`
	Path               string `json:"path"`
	RelativePath       string `json:"relative_path"`
	ContentSHA256      string `json:"content_sha256"`
	ContentLengthBytes int64  `json:"content_length_bytes"`
	RecordsWritten     int64  `json:"records_written"`
}

type DownloadFileRequest struct {
	Country           string
	Source            string
	FileKey           string
	FileKind          string
	RunDir            string
	RelativePath      string
	SourceURL         string
	UserAgentRequired bool
	Config            map[string]any
}
```

Remove the old `DownloadOptions`, `DownloadResult`, and `DownloadRunRequest`.

- [ ] **Step 4: Replace download coordinator**

Update `scheduler/internal/companysources/download.go`:

```go
package companysources

import "context"

func DownloadFile(ctx context.Context, registry Registry, req DownloadFileRequest) (DownloadedFile, error) {
	source, err := registry.Get(req.Country, req.Source)
	if err != nil {
		return DownloadedFile{}, err
	}
	return source.DownloadFile(ctx, DownloadFileOptions{
		FileKey:           req.FileKey,
		FileKind:          req.FileKind,
		RunDir:            req.RunDir,
		RelativePath:      req.RelativePath,
		SourceURL:         req.SourceURL,
		UserAgentRequired: req.UserAgentRequired,
		Config:            req.Config,
	})
}
```

- [ ] **Step 5: Make HTTP file writer accept relative path**

Update `DirectFileDownload` in `scheduler/internal/companysources/download_http.go`:

```go
type DirectFileDownload struct {
	URL               string
	RunDir            string
	RelativePath      string
	UserAgentRequired bool
}
```

Replace the file-name validation and path creation:

```go
if req.RelativePath == "" {
	return FileWriteResult{}, errors.New("relative path is required")
}
path := filepath.Join(req.RunDir, req.RelativePath)
if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
	return FileWriteResult{}, errors.Wrap(err, "create source file directory")
}
```

- [ ] **Step 6: Update United States source downloads**

For JSON-array to NDJSON sources, use this pattern:

```go
func (Source) DownloadFile(ctx context.Context, opts companysources.DownloadFileOptions) (companysources.DownloadedFile, error) {
	relativePath := opts.RelativePath
	if relativePath == "" {
		relativePath = "source.ndjson"
	}
	records, err := companysources.DownloadJSONArray(ctx, http.DefaultClient, opts.SourceURL, opts.UserAgentRequired)
	if err != nil {
		return companysources.DownloadedFile{}, err
	}
	written, err := companysources.WriteRawMessagesAsNDJSON(filepath.Join(opts.RunDir, relativePath), records)
	if err != nil {
		return companysources.DownloadedFile{}, err
	}
	return companysources.DownloadedFile{
		FileKey:            opts.FileKey,
		Kind:               opts.FileKind,
		RunDir:             opts.RunDir,
		Path:               written.SourceFilePath,
		RelativePath:       relativePath,
		ContentSHA256:      written.ContentSHA256,
		ContentLengthBytes: written.ContentLengthBytes,
		RecordsWritten:     written.RecordsWritten,
	}, nil
}
```

For SEC EDGAR direct JSON, use:

```go
func (Source) DownloadFile(ctx context.Context, opts companysources.DownloadFileOptions) (companysources.DownloadedFile, error) {
	relativePath := opts.RelativePath
	if relativePath == "" {
		relativePath = "source.json"
	}
	written, err := companysources.DownloadDirectFile(ctx, http.DefaultClient, companysources.DirectFileDownload{
		URL:               opts.SourceURL,
		RunDir:            opts.RunDir,
		RelativePath:      relativePath,
		UserAgentRequired: opts.UserAgentRequired,
	})
	if err != nil {
		return companysources.DownloadedFile{}, err
	}
	return companysources.DownloadedFile{
		FileKey:            opts.FileKey,
		Kind:               opts.FileKind,
		RunDir:             opts.RunDir,
		Path:               written.SourceFilePath,
		RelativePath:       relativePath,
		ContentSHA256:      written.ContentSHA256,
		ContentLengthBytes: written.ContentLengthBytes,
		RecordsWritten:     written.RecordsWritten,
	}, nil
}
```

- [ ] **Step 7: Update Finland source download**

In `scheduler/internal/companysources/finland/prhytj/download.go`, route by file key:

```go
func (Source) DownloadFile(ctx context.Context, opts companysources.DownloadFileOptions) (companysources.DownloadedFile, error) {
	switch opts.FileKind {
	case "source_snapshot":
		return downloadSourceSnapshot(ctx, opts)
	case "code_list":
		return downloadCodeList(ctx, opts)
	default:
		return companysources.DownloadedFile{}, errors.Errorf("unsupported PRH YTJ file kind %q", opts.FileKind)
	}
}
```

Use PRH description endpoint for code lists:

```go
func downloadCodeList(ctx context.Context, opts companysources.DownloadFileOptions) (companysources.DownloadedFile, error) {
	code, _ := opts.Config["code"].(string)
	lang, _ := opts.Config["lang"].(string)
	if code == "" {
		return companysources.DownloadedFile{}, errors.New("PRH YTJ code list code is required")
	}
	if lang == "" {
		lang = "en"
	}
	sourceURL := opts.SourceURL
	if sourceURL == "" {
		sourceURL = DefaultBaseURL
	}
	u, err := url.Parse(sourceURL)
	if err != nil {
		return companysources.DownloadedFile{}, errors.Wrap(err, "parse PRH YTJ source url")
	}
	u.Path = "/opendata-ytj-api/v3/description"
	q := u.Query()
	q.Set("code", code)
	q.Set("lang", lang)
	u.RawQuery = q.Encode()

	written, err := companysources.DownloadDirectFile(ctx, http.DefaultClient, companysources.DirectFileDownload{
		URL:               u.String(),
		RunDir:            opts.RunDir,
		RelativePath:      opts.RelativePath,
		UserAgentRequired: opts.UserAgentRequired,
	})
	if err != nil {
		return companysources.DownloadedFile{}, err
	}
	return companysources.DownloadedFile{
		FileKey:            opts.FileKey,
		Kind:               opts.FileKind,
		RunDir:             opts.RunDir,
		Path:               written.SourceFilePath,
		RelativePath:       opts.RelativePath,
		ContentSHA256:      written.ContentSHA256,
		ContentLengthBytes: written.ContentLengthBytes,
		RecordsWritten:     written.RecordsWritten,
	}, nil
}
```

Keep the current one-page company snapshot behavior inside `downloadSourceSnapshot`; only change its return type to `DownloadedFile`.

- [ ] **Step 8: Run source tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/companysources/... -count=1
```

Expected: PASS.

- [ ] **Step 9: Commit source download refactor**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
git add scheduler/internal/companysources
git commit -m "feat: download individual company source files"
```

---

## Task 5: Add Selected File Import Contract

**Files:**
- Modify: `scheduler/internal/companysources/source.go`
- Modify: `scheduler/internal/companysources/importer.go`
- Modify source-specific import files
- Modify source import tests

- [ ] **Step 1: Add selected file helpers**

Update `scheduler/internal/companysources/source.go`:

```go
type SelectedSourceFile struct {
	FileKey      string
	Path         string
	RelativePath string
}

type ImportOptions struct {
	RunDir              string
	Files               []SelectedSourceFile
	ClickHouseNativeURL string
	BatchSize           int
	Limit               int64
	Truncate            bool
}

func (o ImportOptions) FilePath(fileKey string) (string, bool) {
	for _, file := range o.Files {
		if file.FileKey == fileKey && file.Path != "" {
			return file.Path, true
		}
	}
	return "", false
}

type ImportRunRequest struct {
	Country             string
	Source              string
	RunDir              string
	Files               []SelectedSourceFile
	ClickHouseNativeURL string
	BatchSize           int
	Limit               int64
	Truncate            bool
}
```

- [ ] **Step 2: Pass selected files into imports**

Update `scheduler/internal/companysources/importer.go`:

```go
return source.Import(ctx, ImportOptions{
	RunDir:              req.RunDir,
	Files:               req.Files,
	ClickHouseNativeURL: req.ClickHouseNativeURL,
	BatchSize:           req.BatchSize,
	Limit:               req.Limit,
	Truncate:            req.Truncate,
})
```

- [ ] **Step 3: Update source imports to require selected source file**

In each source import, replace `filepath.Join(opts.RunDir, "source.ndjson")` or `source.json` with:

```go
snapshotPath, ok := opts.FilePath("source")
if !ok {
	return companysources.ImportResult{}, errors.New("selected source file is required")
}
```

Keep `RunDir` in `ImportResult` for now:

```go
return companysources.ImportResult{
	RunDir:         opts.RunDir,
	ImportedTables: NormalizedTableNames(),
	ImportedRows:   seen,
}, nil
```

- [ ] **Step 4: Update import tests**

Where a test currently creates `source.ndjson` under a temp run dir, pass:

```go
result, err := source.Import(context.Background(), companysources.ImportOptions{
	RunDir: t.TempDir(),
	Files: []companysources.SelectedSourceFile{
		{FileKey: "source", Path: snapshotPath, RelativePath: "source.ndjson"},
	},
	ClickHouseNativeURL: clickhouseURL,
	BatchSize:           1000,
})
```

- [ ] **Step 5: Run import tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/companysources/... -run 'Import|TestImport|Test.*Import' -count=1
```

Expected: PASS.

- [ ] **Step 6: Commit selected file import contract**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
git add scheduler/internal/companysources
git commit -m "feat: import company sources from selected files"
```

---

## Task 6: Add Temporal Workflow Contracts And Tests

**Files:**
- Modify: `scheduler/internal/temporal/workflow/companysources/workflow.go`
- Modify: `scheduler/internal/temporal/workflow/companysources/workflow_test.go`

- [ ] **Step 1: Write workflow tests for deterministic child file workflows**

Add to `workflow_test.go`:

```go
func TestDownloadSourceRunsPreparedFileWorkflows(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflowWithOptions(DownloadSource, workflow.RegisterOptions{Name: DownloadSourceWorkflowName})
	env.RegisterWorkflowWithOptions(DownloadSourceFile, workflow.RegisterOptions{Name: DownloadSourceFileWorkflowName})

	env.OnActivity(PrepareSourceDownloadActivityName, mock.Anything, SyncSourceDownloadInput{
		ActionRunID: "action-run-1",
		SourceName:  "finland_prhytj",
		Trigger:     "manual",
	}).Return(PrepareSourceDownloadResult{
		ActionRunID: "action-run-1",
		Files: []DownloadSourceFileInput{
			{
				FileRunID:         "file-run-1",
				SourceName:        "finland_prhytj",
				FileKey:           "source",
				Trigger:           "manual",
				ParentActionRunID: "action-run-1",
			},
		},
	}, nil)
	env.OnWorkflow(DownloadSourceFileWorkflowName, mock.Anything, DownloadSourceFileInput{
		FileRunID:         "file-run-1",
		SourceName:        "finland_prhytj",
		FileKey:           "source",
		Trigger:           "manual",
		ParentActionRunID: "action-run-1",
	}).Return(DownloadSourceFileResult{
		FileRunID:      "file-run-1",
		SourceName:     "finland_prhytj",
		FileKey:        "source",
		Path:           "/tmp/source.ndjson",
		RecordsWritten: 2,
	}, nil)
	env.OnActivity(FinishSourceDownloadActivityName, mock.Anything, FinishSourceDownloadInput{
		ActionRunID: "action-run-1",
		Status:      StatusSucceeded,
		Files: []DownloadedSourceFileSummary{
			{FileKey: "source", FileRunID: "file-run-1", Path: "/tmp/source.ndjson", RecordsWritten: 2},
		},
	}).Return(nil)

	env.ExecuteWorkflow(DownloadSourceWorkflowName, SyncSourceDownloadInput{
		ActionRunID: "action-run-1",
		SourceName:  "finland_prhytj",
		Trigger:     "manual",
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
}
```

- [ ] **Step 2: Run workflow test and verify failure**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/temporal/workflow/companysources -run TestDownloadSourceRunsPreparedFileWorkflows -count=1
```

Expected: FAIL because new workflow types do not exist.

- [ ] **Step 3: Add workflow constants and identity helpers**

Update `workflow.go` constants:

```go
const (
	SourceTaskQueue = "corpscout-company-sources"

	DownloadSourceWorkflowName           = "CompanySourceDownloadWorkflow"
	DownloadSourceFileWorkflowName       = "CompanySourceDownloadFileWorkflow"
	ImportSourceToClickHouseWorkflowName = "CompanySourceClickHouseImportWorkflow"
	SyncSourceToClickHouseWorkflowName   = "CompanySourceSyncClickHouseWorkflow"

	PrepareSourceDownloadActivityName    = "PrepareSourceDownloadActivity"
	FinishSourceDownloadActivityName     = "FinishSourceDownloadActivity"
	DownloadSourceFileActivityName       = "DownloadSourceFileActivity"
	ImportSourceToClickHouseActivityName = "ImportSourceToClickHouseActivity"

	ActionPullSource       = "pull_source"
	ActionImportClickHouse = "import_clickhouse"
	StatusSucceeded        = "succeeded"
	StatusFailed           = "failed"
)

func ActionRunWorkflowID(actionRunID string) string {
	return "company-source-action-run-" + actionRunID
}

func FileRunWorkflowID(fileRunID string) string {
	return "company-source-file-run-" + fileRunID
}
```

- [ ] **Step 4: Add workflow input/result types**

Add:

```go
type SyncSourceDownloadInput struct {
	ActionRunID string `json:"action_run_id"`
	SourceName  string `json:"source_name"`
	Trigger     string `json:"trigger"`
}

type DownloadSourceFileInput struct {
	FileRunID         string `json:"file_run_id"`
	SourceName        string `json:"source_name"`
	FileKey           string `json:"file_key"`
	Trigger           string `json:"trigger"`
	ParentActionRunID string `json:"parent_action_run_id,omitempty"`
}

type DownloadSourceFileResult struct {
	FileRunID          string `json:"file_run_id"`
	SourceName         string `json:"source_name"`
	FileKey            string `json:"file_key"`
	Path               string `json:"path"`
	ContentSHA256      string `json:"content_sha256"`
	ContentLengthBytes int64  `json:"content_length_bytes"`
	RecordsWritten     int64  `json:"records_written"`
}

type DownloadedSourceFileSummary struct {
	FileKey        string `json:"file_key"`
	FileRunID      string `json:"file_run_id"`
	Path           string `json:"path"`
	RecordsWritten int64 `json:"records_written"`
}

type PrepareSourceDownloadResult struct {
	ActionRunID string                    `json:"action_run_id"`
	Files       []DownloadSourceFileInput `json:"files"`
}

type FinishSourceDownloadInput struct {
	ActionRunID  string                        `json:"action_run_id"`
	Status       string                        `json:"status"`
	Files        []DownloadedSourceFileSummary `json:"files"`
	ErrorMessage string                        `json:"error_message"`
}

type DownloadSourceResult struct {
	ActionRunID string                        `json:"action_run_id"`
	Files       []DownloadedSourceFileSummary `json:"files"`
}
```

- [ ] **Step 5: Implement file workflow and full download workflow**

Add:

```go
func DownloadSourceFile(ctx workflow.Context, input DownloadSourceFileInput) (DownloadSourceFileResult, error) {
	ctx = withSourceActivityOptions(ctx, 60*time.Minute)
	var result DownloadSourceFileResult
	if err := workflow.ExecuteActivity(ctx, DownloadSourceFileActivityName, input).Get(ctx, &result); err != nil {
		return DownloadSourceFileResult{}, errors.Wrap(err, "download source file activity")
	}
	return result, nil
}

func DownloadSource(ctx workflow.Context, input SyncSourceDownloadInput) (DownloadSourceResult, error) {
	ctx = withSourceActivityOptions(ctx, 60*time.Minute)
	var prepared PrepareSourceDownloadResult
	if err := workflow.ExecuteActivity(ctx, PrepareSourceDownloadActivityName, input).Get(ctx, &prepared); err != nil {
		return DownloadSourceResult{}, errors.Wrap(err, "prepare source download")
	}

	summaries := make([]DownloadedSourceFileSummary, 0, len(prepared.Files))
	var runErr error
	for _, file := range prepared.Files {
		childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			WorkflowID: FileRunWorkflowID(file.FileRunID),
		})
		var downloaded DownloadSourceFileResult
		if err := workflow.ExecuteChildWorkflow(childCtx, DownloadSourceFileWorkflowName, file).Get(ctx, &downloaded); err != nil {
			runErr = errors.Wrapf(err, "download source file %s", file.FileKey)
			break
		}
		summaries = append(summaries, DownloadedSourceFileSummary{
			FileKey:        downloaded.FileKey,
			FileRunID:      downloaded.FileRunID,
			Path:           downloaded.Path,
			RecordsWritten: downloaded.RecordsWritten,
		})
	}

	finish := FinishSourceDownloadInput{
		ActionRunID: input.ActionRunID,
		Status:      StatusSucceeded,
		Files:       summaries,
	}
	if runErr != nil {
		finish.Status = StatusFailed
		finish.ErrorMessage = runErr.Error()
	}
	if err := workflow.ExecuteActivity(ctx, FinishSourceDownloadActivityName, finish).Get(ctx, nil); err != nil {
		if runErr != nil {
			return DownloadSourceResult{ActionRunID: input.ActionRunID, Files: summaries}, errors.WithSecondaryError(runErr, err)
		}
		return DownloadSourceResult{}, errors.Wrap(err, "finish source download")
	}
	if runErr != nil {
		return DownloadSourceResult{ActionRunID: input.ActionRunID, Files: summaries}, runErr
	}
	return DownloadSourceResult{ActionRunID: input.ActionRunID, Files: summaries}, nil
}
```

- [ ] **Step 6: Update import input with action run ID**

Update `ImportSourceToClickHouseInput`:

```go
type ImportSourceToClickHouseInput struct {
	ActionRunID         string   `json:"action_run_id"`
	SourceName          string   `json:"source_name"`
	Trigger             string   `json:"trigger"`
	DownloadActionRunID string   `json:"download_action_run_id,omitempty"`
	FileRunIDs          []string `json:"file_run_ids,omitempty"`
	BatchSize           int      `json:"batch_size"`
	Limit               int64    `json:"limit"`
}
```

- [ ] **Step 7: Run workflow package tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/temporal/workflow/companysources -count=1
```

Expected: PASS.

- [ ] **Step 8: Commit workflow contracts**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
git add scheduler/internal/temporal/workflow/companysources
git commit -m "feat: add source file Temporal workflows"
```

---

## Task 7: Implement Temporal Activities

**Files:**
- Modify: `scheduler/internal/temporal/actions/companysources/actions.go`
- Modify: `scheduler/internal/temporal/actions/companysources/actions_test.go`
- Modify: `scheduler/internal/app/temporal.go`

- [ ] **Step 1: Add activity result JSON test**

Update `actions_test.go`:

```go
func TestDownloadSourceResultJSONIncludesFileSummaries(t *testing.T) {
	result := marshalActionResult(sourceworkflow.DownloadSourceResult{
		ActionRunID: "action-run-1",
		Files: []sourceworkflow.DownloadedSourceFileSummary{
			{FileKey: "source", FileRunID: "file-run-1", Path: "/runs/source.ndjson", RecordsWritten: 2},
		},
	})

	var got map[string]any
	require.NoError(t, json.Unmarshal(result, &got))
	require.Equal(t, "action-run-1", got["action_run_id"])
	files := got["files"].([]any)
	first := files[0].(map[string]any)
	require.Equal(t, "source", first["file_key"])
	require.Equal(t, "file-run-1", first["file_run_id"])
}
```

- [ ] **Step 2: Implement run directory helper**

In `actions.go`, replace `runDirectoryName(country, source string)` with:

```go
func fileRunDirectoryName(fileKey string, fileRunID string) string {
	return time.Now().UTC().Format("20060102T150405Z") + "-" + fileKey + "-" + fileRunID
}
```

- [ ] **Step 3: Implement prepare activity**

Add:

```go
func (a *Actions) PrepareSourceDownloadActivity(ctx context.Context, input SyncSourceDownloadInput) (sourceworkflow.PrepareSourceDownloadResult, error) {
	if a == nil || a.pool == nil {
		return sourceworkflow.PrepareSourceDownloadResult{}, errors.New("company source database is not available")
	}
	queries := db.New(a.pool)
	actionRunID, err := uuid.Parse(input.ActionRunID)
	if err != nil {
		return sourceworkflow.PrepareSourceDownloadResult{}, errors.Wrap(err, "parse action run id")
	}
	actionRun, err := queries.GetSourceActionRun(ctx, actionRunID)
	if err != nil {
		return sourceworkflow.PrepareSourceDownloadResult{}, errors.Wrap(err, "load source action run")
	}
	if actionRun.Action != sourceworkflow.ActionPullSource {
		return sourceworkflow.PrepareSourceDownloadResult{}, errors.Errorf("action run %s has action %q", actionRun.ID, actionRun.Action)
	}

	files, err := queries.ListSourceFilesWithLatestRun(ctx, input.SourceName)
	if err != nil {
		return sourceworkflow.PrepareSourceDownloadResult{}, errors.Wrap(err, "list source files")
	}

	result := sourceworkflow.PrepareSourceDownloadResult{ActionRunID: input.ActionRunID}
	for _, file := range files {
		if !file.Enabled {
			continue
		}
		fileRunID := uuid.New()
		workflowID := sourceworkflow.FileRunWorkflowID(fileRunID.String())
		fileRun, err := queries.CreateSourceFileRun(ctx, db.CreateSourceFileRunParams{
			ID:                 fileRunID,
			SourceFileID:       file.ID,
			ParentActionRunID:  &actionRun.ID,
			TemporalWorkflowID: &workflowID,
		})
		if err != nil {
			return sourceworkflow.PrepareSourceDownloadResult{}, errors.Wrapf(err, "create file run %s", file.FileKey)
		}
		result.Files = append(result.Files, sourceworkflow.DownloadSourceFileInput{
			FileRunID:         fileRun.ID.String(),
			SourceName:        input.SourceName,
			FileKey:           file.FileKey,
			Trigger:           input.Trigger,
			ParentActionRunID: input.ActionRunID,
		})
	}
	return result, nil
}
```

- [ ] **Step 4: Implement file download activity**

Add:

```go
func (a *Actions) DownloadSourceFileActivity(ctx context.Context, input sourceworkflow.DownloadSourceFileInput) (sourceworkflow.DownloadSourceFileResult, error) {
	if a == nil || a.pool == nil {
		return sourceworkflow.DownloadSourceFileResult{}, errors.New("company source database is not available")
	}
	if a.sourceRunsRoot == "" {
		return sourceworkflow.DownloadSourceFileResult{}, errors.New("company source run root is required")
	}
	fileRunID, err := uuid.Parse(input.FileRunID)
	if err != nil {
		return sourceworkflow.DownloadSourceFileResult{}, errors.Wrap(err, "parse file run id")
	}

	queries := db.New(a.pool)
	fileRun, err := queries.GetSourceFileRunWithDefinition(ctx, fileRunID)
	if err != nil {
		return sourceworkflow.DownloadSourceFileResult{}, errors.Wrap(err, "load source file run")
	}
	if fileRun.SourceName != input.SourceName || fileRun.FileKey != input.FileKey {
		return sourceworkflow.DownloadSourceFileResult{}, errors.Errorf("file run %s does not match %s/%s", fileRunID, input.SourceName, input.FileKey)
	}

	runDir := filepath.Join(a.sourceRunsRoot, fileRun.Country, fileRun.Source, "files", fileRun.FileKey, fileRunDirectoryName(fileRun.FileKey, input.FileRunID))
	downloaded, err := sourcecore.DownloadFile(ctx, a.registry, sourcecore.DownloadFileRequest{
		Country:           fileRun.Country,
		Source:            fileRun.Source,
		FileKey:           fileRun.FileKey,
		FileKind:          fileRun.Kind,
		RunDir:            runDir,
		RelativePath:      fileRun.RelativePath,
		SourceURL:         fileRun.SourceUrl,
		UserAgentRequired: fileRun.UserAgentRequired,
		Config:            decodeObject(fileRun.Config),
	})
	if err != nil {
		finishErr := a.finishFileRunFailed(fileRun.ID, err)
		return sourceworkflow.DownloadSourceFileResult{FileRunID: input.FileRunID, SourceName: input.SourceName, FileKey: input.FileKey}, combineWithFinishError(errors.Wrap(err, "download source file"), finishErr)
	}

	if _, err := queries.FinishSourceFileRun(ctx, db.FinishSourceFileRunParams{
		Status:             sourceworkflow.StatusSucceeded,
		Path:               downloaded.Path,
		ContentSha256:      downloaded.ContentSHA256,
		ContentLengthBytes: &downloaded.ContentLengthBytes,
		RecordsWritten:    &downloaded.RecordsWritten,
		ErrorMessage:       "",
		Log:                marshalActionResult([]map[string]any{{"level": "info", "message": "file written", "path": downloaded.Path}}),
		ID:                 fileRun.ID,
	}); err != nil {
		return sourceworkflow.DownloadSourceFileResult{}, errors.Wrap(err, "finish source file run")
	}

	return sourceworkflow.DownloadSourceFileResult{
		FileRunID:          input.FileRunID,
		SourceName:         input.SourceName,
		FileKey:            input.FileKey,
		Path:               downloaded.Path,
		ContentSHA256:      downloaded.ContentSHA256,
		ContentLengthBytes: downloaded.ContentLengthBytes,
		RecordsWritten:     downloaded.RecordsWritten,
	}, nil
}
```

Add helper:

```go
func decodeObject(raw json.RawMessage) map[string]any {
	if len(raw) == 0 {
		return map[string]any{}
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return map[string]any{}
	}
	return out
}
```

- [ ] **Step 5: Implement finish download activity**

Add:

```go
func (a *Actions) FinishSourceDownloadActivity(ctx context.Context, input sourceworkflow.FinishSourceDownloadInput) error {
	actionRunID, err := uuid.Parse(input.ActionRunID)
	if err != nil {
		return errors.Wrap(err, "parse action run id")
	}
	queries := db.New(a.pool)
	_, err = queries.FinishSourceActionRun(ctx, db.FinishSourceActionRunParams{
		Status:       input.Status,
		Result:       marshalActionResult(map[string]any{"action_run_id": input.ActionRunID, "files": input.Files}),
		ErrorMessage: input.ErrorMessage,
		ID:           actionRunID,
	})
	return errors.Wrap(err, "finish source download action run")
}
```

- [ ] **Step 6: Update import activity to load existing action run and selected files**

At the start of `ImportSourceToClickHouseActivity`, parse `input.ActionRunID` and load that action run. Remove creation of a new action run inside the activity. Select files using this order:

```go
selected, err := a.selectedImportFiles(ctx, queries, input)
if err != nil {
	finishErr := a.finishActionRunFailed(actionRun.ID, err)
	return result, combineWithFinishError(errors.Wrap(err, "select import files"), finishErr)
}
```

Create helper:

```go
func (a *Actions) selectedImportFiles(ctx context.Context, queries *db.Queries, input ImportSourceToClickHouseInput) ([]sourcecore.SelectedSourceFile, error) {
	if len(input.FileRunIDs) > 0 {
		files := make([]sourcecore.SelectedSourceFile, 0, len(input.FileRunIDs))
		for _, id := range input.FileRunIDs {
			fileRunID, err := uuid.Parse(id)
			if err != nil {
				return nil, errors.Wrap(err, "parse file run id")
			}
			row, err := queries.GetSourceFileRunWithDefinition(ctx, fileRunID)
			if err != nil {
				return nil, errors.Wrap(err, "load source file run")
			}
			if row.Status != sourceworkflow.StatusSucceeded {
				return nil, errors.Errorf("source file run %s has status %q", row.ID, row.Status)
			}
			if strings.TrimSpace(row.Path) == "" {
				return nil, errors.Errorf("source file run %s has no path", row.ID)
			}
			if _, err := os.Stat(row.Path); err != nil {
				return nil, errors.Wrapf(err, "stat source file %s", row.Path)
			}
			files = append(files, sourcecore.SelectedSourceFile{FileKey: row.FileKey, Path: row.Path, RelativePath: row.RelativePath})
		}
		return files, nil
	}

	if input.DownloadActionRunID != "" {
		actionRunID, err := uuid.Parse(input.DownloadActionRunID)
		if err != nil {
			return nil, errors.Wrap(err, "parse download action run id")
		}
		rows, err := queries.ListSuccessfulSourceFileRunsForAction(ctx, actionRunID)
		if err != nil {
			return nil, errors.Wrap(err, "list successful source file runs for action")
		}
		return selectedSourceFilesFromActionRows(rows)
	}

	rows, err := queries.ListLatestSuccessfulRequiredSourceFileRuns(ctx, input.SourceName)
	if err != nil {
		return nil, errors.Wrap(err, "list latest successful required source file runs")
	}
	return selectedSourceFilesFromLatestRows(rows)
}
```

Use selected files when calling `sourcecore.ImportRun`.

- [ ] **Step 7: Register new workflow and activities**

Update `scheduler/internal/app/temporal.go`:

```go
worker.RegisterWorkflowWithOptions(
	companysourceworkflows.DownloadSourceFile,
	workflow.RegisterOptions{Name: companysourceworkflows.DownloadSourceFileWorkflowName},
)
worker.RegisterActivityWithOptions(
	resources.companySourceActions.PrepareSourceDownloadActivity,
	activity.RegisterOptions{Name: companysourceworkflows.PrepareSourceDownloadActivityName},
)
worker.RegisterActivityWithOptions(
	resources.companySourceActions.FinishSourceDownloadActivity,
	activity.RegisterOptions{Name: companysourceworkflows.FinishSourceDownloadActivityName},
)
worker.RegisterActivityWithOptions(
	resources.companySourceActions.DownloadSourceFileActivity,
	activity.RegisterOptions{Name: companysourceworkflows.DownloadSourceFileActivityName},
)
```

- [ ] **Step 8: Run Temporal action tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/temporal/actions/companysources ./internal/app -count=1
```

Expected: PASS.

- [ ] **Step 9: Commit Temporal activities**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
git add scheduler/internal/temporal/actions/companysources scheduler/internal/app/temporal.go scheduler/internal/app/temporal_test.go
git commit -m "feat: persist company source file Temporal runs"
```

---

## Task 8: Add HTTP File APIs And Deterministic Action Starts

**Files:**
- Modify: `scheduler/internal/httpapi/source_actions.go`
- Create: `scheduler/internal/httpapi/source_files.go`
- Modify: `scheduler/internal/httpapi/handlers.go`
- Modify: `scheduler/internal/httpapi/testhelpers_test.go`
- Add/modify tests

- [ ] **Step 1: Add route registrations**

In `handlers.go`, add:

```go
r.Get("/sources/{name}/files", h.handleListSourceFiles)
r.Get("/sources/{name}/files/{file_key}/runs", h.handleListSourceFileRuns)
r.Post("/sources/{name}/files/{file_key}/download", h.handleTriggerSourceFileDownload)
r.Get("/source-action-runs/{id}/temporal-status", h.handleGetSourceActionRunTemporalStatus)
r.Get("/source-file-runs/{id}/temporal-status", h.handleGetSourceFileRunTemporalStatus)
```

- [ ] **Step 2: Create file API handlers**

Create `scheduler/internal/httpapi/source_files.go`:

```go
package httpapi

import (
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"go.temporal.io/sdk/client"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
	companysourceworkflows "github.com/pulsarpoint/corpscout/scheduler/internal/temporal/workflow/companysources"
)

const defaultSourceFileRunsLimit = 20
const maxSourceFileRunsLimit = 100

func (h *Handlers) handleListSourceFiles(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	rows, err := h.db.ListSourceFilesWithLatestRun(r.Context(), name)
	if err != nil {
		slog.ErrorContext(r.Context(), "list source files", "source", name, "error", err)
		writeError(w, http.StatusInternalServerError, "list source files failed")
		return
	}
	items := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		missing := row.LatestSuccessfulRunID == nil || row.LatestSuccessfulPath == nil || !pathExists(*row.LatestSuccessfulPath)
		items = append(items, map[string]any{
			"id":                           row.ID,
			"source_id":                    row.SourceID,
			"source_name":                  row.SourceName,
			"file_key":                     row.FileKey,
			"display_name":                 row.DisplayName,
			"description":                  row.Description,
			"kind":                         row.Kind,
			"required":                     row.Required,
			"relative_path":                row.RelativePath,
			"enabled":                      row.Enabled,
			"sort_order":                   row.SortOrder,
			"latest_status":                row.LatestStatus,
			"missing":                      missing,
			"latest_run_id":                row.LatestRunID,
			"latest_started_at":            row.LatestStartedAt,
			"latest_finished_at":           row.LatestFinishedAt,
			"latest_path":                  row.LatestPath,
			"latest_content_sha256":        row.LatestContentSha256,
			"latest_content_length_bytes":  row.LatestContentLengthBytes,
			"latest_records_written":       row.LatestRecordsWritten,
			"latest_error_message":         row.LatestErrorMessage,
			"latest_successful_run_id":     row.LatestSuccessfulRunID,
			"latest_successful_path":       row.LatestSuccessfulPath,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func pathExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
```

- [ ] **Step 3: Add one-file run list and trigger handlers**

Add to `source_files.go`:

```go
func (h *Handlers) handleListSourceFileRuns(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	fileKey := chi.URLParam(r, "file_key")
	limit := parseBoundedLimit(r.URL.Query().Get("limit"), defaultSourceFileRunsLimit, maxSourceFileRunsLimit)
	rows, err := h.db.ListSourceFileRuns(r.Context(), db.ListSourceFileRunsParams{
		SourceName: name,
		FileKey:    fileKey,
		Limit:      int32(limit),
	})
	if err != nil {
		slog.ErrorContext(r.Context(), "list source file runs", "source", name, "file_key", fileKey, "error", err)
		writeError(w, http.StatusInternalServerError, "list source file runs failed")
		return
	}
	if rows == nil {
		rows = []db.ListSourceFileRunsRow{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": rows})
}

func (h *Handlers) handleTriggerSourceFileDownload(w http.ResponseWriter, r *http.Request) {
	if h.temporal == nil {
		writeError(w, http.StatusServiceUnavailable, "temporal client not available")
		return
	}
	sourceName := chi.URLParam(r, "name")
	fileKey := chi.URLParam(r, "file_key")
	req, err := decodeSourceActionTriggerRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	file, err := h.db.GetSourceFileBySourceNameAndKey(r.Context(), db.GetSourceFileBySourceNameAndKeyParams{
		Name: sourceName,
		FileKey: fileKey,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeError(w, http.StatusNotFound, "source file not found")
			return
		}
		slog.ErrorContext(r.Context(), "get source file", "source", sourceName, "file_key", fileKey, "error", err)
		writeError(w, http.StatusInternalServerError, "get source file failed")
		return
	}
	fileRunID := uuid.New()
	workflowID := companysourceworkflows.FileRunWorkflowID(fileRunID.String())
	fileRun, err := h.db.CreateSourceFileRun(r.Context(), db.CreateSourceFileRunParams{
		ID:                 fileRunID,
		SourceFileID:       file.ID,
		TemporalWorkflowID: &workflowID,
	})
	if err != nil {
		slog.ErrorContext(r.Context(), "create source file run", "source", sourceName, "file_key", fileKey, "error", err)
		writeError(w, http.StatusInternalServerError, "create source file run failed")
		return
	}
	run, err := h.temporal.ExecuteWorkflow(r.Context(), client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: companysourceworkflows.SourceTaskQueue,
	}, companysourceworkflows.DownloadSourceFileWorkflowName, companysourceworkflows.DownloadSourceFileInput{
		FileRunID:  fileRun.ID.String(),
		SourceName: sourceName,
		FileKey:    fileKey,
		Trigger:    req.Trigger,
	})
	if err != nil {
		_, _ = h.db.FinishSourceFileRun(r.Context(), db.FinishSourceFileRunParams{
			Status:             companysourceworkflows.StatusFailed,
			Path:               "",
			ContentSha256:      "",
			ContentLengthBytes: nil,
			RecordsWritten:    nil,
			ErrorMessage:       "failed to start workflow",
			Log:                []byte(`[]`),
			ID:                 fileRun.ID,
		})
		slog.ErrorContext(r.Context(), "start source file download workflow", "source", sourceName, "file_key", fileKey, "error", err)
		writeError(w, http.StatusInternalServerError, "failed to start workflow")
		return
	}
	_ = h.db.UpdateSourceFileRunTemporalRunID(r.Context(), db.UpdateSourceFileRunTemporalRunIDParams{
		ID:            fileRun.ID,
		TemporalRunID: optionalStringPointer(run.GetRunID()),
	})
	writeJSON(w, http.StatusAccepted, startWorkflowResponse{
		Status:        "started",
		Workflow:      companysourceworkflows.DownloadSourceFileWorkflowName,
		TaskQueue:     companysourceworkflows.SourceTaskQueue,
		WorkflowID:    workflowID,
		WorkflowRunID: run.GetRunID(),
	})
}
```

- [ ] **Step 4: Make source action trigger create action run before Temporal**

In `source_actions.go`, after loading the action and before `ExecuteWorkflow`:

```go
actionRunID := uuid.New()
workflowID := companysourceworkflows.ActionRunWorkflowID(actionRunID.String())
input, err := sourceActionWorkflowInput(action.Action, sourceName, actionRunID.String(), req)
if err != nil {
	writeError(w, http.StatusBadRequest, err.Error())
	return
}
actionRun, err := h.db.CreateSourceActionRun(r.Context(), db.CreateSourceActionRunParams{
	ID:                 actionRunID,
	TemporalWorkflowID: optionalStringPointer(workflowID),
	Input:              marshalJSON(input),
	ActionID:           action.ID,
})
if err != nil {
	slog.ErrorContext(r.Context(), "create source action run", "source", sourceName, "action", actionKey, "error", err)
	writeError(w, http.StatusInternalServerError, "create source action run failed")
	return
}
run, err := h.temporal.ExecuteWorkflow(r.Context(), client.StartWorkflowOptions{
	ID:        workflowID,
	TaskQueue: companysourceworkflows.SourceTaskQueue,
}, action.TemporalWorkflowType, input)
if err != nil {
	_, _ = h.db.FinishSourceActionRun(r.Context(), db.FinishSourceActionRunParams{
		Status:       companysourceworkflows.StatusFailed,
		Result:       []byte(`{}`),
		ErrorMessage: "failed to start workflow",
		ID:           actionRun.ID,
	})
	slog.ErrorContext(r.Context(), "start source action workflow", "source", sourceName, "action", actionKey, "error", err)
	writeError(w, http.StatusInternalServerError, "failed to start workflow")
	return
}
_ = h.db.UpdateSourceActionRunTemporalRunID(r.Context(), db.UpdateSourceActionRunTemporalRunIDParams{
	ID:            actionRun.ID,
	TemporalRunID: optionalStringPointer(run.GetRunID()),
})
```

Add helper:

```go
func sourceActionWorkflowInput(action string, sourceName string, actionRunID string, req sourceActionTriggerRequest) (any, error) {
	switch action {
	case companysourceworkflows.ActionPullSource:
		return companysourceworkflows.SyncSourceDownloadInput{
			ActionRunID: actionRunID,
			SourceName:  sourceName,
			Trigger:     req.Trigger,
		}, nil
	case companysourceworkflows.ActionImportClickHouse:
		if req.BatchSize <= 0 {
			req.BatchSize = 1000
		}
		return companysourceworkflows.ImportSourceToClickHouseInput{
			ActionRunID:         actionRunID,
			SourceName:          sourceName,
			Trigger:             req.Trigger,
			DownloadActionRunID: req.DownloadActionRunID,
			BatchSize:           req.BatchSize,
			Limit:               req.Limit,
		}, nil
	default:
		return nil, errors.New("unsupported source action")
	}
}
```

- [ ] **Step 5: Add Temporal status handlers**

Add to `source_actions.go`:

```go
func (h *Handlers) handleGetSourceActionRunTemporalStatus(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid action run id")
		return
	}
	run, err := h.db.GetSourceActionRun(r.Context(), id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeError(w, http.StatusNotFound, "source action run not found")
			return
		}
		slog.ErrorContext(r.Context(), "get source action run", "id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "get source action run failed")
		return
	}
	h.writeTemporalStatus(w, r, run.ID.String(), run.Status, run.TemporalWorkflowID, run.TemporalRunID, run.StartedAt, run.FinishedAt)
}
```

Implement `writeTemporalStatus` with `h.temporal.DescribeWorkflowExecution`.

- [ ] **Step 6: Run HTTP tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/httpapi -run 'TestListSourceFiles|TestTriggerSourceFileDownload|TestTriggerSourceActionStartsTemporalWorkflow|Test.*TemporalStatus' -count=1
```

Expected: PASS after adding matching test stubs.

- [ ] **Step 7: Commit HTTP API**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
git add scheduler/internal/httpapi
git commit -m "feat: expose company source file status APIs"
```

---

## Task 9: Add UI File Status Table

**Files:**
- Modify: `ui/app/types/api.ts`
- Modify: `ui/app/lib/api.ts`
- Modify: `ui/app/components/app/source-detail/ActionsTab.tsx`

- [ ] **Step 1: Add API types**

In `ui/app/types/api.ts`, add:

```ts
export interface SourceFile {
  id: string;
  source_id: string;
  source_name: string;
  file_key: string;
  display_name: string;
  description: string | null;
  kind: "source_snapshot" | "code_list" | "reference_data" | "archive";
  required: boolean;
  relative_path: string;
  enabled: boolean;
  sort_order: number;
  latest_status: "running" | "succeeded" | "failed" | "missing" | "skipped" | "cancelled" | null;
  missing: boolean;
  latest_run_id: string | null;
  latest_started_at: string | null;
  latest_finished_at: string | null;
  latest_path: string | null;
  latest_content_sha256: string | null;
  latest_content_length_bytes: number | null;
  latest_records_written: number | null;
  latest_error_message: string | null;
  latest_successful_run_id: string | null;
  latest_successful_path: string | null;
}

export interface SourceFileRun {
  id: string;
  source_id: string;
  source_file_id: string;
  parent_action_run_id: string | null;
  file_key: string;
  display_name: string;
  source_name: string;
  status: "running" | "succeeded" | "failed" | "missing" | "skipped" | "cancelled";
  temporal_workflow_id: string | null;
  temporal_run_id: string | null;
  started_at: string;
  finished_at: string | null;
  path: string | null;
  content_sha256: string | null;
  content_length_bytes: number | null;
  records_written: number | null;
  error_message: string | null;
  log: unknown[];
  created_at: string;
}

export interface SourceFileListResponse {
  items: SourceFile[];
}

export interface SourceFileRunListResponse {
  items: SourceFileRun[];
}
```

- [ ] **Step 2: Add API methods**

In `ui/app/lib/api.ts`, import the new response types and add:

```ts
getSourceFiles: (name: string) =>
  get<SourceFileListResponse>(`/sources/${name}/files`),

getSourceFileRuns: (name: string, fileKey: string, limit = 20) =>
  get<SourceFileRunListResponse>(
    `/sources/${name}/files/${fileKey}/runs?limit=${limit}`,
  ),

triggerSourceFileDownload: (
  name: string,
  fileKey: string,
  body: { trigger?: "manual" } = {},
) =>
  post<StartWorkflowResponse>(
    `/sources/${name}/files/${fileKey}/download`,
    body,
  ),
```

- [ ] **Step 3: Load file data in ActionsTab**

In `ActionsTab.tsx`, import `SourceFile` and update `loadSourceActionData`:

```ts
async function loadSourceActionData(sourceName: string) {
  const [loadedActions, loadedRuns, latestDownload, loadedFiles] = await Promise.all([
    api.getSourceActions(sourceName),
    api.getSourceActionRuns(sourceName),
    api.getLatestSuccessfulSourceDownload(sourceName).catch((err) => {
      if (err instanceof Error && "status" in err && err.status === 404) {
        return undefined;
      }
      throw err;
    }),
    api.getSourceFiles(sourceName),
  ]);
  return {
    actions: loadedActions.items,
    runs: loadedRuns.items,
    latestDownload,
    files: loadedFiles.items,
  };
}
```

Add state:

```ts
const [files, setFiles] = useState<SourceFile[]>([]);
const [triggeringFile, setTriggeringFile] = useState<string>();
```

- [ ] **Step 4: Add file status helpers**

Add:

```ts
function sourceFileStatus(file: SourceFile): string {
  if (file.missing) return "Missing";
  if (!file.latest_status) return "Not downloaded";
  return file.latest_status;
}

function formatBytes(value?: number | null): string {
  if (value == null) return "-";
  if (value < 1024) return `${value} B`;
  if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KB`;
  if (value < 1024 * 1024 * 1024) return `${(value / 1024 / 1024).toFixed(1)} MB`;
  return `${(value / 1024 / 1024 / 1024).toFixed(1)} GB`;
}
```

- [ ] **Step 5: Add per-file trigger**

Add:

```ts
function triggerFileDownload(file: SourceFile) {
  setTriggeringFile(file.file_key);
  api
    .triggerSourceFileDownload(source.name, file.file_key, { trigger: "manual" })
    .then(() => {
      toast.success("File workflow started.");
      return refresh();
    })
    .catch((err) => {
      toast.error(errorMessage(err, "Failed to start file workflow."));
    })
    .finally(() => setTriggeringFile(undefined));
}
```

- [ ] **Step 6: Render files table above run history**

Add a section before `Configured actions`:

```tsx
<section className="space-y-2">
  <h2 className="text-sm font-medium">Files</h2>
  <Table>
    <TableHeader>
      <TableRow>
        <TableHead>File</TableHead>
        <TableHead>Kind</TableHead>
        <TableHead>Required</TableHead>
        <TableHead>Status</TableHead>
        <TableHead>Last downloaded</TableHead>
        <TableHead>Size</TableHead>
        <TableHead>Rows</TableHead>
        <TableHead>Path</TableHead>
        <TableHead className="text-right">Actions</TableHead>
      </TableRow>
    </TableHeader>
    <TableBody>
      {files.map((file) => (
        <TableRow key={file.id}>
          <TableCell>
            <div className="font-medium">{file.display_name}</div>
            <div className="font-mono text-xs text-muted-foreground">{file.file_key}</div>
          </TableCell>
          <TableCell>{file.kind}</TableCell>
          <TableCell>{file.required ? "Yes" : "No"}</TableCell>
          <TableCell>
            <Badge variant="outline" className={file.missing ? "border-red-200 bg-red-100 text-red-800" : undefined}>
              {sourceFileStatus(file)}
            </Badge>
          </TableCell>
          <TableCell>{formattedDate(file.latest_finished_at)}</TableCell>
          <TableCell>{formatBytes(file.latest_content_length_bytes)}</TableCell>
          <TableCell>{file.latest_records_written ?? "-"}</TableCell>
          <TableCell className="max-w-[260px] truncate font-mono text-xs">{file.latest_path ?? "-"}</TableCell>
          <TableCell className="text-right">
            <Button
              size="sm"
              variant="outline"
              disabled={busy || loading || triggeringFile === file.file_key || !file.enabled}
              onClick={() => triggerFileDownload(file)}
            >
              <Download className="size-4" />
              Download
            </Button>
          </TableCell>
        </TableRow>
      ))}
      {files.length === 0 ? (
        <TableRow>
          <TableCell colSpan={9} className="h-16 text-muted-foreground">
            No files configured.
          </TableCell>
        </TableRow>
      ) : null}
    </TableBody>
  </Table>
</section>
```

- [ ] **Step 7: Run UI typecheck**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/ui
npm run typecheck
```

Expected: PASS.

- [ ] **Step 8: Commit UI changes**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
git add ui/app/types/api.ts ui/app/lib/api.ts ui/app/components/app/source-detail/ActionsTab.tsx
git commit -m "feat: show company source file status in UI"
```

---

## Task 10: End-To-End Verification

**Files:**
- No planned source changes unless verification exposes a defect.

- [ ] **Step 1: Run backend tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./...
```

Expected: PASS.

- [ ] **Step 2: Run UI build**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/ui
npm run build
```

Expected: PASS.

- [ ] **Step 3: Apply Postgres migrations**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
make migrate-up
```

Expected: migration `000115_source_file_status` applies successfully.

- [ ] **Step 4: Regenerate source catalog into database**

Restart scheduler:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
make rebuild
```

Expected: scheduler starts and source catalog sync inserts `data_source_files`.

- [ ] **Step 5: Verify source file API**

Run:

```bash
curl -s http://localhost:8094/api/v1/sources/finland_prhytj/files | jq '.items[].file_key'
```

Expected output includes:

```text
"source"
"codelist_REK_en"
"codelist_REK_KDI_en"
"codelist_VIRANOM_en"
"codelist_TLAJI_en"
"codelist_YRMU_en"
"codelist_STATUS3_en"
"codelist_KIELI_en"
```

- [ ] **Step 6: Trigger one file download**

Run:

```bash
curl -s -X POST http://localhost:8094/api/v1/sources/finland_prhytj/files/codelist_REK_en/download \
  -H 'Content-Type: application/json' \
  -d '{"trigger":"manual"}' | jq .
```

Expected:

```json
{
  "status": "started",
  "workflow": "CompanySourceDownloadFileWorkflow",
  "task_queue": "corpscout-company-sources"
}
```

- [ ] **Step 7: Verify deterministic Temporal status by file run**

From the file list response, capture `.items[] | select(.file_key=="codelist_REK_en") | .latest_run_id`, then run:

```bash
curl -s http://localhost:8094/api/v1/source-file-runs/<file-run-id>/temporal-status | jq .
```

Expected: response contains `workflow_id` starting with `company-source-file-run-`.

- [ ] **Step 8: Verify UI manually**

Open:

```text
http://localhost:8094/sources/finland_prhytj/actions
```

Expected:

- Files table is visible.
- Missing files show a missing badge.
- Latest downloaded file shows timestamp, path, size, and rows.
- Clicking a file Download button starts a workflow and refreshes status.

- [ ] **Step 9: Commit verification fixes**

If verification required fixes, commit only those fixes:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
git add <fixed-files>
git commit -m "fix: stabilize company source file status flow"
```

If no fixes were required, do not create an empty commit.

---

## Self-Review Checklist

- Spec coverage:
  - Source file definitions: Tasks 1-3
  - Per-file download runs: Tasks 1-2 and 7-8
  - Missing-file status: Tasks 8-9
  - Exact-file download trigger: Tasks 6-9
  - Deterministic Temporal IDs from run IDs: Tasks 6-8
  - Import selected file runs: Tasks 5 and 7
  - UI visibility and retry: Task 9
- Placeholder scan:
  - The plan contains no unresolved implementation markers.
- Type consistency:
  - Run ID fields are `ActionRunID` and `FileRunID`.
  - Workflow ID helpers are `ActionRunWorkflowID` and `FileRunWorkflowID`.
  - File identity is consistently `file_key`.
  - Durable run tables remain `data_source_action_runs` and `data_source_file_runs`.
