# Finland PRH XBRL Discovery Download Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the first production-shaped source action for discovering and downloading free PRH financial XBRL statement XML for explicit Finland registration-date windows.

**Architecture:** Register `finland_prh_xbrl` as a normal company source while adding source-specific Postgres ledger tables under `financial_xbrl`. The existing company-source Temporal download workflow remains the entrypoint, with extra date-window input and a concrete PRH XBRL download path that updates the ledger and writes `statements.ndjson` plus raw XML files.

**Tech Stack:** Go, Temporal, PostgreSQL migrations, sqlc, React/TypeScript, PRH Open Data XBRL API.

---

## File Structure

- `corpscout/database/migrations/000118_financial_xbrl_finland_prh_download_ledger.{up,down}.sql`
  - Creates the `financial_xbrl` schema, `finland_prh_xbrl_*` ledger tables, constraints, and indexes. Source/file/action metadata remains owned by source catalog JSON startup sync.
- `corpscout/database/queries/sources.sql`
  - Adds a source-catalog action upsert query so JSON specs can own `data_source_actions`.
- `corpscout/database/queries/financial_xbrl.sql`
  - sqlc queries for discovery-window and statement-artifact upsert/update/list operations.
- `corpscout/scheduler/internal/db/financial_xbrl_migration_test.go`
  - Migration-shape tests.
- `corpscout/scheduler/internal/companysources/sourcecatalog/spec.go`
  - Allows `source_manifest` file kind, `statements.ndjson` source file name, and source-catalog action definitions.
- `corpscout/scheduler/internal/companysources/sourcecatalog/sync.go`
  - Upserts source actions from catalog specs at startup.
- `corpscout/scheduler/internal/companysources/sourcecatalog/sources/finland_prh_xbrl.json`
  - Source catalog definition for startup sync.
- `corpscout/scheduler/internal/companysources/sourcecatalog/catalog_test.go`
  - Tests the new spec loads and file validation accepts `source_manifest`.
- `corpscout/scheduler/internal/companysources/sourcecatalog/sync_test.go`
  - Tests action upserts are produced from catalog sync.
- `corpscout/scheduler/internal/companysources/finland/prhxbrl/{types.go,source.go,client.go,download.go,download_test.go}`
  - Source key/stub import plus PRH API client, manifest generation, XML download, hashing, and Postgres-ledger orchestration.
- `corpscout/scheduler/internal/app/temporal.go`
  - Registers `prhxbrl.Source{}` with the company-source registry.
- `corpscout/scheduler/cmd/corpscout-source/main.go`
  - Registers `prhxbrl.Source{}` for `list-sources`.
- `corpscout/scheduler/internal/temporal/workflow/companysources/workflow.go`
  - Adds date-window fields to download workflow inputs and propagates them to file child workflows.
- `corpscout/scheduler/internal/temporal/workflow/companysources/workflow_test.go`
  - Tests input propagation.
- `corpscout/scheduler/internal/temporal/actions/companysources/actions.go`
  - Routes `finland_prh_xbrl` `statements_manifest` file downloads to the concrete PRH XBRL downloader.
- `corpscout/scheduler/internal/temporal/actions/companysources/actions_test.go`
  - Tests validation and result behavior for PRH XBRL file download.
- `corpscout/scheduler/internal/httpapi/source_actions.go`
  - Accepts `registered_date_start`, `registered_date_end`, `max_statements`, and `retry_failed` in source action trigger payload.
- `corpscout/scheduler/internal/httpapi/source_actions_test.go`
  - Tests payload decoding and workflow input shape.
- `corpscout/ui/app/types/api.ts`
  - Adds optional PRH XBRL action trigger fields.
- `corpscout/ui/app/lib/api.ts`
  - Passes typed trigger bodies through without losing new fields.
- `corpscout/ui/app/components/app/source-detail/ActionsTab.tsx`
  - Adds a source-specific PRH XBRL download form for date window, max statements, and retry failed.

---

### Task 1: Add Source Catalog And Registry Stub

**Files:**
- Modify: `corpscout/database/queries/sources.sql`
- Generated: `corpscout/scheduler/internal/db/gen/*.go`
- Modify: `corpscout/scheduler/internal/companysources/sourcecatalog/spec.go`
- Modify: `corpscout/scheduler/internal/companysources/sourcecatalog/sync.go`
- Modify: `corpscout/scheduler/internal/companysources/sourcecatalog/catalog_test.go`
- Modify: `corpscout/scheduler/internal/companysources/sourcecatalog/sync_test.go`
- Create: `corpscout/scheduler/internal/companysources/sourcecatalog/sources/finland_prh_xbrl.json`
- Create: `corpscout/scheduler/internal/companysources/finland/prhxbrl/types.go`
- Create: `corpscout/scheduler/internal/companysources/finland/prhxbrl/source.go`
- Create: `corpscout/scheduler/internal/companysources/finland/prhxbrl/import.go`
- Modify: `corpscout/scheduler/internal/app/temporal.go`
- Modify: `corpscout/scheduler/cmd/corpscout-source/main.go`

- [ ] **Step 1: Add failing source catalog tests**

Add these assertions to `corpscout/scheduler/internal/companysources/sourcecatalog/catalog_test.go`:

```go
func TestLoadEmbeddedSpecsIncludesFinlandPRHXBRL(t *testing.T) {
	specs, err := LoadEmbedded()
	require.NoError(t, err)

	byRegistryKey := make(map[string]Spec, len(specs))
	for _, spec := range specs {
		byRegistryKey[spec.RegistryKey] = spec
	}

	spec, ok := byRegistryKey["finland/prh_xbrl"]
	require.True(t, ok)
	require.Equal(t, "finland_prh_xbrl", spec.Name)
	require.Equal(t, "financial_statements", spec.SourceGroup)
	require.Equal(t, "statements.ndjson", spec.SourceFileName)
	require.Contains(t, spec.Capabilities, "source_download")
	require.Len(t, spec.Files, 1)
	require.Equal(t, "statements_manifest", spec.Files[0].FileKey)
	require.Equal(t, "source_manifest", spec.Files[0].Kind)
	require.Equal(t, "statements.ndjson", spec.Files[0].RelativePath)
	require.Len(t, spec.Actions, 1)
	require.Equal(t, "pull_source", spec.Actions[0].Action)
	require.Equal(t, "CompanySourceDownloadWorkflow", spec.Actions[0].TemporalWorkflowType)
	require.Equal(t, "corpscout-company-sources", spec.Actions[0].TemporalTaskQueue)
	require.False(t, spec.Actions[0].Enabled)
}

func TestSourceSpecAllowsStatementManifestFile(t *testing.T) {
	spec := Spec{
		Name:                  "finland_prh_xbrl",
		Country:               "finland",
		Source:                "prh_xbrl",
		RegistryKey:           "finland/prh_xbrl",
		DisplayName:           "Finland PRH financial XBRL",
		Description:           "Digital financial statement information from PRH Open Data XBRL API.",
		SourceGroup:           "financial_statements",
		InputTableName:        "financial_xbrl.finland_prh_xbrl_*",
		Enabled:               true,
		StorageKind:           "clickhouse",
		ClickHouseDatabase:    "corpscout_sources",
		ClickHouseTablePrefix: "fi_prh_xbrl",
		SourceURL:             "https://avoindata.prh.fi/opendata-xbrl-api/v3",
		DocsURL:               "https://avoindata.prh.fi/en",
		RawSourceRetention:    "filesystem_run_directory",
		SourceFileName:        "statements.ndjson",
		Files: []FileSpec{{
			FileKey:      "statements_manifest",
			DisplayName:  "PRH XBRL statements manifest",
			Kind:         "source_manifest",
			Required:     true,
			RelativePath: "statements.ndjson",
			Enabled:      true,
			SortOrder:    10,
		}},
		Actions: []ActionSpec{{
			Action:               "pull_source",
			DisplayName:          "Download statements",
			TemporalWorkflowType: "CompanySourceDownloadWorkflow",
			TemporalTaskQueue:    "corpscout-company-sources",
			Enabled:              false,
		}},
	}

	require.NoError(t, spec.Validate())
}
```

- [ ] **Step 2: Run the failing tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/companysources/sourcecatalog -run 'TestLoadEmbeddedSpecsIncludesFinlandPRHXBRL|TestSourceSpecAllowsStatementManifestFile' -count=1
```

Expected: FAIL because `finland/prh_xbrl`, `statements.ndjson`, `source_manifest`, and catalog actions are not supported yet.

- [ ] **Step 3: Extend catalog validation**

In `corpscout/scheduler/internal/companysources/sourcecatalog/spec.go`, add this field to `Spec` after `Files`:

```go
Actions []ActionSpec `json:"actions"`
```

Create `ActionSpec` after `FileSpec`:

```go
type ActionSpec struct {
	Action               string         `json:"action"`
	DisplayName          string         `json:"display_name"`
	TemporalWorkflowType string         `json:"temporal_workflow_type"`
	TemporalTaskQueue    string         `json:"temporal_task_queue"`
	Enabled              bool           `json:"enabled"`
	Config               map[string]any `json:"config"`
}
```

Change the source file name validation:

```go
switch s.SourceFileName {
case "source.ndjson", "source.json", "statements.ndjson":
default:
	return errors.Errorf("source spec source file name %q is not supported", s.SourceFileName)
}
```

In `FileSpec.Validate`, add `source_manifest`:

```go
switch f.Kind {
case "source_snapshot", "source_manifest", "code_list", "reference_data", "archive":
	return validateRelativePath(f.RelativePath)
default:
	return errors.Errorf("source file spec kind %q is not supported", f.Kind)
}
```

Change the end of `Spec.Validate` so it validates files and optional actions:

```go
if err := s.validateFiles(); err != nil {
	return err
}
return s.validateActions()
```

Add action validation:

```go
func (s Spec) validateActions() error {
	seen := make(map[string]struct{}, len(s.Actions))
	for _, action := range s.Actions {
		if err := action.Validate(); err != nil {
			return errors.Wrapf(err, "validate source action %s/%s", s.RegistryKey, action.Action)
		}
		actionName := strings.TrimSpace(action.Action)
		if _, ok := seen[actionName]; ok {
			return errors.Errorf("source spec %s has duplicate action %q", s.RegistryKey, actionName)
		}
		seen[actionName] = struct{}{}
	}
	return nil
}

func (a ActionSpec) Validate() error {
	required := map[string]string{
		"action":                 a.Action,
		"display_name":           a.DisplayName,
		"temporal_workflow_type": a.TemporalWorkflowType,
		"temporal_task_queue":    a.TemporalTaskQueue,
	}
	for field, value := range required {
		if strings.TrimSpace(value) == "" {
			return errors.Errorf("source action spec %s is required", field)
		}
	}
	switch a.Action {
	case "pull_source", "import_clickhouse", "refresh_explorer_cache", "map_industries_to_nace":
		return nil
	default:
		return errors.Errorf("source action spec action %q is not supported", a.Action)
	}
}
```

- [ ] **Step 4: Add source catalog JSON**

Create `corpscout/scheduler/internal/companysources/sourcecatalog/sources/finland_prh_xbrl.json`:

```json
{
  "name": "finland_prh_xbrl",
  "country": "finland",
  "source": "prh_xbrl",
  "registry_key": "finland/prh_xbrl",
  "display_name": "Finland PRH financial XBRL",
  "description": "Digital financial statement information from PRH Open Data XBRL API.",
  "source_group": "financial_statements",
  "input_table_name": "financial_xbrl.finland_prh_xbrl_*",
  "enabled": true,
  "auth_required": false,
  "storage_kind": "clickhouse",
  "clickhouse_database": "corpscout_sources",
  "clickhouse_table_prefix": "fi_prh_xbrl",
  "source_url": "https://avoindata.prh.fi/opendata-xbrl-api/v3",
  "docs_url": "https://avoindata.prh.fi/en",
  "raw_source_retention": "filesystem_run_directory",
  "source_file_name": "statements.ndjson",
  "user_agent_required": false,
  "capabilities": ["source_download", "financial_statements"],
  "requires_translation": false,
  "files": [
    {
      "file_key": "statements_manifest",
      "display_name": "PRH XBRL statements manifest",
      "description": "NDJSON manifest generated from PRH XBRL financial statement discovery and raw XML download results.",
      "kind": "source_manifest",
      "required": true,
      "relative_path": "statements.ndjson",
      "enabled": true,
      "sort_order": 10
    }
  ],
  "actions": [
    {
      "action": "pull_source",
      "display_name": "Download statements",
      "temporal_workflow_type": "CompanySourceDownloadWorkflow",
      "temporal_task_queue": "corpscout-company-sources",
      "enabled": false,
      "config": {}
    }
  ]
}
```

- [ ] **Step 5: Add source-catalog action sync**

Append this query to `corpscout/database/queries/sources.sql`:

```sql
-- name: UpsertDataSourceActionFromCatalog :exec
INSERT INTO data_source_actions (
  source_id,
  action,
  display_name,
  temporal_workflow_type,
  temporal_task_queue,
  enabled,
  config
)
SELECT
  s.id,
  sqlc.arg(action),
  sqlc.arg(display_name),
  sqlc.arg(temporal_workflow_type),
  sqlc.arg(temporal_task_queue),
  sqlc.arg(enabled),
  sqlc.arg(config)
FROM data_sources s
WHERE s.registry_key = sqlc.arg(registry_key)
ON CONFLICT (source_id, action) DO UPDATE SET
  display_name = EXCLUDED.display_name,
  temporal_workflow_type = EXCLUDED.temporal_workflow_type,
  temporal_task_queue = EXCLUDED.temporal_task_queue,
  enabled = EXCLUDED.enabled,
  config = EXCLUDED.config,
  updated_at = now();
```

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
make sqlc-generate
```

In `corpscout/scheduler/internal/companysources/sourcecatalog/sync.go`, add the action upsert method to `Store`:

```go
UpsertDataSourceActionFromCatalog(ctx context.Context, arg db.UpsertDataSourceActionFromCatalogParams) error
```

After the file sync loop for each spec, upsert actions:

```go
for _, action := range spec.Actions {
	config := action.Config
	if config == nil {
		config = map[string]any{}
	}
	configData, err := json.Marshal(config)
	if err != nil {
		return errors.Wrapf(err, "marshal source action config %s/%s", spec.RegistryKey, action.Action)
	}
	if err := store.UpsertDataSourceActionFromCatalog(ctx, db.UpsertDataSourceActionFromCatalogParams{
		Action:               action.Action,
		DisplayName:          action.DisplayName,
		TemporalWorkflowType: action.TemporalWorkflowType,
		TemporalTaskQueue:    action.TemporalTaskQueue,
		Enabled:              action.Enabled,
		Config:               configData,
		RegistryKey:          spec.RegistryKey,
	}); err != nil {
		return errors.Wrapf(err, "upsert source action catalog spec %s/%s", spec.RegistryKey, action.Action)
	}
}
```

In `corpscout/scheduler/internal/companysources/sourcecatalog/sync_test.go`, add `actionUpserts` to `fakeStore` and implement the new method:

```go
actionUpserts []db.UpsertDataSourceActionFromCatalogParams
```

```go
func (s *fakeStore) UpsertDataSourceActionFromCatalog(ctx context.Context, arg db.UpsertDataSourceActionFromCatalogParams) error {
	s.actionUpserts = append(s.actionUpserts, arg)
	return nil
}
```

Update `TestSyncUpsertsAndPrunesCatalogSources`:

```go
require.Len(t, store.upserts, 5)
require.NotEmpty(t, store.actionUpserts)
require.Contains(t, store.disabled, "finland/prh_xbrl")
require.Contains(t, store.disabled["finland/prh_xbrl"], "statements_manifest")
require.ElementsMatch(t, []string{
	"finland/prhytj",
	"finland/prh_xbrl",
	"united_states/coloradoentities",
	"united_states/irseobmf",
	"united_states/secedgar",
}, store.pruned)
var prhXBRLAction db.UpsertDataSourceActionFromCatalogParams
var foundPRHXBRLAction bool
for _, action := range store.actionUpserts {
	if action.RegistryKey == "finland/prh_xbrl" {
		prhXBRLAction = action
		foundPRHXBRLAction = true
	}
}
require.True(t, foundPRHXBRLAction)
require.Equal(t, "pull_source", prhXBRLAction.Action)
require.Equal(t, "CompanySourceDownloadWorkflow", prhXBRLAction.TemporalWorkflowType)
require.False(t, prhXBRLAction.Enabled)
```

- [ ] **Step 6: Add the source package stub**

Create `corpscout/scheduler/internal/companysources/finland/prhxbrl/types.go`:

```go
package prhxbrl

const (
	SourceKey  = "prh_xbrl"
	SourceName = "finland_prh_xbrl"
	DefaultURL = "https://avoindata.prh.fi/opendata-xbrl-api/v3"
)
```

Create `corpscout/scheduler/internal/companysources/finland/prhxbrl/source.go`:

```go
package prhxbrl

import "github.com/pulsarpoint/corpscout/scheduler/internal/companysources"

type Source struct{}

func (Source) Key() companysources.Key {
	return companysources.Key{Country: "finland", Source: SourceKey}
}

func (Source) DisplayName() string {
	return "Finland PRH financial XBRL"
}
```

Create `corpscout/scheduler/internal/companysources/finland/prhxbrl/import.go`:

```go
package prhxbrl

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/pulsarpoint/corpscout/scheduler/internal/companysources"
)

func (Source) Import(ctx context.Context, opts companysources.ImportOptions) (companysources.ImportResult, error) {
	_ = ctx
	_ = opts
	return companysources.ImportResult{}, errors.New("Finland PRH financial XBRL ClickHouse import is not implemented")
}
```

Create `corpscout/scheduler/internal/companysources/finland/prhxbrl/download.go` with the generic source API method returning the source-specific Temporal action message. The package-level downloader is added in Task 6 and called directly from the Temporal activity.

```go
package prhxbrl

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/pulsarpoint/corpscout/scheduler/internal/companysources"
)

func (Source) DownloadFile(ctx context.Context, opts companysources.DownloadFileOptions) (companysources.DownloadedFile, error) {
	_ = ctx
	_ = opts
	return companysources.DownloadedFile{}, errors.New("Finland PRH financial XBRL download requires the source-specific Temporal action")
}
```

- [ ] **Step 7: Register the source in app wiring**

In `corpscout/scheduler/internal/app/temporal.go`, import:

```go
	"github.com/pulsarpoint/corpscout/scheduler/internal/companysources/finland/prhxbrl"
```

Add `prhxbrl.Source{}` to the registry:

```go
sourceRegistry := companysources.NewRegistry(
	prhytj.Source{},
	prhxbrl.Source{},
	coloradoentities.Source{},
	irseobmf.Source{},
	secedgar.Source{},
)
```

In `corpscout/scheduler/cmd/corpscout-source/main.go`, import `prhxbrl` and add it to `defaultRegistry()` the same way.

- [ ] **Step 8: Run tests and source list**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/companysources/sourcecatalog ./cmd/corpscout-source -count=1
GOWORK=off go run ./cmd/corpscout-source list-sources
```

Expected: tests pass and `list-sources` includes `finland/prh_xbrl`.

- [ ] **Step 9: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/scheduler/internal/companysources/sourcecatalog/spec.go \
  corpscout/scheduler/internal/companysources/sourcecatalog/sync.go \
  corpscout/scheduler/internal/companysources/sourcecatalog/catalog_test.go \
  corpscout/scheduler/internal/companysources/sourcecatalog/sync_test.go \
  corpscout/scheduler/internal/companysources/sourcecatalog/sources/finland_prh_xbrl.json \
  corpscout/database/queries/sources.sql \
  corpscout/scheduler/internal/db/gen \
  corpscout/scheduler/internal/companysources/finland/prhxbrl \
  corpscout/scheduler/internal/app/temporal.go \
  corpscout/scheduler/cmd/corpscout-source/main.go
git commit -m "feat: register finland prh xbrl source"
```

---

### Task 2: Add Postgres Ledger Migration

**Files:**
- Create: `corpscout/database/migrations/000118_financial_xbrl_finland_prh_download_ledger.up.sql`
- Create: `corpscout/database/migrations/000118_financial_xbrl_finland_prh_download_ledger.down.sql`
- Create: `corpscout/scheduler/internal/db/financial_xbrl_migration_test.go`

- [ ] **Step 1: Write migration-shape tests**

Create `corpscout/scheduler/internal/db/financial_xbrl_migration_test.go`:

```go
package db_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFinancialXBRLFinlandPRHDownloadLedgerMigrationShape(t *testing.T) {
	up, err := os.ReadFile("../../../database/migrations/000118_financial_xbrl_finland_prh_download_ledger.up.sql")
	require.NoError(t, err)
	down, err := os.ReadFile("../../../database/migrations/000118_financial_xbrl_finland_prh_download_ledger.down.sql")
	require.NoError(t, err)

	sql := string(up)
	for _, needle := range []string{
		"CREATE SCHEMA IF NOT EXISTS financial_xbrl",
		"CREATE TABLE financial_xbrl.finland_prh_xbrl_discovery_windows",
		"CREATE TABLE financial_xbrl.finland_prh_xbrl_statement_artifacts",
		"UNIQUE (source_id, registered_date_start, registered_date_end)",
		"UNIQUE (source_id, business_id, financial_date)",
		"download_status IN ('pending', 'downloading', 'succeeded', 'failed')",
		"'financial_statements'",
		"'source_manifest'",
	} {
		require.Contains(t, sql, needle)
	}
	require.NotContains(t, sql, "INSERT INTO data_sources")
	require.NotContains(t, sql, "INSERT INTO data_source_files")
	require.NotContains(t, sql, "INSERT INTO data_source_actions")
	require.Contains(t, sql, "source_group IN (")
	require.Contains(t, sql, "source_file_name IS NULL OR source_file_name IN ('source.ndjson', 'source.json', 'statements.ndjson')")
	require.Contains(t, sql, "kind IN ('source_snapshot', 'source_manifest', 'code_list', 'reference_data', 'archive')")

	downSQL := string(down)
	require.Contains(t, downSQL, "DROP TABLE IF EXISTS financial_xbrl.finland_prh_xbrl_statement_artifacts")
	require.Contains(t, downSQL, "DROP TABLE IF EXISTS financial_xbrl.finland_prh_xbrl_discovery_windows")
	require.Contains(t, downSQL, "DELETE FROM data_source_actions")
	require.Contains(t, downSQL, "DELETE FROM data_source_files")
	require.Contains(t, downSQL, "DELETE FROM data_sources")
	require.Contains(t, downSQL, "DROP SCHEMA IF EXISTS financial_xbrl")
	require.Contains(t, downSQL, "DROP CONSTRAINT IF EXISTS chk_data_sources_source_group")
	require.Contains(t, downSQL, "DROP CONSTRAINT IF EXISTS chk_data_source_files_kind")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/db -run TestFinancialXBRLFinlandPRHDownloadLedgerMigrationShape -count=1
```

Expected: FAIL because migration files do not exist.

- [ ] **Step 3: Create migration up file**

Create `corpscout/database/migrations/000118_financial_xbrl_finland_prh_download_ledger.up.sql`:

```sql
ALTER TABLE data_sources
  DROP CONSTRAINT IF EXISTS chk_data_sources_source_group;

ALTER TABLE data_sources
  ADD CONSTRAINT chk_data_sources_source_group CHECK (
    source_group IN (
      'security_identifier', 'registry', 'domain', 'website',
      'github', 'ai_research', 'manual', 'other', 'financial_statements'
    )
  );

ALTER TABLE data_sources
  DROP CONSTRAINT IF EXISTS chk_data_sources_source_file_name;

ALTER TABLE data_sources
  ADD CONSTRAINT chk_data_sources_source_file_name CHECK (
    source_file_name IS NULL OR source_file_name IN ('source.ndjson', 'source.json', 'statements.ndjson')
  );

ALTER TABLE data_source_files
  DROP CONSTRAINT IF EXISTS chk_data_source_files_kind;

ALTER TABLE data_source_files
  ADD CONSTRAINT chk_data_source_files_kind CHECK (
    kind IN ('source_snapshot', 'source_manifest', 'code_list', 'reference_data', 'archive')
  );

CREATE SCHEMA IF NOT EXISTS financial_xbrl;

CREATE TABLE financial_xbrl.finland_prh_xbrl_discovery_windows (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source_id UUID NOT NULL REFERENCES data_sources(id) ON DELETE CASCADE,
  registered_date_start DATE NOT NULL,
  registered_date_end DATE NOT NULL,
  action_run_id UUID REFERENCES data_source_action_runs(id) ON DELETE SET NULL,
  temporal_workflow_id TEXT,
  temporal_run_id TEXT,
  total_results BIGINT NOT NULL DEFAULT 0,
  pages_discovered INTEGER NOT NULL DEFAULT 0,
  statements_discovered BIGINT NOT NULL DEFAULT 0,
  last_completed_page INTEGER NOT NULL DEFAULT 0,
  completed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (source_id, registered_date_start, registered_date_end),
  CONSTRAINT chk_finland_prh_xbrl_window_dates CHECK (registered_date_start <= registered_date_end),
  CONSTRAINT chk_finland_prh_xbrl_window_counts CHECK (
    total_results >= 0 AND pages_discovered >= 0 AND statements_discovered >= 0 AND last_completed_page >= 0
  )
);

CREATE TABLE financial_xbrl.finland_prh_xbrl_statement_artifacts (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source_id UUID NOT NULL REFERENCES data_sources(id) ON DELETE CASCADE,
  business_id TEXT NOT NULL,
  financial_date DATE NOT NULL,
  registration_date DATE,
  source_url TEXT NOT NULL,
  xml_path TEXT,
  xml_sha256 TEXT,
  xml_size_bytes BIGINT,
  download_status TEXT NOT NULL DEFAULT 'pending',
  attempts INTEGER NOT NULL DEFAULT 0,
  last_attempt_at TIMESTAMPTZ,
  downloaded_at TIMESTAMPTZ,
  last_error_message TEXT,
  first_discovered_run_id UUID REFERENCES data_source_action_runs(id) ON DELETE SET NULL,
  latest_action_run_id UUID REFERENCES data_source_action_runs(id) ON DELETE SET NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (source_id, business_id, financial_date),
  CONSTRAINT chk_finland_prh_xbrl_business_id CHECK (btrim(business_id) <> ''),
  CONSTRAINT chk_finland_prh_xbrl_source_url CHECK (btrim(source_url) <> ''),
  CONSTRAINT chk_finland_prh_xbrl_download_status CHECK (
    download_status IN ('pending', 'downloading', 'succeeded', 'failed')
  ),
  CONSTRAINT chk_finland_prh_xbrl_xml_size CHECK (xml_size_bytes IS NULL OR xml_size_bytes >= 0),
  CONSTRAINT chk_finland_prh_xbrl_attempts CHECK (attempts >= 0)
);

CREATE INDEX idx_finland_prh_xbrl_windows_source_dates
  ON financial_xbrl.finland_prh_xbrl_discovery_windows (source_id, registered_date_start, registered_date_end);

CREATE INDEX idx_finland_prh_xbrl_statement_artifacts_source_status
  ON financial_xbrl.finland_prh_xbrl_statement_artifacts (source_id, download_status, registration_date);

CREATE INDEX idx_finland_prh_xbrl_statement_artifacts_business_date
  ON financial_xbrl.finland_prh_xbrl_statement_artifacts (business_id, financial_date DESC);
```

- [ ] **Step 4: Create migration down file**

Create `corpscout/database/migrations/000118_financial_xbrl_finland_prh_download_ledger.down.sql`:

```sql
DROP TABLE IF EXISTS financial_xbrl.finland_prh_xbrl_statement_artifacts;
DROP TABLE IF EXISTS financial_xbrl.finland_prh_xbrl_discovery_windows;
DROP SCHEMA IF EXISTS financial_xbrl;

DELETE FROM data_source_action_runs
WHERE source_id IN (
  SELECT id FROM data_sources WHERE registry_key = 'finland/prh_xbrl'
);

DELETE FROM data_source_actions
WHERE source_id IN (
  SELECT id FROM data_sources WHERE registry_key = 'finland/prh_xbrl'
);

DELETE FROM data_source_file_runs
WHERE source_id IN (
  SELECT id FROM data_sources WHERE registry_key = 'finland/prh_xbrl'
);

DELETE FROM data_source_files
WHERE source_id IN (
  SELECT id FROM data_sources WHERE registry_key = 'finland/prh_xbrl'
);

DELETE FROM data_sources
WHERE registry_key = 'finland/prh_xbrl';

ALTER TABLE data_sources
  DROP CONSTRAINT IF EXISTS chk_data_sources_source_group;

ALTER TABLE data_sources
  ADD CONSTRAINT chk_data_sources_source_group CHECK (
    source_group IN (
      'security_identifier', 'registry', 'domain', 'website',
      'github', 'ai_research', 'manual', 'other'
    )
  );

ALTER TABLE data_sources
  DROP CONSTRAINT IF EXISTS chk_data_sources_source_file_name;

ALTER TABLE data_sources
  ADD CONSTRAINT chk_data_sources_source_file_name CHECK (
    source_file_name IS NULL OR source_file_name IN ('source.ndjson', 'source.json')
  );

ALTER TABLE data_source_files
  DROP CONSTRAINT IF EXISTS chk_data_source_files_kind;

ALTER TABLE data_source_files
  ADD CONSTRAINT chk_data_source_files_kind CHECK (
    kind IN ('source_snapshot', 'code_list', 'reference_data', 'archive')
  );
```

- [ ] **Step 5: Run migration-shape test**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/db -run TestFinancialXBRLFinlandPRHDownloadLedgerMigrationShape -count=1
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/database/migrations/000118_financial_xbrl_finland_prh_download_ledger.up.sql \
  corpscout/database/migrations/000118_financial_xbrl_finland_prh_download_ledger.down.sql \
  corpscout/scheduler/internal/db/financial_xbrl_migration_test.go
git commit -m "feat: add finland prh xbrl download ledger"
```

---

### Task 3: Add sqlc Ledger Queries

**Files:**
- Create: `corpscout/database/queries/financial_xbrl.sql`
- Generated: `corpscout/scheduler/internal/db/gen/*.go`
- Create: `corpscout/scheduler/internal/db/financial_xbrl_queries_test.go`

- [ ] **Step 1: Write query shape tests**

Create `corpscout/scheduler/internal/db/financial_xbrl_queries_test.go`:

```go
package db_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFinancialXBRLQueriesShape(t *testing.T) {
	sqlBytes, err := os.ReadFile("../../../database/queries/financial_xbrl.sql")
	require.NoError(t, err)
	sql := string(sqlBytes)

	for _, needle := range []string{
		"-- name: UpsertFinlandPRHXBRLDiscoveryWindow :one",
		"-- name: UpdateFinlandPRHXBRLDiscoveryProgress :one",
		"-- name: CompleteFinlandPRHXBRLDiscoveryWindow :one",
		"-- name: UpsertFinlandPRHXBRLStatementArtifact :one",
		"-- name: ListFinlandPRHXBRLStatementArtifactsToDownload :many",
		"-- name: MarkFinlandPRHXBRLStatementArtifactDownloading :one",
		"-- name: MarkFinlandPRHXBRLStatementArtifactSucceeded :one",
		"-- name: MarkFinlandPRHXBRLStatementArtifactFailed :one",
		"financial_xbrl.finland_prh_xbrl_discovery_windows",
		"financial_xbrl.finland_prh_xbrl_statement_artifacts",
		"ON CONFLICT (source_id, registered_date_start, registered_date_end) DO UPDATE",
		"ON CONFLICT (source_id, business_id, financial_date) DO UPDATE",
	} {
		require.Contains(t, sql, needle)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/db -run TestFinancialXBRLQueriesShape -count=1
```

Expected: FAIL because `financial_xbrl.sql` does not exist.

- [ ] **Step 3: Create sqlc query file**

Create `corpscout/database/queries/financial_xbrl.sql`:

```sql
-- name: UpsertFinlandPRHXBRLDiscoveryWindow :one
INSERT INTO financial_xbrl.finland_prh_xbrl_discovery_windows (
  source_id,
  registered_date_start,
  registered_date_end,
  action_run_id,
  temporal_workflow_id,
  temporal_run_id
) VALUES (
  sqlc.arg(source_id),
  sqlc.arg(registered_date_start),
  sqlc.arg(registered_date_end),
  sqlc.narg(action_run_id),
  sqlc.narg(temporal_workflow_id),
  sqlc.narg(temporal_run_id)
)
ON CONFLICT (source_id, registered_date_start, registered_date_end) DO UPDATE SET
  action_run_id = EXCLUDED.action_run_id,
  temporal_workflow_id = EXCLUDED.temporal_workflow_id,
  temporal_run_id = EXCLUDED.temporal_run_id,
  updated_at = now()
RETURNING *;

-- name: UpdateFinlandPRHXBRLDiscoveryProgress :one
UPDATE financial_xbrl.finland_prh_xbrl_discovery_windows
SET
  total_results = sqlc.arg(total_results),
  pages_discovered = sqlc.arg(pages_discovered),
  statements_discovered = sqlc.arg(statements_discovered),
  last_completed_page = sqlc.arg(last_completed_page),
  updated_at = now()
WHERE id = sqlc.arg(id)
RETURNING *;

-- name: CompleteFinlandPRHXBRLDiscoveryWindow :one
UPDATE financial_xbrl.finland_prh_xbrl_discovery_windows
SET
  total_results = sqlc.arg(total_results),
  pages_discovered = sqlc.arg(pages_discovered),
  statements_discovered = sqlc.arg(statements_discovered),
  last_completed_page = sqlc.arg(last_completed_page),
  completed_at = now(),
  updated_at = now()
WHERE id = sqlc.arg(id)
RETURNING *;

-- name: UpsertFinlandPRHXBRLStatementArtifact :one
INSERT INTO financial_xbrl.finland_prh_xbrl_statement_artifacts (
  source_id,
  business_id,
  financial_date,
  registration_date,
  source_url,
  first_discovered_run_id,
  latest_action_run_id
) VALUES (
  sqlc.arg(source_id),
  sqlc.arg(business_id),
  sqlc.arg(financial_date),
  sqlc.narg(registration_date),
  sqlc.arg(source_url),
  sqlc.narg(first_discovered_run_id),
  sqlc.narg(latest_action_run_id)
)
ON CONFLICT (source_id, business_id, financial_date) DO UPDATE SET
  registration_date = COALESCE(EXCLUDED.registration_date, financial_xbrl.finland_prh_xbrl_statement_artifacts.registration_date),
  source_url = EXCLUDED.source_url,
  latest_action_run_id = EXCLUDED.latest_action_run_id,
  updated_at = now()
RETURNING *;

-- name: ListFinlandPRHXBRLStatementArtifactsToDownload :many
SELECT *
FROM financial_xbrl.finland_prh_xbrl_statement_artifacts
WHERE source_id = sqlc.arg(source_id)
  AND (
    download_status = 'pending'
    OR (download_status = 'failed' AND sqlc.arg(retry_failed)::boolean)
  )
ORDER BY registration_date NULLS LAST, business_id, financial_date
LIMIT sqlc.arg(row_limit);

-- name: MarkFinlandPRHXBRLStatementArtifactDownloading :one
UPDATE financial_xbrl.finland_prh_xbrl_statement_artifacts
SET
  download_status = 'downloading',
  attempts = attempts + 1,
  last_attempt_at = now(),
  latest_action_run_id = sqlc.narg(latest_action_run_id),
  last_error_message = NULL,
  updated_at = now()
WHERE id = sqlc.arg(id)
RETURNING *;

-- name: MarkFinlandPRHXBRLStatementArtifactSucceeded :one
UPDATE financial_xbrl.finland_prh_xbrl_statement_artifacts
SET
  download_status = 'succeeded',
  xml_path = sqlc.arg(xml_path),
  xml_sha256 = sqlc.arg(xml_sha256),
  xml_size_bytes = sqlc.arg(xml_size_bytes),
  downloaded_at = now(),
  latest_action_run_id = sqlc.narg(latest_action_run_id),
  last_error_message = NULL,
  updated_at = now()
WHERE id = sqlc.arg(id)
RETURNING *;

-- name: MarkFinlandPRHXBRLStatementArtifactFailed :one
UPDATE financial_xbrl.finland_prh_xbrl_statement_artifacts
SET
  download_status = 'failed',
  latest_action_run_id = sqlc.narg(latest_action_run_id),
  last_error_message = NULLIF(sqlc.arg(last_error_message)::text, ''),
  updated_at = now()
WHERE id = sqlc.arg(id)
RETURNING *;
```

- [ ] **Step 4: Generate sqlc code**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
make sqlc-generate
```

Expected: generated code under `corpscout/scheduler/internal/db/gen`.

- [ ] **Step 5: Run query-shape test**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/db -run TestFinancialXBRLQueriesShape -count=1
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/database/queries/financial_xbrl.sql corpscout/scheduler/internal/db/gen corpscout/scheduler/internal/db/financial_xbrl_queries_test.go
git commit -m "feat: add finland prh xbrl ledger queries"
```

---

### Task 4: Extend Source Action Payload And Workflow Inputs

**Files:**
- Modify: `corpscout/scheduler/internal/temporal/workflow/companysources/workflow.go`
- Modify: `corpscout/scheduler/internal/temporal/workflow/companysources/workflow_test.go`
- Modify: `corpscout/scheduler/internal/temporal/actions/companysources/actions.go`
- Modify: `corpscout/scheduler/internal/httpapi/source_actions.go`
- Modify: `corpscout/scheduler/internal/httpapi/source_actions_test.go`

- [ ] **Step 1: Add workflow propagation test**

In `corpscout/scheduler/internal/temporal/workflow/companysources/workflow_test.go`, add a test that verifies download action input reaches file child workflow:

```go
func TestDownloadSourcePropagatesFinancialXBRLWindowInput(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(DownloadSourceFile, workflow.RegisterOptions{Name: DownloadSourceFileWorkflowName})

	prepared := PrepareSourceDownloadResult{
		ActionRunID: "action-1",
		Files: []DownloadSourceFileInput{{
			FileRunID:         "file-1",
			SourceName:        "finland_prh_xbrl",
			FileKey:           "statements_manifest",
			Trigger:           "manual",
			ParentActionRunID: "action-1",
			RegisteredDateStart: "2026-06-01",
			RegisteredDateEnd:   "2026-06-03",
			MaxStatements:       50,
			RetryFailed:         true,
		}},
	}

	env.OnActivity(PrepareSourceDownloadActivityName, mock.Anything, SyncSourceDownloadInput{
		ActionRunID:         "action-1",
		SourceName:          "finland_prh_xbrl",
		Trigger:             "manual",
		RegisteredDateStart: "2026-06-01",
		RegisteredDateEnd:   "2026-06-03",
		MaxStatements:       50,
		RetryFailed:         true,
	}).Return(prepared, nil)
	env.OnWorkflow(DownloadSourceFileWorkflowName, mock.Anything, prepared.Files[0]).Return(DownloadSourceFileResult{
		FileRunID:          "file-1",
		SourceName:         "finland_prh_xbrl",
		FileKey:            "statements_manifest",
		Path:               "/tmp/statements.ndjson",
		ContentSHA256:      "abc",
		ContentLengthBytes: 12,
		RecordsWritten:     1,
	}, nil)
	env.OnActivity(FinishSourceDownloadActivityName, mock.Anything, mock.Anything).Return(nil)

	env.ExecuteWorkflow(DownloadSource, SyncSourceDownloadInput{
		ActionRunID:         "action-1",
		SourceName:          "finland_prh_xbrl",
		Trigger:             "manual",
		RegisteredDateStart: "2026-06-01",
		RegisteredDateEnd:   "2026-06-03",
		MaxStatements:       50,
		RetryFailed:         true,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
}
```

- [ ] **Step 2: Add HTTP payload decoding test**

In `corpscout/scheduler/internal/httpapi/source_actions_test.go`, add:

```go
func TestDecodeSourceActionTriggerRequestIncludesFinancialXBRLWindow(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/api/v1/sources/finland_prh_xbrl/actions/pull_source", strings.NewReader(`{
		"trigger": "manual",
		"registered_date_start": "2026-06-01",
		"registered_date_end": "2026-06-03",
		"max_statements": 50,
		"retry_failed": true
	}`))

	got, err := decodeSourceActionTriggerRequest(req)

	require.NoError(t, err)
	require.Equal(t, "manual", got.Trigger)
	require.Equal(t, "2026-06-01", got.RegisteredDateStart)
	require.Equal(t, "2026-06-03", got.RegisteredDateEnd)
	require.Equal(t, int32(50), got.MaxStatements)
	require.True(t, got.RetryFailed)
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/temporal/workflow/companysources ./internal/httpapi -run 'TestDownloadSourcePropagatesFinancialXBRLWindowInput|TestDecodeSourceActionTriggerRequestIncludesFinancialXBRLWindow' -count=1
```

Expected: FAIL because fields do not exist.

- [ ] **Step 4: Add input fields**

In `corpscout/scheduler/internal/temporal/workflow/companysources/workflow.go`, extend both `SyncSourceDownloadInput` and `DownloadSourceFileInput`:

```go
RegisteredDateStart string `json:"registered_date_start,omitempty"`
RegisteredDateEnd   string `json:"registered_date_end,omitempty"`
MaxStatements       int32  `json:"max_statements,omitempty"`
RetryFailed         bool   `json:"retry_failed,omitempty"`
```

In `PrepareSourceDownloadActivity`, when appending `DownloadSourceFileInput`, copy these fields from `input`:

```go
RegisteredDateStart: input.RegisteredDateStart,
RegisteredDateEnd:   input.RegisteredDateEnd,
MaxStatements:       input.MaxStatements,
RetryFailed:         input.RetryFailed,
```

- [ ] **Step 5: Add HTTP request fields**

In `corpscout/scheduler/internal/httpapi/source_actions.go`, extend `sourceActionTriggerRequest`:

```go
RegisteredDateStart string `json:"registered_date_start,omitempty"`
RegisteredDateEnd   string `json:"registered_date_end,omitempty"`
MaxStatements       int32  `json:"max_statements,omitempty"`
RetryFailed         bool   `json:"retry_failed,omitempty"`
```

Trim date strings in `decodeSourceActionTriggerRequest`:

```go
req.RegisteredDateStart = strings.TrimSpace(req.RegisteredDateStart)
req.RegisteredDateEnd = strings.TrimSpace(req.RegisteredDateEnd)
```

In `sourceActionWorkflowInput`, populate `SyncSourceDownloadInput`:

```go
return companysourceworkflows.SyncSourceDownloadInput{
	ActionRunID:         actionRunID,
	SourceName:          sourceName,
	Trigger:             req.Trigger,
	RegisteredDateStart: req.RegisteredDateStart,
	RegisteredDateEnd:   req.RegisteredDateEnd,
	MaxStatements:       req.MaxStatements,
	RetryFailed:         req.RetryFailed,
}, nil
```

- [ ] **Step 6: Run focused tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/temporal/workflow/companysources ./internal/httpapi -run 'TestDownloadSourcePropagatesFinancialXBRLWindowInput|TestDecodeSourceActionTriggerRequestIncludesFinancialXBRLWindow' -count=1
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/scheduler/internal/temporal/workflow/companysources/workflow.go \
  corpscout/scheduler/internal/temporal/workflow/companysources/workflow_test.go \
  corpscout/scheduler/internal/temporal/actions/companysources/actions.go \
  corpscout/scheduler/internal/httpapi/source_actions.go \
  corpscout/scheduler/internal/httpapi/source_actions_test.go
git commit -m "feat: pass finland prh xbrl download window input"
```

---

### Task 5: Implement PRH XBRL Client And Manifest Download Logic

**Files:**
- Create: `corpscout/scheduler/internal/companysources/finland/prhxbrl/client.go`
- Modify: `corpscout/scheduler/internal/companysources/finland/prhxbrl/download.go`
- Create: `corpscout/scheduler/internal/companysources/finland/prhxbrl/download_test.go`

- [ ] **Step 1: Write client and manifest tests**

Create `corpscout/scheduler/internal/companysources/finland/prhxbrl/download_test.go`:

```go
package prhxbrl

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildAllFinancialStatementsURL(t *testing.T) {
	got, err := buildAllFinancialStatementsURL("https://avoindata.prh.fi/opendata-xbrl-api/v3", "2026-06-01", "2026-06-03", 2)
	require.NoError(t, err)
	require.Equal(t, "https://avoindata.prh.fi/opendata-xbrl-api/v3/all_financial_statements?page=2&registeredDateEnd=2026-06-03&registeredDateStart=2026-06-01", got)
}

func TestDownloadDiscoveryPageDecodesFinancials(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/opendata-xbrl-api/v3/all_financial_statements", r.URL.Path)
		require.Equal(t, "2026-06-01", r.URL.Query().Get("registeredDateStart"))
		require.Equal(t, "2026-06-03", r.URL.Query().Get("registeredDateEnd"))
		_, _ = w.Write([]byte(`{"totalResults":1,"financials":[{"businessId":"0100130-4","financialDate":"2024-12-31","registrationDate":"2025-04-15"}]}`))
	}))
	defer server.Close()

	page, err := downloadDiscoveryPage(context.Background(), server.Client(), server.URL+"/opendata-xbrl-api/v3", "2026-06-01", "2026-06-03", 1, false)

	require.NoError(t, err)
	require.Equal(t, int64(1), page.TotalResults)
	require.Len(t, page.Financials, 1)
	require.Equal(t, "0100130-4", page.Financials[0].BusinessID)
}

func TestDownloadStatementXMLWritesHashAndSize(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/opendata-xbrl-api/v3/financial", r.URL.Path)
		require.Equal(t, "0100130-4", r.URL.Query().Get("businessId"))
		require.Equal(t, "2024-12-31", r.URL.Query().Get("financialDate"))
		w.Header().Set("Content-Type", "text/xml")
		_, _ = w.Write([]byte(`<xbrl><fact>100</fact></xbrl>`))
	}))
	defer server.Close()

	tmp := t.TempDir()
	result, err := downloadStatementXML(context.Background(), server.Client(), server.URL+"/opendata-xbrl-api/v3", "0100130-4", "2024-12-31", tmp, false)

	require.NoError(t, err)
	require.FileExists(t, result.Path)
	require.Equal(t, filepath.Join(tmp, "statements", "0100130-4", "2024-12-31.xml"), result.Path)
	require.NotEmpty(t, result.SHA256)
	require.Equal(t, int64(len(`<xbrl><fact>100</fact></xbrl>`)), result.SizeBytes)
}

func TestWriteStatementsManifest(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "statements.ndjson")
	rows := []ManifestStatement{{
		BusinessID:     "0100130-4",
		FinancialDate:  "2024-12-31",
		RegistrationDate: "2025-04-15",
		SourceURL:      "https://example.test/financial?businessId=0100130-4&financialDate=2024-12-31",
		DownloadStatus: "succeeded",
		XMLPath:        filepath.Join(tmp, "statements", "0100130-4", "2024-12-31.xml"),
		XMLSHA256:      "abc",
		XMLSizeBytes:   12,
	}}

	result, err := writeStatementsManifest(path, rows)
	require.NoError(t, err)
	require.Equal(t, int64(1), result.RecordsWritten)

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var decoded ManifestStatement
	require.NoError(t, json.Unmarshal(data[:len(data)-1], &decoded))
	require.Equal(t, rows[0].BusinessID, decoded.BusinessID)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/companysources/finland/prhxbrl -count=1
```

Expected: FAIL because helper types/functions do not exist.

- [ ] **Step 3: Implement PRH XBRL client**

Create `corpscout/scheduler/internal/companysources/finland/prhxbrl/client.go`:

```go
package prhxbrl

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"

	"github.com/cockroachdb/errors"
	"github.com/pulsarpoint/corpscout/scheduler/internal/companysources"
)

type DiscoveryPage struct {
	TotalResults int64                `json:"totalResults"`
	Financials   []DiscoveredStatement `json:"financials"`
}

type DiscoveredStatement struct {
	BusinessID        string `json:"businessId"`
	FinancialDate     string `json:"financialDate"`
	RegistrationDate  string `json:"registrationDate"`
}

func buildAllFinancialStatementsURL(baseURL string, registeredDateStart string, registeredDateEnd string, page int) (string, error) {
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return "", errors.Wrap(err, "parse PRH XBRL base url")
	}
	parsed.Path = "/opendata-xbrl-api/v3/all_financial_statements"
	query := parsed.Query()
	query.Set("registeredDateStart", registeredDateStart)
	query.Set("registeredDateEnd", registeredDateEnd)
	query.Set("page", strconv.Itoa(page))
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func buildFinancialStatementURL(baseURL string, businessID string, financialDate string) (string, error) {
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return "", errors.Wrap(err, "parse PRH XBRL base url")
	}
	parsed.Path = "/opendata-xbrl-api/v3/financial"
	query := parsed.Query()
	query.Set("businessId", businessID)
	query.Set("financialDate", financialDate)
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func downloadDiscoveryPage(ctx context.Context, client *http.Client, baseURL string, registeredDateStart string, registeredDateEnd string, page int, userAgentRequired bool) (DiscoveryPage, error) {
	if client == nil {
		client = http.DefaultClient
	}
	pageURL, err := buildAllFinancialStatementsURL(baseURL, registeredDateStart, registeredDateEnd, page)
	if err != nil {
		return DiscoveryPage{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, pageURL, nil)
	if err != nil {
		return DiscoveryPage{}, errors.Wrap(err, "create PRH XBRL discovery request")
	}
	if userAgentRequired {
		req.Header.Set("User-Agent", companysources.DownloadUserAgent)
	}
	resp, err := client.Do(req)
	if err != nil {
		return DiscoveryPage{}, errors.Wrap(err, "download PRH XBRL discovery page")
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return DiscoveryPage{}, errors.Errorf("download PRH XBRL discovery page: status %d", resp.StatusCode)
	}
	var pageResult DiscoveryPage
	if err := json.NewDecoder(resp.Body).Decode(&pageResult); err != nil {
		return DiscoveryPage{}, errors.Wrap(err, "decode PRH XBRL discovery page")
	}
	return pageResult, nil
}
```

- [ ] **Step 4: Implement XML and manifest writing**

Replace the temporary `download.go` implementation with helper functions. Keep `Source.DownloadFile` returning the source-specific Temporal message until Task 6.

```go
package prhxbrl

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"

	"github.com/cockroachdb/errors"
	"github.com/pulsarpoint/corpscout/scheduler/internal/companysources"
)

type StatementXMLDownload struct {
	Path      string
	SHA256    string
	SizeBytes int64
	SourceURL string
}

type ManifestStatement struct {
	BusinessID        string `json:"business_id"`
	FinancialDate     string `json:"financial_date"`
	RegistrationDate  string `json:"registration_date,omitempty"`
	SourceURL         string `json:"source_url"`
	DownloadStatus    string `json:"download_status"`
	XMLPath           string `json:"xml_path,omitempty"`
	XMLSHA256         string `json:"xml_sha256,omitempty"`
	XMLSizeBytes      int64  `json:"xml_size_bytes,omitempty"`
	ErrorMessage      string `json:"error_message,omitempty"`
}

func (Source) DownloadFile(ctx context.Context, opts companysources.DownloadFileOptions) (companysources.DownloadedFile, error) {
	_ = ctx
	_ = opts
	return companysources.DownloadedFile{}, errors.New("Finland PRH financial XBRL download requires the source-specific Temporal action")
}

func downloadStatementXML(ctx context.Context, client *http.Client, baseURL string, businessID string, financialDate string, runDir string, userAgentRequired bool) (StatementXMLDownload, error) {
	if client == nil {
		client = http.DefaultClient
	}
	statementURL, err := buildFinancialStatementURL(baseURL, businessID, financialDate)
	if err != nil {
		return StatementXMLDownload{}, err
	}
	outputPath, err := companysources.SafeRunRelativePath(runDir, filepath.Join("statements", businessID, financialDate+".xml"))
	if err != nil {
		return StatementXMLDownload{}, err
	}
	if err := os.MkdirAll(filepath.Dir(outputPath), 0o755); err != nil {
		return StatementXMLDownload{}, errors.Wrap(err, "create PRH XBRL statement directory")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, statementURL, nil)
	if err != nil {
		return StatementXMLDownload{}, errors.Wrap(err, "create PRH XBRL statement request")
	}
	if userAgentRequired {
		req.Header.Set("User-Agent", companysources.DownloadUserAgent)
	}
	resp, err := client.Do(req)
	if err != nil {
		return StatementXMLDownload{}, errors.Wrap(err, "download PRH XBRL statement XML")
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return StatementXMLDownload{}, errors.Errorf("download PRH XBRL statement XML: status %d", resp.StatusCode)
	}

	file, err := os.Create(outputPath)
	if err != nil {
		return StatementXMLDownload{}, errors.Wrap(err, "create PRH XBRL statement XML")
	}
	defer file.Close()
	hasher := sha256.New()
	size, err := io.Copy(io.MultiWriter(file, hasher), resp.Body)
	if err != nil {
		return StatementXMLDownload{}, errors.Wrap(err, "write PRH XBRL statement XML")
	}
	return StatementXMLDownload{
		Path:      outputPath,
		SHA256:    hex.EncodeToString(hasher.Sum(nil)),
		SizeBytes: size,
		SourceURL: statementURL,
	}, nil
}

func writeStatementsManifest(path string, rows []ManifestStatement) (companysources.FileWriteResult, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return companysources.FileWriteResult{}, errors.Wrap(err, "create PRH XBRL manifest directory")
	}
	file, err := os.Create(path)
	if err != nil {
		return companysources.FileWriteResult{}, errors.Wrap(err, "create PRH XBRL manifest")
	}
	defer file.Close()

	hasher := sha256.New()
	writer := bufio.NewWriter(io.MultiWriter(file, hasher))
	var bytesWritten int64
	for _, row := range rows {
		line, err := json.Marshal(row)
		if err != nil {
			return companysources.FileWriteResult{}, errors.Wrap(err, "marshal PRH XBRL manifest row")
		}
		n, err := writer.Write(line)
		if err != nil {
			return companysources.FileWriteResult{}, errors.Wrap(err, "write PRH XBRL manifest row")
		}
		bytesWritten += int64(n)
		n, err = writer.WriteString("\n")
		if err != nil {
			return companysources.FileWriteResult{}, errors.Wrap(err, "write PRH XBRL manifest newline")
		}
		bytesWritten += int64(n)
	}
	if err := writer.Flush(); err != nil {
		return companysources.FileWriteResult{}, errors.Wrap(err, "flush PRH XBRL manifest")
	}
	return companysources.FileWriteResult{
		SourceFilePath:     path,
		ContentSHA256:      hex.EncodeToString(hasher.Sum(nil)),
		ContentLengthBytes: bytesWritten,
		RecordsWritten:     int64(len(rows)),
	}, nil
}
```

- [ ] **Step 5: Run package tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/companysources/finland/prhxbrl -count=1
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/scheduler/internal/companysources/finland/prhxbrl
git commit -m "feat: add prh xbrl download client"
```

---

### Task 6: Connect PRH XBRL Downloader To Temporal Activity And Ledger

**Files:**
- Modify: `corpscout/scheduler/internal/companysources/finland/prhxbrl/download.go`
- Modify: `corpscout/scheduler/internal/companysources/sourcecatalog/sources/finland_prh_xbrl.json`
- Modify: `corpscout/scheduler/internal/companysources/sourcecatalog/catalog_test.go`
- Modify: `corpscout/scheduler/internal/companysources/sourcecatalog/sync_test.go`
- Modify: `corpscout/scheduler/internal/temporal/actions/companysources/actions.go`
- Modify: `corpscout/scheduler/internal/temporal/actions/companysources/actions_test.go`

- [ ] **Step 1: Add activity validation tests**

In `corpscout/scheduler/internal/temporal/actions/companysources/actions_test.go`, add tests around the new validation helper:

```go
func TestValidatePRHXBRLWindowInputRequiresDates(t *testing.T) {
	_, err := validatePRHXBRLWindowInput(DownloadSourceFileInput{
		SourceName: "finland_prh_xbrl",
		FileKey:    "statements_manifest",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "registered_date_start is required")
}

func TestValidatePRHXBRLWindowInputRejectsInvertedDates(t *testing.T) {
	_, err := validatePRHXBRLWindowInput(DownloadSourceFileInput{
		SourceName:           "finland_prh_xbrl",
		FileKey:              "statements_manifest",
		RegisteredDateStart:  "2026-06-03",
		RegisteredDateEnd:    "2026-06-01",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "registered_date_start must be on or before registered_date_end")
}

func TestValidatePRHXBRLWindowInputDefaultsMaxStatements(t *testing.T) {
	got, err := validatePRHXBRLWindowInput(DownloadSourceFileInput{
		SourceName:           "finland_prh_xbrl",
		FileKey:              "statements_manifest",
		RegisteredDateStart:  "2026-06-01",
		RegisteredDateEnd:    "2026-06-03",
	})
	require.NoError(t, err)
	require.Equal(t, int32(50), got.MaxStatements)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/temporal/actions/companysources -run TestValidatePRHXBRLWindowInput -count=1
```

Expected: FAIL because validation does not exist.

- [ ] **Step 3: Add PRH XBRL orchestration function**

In `corpscout/scheduler/internal/companysources/finland/prhxbrl/download.go`, add an exported function that accepts concrete sqlc queries. Use the generated names from Task 3.

```go
type DownloadOptions struct {
	Queries             *db.Queries
	HTTPClient          *http.Client
	SourceID            uuid.UUID
	ActionRunID         uuid.UUID
	TemporalWorkflowID  string
	TemporalRunID       string
	RunDir              string
	ManifestRelativePath string
	SourceURL           string
	UserAgentRequired   bool
	RegisteredDateStart string
	RegisteredDateEnd   string
	MaxStatements       int32
	RetryFailed         bool
}
```

Add these conversion helpers in the same file:

```go
func parseDateToPgtype(value string) (pgtype.Date, error) {
	parsed, err := time.Parse(time.DateOnly, strings.TrimSpace(value))
	if err != nil {
		return pgtype.Date{}, errors.Wrap(err, "parse PRH XBRL date")
	}
	return pgtype.Date{Time: parsed, Valid: true}, nil
}

func nullableDate(value string) (pgtype.Date, error) {
	if strings.TrimSpace(value) == "" {
		return pgtype.Date{}, nil
	}
	return parseDateToPgtype(value)
}

func pgUUID(id uuid.UUID) pgtype.UUID {
	return pgtype.UUID{Bytes: id, Valid: true}
}

func optionalText(value string) *string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}
```

Implement `Download(ctx context.Context, opts DownloadOptions) (companysources.DownloadedFile, error)` with these concrete operations:

```go
startDate, err := parseDateToPgtype(opts.RegisteredDateStart)
if err != nil {
	return companysources.DownloadedFile{}, err
}
endDate, err := parseDateToPgtype(opts.RegisteredDateEnd)
if err != nil {
	return companysources.DownloadedFile{}, err
}
window, err := opts.Queries.UpsertFinlandPRHXBRLDiscoveryWindow(ctx, db.UpsertFinlandPRHXBRLDiscoveryWindowParams{
	SourceID:            opts.SourceID,
	RegisteredDateStart: startDate,
	RegisteredDateEnd:   endDate,
	ActionRunID:         pgUUID(opts.ActionRunID),
	TemporalWorkflowID:  optionalText(opts.TemporalWorkflowID),
	TemporalRunID:       optionalText(opts.TemporalRunID),
})
```

Then loop discovery pages from 1 until:

- an empty `financials` array is returned;
- discovered count reaches `totalResults`;
- `maxStatements` is reached and at least one page has been processed.

For each discovered item:

```go
registrationDate, err := nullableDate(statement.RegistrationDate)
if err != nil {
	return companysources.DownloadedFile{}, err
}
financialDate, err := parseDateToPgtype(statement.FinancialDate)
if err != nil {
	return companysources.DownloadedFile{}, err
}
artifact, err := opts.Queries.UpsertFinlandPRHXBRLStatementArtifact(ctx, db.UpsertFinlandPRHXBRLStatementArtifactParams{
	SourceID:              opts.SourceID,
	BusinessID:            statement.BusinessID,
	FinancialDate:         financialDate,
	RegistrationDate:      registrationDate,
	SourceUrl:             statementURL,
	FirstDiscoveredRunID:  pgUUID(opts.ActionRunID),
	LatestActionRunID:     pgUUID(opts.ActionRunID),
})
```

After discovery, list artifacts to download:

```go
artifacts, err := opts.Queries.ListFinlandPRHXBRLStatementArtifactsToDownload(ctx, db.ListFinlandPRHXBRLStatementArtifactsToDownloadParams{
	SourceID:    opts.SourceID,
	RetryFailed: opts.RetryFailed,
	RowLimit:    opts.MaxStatements,
})
```

For each artifact, mark `downloading`, download XML, mark `succeeded` or `failed`, and append a `ManifestStatement` row. Finally write the manifest and return `companysources.DownloadedFile` for `statements.ndjson`.

- [ ] **Step 4: Add source-specific branch to activity**

In `corpscout/scheduler/internal/temporal/actions/companysources/actions.go`, before generic `sourcecore.DownloadFile`, branch:

```go
var downloaded sourcecore.DownloadedFile
if fileRun.SourceName == prhxbrl.SourceName && fileRun.FileKey == "statements_manifest" {
	windowInput, err := validatePRHXBRLWindowInput(input)
	if err != nil {
		finishErr := a.finishFileRunFailed(fileRun.ID, err)
		return DownloadSourceFileResult{FileRunID: input.FileRunID, SourceName: input.SourceName, FileKey: input.FileKey}, combineWithFinishError(err, finishErr)
	}
	parentActionRunID, err := uuid.Parse(input.ParentActionRunID)
	if err != nil {
		finishErr := a.finishFileRunFailed(fileRun.ID, err)
		return DownloadSourceFileResult{FileRunID: input.FileRunID, SourceName: input.SourceName, FileKey: input.FileKey}, combineWithFinishError(errors.Wrap(err, "parse parent action run id"), finishErr)
	}
	downloaded, err = prhxbrl.Download(ctx, prhxbrl.DownloadOptions{
		Queries:              queries,
		HTTPClient:           http.DefaultClient,
		SourceID:             fileRun.SourceID,
		ActionRunID:          parentActionRunID,
		TemporalWorkflowID:   workflowID,
		TemporalRunID:        runID,
		RunDir:               runDir,
		ManifestRelativePath: fileRun.RelativePath,
		SourceURL:            fileRun.SourceUrl,
		UserAgentRequired:    fileRun.UserAgentRequired,
		RegisteredDateStart:  windowInput.RegisteredDateStart,
		RegisteredDateEnd:    windowInput.RegisteredDateEnd,
		MaxStatements:        windowInput.MaxStatements,
		RetryFailed:          windowInput.RetryFailed,
	})
} else {
	downloaded, err = sourcecore.DownloadFile(ctx, a.registry, sourcecore.DownloadFileRequest{
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
}
```

Add imports:

```go
	"net/http"
	"github.com/pulsarpoint/corpscout/scheduler/internal/companysources/finland/prhxbrl"
```

Implement `validatePRHXBRLWindowInput` in the same file:

```go
type prhxbrlWindowInput struct {
	RegisteredDateStart string
	RegisteredDateEnd   string
	MaxStatements       int32
	RetryFailed         bool
}

func validatePRHXBRLWindowInput(input DownloadSourceFileInput) (prhxbrlWindowInput, error) {
	start := strings.TrimSpace(input.RegisteredDateStart)
	end := strings.TrimSpace(input.RegisteredDateEnd)
	if start == "" {
		return prhxbrlWindowInput{}, errors.New("registered_date_start is required")
	}
	if end == "" {
		return prhxbrlWindowInput{}, errors.New("registered_date_end is required")
	}
	startDate, err := time.Parse(time.DateOnly, start)
	if err != nil {
		return prhxbrlWindowInput{}, errors.Wrap(err, "parse registered_date_start")
	}
	endDate, err := time.Parse(time.DateOnly, end)
	if err != nil {
		return prhxbrlWindowInput{}, errors.Wrap(err, "parse registered_date_end")
	}
	if startDate.After(endDate) {
		return prhxbrlWindowInput{}, errors.New("registered_date_start must be on or before registered_date_end")
	}
	maxStatements := input.MaxStatements
	if maxStatements <= 0 {
		maxStatements = 50
	}
	if maxStatements > 1000 {
		return prhxbrlWindowInput{}, errors.New("max_statements must be 1000 or less")
	}
	return prhxbrlWindowInput{
		RegisteredDateStart: start,
		RegisteredDateEnd:   end,
		MaxStatements:       maxStatements,
		RetryFailed:         input.RetryFailed,
	}, nil
}
```

- [ ] **Step 5: Enable the catalog action**

In `corpscout/scheduler/internal/companysources/sourcecatalog/sources/finland_prh_xbrl.json`, set the `pull_source` action `"enabled"` value to `true`.

In `corpscout/scheduler/internal/companysources/sourcecatalog/catalog_test.go`, change the `finland_prh_xbrl` action assertions to:

```go
require.True(t, spec.Actions[0].Enabled)
```

In `corpscout/scheduler/internal/companysources/sourcecatalog/sync_test.go`, change the PRH XBRL action assertion to:

```go
require.True(t, prhXBRLAction.Enabled)
```

- [ ] **Step 6: Run focused tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/companysources/finland/prhxbrl ./internal/companysources/sourcecatalog ./internal/temporal/actions/companysources -count=1
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/scheduler/internal/companysources/finland/prhxbrl \
  corpscout/scheduler/internal/companysources/sourcecatalog/sources/finland_prh_xbrl.json \
  corpscout/scheduler/internal/companysources/sourcecatalog/catalog_test.go \
  corpscout/scheduler/internal/companysources/sourcecatalog/sync_test.go \
  corpscout/scheduler/internal/temporal/actions/companysources/actions.go \
  corpscout/scheduler/internal/temporal/actions/companysources/actions_test.go
git commit -m "feat: download finland prh xbrl statements"
```

---

### Task 7: Add Minimal UI Trigger Fields

**Files:**
- Modify: `corpscout/ui/app/types/api.ts`
- Modify: `corpscout/ui/app/lib/api.ts`
- Modify: `corpscout/ui/app/components/app/source-detail/ActionsTab.tsx`

- [ ] **Step 1: Add TypeScript trigger body type**

In `corpscout/ui/app/types/api.ts`, add:

```ts
export interface SourceActionTriggerRequest {
  trigger?: string;
  download_action_run_id?: string;
  batch_size?: number;
  limit?: number;
  registered_date_start?: string;
  registered_date_end?: string;
  max_statements?: number;
  retry_failed?: boolean;
}
```

If a trigger body type already exists, extend it with the last four fields.

- [ ] **Step 2: Use the type in API client**

In `corpscout/ui/app/lib/api.ts`, make `triggerSourceAction` accept `SourceActionTriggerRequest`:

```ts
triggerSourceAction: (
  name: string,
  action: SourceActionName,
  body: SourceActionTriggerRequest = {},
) =>
  post<StartWorkflowResponse>(
    `/sources/${name}/actions/${action}`,
    body,
  ),
```

- [ ] **Step 3: Add PRH XBRL form state**

In `corpscout/ui/app/components/app/source-detail/ActionsTab.tsx`, add state:

```tsx
const [xbrlStartDate, setXbrlStartDate] = useState("");
const [xbrlEndDate, setXbrlEndDate] = useState("");
const [xbrlMaxStatements, setXbrlMaxStatements] = useState("50");
const [xbrlRetryFailed, setXbrlRetryFailed] = useState(false);
```

Add helper:

```tsx
function triggerPRHXBRLDownload() {
  const maxStatements = Number(xbrlMaxStatements);
  void runAndRefresh("pull_source", () =>
    api.triggerSourceAction(source.name, "pull_source", {
      trigger: "manual",
      registered_date_start: xbrlStartDate.trim(),
      registered_date_end: xbrlEndDate.trim(),
      max_statements: Number.isFinite(maxStatements) ? maxStatements : 50,
      retry_failed: xbrlRetryFailed,
    }),
  );
}
```

For source `finland_prh_xbrl`, render a compact form in the download action card:

```tsx
{source.name === "finland_prh_xbrl" ? (
  <div className="flex flex-col gap-3">
    <div className="grid gap-2 sm:grid-cols-3">
      <Input
        type="date"
        value={xbrlStartDate}
        onChange={(event) => setXbrlStartDate(event.target.value)}
        aria-label="Registered date start"
      />
      <Input
        type="date"
        value={xbrlEndDate}
        onChange={(event) => setXbrlEndDate(event.target.value)}
        aria-label="Registered date end"
      />
      <Input
        type="number"
        min={1}
        max={1000}
        value={xbrlMaxStatements}
        onChange={(event) => setXbrlMaxStatements(event.target.value)}
        aria-label="Maximum statements"
      />
    </div>
    <label className="flex items-center gap-2 text-sm">
      <Checkbox
        checked={xbrlRetryFailed}
        onCheckedChange={(checked) => setXbrlRetryFailed(checked === true)}
      />
      Retry failed statement downloads
    </label>
    <Button
      type="button"
      size="sm"
      onClick={triggerPRHXBRLDownload}
      disabled={busy || loading || !downloadEnabled || !xbrlStartDate || !xbrlEndDate}
    >
      <Download data-icon="inline-start" />
      {busy ? "Starting" : "Download statements"}
    </Button>
  </div>
) : (
  <Button type="button" size="sm" onClick={triggerDownload} disabled={busy || loading || !downloadEnabled}>
    <Download data-icon="inline-start" />
    {busy ? "Starting" : "Download"}
  </Button>
)}
```

Import `Checkbox` if not already imported:

```tsx
import { Checkbox } from "~/components/ui/checkbox";
```

- [ ] **Step 4: Run UI typecheck**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/ui
npm run typecheck
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/ui/app/types/api.ts corpscout/ui/app/lib/api.ts corpscout/ui/app/components/app/source-detail/ActionsTab.tsx
git commit -m "feat: add prh xbrl download action form"
```

---

### Task 8: Final Verification And Manual Smoke Instructions

**Files:**
- No source changes expected unless verification finds defects.

- [ ] **Step 1: Run backend tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./... -count=1
```

Expected: PASS.

- [ ] **Step 2: Run frontend typecheck**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/ui
npm run typecheck
```

Expected: PASS.

- [ ] **Step 3: Run whitespace check**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git diff --check
```

Expected: no output.

- [ ] **Step 4: Apply migrations and catalog sync in a local environment**

Run only when local Postgres and the app environment are available:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
make migrate-up
```

Restart the scheduler/API so startup source catalog sync runs.

Expected:

```sql
SELECT name, registry_key FROM data_sources WHERE registry_key = 'finland/prh_xbrl';
SELECT action FROM data_source_actions a JOIN data_sources s ON s.id = a.source_id WHERE s.registry_key = 'finland/prh_xbrl';
SELECT file_key FROM data_source_files f JOIN data_sources s ON s.id = f.source_id WHERE s.registry_key = 'finland/prh_xbrl';
```

returns `finland_prh_xbrl`, `pull_source`, and `statements_manifest`.

- [ ] **Step 5: Run a small manual download smoke**

Use the API or UI to trigger a small date window:

```bash
curl -X POST http://localhost:8094/api/v1/sources/finland_prh_xbrl/actions/pull_source \
  -H 'Content-Type: application/json' \
  -d '{
    "trigger": "manual",
    "registered_date_start": "2026-06-01",
    "registered_date_end": "2026-06-03",
    "max_statements": 5,
    "retry_failed": false
  }'
```

Expected:

- response status `202`;
- a `data_source_action_runs` row for `finland_prh_xbrl`;
- a `data_source_file_runs` row for `statements_manifest`;
- rows in `financial_xbrl.finland_prh_xbrl_discovery_windows`;
- rows in `financial_xbrl.finland_prh_xbrl_statement_artifacts`;
- `statements.ndjson` and XML files under the configured source runs root.

- [ ] **Step 6: Commit any verification fixes**

If verification required code changes, return to the task that owns the affected file, rerun that task's focused tests, and commit with that task's commit command. Do not create an extra catch-all verification commit.

If no fixes were needed, do not create an empty commit.
