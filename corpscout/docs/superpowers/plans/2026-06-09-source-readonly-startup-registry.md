# Source Read-Only Startup Registry Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make source information read-only in the UI/API and make Corpscout load typed source declarations from per-source JSON files at startup into `data_sources`.

**Architecture:** Source handlers own source metadata. Each source has one checked-in JSON declaration that is embedded in the scheduler binary, validated on startup, and upserted into explicit `data_sources` columns. The UI only reads source metadata; users cannot add, edit, or save source config fields.

**Tech Stack:** Go 1.26, sqlc, pgx, React Router, TypeScript, shadcn/ui, Postgres migrations.

---

## File Structure

- Create `corpscout/scheduler/internal/companysources/sourcecatalog/spec.go`
  - Defines the typed source declaration loaded from JSON.
  - Validates stable keys and required values before DB sync.
- Create `corpscout/scheduler/internal/companysources/sourcecatalog/catalog.go`
  - Embeds `sources/*.json`.
  - Loads and validates all source specs.
- Create `corpscout/scheduler/internal/companysources/sourcecatalog/sync.go`
  - Syncs embedded specs into Postgres on scheduler startup.
  - Prunes `data_sources` rows not present in the embedded catalog.
- Create `corpscout/scheduler/internal/companysources/sourcecatalog/sources/finland_prhytj.json`
- Create `corpscout/scheduler/internal/companysources/sourcecatalog/sources/united_states_coloradoentities.json`
- Create `corpscout/scheduler/internal/companysources/sourcecatalog/sources/united_states_irseobmf.json`
- Create `corpscout/scheduler/internal/companysources/sourcecatalog/sources/united_states_secedgar.json`
  - One JSON declaration per source.
- Modify `corpscout/database/migrations/000113_source_catalog_columns.up.sql`
  - Adds explicit columns currently hidden in `data_sources.config`: `source_url`, `docs_url`, `raw_source_retention`, `source_file_name`, `user_agent_required`.
  - Does not insert or backfill source values; startup catalog sync is the only owner of source values.
- Modify `corpscout/database/migrations/000113_source_catalog_columns.down.sql`
  - Drops the added columns.
- Modify `corpscout/database/queries/sources.sql`
  - Select new explicit columns.
  - Add `UpsertDataSourceFromCatalog` and `PruneDataSourcesNotInCatalog`.
  - Remove or stop using config update queries.
- Regenerate `corpscout/scheduler/internal/db/gen/sources.sql.go`, `models.go`, and `querier.go`.
- Modify `corpscout/scheduler/internal/app/server.go`
  - Sync source catalog after DB connection is ready and before HTTP handlers are created.
- Modify `corpscout/scheduler/internal/httpapi/source_read.go`
  - Return new explicit fields.
  - Stop exposing source config as an editable operational object.
- Modify `corpscout/scheduler/internal/httpapi/source_patch.go`
  - Reject `config` patches. Keep only fields we intentionally allow; for this task, no source metadata mutation is allowed through UI.
- Modify `corpscout/ui/app/types/api.ts`
  - Add new explicit source metadata fields.
  - Remove editable `config` dependency from source detail UI types.
- Modify `corpscout/ui/app/components/app/source-detail/ConfigTab.tsx`
  - Replace editable rows with read-only source metadata.
  - Remove `Add field`, `Reset`, and `Save`.
- Modify `corpscout/ui/app/routes/sources_.$name.config.tsx`
  - Stop passing `saving` and `onPatch` into `ConfigTab`.
- Modify `corpscout/ui/app/routes/sources_.$name.tsx`
  - Remove config-editing responsibility from source detail context.

---

## Source Declaration Shape

Each source declaration JSON maps directly to typed DB columns. It is not stored as JSON in Postgres.
Migrations create the columns only. They must not duplicate source declaration values in SQL `UPDATE` or `INSERT` statements.
On scheduler startup, `sourcecatalog.LoadEmbedded()` walks the `sourcecatalog/sources/*.json` folder and `sourcecatalog.Sync()` upserts those values into `data_sources`.

Example: `corpscout/scheduler/internal/companysources/sourcecatalog/sources/finland_prhytj.json`

```json
{
  "name": "finland_prhytj",
  "country": "finland",
  "source": "prhytj",
  "registry_key": "finland/prhytj",
  "display_name": "Finland PRH YTJ",
  "description": "Finnish company registry data from PRH Open Data YTJ API v3",
  "source_group": "registry",
  "input_table_name": "corpscout_sources.fi_prhytj_*",
  "enabled": true,
  "auth_required": false,
  "storage_kind": "clickhouse",
  "clickhouse_database": "corpscout_sources",
  "clickhouse_table_prefix": "fi_prhytj",
  "source_url": "https://avoindata.prh.fi/opendata-ytj-api/v3/companies",
  "docs_url": "https://www.prh.fi/en/kaupparekisteri/tietopalvelut/open_data.html",
  "raw_source_retention": "filesystem_run_directory",
  "source_file_name": "source.ndjson",
  "user_agent_required": false,
  "capabilities": ["company_import", "clickhouse", "source_download"],
  "requires_translation": false
}
```

---

### Task 1: Add Explicit Source Catalog Columns

**Files:**
- Create: `corpscout/database/migrations/000113_source_catalog_columns.up.sql`
- Create: `corpscout/database/migrations/000113_source_catalog_columns.down.sql`
- Modify: `corpscout/scheduler/internal/db/source_metadata_actions_migration_test.go`
- Create: `corpscout/scheduler/internal/db/source_catalog_columns_migration_test.go`

- [ ] **Step 1: Write the migration test**

Create `corpscout/scheduler/internal/db/source_catalog_columns_migration_test.go`:

```go
package db_test

import (
	"os"
	"strings"
	"testing"
)

func TestSourceCatalogColumnsMigrationAddsTypedSourceMetadata(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000113_source_catalog_columns.up.sql")
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	sql := string(body)

	required := []string{
		"ADD COLUMN IF NOT EXISTS source_url TEXT",
		"ADD COLUMN IF NOT EXISTS docs_url TEXT",
		"ADD COLUMN IF NOT EXISTS raw_source_retention TEXT",
		"ADD COLUMN IF NOT EXISTS source_file_name TEXT",
		"ADD COLUMN IF NOT EXISTS user_agent_required BOOLEAN NOT NULL DEFAULT false",
		"chk_data_sources_source_file_name",
	}
	for _, needle := range required {
		if !strings.Contains(sql, needle) {
			t.Fatalf("migration missing %q", needle)
		}
	}

	forbidden := []string{
		"UPDATE data_sources",
		"https://avoindata.prh.fi",
		"https://data.colorado.gov",
		"https://www.irs.gov",
		"https://www.sec.gov",
	}
	for _, needle := range forbidden {
		if strings.Contains(sql, needle) {
			t.Fatalf("migration must not seed source catalog value %q", needle)
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/db -count=1
```

Expected: FAIL because `000113_source_catalog_columns.up.sql` does not exist.

- [ ] **Step 3: Create migration**

Create `corpscout/database/migrations/000113_source_catalog_columns.up.sql`:

```sql
ALTER TABLE data_sources
  ADD COLUMN IF NOT EXISTS source_url TEXT,
  ADD COLUMN IF NOT EXISTS docs_url TEXT,
  ADD COLUMN IF NOT EXISTS raw_source_retention TEXT,
  ADD COLUMN IF NOT EXISTS source_file_name TEXT,
  ADD COLUMN IF NOT EXISTS user_agent_required BOOLEAN NOT NULL DEFAULT false;

ALTER TABLE data_sources
  ADD CONSTRAINT chk_data_sources_source_file_name CHECK (
    source_file_name IS NULL OR source_file_name IN ('source.ndjson', 'source.json')
  );
```

Create `corpscout/database/migrations/000113_source_catalog_columns.down.sql`:

```sql
ALTER TABLE data_sources
  DROP CONSTRAINT IF EXISTS chk_data_sources_source_file_name,
  DROP COLUMN IF EXISTS user_agent_required,
  DROP COLUMN IF EXISTS source_file_name,
  DROP COLUMN IF EXISTS raw_source_retention,
  DROP COLUMN IF EXISTS docs_url,
  DROP COLUMN IF EXISTS source_url;
```

- [ ] **Step 4: Run DB test**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/db -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/database/migrations/000113_source_catalog_columns.* corpscout/scheduler/internal/db/source_catalog_columns_migration_test.go
git commit -m "feat: add typed source catalog columns"
```

---

### Task 2: Define Embedded Source Declarations

**Files:**
- Create: `corpscout/scheduler/internal/companysources/sourcecatalog/spec.go`
- Create: `corpscout/scheduler/internal/companysources/sourcecatalog/catalog.go`
- Create: `corpscout/scheduler/internal/companysources/sourcecatalog/catalog_test.go`
- Create: `corpscout/scheduler/internal/companysources/sourcecatalog/sources/*.json`

- [ ] **Step 1: Write failing catalog test**

Create `corpscout/scheduler/internal/companysources/sourcecatalog/catalog_test.go`:

```go
package sourcecatalog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadEmbeddedSpecs(t *testing.T) {
	specs, err := LoadEmbedded()
	require.NoError(t, err)
	require.Len(t, specs, 4)

	byRegistryKey := map[string]Spec{}
	for _, spec := range specs {
		require.NoError(t, spec.Validate())
		byRegistryKey[spec.RegistryKey] = spec
	}

	require.Equal(t, "source.ndjson", byRegistryKey["finland/prhytj"].SourceFileName)
	require.Equal(t, "source.json", byRegistryKey["united_states/secedgar"].SourceFileName)
	require.True(t, byRegistryKey["united_states/secedgar"].UserAgentRequired)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/companysources/sourcecatalog -count=1
```

Expected: FAIL because package does not exist.

- [ ] **Step 3: Add spec type**

Create `corpscout/scheduler/internal/companysources/sourcecatalog/spec.go`:

```go
package sourcecatalog

import (
	"strings"

	"github.com/cockroachdb/errors"
)

type Spec struct {
	Name                  string   `json:"name"`
	Country               string   `json:"country"`
	Source                string   `json:"source"`
	RegistryKey           string   `json:"registry_key"`
	DisplayName           string   `json:"display_name"`
	Description           string   `json:"description"`
	SourceGroup           string   `json:"source_group"`
	InputTableName        string   `json:"input_table_name"`
	Enabled               bool     `json:"enabled"`
	AuthRequired          bool     `json:"auth_required"`
	StorageKind           string   `json:"storage_kind"`
	ClickHouseDatabase    string   `json:"clickhouse_database"`
	ClickHouseTablePrefix string   `json:"clickhouse_table_prefix"`
	SourceURL             string   `json:"source_url"`
	DocsURL               string   `json:"docs_url"`
	RawSourceRetention    string   `json:"raw_source_retention"`
	SourceFileName        string   `json:"source_file_name"`
	UserAgentRequired     bool     `json:"user_agent_required"`
	Capabilities          []string `json:"capabilities"`
	RequiresTranslation   bool     `json:"requires_translation"`
}

func (s Spec) Validate() error {
	required := map[string]string{
		"name":                    s.Name,
		"country":                 s.Country,
		"source":                  s.Source,
		"registry_key":            s.RegistryKey,
		"display_name":            s.DisplayName,
		"description":             s.Description,
		"source_group":            s.SourceGroup,
		"input_table_name":        s.InputTableName,
		"storage_kind":            s.StorageKind,
		"clickhouse_database":     s.ClickHouseDatabase,
		"clickhouse_table_prefix": s.ClickHouseTablePrefix,
		"source_url":              s.SourceURL,
		"docs_url":                s.DocsURL,
		"raw_source_retention":    s.RawSourceRetention,
		"source_file_name":        s.SourceFileName,
	}
	for field, value := range required {
		if strings.TrimSpace(value) == "" {
			return errors.Errorf("source spec %s is required", field)
		}
	}
	if s.RegistryKey != s.Country+"/"+s.Source {
		return errors.Errorf("source spec registry key %q must match country/source", s.RegistryKey)
	}
	if s.StorageKind != "clickhouse" {
		return errors.Errorf("source spec storage kind %q is not supported", s.StorageKind)
	}
	if s.SourceFileName != "source.ndjson" && s.SourceFileName != "source.json" {
		return errors.Errorf("source spec source file name %q is not supported", s.SourceFileName)
	}
	return nil
}
```

- [ ] **Step 4: Add embedded loader**

Create `corpscout/scheduler/internal/companysources/sourcecatalog/catalog.go`:

```go
package sourcecatalog

import (
	"embed"
	"encoding/json"
	"sort"

	"github.com/cockroachdb/errors"
)

//go:embed sources/*.json
var embeddedSources embed.FS

func LoadEmbedded() ([]Spec, error) {
	entries, err := embeddedSources.ReadDir("sources")
	if err != nil {
		return nil, errors.Wrap(err, "read embedded source specs")
	}
	specs := make([]Spec, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		payload, err := embeddedSources.ReadFile("sources/" + entry.Name())
		if err != nil {
			return nil, errors.Wrapf(err, "read embedded source spec %s", entry.Name())
		}
		var spec Spec
		if err := json.Unmarshal(payload, &spec); err != nil {
			return nil, errors.Wrapf(err, "decode embedded source spec %s", entry.Name())
		}
		if err := spec.Validate(); err != nil {
			return nil, errors.Wrapf(err, "validate embedded source spec %s", entry.Name())
		}
		specs = append(specs, spec)
	}
	sort.Slice(specs, func(i, j int) bool {
		return specs[i].RegistryKey < specs[j].RegistryKey
	})
	return specs, nil
}
```

- [ ] **Step 5: Add four JSON declarations**

Create the four JSON files listed in the file structure. Use the values from the “Source Declaration Shape” section and adapt these fields:

```json
{
  "name": "united_states_secedgar",
  "country": "united_states",
  "source": "secedgar",
  "registry_key": "united_states/secedgar",
  "display_name": "SEC EDGAR Company Tickers",
  "description": "U.S. public company CIK/ticker map from the SEC EDGAR company_tickers.json file",
  "source_group": "registry",
  "input_table_name": "corpscout_sources.us_secedgar_*",
  "enabled": true,
  "auth_required": false,
  "storage_kind": "clickhouse",
  "clickhouse_database": "corpscout_sources",
  "clickhouse_table_prefix": "us_secedgar",
  "source_url": "https://www.sec.gov/files/company_tickers.json",
  "docs_url": "https://www.sec.gov/os/webmaster-faq#developers",
  "raw_source_retention": "filesystem_run_directory",
  "source_file_name": "source.json",
  "user_agent_required": true,
  "capabilities": ["company_import", "clickhouse", "source_download"],
  "requires_translation": false
}
```

- [ ] **Step 6: Run package test**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/companysources/sourcecatalog -count=1
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/scheduler/internal/companysources/sourcecatalog
git commit -m "feat: add embedded company source catalog"
```

---

### Task 3: Sync Source Catalog Into Postgres On Startup

**Files:**
- Modify: `corpscout/database/queries/sources.sql`
- Regenerate: `corpscout/scheduler/internal/db/gen/sources.sql.go`
- Regenerate: `corpscout/scheduler/internal/db/gen/querier.go`
- Create: `corpscout/scheduler/internal/companysources/sourcecatalog/sync.go`
- Create: `corpscout/scheduler/internal/companysources/sourcecatalog/sync_test.go`
- Modify: `corpscout/scheduler/internal/app/server.go`

- [ ] **Step 1: Add sqlc query test target**

In `corpscout/database/queries/sources.sql`, add:

```sql
-- name: UpsertDataSourceFromCatalog :exec
INSERT INTO data_sources (
  name,
  country,
  source,
  registry_key,
  display_name,
  description,
  source_group,
  input_table_name,
  enabled,
  auth_required,
  storage_kind,
  clickhouse_database,
  clickhouse_table_prefix,
  source_url,
  docs_url,
  raw_source_retention,
  source_file_name,
  user_agent_required,
  capabilities,
  requires_translation,
  config
) VALUES (
  $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
  $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
  '{}'::jsonb
)
ON CONFLICT (name) DO UPDATE SET
  country = EXCLUDED.country,
  source = EXCLUDED.source,
  registry_key = EXCLUDED.registry_key,
  display_name = EXCLUDED.display_name,
  description = EXCLUDED.description,
  source_group = EXCLUDED.source_group,
  input_table_name = EXCLUDED.input_table_name,
  enabled = EXCLUDED.enabled,
  auth_required = EXCLUDED.auth_required,
  storage_kind = EXCLUDED.storage_kind,
  clickhouse_database = EXCLUDED.clickhouse_database,
  clickhouse_table_prefix = EXCLUDED.clickhouse_table_prefix,
  source_url = EXCLUDED.source_url,
  docs_url = EXCLUDED.docs_url,
  raw_source_retention = EXCLUDED.raw_source_retention,
  source_file_name = EXCLUDED.source_file_name,
  user_agent_required = EXCLUDED.user_agent_required,
  capabilities = EXCLUDED.capabilities,
  requires_translation = EXCLUDED.requires_translation,
  config = '{}'::jsonb,
  updated_at = now();

-- name: PruneDataSourcesNotInCatalog :exec
DELETE FROM data_sources
WHERE registry_key <> ALL($1::text[]);
```

- [ ] **Step 2: Regenerate sqlc**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off sqlc generate -f ../database/sqlc.yaml
```

Expected: generated methods `UpsertDataSourceFromCatalog` and `PruneDataSourcesNotInCatalog`.

- [ ] **Step 3: Add sync implementation**

Create `corpscout/scheduler/internal/companysources/sourcecatalog/sync.go`:

```go
package sourcecatalog

import (
	"context"
	"encoding/json"

	"github.com/cockroachdb/errors"
	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

type Store interface {
	UpsertDataSourceFromCatalog(ctx context.Context, arg db.UpsertDataSourceFromCatalogParams) error
	PruneDataSourcesNotInCatalog(ctx context.Context, registryKeys []string) error
}

func Sync(ctx context.Context, store Store, specs []Spec) error {
	registryKeys := make([]string, 0, len(specs))
	for _, spec := range specs {
		if err := spec.Validate(); err != nil {
			return err
		}
		registryKeys = append(registryKeys, spec.RegistryKey)
		if err := store.UpsertDataSourceFromCatalog(ctx, db.UpsertDataSourceFromCatalogParams{
			Name:                  spec.Name,
			Country:               spec.Country,
			Source:                spec.Source,
			RegistryKey:           spec.RegistryKey,
			DisplayName:           &spec.DisplayName,
			Description:           &spec.Description,
			SourceGroup:           spec.SourceGroup,
			InputTableName:        spec.InputTableName,
			Enabled:               spec.Enabled,
			AuthRequired:          spec.AuthRequired,
			StorageKind:           spec.StorageKind,
			ClickhouseDatabase:    spec.ClickHouseDatabase,
			ClickhouseTablePrefix: spec.ClickHouseTablePrefix,
			SourceUrl:             spec.SourceURL,
			DocsUrl:               spec.DocsURL,
			RawSourceRetention:    spec.RawSourceRetention,
			SourceFileName:        spec.SourceFileName,
			UserAgentRequired:     spec.UserAgentRequired,
			Capabilities:          spec.Capabilities,
			RequiresTranslation:   spec.RequiresTranslation,
		}); err != nil {
			return errors.Wrapf(err, "upsert source catalog spec %s", spec.RegistryKey)
		}
	}
	if len(registryKeys) == 0 {
		return errors.New("source catalog must contain at least one source")
	}
	if _, err := json.Marshal(registryKeys); err != nil {
		return errors.Wrap(err, "validate source registry keys")
	}
	return errors.Wrap(store.PruneDataSourcesNotInCatalog(ctx, registryKeys), "prune data sources not in catalog")
}
```

- [ ] **Step 4: Add sync test**

Create `corpscout/scheduler/internal/companysources/sourcecatalog/sync_test.go` with a fake store that records upserts and prunes:

```go
package sourcecatalog

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

type fakeStore struct {
	upserts []db.UpsertDataSourceFromCatalogParams
	pruned  []string
}

func (s *fakeStore) UpsertDataSourceFromCatalog(ctx context.Context, arg db.UpsertDataSourceFromCatalogParams) error {
	s.upserts = append(s.upserts, arg)
	return nil
}

func (s *fakeStore) PruneDataSourcesNotInCatalog(ctx context.Context, registryKeys []string) error {
	s.pruned = append([]string(nil), registryKeys...)
	return nil
}

func TestSyncUpsertsAndPrunesCatalogSources(t *testing.T) {
	specs, err := LoadEmbedded()
	require.NoError(t, err)

	store := &fakeStore{}
	require.NoError(t, Sync(context.Background(), store, specs))

	require.Len(t, store.upserts, 4)
	require.ElementsMatch(t, []string{
		"finland/prhytj",
		"united_states/coloradoentities",
		"united_states/irseobmf",
		"united_states/secedgar",
	}, store.pruned)
	require.Equal(t, "corpscout_sources", store.upserts[0].ClickhouseDatabase)
}
```

- [ ] **Step 5: Wire sync into startup**

In `corpscout/scheduler/internal/app/server.go`, after the `queries := db.New(pool)` line and before handlers are created, add:

```go
sourceSpecs, err := sourcecatalog.LoadEmbedded()
if err != nil {
	return nil, errors.Wrap(err, "load source catalog")
}
if err := sourcecatalog.Sync(ctx, queries, sourceSpecs); err != nil {
	return nil, errors.Wrap(err, "sync source catalog")
}
```

Add import:

```go
"github.com/pulsarpoint/corpscout/scheduler/internal/companysources/sourcecatalog"
```

- [ ] **Step 6: Run backend checks**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/companysources/sourcecatalog ./internal/app ./internal/httpapi -count=1
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/database/queries/sources.sql corpscout/scheduler/internal/db/gen corpscout/scheduler/internal/companysources/sourcecatalog corpscout/scheduler/internal/app/server.go
git commit -m "feat: sync source catalog on scheduler startup"
```

---

### Task 4: Make Source Config UI Read-Only

**Files:**
- Modify: `corpscout/ui/app/components/app/source-detail/ConfigTab.tsx`
- Modify: `corpscout/ui/app/routes/sources_.$name.config.tsx`
- Modify: `corpscout/ui/app/routes/sources_.$name.tsx`
- Modify: `corpscout/ui/app/types/api.ts`

- [ ] **Step 1: Replace editable config component**

Replace `ConfigTab.tsx` with a read-only metadata view:

```tsx
import type { ReactNode } from "react";
import type { DataSource } from "~/types/api";
import { Badge } from "~/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "~/components/ui/card";

interface ConfigTabProps {
  source: DataSource;
}

function Field({ label, value }: { label: string; value: ReactNode }) {
  return (
    <div className="grid gap-1 border-b py-3 last:border-b-0 md:grid-cols-[220px_1fr]">
      <div className="text-sm text-muted-foreground">{label}</div>
      <div className="min-w-0 text-sm">{value}</div>
    </div>
  );
}

function CodeValue({ value }: { value: string }) {
  return <code className="break-all font-mono text-xs">{value}</code>;
}

export function ConfigTab({ source }: ConfigTabProps) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>Source information</CardTitle>
      </CardHeader>
      <CardContent>
        <Field label="Registry key" value={<CodeValue value={source.registry_key} />} />
        <Field label="Country" value={<CodeValue value={source.country} />} />
        <Field label="Source" value={<CodeValue value={source.source} />} />
        <Field label="Input tables" value={<CodeValue value={source.input_table_name} />} />
        <Field label="Storage" value={<Badge variant="outline">{source.storage_kind}</Badge>} />
        <Field label="ClickHouse database" value={<CodeValue value={source.clickhouse_database} />} />
        <Field label="ClickHouse table prefix" value={<CodeValue value={source.clickhouse_table_prefix} />} />
        <Field label="Source URL" value={<CodeValue value={source.source_url} />} />
        <Field label="Documentation" value={<CodeValue value={source.docs_url} />} />
        <Field label="Raw retention" value={<CodeValue value={source.raw_source_retention} />} />
        <Field label="Source file" value={<CodeValue value={source.source_file_name} />} />
        <Field label="Auth required" value={source.auth_required ? "Yes" : "No"} />
        <Field label="User-Agent required" value={source.user_agent_required ? "Yes" : "No"} />
        <Field
          label="Capabilities"
          value={
            <div className="flex flex-wrap gap-1">
              {source.capabilities.map((capability) => (
                <Badge key={capability} variant="outline">{capability}</Badge>
              ))}
            </div>
          }
        />
      </CardContent>
    </Card>
  );
}
```

- [ ] **Step 2: Simplify route**

Update `corpscout/ui/app/routes/sources_.$name.config.tsx`:

```tsx
import { useOutletContext } from "react-router";
import type { SourceDetailContext } from "~/routes/sources_.$name";
import { ConfigTab } from "~/components/app/source-detail/ConfigTab";

export default function SourceConfigPage() {
  const { source } = useOutletContext<SourceDetailContext>();
  return <ConfigTab source={source} />;
}
```

- [ ] **Step 3: Remove config mutation from source detail context**

In `corpscout/ui/app/routes/sources_.$name.tsx`:

Remove `saving`, `onPatch`, and `handlePatch` from `SourceDetailContext` if no route uses them after this task.

The context should become:

```ts
export interface SourceDetailContext {
  source: DataSource;
  triggering: boolean;
  onTrigger: () => Promise<void>;
}
```

If `triggering` is only legacy code after source pruning, keep it for now and remove it in the later source-actions workflow task.

- [ ] **Step 4: Extend frontend source type**

In `corpscout/ui/app/types/api.ts`, add:

```ts
source_url: string;
docs_url: string;
raw_source_retention: string;
source_file_name: "source.ndjson" | "source.json";
user_agent_required: boolean;
```

Remove direct UI dependency on `config` from the source detail config page. Keep the API field temporarily only if other code still compiles against it.

- [ ] **Step 5: Run UI typecheck**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/ui
npm run typecheck
```

Expected: PASS.

- [ ] **Step 6: Browser verify**

Run the app and open:

```text
http://localhost:8094/sources/finland_prhytj/config
```

Expected:
- Page shows source metadata.
- No “Add field”.
- No “Save”.
- No editable inputs.

- [ ] **Step 7: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/ui/app/components/app/source-detail/ConfigTab.tsx corpscout/ui/app/routes/sources_.$name.config.tsx corpscout/ui/app/routes/sources_.$name.tsx corpscout/ui/app/types/api.ts
git commit -m "feat: make source config read only"
```

---

### Task 5: Make API Source Metadata Read-Only

**Files:**
- Modify: `corpscout/scheduler/internal/httpapi/source_patch.go`
- Modify: `corpscout/scheduler/internal/httpapi/source_read.go`
- Modify: `corpscout/scheduler/internal/httpapi/sources_test.go`
- Modify: `corpscout/scheduler/internal/httpapi/testhelpers_test.go`

- [ ] **Step 1: Add failing API test for config patch rejection**

In `corpscout/scheduler/internal/httpapi/sources_test.go`, add:

```go
func TestPatchSourceRejectsConfigMutation(t *testing.T) {
	h := newTestHandlers(t)
	req := httptest.NewRequest(http.MethodPatch, "/api/v1/sources/finland_prhytj", strings.NewReader(`{"config":{"source_url":"https://example.invalid"}}`))
	rec := httptest.NewRecorder()

	h.handlePatchSource(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Contains(t, rec.Body.String(), "source metadata is read-only")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/httpapi -run TestPatchSourceRejectsConfigMutation -count=1
```

Expected: FAIL because config patch is currently accepted.

- [ ] **Step 3: Reject source metadata mutation**

In `source_patch.go`, remove `Config` from the accepted patch shape. Decode into a struct with no metadata fields:

```go
type patchSourceRequest struct {
	Enabled *bool `json:"enabled,omitempty"`
}
```

Set `decoder.DisallowUnknownFields()` so `config`, `country`, `source_url`, and other source metadata fields are rejected with a safe message:

```go
if err := decoder.Decode(&req); err != nil {
	writeError(w, http.StatusBadRequest, "source metadata is read-only")
	return
}
```

If `enabled` is also considered source-owned, replace the handler body with:

```go
writeError(w, http.StatusBadRequest, "source metadata is read-only")
```

Use the stricter version if no caller still needs source enable toggling.

- [ ] **Step 4: Extend source read response**

In `source_read.go`, add response fields:

```go
SourceURL          string `json:"source_url"`
DocsURL            string `json:"docs_url"`
RawSourceRetention string `json:"raw_source_retention"`
SourceFileName     string `json:"source_file_name"`
UserAgentRequired  bool   `json:"user_agent_required"`
```

Map from sqlc rows into these fields.

- [ ] **Step 5: Run HTTP tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/httpapi -count=1
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/scheduler/internal/httpapi/source_patch.go corpscout/scheduler/internal/httpapi/source_read.go corpscout/scheduler/internal/httpapi/sources_test.go corpscout/scheduler/internal/httpapi/testhelpers_test.go
git commit -m "feat: expose source metadata as read only"
```

---

### Task 6: End-To-End Verification

**Files:**
- No source file edits unless verification reveals a defect.

- [ ] **Step 1: Run migrations**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
make migrate-up
```

Expected: migration `113/u source_catalog_columns` applies.

- [ ] **Step 2: Run backend tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/db ./internal/companysources/... ./internal/httpapi ./internal/app -count=1
```

Expected: PASS.

- [ ] **Step 3: Run frontend checks**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/ui
npm run typecheck
npm run build
```

Expected: both commands exit 0.

- [ ] **Step 4: Rebuild local app**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
docker compose up -d --build scheduler ui
```

Expected: `scheduler` and `ui` containers are up.

- [ ] **Step 5: Verify API source fields**

Run:

```bash
curl -sS http://localhost:8094/api/v1/sources/finland_prhytj
```

Expected JSON contains:

```json
{
  "registry_key": "finland/prhytj",
  "source_url": "https://avoindata.prh.fi/opendata-ytj-api/v3/companies",
  "source_file_name": "source.ndjson",
  "user_agent_required": false
}
```

- [ ] **Step 6: Verify UI read-only behavior**

Open:

```text
http://localhost:8094/sources/finland_prhytj/config
```

Expected:
- Source metadata is visible.
- No editable config rows.
- No add/reset/save buttons.

---

## Self-Review

- Requirement covered: source information read-only in UI.
  - Task 4 removes editable controls and displays metadata only.
- Requirement covered: source handler/source code owns source information.
  - Task 2 adds checked-in per-source JSON declarations.
  - Task 3 syncs those declarations on scheduler startup.
- Requirement covered: JSON declaration values go into specific columns, not a JSON blob.
  - Task 1 adds typed columns.
  - Task 3 upserts each source spec field into explicit columns and sets `config = '{}'::jsonb`.
- Requirement covered: existing old source rows do not come back.
  - Task 3 prunes `data_sources` rows not present in embedded catalog.
- No placeholders remain.
