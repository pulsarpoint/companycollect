# Finland PRH YTJ Postgres Storage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Postgres storage for Finland PRH YTJ v3 source metadata, download audit metadata, and raw company records under the new `countrydata_finland_prh_ytj` schema.

**Architecture:** Keep `corpscout/countrydata` independent from Corpscout scheduler and sqlc. The Corpscout database owns source-specific storage in `countrydata_finland_prh_ytj`, while `scheduler/internal/countrydata` provides the concrete DB-backed metadata/store adapter that plugs into the existing PRH YTJ `Download`, `Process`, and `Store` methods.

**Tech Stack:** PostgreSQL migrations, JSONB, pgcrypto UUIDs, sqlc, pgx v5, Go 1.26.1, `log/slog`, `github.com/cockroachdb/errors`.

---

## Scope

This plan implements only Finland PRH YTJ raw source storage. It does not migrate or remove existing `france_workflow`, `se_workflow`, `cvr_workflow`, or `ariregister_workflow` schemas. Those can be unified later using the same `countrydata_{country}_{source}` schema pattern.

## File Structure

- Modify: `corpscout/countrydata/finland/prhytj/process.go`
  - Set `CompanyRecord.RawPayload` and `CompanyRecord.PayloadHash` for every successfully decoded snapshot line.
- Modify: `corpscout/countrydata/finland/prhytj/process_test.go`
  - Verify process preserves the exact raw JSON line and computes a SHA-256 hash.
- Create: `corpscout/database/migrations/000105_finland_prh_ytj_countrydata_storage.up.sql`
  - Create schema `countrydata_finland_prh_ytj`.
  - Create source registry, download audit, and raw record tables.
  - Seed the local schema source row and global `data_sources` row.
- Create: `corpscout/database/migrations/000105_finland_prh_ytj_countrydata_storage.down.sql`
  - Remove the global `data_sources` row and drop the owned schema.
- Create: `corpscout/database/queries/countrydata_finland_prh_ytj.sql`
  - sqlc queries for source lookup/upsert, download audit insertion, raw record current lookup, supersede, and upsert.
- Modify generated files under `corpscout/scheduler/internal/db/gen/`
  - Regenerate with sqlc.
- Create: `corpscout/scheduler/internal/db/finland_prh_ytj_countrydata_storage_migration_test.go`
  - String-shape migration tests matching existing migration test style.
- Create: `corpscout/scheduler/internal/countrydata/finland_prhytj_db_store.go`
  - Concrete DB adapter that implements `countryimport.MetadataStore` and provides `StoreCompanies`.
- Create: `corpscout/scheduler/internal/countrydata/finland_prhytj_db_store_test.go`
  - Conversion tests and optional DB round-trip tests gated by `CORPSCOUT_TEST_DATABASE_URL`.
- Modify: `corpscout/scheduler/internal/countrydata/finland_prhytj.go`
  - Allow callers to provide a typed PRH YTJ store function.
- Modify: `corpscout/scheduler/internal/countrydata/finland_prhytj_test.go`
  - Verify importer uses the injected typed store function.
- Create: `corpscout/scheduler/cmd/finland-prhytj-sync/main.go`
  - One-shot Corpscout DB-backed sync command for manual full-flow testing after migrations.
- Create: `corpscout/scheduler/cmd/finland-prhytj-sync/main_test.go`
  - CLI parsing and configuration tests for the sync command.

---

### Task 1: Preserve Raw Payload And Per-Record Hash During PRH Processing

**Files:**
- Modify: `corpscout/countrydata/finland/prhytj/process.go`
- Modify: `corpscout/countrydata/finland/prhytj/process_test.go`

- [ ] **Step 1: Write the failing process test**

Add this test to `corpscout/countrydata/finland/prhytj/process_test.go`:

```go
func TestProcessSetsRawPayloadAndPayloadHash(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	snapshotPath := filepath.Join(dir, "snapshot.ndjson")
	line := `{"businessId":{"value":"0100002-9"},"names":[{"name":"Example Oy","type":"1"}],"tradeRegisterStatus":"1","status":"1"}`
	if err := os.WriteFile(snapshotPath, []byte(line+"\n"), 0o600); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}

	var stored []CompanyRecord
	source := NewSource(Config{})
	source.StoreFunc = func(ctx context.Context, records []CompanyRecord) (countryimport.StoreResult, error) {
		stored = append(stored, records...)
		return countryimport.StoreResult{
			RecordsReceived: int64(len(records)),
			RecordsStored:   int64(len(records)),
		}, nil
	}

	result, err := source.Process(ctx, countryimport.ProcessOptions{
		SnapshotPath: snapshotPath,
		ChunkSize:    1,
	})
	if err != nil {
		t.Fatalf("process snapshot: %v", err)
	}
	if result.RecordsStored != 1 {
		t.Fatalf("expected 1 stored record, got %d", result.RecordsStored)
	}
	if len(stored) != 1 {
		t.Fatalf("expected one stored record, got %d", len(stored))
	}
	if got := string(stored[0].RawPayload); got != line {
		t.Fatalf("raw payload mismatch:\nwant %s\ngot  %s", line, got)
	}
	sum := sha256.Sum256([]byte(line))
	if want := hex.EncodeToString(sum[:]); stored[0].PayloadHash != want {
		t.Fatalf("payload hash mismatch: want %s got %s", want, stored[0].PayloadHash)
	}
}
```

Add imports if missing:

```go
import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	countryimport "github.com/pulsarpoint/corpscout/countrydata/import"
)
```

- [ ] **Step 2: Run the test and verify it fails**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/countrydata
GOWORK=off go test ./finland/prhytj -run TestProcessSetsRawPayloadAndPayloadHash -count=1 -v
```

Expected: FAIL because `RawPayload` and `PayloadHash` are empty on processed records.

- [ ] **Step 3: Implement raw payload and hash preservation**

Modify `corpscout/countrydata/finland/prhytj/process.go`.

Add imports:

```go
import (
	"crypto/sha256"
	"encoding/hex"
)
```

Inside the scan loop, copy scanner bytes before unmarshalling and set both fields after decode:

```go
rawLine := append([]byte(nil), scanner.Bytes()...)

var record CompanyRecord
if err := json.Unmarshal(rawLine, &record); err != nil {
	result.DecodeErrors++
	slog.WarnContext(ctx, "decode PRH YTJ snapshot line",
		"source", SourceSlug,
		"line", lineNumber,
		"error", err,
	)
	continue
}

record.RawPayload = rawLine
sum := sha256.Sum256(rawLine)
record.PayloadHash = hex.EncodeToString(sum[:])
```

- [ ] **Step 4: Run PRH process tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/countrydata
GOWORK=off go test ./finland/prhytj -run 'TestProcess' -count=1 -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add corpscout/countrydata/finland/prhytj/process.go corpscout/countrydata/finland/prhytj/process_test.go
git commit -m "feat: preserve prh ytj raw record hashes"
```

---

### Task 2: Add Migration Shape Tests For Finland Countrydata Storage

**Files:**
- Create: `corpscout/scheduler/internal/db/finland_prh_ytj_countrydata_storage_migration_test.go`

- [ ] **Step 1: Write the failing migration tests**

Create `corpscout/scheduler/internal/db/finland_prh_ytj_countrydata_storage_migration_test.go`:

```go
package db

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFinlandPRHYTJCountrydataStorageMigrationDefinesSourceSchema(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000105_finland_prh_ytj_countrydata_storage.up.sql")
	require.NoError(t, err)
	sql := string(body)

	required := []string{
		"CREATE SCHEMA IF NOT EXISTS countrydata_finland_prh_ytj",
		"CREATE TABLE countrydata_finland_prh_ytj.sources",
		"CREATE TABLE countrydata_finland_prh_ytj.download_runs",
		"CREATE TABLE countrydata_finland_prh_ytj.raw_records",
		"source_identity TEXT NOT NULL UNIQUE",
		"supports_incremental BOOLEAN NOT NULL DEFAULT false",
		"snapshot_sha256 TEXT",
		"bytes_downloaded BIGINT",
		"pages_downloaded INTEGER",
		"business_id TEXT NOT NULL",
		"raw_payload JSONB NOT NULL",
		"payload_hash TEXT NOT NULL",
		"UNIQUE (business_id, payload_hash)",
		"uq_countrydata_finland_prh_ytj_raw_current_business_id",
		"jsonb_typeof(raw_payload) = 'object'",
		"'finland_prh_ytj_v3'",
		"'countrydata_finland_prh_ytj.raw_records'",
	}

	for _, needle := range required {
		require.Contains(t, sql, needle)
	}
}

func TestFinlandPRHYTJCountrydataStorageDownMigrationDropsOwnedSchema(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000105_finland_prh_ytj_countrydata_storage.down.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "DELETE FROM data_sources")
	require.Contains(t, sql, "WHERE name = 'finland_prh_ytj_v3'")
	require.Contains(t, sql, "DROP SCHEMA IF EXISTS countrydata_finland_prh_ytj CASCADE")
}
```

- [ ] **Step 2: Run the migration tests and verify they fail**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/db -run TestFinlandPRHYTJCountrydataStorage -count=1 -v
```

Expected: FAIL because migration files do not exist.

- [ ] **Step 3: Commit the failing tests**

```bash
git add corpscout/scheduler/internal/db/finland_prh_ytj_countrydata_storage_migration_test.go
git commit -m "test: define finland prh ytj storage migration contract"
```

---

### Task 3: Create Finland PRH YTJ Countrydata Storage Migration

**Files:**
- Create: `corpscout/database/migrations/000105_finland_prh_ytj_countrydata_storage.up.sql`
- Create: `corpscout/database/migrations/000105_finland_prh_ytj_countrydata_storage.down.sql`

- [ ] **Step 1: Create the up migration**

Create `corpscout/database/migrations/000105_finland_prh_ytj_countrydata_storage.up.sql`:

```sql
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE SCHEMA IF NOT EXISTS countrydata_finland_prh_ytj;

CREATE TABLE countrydata_finland_prh_ytj.sources (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source_identity TEXT NOT NULL UNIQUE,
  source_slug TEXT NOT NULL,
  source_name TEXT NOT NULL,
  source_type TEXT NOT NULL,
  country_slug TEXT NOT NULL DEFAULT 'finland',
  country_iso2 TEXT NOT NULL DEFAULT 'FI',
  organization TEXT NOT NULL,
  base_url TEXT,
  access_mode TEXT NOT NULL,
  license TEXT,
  attribution TEXT,
  supports_incremental BOOLEAN NOT NULL DEFAULT false,
  incremental_mode TEXT,
  input_table_name TEXT NOT NULL,
  config JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_countrydata_finland_prh_ytj_sources_config_object CHECK (jsonb_typeof(config) = 'object'),
  CONSTRAINT chk_countrydata_finland_prh_ytj_sources_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  CONSTRAINT chk_countrydata_finland_prh_ytj_sources_incremental CHECK (
    supports_incremental OR incremental_mode IS NULL
  )
);

CREATE TABLE countrydata_finland_prh_ytj.download_runs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source_id UUID NOT NULL REFERENCES countrydata_finland_prh_ytj.sources(id) ON DELETE RESTRICT,
  status TEXT NOT NULL DEFAULT 'succeeded',
  source_url TEXT,
  snapshot_path TEXT,
  snapshot_sha256 TEXT,
  bytes_downloaded BIGINT,
  records_seen BIGINT NOT NULL DEFAULT 0,
  records_processed BIGINT NOT NULL DEFAULT 0,
  records_stored BIGINT NOT NULL DEFAULT 0,
  decode_errors BIGINT NOT NULL DEFAULT 0,
  pages_downloaded INTEGER,
  first_page INTEGER,
  last_page INTEGER,
  total_results_reported BIGINT,
  started_at TIMESTAMPTZ NOT NULL,
  finished_at TIMESTAMPTZ,
  duration_ms BIGINT,
  error TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_countrydata_finland_prh_ytj_download_runs_status CHECK (
    status IN ('running', 'succeeded', 'failed', 'cancelled')
  ),
  CONSTRAINT chk_countrydata_finland_prh_ytj_download_runs_counts CHECK (
    records_seen >= 0
    AND records_processed >= 0
    AND records_stored >= 0
    AND decode_errors >= 0
    AND (bytes_downloaded IS NULL OR bytes_downloaded >= 0)
    AND (pages_downloaded IS NULL OR pages_downloaded >= 0)
    AND (duration_ms IS NULL OR duration_ms >= 0)
  ),
  CONSTRAINT chk_countrydata_finland_prh_ytj_download_runs_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE TABLE countrydata_finland_prh_ytj.raw_records (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  download_run_id UUID REFERENCES countrydata_finland_prh_ytj.download_runs(id) ON DELETE SET NULL,
  source_native_id TEXT NOT NULL,
  business_id TEXT NOT NULL,
  business_id_digits TEXT,
  vat_id TEXT,
  euid TEXT,
  legal_name TEXT,
  trade_register_status TEXT,
  status TEXT,
  registration_date DATE,
  end_date DATE,
  last_modified TIMESTAMPTZ,
  raw_payload JSONB NOT NULL,
  payload_hash TEXT NOT NULL,
  is_current BOOLEAN NOT NULL DEFAULT true,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_countrydata_finland_prh_ytj_raw_source_native CHECK (source_native_id = business_id),
  CONSTRAINT chk_countrydata_finland_prh_ytj_raw_payload_object CHECK (jsonb_typeof(raw_payload) = 'object'),
  CONSTRAINT chk_countrydata_finland_prh_ytj_raw_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  UNIQUE (business_id, payload_hash)
);

CREATE INDEX idx_countrydata_finland_prh_ytj_download_runs_source_started
  ON countrydata_finland_prh_ytj.download_runs (source_id, started_at DESC);

CREATE INDEX idx_countrydata_finland_prh_ytj_download_runs_hash
  ON countrydata_finland_prh_ytj.download_runs (snapshot_sha256)
  WHERE snapshot_sha256 IS NOT NULL;

CREATE UNIQUE INDEX uq_countrydata_finland_prh_ytj_download_runs_success_hash
  ON countrydata_finland_prh_ytj.download_runs (source_id, snapshot_sha256)
  WHERE status = 'succeeded' AND snapshot_sha256 IS NOT NULL;

CREATE INDEX idx_countrydata_finland_prh_ytj_raw_business_id
  ON countrydata_finland_prh_ytj.raw_records (business_id);

CREATE INDEX idx_countrydata_finland_prh_ytj_raw_hash
  ON countrydata_finland_prh_ytj.raw_records (payload_hash);

CREATE INDEX idx_countrydata_finland_prh_ytj_raw_euid
  ON countrydata_finland_prh_ytj.raw_records (euid)
  WHERE euid IS NOT NULL;

CREATE INDEX idx_countrydata_finland_prh_ytj_raw_legal_name
  ON countrydata_finland_prh_ytj.raw_records (legal_name)
  WHERE legal_name IS NOT NULL;

CREATE UNIQUE INDEX uq_countrydata_finland_prh_ytj_raw_current_business_id
  ON countrydata_finland_prh_ytj.raw_records (business_id)
  WHERE is_current;

INSERT INTO countrydata_finland_prh_ytj.sources (
  source_identity,
  source_slug,
  source_name,
  source_type,
  country_slug,
  country_iso2,
  organization,
  base_url,
  access_mode,
  license,
  attribution,
  supports_incremental,
  incremental_mode,
  input_table_name,
  config,
  metadata
) VALUES (
  'finland_prh_ytj_v3',
  'prh_ytj_v3',
  'PRH Open Data YTJ API v3 companies',
  'official_registry_api',
  'finland',
  'FI',
  'Finnish Patent and Registration Office (PRH) and Finnish Tax Administration',
  'https://avoindata.prh.fi/opendata-ytj-api/v3/companies',
  'paginated_json_api',
  'CC-BY-4.0',
  'Finnish Patent and Registration Office (PRH) and Finnish Tax Administration',
  false,
  NULL,
  'countrydata_finland_prh_ytj.raw_records',
  jsonb_build_object(
    'page_param', 'page',
    'page_start', 1,
    'page_size', 100,
    'total_results_param', 'totalResults',
    'record_path', 'companies',
    'snapshot_strategy', 'ndjson_one_record_per_line',
    'supports_diff', false
  ),
  jsonb_build_object(
    'source_package', 'prhytj',
    'env_prefix', 'PRH_YTJ',
    'countrydata_module', 'github.com/pulsarpoint/corpscout/countrydata/finland/prhytj'
  )
)
ON CONFLICT (source_identity) DO UPDATE SET
  source_slug = EXCLUDED.source_slug,
  source_name = EXCLUDED.source_name,
  source_type = EXCLUDED.source_type,
  organization = EXCLUDED.organization,
  base_url = EXCLUDED.base_url,
  access_mode = EXCLUDED.access_mode,
  license = EXCLUDED.license,
  attribution = EXCLUDED.attribution,
  supports_incremental = EXCLUDED.supports_incremental,
  incremental_mode = EXCLUDED.incremental_mode,
  input_table_name = EXCLUDED.input_table_name,
  config = EXCLUDED.config,
  metadata = EXCLUDED.metadata,
  updated_at = now();

INSERT INTO data_sources (
  name,
  display_name,
  description,
  source_group,
  input_table_name,
  enabled,
  schedule_enabled,
  schedule_kind,
  schedule_expression,
  requires_translation,
  capabilities,
  config,
  country_id
)
VALUES (
  'finland_prh_ytj_v3',
  'Finland PRH YTJ v3',
  'Finnish PRH Open Data YTJ API v3 company registry source.',
  'registry',
  'countrydata_finland_prh_ytj.raw_records',
  false,
  false,
  'manual',
  NULL,
  false,
  '{company_name,org_number,legal_form,status,locations,industries,website}'::text[],
  jsonb_build_object(
    'api_url', 'https://avoindata.prh.fi/opendata-ytj-api/v3/companies',
    'docs_url', 'https://avoindata.prh.fi/en',
    'protocol', 'PRH YTJ v3 paginated JSON API',
    'page_size', 100,
    'supports_diff', false,
    'auth_env', NULL,
    'fields', jsonb_build_array(
      'businessId',
      'euId',
      'names',
      'mainBusinessLine',
      'companyForms',
      'companySituations',
      'registeredEntries',
      'addresses',
      'tradeRegisterStatus',
      'status',
      'registrationDate',
      'endDate',
      'lastModified'
    ),
    'target_table', 'countrydata_finland_prh_ytj.raw_records',
    'notes', 'Initial countrydata storage uses full paginated snapshots. PRH YTJ v3 does not expose a documented diff-only pull option.'
  ),
  (SELECT id FROM countries WHERE iso_alpha2 = 'FI')
)
ON CONFLICT (name) DO UPDATE SET
  display_name = EXCLUDED.display_name,
  description = EXCLUDED.description,
  source_group = EXCLUDED.source_group,
  input_table_name = EXCLUDED.input_table_name,
  enabled = EXCLUDED.enabled,
  schedule_enabled = EXCLUDED.schedule_enabled,
  schedule_kind = EXCLUDED.schedule_kind,
  schedule_expression = EXCLUDED.schedule_expression,
  requires_translation = EXCLUDED.requires_translation,
  capabilities = EXCLUDED.capabilities,
  config = EXCLUDED.config,
  country_id = EXCLUDED.country_id,
  updated_at = now();

GRANT USAGE ON SCHEMA countrydata_finland_prh_ytj TO corpscout_anon;
GRANT SELECT ON ALL TABLES IN SCHEMA countrydata_finland_prh_ytj TO corpscout_anon;
```

- [ ] **Step 2: Create the down migration**

Create `corpscout/database/migrations/000105_finland_prh_ytj_countrydata_storage.down.sql`:

```sql
DELETE FROM data_sources
WHERE name = 'finland_prh_ytj_v3';

DROP SCHEMA IF EXISTS countrydata_finland_prh_ytj CASCADE;
```

- [ ] **Step 3: Run migration shape tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/db -run TestFinlandPRHYTJCountrydataStorage -count=1 -v
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add corpscout/database/migrations/000105_finland_prh_ytj_countrydata_storage.*.sql corpscout/scheduler/internal/db/finland_prh_ytj_countrydata_storage_migration_test.go
git commit -m "feat: add finland prh ytj countrydata storage schema"
```

---

### Task 4: Add sqlc Queries For Finland PRH YTJ Storage

**Files:**
- Create: `corpscout/database/queries/countrydata_finland_prh_ytj.sql`
- Modify generated files under: `corpscout/scheduler/internal/db/gen/`

- [ ] **Step 1: Create the query file**

Create `corpscout/database/queries/countrydata_finland_prh_ytj.sql`:

```sql
-- name: GetFinlandPRHYTJSource :one
SELECT *
FROM countrydata_finland_prh_ytj.sources
WHERE source_identity = sqlc.arg('source_identity')::text;

-- name: UpsertFinlandPRHYTJSource :one
INSERT INTO countrydata_finland_prh_ytj.sources (
  source_identity,
  source_slug,
  source_name,
  source_type,
  organization,
  base_url,
  access_mode,
  license,
  attribution,
  supports_incremental,
  incremental_mode,
  input_table_name,
  config,
  metadata
) VALUES (
  sqlc.arg('source_identity')::text,
  sqlc.arg('source_slug')::text,
  sqlc.arg('source_name')::text,
  sqlc.arg('source_type')::text,
  sqlc.arg('organization')::text,
  sqlc.narg('base_url')::text,
  sqlc.arg('access_mode')::text,
  sqlc.narg('license')::text,
  sqlc.narg('attribution')::text,
  sqlc.arg('supports_incremental')::boolean,
  sqlc.narg('incremental_mode')::text,
  sqlc.arg('input_table_name')::text,
  COALESCE(sqlc.narg('config')::jsonb, '{}'::jsonb),
  COALESCE(sqlc.narg('metadata')::jsonb, '{}'::jsonb)
)
ON CONFLICT (source_identity) DO UPDATE SET
  source_slug = EXCLUDED.source_slug,
  source_name = EXCLUDED.source_name,
  source_type = EXCLUDED.source_type,
  organization = EXCLUDED.organization,
  base_url = EXCLUDED.base_url,
  access_mode = EXCLUDED.access_mode,
  license = EXCLUDED.license,
  attribution = EXCLUDED.attribution,
  supports_incremental = EXCLUDED.supports_incremental,
  incremental_mode = EXCLUDED.incremental_mode,
  input_table_name = EXCLUDED.input_table_name,
  config = EXCLUDED.config,
  metadata = EXCLUDED.metadata,
  updated_at = now()
RETURNING id;

-- name: RecordFinlandPRHYTJDownloadRun :one
INSERT INTO countrydata_finland_prh_ytj.download_runs (
  source_id,
  status,
  source_url,
  snapshot_path,
  snapshot_sha256,
  bytes_downloaded,
  records_seen,
  pages_downloaded,
  first_page,
  last_page,
  total_results_reported,
  started_at,
  finished_at,
  duration_ms,
  error,
  metadata
) VALUES (
  sqlc.arg('source_id')::uuid,
  sqlc.arg('status')::text,
  sqlc.narg('source_url')::text,
  sqlc.narg('snapshot_path')::text,
  sqlc.narg('snapshot_sha256')::text,
  sqlc.narg('bytes_downloaded')::bigint,
  sqlc.arg('records_seen')::bigint,
  sqlc.narg('pages_downloaded')::integer,
  sqlc.narg('first_page')::integer,
  sqlc.narg('last_page')::integer,
  sqlc.narg('total_results_reported')::bigint,
  sqlc.arg('started_at')::timestamptz,
  sqlc.narg('finished_at')::timestamptz,
  sqlc.narg('duration_ms')::bigint,
  sqlc.narg('error')::text,
  COALESCE(sqlc.narg('metadata')::jsonb, '{}'::jsonb)
)
ON CONFLICT (source_id, snapshot_sha256) WHERE status = 'succeeded' AND snapshot_sha256 IS NOT NULL
DO UPDATE SET
  source_url = EXCLUDED.source_url,
  snapshot_path = EXCLUDED.snapshot_path,
  bytes_downloaded = EXCLUDED.bytes_downloaded,
  records_seen = EXCLUDED.records_seen,
  pages_downloaded = EXCLUDED.pages_downloaded,
  first_page = EXCLUDED.first_page,
  last_page = EXCLUDED.last_page,
  total_results_reported = EXCLUDED.total_results_reported,
  started_at = EXCLUDED.started_at,
  finished_at = EXCLUDED.finished_at,
  duration_ms = EXCLUDED.duration_ms,
  error = EXCLUDED.error,
  metadata = EXCLUDED.metadata,
  updated_at = now()
RETURNING id;

-- name: UpdateFinlandPRHYTJDownloadRunProcessStats :exec
UPDATE countrydata_finland_prh_ytj.download_runs
SET
  records_processed = sqlc.arg('records_processed')::bigint,
  records_stored = sqlc.arg('records_stored')::bigint,
  decode_errors = sqlc.arg('decode_errors')::bigint,
  metadata = metadata || COALESCE(sqlc.narg('metadata')::jsonb, '{}'::jsonb),
  updated_at = now()
WHERE id = sqlc.arg('id')::uuid;

-- name: GetCurrentFinlandPRHYTJRawRecord :one
SELECT id, payload_hash
FROM countrydata_finland_prh_ytj.raw_records
WHERE business_id = sqlc.arg('business_id')::text
  AND is_current = true;

-- name: SupersedeCurrentFinlandPRHYTJRawRecord :exec
UPDATE countrydata_finland_prh_ytj.raw_records
SET
  is_current = false,
  last_seen_at = now(),
  updated_at = now()
WHERE business_id = sqlc.arg('business_id')::text
  AND payload_hash <> sqlc.arg('payload_hash')::text
  AND is_current = true;

-- name: UpsertFinlandPRHYTJRawRecord :one
WITH upserted AS (
  INSERT INTO countrydata_finland_prh_ytj.raw_records (
    download_run_id,
    source_native_id,
    business_id,
    business_id_digits,
    vat_id,
    euid,
    legal_name,
    trade_register_status,
    status,
    registration_date,
    end_date,
    last_modified,
    raw_payload,
    payload_hash,
    is_current,
    metadata
  ) VALUES (
    sqlc.narg('download_run_id')::uuid,
    sqlc.arg('source_native_id')::text,
    sqlc.arg('business_id')::text,
    sqlc.narg('business_id_digits')::text,
    sqlc.narg('vat_id')::text,
    sqlc.narg('euid')::text,
    sqlc.narg('legal_name')::text,
    sqlc.narg('trade_register_status')::text,
    sqlc.narg('status')::text,
    sqlc.narg('registration_date')::date,
    sqlc.narg('end_date')::date,
    sqlc.narg('last_modified')::timestamptz,
    sqlc.arg('raw_payload')::jsonb,
    sqlc.arg('payload_hash')::text,
    true,
    COALESCE(sqlc.narg('metadata')::jsonb, '{}'::jsonb)
  )
  ON CONFLICT (business_id, payload_hash) DO UPDATE
  SET
    download_run_id = EXCLUDED.download_run_id,
    business_id_digits = EXCLUDED.business_id_digits,
    vat_id = EXCLUDED.vat_id,
    euid = EXCLUDED.euid,
    legal_name = EXCLUDED.legal_name,
    trade_register_status = EXCLUDED.trade_register_status,
    status = EXCLUDED.status,
    registration_date = EXCLUDED.registration_date,
    end_date = EXCLUDED.end_date,
    last_modified = EXCLUDED.last_modified,
    raw_payload = EXCLUDED.raw_payload,
    is_current = true,
    last_seen_at = now(),
    metadata = EXCLUDED.metadata,
    updated_at = now()
  RETURNING id
)
SELECT
  id,
  1::integer AS rows_written
FROM upserted;
```

- [ ] **Step 2: Generate sqlc code**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off sqlc generate -f ../database/sqlc.yaml
```

Expected: command exits `0` and updates `internal/db/gen`.

- [ ] **Step 3: Run a compile check for generated queries**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/db -run TestFinlandPRHYTJCountrydataStorage -count=1 -v
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add corpscout/database/queries/countrydata_finland_prh_ytj.sql corpscout/scheduler/internal/db/gen
git commit -m "feat: add finland prh ytj storage queries"
```

---

### Task 5: Add Scheduler DB Store For PRH YTJ Records And Metadata

**Files:**
- Create: `corpscout/scheduler/internal/countrydata/finland_prhytj_db_store.go`
- Create: `corpscout/scheduler/internal/countrydata/finland_prhytj_db_store_test.go`

- [ ] **Step 1: Write conversion tests**

Create `corpscout/scheduler/internal/countrydata/finland_prhytj_db_store_test.go`:

```go
package countrydata

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pulsarpoint/corpscout/countrydata/finland/prhytj"
	countryimport "github.com/pulsarpoint/corpscout/countrydata/import"
)

func TestFinlandPRHYTJRawRecordParamsUsesNaturalKeyHashAndDerivedFields(t *testing.T) {
	raw := []byte(`{"businessId":{"value":"0100002-9"},"euId":{"value":"FIFPRO.0100002-9"},"names":[{"name":"Example Oy","type":"1","registrationDate":"2024-01-01"}],"website":{"url":"example.fi"},"tradeRegisterStatus":"1","status":"1","registrationDate":"2024-01-01","lastModified":"2024-01-02T03:04:05Z"}`)
	sum := sha256.Sum256(raw)

	record := prhytj.CompanyRecord{
		BusinessID:          prhytj.Identifier{Value: "0100002-9"},
		EUID:                &prhytj.Identifier{Value: "FIFPRO.0100002-9"},
		Names:               []prhytj.Name{{Name: "Example Oy", Type: "1", RegistrationDate: "2024-01-01"}},
		Website:             prhytj.Website{URL: "example.fi"},
		TradeRegisterStatus: "1",
		Status:              "1",
		RegistrationDate:    "2024-01-01",
		LastModified:        "2024-01-02T03:04:05Z",
		RawPayload:          raw,
		PayloadHash:         hex.EncodeToString(sum[:]),
	}

	params, err := finlandPRHYTJRawRecordParams(record, nilDownloadRunID())
	require.NoError(t, err)
	require.Equal(t, "0100002-9", params.SourceNativeID)
	require.Equal(t, "0100002-9", params.BusinessID)
	require.Equal(t, "01000029", *params.BusinessIDDigits)
	require.Equal(t, "FI01000029", *params.VatID)
	require.Equal(t, "FIFPRO.0100002-9", *params.Euid)
	require.Equal(t, "Example Oy", *params.LegalName)
	require.Equal(t, "1", *params.TradeRegisterStatus)
	require.Equal(t, "1", *params.Status)
	require.JSONEq(t, string(raw), string(params.RawPayload))
	require.Equal(t, hex.EncodeToString(sum[:]), params.PayloadHash)
}

func TestFinlandPRHYTJStoreCompaniesRejectsMissingBusinessID(t *testing.T) {
	store := &FinlandPRHYTJDBStore{}
	result, err := store.StoreCompanies(context.Background(), []prhytj.CompanyRecord{{}})
	require.Error(t, err)
	require.Equal(t, int64(1), result.RecordsReceived)
}

func TestFinlandPRHYTJSaveDownloadRecordsLatestRunID(t *testing.T) {
	store := &FinlandPRHYTJDBStore{}
	err := store.SaveDownload(context.Background(), countryimport.DownloadMetadata{
		SourceSlug:      prhytj.SourceSlug,
		SourceName:      prhytj.SourceName,
		BaseURL:         prhytj.DefaultBaseURL,
		SnapshotPath:    "/tmp/prh.ndjson",
		SHA256:          "abc123",
		BytesDownloaded: 10,
		RecordsSeen:     1,
		PagesDownloaded: 1,
	})
	require.Error(t, err)
}
```

The last test verifies nil DB protection; a DB round-trip test is added in Step 4.

- [ ] **Step 2: Run conversion tests and verify they fail**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/countrydata -run 'TestFinlandPRHYTJ.*(RawRecordParams|StoreCompanies|SaveDownload)' -count=1 -v
```

Expected: FAIL because `FinlandPRHYTJDBStore` and conversion helpers do not exist.

- [ ] **Step 3: Implement the DB store**

Create `corpscout/scheduler/internal/countrydata/finland_prhytj_db_store.go`:

```go
package countrydata

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/pulsarpoint/corpscout/countrydata/finland/prhytj"
	countryimport "github.com/pulsarpoint/corpscout/countrydata/import"
	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

type FinlandPRHYTJTxPool interface {
	db.DBTX
	Begin(context.Context) (pgx.Tx, error)
}

type FinlandPRHYTJDBStore struct {
	pool                FinlandPRHYTJTxPool
	latestDownloadRunID uuid.UUID
	hasDownloadRunID    bool
}

func NewFinlandPRHYTJDBStore(pool FinlandPRHYTJTxPool) *FinlandPRHYTJDBStore {
	return &FinlandPRHYTJDBStore{pool: pool}
}

func (s *FinlandPRHYTJDBStore) SaveDownload(ctx context.Context, metadata countryimport.DownloadMetadata) error {
	if s == nil || s.pool == nil {
		return errors.New("finland prh ytj database store not available")
	}
	q := db.New(s.pool)
	sourceID, err := q.UpsertFinlandPRHYTJSource(ctx, db.UpsertFinlandPRHYTJSourceParams{
		SourceIdentity:      prhytj.SourceSlug,
		SourceSlug:          "prh_ytj_v3",
		SourceName:          prhytj.SourceName,
		SourceType:          "official_registry_api",
		Organization:        "Finnish Patent and Registration Office (PRH) and Finnish Tax Administration",
		BaseUrl:             optionalString(prhytj.DefaultBaseURL),
		AccessMode:          "paginated_json_api",
		License:             optionalString("CC-BY-4.0"),
		Attribution:         optionalString("Finnish Patent and Registration Office (PRH) and Finnish Tax Administration"),
		SupportsIncremental: false,
		IncrementalMode:     nil,
		InputTableName:      "countrydata_finland_prh_ytj.raw_records",
		Config:              jsonObjectBytes(map[string]any{"supports_diff": false}),
		Metadata:            jsonObjectBytes(map[string]any{"source_package": "prhytj"}),
	})
	if err != nil {
		return errors.Wrap(err, "upsert finland prh ytj source")
	}

	runID, err := q.RecordFinlandPRHYTJDownloadRun(ctx, db.RecordFinlandPRHYTJDownloadRunParams{
		SourceID:             sourceID,
		Status:               "succeeded",
		SourceUrl:            optionalString(metadata.BaseURL),
		SnapshotPath:         optionalString(metadata.SnapshotPath),
		SnapshotSha256:       optionalString(metadata.SHA256),
		BytesDownloaded:      optionalInt64(metadata.BytesDownloaded),
		RecordsSeen:          metadata.RecordsSeen,
		PagesDownloaded:      optionalInt32(int32(metadata.PagesDownloaded)),
		FirstPage:            optionalInt32(int32(metadata.FirstPage)),
		LastPage:             optionalInt32(int32(metadata.LastPage)),
		TotalResultsReported: optionalInt64Ptr(metadata.TotalResultsReported),
		StartedAt:            metadata.StartedAt,
		FinishedAt:           optionalTime(metadata.FinishedAt),
		DurationMs:           optionalInt64(metadata.DurationMS),
		Error:                nil,
		Metadata:             jsonObjectBytes(map[string]any{"license": metadata.License, "attribution": metadata.Attribution}),
	})
	if err != nil {
		return errors.Wrap(err, "record finland prh ytj download run")
	}
	s.latestDownloadRunID = runID
	s.hasDownloadRunID = true
	return nil
}

func (s *FinlandPRHYTJDBStore) SaveProcess(ctx context.Context, metadata countryimport.ProcessMetadata) error {
	if s == nil || s.pool == nil || !s.hasDownloadRunID {
		return nil
	}
	if err := db.New(s.pool).UpdateFinlandPRHYTJDownloadRunProcessStats(ctx, db.UpdateFinlandPRHYTJDownloadRunProcessStatsParams{
		ID:               s.latestDownloadRunID,
		RecordsProcessed: metadata.RecordsProcessed,
		RecordsStored:    metadata.RecordsStored,
		DecodeErrors:     metadata.DecodeErrors,
		Metadata:         jsonObjectBytes(map[string]any{"chunks_processed": metadata.ChunksProcessed}),
	}); err != nil {
		return errors.Wrap(err, "update finland prh ytj process stats")
	}
	return nil
}

func (s *FinlandPRHYTJDBStore) StoreCompanies(ctx context.Context, records []prhytj.CompanyRecord) (countryimport.StoreResult, error) {
	result := countryimport.StoreResult{RecordsReceived: int64(len(records))}
	if len(records) == 0 {
		return result, nil
	}
	if s == nil || s.pool == nil {
		return result, errors.New("finland prh ytj database store not available")
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return result, errors.Wrap(err, "begin finland prh ytj store transaction")
	}
	defer func() { _ = tx.Rollback(ctx) }()
	q := db.New(tx)

	for _, record := range records {
		params, err := finlandPRHYTJRawRecordParams(record, s.downloadRunID())
		if err != nil {
			return result, err
		}
		current, hasCurrent, err := currentFinlandPRHYTJRawRecord(ctx, q, params.BusinessID)
		if err != nil {
			return result, err
		}
		if hasCurrent && current.PayloadHash != params.PayloadHash {
			if err := q.SupersedeCurrentFinlandPRHYTJRawRecord(ctx, db.SupersedeCurrentFinlandPRHYTJRawRecordParams{
				BusinessID:  params.BusinessID,
				PayloadHash: params.PayloadHash,
			}); err != nil {
				return result, errors.Wrap(err, "supersede current finland prh ytj raw record")
			}
		}
		row, err := q.UpsertFinlandPRHYTJRawRecord(ctx, params)
		if err != nil {
			return result, errors.Wrap(err, "upsert finland prh ytj raw record")
		}
		result.RecordsStored += int64(row.RowsWritten)
	}
	if err := tx.Commit(ctx); err != nil {
		return result, errors.Wrap(err, "commit finland prh ytj store transaction")
	}
	return result, nil
}

func (s *FinlandPRHYTJDBStore) downloadRunID() *uuid.UUID {
	if s != nil && s.hasDownloadRunID {
		return &s.latestDownloadRunID
	}
	return nil
}

func currentFinlandPRHYTJRawRecord(ctx context.Context, q *db.Queries, businessID string) (db.GetCurrentFinlandPRHYTJRawRecordRow, bool, error) {
	current, err := q.GetCurrentFinlandPRHYTJRawRecord(ctx, businessID)
	if errors.Is(err, pgx.ErrNoRows) {
		return db.GetCurrentFinlandPRHYTJRawRecordRow{}, false, nil
	}
	if err != nil {
		return db.GetCurrentFinlandPRHYTJRawRecordRow{}, false, errors.Wrap(err, "get current finland prh ytj raw record")
	}
	return current, true, nil
}
```

Add helper functions in the same file:

```go
func finlandPRHYTJRawRecordParams(record prhytj.CompanyRecord, downloadRunID *uuid.UUID) (db.UpsertFinlandPRHYTJRawRecordParams, error) {
	businessID := strings.TrimSpace(record.BusinessID.Value)
	if businessID == "" {
		return db.UpsertFinlandPRHYTJRawRecordParams{}, errors.New("finland prh ytj record missing business id")
	}
	rawPayload := record.RawPayload
	if len(rawPayload) == 0 {
		marshaled, err := json.Marshal(record)
		if err != nil {
			return db.UpsertFinlandPRHYTJRawRecordParams{}, errors.Wrap(err, "marshal finland prh ytj raw payload")
		}
		rawPayload = marshaled
	}
	payloadHash := strings.TrimSpace(record.PayloadHash)
	if payloadHash == "" {
		sum := sha256.Sum256(rawPayload)
		payloadHash = hex.EncodeToString(sum[:])
	}
	profile := record.ToProfile()

	return db.UpsertFinlandPRHYTJRawRecordParams{
		DownloadRunID:       optionalUUID(downloadRunID),
		SourceNativeID:      businessID,
		BusinessID:          businessID,
		BusinessIDDigits:    optionalString(onlyDigits(businessID)),
		VatID:               optionalString(profile.VATID),
		Euid:                optionalString(profile.EUID),
		LegalName:           optionalString(profile.LegalName),
		TradeRegisterStatus: optionalString(record.TradeRegisterStatus),
		Status:              optionalString(record.Status),
		RegistrationDate:    optionalDate(record.RegistrationDate),
		EndDate:             optionalDate(record.EndDate),
		LastModified:        optionalTimestamp(record.LastModified),
		RawPayload:          rawPayload,
		PayloadHash:         payloadHash,
		Metadata:            jsonObjectBytes(map[string]any{"source_slug": prhytj.SourceSlug}),
	}, nil
}

func onlyDigits(value string) string {
	var builder strings.Builder
	for _, r := range value {
		if r >= '0' && r <= '9' {
			builder.WriteRune(r)
		}
	}
	return builder.String()
}

func optionalString(value string) *string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}

func optionalInt64(value int64) *int64 {
	if value <= 0 {
		return nil
	}
	return &value
}

func optionalInt64Ptr(value *int64) *int64 {
	if value == nil {
		return nil
	}
	return value
}

func optionalInt32(value int32) *int32 {
	if value <= 0 {
		return nil
	}
	return &value
}

func optionalTime(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	return &value
}

func optionalUUID(value *uuid.UUID) *uuid.UUID {
	if value == nil || *value == uuid.Nil {
		return nil
	}
	return value
}

func optionalDate(value string) pgtype.Date {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return pgtype.Date{}
	}
	parsed, err := time.Parse("2006-01-02", trimmed)
	if err != nil {
		return pgtype.Date{}
	}
	return pgtype.Date{Time: parsed, Valid: true}
}

func optionalTimestamp(value string) *time.Time {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04:05"} {
		parsed, err := time.Parse(layout, trimmed)
		if err == nil {
			return &parsed
		}
	}
	return nil
}

func jsonObjectBytes(value map[string]any) []byte {
	if len(value) == 0 {
		return []byte(`{}`)
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return []byte(`{}`)
	}
	return encoded
}

func nilDownloadRunID() *uuid.UUID {
	return nil
}
```

- [ ] **Step 4: Add an optional DB round-trip test**

Append this test to `finland_prhytj_db_store_test.go`:

```go
func TestFinlandPRHYTJDBStorePersistsRawRecordWhenTestDatabaseIsConfigured(t *testing.T) {
	tx := testdb.BeginTx(t)
	store := NewFinlandPRHYTJDBStore(tx)
	ctx := context.Background()

	err := store.SaveDownload(ctx, countryimport.DownloadMetadata{
		SourceSlug:       prhytj.SourceSlug,
		SourceName:       prhytj.SourceName,
		BaseURL:          prhytj.DefaultBaseURL,
		SnapshotPath:     "/tmp/prh.ndjson",
		SHA256:           "test-sha",
		BytesDownloaded:  100,
		RecordsSeen:      1,
		PagesDownloaded:  1,
		StartedAt:        time.Now().UTC().Add(-time.Minute),
		FinishedAt:       time.Now().UTC(),
		DurationMS:       1000,
	})
	require.NoError(t, err)

	result, err := store.StoreCompanies(ctx, []prhytj.CompanyRecord{
		{
			BusinessID:          prhytj.Identifier{Value: "0100002-9"},
			Names:               []prhytj.Name{{Name: "Example Oy", Type: "1", RegistrationDate: "2024-01-01"}},
			TradeRegisterStatus: "1",
			Status:              "1",
			RegistrationDate:    "2024-01-01",
			RawPayload:          []byte(`{"businessId":{"value":"0100002-9"},"names":[{"name":"Example Oy","type":"1","registrationDate":"2024-01-01"}]}`),
		},
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), result.RecordsStored)
}
```

Add imports:

```go
import (
	"time"

	"github.com/pulsarpoint/corpscout/scheduler/internal/testdb"
)
```

- [ ] **Step 5: Run countrydata adapter tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/countrydata -run TestFinlandPRHYTJ -count=1 -v
```

Expected: PASS. The DB round-trip test skips when `CORPSCOUT_TEST_DATABASE_URL` is not set.

- [ ] **Step 6: Commit**

```bash
git add corpscout/scheduler/internal/countrydata/finland_prhytj_db_store.go corpscout/scheduler/internal/countrydata/finland_prhytj_db_store_test.go
git commit -m "feat: store finland prh ytj records in postgres"
```

---

### Task 6: Wire The DB Store Into The Existing Importer Adapter

**Files:**
- Modify: `corpscout/scheduler/internal/countrydata/finland_prhytj.go`
- Modify: `corpscout/scheduler/internal/countrydata/finland_prhytj_test.go`

- [ ] **Step 1: Add an importer test for the injected store function**

Append this test to `corpscout/scheduler/internal/countrydata/finland_prhytj_test.go`:

```go
func TestFinlandPRHYTJImporterRunUsesInjectedStoreFunc(t *testing.T) {
	ctx := context.Background()
	page := `{"totalResults":1,"companies":[{"businessId":{"value":"0100002-9"},"names":[{"name":"Example Oy","type":"1"}]}]}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(page))
	}))
	t.Cleanup(server.Close)

	var stored int
	importer := FinlandPRHYTJImporter{HTTPClient: server.Client()}
	_, err := importer.Run(ctx, FinlandPRHYTJImportInput{
		BaseURL:   server.URL,
		DataDir:   t.TempDir(),
		MaxPages:  1,
		ChunkSize: 1,
		StoreFunc: func(ctx context.Context, records []prhytj.CompanyRecord) (countryimport.StoreResult, error) {
			stored += len(records)
			return countryimport.StoreResult{
				RecordsReceived: int64(len(records)),
				RecordsStored:   int64(len(records)),
			}, nil
		},
	})
	if err != nil {
		t.Fatalf("run import: %v", err)
	}
	if stored != 1 {
		t.Fatalf("expected injected store to receive 1 record, got %d", stored)
	}
}
```

Add imports if missing:

```go
import (
	"net/http"
	"net/http/httptest"

	"github.com/pulsarpoint/corpscout/countrydata/finland/prhytj"
	countryimport "github.com/pulsarpoint/corpscout/countrydata/import"
)
```

- [ ] **Step 2: Run the test and verify it fails**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/countrydata -run TestFinlandPRHYTJImporterRunUsesInjectedStoreFunc -count=1 -v
```

Expected: FAIL because `FinlandPRHYTJImportInput.StoreFunc` does not exist.

- [ ] **Step 3: Add StoreFunc to the importer input and wire it**

Modify `corpscout/scheduler/internal/countrydata/finland_prhytj.go`:

```go
type FinlandPRHYTJImportInput struct {
	BaseURL       string
	DataDir       string
	MaxPages      int
	ChunkSize     int
	PageDelay     time.Duration
	MetadataStore countryimport.MetadataStore
	StoreFunc     func(context.Context, []prhytj.CompanyRecord) (countryimport.StoreResult, error)
}
```

After `source := prhytj.NewSource(...)`, add:

```go
if input.StoreFunc != nil {
	source.StoreFunc = input.StoreFunc
}
```

- [ ] **Step 4: Run adapter tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/countrydata -count=1 -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add corpscout/scheduler/internal/countrydata/finland_prhytj.go corpscout/scheduler/internal/countrydata/finland_prhytj_test.go
git commit -m "feat: wire finland prh ytj importer store function"
```

---

### Task 7: Add A DB-Backed One-Shot Sync Command

**Files:**
- Create: `corpscout/scheduler/cmd/finland-prhytj-sync/main.go`
- Create: `corpscout/scheduler/cmd/finland-prhytj-sync/main_test.go`

This command intentionally lives in `corpscout/scheduler`, not in
`corpscout/countrydata`, because it needs Corpscout database access, sqlc, and
the scheduler-owned DB store. The existing `corpscout/countrydata/cmd/prhytj-import`
command remains the standalone no-DB source command.

- [ ] **Step 1: Write CLI parsing tests**

Create `corpscout/scheduler/cmd/finland-prhytj-sync/main_test.go`:

```go
package main

import (
	"testing"
	"time"
)

func TestParseArgsUsesFullSyncDefaults(t *testing.T) {
	cfg, err := parseArgs([]string{
		"--database-url", "postgres://example",
		"--data-dir", "/tmp/prh",
	})
	if err != nil {
		t.Fatalf("parse args: %v", err)
	}
	if cfg.databaseURL != "postgres://example" {
		t.Fatalf("database url mismatch: %q", cfg.databaseURL)
	}
	if cfg.dataDir != "/tmp/prh" {
		t.Fatalf("data dir mismatch: %q", cfg.dataDir)
	}
	if cfg.maxPages != 0 {
		t.Fatalf("max pages should default to full sync, got %d", cfg.maxPages)
	}
	if cfg.chunkSize != 500 {
		t.Fatalf("chunk size mismatch: %d", cfg.chunkSize)
	}
}

func TestParseArgsSupportsSmokeSyncFlags(t *testing.T) {
	cfg, err := parseArgs([]string{
		"--env", ".env",
		"--base-url", "http://localhost:8080/companies",
		"--database-url", "postgres://example",
		"--data-dir", "/tmp/prh",
		"--max-pages", "2",
		"--chunk-size", "100",
		"--page-delay-ms", "25",
	})
	if err != nil {
		t.Fatalf("parse args: %v", err)
	}
	if cfg.envPath != ".env" {
		t.Fatalf("env path mismatch: %q", cfg.envPath)
	}
	if cfg.baseURL != "http://localhost:8080/companies" {
		t.Fatalf("base url mismatch: %q", cfg.baseURL)
	}
	if cfg.maxPages != 2 {
		t.Fatalf("max pages mismatch: %d", cfg.maxPages)
	}
	if cfg.chunkSize != 100 {
		t.Fatalf("chunk size mismatch: %d", cfg.chunkSize)
	}
	if cfg.pageDelay != 25*time.Millisecond {
		t.Fatalf("page delay mismatch: %s", cfg.pageDelay)
	}
}
```

- [ ] **Step 2: Run command tests and verify they fail**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./cmd/finland-prhytj-sync -count=1 -v
```

Expected: FAIL because the command package does not exist.

- [ ] **Step 3: Implement the sync command**

Create `corpscout/scheduler/cmd/finland-prhytj-sync/main.go`:

```go
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5/pgxpool"

	schedcountrydata "github.com/pulsarpoint/corpscout/scheduler/internal/countrydata"
	countryimport "github.com/pulsarpoint/corpscout/countrydata/import"
)

type cliConfig struct {
	envPath     string
	baseURL     string
	databaseURL string
	dataDir     string
	maxPages    int
	chunkSize   int
	pageDelay   time.Duration
}

func main() {
	cfg, err := parseArgs(os.Args[1:])
	if err != nil {
		slog.Error("parse Finland PRH YTJ sync command", "error", err)
		os.Exit(2)
	}
	if err := run(context.Background(), cfg); err != nil {
		slog.Error("run Finland PRH YTJ sync command",
			"error_kind", countryimport.Classify(err),
			"error", err,
		)
		os.Exit(1)
	}
}

func parseArgs(args []string) (cliConfig, error) {
	flags := flag.NewFlagSet("finland-prhytj-sync", flag.ContinueOnError)
	flags.SetOutput(io.Discard)

	var pageDelayMS int
	cfg := cliConfig{chunkSize: countryimport.DefaultChunkSize}
	flags.StringVar(&cfg.envPath, "env", "", "path to env file")
	flags.StringVar(&cfg.baseURL, "base-url", "", "override PRH YTJ API base URL")
	flags.StringVar(&cfg.databaseURL, "database-url", "", "Postgres database URL; defaults to DATABASE_URL or CORPSCOUT_DATABASE_URL")
	flags.StringVar(&cfg.dataDir, "data-dir", "", "PRH YTJ local data directory")
	flags.IntVar(&cfg.maxPages, "max-pages", 0, "maximum pages to download; 0 means full sync")
	flags.IntVar(&cfg.chunkSize, "chunk-size", countryimport.DefaultChunkSize, "records per processing chunk")
	flags.IntVar(&pageDelayMS, "page-delay-ms", 0, "delay between PRH API pages in milliseconds")

	if err := flags.Parse(args); err != nil {
		return cliConfig{}, err
	}
	if pageDelayMS > 0 {
		cfg.pageDelay = time.Duration(pageDelayMS) * time.Millisecond
	}
	if cfg.databaseURL == "" {
		cfg.databaseURL = firstEnv("DATABASE_URL", "CORPSCOUT_DATABASE_URL")
	}
	if cfg.databaseURL == "" {
		return cliConfig{}, fmt.Errorf("database url is required via --database-url, DATABASE_URL, or CORPSCOUT_DATABASE_URL")
	}
	return cfg, nil
}

func run(ctx context.Context, cfg cliConfig) error {
	if cfg.envPath != "" {
		if err := countryimport.LoadEnvFile(cfg.envPath); err != nil {
			return errors.Wrapf(err, "load env file %s", cfg.envPath)
		}
	}

	pool, err := pgxpool.New(ctx, cfg.databaseURL)
	if err != nil {
		return errors.Wrap(err, "open Postgres pool")
	}
	defer pool.Close()

	store := schedcountrydata.NewFinlandPRHYTJDBStore(pool)
	importer := schedcountrydata.FinlandPRHYTJImporter{}
	result, err := importer.Run(ctx, schedcountrydata.FinlandPRHYTJImportInput{
		BaseURL:       cfg.baseURL,
		DataDir:       cfg.dataDir,
		MaxPages:      cfg.maxPages,
		ChunkSize:     cfg.chunkSize,
		PageDelay:     cfg.pageDelay,
		MetadataStore: store,
		StoreFunc:     store.StoreCompanies,
	})
	if err != nil {
		return errors.Wrap(err, "sync Finland PRH YTJ")
	}

	slog.Info("synced Finland PRH YTJ",
		"snapshot_path", result.Download.SnapshotPath,
		"pages_downloaded", result.Download.PagesDownloaded,
		"records_downloaded", result.Download.RecordsSeen,
		"records_processed", result.Process.RecordsProcessed,
		"records_stored", result.Process.RecordsStored,
		"decode_errors", result.Process.DecodeErrors,
		"snapshot_sha256", result.Download.SHA256,
	)
	return nil
}

func firstEnv(keys ...string) string {
	for _, key := range keys {
		if value := os.Getenv(key); value != "" {
			return value
		}
	}
	return ""
}
```

- [ ] **Step 4: Run command tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./cmd/finland-prhytj-sync -count=1 -v
```

Expected: PASS.

- [ ] **Step 5: Run a bounded manual sync after migrations are applied**

Run this only against a migrated development database:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
DATABASE_URL="$DATABASE_URL" GOWORK=off go run ./cmd/finland-prhytj-sync \
  --env ../.env \
  --data-dir ../data/countrydata/finland/prhytj \
  --max-pages 2 \
  --chunk-size 100
```

Expected: command exits `0`, logs a snapshot path, downloads two PRH pages, and
stores roughly 200 raw records into `countrydata_finland_prh_ytj.raw_records`.

- [ ] **Step 6: Run a full manual sync after the bounded sync succeeds**

Run this only against a migrated development database when a full PRH pull is
acceptable:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
DATABASE_URL="$DATABASE_URL" GOWORK=off go run ./cmd/finland-prhytj-sync \
  --env ../.env \
  --data-dir ../data/countrydata/finland/prhytj \
  --chunk-size 500
```

Expected: command exits `0`, downloads the full PRH snapshot, stores records in
chunks, and logs pages, records, decode errors, and snapshot SHA-256. Do not run
this in normal CI.

- [ ] **Step 7: Commit**

```bash
git add corpscout/scheduler/cmd/finland-prhytj-sync
git commit -m "feat: add finland prh ytj sync command"
```

---

### Task 8: Run Full Focused Verification

**Files:**
- No file edits.

- [ ] **Step 1: Run countrydata module tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/countrydata
GOWORK=off go test ./... -count=1
```

Expected: PASS.

- [ ] **Step 2: Run scheduler DB and countrydata adapter tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/db ./internal/countrydata -count=1 -v
```

Expected: PASS, with DB round-trip tests skipped when `CORPSCOUT_TEST_DATABASE_URL` is unset.

- [ ] **Step 3: Run sqlc generation as a stability check**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off sqlc generate -f ../database/sqlc.yaml
git diff --exit-code -- internal/db/gen
```

Expected: sqlc exits `0`; `git diff --exit-code` exits `0` because generated files are already committed.

- [ ] **Step 4: Run optional migrated DB test when a test database is available**

Run only when `CORPSCOUT_TEST_DATABASE_URL` points to a migrated test database:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
CORPSCOUT_TEST_DATABASE_URL="$CORPSCOUT_TEST_DATABASE_URL" GOWORK=off go test ./internal/countrydata -run TestFinlandPRHYTJDBStorePersistsRawRecordWhenTestDatabaseIsConfigured -count=1 -v
```

Expected: PASS, or SKIP if `CORPSCOUT_TEST_DATABASE_URL` is unset.

- [ ] **Step 5: Review final diff**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git status --short
git log --oneline --decorate -8
```

Expected: only intentional committed changes are present. Existing unrelated untracked files, such as `companies/analysis/united_states/data_model/`, must remain untouched.

---

## Self-Review

- Schema requirement coverage:
  - Source metadata table: Task 3 creates `countrydata_finland_prh_ytj.sources`.
  - Download/audit metadata table: Task 3 creates `download_runs`.
  - Diff support flag: Task 3 adds `supports_incremental` and config `supports_diff=false`.
  - Raw record table: Task 3 creates `raw_records`.
  - Unique source row identifier: Task 3 adds `business_id` and current unique index.
  - Row payload hash: Task 1 sets per-record hash; Task 3 stores `payload_hash`.
  - Full raw data JSONB: Task 3 stores `raw_payload JSONB NOT NULL`.
  - Name/key scalar columns: Task 3 stores `business_id`, `business_id_digits`, `vat_id`, `euid`, and `legal_name`.
  - Manual full sync command: Task 7 adds `go run ./cmd/finland-prhytj-sync` with bounded and full-run verification.
- Boundary coverage:
  - `corpscout/countrydata` remains independent from scheduler/sqlc.
  - `scheduler/internal/countrydata` owns the DB-backed adapter.
  - sqlc generated params are created at the scheduler DB boundary.
- Placeholder scan:
  - No unresolved implementation placeholders are required for execution.
