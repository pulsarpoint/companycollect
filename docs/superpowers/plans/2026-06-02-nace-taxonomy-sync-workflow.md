# NACE Taxonomy Sync Workflow Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an API-triggered Temporal workflow that downloads official NACE taxonomy data, hashes the downloaded file, skips already-processed content, and imports new content into the existing canonical NACE database tables.

**Architecture:** The scheduler owns this import. A new hash/audit table records downloaded NACE source files before parsing; the import activity checks that table and skips when the same hash was already processed. The workflow has one main activity that downloads, hashes, deduplicates, parses, and upserts inside the scheduler process so large file bytes never enter Temporal workflow history.

**Tech Stack:** PostgreSQL migrations, sqlc, Go, `net/http`, standard-library `encoding/xml` for RDF/XML-shaped NACE payloads, Temporal Go SDK, `log/slog`, `github.com/cockroachdb/errors`, existing Corpscout scheduler/httpapi/app wiring.

---

## Official Source Context

NACE Rev. 2.1 is the current European NACE revision for statistics from 2025 onward. Eurostat says it is the newest version and NACE Rev. 2 was used for 2008-2024. Eurostat also says NACE Rev. 2.1 is accessible as Linked Open Data, and the EU data portal exposes NACE Rev. 2.1 distributions.

Use an environment-configured URL rather than hardcoding one in code, because EU vocabulary distribution URLs can change while the semantic source remains official.

Default source candidates:

- Eurostat NACE overview: `https://ec.europa.eu/eurostat/web/nace`
- NACE Rev. 2.1 publication: `https://ec.europa.eu/eurostat/web/products-manuals-and-guidelines/w/ks-gq-24-007`
- EU data portal NACE Rev. 2.1 dataset: `https://data.europa.eu/data/datasets/stat_nace-rev-2-1?locale=en`
- EU Vocabularies / ShowVoc linked data entry: `https://showvoc.op.europa.eu/`

The implementation should default to `CORPSCOUT_NACE_REV21_SOURCE_URL`; if it is not configured, the API rejects workflow start with a clear JSON error instead of starting a workflow that cannot download anything.

## Parser Library Decision

Use the Go standard library `encoding/xml` for the first implementation. The NACE importer needs a narrow and stable subset of the RDF/XML-shaped source:

- `skos:notation`
- English `skos:prefLabel`
- English `skos:scopeNote`
- `skos:broader rdf:resource`

Do not add a NACE-specific or generic RDF dependency in this first version. A library such as `github.com/tggo/goRDFlib/rdfxml` can be considered later only if the official source contains RDF constructs that cannot be handled cleanly with a streaming XML parser.

## Current Repo State

Already implemented:

- `nace_classifications`
- `nace_codes`
- `nace_code_aliases`
- `v_nace_taxonomy_state`
- `v_nace_code_tree`
- `nacetaxonomy.NormalizeCode`
- `nacetaxonomy.LevelForCode`
- `nacetaxonomy.LevelNameForCode`
- `nacetaxonomy.DefaultRevision = "2.1"`
- BRREG source/native mapping table `brreg_workflow.nace_mappings`

Relevant current files:

- `corpscout/database/migrations/000070_nace_taxonomy.up.sql`
- `corpscout/database/queries/nace_taxonomy.sql`
- `corpscout/scheduler/internal/nacetaxonomy/code.go`
- `corpscout/scheduler/internal/app/temporal.go`
- `corpscout/scheduler/internal/httpapi/workflow_triggers.go`
- `corpscout/scheduler/internal/httpapi/handlers.go`
- `corpscout/scheduler/internal/config/config.go`

## Scope

In scope:

- Table for source file hash and processed status.
- Optional import-run audit table for user-facing workflow result and troubleshooting.
- sqlc queries for registering files, detecting already-processed hashes, marking processed/failed.
- NACE downloader.
- NACE parser for RDF/XML-style linked-data payloads, implemented with `encoding/xml`.
- Import activity that upserts `nace_classifications`, `nace_codes`, aliases, and parent links.
- Temporal workflow and worker registration.
- HTTP trigger endpoint.
- Tests.

Out of scope:

- UI button/page.
- BRREG remapping after new NACE import.
- `company_industries` migration.
- Scheduled automatic sync.
- S3 storage of the source file. The hash table is enough for this step.

## Data Contract

### Source file dedupe behavior

1. Activity downloads the configured source URL.
2. Activity computes SHA-256 hash of the exact downloaded bytes.
3. Activity inserts or finds a `nace_source_files` row for `(revision, source_url, content_sha256)`.
4. If an existing row has `status='processed'` and `force_reprocess=false`, activity returns `status='skipped'` with a user-visible message.
5. If the hash is new, activity stores the source file row with `status='downloaded'`.
6. Activity parses and upserts NACE rows.
7. Activity marks the source file row `processed`.
8. Workflow returns summary including hash and counts.

### Workflow result statuses

- `succeeded`: source file was new or forced and import completed.
- `skipped`: same hash was already processed.
- `failed`: download, parse, or database import failed.

## Task 1: Source File Hash Schema

**Files:**

- Create: `corpscout/database/migrations/000072_nace_source_files.up.sql`
- Create: `corpscout/database/migrations/000072_nace_source_files.down.sql`
- Create: `corpscout/scheduler/internal/db/nace_source_files_migration_test.go`

- [ ] **Step 1: Write migration shape test**

Create `corpscout/scheduler/internal/db/nace_source_files_migration_test.go`:

```go
package db

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNACESourceFilesMigrationDefinesHashAndRunTables(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000072_nace_source_files.up.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "CREATE TABLE nace_source_files")
	require.Contains(t, sql, "CREATE TABLE nace_import_runs")
	require.Contains(t, sql, "content_sha256 TEXT NOT NULL")
	require.Contains(t, sql, "UNIQUE (revision, source_url, content_sha256)")
	require.Contains(t, sql, "status IN ('downloaded', 'processing', 'processed', 'failed')")
	require.Contains(t, sql, "status IN ('running', 'skipped', 'succeeded', 'failed')")
	require.Contains(t, sql, "CREATE OR REPLACE VIEW v_nace_source_file_imports")
	require.Contains(t, sql, "GRANT SELECT ON nace_source_files TO corpscout_anon")
	require.Contains(t, sql, "GRANT SELECT ON v_nace_source_file_imports TO corpscout_anon")
}

func TestNACESourceFilesDownMigrationDropsHashAndRunTables(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000072_nace_source_files.down.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "DROP VIEW IF EXISTS v_nace_source_file_imports")
	require.Contains(t, sql, "DROP TABLE IF EXISTS nace_import_runs")
	require.Contains(t, sql, "DROP TABLE IF EXISTS nace_source_files")
}
```

- [ ] **Step 2: Run test and verify it fails**

Run:

```bash
cd corpscout/scheduler
GOWORK=off go test ./internal/db -run TestNACESourceFiles -count=1
```

Expected: fail because migration `000072` does not exist.

- [ ] **Step 3: Create migration**

Create `corpscout/database/migrations/000072_nace_source_files.up.sql`:

```sql
CREATE TABLE nace_source_files (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  revision TEXT NOT NULL,
  source_url TEXT NOT NULL,
  content_sha256 TEXT NOT NULL,
  content_length_bytes BIGINT NOT NULL,
  content_type TEXT,
  etag TEXT,
  last_modified TEXT,
  status TEXT NOT NULL DEFAULT 'downloaded',
  processed_at TIMESTAMPTZ,
  error TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_nace_source_files_revision CHECK (btrim(revision) <> ''),
  CONSTRAINT chk_nace_source_files_source_url CHECK (btrim(source_url) <> ''),
  CONSTRAINT chk_nace_source_files_sha256 CHECK (content_sha256 ~ '^[a-f0-9]{64}$'),
  CONSTRAINT chk_nace_source_files_content_length CHECK (content_length_bytes >= 0),
  CONSTRAINT chk_nace_source_files_status CHECK (status IN ('downloaded', 'processing', 'processed', 'failed')),
  CONSTRAINT chk_nace_source_files_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  UNIQUE (revision, source_url, content_sha256)
);

CREATE TABLE nace_import_runs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  temporal_workflow_id TEXT NOT NULL UNIQUE,
  source_file_id UUID REFERENCES nace_source_files(id) ON DELETE SET NULL,
  revision TEXT NOT NULL,
  source_url TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'running',
  content_sha256 TEXT,
  records_seen INTEGER NOT NULL DEFAULT 0,
  records_imported INTEGER NOT NULL DEFAULT 0,
  records_deactivated INTEGER NOT NULL DEFAULT 0,
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at TIMESTAMPTZ,
  error TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  CONSTRAINT chk_nace_import_runs_revision CHECK (btrim(revision) <> ''),
  CONSTRAINT chk_nace_import_runs_source_url CHECK (btrim(source_url) <> ''),
  CONSTRAINT chk_nace_import_runs_status CHECK (status IN ('running', 'skipped', 'succeeded', 'failed')),
  CONSTRAINT chk_nace_import_runs_sha256 CHECK (content_sha256 IS NULL OR content_sha256 ~ '^[a-f0-9]{64}$'),
  CONSTRAINT chk_nace_import_runs_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE INDEX idx_nace_source_files_revision_status
  ON nace_source_files(revision, status, created_at DESC);

CREATE INDEX idx_nace_source_files_sha256
  ON nace_source_files(content_sha256);

CREATE INDEX idx_nace_import_runs_revision_started
  ON nace_import_runs(revision, started_at DESC);

CREATE OR REPLACE VIEW v_nace_source_file_imports AS
SELECT
  run.id AS import_run_id,
  run.temporal_workflow_id,
  run.revision,
  run.source_url,
  run.status AS import_status,
  run.content_sha256,
  run.records_seen,
  run.records_imported,
  run.records_deactivated,
  run.started_at,
  run.finished_at,
  run.error AS import_error,
  file.id AS source_file_id,
  file.status AS source_file_status,
  file.content_length_bytes,
  file.content_type,
  file.etag,
  file.last_modified,
  file.processed_at
FROM nace_import_runs run
LEFT JOIN nace_source_files file ON file.id = run.source_file_id;

GRANT SELECT ON nace_source_files TO corpscout_anon;
GRANT SELECT ON nace_import_runs TO corpscout_anon;
GRANT SELECT ON v_nace_source_file_imports TO corpscout_anon;
```

Create `corpscout/database/migrations/000072_nace_source_files.down.sql`:

```sql
DROP VIEW IF EXISTS v_nace_source_file_imports;
DROP TABLE IF EXISTS nace_import_runs;
DROP TABLE IF EXISTS nace_source_files;
```

- [ ] **Step 4: Run migration tests**

Run:

```bash
cd corpscout/scheduler
GOWORK=off go test ./internal/db -run 'TestNACESourceFiles|TestNACETaxonomy' -count=1
```

Expected: pass.

## Task 2: sqlc Queries For Source Files And Imports

**Files:**

- Modify: `corpscout/database/queries/nace_taxonomy.sql`
- Generated: `corpscout/scheduler/internal/db/gen/*.go`

- [ ] **Step 1: Add queries**

Append to `corpscout/database/queries/nace_taxonomy.sql`:

```sql
-- name: BeginNACEImportRun :one
INSERT INTO nace_import_runs (
  temporal_workflow_id,
  revision,
  source_url,
  metadata
) VALUES ($1, $2, $3, COALESCE($4::jsonb, '{}'::jsonb))
ON CONFLICT (temporal_workflow_id)
DO UPDATE SET
  revision = EXCLUDED.revision,
  source_url = EXCLUDED.source_url,
  status = 'running',
  source_file_id = NULL,
  content_sha256 = NULL,
  records_seen = 0,
  records_imported = 0,
  records_deactivated = 0,
  started_at = now(),
  finished_at = NULL,
  error = NULL,
  metadata = EXCLUDED.metadata
RETURNING *;

-- name: GetProcessedNACESourceFileByHash :one
SELECT *
FROM nace_source_files
WHERE revision = $1
  AND source_url = $2
  AND content_sha256 = $3
  AND status = 'processed';

-- name: UpsertDownloadedNACESourceFile :one
INSERT INTO nace_source_files (
  revision,
  source_url,
  content_sha256,
  content_length_bytes,
  content_type,
  etag,
  last_modified,
  status,
  metadata
) VALUES (
  $1, $2, $3, $4, $5, $6, $7, 'downloaded', COALESCE($8::jsonb, '{}'::jsonb)
)
ON CONFLICT (revision, source_url, content_sha256)
DO UPDATE SET
  content_length_bytes = EXCLUDED.content_length_bytes,
  content_type = EXCLUDED.content_type,
  etag = EXCLUDED.etag,
  last_modified = EXCLUDED.last_modified,
  status = CASE
    WHEN nace_source_files.status = 'processed' THEN nace_source_files.status
    ELSE 'downloaded'
  END,
  error = NULL,
  metadata = EXCLUDED.metadata,
  updated_at = now()
RETURNING *;

-- name: MarkNACESourceFileProcessing :one
UPDATE nace_source_files
SET status = 'processing',
    error = NULL,
    updated_at = now()
WHERE id = $1
RETURNING *;

-- name: MarkNACESourceFileProcessed :one
UPDATE nace_source_files
SET status = 'processed',
    processed_at = now(),
    error = NULL,
    metadata = metadata || COALESCE($2::jsonb, '{}'::jsonb),
    updated_at = now()
WHERE id = $1
RETURNING *;

-- name: MarkNACESourceFileFailed :one
UPDATE nace_source_files
SET status = 'failed',
    error = $2,
    metadata = metadata || COALESCE($3::jsonb, '{}'::jsonb),
    updated_at = now()
WHERE id = $1
RETURNING *;

-- name: FinishNACEImportRun :one
UPDATE nace_import_runs
SET
  source_file_id = $2,
  status = $3,
  content_sha256 = $4,
  records_seen = $5,
  records_imported = $6,
  records_deactivated = $7,
  finished_at = now(),
  error = NULLIF($8, ''),
  metadata = metadata || COALESCE($9::jsonb, '{}'::jsonb)
WHERE id = $1
RETURNING *;

-- name: ListNACESourceFileImports :many
SELECT *
FROM v_nace_source_file_imports
WHERE revision = COALESCE(sqlc.narg('revision')::text, revision)
ORDER BY started_at DESC
LIMIT sqlc.arg('limit')::integer;
```

- [ ] **Step 2: Regenerate sqlc**

Run:

```bash
cd corpscout/database
sqlc generate
```

Expected: generated methods include `BeginNACEImportRun`, `GetProcessedNACESourceFileByHash`, `UpsertDownloadedNACESourceFile`, and `FinishNACEImportRun`.

- [ ] **Step 3: Verify generated package**

Run:

```bash
cd corpscout/scheduler
GOWORK=off go test ./internal/db/gen -count=1
```

Expected: pass.

## Task 3: NACE Downloader And Parser

**Files:**

- Create: `corpscout/scheduler/internal/nacetaxonomy/source_file.go`
- Create: `corpscout/scheduler/internal/nacetaxonomy/source_file_test.go`
- Create: `corpscout/scheduler/internal/nacetaxonomy/rdf_parser.go`
- Create: `corpscout/scheduler/internal/nacetaxonomy/rdf_parser_test.go`

- [ ] **Step 1: Add downloader tests**

Create `corpscout/scheduler/internal/nacetaxonomy/source_file_test.go`:

```go
package nacetaxonomy

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDownloadSourceFileComputesSHA256(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/rdf+xml")
		w.Header().Set("ETag", "etag-1")
		w.Header().Set("Last-Modified", "Tue, 02 Jun 2026 09:00:00 GMT")
		_, _ = w.Write([]byte("nace fixture"))
	}))
	defer server.Close()

	file, err := DownloadSourceFile(t.Context(), server.Client(), server.URL, 1_000_000)

	require.NoError(t, err)
	require.Equal(t, "cdfc91f61b035ddf5809186f3198337db5d45430dc70b058ad32dab1727b1480", file.SHA256)
	require.Equal(t, int64(12), file.ContentLengthBytes)
	require.Equal(t, "application/rdf+xml", file.ContentType)
	require.Equal(t, "etag-1", file.ETag)
	require.NotEmpty(t, file.Body)
}

func TestDownloadSourceFileRejectsTooLargeResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("too-large"))
	}))
	defer server.Close()

	_, err := DownloadSourceFile(t.Context(), server.Client(), server.URL, 3)

	require.ErrorContains(t, err, "nace source file exceeds maximum size")
}
```

- [ ] **Step 2: Implement downloader**

Create `corpscout/scheduler/internal/nacetaxonomy/source_file.go`:

```go
package nacetaxonomy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"strings"

	"github.com/cockroachdb/errors"
)

const DefaultMaxSourceFileBytes int64 = 25 * 1024 * 1024

type DownloadedSourceFile struct {
	Body               []byte
	SHA256             string
	ContentLengthBytes int64
	ContentType        string
	ETag               string
	LastModified       string
}

func DownloadSourceFile(ctx context.Context, httpClient *http.Client, sourceURL string, maxBytes int64) (DownloadedSourceFile, error) {
	if strings.TrimSpace(sourceURL) == "" {
		return DownloadedSourceFile{}, errors.New("nace source url is required")
	}
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	if maxBytes <= 0 {
		maxBytes = DefaultMaxSourceFileBytes
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, sourceURL, nil)
	if err != nil {
		return DownloadedSourceFile{}, errors.Wrap(err, "create nace source request")
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return DownloadedSourceFile{}, errors.Wrap(err, "download nace source file")
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return DownloadedSourceFile{}, errors.Newf("download nace source file failed with status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBytes+1))
	if err != nil {
		return DownloadedSourceFile{}, errors.Wrap(err, "read nace source file")
	}
	if int64(len(body)) > maxBytes {
		return DownloadedSourceFile{}, errors.New("nace source file exceeds maximum size")
	}
	sum := sha256.Sum256(body)
	return DownloadedSourceFile{
		Body:               body,
		SHA256:             hex.EncodeToString(sum[:]),
		ContentLengthBytes: int64(len(body)),
		ContentType:        resp.Header.Get("Content-Type"),
		ETag:               resp.Header.Get("ETag"),
		LastModified:       resp.Header.Get("Last-Modified"),
	}, nil
}
```

- [ ] **Step 3: Add RDF parser tests**

Create `corpscout/scheduler/internal/nacetaxonomy/rdf_parser_test.go`:

```go
package nacetaxonomy

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseRDFXMLNACECodes(t *testing.T) {
	body := []byte(`<?xml version="1.0" encoding="UTF-8"?>
<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
         xmlns:skos="http://www.w3.org/2004/02/skos/core#">
  <skos:Concept rdf:about="http://data.europa.eu/ux2/nace2.1/M">
    <skos:notation>M</skos:notation>
    <skos:prefLabel xml:lang="en">Real estate activities</skos:prefLabel>
  </skos:Concept>
  <skos:Concept rdf:about="http://data.europa.eu/ux2/nace2.1/68">
    <skos:notation>68</skos:notation>
    <skos:broader rdf:resource="http://data.europa.eu/ux2/nace2.1/M"/>
    <skos:prefLabel xml:lang="en">Real estate activities</skos:prefLabel>
  </skos:Concept>
  <skos:Concept rdf:about="http://data.europa.eu/ux2/nace2.1/68.20">
    <skos:notation>68.20</skos:notation>
    <skos:broader rdf:resource="http://data.europa.eu/ux2/nace2.1/68.2"/>
    <skos:prefLabel xml:lang="en">Renting and operating of own or leased real estate</skos:prefLabel>
    <skos:scopeNote xml:lang="en">This class includes landlords.</skos:scopeNote>
  </skos:Concept>
</rdf:RDF>`)

	codes, err := ParseRDFXMLNACECodes(body)

	require.NoError(t, err)
	require.Len(t, codes, 3)
	require.Equal(t, "M", codes[0].Code)
	require.Equal(t, "section", codes[0].LevelName)
	require.Equal(t, "68.20", codes[2].Code)
	require.Equal(t, "class", codes[2].LevelName)
	require.Equal(t, "68.2", codes[2].ParentCode)
	require.Contains(t, codes[2].Description, "landlords")
}
```

- [ ] **Step 4: Implement RDF parser**

Create `corpscout/scheduler/internal/nacetaxonomy/rdf_parser.go` with a streaming XML parser:

```go
package nacetaxonomy

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"path"
	"strings"

	"github.com/cockroachdb/errors"
)

type ParsedCode struct {
	Code          string
	NormalizedCode string
	Level         int16
	LevelName     string
	ParentCode    string
	Title         string
	Description   string
	Includes      string
	Excludes      string
	Notes         []byte
	SourcePayload []byte
	SourceHash    string
}

func ParseRDFXMLNACECodes(body []byte) ([]ParsedCode, error) {
	decoder := xml.NewDecoder(bytes.NewReader(body))
	var codes []ParsedCode
	for {
		token, err := decoder.Token()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, errors.Wrap(err, "parse nace rdf xml")
		}
		start, ok := token.(xml.StartElement)
		if !ok || start.Name.Local != "Concept" {
			continue
		}
		code, err := parseConcept(decoder, start)
		if err != nil {
			return nil, err
		}
		if code.Code != "" && code.Title != "" {
			codes = append(codes, code)
		}
	}
	if len(codes) == 0 {
		return nil, errors.New("nace rdf xml contained no codes")
	}
	return codes, nil
}
```

The implementation must import `io` and complete `parseConcept` with these rules:

- `skos:notation` becomes `Code`.
- `skos:prefLabel` with `xml:lang="en"` becomes `Title`.
- `skos:scopeNote` with `xml:lang="en"` becomes `Description`.
- `skos:broader rdf:resource=".../68.2"` becomes `ParentCode="68.2"` by taking the URI basename.
- `NormalizedCode`, `Level`, and `LevelName` use existing helper functions.
- `SourcePayload` is a JSON object containing `about`, `notation`, `pref_label_en`, `scope_note_en`, and `broader`.
- `SourceHash` is SHA-256 of `SourcePayload`.

Do not support ZIP in this first parser unless the chosen official source URL returns ZIP; if it does, add ZIP extraction in the importer activity and parse the first `.rdf`/`.xml` entry.

- [ ] **Step 5: Run parser/downloader tests**

Run:

```bash
cd corpscout/scheduler
GOWORK=off go test ./internal/nacetaxonomy -count=1
```

Expected: pass.

## Task 4: Concrete Store/Activity For Download, Hash Check, And Import

**Files:**

- Create: `corpscout/scheduler/internal/nacetaxonomy/actions.go`
- Create: `corpscout/scheduler/internal/nacetaxonomy/actions_test.go`

- [ ] **Step 1: Define action input/result**

Add to `actions.go`:

```go
type SyncNACETaxonomyActivityInput struct {
	TemporalWorkflowID string `json:"temporal_workflow_id"`
	Revision           string `json:"revision"`
	SourceURL          string `json:"source_url"`
	Trigger            string `json:"trigger"`
	ForceReprocess     bool   `json:"force_reprocess"`
}

type SyncNACETaxonomyActivityResult struct {
	Status             string `json:"status"`
	ImportRunID        string `json:"import_run_id"`
	SourceFileID       string `json:"source_file_id"`
	ContentSHA256      string `json:"content_sha256"`
	RecordsSeen        int32  `json:"records_seen"`
	RecordsImported    int32  `json:"records_imported"`
	RecordsDeactivated int32  `json:"records_deactivated"`
	Message            string `json:"message"`
}
```

- [ ] **Step 2: Implement concrete `Actions`**

Create `Actions` with concrete dependencies:

```go
type Actions struct {
	db         *db.Queries
	httpClient *http.Client
}

func NewActions(database db.DBTX, httpClient *http.Client) *Actions {
	return &Actions{db: db.New(database), httpClient: httpClient}
}
```

Activity behavior:

1. Validate `Revision`, `SourceURL`, `TemporalWorkflowID`.
2. `BeginNACEImportRun`.
3. `DownloadSourceFile`.
4. Check `GetProcessedNACESourceFileByHash` unless `ForceReprocess`.
5. If processed row exists:
   - `FinishNACEImportRun(status='skipped', source_file_id=existing.ID, content_sha256=hash, metadata={"message":"source file hash already processed"})`
   - return `Status="skipped"`.
6. `UpsertDownloadedNACESourceFile`.
7. `MarkNACESourceFileProcessing`.
8. Parse file into `ParsedCode`.
9. Upsert classification with `revision`, name `NACE Rev. <revision>`, source URL and source metadata containing content hash.
10. Upsert every code into `nace_codes`.
11. Insert aliases:
    - exact alias: original code
    - normalized alias: `NormalizeCode(code)`
12. `LinkNACECodeParents`, `ClearRootNACECodeParents`, `DeactivateMissingNACECodes`.
13. `MarkNACESourceFileProcessed`.
14. `FinishNACEImportRun(status='succeeded')`.
15. On parse/import failure after source row exists, mark source file failed and import run failed.

Use one database transaction for steps 9-14. The download itself should not be inside the transaction.

- [ ] **Step 3: Add activity unit tests**

`actions_test.go` should cover:

```go
func TestSyncNACETaxonomyActivityRejectsMissingSourceURL(t *testing.T)
func TestSyncNACETaxonomyActivitySkipsAlreadyProcessedHash(t *testing.T)
func TestSyncNACETaxonomyActivityImportsNewHash(t *testing.T)
```

For DB integration tests use `internal/testdb.BeginTx(t)`. If `CORPSCOUT_TEST_DATABASE_URL` is not set, tests skip cleanly.

- [ ] **Step 4: Run activity tests**

Run:

```bash
cd corpscout/scheduler
GOWORK=off go test ./internal/nacetaxonomy -count=1
```

Expected: pass; DB integration tests skip unless `CORPSCOUT_TEST_DATABASE_URL` is configured.

## Task 5: Temporal Workflow

**Files:**

- Create: `corpscout/scheduler/internal/nacetaxonomy/workflow.go`
- Create: `corpscout/scheduler/internal/nacetaxonomy/workflow_test.go`

- [ ] **Step 1: Add workflow**

Create `workflow.go`:

```go
package nacetaxonomy

import (
	"time"

	"github.com/cockroachdb/errors"
	"go.temporal.io/sdk/temporal"
	temporalworkflow "go.temporal.io/sdk/workflow"
)

const SyncWorkflowName = "SyncNACETaxonomy"
const SyncTaskQueue = "nace-taxonomy-sync"

type SyncNACETaxonomyInput struct {
	Revision       string `json:"revision,omitempty"`
	SourceURL      string `json:"source_url,omitempty"`
	Trigger        string `json:"trigger,omitempty"`
	ForceReprocess bool   `json:"force_reprocess,omitempty"`
}

type SyncNACETaxonomyResult = SyncNACETaxonomyActivityResult

func SyncNACETaxonomy(ctx temporalworkflow.Context, input SyncNACETaxonomyInput) (SyncNACETaxonomyResult, error) {
	if input.Revision == "" {
		input.Revision = DefaultRevision
	}
	if input.Trigger == "" {
		input.Trigger = "manual"
	}
	info := temporalworkflow.GetInfo(ctx)
	ctx = temporalworkflow.WithActivityOptions(ctx, temporalworkflow.ActivityOptions{
		StartToCloseTimeout: 15 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    10 * time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    2 * time.Minute,
			MaximumAttempts:    3,
		},
	})

	var result SyncNACETaxonomyActivityResult
	err := temporalworkflow.ExecuteActivity(ctx, "SyncNACETaxonomyActivity", SyncNACETaxonomyActivityInput{
		TemporalWorkflowID: info.WorkflowExecution.ID,
		Revision:           input.Revision,
		SourceURL:          input.SourceURL,
		Trigger:            input.Trigger,
		ForceReprocess:     input.ForceReprocess,
	}).Get(ctx, &result)
	if err != nil {
		return SyncNACETaxonomyResult{}, errors.Wrap(err, "sync nace taxonomy activity")
	}
	return result, nil
}
```

- [ ] **Step 2: Add workflow tests**

Create `workflow_test.go` using Temporal testsuite:

```go
func TestSyncNACETaxonomyWorkflowCallsActivityWithDefaults(t *testing.T)
func TestSyncNACETaxonomyWorkflowReturnsSkippedResult(t *testing.T)
```

The fake activity should assert:

- revision defaults to `2.1`
- trigger defaults to `manual`
- source URL is passed through unchanged
- workflow id is included

- [ ] **Step 3: Run workflow tests**

Run:

```bash
cd corpscout/scheduler
GOWORK=off go test ./internal/nacetaxonomy -run TestSyncNACETaxonomyWorkflow -count=1
```

Expected: pass.

## Task 6: App Worker Wiring And Config

**Files:**

- Modify: `corpscout/scheduler/internal/config/config.go`
- Modify: `corpscout/scheduler/internal/app/temporal.go`
- Create: `corpscout/scheduler/internal/app/nace_taxonomy_temporal.go`
- Modify: `corpscout/.env.example`

- [ ] **Step 1: Add config fields**

Add to `Config`:

```go
NACESourceURL string
```

In `Load()`:

```go
NACESourceURL: getEnv("CORPSCOUT_NACE_REV21_SOURCE_URL", ""),
```

Add to `.env.example`:

```dotenv
# Official NACE Rev. 2.1 source file URL. Use an EU Vocabularies/data.europa.eu RDF/XML distribution URL.
CORPSCOUT_NACE_REV21_SOURCE_URL=
```

- [ ] **Step 2: Add Temporal resources**

In `app/temporal.go`, extend `temporalWorkerResources`:

```go
naceTaxonomyActions *nacetaxonomy.Actions
```

Construct it in `newTemporalWorkerResources`:

```go
naceTaxonomyActions: nacetaxonomy.NewActions(pool, http.DefaultClient),
```

Add worker to `newTemporalWorkers`:

```go
newNACETaxonomyTemporalWorker(temporalClient, resources),
```

- [ ] **Step 3: Add worker registration file**

Create `corpscout/scheduler/internal/app/nace_taxonomy_temporal.go`:

```go
package app

import (
	"log/slog"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	temporalworker "go.temporal.io/sdk/worker"

	"github.com/pulsarpoint/corpscout/scheduler/internal/nacetaxonomy"
)

func newNACETaxonomyTemporalWorker(temporalClient client.Client, resources *temporalWorkerResources) temporalworker.Worker {
	slog.Debug("creating nace taxonomy temporal worker", "task_queue", nacetaxonomy.SyncTaskQueue)
	worker := temporalworker.New(temporalClient, nacetaxonomy.SyncTaskQueue, temporalworker.Options{})
	worker.RegisterWorkflow(nacetaxonomy.SyncNACETaxonomy)
	worker.RegisterActivityWithOptions(
		resources.naceTaxonomyActions.SyncNACETaxonomyActivity,
		activity.RegisterOptions{Name: "SyncNACETaxonomyActivity"},
	)
	return worker
}
```

- [ ] **Step 4: Run app/config tests**

Run:

```bash
cd corpscout/scheduler
GOWORK=off go test ./internal/app ./internal/config -count=1
```

Expected: pass.

## Task 7: HTTP API Trigger

**Files:**

- Modify: `corpscout/scheduler/internal/httpapi/handlers.go`
- Modify: `corpscout/scheduler/internal/httpapi/workflow_triggers.go`
- Modify: `corpscout/scheduler/internal/httpapi/workflow_triggers_test.go`

- [ ] **Step 1: Add request type**

In `workflow_triggers.go`:

```go
type startNACETaxonomySyncWorkflowRequest struct {
	Revision       string `json:"revision,omitempty"`
	SourceURL      string `json:"source_url,omitempty"`
	Trigger        string `json:"trigger,omitempty"`
	ForceReprocess bool   `json:"force_reprocess,omitempty"`
}
```

- [ ] **Step 2: Add handler**

Add `handleStartNACETaxonomySyncWorkflow`:

- Require Temporal client.
- Decode JSON with `DisallowUnknownFields`.
- Trim revision/source URL/trigger.
- Default revision to `nacetaxonomy.DefaultRevision`.
- If `source_url` missing, use configured `h.naceSourceURL`.
- If still missing, return `400` JSON error: `"nace source url is required"`.
- Validate URL scheme is `https` or `http`.
- Start workflow with ID prefix `nace-taxonomy-sync`.
- Task queue `nacetaxonomy.SyncTaskQueue`.
- Workflow `nacetaxonomy.SyncNACETaxonomy`.

The `Handlers` struct needs a new field:

```go
naceSourceURL string
```

Update `NewHandlers` signature only if necessary; otherwise add a chain method:

```go
func (h *Handlers) ConfigureNACE(sourceURL string) *Handlers {
	h.naceSourceURL = sourceURL
	return h
}
```

Prefer `ConfigureNACE` to avoid widening existing test constructor calls.

- [ ] **Step 3: Register route**

In `handlers.go`:

```go
r.Post("/workflows/nace/taxonomy-sync", h.handleStartNACETaxonomySyncWorkflow)
```

- [ ] **Step 4: Add HTTP tests**

Append tests in `workflow_triggers_test.go`:

```go
func TestStartNACETaxonomySyncWorkflowStartsTemporalWorkflow(t *testing.T)
func TestStartNACETaxonomySyncWorkflowUsesConfiguredSourceURL(t *testing.T)
func TestStartNACETaxonomySyncWorkflowRejectsMissingSourceURL(t *testing.T)
func TestStartNACETaxonomySyncWorkflowRejectsInvalidURL(t *testing.T)
```

Expected assertions:

- HTTP `202`
- response workflow `"SyncNACETaxonomy"`
- task queue `"nace-taxonomy-sync"`
- input revision `"2.1"` by default
- input source URL from request or configured default
- input force flag preserved

- [ ] **Step 5: Wire config in app server**

In `app/server.go`, after `httpapi.NewHandlers(...)`, call:

```go
.ConfigureNACE(cfg.NACESourceURL)
```

- [ ] **Step 6: Run HTTP tests**

Run:

```bash
cd corpscout/scheduler
GOWORK=off go test ./internal/httpapi -run TestStartNACETaxonomySyncWorkflow -count=1
```

Expected: pass.

## Task 8: Verification

**Files:**

- No new files unless tests reveal a bug.

- [ ] **Step 1: Regenerate sqlc**

Run:

```bash
cd corpscout/database
sqlc generate
```

Expected: no output and exit 0.

- [ ] **Step 2: Run focused tests**

Run:

```bash
cd corpscout/scheduler
GOWORK=off go test ./internal/db ./internal/db/gen ./internal/nacetaxonomy ./internal/httpapi ./internal/app ./internal/config -count=1
```

Expected: pass.

- [ ] **Step 3: Run full scheduler suite**

Run:

```bash
cd corpscout/scheduler
GOWORK=off go test ./...
```

Expected: pass.

- [ ] **Step 4: Manual smoke after migrations**

After applying migrations locally:

```bash
curl -sS -X POST http://localhost:8094/api/v1/workflows/nace/taxonomy-sync \
  -H 'Content-Type: application/json' \
  -d '{"revision":"2.1","source_url":"https://example.invalid/nace.rdf"}'
```

Expected with invalid URL: workflow starts but later fails in Temporal with a structured activity error. With a real configured source URL:

- first run: `succeeded`
- second run with same source file hash: `skipped`

Check:

```sql
SELECT revision, content_sha256, status, processed_at
FROM nace_source_files
ORDER BY created_at DESC
LIMIT 5;

SELECT revision, import_status, content_sha256, records_imported, finished_at
FROM v_nace_source_file_imports
ORDER BY started_at DESC
LIMIT 5;
```

## Task 9: Commit

- [ ] **Step 1: Commit implementation**

```bash
git add corpscout/database/migrations/000072_nace_source_files.* \
  corpscout/database/queries/nace_taxonomy.sql \
  corpscout/scheduler/internal/db/gen \
  corpscout/scheduler/internal/db/nace_source_files_migration_test.go \
  corpscout/scheduler/internal/nacetaxonomy \
  corpscout/scheduler/internal/app \
  corpscout/scheduler/internal/config/config.go \
  corpscout/scheduler/internal/httpapi \
  corpscout/.env.example

git commit -m "feat: add NACE taxonomy sync workflow"
```

## Open Implementation Note

The official source URL must be configured. The plan intentionally does not bake a specific data.europa.eu or ShowVoc download URL into code, because those distribution URLs may move. The API can accept `source_url` per trigger, and production can set `CORPSCOUT_NACE_REV21_SOURCE_URL`.

## Self-Review

- Requirement coverage: The plan adds a table for file hashes, skips already processed hashes, and adds API + Temporal workflow/action to download and import NACE data.
- Scope control: UI, scheduled sync, BRREG remapping, and company table publishing are not included.
- Temporal safety: Large downloaded bytes are not returned from activities to workflow history.
- Architecture consistency: Uses concrete package/functions and direct Temporal registration. No registry/facade interfaces are introduced.
- Error handling: Boundary handlers return safe JSON errors; activities wrap internal errors with context.
