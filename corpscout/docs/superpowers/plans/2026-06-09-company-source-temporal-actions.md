# Company Source Temporal Actions Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Temporal-backed company source workflows for downloading source files, importing downloaded source files into ClickHouse, and running both steps as one composite workflow while storing run metadata in Postgres.

**Architecture:** Source packages own source-specific download and import logic. Temporal orchestration lives in one tree under `scheduler/internal/temporal`: shared contracts, workflows, actions, and a root registry that registers all company-source workflows and actions on the Temporal worker directly. Source files are stored as artifacts under a scheduler run root; all artifact metadata, Temporal IDs, status, inputs, results, and errors are stored in Postgres.

**Tech Stack:** Go, Temporal Go SDK, pgx/sqlc, PostgreSQL, ClickHouse native client, existing Corpscout React UI.

---

## Current Context

The source catalog already stores the current four sources in Postgres from embedded JSON declarations:

- `finland/prhytj`
- `united_states/coloradoentities`
- `united_states/irseobmf`
- `united_states/secedgar`

`data_source_actions` already has the two production actions:

- `pull_source` - download source data to a source artifact file.
- `import_clickhouse` - import a previously downloaded source artifact file into ClickHouse.

The composite "download and import" behavior must be a Temporal workflow that runs these two actions and records two action-run rows. Do not add a third `data_source_actions` row for the composite in this plan.

No filesystem manifest or run-index metadata should be introduced. Download result metadata is stored in `data_source_action_runs.result`.

## File Structure

- `scheduler/internal/companysources/source.go`
  - Extend the source interface with `Download`.
  - Add download option/result types.
- `scheduler/internal/companysources/download.go`
  - Add `DownloadRun`, mirroring the current `ImportRun`.
- `scheduler/internal/companysources/download_http.go`
  - Shared HTTP download helpers for direct JSON/NDJSON source files.
- `scheduler/internal/companysources/finland/prhytj/download.go`
  - Source-specific PRH YTJ paged API download to `source.ndjson`.
- `scheduler/internal/companysources/unitedstates/coloradoentities/download.go`
  - Source-specific Socrata JSON array download to `source.ndjson`.
- `scheduler/internal/companysources/unitedstates/irseobmf/download.go`
  - Source-specific IRS EO BMF download/normalization to `source.ndjson`.
- `scheduler/internal/companysources/unitedstates/secedgar/download.go`
  - Source-specific SEC EDGAR direct download to `source.json`.
- `scheduler/internal/temporal/workflow/companysources/workflow.go`
  - Own the Temporal contract for company sources: task queue, workflow names, activity names, action keys, input/result types, and workflows.
  - Add `DownloadSource`, `ImportSourceToClickHouse`, and `SyncSourceToClickHouse` workflows.
- `scheduler/internal/temporal/actions/companysources/actions.go`
  - Add concrete Temporal activity receiver.
  - Own Postgres action-run updates and calls to source registry.
- `scheduler/internal/temporal/registry.go`
  - Root Temporal registry for company-source workflows and actions.
  - This is the single place that registers these workflows/actions with a Temporal worker.
- `scheduler/internal/app/temporal.go`
  - Wire source registry and company-source Temporal actions.
- `scheduler/internal/config/config.go`
  - Add source run root and ClickHouse native URL configuration.
- `database/queries/sources.sql`
  - Add action/action-run mutation queries.
- `scheduler/internal/httpapi/source_actions.go`
  - Add source action list/run/trigger handlers.
- `scheduler/internal/httpapi/handlers.go`
  - Register source action routes.
- `ui/app/types/api.ts`
  - Add source action/run types.
- `ui/app/lib/api.ts`
  - Add source action API client methods.
- `ui/app/components/app/source-detail/ActionsTab.tsx`
  - New source actions and run history view.
- `ui/app/components/app/source-detail/sourceDetailUtils.ts`
  - Add an Actions tab.
- `ui/app/routes/sources_.$name.actions.tsx`
  - Route for the Actions tab.

---

### Task 1: Add Source Action Run Queries

**Files:**
- Modify: `database/queries/sources.sql`
- Generated: `scheduler/internal/db/gen/sources.sql.go`
- Test: `scheduler/internal/db/source_action_runs_query_shape_test.go`

- [ ] **Step 1: Write failing query-shape test**

Create `scheduler/internal/db/source_action_runs_query_shape_test.go`:

```go
package db_test

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSourceActionRunQueriesExist(t *testing.T) {
	source, err := os.ReadFile("../../../database/queries/sources.sql")
	require.NoError(t, err)
	sql := string(source)

	for _, queryName := range []string{
		"-- name: GetSourceActionByName :one",
		"-- name: CreateSourceActionRun :one",
		"-- name: GetSourceActionRun :one",
		"-- name: GetLatestSuccessfulSourceActionRun :one",
		"-- name: FinishSourceActionRun :one",
	} {
		require.True(t, strings.Contains(sql, queryName), "missing %s", queryName)
	}
}

func TestCreateSourceActionRunDerivesActionIdentity(t *testing.T) {
	source, err := os.ReadFile("../../../database/queries/sources.sql")
	require.NoError(t, err)
	sql := string(source)

	createQuery := sql[strings.Index(sql, "-- name: CreateSourceActionRun :one"):]
	createQuery = createQuery[:strings.Index(createQuery, "-- name: GetSourceActionRun :one")]

	require.Contains(t, createQuery, "SELECT\n  a.source_id,\n  a.id,\n  a.action,")
	require.Contains(t, createQuery, "FROM data_source_actions a")
	require.Contains(t, createQuery, "WHERE a.id = sqlc.arg(action_id)")
	require.NotContains(t, createQuery, "$1, $2, $3, 'running'")
}
```

- [ ] **Step 2: Verify RED**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/db -run TestSourceActionRunQueriesExist -count=1
```

Expected: FAIL because the query names are not present.

- [ ] **Step 3: Add SQL queries**

Append to `database/queries/sources.sql`:

```sql
-- name: GetSourceActionByName :one
SELECT
  a.id,
  a.source_id,
  s.name AS source_name,
  s.country,
  s.source,
  s.registry_key,
  COALESCE(s.source_url, '') AS source_url,
  COALESCE(s.source_file_name, '') AS source_file_name,
  s.user_agent_required,
  a.action,
  a.display_name,
  a.temporal_workflow_type,
  a.temporal_task_queue,
  a.enabled,
  a.config
FROM data_source_actions a
JOIN data_sources s ON s.id = a.source_id
WHERE s.name = $1
  AND a.action = $2;

-- name: CreateSourceActionRun :one
INSERT INTO data_source_action_runs (
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

-- name: GetSourceActionRun :one
SELECT * FROM data_source_action_runs WHERE id = $1;

-- name: GetLatestSuccessfulSourceActionRun :one
SELECT r.*
FROM data_source_action_runs r
JOIN data_source_actions a ON a.id = r.action_id
JOIN data_sources s ON s.id = r.source_id
WHERE s.name = $1
  AND r.action = $2
  AND r.status = 'succeeded'
ORDER BY r.finished_at DESC
LIMIT 1;

-- name: FinishSourceActionRun :one
UPDATE data_source_action_runs
SET
  status = $2,
  finished_at = now(),
  result = $3,
  error_message = NULLIF($4, '')
WHERE id = $1
RETURNING *;
```

- [ ] **Step 4: Generate sqlc**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off sqlc generate -f ../database/sqlc.yaml
```

Expected: exit 0 and generated methods in `scheduler/internal/db/gen/sources.sql.go`.

- [ ] **Step 5: Verify GREEN**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/db -run TestSourceActionRunQueriesExist -count=1
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add database/queries/sources.sql scheduler/internal/db/gen/sources.sql.go scheduler/internal/db/gen/querier.go scheduler/internal/db/source_action_runs_query_shape_test.go
git commit -m "feat: add source action run queries"
```

---

### Task 2: Add Source Download Contract

**Files:**
- Modify: `scheduler/internal/companysources/source.go`
- Create: `scheduler/internal/companysources/download.go`
- Test: `scheduler/internal/companysources/download_test.go`

- [ ] **Step 1: Write failing download orchestration test**

Create `scheduler/internal/companysources/download_test.go`:

```go
package companysources

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

type downloadingSource struct {
	key Key
}

func (s downloadingSource) Key() Key { return s.key }
func (s downloadingSource) DisplayName() string { return "Downloading Source" }
func (s downloadingSource) Download(ctx context.Context, opts DownloadOptions) (DownloadResult, error) {
	return DownloadResult{
		RunDir:             opts.RunDir,
		SourceFilePath:     opts.RunDir + "/" + opts.SourceFileName,
		SourceFileName:     opts.SourceFileName,
		ContentSHA256:      "abc123",
		ContentLengthBytes: 27,
		RecordsWritten:     2,
	}, nil
}
func (s downloadingSource) Import(ctx context.Context, opts ImportOptions) (ImportResult, error) {
	return ImportResult{RunDir: opts.RunDir, ImportedRows: 0}, nil
}

func TestDownloadRunCallsRegisteredSource(t *testing.T) {
	registry := NewRegistry(downloadingSource{key: Key{Country: "finland", Source: "prhytj"}})

	result, err := DownloadRun(context.Background(), registry, DownloadRunRequest{
		Country:           "finland",
		Source:            "prhytj",
		RunDir:            "/tmp/run",
		SourceURL:         "https://example.test/source",
		SourceFileName:    "source.ndjson",
		UserAgentRequired: false,
	})

	require.NoError(t, err)
	require.Equal(t, "/tmp/run", result.RunDir)
	require.Equal(t, "source.ndjson", result.SourceFileName)
	require.Equal(t, int64(2), result.RecordsWritten)
}
```

- [ ] **Step 2: Verify RED**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/companysources -run TestDownloadRunCallsRegisteredSource -count=1
```

Expected: FAIL because `DownloadOptions`, `DownloadResult`, and `DownloadRun` are undefined.

- [ ] **Step 3: Add download types**

Modify `scheduler/internal/companysources/source.go`:

```go
type Source interface {
	Key() Key
	DisplayName() string
	Download(ctx context.Context, opts DownloadOptions) (DownloadResult, error)
	Import(ctx context.Context, opts ImportOptions) (ImportResult, error)
}

type DownloadOptions struct {
	RunDir            string
	SourceURL         string
	SourceFileName    string
	UserAgentRequired bool
}

type DownloadResult struct {
	RunDir             string `json:"run_dir"`
	SourceFilePath     string `json:"source_file_path"`
	SourceFileName     string `json:"source_file_name"`
	ContentSHA256      string `json:"content_sha256"`
	ContentLengthBytes int64  `json:"content_length_bytes"`
	RecordsWritten     int64  `json:"records_written"`
}

type DownloadRunRequest struct {
	Country           string
	Source            string
	RunDir            string
	SourceURL         string
	SourceFileName    string
	UserAgentRequired bool
}
```

- [ ] **Step 4: Add `DownloadRun`**

Create `scheduler/internal/companysources/download.go`:

```go
package companysources

import "context"

func DownloadRun(ctx context.Context, registry Registry, req DownloadRunRequest) (DownloadResult, error) {
	source, err := registry.Get(req.Country, req.Source)
	if err != nil {
		return DownloadResult{}, err
	}
	return source.Download(ctx, DownloadOptions{
		RunDir:            req.RunDir,
		SourceURL:         req.SourceURL,
		SourceFileName:    req.SourceFileName,
		UserAgentRequired: req.UserAgentRequired,
	})
}
```

- [ ] **Step 5: Add temporary compile implementations**

Each current source struct must satisfy `Source`. Add this method temporarily to each source package; source-specific tasks replace it:

```go
func (Source) Download(ctx context.Context, opts companysources.DownloadOptions) (companysources.DownloadResult, error) {
	return companysources.DownloadResult{}, errors.New("source download is not implemented")
}
```

Files:

- `scheduler/internal/companysources/finland/prhytj/download.go`
- `scheduler/internal/companysources/unitedstates/coloradoentities/download.go`
- `scheduler/internal/companysources/unitedstates/irseobmf/download.go`
- `scheduler/internal/companysources/unitedstates/secedgar/download.go`

- [ ] **Step 6: Verify GREEN**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/companysources -run TestDownloadRunCallsRegisteredSource -count=1
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add scheduler/internal/companysources/source.go scheduler/internal/companysources/download.go scheduler/internal/companysources/download_test.go scheduler/internal/companysources/finland/prhytj/download.go scheduler/internal/companysources/unitedstates/coloradoentities/download.go scheduler/internal/companysources/unitedstates/irseobmf/download.go scheduler/internal/companysources/unitedstates/secedgar/download.go
git commit -m "feat: add company source download contract"
```

---

### Task 3: Add Shared Download File Helpers

**Files:**
- Create: `scheduler/internal/companysources/download_http.go`
- Test: `scheduler/internal/companysources/download_http_test.go`

- [ ] **Step 1: Write failing helper tests**

Create `scheduler/internal/companysources/download_http_test.go`:

```go
package companysources

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDownloadDirectFileWritesSourceFileAndHash(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "Corpscout Company Source Downloader", r.Header.Get("User-Agent"))
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()

	runDir := t.TempDir()
	result, err := DownloadDirectFile(context.Background(), http.DefaultClient, DirectFileDownload{
		URL:               server.URL,
		RunDir:            runDir,
		SourceFileName:    "source.json",
		UserAgentRequired: true,
	})

	require.NoError(t, err)
	require.Equal(t, filepath.Join(runDir, "source.json"), result.SourceFilePath)
	require.Equal(t, int64(len(`{"ok":true}`)), result.ContentLengthBytes)
	require.Len(t, result.ContentSHA256, 64)
	body, err := os.ReadFile(result.SourceFilePath)
	require.NoError(t, err)
	require.JSONEq(t, `{"ok":true}`, string(body))
}

func TestWriteJSONArrayAsNDJSONPreservesEachObject(t *testing.T) {
	runDir := t.TempDir()
	path := filepath.Join(runDir, "source.ndjson")
	records := []json.RawMessage{json.RawMessage(`{"a":1}`), json.RawMessage(`{"b":2}`)}

	written, err := WriteRawMessagesAsNDJSON(path, records)

	require.NoError(t, err)
	require.Equal(t, int64(2), written.RecordsWritten)
	body, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "{\"a\":1}\n{\"b\":2}\n", strings.ReplaceAll(string(body), " ", ""))
}
```

- [ ] **Step 2: Verify RED**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/companysources -run 'TestDownloadDirectFileWritesSourceFileAndHash|TestWriteJSONArrayAsNDJSONPreservesEachObject' -count=1
```

Expected: FAIL because helper functions are undefined.

- [ ] **Step 3: Implement helpers**

Create `scheduler/internal/companysources/download_http.go`:

```go
package companysources

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
)

const DownloadUserAgent = "Corpscout Company Source Downloader"

type DirectFileDownload struct {
	URL               string
	RunDir            string
	SourceFileName    string
	UserAgentRequired bool
}

type FileWriteResult struct {
	SourceFilePath     string
	ContentSHA256      string
	ContentLengthBytes int64
	RecordsWritten     int64
}

func DownloadDirectFile(ctx context.Context, client *http.Client, req DirectFileDownload) (FileWriteResult, error) {
	if client == nil {
		client = http.DefaultClient
	}
	if req.URL == "" {
		return FileWriteResult{}, errors.New("source url is required")
	}
	if req.RunDir == "" {
		return FileWriteResult{}, errors.New("run dir is required")
	}
	if req.SourceFileName == "" {
		return FileWriteResult{}, errors.New("source file name is required")
	}
	if err := os.MkdirAll(req.RunDir, 0o755); err != nil {
		return FileWriteResult{}, errors.Wrap(err, "create source run directory")
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, req.URL, nil)
	if err != nil {
		return FileWriteResult{}, errors.Wrap(err, "create source download request")
	}
	if req.UserAgentRequired {
		httpReq.Header.Set("User-Agent", DownloadUserAgent)
	}

	resp, err := client.Do(httpReq)
	if err != nil {
		return FileWriteResult{}, errors.Wrap(err, "download source file")
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return FileWriteResult{}, errors.Errorf("download source file: status %d", resp.StatusCode)
	}

	path := filepath.Join(req.RunDir, req.SourceFileName)
	file, err := os.Create(path)
	if err != nil {
		return FileWriteResult{}, errors.Wrap(err, "create source file")
	}
	defer file.Close()

	hasher := sha256.New()
	written, err := io.Copy(io.MultiWriter(file, hasher), resp.Body)
	if err != nil {
		return FileWriteResult{}, errors.Wrap(err, "write source file")
	}

	return FileWriteResult{
		SourceFilePath:     path,
		ContentSHA256:      hex.EncodeToString(hasher.Sum(nil)),
		ContentLengthBytes: written,
		RecordsWritten:     0,
	}, nil
}

func WriteRawMessagesAsNDJSON(path string, records []json.RawMessage) (FileWriteResult, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return FileWriteResult{}, errors.Wrap(err, "create ndjson directory")
	}
	file, err := os.Create(path)
	if err != nil {
		return FileWriteResult{}, errors.Wrap(err, "create ndjson file")
	}
	defer file.Close()

	hasher := sha256.New()
	writer := bufio.NewWriter(io.MultiWriter(file, hasher))
	var bytesWritten int64
	for _, record := range records {
		trimmed := json.RawMessage(record)
		n, err := writer.Write(trimmed)
		if err != nil {
			return FileWriteResult{}, errors.Wrap(err, "write ndjson record")
		}
		bytesWritten += int64(n)
		n, err = writer.WriteString("\n")
		if err != nil {
			return FileWriteResult{}, errors.Wrap(err, "write ndjson newline")
		}
		bytesWritten += int64(n)
	}
	if err := writer.Flush(); err != nil {
		return FileWriteResult{}, errors.Wrap(err, "flush ndjson file")
	}

	return FileWriteResult{
		SourceFilePath:     path,
		ContentSHA256:      hex.EncodeToString(hasher.Sum(nil)),
		ContentLengthBytes: bytesWritten,
		RecordsWritten:     int64(len(records)),
	}, nil
}
```

- [ ] **Step 5: Verify GREEN**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/companysources -run 'TestDownloadDirectFileWritesSourceFileAndHash|TestWriteJSONArrayAsNDJSONPreservesEachObject' -count=1
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add scheduler/internal/companysources/download_http.go scheduler/internal/companysources/download_http_test.go
git commit -m "feat: add source download helpers"
```

---

### Task 4: Implement Source-Specific Downloads

**Files:**
- Modify: `scheduler/internal/companysources/finland/prhytj/download.go`
- Modify: `scheduler/internal/companysources/unitedstates/coloradoentities/download.go`
- Modify: `scheduler/internal/companysources/unitedstates/irseobmf/download.go`
- Modify: `scheduler/internal/companysources/unitedstates/secedgar/download.go`
- Tests beside each source package.

- [ ] **Step 1: Add PRH YTJ download test**

Create `scheduler/internal/companysources/finland/prhytj/download_test.go`:

```go
package prhytj

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pulsarpoint/corpscout/scheduler/internal/companysources"
	"github.com/stretchr/testify/require"
)

func TestDownloadWritesPRHCompaniesAsNDJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"companies":[{"businessId":{"type":"businessId","value":"1234567-8"}},{"businessId":{"type":"businessId","value":"2345678-9"}}]}`))
	}))
	defer server.Close()

	runDir := t.TempDir()
	result, err := Source{}.Download(context.Background(), companysources.DownloadOptions{
		RunDir:         runDir,
		SourceURL:      server.URL,
		SourceFileName: "source.ndjson",
	})

	require.NoError(t, err)
	require.Equal(t, int64(2), result.RecordsWritten)
	body, err := os.ReadFile(filepath.Join(runDir, "source.ndjson"))
	require.NoError(t, err)
	require.Contains(t, string(body), `"1234567-8"`)
	require.Equal(t, 2, strings.Count(string(body), "\n"))
}
```

- [ ] **Step 2: Add Colorado download test**

Create `scheduler/internal/companysources/unitedstates/coloradoentities/download_test.go`:

```go
package coloradoentities

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pulsarpoint/corpscout/scheduler/internal/companysources"
	"github.com/stretchr/testify/require"
)

func TestDownloadWritesSocrataArrayAsNDJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`[{"entityid":"1","entityname":"One"},{"entityid":"2","entityname":"Two"}]`))
	}))
	defer server.Close()

	runDir := t.TempDir()
	result, err := Source{}.Download(context.Background(), companysources.DownloadOptions{
		RunDir:         runDir,
		SourceURL:      server.URL,
		SourceFileName: "source.ndjson",
	})

	require.NoError(t, err)
	require.Equal(t, int64(2), result.RecordsWritten)
	body, err := os.ReadFile(filepath.Join(runDir, "source.ndjson"))
	require.NoError(t, err)
	require.Equal(t, 2, strings.Count(string(body), "\n"))
}
```

- [ ] **Step 3: Add IRS EO BMF download test**

Create `scheduler/internal/companysources/unitedstates/irseobmf/download_test.go`:

```go
package irseobmf

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pulsarpoint/corpscout/scheduler/internal/companysources"
	"github.com/stretchr/testify/require"
)

func TestDownloadWritesIRSRecordsAsNDJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`[{"ein":"1","name":"One"},{"ein":"2","name":"Two"}]`))
	}))
	defer server.Close()

	runDir := t.TempDir()
	result, err := Source{}.Download(context.Background(), companysources.DownloadOptions{
		RunDir:         runDir,
		SourceURL:      server.URL,
		SourceFileName: "source.ndjson",
	})

	require.NoError(t, err)
	require.Equal(t, int64(2), result.RecordsWritten)
	body, err := os.ReadFile(filepath.Join(runDir, "source.ndjson"))
	require.NoError(t, err)
	require.Equal(t, 2, strings.Count(string(body), "\n"))
}
```

- [ ] **Step 4: Add SEC EDGAR download test**

Create `scheduler/internal/companysources/unitedstates/secedgar/download_test.go`:

```go
package secedgar

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/pulsarpoint/corpscout/scheduler/internal/companysources"
	"github.com/stretchr/testify/require"
)

func TestDownloadWritesSECJSONFileWithUserAgent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, companysources.DownloadUserAgent, r.Header.Get("User-Agent"))
		_, _ = w.Write([]byte(`{"0":{"cik_str":320193,"ticker":"AAPL","title":"Apple Inc."}}`))
	}))
	defer server.Close()

	runDir := t.TempDir()
	result, err := Source{}.Download(context.Background(), companysources.DownloadOptions{
		RunDir:            runDir,
		SourceURL:         server.URL,
		SourceFileName:    "source.json",
		UserAgentRequired: true,
	})

	require.NoError(t, err)
	require.Equal(t, filepath.Join(runDir, "source.json"), result.SourceFilePath)
	body, err := os.ReadFile(result.SourceFilePath)
	require.NoError(t, err)
	require.Contains(t, string(body), `"AAPL"`)
}
```

- [ ] **Step 5: Verify RED**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/companysources/finland/prhytj ./internal/companysources/unitedstates/coloradoentities ./internal/companysources/unitedstates/irseobmf ./internal/companysources/unitedstates/secedgar -run Download -count=1
```

Expected: FAIL because source-specific download methods still return "source download is not implemented".

- [ ] **Step 6: Implement downloads**

Use the helper from Task 3.

For PRH YTJ:

```go
func (Source) Download(ctx context.Context, opts companysources.DownloadOptions) (companysources.DownloadResult, error) {
	if opts.SourceFileName == "" {
		opts.SourceFileName = "source.ndjson"
	}
	page, err := downloadPage(ctx, http.DefaultClient, opts.SourceURL)
	if err != nil {
		return companysources.DownloadResult{}, err
	}
	records := make([]json.RawMessage, 0, len(page.Companies))
	for _, company := range page.Companies {
		raw, err := json.Marshal(company)
		if err != nil {
			return companysources.DownloadResult{}, errors.Wrap(err, "encode PRH YTJ company")
		}
		records = append(records, raw)
	}
	written, err := companysources.WriteRawMessagesAsNDJSON(filepath.Join(opts.RunDir, opts.SourceFileName), records)
	if err != nil {
		return companysources.DownloadResult{}, err
	}
	return companysources.DownloadResult{
		RunDir:             opts.RunDir,
		SourceFilePath:     written.SourceFilePath,
		SourceFileName:     opts.SourceFileName,
		ContentSHA256:      written.ContentSHA256,
		ContentLengthBytes: written.ContentLengthBytes,
		RecordsWritten:     written.RecordsWritten,
	}, nil
}
```

For Colorado and IRS:

```go
func (Source) Download(ctx context.Context, opts companysources.DownloadOptions) (companysources.DownloadResult, error) {
	if opts.SourceFileName == "" {
		opts.SourceFileName = "source.ndjson"
	}
	records, err := downloadJSONArray(ctx, http.DefaultClient, opts.SourceURL, opts.UserAgentRequired)
	if err != nil {
		return companysources.DownloadResult{}, err
	}
	written, err := companysources.WriteRawMessagesAsNDJSON(filepath.Join(opts.RunDir, opts.SourceFileName), records)
	if err != nil {
		return companysources.DownloadResult{}, err
	}
	return companysources.DownloadResult{
		RunDir:             opts.RunDir,
		SourceFilePath:     written.SourceFilePath,
		SourceFileName:     opts.SourceFileName,
		ContentSHA256:      written.ContentSHA256,
		ContentLengthBytes: written.ContentLengthBytes,
		RecordsWritten:     written.RecordsWritten,
	}, nil
}
```

For SEC:

```go
func (Source) Download(ctx context.Context, opts companysources.DownloadOptions) (companysources.DownloadResult, error) {
	if opts.SourceFileName == "" {
		opts.SourceFileName = "source.json"
	}
	written, err := companysources.DownloadDirectFile(ctx, http.DefaultClient, companysources.DirectFileDownload{
		URL:               opts.SourceURL,
		RunDir:            opts.RunDir,
		SourceFileName:    opts.SourceFileName,
		UserAgentRequired: opts.UserAgentRequired,
	})
	if err != nil {
		return companysources.DownloadResult{}, err
	}
	return companysources.DownloadResult{
		RunDir:             opts.RunDir,
		SourceFilePath:     written.SourceFilePath,
		SourceFileName:     opts.SourceFileName,
		ContentSHA256:      written.ContentSHA256,
		ContentLengthBytes: written.ContentLengthBytes,
		RecordsWritten:     written.RecordsWritten,
	}, nil
}
```

- [ ] **Step 7: Verify GREEN**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/companysources/finland/prhytj ./internal/companysources/unitedstates/coloradoentities ./internal/companysources/unitedstates/irseobmf ./internal/companysources/unitedstates/secedgar -run Download -count=1
```

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add scheduler/internal/companysources/finland/prhytj/download.go scheduler/internal/companysources/finland/prhytj/download_test.go scheduler/internal/companysources/unitedstates/coloradoentities/download.go scheduler/internal/companysources/unitedstates/coloradoentities/download_test.go scheduler/internal/companysources/unitedstates/irseobmf/download.go scheduler/internal/companysources/unitedstates/irseobmf/download_test.go scheduler/internal/companysources/unitedstates/secedgar/download.go scheduler/internal/companysources/unitedstates/secedgar/download_test.go
git commit -m "feat: add source-specific downloads"
```

---

### Task 5: Add Company Source Temporal Workflows

**Files:**
- Create: `scheduler/internal/temporal/workflow/companysources/workflow.go`
- Test: `scheduler/internal/temporal/workflow/companysources/workflow_test.go`

- [ ] **Step 1: Write workflow unit tests with Temporal test suite**

Create `scheduler/internal/temporal/workflow/companysources/workflow_test.go`:

```go
package companysources

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
)

func TestSyncSourceToClickHouseRunsDownloadThenImport(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(DownloadSource)
	env.RegisterWorkflow(ImportSourceToClickHouse)
	env.RegisterWorkflow(SyncSourceToClickHouse)

	env.OnWorkflow(DownloadSource, SyncSourceDownloadInput{
		SourceName: "finland_prhytj",
		Trigger:    "manual",
	}).Return(DownloadSourceResult{
		ActionRunID:     "download-run-1",
		RunDir:          "/tmp/source-run",
		SourceFileName:  "source.ndjson",
		ContentSHA256:   "abc123",
		RecordsWritten:  2,
	}, nil)
	env.OnWorkflow(ImportSourceToClickHouse, ImportSourceToClickHouseInput{
		SourceName:          "finland_prhytj",
		Trigger:             "manual",
		DownloadActionRunID: "download-run-1",
		BatchSize:           1000,
	}).Return(ImportSourceToClickHouseResult{
		ActionRunID:  "import-run-1",
		ImportedRows: 2,
	}, nil)

	env.ExecuteWorkflow(SyncSourceToClickHouse, SyncSourceToClickHouseInput{
		SourceName: "finland_prhytj",
		Trigger:    "manual",
		BatchSize:  1000,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var result SyncSourceToClickHouseResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "download-run-1", result.Download.ActionRunID)
	require.Equal(t, "import-run-1", result.Import.ActionRunID)
}
```

- [ ] **Step 2: Verify RED**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/temporal/workflow/companysources -run TestSyncSourceToClickHouseRunsDownloadThenImport -count=1
```

Expected: FAIL because workflow functions/types are undefined.

- [ ] **Step 3: Implement workflow package contract and workflows**

Create `scheduler/internal/temporal/workflow/companysources/workflow.go`:

```go
package companysources

import (
	"time"

	"github.com/cockroachdb/errors"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	SourceTaskQueue = "corpscout-company-sources"

	DownloadSourceWorkflowName           = "CompanySourceDownloadWorkflow"
	ImportSourceToClickHouseWorkflowName = "CompanySourceClickHouseImportWorkflow"
	SyncSourceToClickHouseWorkflowName   = "CompanySourceSyncClickHouseWorkflow"
	DownloadSourceActivityName           = "DownloadSourceActivity"
	ImportSourceToClickHouseActivityName = "ImportSourceToClickHouseActivity"
	ActionPullSource                     = "pull_source"
	ActionImportClickHouse               = "import_clickhouse"
	StatusSucceeded                      = "succeeded"
	StatusFailed                         = "failed"
)

type SyncSourceDownloadInput struct {
	SourceName string `json:"source_name"`
	Trigger    string `json:"trigger"`
}

type ImportSourceToClickHouseInput struct {
	SourceName          string `json:"source_name"`
	Trigger             string `json:"trigger"`
	DownloadActionRunID string `json:"download_action_run_id"`
	BatchSize           int    `json:"batch_size"`
	Limit               int64  `json:"limit"`
}

type SyncSourceToClickHouseInput struct {
	SourceName string `json:"source_name"`
	Trigger    string `json:"trigger"`
	BatchSize  int    `json:"batch_size"`
	Limit      int64  `json:"limit"`
}

type DownloadSourceResult struct {
	ActionRunID        string `json:"action_run_id"`
	RunDir             string `json:"run_dir"`
	SourceFilePath     string `json:"source_file_path"`
	SourceFileName     string `json:"source_file_name"`
	ContentSHA256      string `json:"content_sha256"`
	ContentLengthBytes int64  `json:"content_length_bytes"`
	RecordsWritten    int64  `json:"records_written"`
}

type ImportSourceToClickHouseResult struct {
	ActionRunID    string   `json:"action_run_id"`
	ImportedTables []string `json:"imported_tables"`
	ImportedRows   int64    `json:"imported_rows"`
}

type SyncSourceToClickHouseResult struct {
	Download DownloadSourceResult           `json:"download"`
	Import   ImportSourceToClickHouseResult `json:"import"`
}

func DownloadSource(ctx workflow.Context, input SyncSourceDownloadInput) (DownloadSourceResult, error) {
	ctx = withSourceActivityOptions(ctx, 60*time.Minute)
	var result DownloadSourceResult
	if err := workflow.ExecuteActivity(ctx, DownloadSourceActivityName, input).Get(ctx, &result); err != nil {
		return DownloadSourceResult{}, errors.Wrap(err, "download source activity")
	}
	return result, nil
}

func ImportSourceToClickHouse(ctx workflow.Context, input ImportSourceToClickHouseInput) (ImportSourceToClickHouseResult, error) {
	ctx = withSourceActivityOptions(ctx, 2*time.Hour)
	var result ImportSourceToClickHouseResult
	if err := workflow.ExecuteActivity(ctx, ImportSourceToClickHouseActivityName, input).Get(ctx, &result); err != nil {
		return ImportSourceToClickHouseResult{}, errors.Wrap(err, "import source to clickhouse activity")
	}
	return result, nil
}

func SyncSourceToClickHouse(ctx workflow.Context, input SyncSourceToClickHouseInput) (SyncSourceToClickHouseResult, error) {
	if input.BatchSize <= 0 {
		input.BatchSize = 1000
	}
	if input.Trigger == "" {
		input.Trigger = "manual"
	}
	var downloaded DownloadSourceResult
	if err := workflow.ExecuteChildWorkflow(ctx, DownloadSource, SyncSourceDownloadInput{
		SourceName: input.SourceName,
		Trigger:    input.Trigger,
	}).Get(ctx, &downloaded); err != nil {
		return SyncSourceToClickHouseResult{}, errors.Wrap(err, "download source child workflow")
	}

	var imported ImportSourceToClickHouseResult
	if err := workflow.ExecuteChildWorkflow(ctx, ImportSourceToClickHouse, ImportSourceToClickHouseInput{
		SourceName:          input.SourceName,
		Trigger:             input.Trigger,
		DownloadActionRunID: downloaded.ActionRunID,
		BatchSize:           input.BatchSize,
		Limit:               input.Limit,
	}).Get(ctx, &imported); err != nil {
		return SyncSourceToClickHouseResult{}, errors.Wrap(err, "import source child workflow")
	}

	return SyncSourceToClickHouseResult{Download: downloaded, Import: imported}, nil
}

func withSourceActivityOptions(ctx workflow.Context, timeout time.Duration) workflow.Context {
	return workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: timeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    10 * time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    5 * time.Minute,
			MaximumAttempts:    3,
		},
	})
}
```

- [ ] **Step 4: Verify GREEN**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/temporal/workflow/companysources -run TestSyncSourceToClickHouseRunsDownloadThenImport -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add scheduler/internal/temporal/workflow/companysources/workflow.go scheduler/internal/temporal/workflow/companysources/workflow_test.go
git commit -m "feat: add company source temporal workflows"
```

---

### Task 6: Add Company Source Temporal Actions

**Files:**
- Create: `scheduler/internal/temporal/actions/companysources/actions.go`
- Test: `scheduler/internal/temporal/actions/companysources/actions_test.go`

- [ ] **Step 1: Write action tests for Postgres-backed run records**

Create `scheduler/internal/temporal/actions/companysources/actions_test.go`:

```go
package companysources

import (
	"testing"

	"github.com/stretchr/testify/require"

	companysourceworkflow "github.com/pulsarpoint/corpscout/scheduler/internal/temporal/workflow/companysources"
)

func TestDownloadRunResultJSONIncludesArtifactMetadata(t *testing.T) {
	result := companysourceworkflow.DownloadSourceResult{
		ActionRunID:        "run-id",
		RunDir:             "/var/lib/corpscout/source-runs/finland/prhytj/run-id",
		SourceFilePath:     "/var/lib/corpscout/source-runs/finland/prhytj/run-id/source.ndjson",
		SourceFileName:     "source.ndjson",
		ContentSHA256:      "abc123",
		ContentLengthBytes: 42,
		RecordsWritten:     2,
	}

	payload := marshalActionResult(result)
	require.JSONEq(t, `{
		"action_run_id":"run-id",
		"run_dir":"/var/lib/corpscout/source-runs/finland/prhytj/run-id",
		"source_file_path":"/var/lib/corpscout/source-runs/finland/prhytj/run-id/source.ndjson",
		"source_file_name":"source.ndjson",
		"content_sha256":"abc123",
		"content_length_bytes":42,
		"records_written":2
	}`, string(payload))
}
```

- [ ] **Step 2: Verify RED**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/temporal/actions/companysources -run TestDownloadRunResultJSONIncludesArtifactMetadata -count=1
```

Expected: FAIL because `marshalActionResult` is undefined.

- [ ] **Step 3: Implement `Actions`**

Create `scheduler/internal/temporal/actions/companysources/actions.go`:

```go
package companysources

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.temporal.io/sdk/activity"

	sourcecore "github.com/pulsarpoint/corpscout/scheduler/internal/companysources"
	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
	companysourceworkflow "github.com/pulsarpoint/corpscout/scheduler/internal/temporal/workflow/companysources"
)

type Actions struct {
	pool                *pgxpool.Pool
	registry            sourcecore.Registry
	sourceRunsRoot      string
	clickHouseNativeURL string
}

func NewActions(pool *pgxpool.Pool, registry sourcecore.Registry, sourceRunsRoot string, clickHouseNativeURL string) *Actions {
	return &Actions{
		pool:                pool,
		registry:            registry,
		sourceRunsRoot:      strings.TrimSpace(sourceRunsRoot),
		clickHouseNativeURL: strings.TrimSpace(clickHouseNativeURL),
	}
}

func (a *Actions) DownloadSourceActivity(ctx context.Context, input companysourceworkflow.SyncSourceDownloadInput) (companysourceworkflow.DownloadSourceResult, error) {
	if a == nil || a.pool == nil {
		return companysourceworkflow.DownloadSourceResult{}, errors.New("company source database is not available")
	}
	queries := db.New(a.pool)
	action, err := queries.GetSourceActionByName(ctx, db.GetSourceActionByNameParams{
		Name:   input.SourceName,
		Action: companysourceworkflow.ActionPullSource,
	})
	if err != nil {
		return companysourceworkflow.DownloadSourceResult{}, errors.Wrap(err, "get pull source action")
	}

	workflowID, workflowRunID := workflowExecutionFromContext(ctx)
	run, err := queries.CreateSourceActionRun(ctx, db.CreateSourceActionRunParams{
		ActionID:           action.ID,
		TemporalWorkflowID: &workflowID,
		TemporalRunID:      &workflowRunID,
		Input:              marshalActionResult(input),
	})
	if err != nil {
		return companysourceworkflow.DownloadSourceResult{}, errors.Wrap(err, "create pull source action run")
	}

	runID := time.Now().UTC().Format("20060102T150405Z") + "-" + action.Country + "-" + action.Source + "-" + run.ID.String()
	runDir := filepath.Join(a.sourceRunsRoot, action.Country, action.Source, runID)
	downloaded, err := sourcecore.DownloadRun(ctx, a.registry, sourcecore.DownloadRunRequest{
		Country:           action.Country,
		Source:            action.Source,
		RunDir:            runDir,
		SourceURL:         action.SourceUrl,
		SourceFileName:    action.SourceFileName,
		UserAgentRequired: action.UserAgentRequired,
	})
	if err != nil {
		_, _ = queries.FinishSourceActionRun(ctx, db.FinishSourceActionRunParams{
			ID:           run.ID,
			Status:       companysourceworkflow.StatusFailed,
			Result:       marshalJSONObject(map[string]any{}),
			ErrorMessage: err.Error(),
		})
		return companysourceworkflow.DownloadSourceResult{ActionRunID: run.ID.String()}, errors.Wrap(err, "download source")
	}

	result := companysourceworkflow.DownloadSourceResult{
		ActionRunID:        run.ID.String(),
		RunDir:             downloaded.RunDir,
		SourceFilePath:     downloaded.SourceFilePath,
		SourceFileName:     downloaded.SourceFileName,
		ContentSHA256:      downloaded.ContentSHA256,
		ContentLengthBytes: downloaded.ContentLengthBytes,
		RecordsWritten:     downloaded.RecordsWritten,
	}
	finished, err := queries.FinishSourceActionRun(ctx, db.FinishSourceActionRunParams{
		ID:           run.ID,
		Status:       companysourceworkflow.StatusSucceeded,
		Result:       marshalActionResult(result),
		ErrorMessage: "",
	})
	if err != nil {
		return result, errors.Wrap(err, "finish pull source action run")
	}
	result.ActionRunID = finished.ID.String()
	return result, nil
}
```

Implement `ImportSourceToClickHouseActivity` in the same file:

```go
func (a *Actions) ImportSourceToClickHouseActivity(ctx context.Context, input companysourceworkflow.ImportSourceToClickHouseInput) (companysourceworkflow.ImportSourceToClickHouseResult, error) {
	if a == nil || a.pool == nil {
		return companysourceworkflow.ImportSourceToClickHouseResult{}, errors.New("company source database is not available")
	}
	queries := db.New(a.pool)
	action, err := queries.GetSourceActionByName(ctx, db.GetSourceActionByNameParams{
		Name:   input.SourceName,
		Action: companysourceworkflow.ActionImportClickHouse,
	})
	if err != nil {
		return companysourceworkflow.ImportSourceToClickHouseResult{}, errors.Wrap(err, "get import clickhouse action")
	}
	downloadActionRunID, err := uuid.Parse(input.DownloadActionRunID)
	if err != nil {
		return companysourceworkflow.ImportSourceToClickHouseResult{}, errors.Wrap(err, "parse download action run id")
	}
	downloadRun, err := queries.GetSourceActionRun(ctx, downloadActionRunID)
	if err != nil {
		return companysourceworkflow.ImportSourceToClickHouseResult{}, errors.Wrap(err, "get downloaded source action run")
	}
	if downloadRun.Action != companysourceworkflow.ActionPullSource || downloadRun.Status != companysourceworkflow.StatusSucceeded {
		return companysourceworkflow.ImportSourceToClickHouseResult{}, errors.New("download action run must be a succeeded pull_source run")
	}
	runDir, err := runDirFromDownloadResult(downloadRun.Result)
	if err != nil {
		return companysourceworkflow.ImportSourceToClickHouseResult{}, err
	}

	workflowID, workflowRunID := workflowExecutionFromContext(ctx)
	run, err := queries.CreateSourceActionRun(ctx, db.CreateSourceActionRunParams{
		ActionID:           action.ID,
		TemporalWorkflowID: &workflowID,
		TemporalRunID:      &workflowRunID,
		Input:              marshalActionResult(input),
	})
	if err != nil {
		return companysourceworkflow.ImportSourceToClickHouseResult{}, errors.Wrap(err, "create import clickhouse action run")
	}

	imported, err := sourcecore.ImportRun(ctx, a.registry, sourcecore.ImportRunRequest{
		Country:             action.Country,
		Source:              action.Source,
		RunDir:              runDir,
		ClickHouseNativeURL: a.clickHouseNativeURL,
		BatchSize:           input.BatchSize,
		Limit:               input.Limit,
	})
	if err != nil {
		_, _ = queries.FinishSourceActionRun(ctx, db.FinishSourceActionRunParams{
			ID:           run.ID,
			Status:       companysourceworkflow.StatusFailed,
			Result:       marshalJSONObject(map[string]any{}),
			ErrorMessage: err.Error(),
		})
		return companysourceworkflow.ImportSourceToClickHouseResult{ActionRunID: run.ID.String()}, errors.Wrap(err, "import source into clickhouse")
	}

	result := companysourceworkflow.ImportSourceToClickHouseResult{
		ActionRunID:    run.ID.String(),
		ImportedTables: imported.ImportedTables,
		ImportedRows:   imported.ImportedRows,
	}
	if _, err := queries.FinishSourceActionRun(ctx, db.FinishSourceActionRunParams{
		ID:           run.ID,
		Status:       companysourceworkflow.StatusSucceeded,
		Result:       marshalActionResult(result),
		ErrorMessage: "",
	}); err != nil {
		return result, errors.Wrap(err, "finish import clickhouse action run")
	}
	return result, nil
}
```

Add helpers:

```go
func marshalActionResult(value any) json.RawMessage {
	body, err := json.Marshal(value)
	if err != nil {
		return json.RawMessage(`{}`)
	}
	return body
}

func marshalJSONObject(value map[string]any) json.RawMessage {
	body, err := json.Marshal(value)
	if err != nil {
		return json.RawMessage(`{}`)
	}
	return body
}

func runDirFromDownloadResult(raw json.RawMessage) (string, error) {
	var result companysourceworkflow.DownloadSourceResult
	if err := json.Unmarshal(raw, &result); err != nil {
		return "", errors.Wrap(err, "decode download action result")
	}
	if strings.TrimSpace(result.RunDir) == "" {
		return "", errors.New("download action result has empty run_dir")
	}
	return result.RunDir, nil
}

func workflowExecutionFromContext(ctx context.Context) (string, string) {
	info := activity.GetInfo(ctx)
	if info.WorkflowExecution.ID != "" {
		return info.WorkflowExecution.ID, info.WorkflowExecution.RunID
	}
	return "", ""
}
```

- [ ] **Step 4: Verify GREEN for helper test**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/temporal/actions/companysources -run TestDownloadRunResultJSONIncludesArtifactMetadata -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add scheduler/internal/temporal/actions/companysources/actions.go scheduler/internal/temporal/actions/companysources/actions_test.go
git commit -m "feat: add company source temporal activities"
```

---

### Task 7: Register Company Source Temporal Worker

**Files:**
- Modify: `scheduler/internal/config/config.go`
- Modify: `docker-compose.yml`
- Modify: `scheduler/internal/app/temporal.go`
- Create: `scheduler/internal/temporal/registry.go`
- Test: `scheduler/internal/app/temporal_test.go`

- [ ] **Step 1: Add registry registration test**

Append to `scheduler/internal/app/temporal_test.go`:

```go
func TestCompanySourceTemporalRegistryExists(t *testing.T) {
	body, err := os.ReadFile("../temporal/registry.go")
	require.NoError(t, err)
	source := string(body)
	require.Contains(t, source, "RegisterCompanySourceWorker")
	require.Contains(t, source, "RegisterWorkflowWithOptions(companysourceworkflows.DownloadSource")
	require.Contains(t, source, "RegisterActivityWithOptions(resources.Actions.DownloadSourceActivity")
}
```

- [ ] **Step 2: Verify RED**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/app -run TestCompanySourceTemporalRegistryExists -count=1
```

Expected: FAIL because `../temporal/registry.go` does not exist.

- [ ] **Step 3: Add config**

Modify `scheduler/internal/config/config.go`:

```go
type Config struct {
	// existing fields
	SourceRunsRoot      string
	ClickHouseNativeURL string
}
```

Set defaults in `Load()`:

```go
SourceRunsRoot:      getEnv("CORPSCOUT_SOURCE_RUNS_ROOT", "/var/lib/corpscout/source-runs"),
ClickHouseNativeURL: getEnv("CORPSCOUT_CLICKHOUSE_NATIVE_URL", os.Getenv("CLICKHOUSE_NATIVE_URL")),
```

Update `docker-compose.yml` scheduler environment:

```yaml
CORPSCOUT_SOURCE_RUNS_ROOT: /var/lib/corpscout/source-runs
CORPSCOUT_CLICKHOUSE_NATIVE_URL: ${CLICKHOUSE_NATIVE_URL:-clickhouse://companycollect:9002?username=default&password=change-me&database=corpscout_sources}
```

Add volume:

```yaml
- ./data/source-runs:/var/lib/corpscout/source-runs
```

- [ ] **Step 4: Wire actions resources**

Modify `scheduler/internal/app/temporal.go`:

```go
type temporalWorkerResources struct {
	naceTaxonomyActions   *nacetaxonomy.Actions
	fxActions             *fx.Actions
	companySourceActions  *companysourceactions.Actions
}
```

In `newTemporalWorkerResources`:

```go
sourceRegistry := companysources.NewRegistry(
	prhytj.Source{},
	coloradoentities.Source{},
	irseobmf.Source{},
	secedgar.Source{},
)
```

Set:

```go
companySourceActions: companysourceactions.NewActions(pool, sourceRegistry, cfg.SourceRunsRoot, cfg.ClickHouseNativeURL),
```

In `newTemporalWorkers` append:

```go
newCompanySourcesTemporalWorker(temporalClient, resources),
```

- [ ] **Step 5: Create root Temporal registry**

Create `scheduler/internal/temporal/registry.go`:

```go
package temporal

import (
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	companysourceactions "github.com/pulsarpoint/corpscout/scheduler/internal/temporal/actions/companysources"
	companysourceworkflows "github.com/pulsarpoint/corpscout/scheduler/internal/temporal/workflow/companysources"
)

type CompanySourceResources struct {
	Actions *companysourceactions.Actions
}

func RegisterCompanySourceWorker(w worker.Worker, resources CompanySourceResources) {
	w.RegisterWorkflowWithOptions(
		companysourceworkflows.DownloadSource,
		workflow.RegisterOptions{Name: companysourceworkflows.DownloadSourceWorkflowName},
	)
	w.RegisterWorkflowWithOptions(
		companysourceworkflows.ImportSourceToClickHouse,
		workflow.RegisterOptions{Name: companysourceworkflows.ImportSourceToClickHouseWorkflowName},
	)
	w.RegisterWorkflowWithOptions(
		companysourceworkflows.SyncSourceToClickHouse,
		workflow.RegisterOptions{Name: companysourceworkflows.SyncSourceToClickHouseWorkflowName},
	)
	w.RegisterActivityWithOptions(
		resources.Actions.DownloadSourceActivity,
		activity.RegisterOptions{Name: companysourceworkflows.DownloadSourceActivityName},
	)
	w.RegisterActivityWithOptions(
		resources.Actions.ImportSourceToClickHouseActivity,
		activity.RegisterOptions{Name: companysourceworkflows.ImportSourceToClickHouseActivityName},
	)
}
```

- [ ] **Step 6: Register the company-source worker from app wiring**

Modify `scheduler/internal/app/temporal.go`:

```go
func newCompanySourcesTemporalWorker(temporalClient client.Client, resources *temporalWorkerResources) worker.Worker {
	slog.Debug("creating company sources temporal worker", "task_queue", companysourceworkflows.SourceTaskQueue)
	w := worker.New(temporalClient, companysourceworkflows.SourceTaskQueue, worker.Options{})
	temporalregistry.RegisterCompanySourceWorker(w, temporalregistry.CompanySourceResources{
		Actions: resources.companySourceActions,
	})
	return w
}
```

The file must import:

```go
companysourceactions "github.com/pulsarpoint/corpscout/scheduler/internal/temporal/actions/companysources"
temporalregistry "github.com/pulsarpoint/corpscout/scheduler/internal/temporal"
companysourceworkflows "github.com/pulsarpoint/corpscout/scheduler/internal/temporal/workflow/companysources"
```

- [ ] **Step 7: Verify GREEN**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/app -run TestCompanySourceTemporalRegistryExists -count=1
```

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add scheduler/internal/config/config.go docker-compose.yml scheduler/internal/app/temporal.go scheduler/internal/temporal/registry.go scheduler/internal/app/temporal_test.go
git commit -m "feat: register company source temporal worker"
```

---

### Task 8: Add Source Action HTTP API

**Files:**
- Create: `scheduler/internal/httpapi/source_actions.go`
- Modify: `scheduler/internal/httpapi/handlers.go`
- Test: `scheduler/internal/httpapi/source_actions_test.go`
- Modify: `scheduler/internal/httpapi/testhelpers_test.go`

- [ ] **Step 1: Write failing API tests**

Create `scheduler/internal/httpapi/source_actions_test.go`:

```go
package httpapi_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
	"github.com/pulsarpoint/corpscout/scheduler/internal/httpapi"
	companysourceworkflows "github.com/pulsarpoint/corpscout/scheduler/internal/temporal/workflow/companysources"
)

func TestListSourceActionsReturnsConfiguredActions(t *testing.T) {
	q := &stubQuerier{}
	sourceID := uuid.New()
	actionID := uuid.New()
	q.On("ListSourceActions", mock.Anything, "finland_prhytj").Return([]db.ListSourceActionsRow{{
		ID:                   actionID,
		SourceID:             sourceID,
		SourceName:           "finland_prhytj",
		Action:               "pull_source",
		DisplayName:          "Pull source data",
		TemporalWorkflowType: companysourceworkflows.DownloadSourceWorkflowName,
		TemporalTaskQueue:    ptrString(companysourceworkflows.SourceTaskQueue),
		Enabled:              true,
		Config:               json.RawMessage(`{}`),
	}}, nil)

	r := routerFor(newTestHandlers(q))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/sources/finland_prhytj/actions", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), `"pull_source"`)
	require.Contains(t, w.Body.String(), companysourceworkflows.DownloadSourceWorkflowName)
	q.AssertExpectations(t)
}

func TestTriggerSourceActionStartsTemporalWorkflow(t *testing.T) {
	q := &stubQuerier{}
	sourceID := uuid.New()
	actionID := uuid.New()
	q.On("GetSourceActionByName", mock.Anything, db.GetSourceActionByNameParams{
		Name: "finland_prhytj", Action: "pull_source",
	}).Return(db.GetSourceActionByNameRow{
		ID:                   actionID,
		SourceID:             sourceID,
		SourceName:           "finland_prhytj",
		Country:              "finland",
		Source:               "prhytj",
		RegistryKey:          "finland/prhytj",
		SourceUrl:            "https://example.test/source",
		SourceFileName:       "source.ndjson",
		Action:               "pull_source",
		DisplayName:          "Pull source data",
		TemporalWorkflowType: companysourceworkflows.DownloadSourceWorkflowName,
		TemporalTaskQueue:    ptrString(companysourceworkflows.SourceTaskQueue),
		Enabled:              true,
		Config:               json.RawMessage(`{}`),
	}, nil)

	tc := &temporalExecuteRecorder{}
	r := routerFor(httpapi.NewHandlers(q, nil, nil, nil, "", tc, ""))
	body := strings.NewReader(`{"trigger":"manual"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/sources/finland_prhytj/actions/pull_source/trigger", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusAccepted, w.Code)
	require.Equal(t, companysourceworkflows.SourceTaskQueue, tc.options.TaskQueue)
	require.Equal(t, companysourceworkflows.DownloadSource, tc.workflow)
}

var _ client.Client = (*temporalExecuteRecorder)(nil)
```

- [ ] **Step 2: Verify RED**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/httpapi -run 'TestListSourceActionsReturnsConfiguredActions|TestTriggerSourceActionStartsTemporalWorkflow' -count=1
```

Expected: FAIL because routes are not registered.

- [ ] **Step 3: Implement handlers**

Create `scheduler/internal/httpapi/source_actions.go`:

```go
package httpapi

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"go.temporal.io/sdk/client"

	companysourceworkflows "github.com/pulsarpoint/corpscout/scheduler/internal/temporal/workflow/companysources"
)

type sourceActionTriggerRequest struct {
	Trigger             string `json:"trigger"`
	DownloadActionRunID string `json:"download_action_run_id,omitempty"`
	BatchSize           int    `json:"batch_size,omitempty"`
	Limit               int64  `json:"limit,omitempty"`
}

func (h *Handlers) handleListSourceActions(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	rows, err := h.db.ListSourceActions(r.Context(), name)
	if err != nil {
		slog.ErrorContext(r.Context(), "list source actions", "source", name, "error", err)
		writeError(w, http.StatusInternalServerError, "list source actions failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": rows})
}

func (h *Handlers) handleListSourceActionRuns(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	rows, err := h.db.ListSourceActionRuns(r.Context(), db.ListSourceActionRunsParams{
		Name: name,
		Limit: int32(queryInt(r, "limit", 20)),
	})
	if err != nil {
		slog.ErrorContext(r.Context(), "list source action runs", "source", name, "error", err)
		writeError(w, http.StatusInternalServerError, "list source action runs failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": rows})
}

func (h *Handlers) handleTriggerSourceAction(w http.ResponseWriter, r *http.Request) {
	if h.temporal == nil {
		writeError(w, http.StatusServiceUnavailable, "temporal client not available")
		return
	}
	sourceName := chi.URLParam(r, "name")
	actionKey := chi.URLParam(r, "action")
	var req sourceActionTriggerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.Trigger == "" {
		req.Trigger = "manual"
	}
	action, err := h.db.GetSourceActionByName(r.Context(), db.GetSourceActionByNameParams{
		Name: sourceName, Action: actionKey,
	})
	if err != nil {
		writeError(w, http.StatusNotFound, "source action not found")
		return
	}
	if !action.Enabled {
		writeError(w, http.StatusUnprocessableEntity, "source action is disabled")
		return
	}

	workflowID := newWorkflowID(strings.ReplaceAll(sourceName+"-"+actionKey, "_", "-"))
	workflow, input, err := sourceActionWorkflow(actionKey, sourceName, req)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	run, err := h.temporal.ExecuteWorkflow(r.Context(), client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: companysourceworkflows.SourceTaskQueue,
	}, workflow, input)
	if err != nil {
		slog.ErrorContext(r.Context(), "start source action workflow", "source", sourceName, "action", actionKey, "error", err)
		writeError(w, http.StatusInternalServerError, "failed to start workflow")
		return
	}
	writeJSON(w, http.StatusAccepted, startWorkflowResponse{
		Status:        "started",
		Workflow:      action.TemporalWorkflowType,
		TaskQueue:     companysourceworkflows.SourceTaskQueue,
		WorkflowID:    workflowID,
		WorkflowRunID: run.GetRunID(),
	})
}
```

Add workflow mapper:

```go
func sourceActionWorkflow(action string, sourceName string, req sourceActionTriggerRequest) (any, any, error) {
	switch action {
	case companysourceworkflows.ActionPullSource:
		return companysourceworkflows.DownloadSource, companysourceworkflows.SyncSourceDownloadInput{
			SourceName: sourceName,
			Trigger:    req.Trigger,
		}, nil
	case companysourceworkflows.ActionImportClickHouse:
		if strings.TrimSpace(req.DownloadActionRunID) == "" {
			return nil, nil, errors.New("download_action_run_id is required")
		}
		if req.BatchSize <= 0 {
			req.BatchSize = 1000
		}
		return companysourceworkflows.ImportSourceToClickHouse, companysourceworkflows.ImportSourceToClickHouseInput{
			SourceName:          sourceName,
			Trigger:             req.Trigger,
			DownloadActionRunID: req.DownloadActionRunID,
			BatchSize:           req.BatchSize,
			Limit:               req.Limit,
		}, nil
	default:
		return nil, nil, errors.New("unsupported source action")
	}
}
```

Add import for `github.com/cockroachdb/errors` and `db`.

- [ ] **Step 4: Register routes**

Modify `scheduler/internal/httpapi/handlers.go`:

```go
r.Get("/sources/{name}/actions", h.handleListSourceActions)
r.Get("/sources/{name}/action-runs", h.handleListSourceActionRuns)
r.Post("/sources/{name}/actions/{action}/trigger", h.handleTriggerSourceAction)
```

- [ ] **Step 5: Verify GREEN**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/httpapi -run 'TestListSourceActionsReturnsConfiguredActions|TestTriggerSourceActionStartsTemporalWorkflow' -count=1
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add scheduler/internal/httpapi/source_actions.go scheduler/internal/httpapi/source_actions_test.go scheduler/internal/httpapi/handlers.go scheduler/internal/httpapi/testhelpers_test.go
git commit -m "feat: add source action api"
```

---

### Task 9: Add Composite Sync Trigger Endpoint

**Files:**
- Modify: `scheduler/internal/httpapi/source_actions.go`
- Modify: `scheduler/internal/httpapi/handlers.go`
- Test: `scheduler/internal/httpapi/source_actions_test.go`

- [ ] **Step 1: Write failing composite trigger test**

Append to `scheduler/internal/httpapi/source_actions_test.go`:

```go
func TestTriggerSourceSyncStartsCompositeWorkflow(t *testing.T) {
	q := &stubQuerier{}
	tc := &temporalExecuteRecorder{}
	r := routerFor(httpapi.NewHandlers(q, nil, nil, nil, "", tc, ""))

	body := strings.NewReader(`{"trigger":"manual","batch_size":1000}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/sources/finland_prhytj/sync-clickhouse", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusAccepted, w.Code)
	require.Equal(t, companysourceworkflows.SourceTaskQueue, tc.options.TaskQueue)
	require.Equal(t, companysourceworkflows.SyncSourceToClickHouse, tc.workflow)
}
```

- [ ] **Step 2: Verify RED**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/httpapi -run TestTriggerSourceSyncStartsCompositeWorkflow -count=1
```

Expected: FAIL because the route does not exist.

- [ ] **Step 3: Add composite handler**

Append to `scheduler/internal/httpapi/source_actions.go`:

```go
func (h *Handlers) handleTriggerSourceSyncClickHouse(w http.ResponseWriter, r *http.Request) {
	if h.temporal == nil {
		writeError(w, http.StatusServiceUnavailable, "temporal client not available")
		return
	}
	sourceName := chi.URLParam(r, "name")
	var req sourceActionTriggerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.Trigger == "" {
		req.Trigger = "manual"
	}
	if req.BatchSize <= 0 {
		req.BatchSize = 1000
	}
	workflowID := newWorkflowID(strings.ReplaceAll(sourceName+"-sync-clickhouse", "_", "-"))
	run, err := h.temporal.ExecuteWorkflow(r.Context(), client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: companysourceworkflows.SourceTaskQueue,
	}, companysourceworkflows.SyncSourceToClickHouse, companysourceworkflows.SyncSourceToClickHouseInput{
		SourceName: sourceName,
		Trigger:    req.Trigger,
		BatchSize:  req.BatchSize,
		Limit:      req.Limit,
	})
	if err != nil {
		slog.ErrorContext(r.Context(), "start source sync workflow", "source", sourceName, "error", err)
		writeError(w, http.StatusInternalServerError, "failed to start workflow")
		return
	}
	writeJSON(w, http.StatusAccepted, startWorkflowResponse{
		Status:        "started",
		Workflow:      companysourceworkflows.SyncSourceToClickHouseWorkflowName,
		TaskQueue:     companysourceworkflows.SourceTaskQueue,
		WorkflowID:    workflowID,
		WorkflowRunID: run.GetRunID(),
	})
}
```

- [ ] **Step 4: Register route**

Modify `scheduler/internal/httpapi/handlers.go`:

```go
r.Post("/sources/{name}/sync-clickhouse", h.handleTriggerSourceSyncClickHouse)
```

- [ ] **Step 5: Verify GREEN**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/httpapi -run TestTriggerSourceSyncStartsCompositeWorkflow -count=1
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add scheduler/internal/httpapi/source_actions.go scheduler/internal/httpapi/source_actions_test.go scheduler/internal/httpapi/handlers.go
git commit -m "feat: add source sync workflow trigger"
```

---

### Task 10: Add Source Actions UI

**Files:**
- Modify: `ui/app/types/api.ts`
- Modify: `ui/app/lib/api.ts`
- Create: `ui/app/components/app/source-detail/ActionsTab.tsx`
- Create: `ui/app/routes/sources_.$name.actions.tsx`
- Modify: `ui/app/components/app/source-detail/sourceDetailUtils.ts`

- [ ] **Step 1: Add TypeScript API types**

Modify `ui/app/types/api.ts`:

```ts
export interface SourceAction {
  id: string;
  source_id: string;
  source_name: string;
  action: "pull_source" | "import_clickhouse";
  display_name: string;
  temporal_workflow_type: string;
  temporal_task_queue: string;
  enabled: boolean;
  config: Record<string, unknown>;
  created_at: string;
  updated_at: string;
}

export interface SourceActionRun {
  id: string;
  source_id: string;
  source_name: string;
  action_id: string;
  action: "pull_source" | "import_clickhouse";
  status: "running" | "succeeded" | "failed" | "cancelled";
  temporal_workflow_id?: string;
  temporal_run_id?: string;
  started_at: string;
  finished_at?: string;
  input: Record<string, unknown>;
  result: Record<string, unknown>;
  error_message?: string;
  created_at: string;
}

export interface SourceActionListResponse {
  items: SourceAction[];
}

export interface SourceActionRunListResponse {
  items: SourceActionRun[];
}
```

- [ ] **Step 2: Add API client methods**

Modify `ui/app/lib/api.ts`:

```ts
getSourceActions: (name: string) =>
  get<SourceActionListResponse>(`/sources/${name}/actions`),

getSourceActionRuns: (name: string, limit = 20) =>
  get<SourceActionRunListResponse>(`/sources/${name}/action-runs?limit=${limit}`),

triggerSourceAction: (
  name: string,
  action: "pull_source" | "import_clickhouse",
  body: {
    trigger?: "manual";
    download_action_run_id?: string;
    batch_size?: number;
    limit?: number;
  } = {},
) => post<StartWorkflowResponse>(`/sources/${name}/actions/${action}/trigger`, body),

triggerSourceSyncClickHouse: (
  name: string,
  body: { trigger?: "manual"; batch_size?: number; limit?: number } = {},
) => post<StartWorkflowResponse>(`/sources/${name}/sync-clickhouse`, body),
```

- [ ] **Step 3: Add Actions tab component**

Create `ui/app/components/app/source-detail/ActionsTab.tsx`:

```tsx
import { useEffect, useState } from "react";
import { Download, RefreshCw, Upload } from "lucide-react";
import { toast } from "sonner";
import { api, errorMessage } from "~/lib/api";
import type { DataSource, SourceAction, SourceActionRun } from "~/types/api";
import { Button } from "~/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "~/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "~/components/ui/table";

interface ActionsTabProps {
  source: DataSource;
}

export function ActionsTab({ source }: ActionsTabProps) {
  const [actions, setActions] = useState<SourceAction[]>([]);
  const [runs, setRuns] = useState<SourceActionRun[]>([]);
  const [loading, setLoading] = useState(true);
  const [triggering, setTriggering] = useState<string>();

  async function refresh() {
    const [loadedActions, loadedRuns] = await Promise.all([
      api.getSourceActions(source.name),
      api.getSourceActionRuns(source.name),
    ]);
    setActions(loadedActions.items);
    setRuns(loadedRuns.items);
  }

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    refresh()
      .catch((err) => {
        if (!cancelled) toast.error(errorMessage(err, "Failed to load source actions."));
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => { cancelled = true; };
  }, [source.name]);

  async function triggerDownload() {
    setTriggering("pull_source");
    try {
      await api.triggerSourceAction(source.name, "pull_source", { trigger: "manual" });
      await refresh();
      toast.success("Download workflow started.");
    } catch (err) {
      toast.error(errorMessage(err, "Failed to start download workflow."));
    } finally {
      setTriggering(undefined);
    }
  }

  async function triggerSync() {
    setTriggering("sync");
    try {
      await api.triggerSourceSyncClickHouse(source.name, { trigger: "manual", batch_size: 1000 });
      await refresh();
      toast.success("Download and import workflow started.");
    } catch (err) {
      toast.error(errorMessage(err, "Failed to start source sync workflow."));
    } finally {
      setTriggering(undefined);
    }
  }

  const latestDownload = runs.find((run) => run.action === "pull_source" && run.status === "succeeded");

  async function triggerImport() {
    if (!latestDownload) {
      toast.error("No successful download is available for import.");
      return;
    }
    setTriggering("import_clickhouse");
    try {
      await api.triggerSourceAction(source.name, "import_clickhouse", {
        trigger: "manual",
        download_action_run_id: latestDownload.id,
        batch_size: 1000,
      });
      await refresh();
      toast.success("ClickHouse import workflow started.");
    } catch (err) {
      toast.error(errorMessage(err, "Failed to start ClickHouse import workflow."));
    } finally {
      setTriggering(undefined);
    }
  }

  return (
    <div className="space-y-4">
      <Card>
        <CardHeader>
          <CardTitle>Actions</CardTitle>
        </CardHeader>
        <CardContent className="flex flex-wrap gap-2">
          <Button onClick={triggerDownload} disabled={Boolean(triggering) || loading}>
            <Download className="size-4" />
            Download
          </Button>
          <Button variant="outline" onClick={triggerImport} disabled={Boolean(triggering) || !latestDownload}>
            <Upload className="size-4" />
            Import
          </Button>
          <Button variant="outline" onClick={triggerSync} disabled={Boolean(triggering) || loading}>
            <RefreshCw className="size-4" />
            Download and import
          </Button>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Runs</CardTitle>
        </CardHeader>
        <CardContent>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Action</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Started</TableHead>
                <TableHead>Finished</TableHead>
                <TableHead>Workflow</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {runs.map((run) => (
                <TableRow key={run.id}>
                  <TableCell className="font-mono text-xs">{run.action}</TableCell>
                  <TableCell>{run.status}</TableCell>
                  <TableCell>{new Date(run.started_at).toLocaleString()}</TableCell>
                  <TableCell>{run.finished_at ? new Date(run.finished_at).toLocaleString() : "-"}</TableCell>
                  <TableCell className="font-mono text-xs">{run.temporal_workflow_id ?? "-"}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      </Card>
    </div>
  );
}
```

- [ ] **Step 4: Add route**

Create `ui/app/routes/sources_.$name.actions.tsx`:

```tsx
import { useOutletContext } from "react-router";
import { ActionsTab } from "~/components/app/source-detail/ActionsTab";
import type { SourceDetailContext } from "~/routes/sources_.$name";

export default function SourceActionsPage() {
  const { source } = useOutletContext<SourceDetailContext>();
  return <ActionsTab source={source} />;
}
```

- [ ] **Step 5: Add tab**

Modify `ui/app/components/app/source-detail/sourceDetailUtils.ts`:

```ts
{ label: "Actions", to: `/sources/${source.name}/actions` },
```

Place it before `Schedule`.

- [ ] **Step 6: Verify typecheck**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/ui
npm run typecheck
```

Expected: PASS.

- [ ] **Step 7: Verify build**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/ui
npm run build
```

Expected: PASS. Existing sourcemap warnings in shared UI files are acceptable if the command exits 0.

- [ ] **Step 8: Commit**

```bash
git add ui/app/types/api.ts ui/app/lib/api.ts ui/app/components/app/source-detail/ActionsTab.tsx ui/app/routes/sources_.$name.actions.tsx ui/app/components/app/source-detail/sourceDetailUtils.ts
git commit -m "feat: add source action UI"
```

---

### Task 11: End-to-End Verification

**Files:**
- No new files.

- [ ] **Step 1: Run backend tests**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/companysources ./internal/companysources/finland/prhytj ./internal/companysources/unitedstates/coloradoentities ./internal/companysources/unitedstates/irseobmf ./internal/companysources/unitedstates/secedgar ./internal/app ./internal/httpapi ./internal/db -count=1
```

Expected: PASS.

- [ ] **Step 2: Run UI checks**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/ui
npm run typecheck
npm run build
```

Expected: both commands exit 0.

- [ ] **Step 3: Apply Postgres migrations**

If Task 1 only added queries and no schema migration, skip this step. If an implementation adds a migration for action metadata changes, run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
make migrate-up
```

Expected: migration exits 0.

- [ ] **Step 4: Rebuild containers**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
docker compose up -d --build scheduler ui
```

Expected: scheduler and UI containers start.

- [ ] **Step 5: Verify API routes**

```bash
curl -sS http://localhost:8094/api/v1/sources/finland_prhytj/actions
curl -sS http://localhost:8094/api/v1/sources/finland_prhytj/action-runs
```

Expected: JSON responses with `items`.

- [ ] **Step 6: Verify UI**

Open:

```text
http://localhost:8094/sources/finland_prhytj/actions
```

Expected:

- Actions page loads.
- Download button is visible.
- Import button is disabled until a successful download run exists.
- Download and import button is visible.
- Browser console has no errors.

- [ ] **Step 7: Commit verification adjustments**

```bash
git status --short
git add .
git commit -m "test: verify company source temporal actions"
```

Use this commit only for test/documentation/build adjustments created during verification. Do not stage unrelated user changes.

---

## Self-Review

- Spec coverage: The plan creates separate Temporal workflows for download and ClickHouse import, plus a composite workflow that runs both. It stores action-run metadata in Postgres and does not use filesystem manifests or run-index metadata. Temporal code is grouped under `scheduler/internal/temporal`, with workflows and their public contract in `workflow/companysources`, actions in `actions/companysources`, and root registration in `registry.go`.
- Placeholder scan: The plan avoids placeholder implementation steps. The only conditional instruction is the migration verification step, which is conditional because Task 1 is query-only unless implementation adds a schema migration.
- Type consistency: Action keys remain the current database keys `pull_source` and `import_clickhouse`. Workflow names are `CompanySourceDownloadWorkflow`, `CompanySourceClickHouseImportWorkflow`, and `CompanySourceSyncClickHouseWorkflow`. HTTP, actions, and app wiring import workflow functions, constants, and input/result types from `scheduler/internal/temporal/workflow/companysources`.
- Scope: Scheduling is intentionally excluded from this plan. Once manual workflows/actions are working, schedules can target these workflows through the existing Temporal schedule system.
