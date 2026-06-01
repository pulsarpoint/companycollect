# Temporal Data Pipelines — Phase 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a shared `data-pipelines` service using Temporal that fetches UK company data from Companies House, writes raw records directly to corpscout's PostgreSQL, and lets corpscout track live pipeline status through a new UI tab.

**Architecture:** A new Go+Python service at `ppoint/data-pipelines/` registers Temporal workflow and activity workers. corpscout's scheduler gains a `DataTaskWorker` River job that starts a `PullCompanies` Temporal workflow and stores the workflow ID in a new `temporal_executions` table. The workflow fetches pages (Python activity) and writes rows to `companies_house_company_raw_inputs` (Go activity). corpscout's existing `SourceProcessWorker` picks up those rows unchanged.

**Tech Stack:** Go 1.22, `go.temporal.io/sdk v1.27.0`, `jackc/pgx/v5`, Python 3.11+, `temporalio>=1.7.0`, `httpx`, React Router v7, shadcn/ui. Temporal server runs as Docker container backed by PostgreSQL.

---

## File Map

### New service — `ppoint/data-pipelines/`
| File | Purpose |
|---|---|
| `go.mod` | Go module `github.com/pulsarpoint/data-pipelines` |
| `Makefile` | build, test, docker-compose commands |
| `.gitignore` | Go + Python ignores |
| `docker-compose.yml` | Temporal server + UI + postgres (local dev) |
| `cmd/worker/main.go` | Go workflow + Go activity worker entrypoint |
| `contracts/contracts.go` | Shared Go data types (inputs, results, records) |
| `workflows/pull_companies.go` | PullCompanies workflow |
| `workflows/pull_companies_test.go` | Workflow unit tests using Temporal testsuite |
| `activities/activities.go` | GoActivities struct + WriteRawInputs + MarkExecutionComplete |
| `activities/activities_test.go` | Activity unit tests |
| `python/requirements.txt` | Python dependencies |
| `python/contracts.py` | Pydantic/dataclass mirrors of Go contracts |
| `python/activities/__init__.py` | Package marker |
| `python/activities/fetch_page.py` | FetchPage activity — Companies House HTTP |
| `python/test_activities.py` | pytest tests for fetch_page |
| `python/main.py` | Python activity worker entrypoint |

### Modified — `ppoint/corpscout/`
| File | Purpose |
|---|---|
| `database/migrations/000031_temporal_executions.up.sql` | temporal_executions table + run_id on raw_inputs |
| `database/migrations/000031_temporal_executions.down.sql` | rollback |
| `database/queries/temporal_executions.sql` | sqlc queries for temporal_executions |
| `scheduler/internal/config/config.go` | Add TemporalHost, TemporalUIURL |
| `scheduler/internal/workers/workers.go` | Add DataTaskArgs |
| `scheduler/internal/workers/data_task.go` | DataTaskWorker |
| `scheduler/internal/app/app.go` | Add temporal client, pass to DataTaskWorker |
| `scheduler/internal/app/river.go` | Register DataTaskWorker + data_task queue |
| `scheduler/internal/httpapi/temporal_executions.go` | handleListTemporalExecutions handler |
| `scheduler/internal/httpapi/handlers.go` | Register GET /temporal-executions route + add temporal field |
| `ui/app/types/api.ts` | Add TemporalExecution, TemporalExecutionsResponse |
| `ui/app/lib/api.ts` | Add getTemporalExecutions |
| `ui/app/routes/jobs.tsx` | Add Temporal Tasks tab |

---

## Task 1: Temporal local dev infrastructure

**Files:**
- Create: `data-pipelines/docker-compose.yml`
- Create: `data-pipelines/.gitignore`
- Create: `data-pipelines/Makefile`

- [ ] **Step 1: Create `data-pipelines/` directory and docker-compose**

```bash
mkdir -p /Users/graovic/pulsarpoint/ppoint/data-pipelines
```

Create `data-pipelines/docker-compose.yml`:
```yaml
version: "3.8"

services:
  temporal-postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: temporal
      POSTGRES_PASSWORD: temporal
      POSTGRES_DB: temporal
    volumes:
      - temporal-postgres:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U temporal"]
      interval: 5s
      timeout: 5s
      retries: 10

  temporal:
    image: temporalio/auto-setup:1.24.2
    environment:
      DB: postgresql
      DB_PORT: 5432
      POSTGRES_USER: temporal
      POSTGRES_PWD: temporal
      POSTGRES_SEEDS: temporal-postgres
      DYNAMIC_CONFIG_FILE_PATH: /etc/temporal/dynamicconfig/development-sql.yaml
    depends_on:
      temporal-postgres:
        condition: service_healthy
    ports:
      - "7233:7233"

  temporal-ui:
    image: temporalio/ui:2.28.0
    environment:
      TEMPORAL_ADDRESS: temporal:7233
      TEMPORAL_CORS_ORIGINS: "http://localhost:3000"
    ports:
      - "8088:8080"
    depends_on:
      - temporal

volumes:
  temporal-postgres:
```

Create `data-pipelines/.gitignore`:
```
bin/
__pycache__/
*.pyc
*.pyo
.pytest_cache/
.venv/
*.egg-info/
dist/
```

Create `data-pipelines/Makefile`:
```makefile
.PHONY: up down logs build test create-namespace

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

build:
	GOWORK=off go build -o bin/worker ./cmd/worker

test:
	GOWORK=off go test ./...
	cd python && python -m pytest -v

create-namespace:
	docker compose exec temporal temporal operator namespace create --namespace corpscout --retention 7d

py-install:
	cd python && pip install -r requirements.txt
```

- [ ] **Step 2: Start Temporal and verify it's up**

Run from `data-pipelines/`:
```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines && docker compose up -d
```

Expected: containers start. Then verify:
```bash
docker compose ps
```
Expected: `temporal`, `temporal-postgres`, `temporal-ui` all running.

Visit `http://localhost:8088` — Temporal UI should load.

- [ ] **Step 3: Create the corpscout namespace**

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines && docker compose exec temporal temporal operator namespace create --namespace corpscout --retention 7d
```

Expected: `Namespace corpscout successfully registered.`

- [ ] **Step 4: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines && git add . && git commit -m "feat(data-pipelines): scaffold Temporal local dev infrastructure"
```

---

## Task 2: Go module scaffold and contracts

**Files:**
- Create: `data-pipelines/go.mod`
- Create: `data-pipelines/contracts/contracts.go`

- [ ] **Step 1: Initialise Go module**

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines
GOWORK=off go mod init github.com/pulsarpoint/data-pipelines
GOWORK=off go get go.temporal.io/sdk@v1.27.0
GOWORK=off go get github.com/jackc/pgx/v5@v5.7.2
GOWORK=off go get github.com/google/uuid@v1.6.0
GOWORK=off go get github.com/stretchr/testify@v1.9.0
```

- [ ] **Step 2: Create `data-pipelines/contracts/contracts.go`**

```go
package contracts

import "encoding/json"

// PullCompaniesInput is the input for the PullCompanies workflow.
// IDs nil means bulk pull; populated means individual lookup.
type PullCompaniesInput struct {
	Source         string   `json:"source"`
	Country        string   `json:"country"`
	IDs            []string `json:"ids,omitempty"`
	CorpscoutRunID string   `json:"corpscout_run_id"`
}

// PullCompaniesResult is returned by the PullCompanies workflow.
// Actual records are already written to the DB; this is metadata only.
type PullCompaniesResult struct {
	RecordsWritten int      `json:"records_written"`
	PagesFetched   int      `json:"pages_fetched"`
	Errors         []string `json:"errors,omitempty"`
}

// FetchPageInput is the input for the FetchPage Python activity.
type FetchPageInput struct {
	Source  string   `json:"source"`
	Country string   `json:"country"`
	IDs     []string `json:"ids,omitempty"`
	Page    int      `json:"page"`
	Cursor  string   `json:"cursor,omitempty"`
}

// RawRecord is a single raw company record returned by FetchPage.
// It carries the fields needed to INSERT into raw_inputs tables.
type RawRecord struct {
	NativeID    string          `json:"native_id"`
	Name        string          `json:"name"`
	Status      string          `json:"status"`
	CompanyType string          `json:"company_type,omitempty"`
	RawJSON     json.RawMessage `json:"raw_json"`
	Hash        string          `json:"hash"` // SHA-256 of RawJSON for dedup
}

// FetchResult is returned by the FetchPage Python activity.
type FetchResult struct {
	Records    []RawRecord `json:"records"`
	HasMore    bool        `json:"has_more"`
	NextCursor string      `json:"next_cursor,omitempty"`
}

// WriteRawInputsParams is the input for the WriteRawInputs Go activity.
type WriteRawInputsParams struct {
	Source  string      `json:"source"`
	RunID   string      `json:"run_id"`
	Records []RawRecord `json:"records"`
}

// MarkCompleteParams is the input for the MarkExecutionComplete Go activity.
type MarkCompleteParams struct {
	CorpscoutRunID string              `json:"corpscout_run_id"`
	Result         PullCompaniesResult `json:"result"`
}
```

- [ ] **Step 3: Verify contracts compile**

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines
GOWORK=off go build ./contracts/...
```

Expected: no output (success).

- [ ] **Step 4: Commit**

```bash
git add go.mod go.sum contracts/
git commit -m "feat(data-pipelines): Go module + contracts package"
```

---

## Task 3: Go activities (WriteRawInputs + MarkExecutionComplete)

**Files:**
- Create: `data-pipelines/activities/activities.go`
- Create: `data-pipelines/activities/activities_test.go`

- [ ] **Step 1: Write the failing test**

Create `data-pipelines/activities/activities_test.go`:
```go
package activities_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/pashagolub/pgxmock/v3"
	"github.com/stretchr/testify/require"

	"github.com/pulsarpoint/data-pipelines/activities"
	"github.com/pulsarpoint/data-pipelines/contracts"
)

func TestWriteRawInputs_CompaniesHouse(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	rec := contracts.RawRecord{
		NativeID:    "12345678",
		Name:        "ACME LTD",
		Status:      "active",
		CompanyType: "ltd",
		RawJSON:     json.RawMessage(`{"company_number":"12345678"}`),
		Hash:        "abc123",
	}

	mock.ExpectExec("INSERT INTO companies_house_company_raw_inputs").
		WithArgs("12345678", "ACME LTD", "active", "ltd",
			[]byte(`{"company_number":"12345678"}`), "abc123", "run-001").
		WillReturnResult(pgxmock.NewResult("INSERT", 1))

	acts := activities.NewGoActivities(mock)
	written, err := acts.WriteRawInputs(context.Background(), contracts.WriteRawInputsParams{
		Source:  "companies_house",
		RunID:   "run-001",
		Records: []contracts.RawRecord{rec},
	})
	require.NoError(t, err)
	require.Equal(t, 1, written)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestWriteRawInputs_UnsupportedSource(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	acts := activities.NewGoActivities(mock)
	_, err = acts.WriteRawInputs(context.Background(), contracts.WriteRawInputsParams{
		Source:  "unknown_source",
		RunID:   "run-001",
		Records: []contracts.RawRecord{},
	})
	require.ErrorContains(t, err, "unsupported source")
}

func TestMarkExecutionComplete(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("UPDATE temporal_executions").
		WithArgs("550e8400-e29b-41d4-a716-446655440000", 42, 3).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	acts := activities.NewGoActivities(mock)
	err = acts.MarkExecutionComplete(context.Background(), contracts.MarkCompleteParams{
		CorpscoutRunID: "550e8400-e29b-41d4-a716-446655440000",
		Result:         contracts.PullCompaniesResult{RecordsWritten: 42, PagesFetched: 3},
	})
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines
GOWORK=off go test ./activities/... -v
```

Expected: FAIL with `cannot find package "activities"`.

- [ ] **Step 3: Implement `data-pipelines/activities/activities.go`**

```go
package activities

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pulsarpoint/data-pipelines/contracts"
)

// pgxExecer is the minimal interface needed by GoActivities (satisfied by *pgxpool.Pool and pgxmock).
type pgxExecer interface {
	Exec(ctx context.Context, sql string, arguments ...any) (interface{ RowsAffected() int64 }, error)
}

// GoActivities holds dependencies for all Go-side Temporal activities.
type GoActivities struct {
	pool pgxExecer
}

// NewGoActivities constructs GoActivities with a real pgxpool.
func NewGoActivities(pool *pgxpool.Pool) *GoActivities {
	return &GoActivities{pool: pool}
}

// WriteRawInputs inserts raw records into the appropriate source raw_inputs table.
// It is idempotent: ON CONFLICT updates last_seen_at only.
func (a *GoActivities) WriteRawInputs(ctx context.Context, params contracts.WriteRawInputsParams) (int, error) {
	switch params.Source {
	case "companies_house":
		return a.writeCompaniesHouse(ctx, params.RunID, params.Records)
	default:
		return 0, fmt.Errorf("unsupported source: %s", params.Source)
	}
}

func (a *GoActivities) writeCompaniesHouse(ctx context.Context, runID string, records []contracts.RawRecord) (int, error) {
	written := 0
	for _, rec := range records {
		if rec.NativeID == "" {
			continue
		}
		_, err := a.pool.Exec(ctx, `
			INSERT INTO companies_house_company_raw_inputs
				(source_pull_run_id, source_native_id, company_number, company_name,
				 company_status, company_type, source_updated_at, raw_payload, payload_hash, run_id)
			VALUES
				(NULL, $1, $1, $2, $3, $4, NULL, $5, $6, $7)
			ON CONFLICT (company_number, payload_hash) DO UPDATE
				SET last_seen_at = now(), run_id = EXCLUDED.run_id
		`, rec.NativeID, rec.Name, rec.Status, rec.CompanyType, []byte(rec.RawJSON), rec.Hash, runID)
		if err != nil {
			return written, fmt.Errorf("insert companies_house row %s: %w", rec.NativeID, err)
		}
		written++
	}
	return written, nil
}

// MarkExecutionComplete marks a temporal_executions row as completed with result metadata.
func (a *GoActivities) MarkExecutionComplete(ctx context.Context, params contracts.MarkCompleteParams) error {
	_, err := a.pool.Exec(ctx, `
		UPDATE temporal_executions
		SET status          = 'completed',
		    records_written = $2,
		    pages_fetched   = $3,
		    completed_at    = now()
		WHERE id = $1::uuid
	`, params.CorpscoutRunID, params.Result.RecordsWritten, params.Result.PagesFetched)
	if err != nil {
		return fmt.Errorf("mark execution complete: %w", err)
	}
	return nil
}
```

Note: `pgxExecer` interface uses `any` return for `Exec` to satisfy both pgxpool and pgxmock. Adjust to use `pgconn.CommandTag` for real pool; pgxmock satisfies this if you use `pgxmock.NewPool()` which returns a `pgxpool.Pool`-compatible mock.

Actually use the simpler approach — inject `*pgxpool.Pool` and use pgxmock's pool variant which satisfies `pgxpool.Pool`. Replace the interface with direct pool injection:

```go
package activities

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pulsarpoint/data-pipelines/contracts"
)

// GoActivities holds dependencies for all Go-side Temporal activities.
type GoActivities struct {
	pool *pgxpool.Pool
}

// NewGoActivities constructs GoActivities with a real pgxpool.
func NewGoActivities(pool *pgxpool.Pool) *GoActivities {
	return &GoActivities{pool: pool}
}

// WriteRawInputs inserts raw records into the appropriate source raw_inputs table.
// Idempotent: ON CONFLICT updates last_seen_at only.
func (a *GoActivities) WriteRawInputs(ctx context.Context, params contracts.WriteRawInputsParams) (int, error) {
	switch params.Source {
	case "companies_house":
		return a.writeCompaniesHouse(ctx, params.RunID, params.Records)
	default:
		return 0, fmt.Errorf("unsupported source: %s", params.Source)
	}
}

func (a *GoActivities) writeCompaniesHouse(ctx context.Context, runID string, records []contracts.RawRecord) (int, error) {
	written := 0
	for _, rec := range records {
		if rec.NativeID == "" {
			continue
		}
		_, err := a.pool.Exec(ctx, `
			INSERT INTO companies_house_company_raw_inputs
				(source_pull_run_id, source_native_id, company_number, company_name,
				 company_status, company_type, source_updated_at, raw_payload, payload_hash, run_id)
			VALUES
				(NULL, $1, $1, $2, $3, $4, NULL, $5, $6, $7)
			ON CONFLICT (company_number, payload_hash) DO UPDATE
				SET last_seen_at = now(), run_id = EXCLUDED.run_id
		`, rec.NativeID, rec.Name, rec.Status, rec.CompanyType, []byte(rec.RawJSON), rec.Hash, runID)
		if err != nil {
			return written, fmt.Errorf("insert row %s: %w", rec.NativeID, err)
		}
		written++
	}
	return written, nil
}

// MarkExecutionComplete marks a temporal_executions row as completed.
func (a *GoActivities) MarkExecutionComplete(ctx context.Context, params contracts.MarkCompleteParams) error {
	_, err := a.pool.Exec(ctx, `
		UPDATE temporal_executions
		SET status          = 'completed',
		    records_written = $2,
		    pages_fetched   = $3,
		    completed_at    = now()
		WHERE id = $1::uuid
	`, params.CorpscoutRunID, params.Result.RecordsWritten, params.Result.PagesFetched)
	if err != nil {
		return fmt.Errorf("mark execution complete: %w", err)
	}
	return nil
}
```

And update the test to use `pgxmock.NewPool()`:
```go
// In test file, replace:
mock, err := pgxmock.NewPool()
// Then pass as *pgxpool.Pool — pgxmock satisfies this via its pool mock type.
// Use: activities.NewGoActivities(mock) where mock implements pgxpool.Pool interface.
```

Since pgxmock's `pgxmock.NewPool()` returns a type that does not embed `*pgxpool.Pool`, use the interface approach instead. Update `activities.go` to accept an interface:

```go
// execer is the minimal pgx interface needed.
type execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}
```

But `pgxpool.Pool.Exec` returns `(pgconn.CommandTag, error)`, while `pgxmock` pool's Exec also returns compatible types. The cleanest approach for testability:

Replace `*pgxpool.Pool` with a local interface in the package:

```go
package activities

import (
	"context"
	"fmt"

	"github.com/jackc/pgconn"
	"github.com/pulsarpoint/data-pipelines/contracts"
)

type dbPool interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}
```

Note: pgx v5 uses `pgconn.CommandTag` from `github.com/jackc/pgx/v5/pgconn` — not `github.com/jackc/pgconn`. Adjust:

```go
import "github.com/jackc/pgx/v5/pgconn"
```

Full final `activities.go`:

```go
package activities

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pulsarpoint/data-pipelines/contracts"
)

type dbPool interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

type GoActivities struct {
	pool dbPool
}

func NewGoActivities(pool *pgxpool.Pool) *GoActivities {
	return &GoActivities{pool: pool}
}

func newGoActivitiesForTest(pool dbPool) *GoActivities {
	return &GoActivities{pool: pool}
}

func (a *GoActivities) WriteRawInputs(ctx context.Context, params contracts.WriteRawInputsParams) (int, error) {
	switch params.Source {
	case "companies_house":
		return a.writeCompaniesHouse(ctx, params.RunID, params.Records)
	default:
		return 0, fmt.Errorf("unsupported source: %s", params.Source)
	}
}

func (a *GoActivities) writeCompaniesHouse(ctx context.Context, runID string, records []contracts.RawRecord) (int, error) {
	written := 0
	for _, rec := range records {
		if rec.NativeID == "" {
			continue
		}
		_, err := a.pool.Exec(ctx, `
			INSERT INTO companies_house_company_raw_inputs
				(source_pull_run_id, source_native_id, company_number, company_name,
				 company_status, company_type, source_updated_at, raw_payload, payload_hash, run_id)
			VALUES
				(NULL, $1, $1, $2, $3, $4, NULL, $5, $6, $7)
			ON CONFLICT (company_number, payload_hash) DO UPDATE
				SET last_seen_at = now(), run_id = EXCLUDED.run_id
		`, rec.NativeID, rec.Name, rec.Status, rec.CompanyType, []byte(rec.RawJSON), rec.Hash, runID)
		if err != nil {
			return written, fmt.Errorf("insert row %s: %w", rec.NativeID, err)
		}
		written++
	}
	return written, nil
}

func (a *GoActivities) MarkExecutionComplete(ctx context.Context, params contracts.MarkCompleteParams) error {
	_, err := a.pool.Exec(ctx, `
		UPDATE temporal_executions
		SET status          = 'completed',
		    records_written = $2,
		    pages_fetched   = $3,
		    completed_at    = now()
		WHERE id = $1::uuid
	`, params.CorpscoutRunID, params.Result.RecordsWritten, params.Result.PagesFetched)
	if err != nil {
		return fmt.Errorf("mark execution complete: %w", err)
	}
	return nil
}
```

And the test file uses `newGoActivitiesForTest(mock)`:
```go
acts := activities.NewGoActivitiesForTest(mock)
// Export it:
func NewGoActivitiesForTest(pool dbPool) *GoActivities { return &GoActivities{pool: pool} }
```

Export it from the package (capital N):
```go
func NewGoActivitiesForTest(pool interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}) *GoActivities {
	return &GoActivities{pool: pool}
}
```

- [ ] **Step 4: Add pgxmock dependency and get test deps**

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines
GOWORK=off go get github.com/pashagolub/pgxmock/v3@v3.4.0
GOWORK=off go mod tidy
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines
GOWORK=off go test ./activities/... -v
```

Expected:
```
--- PASS: TestWriteRawInputs_CompaniesHouse
--- PASS: TestWriteRawInputs_UnsupportedSource
--- PASS: TestMarkExecutionComplete
PASS
```

- [ ] **Step 6: Commit**

```bash
git add activities/ go.mod go.sum
git commit -m "feat(data-pipelines): Go activities WriteRawInputs + MarkExecutionComplete"
```

---

## Task 4: Go PullCompanies workflow

**Files:**
- Create: `data-pipelines/workflows/pull_companies.go`
- Create: `data-pipelines/workflows/pull_companies_test.go`

- [ ] **Step 1: Write the failing test**

Create `data-pipelines/workflows/pull_companies_test.go`:
```go
package workflows_test

import (
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"

	"github.com/pulsarpoint/data-pipelines/activities"
	"github.com/pulsarpoint/data-pipelines/contracts"
	"github.com/pulsarpoint/data-pipelines/workflows"
)

type PullCompaniesSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	env *testsuite.TestWorkflowEnvironment
}

func (s *PullCompaniesSuite) SetupTest() {
	s.env = s.NewTestWorkflowEnvironment()
}

func (s *PullCompaniesSuite) AfterTest(_, _ string) {
	s.env.AssertExpectations(s.T())
}

func TestPullCompaniesSuite(t *testing.T) {
	suite.Run(t, new(PullCompaniesSuite))
}

func (s *PullCompaniesSuite) Test_SinglePage_WritesRecords() {
	fetchResult := contracts.FetchResult{
		Records: []contracts.RawRecord{
			{NativeID: "12345678", Name: "ACME LTD", Status: "active", Hash: "h1"},
			{NativeID: "87654321", Name: "GLOBEX LTD", Status: "active", Hash: "h2"},
		},
		HasMore: false,
	}

	// Mock the Python FetchPage activity (referenced by name string)
	s.env.OnActivity("fetch_page", mock.Anything, contracts.FetchPageInput{
		Source: "companies_house", Country: "GB", Page: 1,
	}).Return(fetchResult, nil)

	// Mock the Go WriteRawInputs activity
	var goAct *activities.GoActivities
	s.env.OnActivity(goAct.WriteRawInputs, mock.Anything, mock.MatchedBy(func(p contracts.WriteRawInputsParams) bool {
		return p.Source == "companies_house" && len(p.Records) == 2
	})).Return(2, nil)

	// Mock MarkExecutionComplete
	s.env.OnActivity(goAct.MarkExecutionComplete, mock.Anything, mock.MatchedBy(func(p contracts.MarkCompleteParams) bool {
		return p.CorpscoutRunID == "exec-123" && p.Result.RecordsWritten == 2
	})).Return(nil)

	s.env.ExecuteWorkflow(workflows.PullCompanies, contracts.PullCompaniesInput{
		Source:         "companies_house",
		Country:        "GB",
		CorpscoutRunID: "exec-123",
	})

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())

	var result contracts.PullCompaniesResult
	s.NoError(s.env.GetWorkflowResult(&result))
	s.Equal(2, result.RecordsWritten)
	s.Equal(1, result.PagesFetched)
}

func (s *PullCompaniesSuite) Test_MultiPage_FetchesAll() {
	page1 := contracts.FetchResult{
		Records:    []contracts.RawRecord{{NativeID: "00000001", Name: "A", Status: "active", Hash: "h1"}},
		HasMore:    true,
		NextCursor: "2024-01-01,1",
	}
	page2 := contracts.FetchResult{
		Records: []contracts.RawRecord{{NativeID: "00000002", Name: "B", Status: "active", Hash: "h2"}},
		HasMore: false,
	}

	var goAct *activities.GoActivities

	s.env.OnActivity("fetch_page", mock.Anything, contracts.FetchPageInput{
		Source: "companies_house", Country: "GB", Page: 1,
	}).Return(page1, nil)
	s.env.OnActivity("fetch_page", mock.Anything, contracts.FetchPageInput{
		Source: "companies_house", Country: "GB", Page: 2, Cursor: "2024-01-01,1",
	}).Return(page2, nil)

	s.env.OnActivity(goAct.WriteRawInputs, mock.Anything, mock.Anything).Return(1, nil).Times(2)
	s.env.OnActivity(goAct.MarkExecutionComplete, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(workflows.PullCompanies, contracts.PullCompaniesInput{
		Source:         "companies_house",
		Country:        "GB",
		CorpscoutRunID: "exec-456",
	})

	s.True(s.env.IsWorkflowCompleted())
	var result contracts.PullCompaniesResult
	s.NoError(s.env.GetWorkflowResult(&result))
	s.Equal(2, result.RecordsWritten)
	s.Equal(2, result.PagesFetched)
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines
GOWORK=off go test ./workflows/... -v
```

Expected: FAIL — `workflows` package not found.

- [ ] **Step 3: Implement `data-pipelines/workflows/pull_companies.go`**

```go
package workflows

import (
	"time"

	"github.com/google/uuid"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/pulsarpoint/data-pipelines/activities"
	"github.com/pulsarpoint/data-pipelines/contracts"
)

// PullCompanies orchestrates fetching all company records from a source and
// writing them directly to corpscout's raw_inputs tables.
// Python activities run on the "corpscout-pipelines-python" task queue.
// Go activities run on the "corpscout-pipelines" task queue.
func PullCompanies(ctx workflow.Context, input contracts.PullCompaniesInput) (contracts.PullCompaniesResult, error) {
	// Generate a stable run ID. SideEffect records it in workflow history so
	// retries use the same ID, enabling idempotent DB upserts.
	var runIDStr string
	if err := workflow.SideEffect(ctx, func(ctx workflow.Context) interface{} {
		return uuid.New().String()
	}).Get(&runIDStr); err != nil {
		return contracts.PullCompaniesResult{}, err
	}

	// Options for the Python FetchPage activity.
	fetchCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:           "corpscout-pipelines-python",
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts:    10,
			InitialInterval:    5 * time.Second,
			MaximumInterval:    2 * time.Minute,
			BackoffCoefficient: 2.0,
		},
	})

	// Options for Go activities.
	writeCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:           "corpscout-pipelines",
		StartToCloseTimeout: 2 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 5,
			InitialInterval: 2 * time.Second,
		},
	})

	var goAct *activities.GoActivities
	total := contracts.PullCompaniesResult{}
	cursor := ""
	page := 1

	for {
		// Step 1: fetch one page (Python activity, referenced by name)
		fetchInput := contracts.FetchPageInput{
			Source:  input.Source,
			Country: input.Country,
			IDs:     input.IDs,
			Page:    page,
			Cursor:  cursor,
		}
		var fetchResult contracts.FetchResult
		if err := workflow.ExecuteActivity(fetchCtx, "fetch_page", fetchInput).Get(ctx, &fetchResult); err != nil {
			return total, err
		}

		if len(fetchResult.Records) == 0 {
			break
		}

		// Step 2: write records to corpscout DB (Go activity)
		writeParams := contracts.WriteRawInputsParams{
			Source:  input.Source,
			RunID:   runIDStr,
			Records: fetchResult.Records,
		}
		var written int
		if err := workflow.ExecuteActivity(writeCtx, goAct.WriteRawInputs, writeParams).Get(ctx, &written); err != nil {
			return total, err
		}

		total.RecordsWritten += written
		total.PagesFetched++

		if !fetchResult.HasMore {
			break
		}
		cursor = fetchResult.NextCursor
		page++
	}

	// Step 3: mark execution complete in corpscout
	markParams := contracts.MarkCompleteParams{
		CorpscoutRunID: input.CorpscoutRunID,
		Result:         total,
	}
	if err := workflow.ExecuteActivity(writeCtx, goAct.MarkExecutionComplete, markParams).Get(ctx, nil); err != nil {
		return total, err
	}

	return total, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines
GOWORK=off go test ./workflows/... -v
```

Expected:
```
--- PASS: TestPullCompaniesSuite/Test_SinglePage_WritesRecords
--- PASS: TestPullCompaniesSuite/Test_MultiPage_FetchesAll
PASS
```

- [ ] **Step 5: Commit**

```bash
git add workflows/
git commit -m "feat(data-pipelines): PullCompanies workflow"
```

---

## Task 5: Go worker entrypoint

**Files:**
- Create: `data-pipelines/cmd/worker/main.go`

- [ ] **Step 1: Create `data-pipelines/cmd/worker/main.go`**

```go
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/pulsarpoint/data-pipelines/activities"
	"github.com/pulsarpoint/data-pipelines/workflows"
)

func main() {
	temporalHost := getEnv("TEMPORAL_HOST", "localhost:7233")
	corpscoutDB := mustEnv("CORPSCOUT_DB_URL")

	ctx := context.Background()

	pool, err := pgxpool.New(ctx, corpscoutDB)
	if err != nil {
		slog.Error("connect to corpscout db", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		slog.Error("ping corpscout db", "error", err)
		os.Exit(1)
	}

	c, err := client.Dial(client.Options{
		HostPort:  temporalHost,
		Namespace: "corpscout",
	})
	if err != nil {
		slog.Error("connect to temporal", "error", err)
		os.Exit(1)
	}
	defer c.Close()

	goActs := activities.NewGoActivities(pool)

	w := worker.New(c, "corpscout-pipelines", worker.Options{})
	w.RegisterWorkflow(workflows.PullCompanies)
	w.RegisterActivity(goActs.WriteRawInputs)
	w.RegisterActivity(goActs.MarkExecutionComplete)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	if err := w.Start(); err != nil {
		slog.Error("start temporal worker", "error", err)
		os.Exit(1)
	}
	slog.Info("temporal Go worker started", "task_queue", "corpscout-pipelines", "host", temporalHost)

	<-stop
	slog.Info("shutting down Go worker")
	w.Stop()
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		panic(fmt.Sprintf("required env var not set: %s", key))
	}
	return v
}
```

- [ ] **Step 2: Build to verify it compiles**

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines
GOWORK=off go build ./cmd/worker/...
```

Expected: no errors. Binary appears at `bin/worker` after `make build`.

- [ ] **Step 3: Commit**

```bash
git add cmd/
git commit -m "feat(data-pipelines): Go worker entrypoint"
```

---

## Task 6: Python FetchPage activity + worker

**Files:**
- Create: `data-pipelines/python/requirements.txt`
- Create: `data-pipelines/python/contracts.py`
- Create: `data-pipelines/python/activities/__init__.py`
- Create: `data-pipelines/python/activities/fetch_page.py`
- Create: `data-pipelines/python/test_activities.py`
- Create: `data-pipelines/python/main.py`

- [ ] **Step 1: Create `data-pipelines/python/requirements.txt`**

```
temporalio>=1.7.0
httpx>=0.27.0
pytest>=8.0.0
pytest-asyncio>=0.24.0
respx>=0.21.0
```

- [ ] **Step 2: Install dependencies**

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/python
pip install -r requirements.txt
```

Expected: packages install without errors.

- [ ] **Step 3: Create `data-pipelines/python/contracts.py`**

```python
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any


@dataclass
class FetchPageInput:
    source: str
    country: str
    page: int
    ids: list[str] = field(default_factory=list)
    cursor: str = ""


@dataclass
class RawRecord:
    native_id: str
    name: str
    status: str
    raw_json: dict[str, Any]
    hash: str
    company_type: str = ""


@dataclass
class FetchResult:
    records: list[RawRecord]
    has_more: bool
    next_cursor: str = ""
```

- [ ] **Step 4: Write the failing test**

Create `data-pipelines/python/test_activities.py`:
```python
from __future__ import annotations
import json
import pytest
import respx
import httpx

from contracts import FetchPageInput, FetchResult
from activities.fetch_page import fetch_page


@pytest.fixture(autouse=True)
def set_api_key(monkeypatch):
    monkeypatch.setenv("COMPANIES_HOUSE_API_KEY", "test-key")


@respx.mock
@pytest.mark.asyncio
async def test_fetch_page_returns_records():
    mock_response = {
        "total_results": 1,
        "items": [
            {
                "company_number": "12345678",
                "company_name": "ACME LIMITED",
                "company_status": "active",
                "company_type": "ltd",
            }
        ],
    }
    respx.get("https://api.company-information.service.gov.uk/advanced-search/companies").mock(
        return_value=httpx.Response(200, json=mock_response)
    )

    result = await fetch_page(FetchPageInput(source="companies_house", country="GB", page=1))

    assert isinstance(result, FetchResult)
    assert len(result.records) == 1
    assert result.records[0].native_id == "12345678"
    assert result.records[0].name == "ACME LIMITED"
    assert result.records[0].status == "active"
    assert result.has_more is False


@respx.mock
@pytest.mark.asyncio
async def test_fetch_page_has_more_when_more_results():
    items = [{"company_number": f"0000000{i}", "company_name": f"CO {i}", "company_status": "active", "company_type": "ltd"} for i in range(100)]
    mock_response = {"total_results": 250, "items": items}
    respx.get("https://api.company-information.service.gov.uk/advanced-search/companies").mock(
        return_value=httpx.Response(200, json=mock_response)
    )

    result = await fetch_page(FetchPageInput(source="companies_house", country="GB", page=1))
    assert result.has_more is True
    assert result.next_cursor == ",1"


@pytest.mark.asyncio
async def test_fetch_page_unsupported_source():
    with pytest.raises(ValueError, match="unsupported source"):
        await fetch_page(FetchPageInput(source="unknown", country="GB", page=1))
```

- [ ] **Step 5: Run tests to verify they fail**

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/python
python -m pytest test_activities.py -v
```

Expected: FAIL — `ModuleNotFoundError: No module named 'activities'`.

- [ ] **Step 6: Create `data-pipelines/python/activities/__init__.py`**

```python
```
(empty file)

- [ ] **Step 7: Implement `data-pipelines/python/activities/fetch_page.py`**

```python
from __future__ import annotations

import hashlib
import json
import os
from typing import Any

import httpx
from temporalio import activity

from contracts import FetchPageInput, FetchResult, RawRecord

_USER_AGENT = "corpscout-data-pipelines/1.0"
_CH_ENDPOINT = "https://api.company-information.service.gov.uk/advanced-search/companies"
_PAGE_SIZE = 100


@activity.defn
async def fetch_page(input: FetchPageInput) -> FetchResult:
    if input.source != "companies_house":
        raise ValueError(f"unsupported source: {input.source}")
    return await _fetch_companies_house(input)


async def _fetch_companies_house(input: FetchPageInput) -> FetchResult:
    api_key = os.environ.get("COMPANIES_HOUSE_API_KEY", "")
    if not api_key:
        raise RuntimeError("COMPANIES_HOUSE_API_KEY is not set")

    # cursor format: "YYYY-MM-DD,N" where N is 0-indexed page offset
    date_cursor: str | None = None
    page_offset = 0
    if input.cursor and "," in input.cursor:
        parts = input.cursor.split(",", 1)
        date_cursor = parts[0] or None
        try:
            page_offset = int(parts[1])
        except ValueError:
            page_offset = 0

    start_index = page_offset * _PAGE_SIZE
    params: dict[str, Any] = {
        "size": str(_PAGE_SIZE),
        "start_index": str(start_index),
        "company_status": "active",
    }
    if date_cursor:
        params["incorporated_from"] = date_cursor

    async with httpx.AsyncClient(timeout=30.0, auth=(api_key, "")) as client:
        resp = await client.get(
            _CH_ENDPOINT,
            params=params,
            headers={"Accept": "application/json", "User-Agent": _USER_AGENT},
        )
        resp.raise_for_status()
        data = resp.json()

    items: list[dict] = data.get("items") or []
    records: list[RawRecord] = []
    for item in items:
        raw_bytes = json.dumps(item, sort_keys=True).encode()
        digest = hashlib.sha256(raw_bytes).hexdigest()
        records.append(RawRecord(
            native_id=item.get("company_number", ""),
            name=item.get("company_name", ""),
            status=item.get("company_status", "active"),
            company_type=item.get("company_type", ""),
            raw_json=item,
            hash=digest,
        ))

    total_results: int = data.get("total_results", 0)
    has_more = (page_offset + 1) * _PAGE_SIZE < total_results
    next_cursor = ""
    if has_more:
        date_part = date_cursor or ""
        next_cursor = f"{date_part},{page_offset + 1}"

    return FetchResult(records=records, has_more=has_more, next_cursor=next_cursor)
```

- [ ] **Step 8: Run tests to verify they pass**

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/python
python -m pytest test_activities.py -v
```

Expected:
```
PASSED test_activities.py::test_fetch_page_returns_records
PASSED test_activities.py::test_fetch_page_has_more_when_more_results
PASSED test_activities.py::test_fetch_page_unsupported_source
```

- [ ] **Step 9: Create `data-pipelines/python/main.py`**

```python
from __future__ import annotations

import asyncio
import logging
import os
import sys

from temporalio.client import Client
from temporalio.worker import Worker

from activities.fetch_page import fetch_page

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


async def main() -> None:
    temporal_host = os.environ.get("TEMPORAL_HOST", "localhost:7233")
    logging.info("connecting to Temporal at %s", temporal_host)

    client = await Client.connect(temporal_host, namespace="corpscout")

    worker = Worker(
        client,
        task_queue="corpscout-pipelines-python",
        activities=[fetch_page],
    )

    logging.info("Python activity worker started on queue: corpscout-pipelines-python")
    await worker.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Python worker shut down")
        sys.exit(0)
```

- [ ] **Step 10: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines
git add python/
git commit -m "feat(data-pipelines): Python FetchPage activity + worker"
```

---

## Task 7: corpscout DB migration — temporal_executions + run_id

**Files:**
- Create: `corpscout/database/migrations/000031_temporal_executions.up.sql`
- Create: `corpscout/database/migrations/000031_temporal_executions.down.sql`

- [ ] **Step 1: Create the up migration**

Create `corpscout/database/migrations/000031_temporal_executions.up.sql`:
```sql
-- temporal_executions: one row per Temporal workflow started by corpscout.
CREATE TABLE temporal_executions (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_id      TEXT,
    workflow_run_id  TEXT,
    workflow_type    TEXT NOT NULL,
    source_name      TEXT NOT NULL,
    country          TEXT,
    input_ids        TEXT[],
    status           TEXT NOT NULL DEFAULT 'starting'
                         CHECK (status IN ('starting', 'running', 'completed', 'failed')),
    records_written  INT,
    pages_fetched    INT,
    error_message    TEXT,
    river_job_id     BIGINT,
    started_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at     TIMESTAMPTZ
);

CREATE INDEX idx_temporal_executions_status ON temporal_executions (status);
CREATE INDEX idx_temporal_executions_source ON temporal_executions (source_name);

-- Add run_id to raw_inputs tables so Temporal pipeline rows are traceable.
-- Nullable: existing rows written by SourcePullWorker will have run_id = NULL.
ALTER TABLE companies_house_company_raw_inputs ADD COLUMN IF NOT EXISTS run_id TEXT;
ALTER TABLE gleif_company_raw_inputs           ADD COLUMN IF NOT EXISTS run_id TEXT;
ALTER TABLE brreg_company_raw_inputs           ADD COLUMN IF NOT EXISTS run_id TEXT;

CREATE INDEX idx_ch_raw_inputs_run_id    ON companies_house_company_raw_inputs (run_id) WHERE run_id IS NOT NULL;
CREATE INDEX idx_gleif_raw_inputs_run_id ON gleif_company_raw_inputs           (run_id) WHERE run_id IS NOT NULL;
CREATE INDEX idx_brreg_raw_inputs_run_id ON brreg_company_raw_inputs           (run_id) WHERE run_id IS NOT NULL;
```

- [ ] **Step 2: Create the down migration**

Create `corpscout/database/migrations/000031_temporal_executions.down.sql`:
```sql
DROP INDEX IF EXISTS idx_ch_raw_inputs_run_id;
DROP INDEX IF EXISTS idx_gleif_raw_inputs_run_id;
DROP INDEX IF EXISTS idx_brreg_raw_inputs_run_id;

ALTER TABLE companies_house_company_raw_inputs DROP COLUMN IF EXISTS run_id;
ALTER TABLE gleif_company_raw_inputs           DROP COLUMN IF EXISTS run_id;
ALTER TABLE brreg_company_raw_inputs           DROP COLUMN IF EXISTS run_id;

DROP TABLE IF EXISTS temporal_executions;
```

- [ ] **Step 3: Apply the migration**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
make migrate-up
```

Expected: migration `000031` applied.

Verify:
```bash
psql "postgres://corpscout:corpscout@localhost:5435/corpscout?sslmode=disable" \
  -c "\d temporal_executions"
```

Expected: table with all columns listed.

- [ ] **Step 4: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
git add database/migrations/000031_temporal_executions.*
git commit -m "feat(corpscout): migration 000031 — temporal_executions + run_id on raw_inputs"
```

---

## Task 8: corpscout SQL queries + sqlc generate

**Files:**
- Create: `corpscout/database/queries/temporal_executions.sql`
- Regenerated: `corpscout/scheduler/internal/db/gen/` (do not edit)

- [ ] **Step 1: Create `corpscout/database/queries/temporal_executions.sql`**

```sql
-- name: CreateTemporalExecution :one
INSERT INTO temporal_executions (workflow_type, source_name, country, input_ids, river_job_id)
VALUES ($1, $2, $3, $4, $5)
RETURNING *;

-- name: UpdateTemporalExecutionStarted :exec
UPDATE temporal_executions
SET workflow_id     = $2,
    workflow_run_id = $3,
    status          = 'running'
WHERE id = $1;

-- name: UpdateTemporalExecutionFailed :exec
UPDATE temporal_executions
SET status        = 'failed',
    error_message = $2,
    completed_at  = now()
WHERE id = $1;

-- name: ListTemporalExecutions :many
SELECT * FROM temporal_executions
ORDER BY started_at DESC
LIMIT $1 OFFSET $2;

-- name: GetTemporalExecution :one
SELECT * FROM temporal_executions
WHERE id = $1;
```

- [ ] **Step 2: Run sqlc generate**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
make sqlc-generate
```

Expected: new methods appear in `scheduler/internal/db/gen/`. No errors.

Check:
```bash
grep -l "CreateTemporalExecution\|ListTemporalExecutions" scheduler/internal/db/gen/*.go
```

Expected: at least one file found.

- [ ] **Step 3: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
git add database/queries/temporal_executions.sql scheduler/internal/db/gen/
git commit -m "feat(corpscout): temporal_executions SQL queries + sqlc generate"
```

---

## Task 9: corpscout Config + DataTaskArgs

**Files:**
- Modify: `corpscout/scheduler/internal/config/config.go`
- Modify: `corpscout/scheduler/internal/workers/workers.go`

- [ ] **Step 1: Add Temporal fields to Config**

In `corpscout/scheduler/internal/config/config.go`, add two fields to `Config` and their `Load()` entries:

```go
// In Config struct, add after LLMModel:
TemporalHost  string
TemporalUIURL string
```

```go
// In Load(), add after LLMModel:
TemporalHost:  getEnv("CORPSCOUT_TEMPORAL_HOST", "localhost:7233"),
TemporalUIURL: getEnv("CORPSCOUT_TEMPORAL_UI_URL", "http://localhost:8088"),
```

- [ ] **Step 2: Add DataTaskArgs to workers.go**

In `corpscout/scheduler/internal/workers/workers.go`, add after the existing `EnrichCompanyFinancialsArgs` block:

```go
// DataTaskArgs triggers a Temporal PullCompanies workflow for a given source.
// IDs nil means bulk pull; populated means individual lookup by native ID.
type DataTaskArgs struct {
	Source  string   `json:"source"`
	Country string   `json:"country"`
	IDs     []string `json:"ids,omitempty"`
}

func (DataTaskArgs) Kind() string { return "data_task" }
```

- [ ] **Step 3: Build to verify no compile errors**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go build ./...
```

Expected: no errors.

- [ ] **Step 4: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
git add scheduler/internal/config/config.go scheduler/internal/workers/workers.go
git commit -m "feat(corpscout): Temporal config fields + DataTaskArgs"
```

---

## Task 10: DataTaskWorker

**Files:**
- Create: `corpscout/scheduler/internal/workers/data_task.go`

- [ ] **Step 1: Add Temporal Go SDK to corpscout scheduler**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go get go.temporal.io/sdk@v1.27.0
GOWORK=off go mod tidy
```

- [ ] **Step 2: Create `corpscout/scheduler/internal/workers/data_task.go`**

```go
package workers

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/cockroachdb/errors"
	"github.com/riverqueue/river"
	"go.temporal.io/sdk/client"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

// DataTaskWorker starts a Temporal PullCompanies workflow and records its ID.
// The River job exits immediately — Temporal runs the pipeline independently.
type DataTaskWorker struct {
	river.WorkerDefaults[DataTaskArgs]
	db       db.Querier
	temporal client.Client
}

func NewDataTaskWorker(q db.Querier, tc client.Client) *DataTaskWorker {
	return &DataTaskWorker{db: q, temporal: tc}
}

func (w *DataTaskWorker) Work(ctx context.Context, job *river.Job[DataTaskArgs]) error {
	args := job.Args

	// 1. Insert a tracking row (status = starting).
	riverJobID := job.ID
	exec, err := w.db.CreateTemporalExecution(ctx, db.CreateTemporalExecutionParams{
		WorkflowType: "PullCompanies",
		SourceName:   args.Source,
		Country:      &args.Country,
		InputIds:     args.IDs,
		RiverJobID:   &riverJobID,
	})
	if err != nil {
		return errors.Wrap(err, "create temporal execution record")
	}

	// 2. Start the Temporal workflow.
	workflowID := fmt.Sprintf("pull-%s-%s-%d", args.Source, args.Country, job.ID)
	we, err := w.temporal.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			ID:        workflowID,
			TaskQueue: "corpscout-pipelines",
		},
		"PullCompanies",
		map[string]any{
			"source":          args.Source,
			"country":         args.Country,
			"ids":             args.IDs,
			"corpscout_run_id": exec.ID.String(),
		},
	)
	if err != nil {
		dbErr := w.db.UpdateTemporalExecutionFailed(ctx, db.UpdateTemporalExecutionFailedParams{
			ID:           exec.ID,
			ErrorMessage: ptr(err.Error()),
		})
		if dbErr != nil {
			slog.Warn("data_task: mark execution failed after workflow start error", "error", dbErr)
		}
		return errors.Wrap(err, "start temporal workflow")
	}

	// 3. Record the workflow ID so the UI can track it.
	runID := we.GetRunID()
	if err := w.db.UpdateTemporalExecutionStarted(ctx, db.UpdateTemporalExecutionStartedParams{
		ID:             exec.ID,
		WorkflowID:     &workflowID,
		WorkflowRunID:  &runID,
	}); err != nil {
		slog.Warn("data_task: update temporal execution started", "error", err)
	}

	slog.Info("data_task: Temporal workflow started",
		"workflow_id", workflowID,
		"source", args.Source,
		"country", args.Country,
	)
	return nil // River job done — Temporal handles the rest.
}

func ptr(s string) *string { return &s }
```

Note: `UpdateTemporalExecutionStarted` takes `WorkflowID *string` and `WorkflowRunID *string` — check the generated sqlc types match. If sqlc generates `pgtype.Text` instead of `*string`, adjust accordingly to pass `pgtype.Text{String: workflowID, Valid: true}`.

- [ ] **Step 3: Build to verify compile**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go build ./internal/workers/...
```

Expected: no errors.

- [ ] **Step 4: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
git add scheduler/internal/workers/data_task.go scheduler/go.mod scheduler/go.sum
git commit -m "feat(corpscout): DataTaskWorker starts Temporal PullCompanies workflow"
```

---

## Task 11: Wire Temporal into corpscout app

**Files:**
- Modify: `corpscout/scheduler/internal/app/app.go`
- Modify: `corpscout/scheduler/internal/app/river.go`

- [ ] **Step 1: Add temporal client to app.go**

In `corpscout/scheduler/internal/app/app.go`:

Add import:
```go
"go.temporal.io/sdk/client"
```

Add field to `Server` struct:
```go
temporal client.Client
```

In `NewServer`, after `riverClient, err := setupRiver(...)`:
```go
temporalClient, err := client.Dial(client.Options{
    HostPort:  cfg.TemporalHost,
    Namespace: "corpscout",
})
if err != nil {
    return nil, errors.Wrap(err, "connect to temporal")
}
```

Pass `temporalClient` to `setupRiver` (update signature) and store on `Server`:
```go
riverClient, err := setupRiver(ctx, pool, cfg, queries, crawler, s3, temporalClient)
```

```go
return &Server{
    cfg:      cfg,
    pool:     pool,
    river:    riverClient,
    http:     &http.Server{Addr: cfg.ListenAddr, Handler: r},
    temporal: temporalClient,
}, nil
```

In `Shutdown`, close temporal client:
```go
s.temporal.Close()
```

Also pass `temporalClient` and `cfg.TemporalUIURL` to `NewHandlers`:
```go
httpapi.NewHandlers(queries, riverClient, pool, crawler, s3, cfg.PostgRESTURL, temporalClient, cfg.TemporalUIURL).RegisterRoutes(r)
```

- [ ] **Step 2: Register DataTaskWorker in river.go**

In `corpscout/scheduler/internal/app/river.go`, update `setupRiver` signature:
```go
func setupRiver(ctx context.Context, pool *pgxpool.Pool, cfg config.Config, q db.Querier, crawler *crawlerclient.Client, s3 *s3client.Client, tc client.Client) (*river.Client[pgx.Tx], error) {
```

Add after existing workers:
```go
dataTaskWorker := workers.NewDataTaskWorker(q, tc)
river.AddWorker(w, dataTaskWorker)
```

Add to `riverCfg.Queues`:
```go
"data_task": {MaxWorkers: 5},
```

Add import:
```go
"go.temporal.io/sdk/client"
```

- [ ] **Step 3: Build to verify**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go build ./...
```

Expected: no errors. If `NewHandlers` signature changed, update its constructor to accept `tc client.Client` and `temporalUIURL string` fields.

- [ ] **Step 4: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
git add scheduler/internal/app/
git commit -m "feat(corpscout): wire Temporal client + DataTaskWorker into app"
```

---

## Task 12: corpscout API handler for temporal executions

**Files:**
- Create: `corpscout/scheduler/internal/httpapi/temporal_executions.go`
- Modify: `corpscout/scheduler/internal/httpapi/handlers.go`

- [ ] **Step 1: Create `corpscout/scheduler/internal/httpapi/temporal_executions.go`**

```go
package httpapi

import (
	"log/slog"
	"net/http"

	"go.temporal.io/sdk/client"
	enumspb "go.temporal.io/api/enums/v1"
)

type temporalExecutionRow struct {
	ID             string  `json:"id"`
	WorkflowID     *string `json:"workflow_id,omitempty"`
	WorkflowRunID  *string `json:"workflow_run_id,omitempty"`
	WorkflowType   string  `json:"workflow_type"`
	SourceName     string  `json:"source_name"`
	Country        *string `json:"country,omitempty"`
	Status         string  `json:"status"`
	RecordsWritten *int32  `json:"records_written,omitempty"`
	PagesFetched   *int32  `json:"pages_fetched,omitempty"`
	ErrorMessage   *string `json:"error_message,omitempty"`
	RiverJobID     *int64  `json:"river_job_id,omitempty"`
	StartedAt      string  `json:"started_at"`
	CompletedAt    *string `json:"completed_at,omitempty"`
	TemporalUIURL  string  `json:"temporal_ui_url,omitempty"`
}

func (h *Handlers) handleListTemporalExecutions(w http.ResponseWriter, r *http.Request) {
	if h.pool == nil {
		writeError(w, http.StatusServiceUnavailable, "database not available")
		return
	}

	page := queryInt(r, "page", 1)
	limit := min(queryInt(r, "limit", 50), 200)
	offset := (page - 1) * limit

	rows, err := h.db.ListTemporalExecutions(r.Context(), int32(limit), int32(offset))
	if err != nil {
		slog.Error("list temporal executions", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	items := make([]temporalExecutionRow, 0, len(rows))
	for _, row := range rows {
		item := temporalExecutionRow{
			ID:           row.ID.String(),
			WorkflowType: row.WorkflowType,
			SourceName:   row.SourceName,
			Status:       row.Status,
			StartedAt:    row.StartedAt.Time.Format("2006-01-02T15:04:05Z"),
		}
		if row.WorkflowID != nil {
			item.WorkflowID = row.WorkflowID
		}
		if row.WorkflowRunID != nil {
			item.WorkflowRunID = row.WorkflowRunID
		}
		if row.Country != nil {
			item.Country = row.Country
		}
		if row.RecordsWritten != nil {
			item.RecordsWritten = row.RecordsWritten
		}
		if row.PagesFetched != nil {
			item.PagesFetched = row.PagesFetched
		}
		if row.ErrorMessage != nil {
			item.ErrorMessage = row.ErrorMessage
		}
		if row.RiverJobID != nil {
			item.RiverJobID = row.RiverJobID
		}
		if row.CompletedAt.Valid {
			t := row.CompletedAt.Time.Format("2006-01-02T15:04:05Z")
			item.CompletedAt = &t
		}

		// Enrich running workflows with live status from Temporal.
		if row.Status == "running" && row.WorkflowID != nil && h.temporal != nil {
			if desc, err := h.temporal.DescribeWorkflowExecution(r.Context(), *row.WorkflowID, ""); err == nil {
				if desc.WorkflowExecutionInfo != nil && desc.WorkflowExecutionInfo.Status == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
					item.Status = "running"
				}
			}
		}

		if row.WorkflowID != nil && h.temporalUIURL != "" {
			item.TemporalUIURL = h.temporalUIURL + "/namespaces/corpscout/workflows/" + *row.WorkflowID
		}

		items = append(items, item)
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"items": items, "page": page, "limit": limit,
	})
}
```

- [ ] **Step 2: Update Handlers struct and NewHandlers in handlers.go**

In `corpscout/scheduler/internal/httpapi/handlers.go`:

Add fields to `Handlers`:
```go
temporal       client.Client
temporalUIURL  string
```

Update `NewHandlers` signature:
```go
func NewHandlers(q db.Querier, rv *river.Client[pgx.Tx], pool *pgxpool.Pool, crawler *crawlerclient.Client, s3 *s3client.Client, postgrestURL string, tc client.Client, temporalUIURL string) *Handlers {
    return &Handlers{
        db:           q,
        rv:           rv,
        pool:         pool,
        crawler:      crawler,
        s3:           s3,
        postgrestURL: postgrestURL,
        temporal:     tc,
        temporalUIURL: temporalUIURL,
    }
}
```

Add import:
```go
"go.temporal.io/sdk/client"
```

Add route in `RegisterRoutes` after `/jobs/{id}/cancel`:
```go
r.Get("/temporal-executions", h.handleListTemporalExecutions)
```

- [ ] **Step 3: Build + run tests**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go build ./...
GOWORK=off go test ./... 2>&1 | grep -v "^ok"
```

Expected: no compile errors. Tests may show failures if `NewHandlers` test helpers need updating — fix any test helper calls that pass the wrong number of arguments to `NewHandlers`.

To fix test helpers: grep for `NewHandlers` in test files:
```bash
grep -rn "NewHandlers" /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler/
```

For any test that calls `NewHandlers`, add `nil, ""` as the last two arguments (nil temporal client, empty UI URL).

- [ ] **Step 4: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
git add scheduler/internal/httpapi/temporal_executions.go scheduler/internal/httpapi/handlers.go
git commit -m "feat(corpscout): API handler GET /temporal-executions"
```

---

## Task 13: corpscout UI — TypeScript types + API method

**Files:**
- Modify: `corpscout/ui/app/types/api.ts`
- Modify: `corpscout/ui/app/lib/api.ts`

- [ ] **Step 1: Add TemporalExecution type to `ui/app/types/api.ts`**

Add at the end of the file:
```typescript
export interface TemporalExecution {
  id: string;
  workflow_id?: string;
  workflow_run_id?: string;
  workflow_type: string;
  source_name: string;
  country?: string;
  status: "starting" | "running" | "completed" | "failed";
  records_written?: number;
  pages_fetched?: number;
  error_message?: string;
  river_job_id?: number;
  started_at: string;
  completed_at?: string;
  temporal_ui_url?: string;
}

export interface TemporalExecutionsResponse {
  items: TemporalExecution[];
  page: number;
  limit: number;
}
```

- [ ] **Step 2: Add getTemporalExecutions to `ui/app/lib/api.ts`**

Add to the imports at the top of `api.ts`:
```typescript
import type {
  // ... existing imports ...
  TemporalExecution,
  TemporalExecutionsResponse,
} from "~/types/api";
```

Add to the `api` export object after `getJobs`:
```typescript
getTemporalExecutions: (params: { page?: number; limit?: number } = {}) => {
  const qs = new URLSearchParams();
  if (params.page) qs.set("page", String(params.page));
  if (params.limit) qs.set("limit", String(params.limit));
  return get<TemporalExecutionsResponse>(`/temporal-executions?${qs}`);
},
```

- [ ] **Step 3: Typecheck**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/ui
pnpm typecheck
```

Expected: no errors related to the new types.

- [ ] **Step 4: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
git add ui/app/types/api.ts ui/app/lib/api.ts
git commit -m "feat(corpscout): TemporalExecution types + api.getTemporalExecutions"
```

---

## Task 14: corpscout UI — Temporal Tasks tab on Jobs page

**Files:**
- Modify: `corpscout/ui/app/routes/jobs.tsx`

- [ ] **Step 1: Add Temporal Tasks tab to `ui/app/routes/jobs.tsx`**

At the top of `jobs.tsx`, add imports:
```typescript
import { useState } from "react";  // likely already imported
import type { TemporalExecution, TemporalExecutionsResponse } from "~/types/api";
import { ExternalLink } from "lucide-react";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "~/components/ui/tabs";
```

Add a `TemporalStatusBadge` component after the existing `KindBadge`:
```typescript
function TemporalStatusBadge({ status }: { status: TemporalExecution["status"] }) {
  if (status === "completed")
    return <Badge className="bg-green-100 text-green-800 border-green-200 gap-1" variant="outline"><CheckCircle2 className="size-3" />Completed</Badge>;
  if (status === "running")
    return <Badge className="bg-blue-100 text-blue-800 border-blue-200 gap-1" variant="outline"><Loader2 className="size-3 animate-spin" />Running</Badge>;
  if (status === "failed")
    return <Badge className="bg-red-100 text-red-800 border-red-200 gap-1" variant="outline"><XCircle className="size-3" />Failed</Badge>;
  return <Badge variant="outline">{status}</Badge>;
}
```

Add a `TemporalTasksTab` component:
```typescript
function TemporalTasksTab() {
  const [page, setPage] = useState(1);
  const [data, setData] = useState<TemporalExecutionsResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async (p: number) => {
    setLoading(true);
    setError(null);
    try {
      const res = await api.getTemporalExecutions({ page: p, limit: 50 });
      setData(res);
      setPage(p);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { load(1); }, [load]);

  if (loading && !data) return <div className="p-6 text-muted-foreground text-sm">Loading…</div>;
  if (error) return <Alert className="m-4"><AlertDescription>{error}</AlertDescription></Alert>;
  if (!data) return null;

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between px-1">
        <p className="text-sm text-muted-foreground">{data.items.length} execution(s)</p>
        <Button variant="ghost" size="sm" onClick={() => load(page)}>
          <RefreshCw className="size-4 mr-1" /> Refresh
        </Button>
      </div>
      <div className="rounded-md border">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Workflow</TableHead>
              <TableHead>Source</TableHead>
              <TableHead>Country</TableHead>
              <TableHead>Status</TableHead>
              <TableHead>Progress</TableHead>
              <TableHead>Started</TableHead>
              <TableHead></TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {data.items.length === 0 && (
              <TableRow>
                <TableCell colSpan={7} className="text-center text-muted-foreground py-8">
                  No Temporal executions yet
                </TableCell>
              </TableRow>
            )}
            {data.items.map((ex) => (
              <TableRow key={ex.id}>
                <TableCell className="font-mono text-xs">{ex.workflow_type}</TableCell>
                <TableCell>
                  <Badge variant="outline" className="text-xs">{ex.source_name}</Badge>
                </TableCell>
                <TableCell className="text-sm">{ex.country ?? "—"}</TableCell>
                <TableCell><TemporalStatusBadge status={ex.status} /></TableCell>
                <TableCell className="text-xs text-muted-foreground">
                  {ex.status === "completed" && ex.records_written != null
                    ? `${ex.records_written} records, ${ex.pages_fetched ?? 0} pages`
                    : ex.status === "failed" && ex.error_message
                    ? <span className="text-red-600">{ex.error_message}</span>
                    : "—"}
                </TableCell>
                <TableCell className="text-xs text-muted-foreground">
                  {timeAgo(ex.started_at)}
                </TableCell>
                <TableCell>
                  {ex.temporal_ui_url && (
                    <a href={ex.temporal_ui_url} target="_blank" rel="noopener noreferrer"
                       className="text-xs text-blue-600 hover:underline flex items-center gap-1">
                      <ExternalLink className="size-3" /> Details
                    </a>
                  )}
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </div>
    </div>
  );
}
```

Wrap the existing Jobs page content in a `Tabs` component. Find the existing top-level `<div>` return in the main `JobsPage` component and wrap:
```tsx
return (
  <div className="container mx-auto py-6 space-y-6">
    <div className="flex items-center justify-between">
      <h1 className="text-2xl font-bold">Jobs</h1>
    </div>

    <Tabs defaultValue="river">
      <TabsList>
        <TabsTrigger value="river">River Jobs</TabsTrigger>
        <TabsTrigger value="temporal">Temporal Tasks</TabsTrigger>
      </TabsList>

      <TabsContent value="river">
        {/* existing stats, filters, table JSX goes here unchanged */}
      </TabsContent>

      <TabsContent value="temporal">
        <TemporalTasksTab />
      </TabsContent>
    </Tabs>
  </div>
);
```

- [ ] **Step 2: Typecheck**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/ui
pnpm typecheck
```

Expected: no errors.

- [ ] **Step 3: Start dev server and verify UI**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/ui
pnpm dev
```

Open `http://localhost:9999/jobs` — verify two tabs appear: "River Jobs" and "Temporal Tasks". Click "Temporal Tasks" — verify it shows "No Temporal executions yet" (since none have been created).

- [ ] **Step 4: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
git add ui/app/routes/jobs.tsx
git commit -m "feat(corpscout): Temporal Tasks tab on Jobs page"
```

---

## Task 15: End-to-end smoke test

This task verifies all pieces work together using a real Temporal cluster and corpscout DB.

- [ ] **Step 1: Start Temporal local dev**

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines
docker compose up -d
make create-namespace
```

Expected: Temporal UI at `http://localhost:8088` shows namespace `corpscout`.

- [ ] **Step 2: Start the Go workflow worker**

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines
TEMPORAL_HOST=localhost:7233 \
CORPSCOUT_DB_URL="postgres://corpscout:corpscout@localhost:5435/corpscout?sslmode=disable" \
GOWORK=off go run ./cmd/worker/
```

Expected: `temporal Go worker started task_queue=corpscout-pipelines`.

- [ ] **Step 3: Start the Python activity worker**

In a separate terminal:
```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/python
TEMPORAL_HOST=localhost:7233 \
COMPANIES_HOUSE_API_KEY=<your-key> \
python main.py
```

Expected: `Python activity worker started on queue: corpscout-pipelines-python`.

- [ ] **Step 4: Start corpscout scheduler with Temporal config**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
CORPSCOUT_TEMPORAL_HOST=localhost:7233 \
CORPSCOUT_TEMPORAL_UI_URL=http://localhost:8088 \
DATABASE_URL="postgres://corpscout:corpscout@localhost:5435/corpscout?sslmode=disable" \
GOWORK=off go run ./cmd/worker/
```

Expected: scheduler starts, River workers register including `data_task`.

- [ ] **Step 5: Trigger a DataTask job via API**

```bash
# Insert a data_task River job via the scheduler API
curl -s -X POST http://localhost:8090/api/v1/sources/companies_house/trigger \
  -H "Content-Type: application/json"
```

Or manually insert via psql if a trigger endpoint isn't wired yet:
```bash
psql "postgres://corpscout:corpscout@localhost:5435/corpscout?sslmode=disable" -c "
SELECT river_job_insert('{
  \"kind\": \"data_task\",
  \"args\": {\"source\": \"companies_house\", \"country\": \"GB\"},
  \"queue\": \"data_task\",
  \"priority\": 1,
  \"max_attempts\": 3
}'::jsonb);
"
```

Or use the River Go client directly in a test program.

- [ ] **Step 6: Verify in Temporal UI**

Open `http://localhost:8088`. Navigate to namespace `corpscout`. Verify a `PullCompanies` workflow appears and starts running.

- [ ] **Step 7: Verify records in DB**

After the workflow completes (or after a few pages):
```bash
psql "postgres://corpscout:corpscout@localhost:5435/corpscout?sslmode=disable" \
  -c "SELECT count(*), run_id IS NOT NULL as has_run_id FROM companies_house_company_raw_inputs GROUP BY has_run_id;"
```

Expected: rows with `has_run_id = true` appear (written by the Temporal pipeline).

```bash
psql "postgres://corpscout:corpscout@localhost:5435/corpscout?sslmode=disable" \
  -c "SELECT id, workflow_type, source_name, status, records_written, pages_fetched FROM temporal_executions;"
```

Expected: one row with `status = completed` after the workflow finishes.

- [ ] **Step 8: Verify UI tab**

Open `http://localhost:9999/jobs` → "Temporal Tasks" tab. Verify the completed execution appears with `records_written` and a "Details" link to the Temporal UI.

---

## Self-Review Checklist

**Spec coverage:**
- ✅ New data-pipelines service with Go + Python workers (Tasks 1–6)
- ✅ Temporal local dev docker-compose (Task 1)
- ✅ `corpscout` namespace (Task 1 step 3)
- ✅ PullCompanies workflow — FetchPage → WriteRawInputs → MarkExecutionComplete (Task 4)
- ✅ Python FetchPage for Companies House (Task 6)
- ✅ Go WriteRawInputs — writes to companies_house_company_raw_inputs (Task 3)
- ✅ Go MarkExecutionComplete — updates temporal_executions (Task 3)
- ✅ DB migration — temporal_executions + run_id on raw_inputs (Task 7)
- ✅ SQL queries + sqlc generate (Task 8)
- ✅ Config — CORPSCOUT_TEMPORAL_HOST, CORPSCOUT_TEMPORAL_UI_URL (Task 9)
- ✅ DataTaskArgs + DataTaskWorker (Tasks 9–10)
- ✅ Temporal client wired into app.go (Task 11)
- ✅ DataTaskWorker registered with data_task queue in river.go (Task 11)
- ✅ GET /api/v1/temporal-executions handler (Task 12)
- ✅ TypeScript types + api.getTemporalExecutions (Task 13)
- ✅ Temporal Tasks tab on Jobs page (Task 14)
- ✅ Deep link to Temporal UI (Task 14)
- ✅ Existing SourcePullWorker and SourceProcessWorker unchanged
