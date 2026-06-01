# BRREG Workflow Raw Record Read Model Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the BRREG raw input page read from `brreg_workflow.raw_records` and workflow artifact/task tables instead of `brreg_company_raw_inputs`.

**Architecture:** Add SQL read-model views under `brreg_workflow`, keep writes in existing workflow tables, and make the BRREG package API query those views. The React raw input route remains generic for non-BRREG sources, while BRREG uses a source-specific client path backed by the workflow schema.

**Tech Stack:** PostgreSQL migrations/views, Go scheduler HTTP/service packages, pgx/pgxmock tests, React/TypeScript UI.

---

### Task 1: Add BRREG Workflow Raw Record List View

**Files:**
- Create: `database/migrations/000054_brreg_workflow_raw_record_read_models.up.sql`
- Create: `database/migrations/000054_brreg_workflow_raw_record_read_models.down.sql`
- Modify: `scheduler/internal/db/brreg_workflow_store_migration_test.go`

- [ ] **Step 1: Write the failing migration-shape test**

Add this test to `scheduler/internal/db/brreg_workflow_store_migration_test.go`:

```go
func TestBrregWorkflowRawRecordReadModelMigrationDefinesListAndDetailViews(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000054_brreg_workflow_raw_record_read_models.up.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "CREATE OR REPLACE VIEW brreg_workflow.v_raw_record_list")
	require.Contains(t, sql, "CREATE OR REPLACE VIEW brreg_workflow.v_raw_record_detail")
	require.Contains(t, sql, "latest_translation")
	require.Contains(t, sql, "latest_domain")
	require.Contains(t, sql, "latest_financial")
	require.Contains(t, sql, "latest_enhanced")
	require.Contains(t, sql, "task_statuses")
	require.Contains(t, sql, "task_errors")
	require.NotContains(t, sql, "brreg_company_raw_inputs")
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./internal/db -run TestBrregWorkflowRawRecordReadModelMigrationDefinesListAndDetailViews -count=1
```

Expected: FAIL because migration `000054_brreg_workflow_raw_record_read_models.up.sql` does not exist.

- [ ] **Step 3: Create the read-model migration**

Create `database/migrations/000054_brreg_workflow_raw_record_read_models.up.sql`:

```sql
CREATE OR REPLACE VIEW brreg_workflow.v_raw_record_list AS
WITH latest_translation AS (
  SELECT DISTINCT ON (raw_record_id)
    raw_record_id,
    status,
    created_at
  FROM brreg_workflow.translation_results
  ORDER BY raw_record_id, created_at DESC
),
latest_domain AS (
  SELECT DISTINCT ON (raw_record_id)
    raw_record_id,
    status,
    best_domain,
    created_at
  FROM brreg_workflow.domain_results
  ORDER BY raw_record_id, created_at DESC
),
latest_financial AS (
  SELECT DISTINCT ON (raw_record_id)
    raw_record_id,
    status,
    original_currency,
    created_at
  FROM brreg_workflow.financial_results
  ORDER BY raw_record_id, created_at DESC
),
latest_enhanced AS (
  SELECT DISTINCT ON (raw_record_id)
    raw_record_id,
    status,
    built_at
  FROM brreg_workflow.enhanced_records
  ORDER BY raw_record_id, built_at DESC
),
task_statuses AS (
  SELECT
    raw_record_id,
    jsonb_object_agg(task_type, status ORDER BY task_type) AS statuses
  FROM brreg_workflow.raw_record_task_states
  GROUP BY raw_record_id
),
task_errors AS (
  SELECT
    raw_record_id,
    jsonb_object_agg(
      task_type,
      jsonb_build_object(
        'status', status,
        'error_category', error_category,
        'error_code', error_code,
        'retry_strategy', retry_strategy,
        'last_error', last_error
      )
      ORDER BY task_type
    ) FILTER (WHERE last_error IS NOT NULL OR error_category IS NOT NULL OR error_code IS NOT NULL) AS errors
  FROM brreg_workflow.raw_record_task_states
  GROUP BY raw_record_id
)
SELECT
  rr.id,
  rr.organization_number,
  rr.organization_name,
  rr.website,
  rr.registration_status,
  rr.country_iso2,
  rr.payload_hash,
  rr.is_current,
  rr.first_seen_at,
  rr.last_seen_at,
  COALESCE(lt.status, 'not_started') AS translation_status,
  COALESCE(ld.status, 'not_started') AS domain_status,
  ld.best_domain,
  COALESCE(lf.status, 'not_started') AS financial_status,
  lf.original_currency,
  COALESCE(le.status, 'not_started') AS enhanced_status,
  CASE
    WHEN le.status IN ('built', 'published') THEN 'enhanced'
    WHEN lt.status IN ('succeeded', 'skipped')
     AND COALESCE(ld.status, 'skipped') IN ('succeeded', 'partial', 'not_found', 'skipped')
     AND COALESCE(lf.status, 'skipped') IN ('succeeded', 'not_available', 'skipped') THEN 'ready_to_enhance'
    WHEN lt.status = 'succeeded' THEN 'translated'
    ELSE 'input'
  END AS lifecycle_state,
  COALESCE(ts.statuses, '{}'::jsonb) AS task_statuses,
  COALESCE(te.errors, '{}'::jsonb) AS task_errors
FROM brreg_workflow.raw_records rr
LEFT JOIN latest_translation lt ON lt.raw_record_id = rr.id
LEFT JOIN latest_domain ld ON ld.raw_record_id = rr.id
LEFT JOIN latest_financial lf ON lf.raw_record_id = rr.id
LEFT JOIN latest_enhanced le ON le.raw_record_id = rr.id
LEFT JOIN task_statuses ts ON ts.raw_record_id = rr.id
LEFT JOIN task_errors te ON te.raw_record_id = rr.id
WHERE rr.is_current;

CREATE OR REPLACE VIEW brreg_workflow.v_raw_record_detail AS
WITH latest_translation AS (
  SELECT DISTINCT ON (raw_record_id)
    raw_record_id,
    status,
    translated_payload,
    model,
    prompt_version,
    error,
    created_at,
    metadata
  FROM brreg_workflow.translation_results
  ORDER BY raw_record_id, created_at DESC
),
latest_domain AS (
  SELECT DISTINCT ON (raw_record_id)
    raw_record_id,
    status,
    best_domain,
    domain_payload,
    error,
    created_at,
    metadata
  FROM brreg_workflow.domain_results
  ORDER BY raw_record_id, created_at DESC
),
latest_financial AS (
  SELECT DISTINCT ON (raw_record_id)
    raw_record_id,
    status,
    original_currency,
    original_payload,
    usd_payload,
    fx_metadata,
    source_uri,
    error,
    created_at,
    metadata
  FROM brreg_workflow.financial_results
  ORDER BY raw_record_id, created_at DESC
),
latest_enhanced AS (
  SELECT DISTINCT ON (raw_record_id)
    raw_record_id,
    schema_version,
    status,
    enhanced_payload,
    enhanced_payload_hash,
    built_at,
    published_at,
    error,
    metadata
  FROM brreg_workflow.enhanced_records
  ORDER BY raw_record_id, built_at DESC
),
task_states AS (
  SELECT
    raw_record_id,
    jsonb_agg(
      jsonb_build_object(
        'task_type', task_type,
        'status', status,
        'attempt_count', attempt_count,
        'last_started_at', last_started_at,
        'last_finished_at', last_finished_at,
        'next_retry_at', next_retry_at,
        'lease_until', lease_until,
        'error_category', error_category,
        'error_code', error_code,
        'retry_strategy', retry_strategy,
        'last_error', last_error,
        'result_summary', result_summary
      )
      ORDER BY task_type
    ) AS tasks
  FROM brreg_workflow.raw_record_task_states
  GROUP BY raw_record_id
)
SELECT
  rl.*,
  rr.raw_payload,
  rr.metadata AS raw_metadata,
  jsonb_build_object(
    'status', lt.status,
    'translated_payload', lt.translated_payload,
    'model', lt.model,
    'prompt_version', lt.prompt_version,
    'error', lt.error,
    'created_at', lt.created_at,
    'metadata', COALESCE(lt.metadata, '{}'::jsonb)
  ) AS translation_result,
  jsonb_build_object(
    'status', ld.status,
    'best_domain', ld.best_domain,
    'domain_payload', COALESCE(ld.domain_payload, '{}'::jsonb),
    'error', ld.error,
    'created_at', ld.created_at,
    'metadata', COALESCE(ld.metadata, '{}'::jsonb)
  ) AS domain_result,
  jsonb_build_object(
    'status', lf.status,
    'original_currency', lf.original_currency,
    'original_payload', COALESCE(lf.original_payload, '{}'::jsonb),
    'usd_payload', COALESCE(lf.usd_payload, '{}'::jsonb),
    'fx_metadata', COALESCE(lf.fx_metadata, '{}'::jsonb),
    'source_uri', lf.source_uri,
    'error', lf.error,
    'created_at', lf.created_at,
    'metadata', COALESCE(lf.metadata, '{}'::jsonb)
  ) AS financial_result,
  jsonb_build_object(
    'schema_version', le.schema_version,
    'status', le.status,
    'enhanced_payload', le.enhanced_payload,
    'enhanced_payload_hash', le.enhanced_payload_hash,
    'built_at', le.built_at,
    'published_at', le.published_at,
    'error', le.error,
    'metadata', COALESCE(le.metadata, '{}'::jsonb)
  ) AS enhanced_result,
  COALESCE(ts.tasks, '[]'::jsonb) AS tasks
FROM brreg_workflow.v_raw_record_list rl
JOIN brreg_workflow.raw_records rr ON rr.id = rl.id
LEFT JOIN latest_translation lt ON lt.raw_record_id = rr.id
LEFT JOIN latest_domain ld ON ld.raw_record_id = rr.id
LEFT JOIN latest_financial lf ON lf.raw_record_id = rr.id
LEFT JOIN latest_enhanced le ON le.raw_record_id = rr.id
LEFT JOIN task_states ts ON ts.raw_record_id = rr.id;
```

Create `database/migrations/000054_brreg_workflow_raw_record_read_models.down.sql`:

```sql
DROP VIEW IF EXISTS brreg_workflow.v_raw_record_detail;
DROP VIEW IF EXISTS brreg_workflow.v_raw_record_list;
```

- [ ] **Step 4: Run the migration-shape test**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./internal/db -run TestBrregWorkflowRawRecordReadModelMigrationDefinesListAndDetailViews -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit Task 1**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
git add database/migrations/000054_brreg_workflow_raw_record_read_models.up.sql database/migrations/000054_brreg_workflow_raw_record_read_models.down.sql scheduler/internal/db/brreg_workflow_store_migration_test.go
git commit -m "Add BRREG workflow raw record read views"
```

### Task 2: Make BRREG List API Use the Read Model

**Files:**
- Modify: `scheduler/internal/brreg/service/raw_records.go`
- Modify: `scheduler/internal/brreg/service/service_test.go`
- Modify: `scheduler/internal/brreg/httpapi/raw_records_test.go`

- [ ] **Step 1: Write the failing service test**

Update `TestListRawRecordsReadsBrregWorkflowRawRecords` in `scheduler/internal/brreg/service/service_test.go` so the expected SQL uses the view and includes workflow status fields:

```go
pool.ExpectQuery("SELECT count(*) FROM brreg_workflow.v_raw_record_list ri;;ri.organization_name ILIKE $1;;!brreg_company_raw_inputs").
	WithArgs("%BORT%").
	WillReturnRows(pgxmock.NewRows([]string{"count"}).AddRow(int64(1)))
pool.ExpectQuery("SELECT ri.id, ri.organization_number, ri.organization_name, ri.website, ri.registration_status, ri.payload_hash, ri.is_current, ri.first_seen_at, ri.last_seen_at, ri.lifecycle_state, ri.translation_status, ri.domain_status, ri.financial_status, ri.enhanced_status, ri.best_domain, ri.task_statuses, ri.task_errors FROM brreg_workflow.v_raw_record_list ri;;ORDER BY ri.last_seen_at DESC, ri.id DESC;;!brreg_company_raw_inputs").
	WithArgs("%BORT%", int32(50), int32(0)).
	WillReturnRows(pgxmock.NewRows([]string{
		"id", "organization_number", "organization_name", "website", "registration_status", "payload_hash", "is_current",
		"first_seen_at", "last_seen_at", "lifecycle_state", "translation_status", "domain_status", "financial_status",
		"enhanced_status", "best_domain", "task_statuses", "task_errors",
	}).AddRow(
		rawRecordID, "810202572", "BORTIGARD AS", "https://bortigard.example", "active", "hash-1", true,
		createdAt, createdAt, "translated", "succeeded", "not_found", "not_started", "not_started", nil,
		[]byte(`{"translate":"succeeded"}`), []byte(`{}`),
	))
```

Add assertions for `LifecycleState`, `TranslationStatus`, `DomainStatus`, `FinancialStatus`, `EnhancedStatus`, `TaskStatuses`, and `TaskErrors`.

- [ ] **Step 2: Run the service test to verify it fails**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./internal/brreg/service -run TestListRawRecordsReadsBrregWorkflowRawRecords -count=1
```

Expected: FAIL because the implementation still queries `brreg_workflow.raw_records` and does not scan the new fields.

- [ ] **Step 3: Update the DTO and query**

In `scheduler/internal/brreg/service/raw_records.go`, extend `RawRecordListItem`:

```go
type RawRecordListItem struct {
	ID                 uuid.UUID       `json:"id"`
	OrganizationNumber string          `json:"organization_number"`
	OrganizationName   string          `json:"organization_name"`
	Website            string          `json:"website"`
	RegistrationStatus string          `json:"registration_status"`
	PayloadHash        string          `json:"payload_hash"`
	IsCurrent          bool            `json:"is_current"`
	FirstSeenAt        time.Time       `json:"first_seen_at"`
	LastSeenAt         time.Time       `json:"last_seen_at"`
	LifecycleState     string          `json:"lifecycle_state"`
	TranslationStatus  string          `json:"translation_status"`
	DomainStatus       string          `json:"domain_status"`
	FinancialStatus    string          `json:"financial_status"`
	EnhancedStatus     string          `json:"enhanced_status"`
	BestDomain         string          `json:"best_domain"`
	TaskStatuses       json.RawMessage `json:"task_statuses"`
	TaskErrors         json.RawMessage `json:"task_errors"`
}
```

Change count SQL to:

```go
countSQL := fmt.Sprintf("SELECT count(*) FROM brreg_workflow.v_raw_record_list ri WHERE %s", where)
```

Change rows SQL to:

```go
rowsSQL := fmt.Sprintf(`
SELECT ri.id, ri.organization_number, ri.organization_name, ri.website, ri.registration_status, ri.payload_hash, ri.is_current,
       ri.first_seen_at, ri.last_seen_at, ri.lifecycle_state, ri.translation_status, ri.domain_status, ri.financial_status,
       ri.enhanced_status, ri.best_domain, ri.task_statuses, ri.task_errors
FROM brreg_workflow.v_raw_record_list ri
WHERE %s
ORDER BY ri.last_seen_at DESC, ri.id DESC
LIMIT $%d OFFSET $%d`, where, limitArg, offsetArg)
```

Scan nullable `best_domain` as `pgtype.Text` and `task_statuses` / `task_errors` as `json.RawMessage`.

- [ ] **Step 4: Run service and HTTP tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./internal/brreg/service ./internal/brreg/httpapi -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit Task 2**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
git add scheduler/internal/brreg/service/raw_records.go scheduler/internal/brreg/service/service_test.go scheduler/internal/brreg/httpapi/raw_records_test.go
git commit -m "Read BRREG raw record list from workflow view"
```

### Task 3: Add BRREG Raw Record Detail API

**Files:**
- Create: `scheduler/internal/brreg/service/raw_record_detail.go`
- Create: `scheduler/internal/brreg/service/raw_record_detail_test.go`
- Create: `scheduler/internal/brreg/httpapi/raw_record_detail_test.go`
- Modify: `scheduler/internal/brreg/httpapi/raw_records.go`
- Modify: `scheduler/internal/brreg/httpapi/translation.go`
- Modify: `scheduler/internal/brreg/httpapi/translation_test.go`

- [ ] **Step 1: Write failing service detail test**

Create `scheduler/internal/brreg/service/raw_record_detail_test.go`:

```go
package service

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pashagolub/pgxmock/v3"
	"github.com/stretchr/testify/require"
)

func TestGetRawRecordDetailReadsWorkflowDetailView(t *testing.T) {
	pool := newServiceSQLMock(t)
	defer pool.Close()

	rawRecordID := uuid.New()
	seenAt := time.Date(2026, 5, 29, 8, 0, 0, 0, time.UTC)
	pool.ExpectQuery("FROM brreg_workflow.v_raw_record_detail ri;;ri.id = $1;;!brreg_company_raw_inputs").
		WithArgs(rawRecordID).
		WillReturnRows(pgxmock.NewRows([]string{
			"id", "organization_number", "organization_name", "website", "registration_status", "country_iso2",
			"payload_hash", "is_current", "first_seen_at", "last_seen_at", "lifecycle_state", "translation_status",
			"domain_status", "financial_status", "enhanced_status", "best_domain", "task_statuses", "task_errors",
			"raw_payload", "raw_metadata", "translation_result", "domain_result", "financial_result", "enhanced_result", "tasks",
		}).AddRow(
			rawRecordID, "810202572", "BORTIGARD AS", "https://bortigard.example", "active", "NO",
			"hash-1", true, seenAt, seenAt, "ready_to_enhance", "succeeded",
			"not_found", "skipped", "not_started", nil, []byte(`{"translate":"succeeded"}`), []byte(`{}`),
			[]byte(`{"navn":"BORTIGARD AS"}`), []byte(`{}`), []byte(`{"status":"succeeded","translated_payload":{"name":"BORTIGARD AS"}}`),
			[]byte(`{"status":"not_found"}`), []byte(`{"status":"skipped"}`), []byte(`{"status":null}`), []byte(`[{"task_type":"translate","status":"succeeded"}]`),
		))

	svc := NewWithBulkClient(nil, nil, pool, nil)

	detail, err := svc.GetRawRecordDetail(context.Background(), rawRecordID)

	require.NoError(t, err)
	require.Equal(t, rawRecordID, detail.ID)
	require.JSONEq(t, `{"navn":"BORTIGARD AS"}`, string(detail.RawPayload))
	require.JSONEq(t, `{"status":"succeeded","translated_payload":{"name":"BORTIGARD AS"}}`, string(detail.TranslationResult))
	require.JSONEq(t, `[{"task_type":"translate","status":"succeeded"}]`, string(detail.Tasks))
	require.NoError(t, pool.ExpectationsWereMet())
}
```

- [ ] **Step 2: Run the detail test to verify it fails**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./internal/brreg/service -run TestGetRawRecordDetailReadsWorkflowDetailView -count=1
```

Expected: FAIL because `GetRawRecordDetail` does not exist.

- [ ] **Step 3: Implement service detail method**

Create `scheduler/internal/brreg/service/raw_record_detail.go` with:

```go
package service

import (
	"context"
	"encoding/json"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

type RawRecordDetail struct {
	RawRecordListItem
	CountryISO2        string          `json:"country_iso2"`
	RawPayload         json.RawMessage `json:"raw_payload"`
	RawMetadata        json.RawMessage `json:"raw_metadata"`
	TranslationResult  json.RawMessage `json:"translation_result"`
	DomainResult       json.RawMessage `json:"domain_result"`
	FinancialResult    json.RawMessage `json:"financial_result"`
	EnhancedResult     json.RawMessage `json:"enhanced_result"`
	Tasks              json.RawMessage `json:"tasks"`
	FirstSeenAt        time.Time       `json:"first_seen_at"`
	LastSeenAt         time.Time       `json:"last_seen_at"`
}

func (s *Service) GetRawRecordDetail(ctx context.Context, id uuid.UUID) (RawRecordDetail, error) {
	if s == nil || s.pool == nil {
		return RawRecordDetail{}, errors.New("brreg raw record storage not available")
	}
	var detail RawRecordDetail
	var organizationName pgtype.Text
	var website pgtype.Text
	var registrationStatus pgtype.Text
	var bestDomain pgtype.Text
	err := s.pool.QueryRow(ctx, `
SELECT id, organization_number, organization_name, website, registration_status, country_iso2,
       payload_hash, is_current, first_seen_at, last_seen_at, lifecycle_state, translation_status,
       domain_status, financial_status, enhanced_status, best_domain, task_statuses, task_errors,
       raw_payload, raw_metadata, translation_result, domain_result, financial_result, enhanced_result, tasks
FROM brreg_workflow.v_raw_record_detail ri
WHERE ri.id = $1`, id).Scan(
		&detail.ID, &detail.OrganizationNumber, &organizationName, &website, &registrationStatus, &detail.CountryISO2,
		&detail.PayloadHash, &detail.IsCurrent, &detail.FirstSeenAt, &detail.LastSeenAt, &detail.LifecycleState,
		&detail.TranslationStatus, &detail.DomainStatus, &detail.FinancialStatus, &detail.EnhancedStatus, &bestDomain,
		&detail.TaskStatuses, &detail.TaskErrors, &detail.RawPayload, &detail.RawMetadata, &detail.TranslationResult,
		&detail.DomainResult, &detail.FinancialResult, &detail.EnhancedResult, &detail.Tasks,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return RawRecordDetail{}, CommandError{Message: "brreg raw record not found"}
	}
	if err != nil {
		return RawRecordDetail{}, errors.Wrap(err, "get brreg workflow raw record detail")
	}
	detail.OrganizationName = pgTextString(organizationName)
	detail.Website = pgTextString(website)
	detail.RegistrationStatus = pgTextString(registrationStatus)
	detail.BestDomain = pgTextString(bestDomain)
	return detail, nil
}
```

- [ ] **Step 4: Add HTTP detail route test and route**

Create `scheduler/internal/brreg/httpapi/raw_record_detail_test.go`:

```go
package httpapi

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	brregservice "github.com/pulsarpoint/corpscout/scheduler/internal/brreg/service"
)

func TestGetRawRecordDetailReturnsWorkflowDetail(t *testing.T) {
	rawRecordID := uuid.New()
	svc := &fakeBrregService{
		rawRecordDetail: brregservice.RawRecordDetail{
			RawRecordListItem: brregservice.RawRecordListItem{
				ID: rawRecordID,
				OrganizationNumber: "810202572",
				OrganizationName: "BORTIGARD AS",
				LifecycleState: "ready_to_enhance",
			},
			RawPayload: []byte(`{"navn":"BORTIGARD AS"}`),
			Tasks: []byte(`[{"task_type":"translate","status":"succeeded"}]`),
		},
	}
	router := chi.NewRouter()
	NewHandler(svc).RegisterRoutes(router)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/brreg/raw-records/"+rawRecordID.String(), nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), `"organization_number":"810202572"`)
	require.Contains(t, rec.Body.String(), `"raw_payload":{"navn":"BORTIGARD AS"}`)
	require.Equal(t, rawRecordID, svc.rawRecordDetailID)
}
```

Modify `scheduler/internal/brreg/httpapi/translation.go` service interface:

```go
GetRawRecordDetail(context.Context, uuid.UUID) (brregservice.RawRecordDetail, error)
```

Register:

```go
r.Get("/api/v1/brreg/raw-records/{id}", h.handleGetRawRecordDetail)
```

Add `handleGetRawRecordDetail` in `scheduler/internal/brreg/httpapi/raw_records.go`, parsing `uuid.Parse(chi.URLParam(r, "id"))`, returning `400` for invalid UUID, `404` for `brregservice.CommandError{Message: "brreg raw record not found"}`, and logging unexpected errors once.

- [ ] **Step 5: Run service and HTTP tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./internal/brreg/service ./internal/brreg/httpapi -count=1
```

Expected: PASS.

- [ ] **Step 6: Commit Task 3**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
git add scheduler/internal/brreg/service/raw_record_detail.go scheduler/internal/brreg/service/raw_record_detail_test.go scheduler/internal/brreg/httpapi/raw_records.go scheduler/internal/brreg/httpapi/raw_record_detail_test.go scheduler/internal/brreg/httpapi/translation.go scheduler/internal/brreg/httpapi/translation_test.go
git commit -m "Add BRREG workflow raw record detail API"
```

### Task 4: Switch BRREG Raw Input UI To BRREG Workflow API

**Files:**
- Modify: `ui/app/lib/api.ts`
- Modify: `ui/app/types/api.ts`
- Modify: `ui/app/routes/sources_.$name.raw_input.tsx`
- Create: `ui/app/components/app/BrregRawRecordsTable.tsx`

- [ ] **Step 1: Add TypeScript API types and client methods**

In `ui/app/types/api.ts`, add:

```ts
export interface BrregRawRecordListItem {
  id: string;
  organization_number: string;
  organization_name: string;
  website: string;
  registration_status: string;
  payload_hash: string;
  is_current: boolean;
  first_seen_at: string;
  last_seen_at: string;
  lifecycle_state: string;
  translation_status: string;
  domain_status: string;
  financial_status: string;
  enhanced_status: string;
  best_domain: string;
  task_statuses: Record<string, string>;
  task_errors: Record<string, unknown>;
}

export interface BrregRawRecordListResponse {
  items: BrregRawRecordListItem[];
  total: number;
  page: number;
  limit: number;
}

export interface BrregRawRecordDetail extends BrregRawRecordListItem {
  country_iso2: string;
  raw_payload: Record<string, unknown>;
  raw_metadata: Record<string, unknown>;
  translation_result: Record<string, unknown>;
  domain_result: Record<string, unknown>;
  financial_result: Record<string, unknown>;
  enhanced_result: Record<string, unknown>;
  tasks: Array<Record<string, unknown>>;
}
```

In `ui/app/lib/api.ts`, add:

```ts
getBrregRawRecords: (params: { page?: number; limit?: number; q?: string } = {}) => {
  const qs = new URLSearchParams();
  if (params.page) qs.set("page", String(params.page));
  if (params.limit) qs.set("limit", String(params.limit));
  if (params.q) qs.set("q", params.q);
  return get<BrregRawRecordListResponse>(`/brreg/raw-records${qs.toString() ? `?${qs.toString()}` : ""}`);
},

getBrregRawRecord: (id: string) =>
  get<BrregRawRecordDetail>(`/brreg/raw-records/${id}`),
```

- [ ] **Step 2: Create the BRREG-specific table component**

Create `ui/app/components/app/BrregRawRecordsTable.tsx` with these behaviors:

```tsx
import { useCallback, useEffect, useState } from "react";
import { useSearchParams } from "react-router";
import { api } from "~/lib/api";
import type { BrregRawRecordListItem } from "~/types/api";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { Input } from "~/components/ui/input";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "~/components/ui/table";

const PAGE_SIZE = 50;

function pageFromParams(searchParams: URLSearchParams) {
  const page = Number(searchParams.get("page") ?? "1");
  return Number.isFinite(page) && page > 0 ? page : 1;
}

function statusClass(status: string) {
  if (status === "succeeded" || status === "built" || status === "published") return "border-green-200 bg-green-100 text-green-800";
  if (status.includes("failed")) return "border-red-200 bg-red-100 text-red-800";
  if (status === "running") return "border-blue-200 bg-blue-100 text-blue-800";
  return "border-gray-200 bg-gray-50 text-gray-700";
}

export function BrregRawRecordsTable() {
  const [searchParams, setSearchParams] = useSearchParams();
  const [items, setItems] = useState<BrregRawRecordListItem[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);
  const [page, setPage] = useState(() => pageFromParams(searchParams));
  const [searchInput, setSearchInput] = useState(searchParams.get("q") ?? "");

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const res = await api.getBrregRawRecords({
        page,
        limit: PAGE_SIZE,
        q: searchParams.get("q") ?? undefined,
      });
      setItems(res.items);
      setTotal(res.total);
    } finally {
      setLoading(false);
    }
  }, [page, searchParams]);

  useEffect(() => {
    setPage(pageFromParams(searchParams));
    setSearchInput(searchParams.get("q") ?? "");
  }, [searchParams]);

  useEffect(() => {
    void load();
  }, [load]);

  function applySearch() {
    const next = new URLSearchParams(searchParams);
    if (searchInput.trim()) next.set("q", searchInput.trim());
    else next.delete("q");
    next.delete("page");
    setSearchParams(next, { replace: true });
  }

  const totalPages = Math.ceil(total / PAGE_SIZE);

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-2">
        <Input value={searchInput} onChange={(event) => setSearchInput(event.target.value)} placeholder="Search company name or org number" className="max-w-sm" />
        <Button type="button" variant="outline" onClick={applySearch}>Search</Button>
      </div>
      <div className="rounded-md border">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Organization</TableHead>
              <TableHead>Website</TableHead>
              <TableHead>Lifecycle</TableHead>
              <TableHead>Translation</TableHead>
              <TableHead>Domain</TableHead>
              <TableHead>Financial</TableHead>
              <TableHead>Enhanced</TableHead>
              <TableHead>Last Seen</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {loading && <TableRow><TableCell colSpan={8}>Loading...</TableCell></TableRow>}
            {!loading && items.length === 0 && <TableRow><TableCell colSpan={8}>No BRREG raw records found.</TableCell></TableRow>}
            {!loading && items.map((row) => (
              <TableRow key={row.id}>
                <TableCell>
                  <div className="font-medium">{row.organization_name || row.organization_number}</div>
                  <div className="text-xs text-muted-foreground">{row.organization_number}</div>
                </TableCell>
                <TableCell>{row.website || row.best_domain || "-"}</TableCell>
                <TableCell><Badge variant="outline">{row.lifecycle_state}</Badge></TableCell>
                <TableCell><Badge variant="outline" className={statusClass(row.translation_status)}>{row.translation_status}</Badge></TableCell>
                <TableCell><Badge variant="outline" className={statusClass(row.domain_status)}>{row.domain_status}</Badge></TableCell>
                <TableCell><Badge variant="outline" className={statusClass(row.financial_status)}>{row.financial_status}</Badge></TableCell>
                <TableCell><Badge variant="outline" className={statusClass(row.enhanced_status)}>{row.enhanced_status}</Badge></TableCell>
                <TableCell className="text-sm text-muted-foreground">{new Date(row.last_seen_at).toLocaleString()}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </div>
      <div className="flex items-center justify-between text-sm text-muted-foreground">
        <span>{total.toLocaleString()} records</span>
        <div className="flex items-center gap-2">
          <Button variant="outline" size="sm" disabled={page <= 1} onClick={() => setSearchParams((prev) => { const next = new URLSearchParams(prev); next.set("page", String(page - 1)); return next; })}>Previous</Button>
          <span>Page {page} of {Math.max(totalPages, 1)}</span>
          <Button variant="outline" size="sm" disabled={page >= totalPages} onClick={() => setSearchParams((prev) => { const next = new URLSearchParams(prev); next.set("page", String(page + 1)); return next; })}>Next</Button>
        </div>
      </div>
    </div>
  );
}
```

- [ ] **Step 3: Route BRREG to the new component**

Modify `ui/app/routes/sources_.$name.raw_input.tsx`:

```tsx
import { useOutletContext } from "react-router";
import type { SourceDetailContext } from "~/routes/sources_.$name";
import { BrregRawRecordsTable } from "~/components/app/BrregRawRecordsTable";
import { RawInputsTable } from "~/components/app/RawInputsTable";

export default function SourceRawInputPage() {
  const { source } = useOutletContext<SourceDetailContext>();
  if (source.name === "brreg") return <BrregRawRecordsTable />;
  return (
    <RawInputsTable
      sourceName={source.name}
      requiresTranslation={source.requires_translation}
      showTranslateAction={source.requires_translation}
    />
  );
}
```

- [ ] **Step 4: Run UI checks**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/ui
pnpm typecheck
pnpm build
```

Expected: both commands pass.

- [ ] **Step 5: Commit Task 4**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
git add ui/app/lib/api.ts ui/app/types/api.ts 'ui/app/routes/sources_.$name.raw_input.tsx' ui/app/components/app/BrregRawRecordsTable.tsx
git commit -m "Use BRREG workflow raw records in source UI"
```

### Task 5: Final Verification

**Files:**
- No new files.

- [ ] **Step 1: Run scheduler tests**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./...
```

Expected: PASS.

- [ ] **Step 2: Run UI checks**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/ui
pnpm typecheck
pnpm build
```

Expected: PASS.

- [ ] **Step 3: Check final git state**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
git status --short
git log --oneline -5
```

Expected: clean worktree after commits, with the read-model commits at the top.
