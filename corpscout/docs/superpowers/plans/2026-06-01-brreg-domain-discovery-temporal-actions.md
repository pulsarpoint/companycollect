# BRREG Domain Discovery Temporal Actions Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace coarse BRREG domain discovery with a Temporal-orchestrated, action-level workflow where search, crawl, LLM analysis, fallback, decision, and persistence are independently observable and retryable.

**Architecture:** Corpscout owns selection, task state, action artifacts, final domain results, and Temporal workflow orchestration. Crawl service exposes small HTTP operations for search fetching, page crawling, search analysis, and site analysis. The parent workflow selects eligible companies and starts one child workflow per company; the child workflow performs deterministic action orchestration and writes a final `domain_results` row only after all relevant artifacts exist.

**Tech Stack:** Go, Temporal Go SDK, PostgreSQL, sqlc, FastAPI, Pydantic, crawl4ai, direct LLM HTTP calls, existing Corpscout scheduler and data-pipelines crawl-service repos.

---

## File Map

**Corpscout database and sqlc**
- Create `database/migrations/000066_brreg_domain_action_artifacts.up.sql`
- Create `database/migrations/000066_brreg_domain_action_artifacts.down.sql`
- Modify `database/queries/brreg_workflow.sql`
- Regenerate `scheduler/internal/db/gen/*`
- Create `scheduler/internal/db/brreg_domain_action_artifacts_migration_test.go`

**Corpscout BRREG DB gateway**
- Create `scheduler/internal/brreg/db/domain_actions.go`
- Modify `scheduler/internal/brreg/db/types.go`
- Modify `scheduler/internal/brreg/db/gateway.go` only if constructor/config needs no-op defaults
- Create or extend `scheduler/internal/brreg/db/gateway_test.go`

**Corpscout crawl service client**
- Modify `scheduler/internal/crawlserviceclient/client.go`
- Modify `scheduler/internal/crawlserviceclient/client_test.go`
- Modify `scheduler/internal/crawlserviceclient/nats_client.go` only if NATS transport remains in use for coarse calls during transition

**Corpscout Temporal workflow**
- Create `scheduler/internal/brreg/temporal/domain_action_types.go`
- Create `scheduler/internal/brreg/temporal/domain_action_activities.go`
- Create `scheduler/internal/brreg/temporal/domain_decision.go`
- Create `scheduler/internal/brreg/temporal/domain_company_workflow.go`
- Modify `scheduler/internal/brreg/temporal/domain.go`
- Modify `scheduler/internal/brreg/temporal/domain_activity.go`
- Modify `scheduler/internal/brreg/temporal/domain_submission.go`
- Modify `scheduler/internal/brreg/temporal/activities.go`
- Create tests:
  - `scheduler/internal/brreg/temporal/domain_action_activities_test.go`
  - `scheduler/internal/brreg/temporal/domain_decision_test.go`
  - `scheduler/internal/brreg/temporal/domain_company_workflow_test.go`

**Crawl service**
- Modify `/Users/graovic/pulsarpoint/ppoint/data-pipelines/services/crawl-service/src/corpscout_crawl_service/models.py`
- Modify `/Users/graovic/pulsarpoint/ppoint/data-pipelines/services/crawl-service/src/corpscout_crawl_service/api.py`
- Modify `/Users/graovic/pulsarpoint/ppoint/data-pipelines/services/crawl-service/src/corpscout_crawl_service/service.py`
- Modify `/Users/graovic/pulsarpoint/ppoint/data-pipelines/services/crawl-service/src/corpscout_crawl_service/site_analyzer.py` only if request/response DTOs need analyzer-specific helper methods
- Create `/Users/graovic/pulsarpoint/ppoint/data-pipelines/services/crawl-service/tests/test_action_endpoints.py`

**UI/API configuration**
- Modify `scheduler/internal/brreg/service/actions.go`
- Modify `scheduler/internal/brreg/httpapi/domain.go`
- Modify `ui/app/components/app/BrregRawRecordActionSheet.tsx` only after backend supports new strategy fields

---

### Task 1: Add Domain Action Artifact Tables

**Files:**
- Create: `database/migrations/000066_brreg_domain_action_artifacts.up.sql`
- Create: `database/migrations/000066_brreg_domain_action_artifacts.down.sql`
- Create: `scheduler/internal/db/brreg_domain_action_artifacts_migration_test.go`

- [ ] **Step 1: Write the failing migration test**

Add `scheduler/internal/db/brreg_domain_action_artifacts_migration_test.go`:

```go
package db

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBrregDomainActionArtifactsMigration(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000066_brreg_domain_action_artifacts.up.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "CREATE TABLE brreg_workflow.domain_action_attempts")
	require.Contains(t, sql, "CREATE TABLE brreg_workflow.domain_action_artifacts")
	require.Contains(t, sql, "action_type IN (")
	require.Contains(t, sql, "'search_page_fetch'")
	require.Contains(t, sql, "'candidate_site_analysis'")
	require.Contains(t, sql, "status IN ('running', 'succeeded', 'failed', 'skipped')")
	require.Contains(t, sql, "artifact_type IN (")
	require.Contains(t, sql, "'site_analysis'")
	require.Contains(t, sql, "raw_record_id UUID NOT NULL REFERENCES brreg_workflow.raw_records(id) ON DELETE CASCADE")
	require.Contains(t, sql, "task_attempt_id UUID REFERENCES brreg_workflow.task_attempts(id) ON DELETE SET NULL")
	require.Contains(t, sql, "CREATE INDEX idx_brreg_domain_action_attempts_raw_action")
	require.Contains(t, sql, "CREATE INDEX idx_brreg_domain_action_artifacts_raw_type")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./internal/db -run TestBrregDomainActionArtifactsMigration
```

Expected: FAIL because migration `000066_brreg_domain_action_artifacts.up.sql` does not exist.

- [ ] **Step 3: Create the migration**

Create `database/migrations/000066_brreg_domain_action_artifacts.up.sql`:

```sql
CREATE TABLE brreg_workflow.domain_action_attempts (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  workflow_run_id UUID REFERENCES brreg_workflow.workflow_runs(id) ON DELETE SET NULL,
  task_attempt_id UUID REFERENCES brreg_workflow.task_attempts(id) ON DELETE SET NULL,
  raw_record_id UUID NOT NULL REFERENCES brreg_workflow.raw_records(id) ON DELETE CASCADE,
  action_type TEXT NOT NULL,
  provider TEXT,
  model TEXT,
  input_hash TEXT NOT NULL,
  attempt INTEGER NOT NULL DEFAULT 1,
  status TEXT NOT NULL DEFAULT 'running',
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at TIMESTAMPTZ,
  error TEXT,
  error_category TEXT,
  error_code TEXT,
  retry_strategy TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  CONSTRAINT chk_brreg_domain_action_type CHECK (
    action_type IN (
      'existing_website_check',
      'search_page_fetch',
      'search_result_analysis',
      'candidate_site_crawl',
      'candidate_site_analysis',
      'domain_decision'
    )
  ),
  CONSTRAINT chk_brreg_domain_action_status CHECK (
    status IN ('running', 'succeeded', 'failed', 'skipped')
  ),
  CONSTRAINT chk_brreg_domain_action_attempt CHECK (attempt > 0),
  CONSTRAINT chk_brreg_domain_action_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  UNIQUE (raw_record_id, action_type, provider, model, input_hash, attempt)
);

CREATE INDEX idx_brreg_domain_action_attempts_raw_action
  ON brreg_workflow.domain_action_attempts(raw_record_id, action_type, started_at DESC);

CREATE INDEX idx_brreg_domain_action_attempts_status
  ON brreg_workflow.domain_action_attempts(status, action_type, started_at DESC);

CREATE INDEX idx_brreg_domain_action_attempts_task_attempt
  ON brreg_workflow.domain_action_attempts(task_attempt_id)
  WHERE task_attempt_id IS NOT NULL;

CREATE TABLE brreg_workflow.domain_action_artifacts (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  attempt_id UUID NOT NULL REFERENCES brreg_workflow.domain_action_attempts(id) ON DELETE CASCADE,
  raw_record_id UUID NOT NULL REFERENCES brreg_workflow.raw_records(id) ON DELETE CASCADE,
  artifact_type TEXT NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  payload_hash TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  CONSTRAINT chk_brreg_domain_action_artifact_type CHECK (
    artifact_type IN (
      'search_page',
      'search_candidates',
      'crawl_page',
      'site_analysis',
      'domain_decision'
    )
  ),
  CONSTRAINT chk_brreg_domain_action_artifact_payload_object CHECK (jsonb_typeof(payload) = 'object'),
  CONSTRAINT chk_brreg_domain_action_artifact_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  UNIQUE (attempt_id, artifact_type, payload_hash)
);

CREATE INDEX idx_brreg_domain_action_artifacts_raw_type
  ON brreg_workflow.domain_action_artifacts(raw_record_id, artifact_type, created_at DESC);

GRANT SELECT ON brreg_workflow.domain_action_attempts TO corpscout_anon;
GRANT SELECT ON brreg_workflow.domain_action_artifacts TO corpscout_anon;
```

Create `database/migrations/000066_brreg_domain_action_artifacts.down.sql`:

```sql
DROP TABLE IF EXISTS brreg_workflow.domain_action_artifacts;
DROP TABLE IF EXISTS brreg_workflow.domain_action_attempts;
```

- [ ] **Step 4: Run tests and sqlc generation**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./internal/db -run TestBrregDomainActionArtifactsMigration
cd /Users/graovic/pulsarpoint/ppoint/corpscout
make sqlc-generate
```

Expected: test passes and sqlc generation succeeds.

- [ ] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
git add database/migrations/000066_brreg_domain_action_artifacts.* scheduler/internal/db/brreg_domain_action_artifacts_migration_test.go scheduler/internal/db/gen
git commit -m "Add BRREG domain action artifact tables"
```

---

### Task 2: Add DB Gateway Methods For Domain Actions

**Files:**
- Modify: `database/queries/brreg_workflow.sql`
- Modify: `scheduler/internal/brreg/db/types.go`
- Create: `scheduler/internal/brreg/db/domain_actions.go`
- Modify: `scheduler/internal/brreg/db/gateway_test.go`
- Regenerate: `scheduler/internal/db/gen/*`

- [ ] **Step 1: Write failing gateway tests**

Append to `scheduler/internal/brreg/db/gateway_test.go`:

```go
func TestGatewayRecordsDomainActionSuccess(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()
	gateway := NewGateway(pool)

	rawRecordID := uuid.New()
	taskAttemptID := uuid.New()
	actionAttemptID := uuid.New()
	payload := []byte(`{"url":"https://example.no","status":"succeeded"}`)

	pool.ExpectBegin()
	pool.ExpectQuery("INSERT INTO brreg_workflow.domain_action_attempts").
		WithArgs(pgxmock.AnyArg(), taskAttemptID, rawRecordID, "candidate_site_crawl", "crawl4ai", nil, pgxmock.AnyArg(), int32(1), pgxmock.AnyArg()).
		WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(actionAttemptID))
	pool.ExpectExec("INSERT INTO brreg_workflow.domain_action_artifacts").
		WithArgs(actionAttemptID, rawRecordID, "crawl_page", payload, pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	pool.ExpectExec("UPDATE brreg_workflow.domain_action_attempts").
		WithArgs(actionAttemptID, "succeeded", nil, nil, nil, nil, nil).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))
	pool.ExpectCommit()

	err = gateway.RecordDomainActionSuccess(context.Background(), RecordDomainActionSuccessCommand{
		WorkflowRunID:  uuid.New(),
		TaskAttemptID:  taskAttemptID,
		RawRecordID:    rawRecordID,
		ActionType:     DomainActionCandidateSiteCrawl,
		Provider:       "crawl4ai",
		Input:          json.RawMessage(`{"url":"https://example.no"}`),
		ArtifactType:   DomainArtifactCrawlPage,
		ArtifactPayload: payload,
		Metadata:       json.RawMessage(`{}`),
	})
	require.NoError(t, err)
	require.NoError(t, pool.ExpectationsWereMet())
}
```

Add a second test for failure:

```go
func TestGatewayRecordsDomainActionFailure(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()
	gateway := NewGateway(pool)

	rawRecordID := uuid.New()
	taskAttemptID := uuid.New()
	actionAttemptID := uuid.New()
	errorText := "site analysis failed"

	pool.ExpectBegin()
	pool.ExpectQuery("INSERT INTO brreg_workflow.domain_action_attempts").
		WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(actionAttemptID))
	pool.ExpectExec("UPDATE brreg_workflow.domain_action_attempts").
		WithArgs(actionAttemptID, "failed", &errorText, "transient_external", "site_analysis_failed", "retry_with_backoff", pgxmock.AnyArg()).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))
	pool.ExpectCommit()

	err = gateway.RecordDomainActionFailure(context.Background(), RecordDomainActionFailureCommand{
		WorkflowRunID:  uuid.New(),
		TaskAttemptID:  taskAttemptID,
		RawRecordID:    rawRecordID,
		ActionType:     DomainActionCandidateSiteAnalysis,
		Provider:       "deepseek",
		Model:          "deepseek-chat",
		Input:          json.RawMessage(`{"url":"https://example.no"}`),
		Error:          errorText,
		ErrorCategory:  "transient_external",
		ErrorCode:      "site_analysis_failed",
		RetryStrategy:  "retry_with_backoff",
		Metadata:       json.RawMessage(`{}`),
	})
	require.NoError(t, err)
	require.NoError(t, pool.ExpectationsWereMet())
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./internal/brreg/db -run 'TestGatewayRecordsDomainAction'
```

Expected: FAIL because the command types and methods do not exist.

- [ ] **Step 3: Add sqlc queries**

Append to `database/queries/brreg_workflow.sql`:

```sql
-- name: CreateBrregDomainActionAttempt :one
INSERT INTO brreg_workflow.domain_action_attempts (
  workflow_run_id,
  task_attempt_id,
  raw_record_id,
  action_type,
  provider,
  model,
  input_hash,
  attempt,
  metadata
) VALUES (
  sqlc.narg('workflow_run_id')::uuid,
  sqlc.narg('task_attempt_id')::uuid,
  sqlc.arg('raw_record_id')::uuid,
  sqlc.arg('action_type')::text,
  sqlc.narg('provider')::text,
  sqlc.narg('model')::text,
  sqlc.arg('input_hash')::text,
  sqlc.arg('attempt')::integer,
  COALESCE(sqlc.narg('metadata')::jsonb, '{}'::jsonb)
)
RETURNING id;

-- name: InsertBrregDomainActionArtifact :exec
INSERT INTO brreg_workflow.domain_action_artifacts (
  attempt_id,
  raw_record_id,
  artifact_type,
  payload,
  payload_hash,
  metadata
) VALUES (
  sqlc.arg('attempt_id')::uuid,
  sqlc.arg('raw_record_id')::uuid,
  sqlc.arg('artifact_type')::text,
  COALESCE(sqlc.arg('payload')::jsonb, '{}'::jsonb),
  sqlc.arg('payload_hash')::text,
  COALESCE(sqlc.narg('metadata')::jsonb, '{}'::jsonb)
);

-- name: FinishBrregDomainActionAttempt :exec
UPDATE brreg_workflow.domain_action_attempts
SET
  status = sqlc.arg('status')::text,
  finished_at = now(),
  error = sqlc.narg('error')::text,
  error_category = sqlc.narg('error_category')::text,
  error_code = sqlc.narg('error_code')::text,
  retry_strategy = sqlc.narg('retry_strategy')::text,
  metadata = COALESCE(sqlc.narg('metadata')::jsonb, metadata)
WHERE id = sqlc.arg('id')::uuid;
```

- [ ] **Step 4: Add public gateway types**

Add to `scheduler/internal/brreg/db/types.go`:

```go
type DomainActionType string

const (
	DomainActionExistingWebsiteCheck  DomainActionType = "existing_website_check"
	DomainActionSearchPageFetch       DomainActionType = "search_page_fetch"
	DomainActionSearchResultAnalysis  DomainActionType = "search_result_analysis"
	DomainActionCandidateSiteCrawl    DomainActionType = "candidate_site_crawl"
	DomainActionCandidateSiteAnalysis DomainActionType = "candidate_site_analysis"
	DomainActionDecision              DomainActionType = "domain_decision"
)

type DomainArtifactType string

const (
	DomainArtifactSearchPage       DomainArtifactType = "search_page"
	DomainArtifactSearchCandidates DomainArtifactType = "search_candidates"
	DomainArtifactCrawlPage        DomainArtifactType = "crawl_page"
	DomainArtifactSiteAnalysis     DomainArtifactType = "site_analysis"
	DomainArtifactDecision         DomainArtifactType = "domain_decision"
)

type RecordDomainActionSuccessCommand struct {
	WorkflowRunID   uuid.UUID
	TaskAttemptID   uuid.UUID
	RawRecordID     uuid.UUID
	ActionType      DomainActionType
	Provider        string
	Model           string
	Input           json.RawMessage
	Attempt         int32
	ArtifactType    DomainArtifactType
	ArtifactPayload json.RawMessage
	Metadata        json.RawMessage
}

type RecordDomainActionFailureCommand struct {
	WorkflowRunID  uuid.UUID
	TaskAttemptID  uuid.UUID
	RawRecordID    uuid.UUID
	ActionType     DomainActionType
	Provider       string
	Model          string
	Input          json.RawMessage
	Attempt        int32
	Error          string
	ErrorCategory  string
	ErrorCode      string
	RetryStrategy  string
	Metadata       json.RawMessage
}
```

- [ ] **Step 5: Implement `domain_actions.go`**

Create `scheduler/internal/brreg/db/domain_actions.go`:

```go
package brregdb

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5/pgtype"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

func (g *Gateway) RecordDomainActionSuccess(ctx context.Context, command RecordDomainActionSuccessCommand) error {
	return g.withTx(ctx, func(q *db.Queries) error {
		attemptID, err := q.CreateBrregDomainActionAttempt(ctx, db.CreateBrregDomainActionAttemptParams{
			WorkflowRunID: pgUUID(command.WorkflowRunID),
			TaskAttemptID: pgUUID(command.TaskAttemptID),
			RawRecordID:   command.RawRecordID,
			ActionType:    string(command.ActionType),
			Provider:      textOrNil(command.Provider),
			Model:         textOrNil(command.Model),
			InputHash:     hashJSON(command.Input),
			Attempt:       normalizedAttempt(command.Attempt),
			Metadata:      jsonOrEmpty(command.Metadata),
		})
		if err != nil {
			return errors.Wrap(err, "create brreg domain action attempt")
		}
		if err := q.InsertBrregDomainActionArtifact(ctx, db.InsertBrregDomainActionArtifactParams{
			AttemptID:    attemptID,
			RawRecordID:  command.RawRecordID,
			ArtifactType: string(command.ArtifactType),
			Payload:      jsonOrEmpty(command.ArtifactPayload),
			PayloadHash:  hashJSON(command.ArtifactPayload),
			Metadata:     jsonOrEmpty(command.Metadata),
		}); err != nil {
			return errors.Wrap(err, "insert brreg domain action artifact")
		}
		return q.FinishBrregDomainActionAttempt(ctx, db.FinishBrregDomainActionAttemptParams{
			ID:       attemptID,
			Status:   "succeeded",
			Metadata: jsonOrEmpty(command.Metadata),
		})
	})
}

func (g *Gateway) RecordDomainActionFailure(ctx context.Context, command RecordDomainActionFailureCommand) error {
	return g.withTx(ctx, func(q *db.Queries) error {
		attemptID, err := q.CreateBrregDomainActionAttempt(ctx, db.CreateBrregDomainActionAttemptParams{
			WorkflowRunID: pgUUID(command.WorkflowRunID),
			TaskAttemptID: pgUUID(command.TaskAttemptID),
			RawRecordID:   command.RawRecordID,
			ActionType:    string(command.ActionType),
			Provider:      textOrNil(command.Provider),
			Model:         textOrNil(command.Model),
			InputHash:     hashJSON(command.Input),
			Attempt:       normalizedAttempt(command.Attempt),
			Metadata:      jsonOrEmpty(command.Metadata),
		})
		if err != nil {
			return errors.Wrap(err, "create failed brreg domain action attempt")
		}
		return q.FinishBrregDomainActionAttempt(ctx, db.FinishBrregDomainActionAttemptParams{
			ID:            attemptID,
			Status:        "failed",
			Error:         &command.Error,
			ErrorCategory: &command.ErrorCategory,
			ErrorCode:     &command.ErrorCode,
			RetryStrategy: &command.RetryStrategy,
			Metadata:      jsonOrEmpty(command.Metadata),
		})
	})
}

func normalizedAttempt(value int32) int32 {
	if value <= 0 {
		return 1
	}
	return value
}

func hashJSON(payload json.RawMessage) string {
	if len(payload) == 0 {
		payload = []byte(`{}`)
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func jsonOrEmpty(payload json.RawMessage) []byte {
	if len(payload) == 0 {
		return []byte(`{}`)
	}
	return payload
}

func textOrNil(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func pgUUID(value uuid.UUID) pgtype.UUID {
	return pgtype.UUID{Bytes: value, Valid: value != uuid.Nil}
}
```

Add imports for `encoding/json` and `github.com/google/uuid` in `types.go` and `domain_actions.go` as needed.

- [ ] **Step 6: Generate sqlc and run tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
make sqlc-generate
cd scheduler
GOWORK=off go test ./internal/brreg/db -run 'TestGatewayRecordsDomainAction'
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
git add database/queries/brreg_workflow.sql scheduler/internal/brreg/db scheduler/internal/db/gen
git commit -m "Add BRREG domain action gateway"
```

---

### Task 3: Split Crawl Service Into Action Endpoints

**Files:**
- Modify: `/Users/graovic/pulsarpoint/ppoint/data-pipelines/services/crawl-service/src/corpscout_crawl_service/models.py`
- Modify: `/Users/graovic/pulsarpoint/ppoint/data-pipelines/services/crawl-service/src/corpscout_crawl_service/service.py`
- Modify: `/Users/graovic/pulsarpoint/ppoint/data-pipelines/services/crawl-service/src/corpscout_crawl_service/api.py`
- Create: `/Users/graovic/pulsarpoint/ppoint/data-pipelines/services/crawl-service/tests/test_action_endpoints.py`

- [ ] **Step 1: Write failing API tests**

Create `tests/test_action_endpoints.py`:

```python
from __future__ import annotations

from fastapi.testclient import TestClient

from corpscout_crawl_service.api import create_app
from corpscout_crawl_service.crawl4ai_service import Crawl4AiResponse
from corpscout_crawl_service.service import CrawlService

from tests.fakes import FakeCrawl4AiService


def test_search_fetch_endpoint_returns_search_page_artifact() -> None:
    fake = FakeCrawl4AiService({
        "https://html.duckduckgo.com/html/?q=BORTIGARD%20AS%20NO%20website": Crawl4AiResponse(
            url="https://html.duckduckgo.com/html/?q=BORTIGARD%20AS%20NO%20website",
            final_url="https://html.duckduckgo.com/html/?q=BORTIGARD%20AS%20NO%20website",
            status="succeeded",
            markdown="# Search results",
            markdown_hash="search-hash",
            links=["https://bortigard.no/"],
            duration_ms=10,
        )
    })
    client = TestClient(create_app(crawl_service=CrawlService(crawl4ai_service=fake)))

    response = client.post("/v1/search/fetch", json={
        "search_term": "BORTIGARD AS NO website",
        "search_engine": "duckduckgo",
        "timeout_seconds": 60,
    })

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "succeeded"
    assert body["markdown_hash"] == "search-hash"
    assert body["links"] == ["https://bortigard.no/"]


def test_page_analyze_endpoint_returns_site_analysis_artifact() -> None:
    fake = FakeCrawl4AiService({})
    service = CrawlService(crawl4ai_service=fake, site_analyzer=StaticSiteAnalyzer())
    client = TestClient(create_app(crawl_service=service))

    response = client.post("/v1/pages/analyze-company", json={
        "company_name": "BORTIGARD AS",
        "country": "NO",
        "url": "https://bortigard.no/",
        "final_url": "https://bortigard.no/",
        "normalized_domain": "bortigard.no",
        "markdown": "# BORTIGARD AS",
        "candidate_score": 88,
        "candidate_reason": "Search result matches.",
    })

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "succeeded"
    assert body["analysis"]["decision"] == "accepted"
    assert body["analysis"]["site_type"] == "company_website"
    assert body["analysis"]["owned_domain"] is True


class StaticSiteAnalyzer:
    async def analyze_site(self, payload):
        return {
            "decision": "accepted",
            "score": 91,
            "site_type": "company_website",
            "relationship": "primary_web_presence",
            "owned_domain": True,
            "reason": "The page names BORTIGARD AS.",
            "evidence": ["BORTIGARD AS"],
        }
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/services/crawl-service
uv run pytest tests/test_action_endpoints.py -q
```

Expected: FAIL with 404 for the new endpoints.

- [ ] **Step 3: Add request/response models**

Add to `models.py`:

```python
ActionStatus = Literal["succeeded", "failed"]


class SearchFetchRequest(BaseModel):
    search_term: str = Field(min_length=1)
    search_engine: str = Field(default="duckduckgo", min_length=1)
    timeout_seconds: int = Field(default=60, ge=1, le=300)


class SearchAnalyzeRequest(BaseModel):
    company_name: str = Field(min_length=1)
    organization_number: str | None = None
    country: str = Field(default="NO", min_length=2)
    address_lines: list[str] = Field(default_factory=list)
    city: str | None = None
    postal_code: str | None = None
    business_activity: list[str] = Field(default_factory=list)
    statutory_purpose: list[str] = Field(default_factory=list)
    industry_codes: list[str] = Field(default_factory=list)
    search_engine: str = Field(default="duckduckgo", min_length=1)
    search_term: str = Field(min_length=1)
    links: list[str] = Field(default_factory=list)
    markdown: str = ""
    candidate_threshold: int = Field(default=50, ge=0, le=100)
    timeout_seconds: int = Field(default=60, ge=1, le=300)


class PageCrawlRequest(BaseModel):
    url: str = Field(min_length=1)
    timeout_seconds: int = Field(default=60, ge=1, le=300)
    metadata: dict[str, Any] = Field(default_factory=dict)


class PageAnalyzeRequest(BaseModel):
    company_name: str = Field(min_length=1)
    organization_number: str | None = None
    country: str = Field(default="NO", min_length=2)
    address_lines: list[str] = Field(default_factory=list)
    city: str | None = None
    postal_code: str | None = None
    business_activity: list[str] = Field(default_factory=list)
    statutory_purpose: list[str] = Field(default_factory=list)
    industry_codes: list[str] = Field(default_factory=list)
    url: str = Field(min_length=1)
    final_url: str = Field(min_length=1)
    normalized_domain: str = Field(min_length=1)
    markdown: str = ""
    candidate_score: int = Field(default=0, ge=0, le=100)
    candidate_reason: str = ""
    timeout_seconds: int = Field(default=60, ge=1, le=300)


class SearchAnalyzeResponse(BaseModel):
    status: ActionStatus
    candidates: list[ScoredLink] = Field(default_factory=list)
    error: ServiceError | None = None


class PageAnalyzeResponse(BaseModel):
    status: ActionStatus
    analysis: dict[str, Any] = Field(default_factory=dict)
    error: ServiceError | None = None
```

- [ ] **Step 4: Add service methods**

Add to `service.py`:

```python
async def fetch_search_page(self, request: SearchFetchRequest | dict) -> Crawl4AiResponse:
    request = SearchFetchRequest.model_validate(request)
    search_url = search_url_for_engine(request.search_term, search_engine=request.search_engine)
    return await self._crawl4ai.crawl(
        Crawl4AiRequest(
            url=search_url,
            llm_enabled=False,
            timeout_seconds=request.timeout_seconds,
            purpose="domain_search",
            metadata={"search_engine": request.search_engine, "search_term": request.search_term},
        )
    )

async def crawl_page(self, request: PageCrawlRequest | dict) -> Crawl4AiResponse:
    request = PageCrawlRequest.model_validate(request)
    return await self._crawl4ai.crawl(
        Crawl4AiRequest(
            url=request.url,
            llm_enabled=False,
            timeout_seconds=request.timeout_seconds,
            purpose="domain_site_check",
            metadata=request.metadata,
        )
    )

async def analyze_search_page(self, request: SearchAnalyzeRequest | dict) -> SearchAnalyzeResponse:
    request = SearchAnalyzeRequest.model_validate(request)
    try:
        output = await self._search_analyzer.analyze_search({
            "company_name": request.company_name,
            "organization_number": request.organization_number,
            "country": request.country,
            "address_lines": request.address_lines,
            "city": request.city,
            "postal_code": request.postal_code,
            "business_activity": request.business_activity,
            "statutory_purpose": request.statutory_purpose,
            "industry_codes": request.industry_codes,
            "search_engine": request.search_engine,
            "search_term": request.search_term,
            "candidate_threshold": request.candidate_threshold,
            "links": request.links[:30],
            "compact_markdown": _compact_markdown(request.markdown),
            "timeout_seconds": request.timeout_seconds,
        })
        candidates = _scored_links_from_llm_output(output, source=f"{request.search_engine}_search_llm", limit=20)
        return SearchAnalyzeResponse(status="succeeded", candidates=candidates)
    except Exception as exc:
        return SearchAnalyzeResponse(status="failed", error=_service_error_from_exception(
            code="search_analysis_failed",
            message="Search result analysis failed.",
            exc=exc,
        ))

async def analyze_company_page(self, request: PageAnalyzeRequest | dict) -> PageAnalyzeResponse:
    request = PageAnalyzeRequest.model_validate(request)
    try:
        output = await self._site_analyzer.analyze_site({
            "company_name": request.company_name,
            "organization_number": request.organization_number,
            "country": request.country,
            "address_lines": request.address_lines,
            "city": request.city,
            "postal_code": request.postal_code,
            "business_activity": request.business_activity,
            "statutory_purpose": request.statutory_purpose,
            "industry_codes": request.industry_codes,
            "url": request.url,
            "final_url": request.final_url,
            "normalized_domain": request.normalized_domain,
            "candidate_score": request.candidate_score,
            "candidate_reason": request.candidate_reason,
            "compact_markdown": _compact_markdown(request.markdown),
            "timeout_seconds": request.timeout_seconds,
        })
        return PageAnalyzeResponse(status="succeeded", analysis=_apply_context_scoring_rules(output))
    except Exception as exc:
        return PageAnalyzeResponse(status="failed", error=_service_error_from_exception(
            code="site_analysis_failed",
            message="Site analysis failed.",
            exc=exc,
        ))
```

Place these methods inside `class CrawlService`. Import the new models.

- [ ] **Step 5: Add API routes**

Add to `api.py`:

```python
from corpscout_crawl_service.models import (
    PageAnalyzeRequest,
    PageCrawlRequest,
    SearchAnalyzeRequest,
    SearchFetchRequest,
)

@app.post("/v1/search/fetch")
async def fetch_search_page(request: SearchFetchRequest):
    return await service.fetch_search_page(request)

@app.post("/v1/search/analyze")
async def analyze_search_page(request: SearchAnalyzeRequest):
    return await service.analyze_search_page(request)

@app.post("/v1/pages/crawl")
async def crawl_page(request: PageCrawlRequest):
    return await service.crawl_page(request)

@app.post("/v1/pages/analyze-company")
async def analyze_company_page(request: PageAnalyzeRequest):
    return await service.analyze_company_page(request)
```

- [ ] **Step 6: Run crawl-service tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/services/crawl-service
uv run pytest -q
```

Expected: all tests pass.

- [ ] **Step 7: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines
git add services/crawl-service/src/corpscout_crawl_service services/crawl-service/tests/test_action_endpoints.py
git commit -m "Add crawl service domain action endpoints"
```

---

### Task 4: Add Go Client Methods For Domain Actions

**Files:**
- Modify: `scheduler/internal/crawlserviceclient/client.go`
- Modify: `scheduler/internal/crawlserviceclient/client_test.go`

- [ ] **Step 1: Write failing client tests**

Add tests:

```go
func TestClientFetchSearchPage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/v1/search/fetch", r.URL.Path)
		require.Equal(t, http.MethodPost, r.Method)
		writeTestJSON(t, w, `{"url":"https://html.duckduckgo.com/html/?q=x","final_url":"https://html.duckduckgo.com/html/?q=x","status":"succeeded","markdown":"# Search","markdown_hash":"hash","links":["https://example.no"],"duration_ms":10,"metadata":{}}`)
	}))
	defer server.Close()

	client := New(server.URL)
	response, err := client.FetchSearchPage(context.Background(), SearchFetchRequest{
		SearchTerm: "BORTIGARD AS NO website",
		SearchEngine: "duckduckgo",
		TimeoutSeconds: 60,
	})
	require.NoError(t, err)
	require.Equal(t, "succeeded", response.Status)
	require.Equal(t, "hash", response.MarkdownHash)
}

func TestClientAnalyzeCompanyPage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/v1/pages/analyze-company", r.URL.Path)
		writeTestJSON(t, w, `{"status":"succeeded","analysis":{"decision":"accepted","score":91,"site_type":"company_website","relationship":"primary_web_presence","owned_domain":true}}`)
	}))
	defer server.Close()

	client := New(server.URL)
	response, err := client.AnalyzeCompanyPage(context.Background(), PageAnalyzeRequest{
		CompanyName: "BORTIGARD AS",
		URL: "https://bortigard.no/",
		FinalURL: "https://bortigard.no/",
		NormalizedDomain: "bortigard.no",
		Markdown: "# BORTIGARD AS",
	})
	require.NoError(t, err)
	require.Equal(t, "succeeded", response.Status)
	require.Equal(t, float64(91), response.Analysis["score"])
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./internal/crawlserviceclient -run 'TestClientFetchSearchPage|TestClientAnalyzeCompanyPage'
```

Expected: FAIL because methods/types do not exist.

- [ ] **Step 3: Add request/response types and methods**

In `client.go`, add:

```go
type SearchFetchRequest struct {
	SearchTerm     string `json:"search_term"`
	SearchEngine   string `json:"search_engine,omitempty"`
	TimeoutSeconds int    `json:"timeout_seconds,omitempty"`
}

type CrawlPageRequest struct {
	URL            string         `json:"url"`
	TimeoutSeconds int           `json:"timeout_seconds,omitempty"`
	Metadata       map[string]any `json:"metadata,omitempty"`
}

type SearchAnalyzeRequest struct {
	CompanyName        string   `json:"company_name"`
	OrganizationNumber string   `json:"organization_number,omitempty"`
	Country            string   `json:"country,omitempty"`
	AddressLines       []string `json:"address_lines,omitempty"`
	City               string   `json:"city,omitempty"`
	PostalCode         string   `json:"postal_code,omitempty"`
	BusinessActivity   []string `json:"business_activity,omitempty"`
	StatutoryPurpose   []string `json:"statutory_purpose,omitempty"`
	IndustryCodes      []string `json:"industry_codes,omitempty"`
	SearchEngine       string   `json:"search_engine,omitempty"`
	SearchTerm         string   `json:"search_term"`
	Links              []string `json:"links,omitempty"`
	Markdown           string   `json:"markdown,omitempty"`
	CandidateThreshold int      `json:"candidate_threshold,omitempty"`
	TimeoutSeconds     int      `json:"timeout_seconds,omitempty"`
}

type PageAnalyzeRequest struct {
	CompanyName        string   `json:"company_name"`
	OrganizationNumber string   `json:"organization_number,omitempty"`
	Country            string   `json:"country,omitempty"`
	AddressLines       []string `json:"address_lines,omitempty"`
	City               string   `json:"city,omitempty"`
	PostalCode         string   `json:"postal_code,omitempty"`
	BusinessActivity   []string `json:"business_activity,omitempty"`
	StatutoryPurpose   []string `json:"statutory_purpose,omitempty"`
	IndustryCodes      []string `json:"industry_codes,omitempty"`
	URL                string   `json:"url"`
	FinalURL           string   `json:"final_url"`
	NormalizedDomain   string   `json:"normalized_domain"`
	Markdown           string   `json:"markdown,omitempty"`
	CandidateScore     int      `json:"candidate_score,omitempty"`
	CandidateReason    string   `json:"candidate_reason,omitempty"`
	TimeoutSeconds     int      `json:"timeout_seconds,omitempty"`
}

type SearchAnalyzeResponse struct {
	Status     string          `json:"status"`
	Candidates json.RawMessage `json:"candidates"`
	Error      *ServiceError   `json:"error,omitempty"`
	RawPayload json.RawMessage `json:"-"`
}

type PageAnalyzeResponse struct {
	Status     string         `json:"status"`
	Analysis   map[string]any `json:"analysis"`
	Error      *ServiceError  `json:"error,omitempty"`
	RawPayload json.RawMessage `json:"-"`
}

func (c *Client) FetchSearchPage(ctx context.Context, request SearchFetchRequest) (Crawl4AiResponse, error) {
	var response Crawl4AiResponse
	err := c.postJSON(ctx, "/v1/search/fetch", request, &response)
	return response, err
}

func (c *Client) CrawlPage(ctx context.Context, request CrawlPageRequest) (Crawl4AiResponse, error) {
	var response Crawl4AiResponse
	err := c.postJSON(ctx, "/v1/pages/crawl", request, &response)
	return response, err
}

func (c *Client) AnalyzeSearchPage(ctx context.Context, request SearchAnalyzeRequest) (SearchAnalyzeResponse, error) {
	var response SearchAnalyzeResponse
	err := c.postJSON(ctx, "/v1/search/analyze", request, &response)
	return response, err
}

func (c *Client) AnalyzeCompanyPage(ctx context.Context, request PageAnalyzeRequest) (PageAnalyzeResponse, error) {
	var response PageAnalyzeResponse
	err := c.postJSON(ctx, "/v1/pages/analyze-company", request, &response)
	return response, err
}
```

Use existing client helper naming; if `postJSON` is private under a different name, match the file’s current style exactly.

- [ ] **Step 4: Run tests**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./internal/crawlserviceclient
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
git add scheduler/internal/crawlserviceclient
git commit -m "Add crawl service action client methods"
```

---

### Task 5: Add Deterministic Domain Decision Logic

**Files:**
- Create: `scheduler/internal/brreg/temporal/domain_action_types.go`
- Create: `scheduler/internal/brreg/temporal/domain_decision.go`
- Create: `scheduler/internal/brreg/temporal/domain_decision_test.go`

- [ ] **Step 1: Write failing decision tests**

Create `domain_decision_test.go`:

```go
package brregtemporal

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDomainDecisionRequiresOwnedCompanyWebsiteForSuccess(t *testing.T) {
	checks := []DomainSiteAnalysisArtifact{
		{
			URL: "https://example.no",
			Domain: "example.no",
			NormalizedDomain: "example.no",
			Analysis: map[string]any{
				"decision": "accepted",
				"score": float64(91),
				"site_type": "company_website",
				"relationship": "primary_web_presence",
				"owned_domain": true,
				"reason": "The page names the company.",
			},
		},
	}

	result := decideDomainResult(checks, nil)

	require.Equal(t, "succeeded", result.Status)
	require.Equal(t, "example.no", *result.BestDomain)
}

func TestDomainDecisionTreatsRelatedOnlyAsNotFound(t *testing.T) {
	checks := []DomainSiteAnalysisArtifact{
		{
			URL: "https://facebook.com/company",
			Domain: "facebook.com",
			NormalizedDomain: "facebook.com",
			Analysis: map[string]any{
				"decision": "accepted",
				"score": float64(88),
				"site_type": "social_profile",
				"relationship": "primary_web_presence",
				"owned_domain": false,
			},
		},
	}

	result := decideDomainResult(checks, nil)

	require.Equal(t, "not_found", result.Status)
	require.Nil(t, result.BestDomain)
	require.Len(t, result.RelatedSites, 1)
}

func TestDomainDecisionFailsWhenAllCandidatesHaveRetryableAnalysisErrors(t *testing.T) {
	result := decideDomainResult(nil, []DomainActionFailure{
		{Code: "site_analysis_failed", Category: "transient_external", RetryStrategy: "retry_with_backoff"},
	})

	require.Equal(t, "failed", result.Status)
	require.Equal(t, "site_analysis_failed", result.Errors[0].Code)
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./internal/brreg/temporal -run TestDomainDecision
```

Expected: FAIL because types/functions do not exist.

- [ ] **Step 3: Add types**

Create `domain_action_types.go`:

```go
package brregtemporal

type DomainSiteAnalysisArtifact struct {
	URL              string         `json:"url"`
	Domain           string         `json:"domain"`
	NormalizedDomain string         `json:"normalized_domain"`
	Analysis         map[string]any `json:"analysis"`
}

type DomainActionFailure struct {
	Code          string `json:"code"`
	Category      string `json:"category"`
	RetryStrategy string `json:"retry_strategy"`
	Message       string `json:"message,omitempty"`
}

type DomainDecisionResult struct {
	Status       string                       `json:"status"`
	BestDomain   *string                      `json:"best_domain,omitempty"`
	Domains      []DomainSiteAnalysisArtifact `json:"domains"`
	RelatedSites []DomainSiteAnalysisArtifact `json:"related_sites"`
	Errors       []DomainActionFailure        `json:"errors"`
}
```

- [ ] **Step 4: Add decision function**

Create `domain_decision.go`:

```go
package brregtemporal

import "sort"

func decideDomainResult(checks []DomainSiteAnalysisArtifact, failures []DomainActionFailure) DomainDecisionResult {
	owned := make([]DomainSiteAnalysisArtifact, 0)
	related := make([]DomainSiteAnalysisArtifact, 0)
	for _, check := range checks {
		score := analysisScore(check.Analysis)
		if score < 70 || analysisString(check.Analysis, "decision") == "rejected" {
			continue
		}
		if analysisString(check.Analysis, "site_type") == "company_website" && analysisBool(check.Analysis, "owned_domain") {
			owned = append(owned, check)
			continue
		}
		if analysisString(check.Analysis, "relationship") != "unrelated" {
			related = append(related, check)
		}
	}
	sort.SliceStable(owned, func(i, j int) bool {
		return analysisScore(owned[i].Analysis) > analysisScore(owned[j].Analysis)
	})
	sort.SliceStable(related, func(i, j int) bool {
		return analysisScore(related[i].Analysis) > analysisScore(related[j].Analysis)
	})
	if len(owned) > 0 {
		best := owned[0].NormalizedDomain
		return DomainDecisionResult{Status: "succeeded", BestDomain: &best, Domains: owned, RelatedSites: related}
	}
	if len(related) > 0 {
		return DomainDecisionResult{Status: "not_found", RelatedSites: related}
	}
	if len(failures) > 0 {
		return DomainDecisionResult{Status: "failed", Errors: failures}
	}
	return DomainDecisionResult{Status: "not_found"}
}

func analysisString(values map[string]any, key string) string {
	value, _ := values[key].(string)
	return value
}

func analysisBool(values map[string]any, key string) bool {
	value, _ := values[key].(bool)
	return value
}

func analysisScore(values map[string]any) int {
	switch value := values["score"].(type) {
	case int:
		return value
	case int32:
		return int(value)
	case int64:
		return int(value)
	case float64:
		return int(value)
	default:
		return 0
	}
}
```

- [ ] **Step 5: Run tests**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./internal/brreg/temporal -run TestDomainDecision
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
git add scheduler/internal/brreg/temporal/domain_action_types.go scheduler/internal/brreg/temporal/domain_decision.go scheduler/internal/brreg/temporal/domain_decision_test.go
git commit -m "Add BRREG domain decision logic"
```

---

### Task 6: Add Action Activities Around Crawl-Service Calls

**Files:**
- Create: `scheduler/internal/brreg/temporal/domain_action_activities.go`
- Create: `scheduler/internal/brreg/temporal/domain_action_activities_test.go`
- Modify: `scheduler/internal/brreg/temporal/domain_activity.go`
- Modify: `scheduler/internal/brreg/temporal/activities.go`

- [ ] **Step 1: Write failing activity tests**

Create `domain_action_activities_test.go` with one test per activity:

```go
func TestFetchSearchPageActivityRecordsArtifact(t *testing.T) {
	activity := Activities{
		gateway: fakeDomainActionGateway{},
		domainClient: fakeActionDomainClient{
			searchPage: crawlserviceclient.Crawl4AiResponse{
				Status: "succeeded",
				Markdown: "# Search",
				MarkdownHash: "search-hash",
				Links: []string{"https://example.no"},
			},
		},
	}

	result, err := activity.FetchBrregDomainSearchPage(context.Background(), BrregDomainSearchPageInput{
		WorkflowRunID: uuid.New().String(),
		TaskAttemptID: uuid.New().String(),
		RawRecordID: uuid.New().String(),
		CompanyName: "BORTIGARD AS",
		SearchEngine: "duckduckgo",
		SearchTerm: "BORTIGARD AS NO website",
	})

	require.NoError(t, err)
	require.Equal(t, "succeeded", result.Status)
	require.Equal(t, "search-hash", result.MarkdownHash)
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./internal/brreg/temporal -run 'Test.*Domain.*Activity'
```

Expected: FAIL because action activities do not exist.

- [ ] **Step 3: Define activity input/output structs**

Create `domain_action_activities.go` with:

```go
type BrregDomainSearchPageInput struct {
	WorkflowRunID string `json:"workflow_run_id"`
	TaskAttemptID string `json:"task_attempt_id"`
	RawRecordID   string `json:"raw_record_id"`
	CompanyName   string `json:"company_name"`
	SearchEngine  string `json:"search_engine"`
	SearchTerm    string `json:"search_term"`
	TimeoutSeconds int   `json:"timeout_seconds"`
}

type BrregDomainSearchPageResult struct {
	Status       string   `json:"status"`
	Markdown     string   `json:"markdown"`
	MarkdownHash string   `json:"markdown_hash"`
	Links        []string `json:"links"`
	ErrorCode    string   `json:"error_code,omitempty"`
}
```

Add similar inputs/results for:
- `BrregDomainSearchAnalysisInput`
- `BrregDomainPageCrawlInput`
- `BrregDomainPageAnalysisInput`
- `BrregDomainDecisionInput`

- [ ] **Step 4: Implement activities**

Implement these methods:

```go
func (a *Activities) FetchBrregDomainSearchPage(ctx context.Context, input BrregDomainSearchPageInput) (BrregDomainSearchPageResult, error)
func (a *Activities) AnalyzeBrregDomainSearchPage(ctx context.Context, input BrregDomainSearchAnalysisInput) (BrregDomainSearchAnalysisResult, error)
func (a *Activities) CrawlBrregDomainCandidatePage(ctx context.Context, input BrregDomainPageCrawlInput) (BrregDomainPageCrawlResult, error)
func (a *Activities) AnalyzeBrregDomainCandidatePage(ctx context.Context, input BrregDomainPageAnalysisInput) (BrregDomainPageAnalysisResult, error)
func (a *Activities) DecideBrregDomainResult(ctx context.Context, input BrregDomainDecisionInput) (DomainDecisionResult, error)
```

Each activity must:
1. call the crawl-service client
2. write a domain action success/failure through the gateway
3. return a small typed result to Temporal

If the crawl-service call fails with transport error, record action failure:

```go
brregdb.RecordDomainActionFailureCommand{
	ActionType: brregdb.DomainActionSearchPageFetch,
	ErrorCategory: "external_service",
	ErrorCode: "crawl_service_request_failed",
	RetryStrategy: "retry_with_backoff",
}
```

If the crawl-service response has `status=failed`, record failure using the response error code/category/retry strategy.

- [ ] **Step 5: Register activities**

Modify `activities.go` so the worker registers the new methods through the existing `Activities` struct. If the worker registers the whole struct, no extra registration is needed; add a test assertion in `activities_test.go` that the method names are present.

- [ ] **Step 6: Run tests**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./internal/brreg/temporal -run 'Domain.*Activity|Activities'
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
git add scheduler/internal/brreg/temporal
git commit -m "Add BRREG domain action activities"
```

---

### Task 7: Add Single-Company Domain Discovery Child Workflow

**Files:**
- Create: `scheduler/internal/brreg/temporal/domain_company_workflow.go`
- Create: `scheduler/internal/brreg/temporal/domain_company_workflow_test.go`
- Modify: `scheduler/internal/brreg/temporal/domain.go`

- [ ] **Step 1: Write failing child workflow tests**

Create `domain_company_workflow_test.go`:

```go
func TestDiscoverBrregDomainForCompanyUsesExistingWebsiteBeforeSearch(t *testing.T) {
	env := testsuite.WorkflowTestSuite{}.NewTestWorkflowEnvironment()
	input := BrregDomainCompanyWorkflowInput{
		WorkflowRunID: uuid.New().String(),
		TaskAttemptID: uuid.New().String(),
		RawRecordID: uuid.New().String(),
		OrganizationName: "A-LOGOPEDENE AS",
		ExistingWebsite: "https://a-logopedene.no",
		SearchProviders: []string{"duckduckgo", "yandex"},
		LLMModels: []string{"deepseek", "local"},
		MaxCandidateSites: 3,
	}

	env.OnActivity(FetchBrregDomainExistingWebsitePageActivityName, mock.Anything, mock.Anything).Return(BrregDomainPageCrawlResult{
		Status: "succeeded",
		URL: "https://a-logopedene.no",
		FinalURL: "https://a-logopedene.no",
		NormalizedDomain: "a-logopedene.no",
		Markdown: "# A-logopedene",
		MarkdownHash: "hash",
	}, nil).Once()
	env.OnActivity(AnalyzeBrregDomainCandidatePageActivityName, mock.Anything, mock.Anything).Return(BrregDomainPageAnalysisResult{
		Status: "succeeded",
		Site: DomainSiteAnalysisArtifact{
			URL: "https://a-logopedene.no",
			Domain: "a-logopedene.no",
			NormalizedDomain: "a-logopedene.no",
			Analysis: map[string]any{"decision":"accepted","score":float64(91),"site_type":"company_website","relationship":"primary_web_presence","owned_domain":true},
		},
	}, nil).Once()
	env.OnActivity(PersistBrregDomainDecisionActivityName, mock.Anything, mock.Anything).Return(nil).Once()

	env.ExecuteWorkflow(DiscoverBrregDomainForCompany, input)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
}
```

Add a second test:

```go
func TestDiscoverBrregDomainForCompanyFallsBackFromDuckDuckGoToYandex(t *testing.T) {
	// DuckDuckGo fetch returns failed/search_blocked.
	// Yandex fetch returns succeeded.
	// Search analysis returns one candidate.
	// Candidate crawl + analysis returns owned domain.
	// Persist is called once with status=succeeded and best_domain set.
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./internal/brreg/temporal -run TestDiscoverBrregDomainForCompany
```

Expected: FAIL because workflow does not exist.

- [ ] **Step 3: Implement child workflow input**

Create `domain_company_workflow.go`:

```go
type BrregDomainCompanyWorkflowInput struct {
	WorkflowRunID       string   `json:"workflow_run_id"`
	TaskAttemptID       string   `json:"task_attempt_id"`
	RawRecordID         string   `json:"raw_record_id"`
	OrganizationNumber  string   `json:"organization_number"`
	OrganizationName    string   `json:"organization_name"`
	ExistingWebsite     string   `json:"existing_website,omitempty"`
	RawPayload          []byte   `json:"raw_payload"`
	SearchProviders     []string `json:"search_providers"`
	LLMModels           []string `json:"llm_models"`
	MaxCandidateSites    int      `json:"max_candidate_sites"`
	TimeoutSeconds      int      `json:"timeout_seconds"`
}
```

- [ ] **Step 4: Implement workflow orchestration**

Implement:

```go
func DiscoverBrregDomainForCompany(ctx workflow.Context, input BrregDomainCompanyWorkflowInput) (DomainDecisionResult, error)
```

Required behavior:
- If `ExistingWebsite` is set:
  - crawl existing website
  - analyze it with each configured model until one succeeds
  - if owned domain accepted, persist and return
  - if action fails retryably, let Temporal retry the activity according to activity retry policy
- If no owned domain from existing website:
  - for each provider in `SearchProviders`
    - fetch search page
    - analyze search page
    - crawl top candidates up to `MaxCandidateSites`
    - analyze candidates
    - stop once owned domain is found
- Always call `PersistBrregDomainDecision` exactly once.

Use activity retry policy:

```go
workflow.ActivityOptions{
	StartToCloseTimeout: 5 * time.Minute,
	RetryPolicy: &temporal.RetryPolicy{
		InitialInterval: time.Minute,
		BackoffCoefficient: 2,
		MaximumInterval: 30 * time.Minute,
		MaximumAttempts: 3,
	},
}
```

For action failures that should switch provider/model, activity should return a typed result with `Status=failed`, not a Temporal error. Reserve Temporal errors for transport/runtime failures.

- [ ] **Step 5: Add persist activity**

Implement:

```go
func (a *Activities) PersistBrregDomainDecision(ctx context.Context, input PersistBrregDomainDecisionInput) error
```

It converts `DomainDecisionResult` into `SubmitDomainResultCommand`:
- `succeeded`: requires `BestDomain != nil`
- `not_found`: no `BestDomain`
- `failed`: uses first error and `Failure` metadata

- [ ] **Step 6: Run tests**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./internal/brreg/temporal -run TestDiscoverBrregDomainForCompany
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
git add scheduler/internal/brreg/temporal
git commit -m "Add BRREG domain discovery child workflow"
```

---

### Task 8: Change Parent Domain Workflow To Start Child Workflows

**Files:**
- Modify: `scheduler/internal/brreg/temporal/domain.go`
- Modify: `scheduler/internal/brreg/temporal/domain_activity.go`
- Modify: `scheduler/internal/brreg/temporal/domain_test.go`
- Modify: `scheduler/internal/brreg/service/actions.go`
- Modify: `scheduler/internal/brreg/httpapi/domain.go`

- [ ] **Step 1: Write failing parent workflow test**

Add to `domain_test.go`:

```go
func TestDiscoverBrregDomainsStartsChildWorkflowPerClaimedCompany(t *testing.T) {
	env := testsuite.WorkflowTestSuite{}.NewTestWorkflowEnvironment()
	input := BrregDomainWorkflowInput{
		Limit: 2,
		MaxParallelCompanyActivities: 2,
		SearchProviders: []string{"duckduckgo", "yandex"},
		LLMModels: []string{"deepseek", "local"},
	}

	env.OnActivity(PrepareBrregDomainWorkflowActivityName, mock.Anything, mock.Anything).Return(BrregWorkflowPrepared{WorkflowRunID: uuid.New().String()}, nil)
	env.OnActivity(ClaimBrregDomainCompanyPageActivityName, mock.Anything, mock.Anything).Return(BrregDomainCompanyPageResult{
		Companies: []BrregDomainCompanyInput{
			{RawRecordID: uuid.New().String(), OrganizationName: "A AS"},
			{RawRecordID: uuid.New().String(), OrganizationName: "B AS"},
		},
	}, nil).Once()
	env.OnWorkflow(DiscoverBrregDomainForCompany, mock.Anything, mock.Anything).Return(DomainDecisionResult{Status:"not_found"}, nil).Twice()
	env.OnActivity(ClaimBrregDomainCompanyPageActivityName, mock.Anything, mock.Anything).Return(BrregDomainCompanyPageResult{}, nil).Once()

	env.ExecuteWorkflow(DiscoverBrregDomains, input)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./internal/brreg/temporal -run TestDiscoverBrregDomainsStartsChildWorkflowPerClaimedCompany
```

Expected: FAIL because parent still runs `DiscoverOneBrregDomainCompanyActivity`.

- [ ] **Step 3: Extend workflow input**

Add to `BrregDomainWorkflowInput`:

```go
SearchProviders []string `json:"search_providers,omitempty"`
LLMModels       []string `json:"llm_models,omitempty"`
```

Defaults:

```go
if len(input.SearchProviders) == 0 {
	input.SearchProviders = []string{"duckduckgo", "yandex"}
}
if len(input.LLMModels) == 0 {
	input.LLMModels = []string{"deepseek", "local"}
}
```

- [ ] **Step 4: Replace company activity loop with child workflow loop**

In `domain.go`, replace `runDomainCompanyActivities` usage with `runDomainCompanyChildWorkflows`.

`runDomainCompanyChildWorkflows` must:
- start child workflow `DiscoverBrregDomainForCompany`
- cap concurrency using existing `MaxParallelCompanyActivities`
- collect counts by result status
- return failed count only when child workflow returns error or final result status is `failed`

- [ ] **Step 5: Keep old coarse activity behind no default path**

Leave `DiscoverOneBrregDomainCompany` in code for one release, but do not call it from `DiscoverBrregDomains`.

- [ ] **Step 6: Run Temporal tests**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./internal/brreg/temporal
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
git add scheduler/internal/brreg/temporal scheduler/internal/brreg/service/actions.go scheduler/internal/brreg/httpapi/domain.go
git commit -m "Run BRREG domain discovery as child workflows"
```

---

### Task 9: Add UI/API Advanced Strategy Controls

**Files:**
- Modify: `scheduler/internal/brreg/httpapi/domain.go`
- Modify: `scheduler/internal/brreg/httpapi/domain_test.go`
- Modify: `ui/app/components/app/BrregRawRecordActionSheet.tsx`
- Modify: `ui/app/lib/api.ts`
- Modify: `ui/app/types/api.ts`

- [ ] **Step 1: Write failing API parsing test**

Add to `domain_test.go`:

```go
func TestDiscoverDomainsAcceptsStrategyOptions(t *testing.T) {
	handler := NewHandler(&fakeBrregService{})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/brreg/discover-domains", strings.NewReader(`{
		"limit": 1000,
		"search_providers": ["duckduckgo", "yandex"],
		"llm_models": ["deepseek", "local"],
		"max_candidate_sites": 3
	}`))
	rec := httptest.NewRecorder()

	handler.handleDiscoverDomains(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, []string{"duckduckgo", "yandex"}, fake.lastDomainCommand.SearchProviders)
	require.Equal(t, []string{"deepseek", "local"}, fake.lastDomainCommand.LLMModels)
	require.Equal(t, 3, fake.lastDomainCommand.MaxCandidateSites)
}
```

- [ ] **Step 2: Add request fields**

Add fields:

```go
SearchProviders   []string `json:"search_providers"`
LLMModels         []string `json:"llm_models"`
MaxCandidateSites int      `json:"max_candidate_sites"`
```

Thread them through:
- HTTP request
- `StartDomainDiscoveryCommand`
- Temporal input

- [ ] **Step 3: Update UI action sheet**

In `BrregRawRecordActionSheet.tsx`, add advanced controls for domain discovery:
- records to process
- max candidate sites
- search provider multi-select default `duckduckgo,yandex`
- LLM fallback order default `deepseek,local`

Use current simple controls; no visual redesign.

- [ ] **Step 4: Run frontend and scheduler checks**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./internal/brreg/httpapi ./internal/brreg/service ./internal/brreg/temporal
cd /Users/graovic/pulsarpoint/ppoint/corpscout/ui
pnpm typecheck
pnpm build
```

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
git add scheduler/internal/brreg ui/app
git commit -m "Add BRREG domain strategy controls"
```

---

### Task 10: Integration Rollout And Cleanup

**Files:**
- Modify: `scheduler/internal/brreg/temporal/domain_company_activity.go`
- Modify: `scheduler/internal/brreg/temporal/domain_activity.go`
- Modify: `scheduler/internal/brreg/temporal/domain_submission.go`
- Modify: `scheduler/internal/brreg/temporal/domain_test.go`
- Add migration only if old artifacts need reclassification after test run.

- [ ] **Step 1: Run local unit suites**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./...
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/services/crawl-service
uv run pytest -q
```

Expected: all pass.

- [ ] **Step 2: Push both repos**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
git push origin main
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines
git push origin main
```

Expected: GitHub Actions build new crawl-service image.

- [ ] **Step 3: Deploy services**

On `companycollect`:

```bash
cd /home/graovic/temporal/services
docker compose pull crawl-service crawl-worker
docker compose up -d crawl-service crawl-worker
```

On Corpscout deployment:

```bash
cd /path/to/corpscout
docker compose pull scheduler ui
docker compose up -d migrate scheduler ui
```

Use the actual remote Corpscout directory currently used on the server.

- [ ] **Step 4: Run a 50-record smoke test**

From UI or API start domain discovery:

```json
{
  "limit": 50,
  "search_providers": ["duckduckgo", "yandex"],
  "llm_models": ["deepseek", "local"],
  "max_candidate_sites": 3,
  "max_attempts": 3
}
```

Then query:

```sql
SELECT status, best_domain IS NULL AS best_domain_null, count(*)
FROM brreg_workflow.domain_results
GROUP BY status, best_domain IS NULL
ORDER BY status, best_domain_null;

SELECT action_type, status, error_code, count(*)
FROM brreg_workflow.domain_action_attempts
GROUP BY action_type, status, error_code
ORDER BY action_type, status, error_code NULLS FIRST;
```

Expected:
- no `succeeded` rows with `best_domain IS NULL`
- action attempts exist for every processed raw record
- failures show action-level error codes

- [ ] **Step 5: Remove old coarse domain execution path**

After smoke test passes, remove the old direct activity path:
- stop calling `DiscoverOneBrregDomainCompany`
- keep the crawl-service coarse `/v1/brreg/domain-discovery` endpoint for manual/debug only
- add a test that `DiscoverBrregDomains` does not invoke the old coarse activity

Commit:

```bash
git add scheduler/internal/brreg/temporal
git commit -m "Retire coarse BRREG domain workflow path"
```

---

## Self-Review

**Spec coverage:** The plan covers DB action artifacts, crawl-service action endpoints, Go client methods, deterministic domain decision logic, child workflow orchestration, parent workflow migration, UI/API strategy controls, rollout, and old-path cleanup.

**Placeholder scan:** No task depends on unnamed future behavior. The only environment-dependent path is the remote Corpscout deployment directory, which must be resolved at deployment time because it is not represented in this repository.

**Type consistency:** Action names are consistent across migration, Go gateway constants, and workflow tasks. Final `domain_results.status` semantics are explicit: `succeeded` requires `best_domain`, related-only results become `not_found`, and action failures become `failed`.

**Execution risk:** Task 2 code snippets may require minor pgx/sqlc type adjustment after generation; the expected behavior and test boundaries are explicit.
