# BRREG Source Flow Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the BRREG-only Flow tab as a vertical React Flow overview that links users into filtered Raw Inputs, where subset actions are executed.

**Architecture:** The backend exposes `GET /api/v1/sources/brreg/flow` as a graph config built from BRREG raw-input counts, source schedule metadata, and a new `source_action_tasks` ledger. The frontend renders that config with React Flow + ELKJS, while Raw Inputs parses URL filters and owns all selected/all-filtered actions.

**Tech Stack:** Go, chi, pgx, PostgreSQL migrations, Temporal SDK, River-compatible task ledger, React Router, shadcn/ui, `@xyflow/react`, `elkjs`, TypeScript.

---

## File Structure

- Create `database/migrations/000045_source_action_tasks.up.sql`: local task ledger table and indexes.
- Create `database/migrations/000045_source_action_tasks.down.sql`: rollback for the ledger table.
- Create `scheduler/internal/httpapi/source_flow.go`: BRREG flow response types, count queries, filter-link construction, and task counter aggregation.
- Create `scheduler/internal/httpapi/source_flow_test.go`: API tests for BRREG flow counts, filter URLs, zero-count link disabling, and task counters.
- Modify `scheduler/internal/httpapi/handlers.go`: register `/sources/brreg/flow`.
- Modify `scheduler/internal/httpapi/raw_inputs.go`: support `has_suggestion`, return suggestion metadata, and expose bulk raw-input actions.
- Modify `scheduler/internal/httpapi/raw_inputs_test.go`: tests for URL filter support and bulk action scope validation.
- Modify `scheduler/internal/httpapi/sources.go`: record BRREG translation tasks in `source_action_tasks`.
- Modify `scheduler/internal/httpapi/sources_test.go`: assert BRREG translation creates task rows.
- Modify `ui/package.json` and `ui/pnpm-lock.yaml`: add `@xyflow/react` and `elkjs`.
- Modify `ui/app/types/api.ts`: add flow graph and raw-input filter/action types.
- Modify `ui/app/lib/api.ts`: add `getSourceFlow`, `translateBrregRawInputs`, and `createBrregCompanySuggestions` methods.
- Create `ui/app/routes/sources_.$name.flow.tsx`: source detail route for BRREG Flow.
- Modify `ui/app/routes/sources_.$name.tsx`: show Flow tab only for BRREG.
- Create `ui/app/components/app/source-detail/flow/BrregFlowTab.tsx`: page-level loading/error/rendering component.
- Create `ui/app/components/app/source-detail/flow/FlowStateNode.tsx`: React Flow node for state/count widgets.
- Create `ui/app/components/app/source-detail/flow/FlowTaskEdge.tsx`: React Flow edge with task counters that link to task list filters.
- Create `ui/app/components/app/source-detail/flow/layout.ts`: ELKJS vertical layout adapter.
- Modify `ui/app/components/app/RawInputsTable.tsx`: URL filter initialization/sync, filter chips, valid bulk action bar, all-filtered action labels.

## Task 1: Add Dependencies And Route Skeleton

**Files:**
- Modify: `ui/package.json`
- Modify: `ui/pnpm-lock.yaml`
- Create: `ui/app/routes/sources_.$name.flow.tsx`
- Modify: `ui/app/routes/sources_.$name.tsx`

- [ ] **Step 1: Add React Flow and ELKJS**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/ui
pnpm add @xyflow/react elkjs
```

Expected: `package.json` includes both dependencies and `pnpm-lock.yaml` is updated.

- [ ] **Step 2: Add a temporary Flow route**

Create `ui/app/routes/sources_.$name.flow.tsx`:

```tsx
import { useOutletContext } from "react-router";
import type { SourceDetailContext } from "~/routes/sources_.$name";
import { Alert, AlertDescription } from "~/components/ui/alert";

export default function SourceFlowPage() {
  const { source } = useOutletContext<SourceDetailContext>();

  if (source.name !== "brreg") {
    return (
      <Alert>
        <AlertDescription>Flow is available for BRREG only.</AlertDescription>
      </Alert>
    );
  }

  return (
    <div className="rounded-md border p-6 text-sm text-muted-foreground">
      BRREG flow will render here.
    </div>
  );
}
```

- [ ] **Step 3: Show the Flow tab only for BRREG**

In `ui/app/routes/sources_.$name.tsx`, add the Flow tab after Schedule:

```tsx
const tabs = [
  { label: "Schedule", to: `/sources/${source.name}/schedule` },
  ...(source.name === "brreg" ? [{ label: "Flow", to: `/sources/${source.name}/flow` }] : []),
  { label: "Config", to: `/sources/${source.name}/config` },
  { label: "Logs", to: `/sources/${source.name}/logs` },
  ...(hasRawInputs(source) ? [{ label: "Raw Inputs", to: `/sources/${source.name}/raw_input` }] : []),
  ...(hasPipeline(source) ? [{ label: "Pipeline", to: `/sources/${source.name}/pipeline` }] : []),
];
```

- [ ] **Step 4: Verify route compiles**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/ui
pnpm typecheck
```

Expected: command exits `0`.

- [ ] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
git add ui/package.json ui/pnpm-lock.yaml ui/app/routes/sources_.'$'name.flow.tsx ui/app/routes/sources_.'$'name.tsx
git commit -m "feat: add brreg flow route shell"
```

## Task 2: Add Source Action Task Ledger

**Files:**
- Create: `database/migrations/000045_source_action_tasks.up.sql`
- Create: `database/migrations/000045_source_action_tasks.down.sql`

- [ ] **Step 1: Create the migration**

Create `database/migrations/000045_source_action_tasks.up.sql`:

```sql
CREATE TABLE source_action_tasks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_name TEXT NOT NULL REFERENCES data_sources(name) ON DELETE CASCADE,
    action_key TEXT NOT NULL,
    executor_type TEXT NOT NULL,
    temporal_workflow_id TEXT,
    temporal_workflow_run_id TEXT,
    river_job_id BIGINT,
    status TEXT NOT NULL DEFAULT 'queued',
    requested_scope JSONB NOT NULL DEFAULT '{}'::jsonb,
    requested_by TEXT,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    error_message TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_source_action_tasks_action_key CHECK (
        action_key IN ('download', 'translate', 'create_suggestions', 'retry_processing')
    ),
    CONSTRAINT chk_source_action_tasks_executor CHECK (
        executor_type IN ('temporal', 'river')
    ),
    CONSTRAINT chk_source_action_tasks_status CHECK (
        status IN ('queued', 'running', 'completed', 'failed', 'cancelled')
    ),
    CONSTRAINT chk_source_action_tasks_executor_ref CHECK (
        temporal_workflow_id IS NOT NULL OR river_job_id IS NOT NULL
    ),
    CONSTRAINT chk_source_action_tasks_json_scope CHECK (jsonb_typeof(requested_scope) = 'object'),
    CONSTRAINT chk_source_action_tasks_json_metadata CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE INDEX idx_source_action_tasks_source_action_status
    ON source_action_tasks (source_name, action_key, status, created_at DESC);

CREATE INDEX idx_source_action_tasks_temporal
    ON source_action_tasks (temporal_workflow_id, temporal_workflow_run_id)
    WHERE temporal_workflow_id IS NOT NULL;

CREATE INDEX idx_source_action_tasks_river
    ON source_action_tasks (river_job_id)
    WHERE river_job_id IS NOT NULL;
```

Create `database/migrations/000045_source_action_tasks.down.sql`:

```sql
DROP TABLE IF EXISTS source_action_tasks;
```

- [ ] **Step 2: Validate migration ordering**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
ls database/migrations/000045_source_action_tasks.*.sql
```

Expected:

```text
database/migrations/000045_source_action_tasks.down.sql
database/migrations/000045_source_action_tasks.up.sql
```

- [ ] **Step 3: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
git add database/migrations/000045_source_action_tasks.up.sql database/migrations/000045_source_action_tasks.down.sql
git commit -m "feat: add source action task ledger"
```

## Task 3: Backend BRREG Flow API

**Files:**
- Create: `scheduler/internal/httpapi/source_flow.go`
- Create: `scheduler/internal/httpapi/source_flow_test.go`
- Modify: `scheduler/internal/httpapi/handlers.go`

- [ ] **Step 1: Write the failing API test**

Create `scheduler/internal/httpapi/source_flow_test.go`:

```go
package httpapi_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/pashagolub/pgxmock/v3"
	"github.com/stretchr/testify/require"

	"github.com/pulsarpoint/corpscout/scheduler/internal/httpapi"
)

func TestGetBrregFlow_returnsCountsAndLinks(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	now := time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC)
	h := httpapi.NewHandlers(&stubQuerier{}, nil, pool, nil, nil, "", nil, "")
	r := routerFor(h)

	pool.ExpectQuery("SELECT;;last_started_at;;next_scheduled_at;;data_sources").
		WillReturnRows(pgxmock.NewRows([]string{"last_started_at", "next_scheduled_at"}).
			AddRow(now.Add(-10*time.Hour), nil))

	pool.ExpectQuery("FROM brreg_company_raw_inputs;;GROUP BY processing_status").
		WillReturnRows(pgxmock.NewRows([]string{"status", "count"}).
			AddRow("pending", int64(3)).
			AddRow("processed", int64(10)).
			AddRow("failed", int64(1)))

	pool.ExpectQuery("FROM brreg_company_raw_inputs;;GROUP BY translation_status").
		WillReturnRows(pgxmock.NewRows([]string{"status", "count"}).
			AddRow("pending", int64(2)).
			AddRow("translated", int64(8)).
			AddRow("failed", int64(1)))

	pool.ExpectQuery("NOT EXISTS;;suggestion_source_links").
		WillReturnRows(pgxmock.NewRows([]string{"count"}).AddRow(int64(5)))

	pool.ExpectQuery("suggestion_source_links;;company_suggestions").
		WillReturnRows(pgxmock.NewRows([]string{"status", "count"}).
			AddRow("pending", int64(4)))

	pool.ExpectQuery("source_action_tasks").
		WillReturnRows(pgxmock.NewRows([]string{"action_key", "status", "count"}).
			AddRow("translate", "running", int64(1)))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/sources/brreg/flow", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var body map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	require.Equal(t, "brreg", body["source"])
	require.Equal(t, "vertical", body["layout"])
	require.Contains(t, w.Body.String(), "/sources/brreg/raw_input?translation_status=pending")
	require.Contains(t, w.Body.String(), "/sources/brreg/raw_input?translation_status=translated&processing_status=pending&has_suggestion=false")
	require.Contains(t, w.Body.String(), `"action_key":"translate"`)
	require.NoError(t, pool.ExpectationsWereMet())
}
```

- [ ] **Step 2: Run the failing test**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test -count=1 ./internal/httpapi -run TestGetBrregFlow_returnsCountsAndLinks
```

Expected: FAIL because `/sources/brreg/flow` is not registered.

- [ ] **Step 3: Register the route**

In `scheduler/internal/httpapi/handlers.go`, add this next to other source routes:

```go
r.Get("/sources/brreg/flow", h.handleGetBrregFlow)
```

- [ ] **Step 4: Implement the handler**

Create `scheduler/internal/httpapi/source_flow.go`:

```go
package httpapi

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"time"
)

type sourceFlowResponse struct {
	Source    string           `json:"source"`
	Layout    string           `json:"layout"`
	Nodes     []sourceFlowNode `json:"nodes"`
	Edges     []sourceFlowEdge `json:"edges"`
	UpdatedAt time.Time        `json:"updated_at"`
}

type sourceFlowNode struct {
	ID          string            `json:"id"`
	Kind        string            `json:"kind"`
	Title       string            `json:"title"`
	Description string            `json:"description,omitempty"`
	Counts      []sourceFlowCount `json:"counts,omitempty"`
	Meta        map[string]any    `json:"meta,omitempty"`
}

type sourceFlowCount struct {
	Key      string `json:"key"`
	Label    string `json:"label"`
	Value    int64  `json:"value"`
	Href     string `json:"href,omitempty"`
	Disabled bool  `json:"disabled"`
}

type sourceFlowEdge struct {
	ID           string                  `json:"id"`
	Source       string                  `json:"source"`
	Target       string                  `json:"target"`
	ActionKey    string                  `json:"action_key"`
	Label        string                  `json:"label"`
	TaskCounters []sourceFlowTaskCounter `json:"task_counters,omitempty"`
}

type sourceFlowTaskCounter struct {
	Status string `json:"status"`
	Count  int64  `json:"count"`
	Href   string `json:"href,omitempty"`
}

func (h *Handlers) handleGetBrregFlow(w http.ResponseWriter, r *http.Request) {
	if h.pool == nil {
		writeError(w, http.StatusServiceUnavailable, "database pool not available")
		return
	}

	ctx := r.Context()
	timing, err := h.brregFlowTiming(ctx)
	if err != nil {
		slog.Error("brreg flow timing", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	processingCounts, err := h.brregProcessingCounts(ctx)
	if err != nil {
		slog.Error("brreg flow processing counts", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	translationCounts, err := h.brregTranslationCounts(ctx)
	if err != nil {
		slog.Error("brreg flow translation counts", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	readyCount, err := h.brregReadyForSuggestionCount(ctx)
	if err != nil {
		slog.Error("brreg flow ready count", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	suggestionCounts, err := h.brregSuggestionCounts(ctx)
	if err != nil {
		slog.Error("brreg flow suggestion counts", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	taskCounters, err := h.sourceActionTaskCounters(ctx, "brreg")
	if err != nil {
		slog.Error("brreg flow task counters", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	resp := sourceFlowResponse{
		Source: "brreg",
		Layout: "vertical",
		UpdatedAt: time.Now(),
		Nodes: []sourceFlowNode{
			{
				ID: "source",
				Kind: "state",
				Title: "BRREG source",
				Description: "Official Norway registry download source.",
				Meta: timing,
			},
			{
				ID: "raw_inputs",
				Kind: "state",
				Title: "Raw inputs",
				Counts: processingFlowCounts(processingCounts),
			},
			{
				ID: "translation",
				Kind: "state",
				Title: "Translation states",
				Counts: translationFlowCounts(translationCounts),
			},
			{
				ID: "ready_for_suggestions",
				Kind: "state",
				Title: "Ready for company suggestions",
				Counts: []sourceFlowCount{flowCount("ready", "Translated + pending", readyCount, rawInputHref(map[string]string{
					"translation_status": "translated",
					"processing_status": "pending",
					"has_suggestion": "false",
				}))},
			},
			{
				ID: "company_suggestions",
				Kind: "state",
				Title: "Company suggestions",
				Counts: suggestionFlowCounts(suggestionCounts),
			},
		},
		Edges: []sourceFlowEdge{
			{ID: "source-download-raw", Source: "source", Target: "raw_inputs", ActionKey: "download", Label: "Download raw data", TaskCounters: taskCounters["download"]},
			{ID: "raw-translate-translation", Source: "raw_inputs", Target: "translation", ActionKey: "translate", Label: "Translation tasks", TaskCounters: taskCounters["translate"]},
			{ID: "translation-ready", Source: "translation", Target: "ready_for_suggestions", ActionKey: "create_suggestions", Label: "Ready rows", TaskCounters: taskCounters["create_suggestions"]},
			{ID: "ready-suggestions", Source: "ready_for_suggestions", Target: "company_suggestions", ActionKey: "create_suggestions", Label: "Company suggestions", TaskCounters: taskCounters["create_suggestions"]},
		},
	}

	writeJSON(w, http.StatusOK, resp)
}
```

In the same file, add helper functions:

```go
func (h *Handlers) brregFlowTiming(ctx context.Context) (map[string]any, error) {
	var lastStarted *time.Time
	var nextScheduled *time.Time
	err := h.pool.QueryRow(ctx, `
		SELECT last_started_at, next_scheduled_at
		FROM (
			SELECT d.last_started_at, NULL::timestamptz AS next_scheduled_at
			FROM data_sources d
			WHERE d.name = 'brreg'
		) s
	`).Scan(&lastStarted, &nextScheduled)
	return map[string]any{
		"last_downloaded_at": lastStarted,
		"next_scheduled_at": nextScheduled,
	}, err
}

func (h *Handlers) brregProcessingCounts(ctx context.Context) (map[string]int64, error) {
	return groupedCount(ctx, h.pool, `
		SELECT processing_status, COUNT(*)
		FROM brreg_company_raw_inputs
		GROUP BY processing_status
	`)
}

func (h *Handlers) brregTranslationCounts(ctx context.Context) (map[string]int64, error) {
	return groupedCount(ctx, h.pool, `
		SELECT translation_status, COUNT(*)
		FROM brreg_company_raw_inputs
		GROUP BY translation_status
	`)
}

func (h *Handlers) brregReadyForSuggestionCount(ctx context.Context) (int64, error) {
	var count int64
	err := h.pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM brreg_company_raw_inputs bri
		WHERE bri.translation_status = 'translated'
		  AND bri.processing_status = 'pending'
		  AND NOT EXISTS (
		  	SELECT 1 FROM suggestion_source_links ssl
		  	WHERE ssl.source_input_table = 'brreg_company_raw_inputs'
		  	  AND ssl.source_input_key = bri.id::text
		  )
	`).Scan(&count)
	return count, err
}

func (h *Handlers) brregSuggestionCounts(ctx context.Context) (map[string]int64, error) {
	return groupedCount(ctx, h.pool, `
		SELECT cs.status, COUNT(*)
		FROM company_suggestions cs
		JOIN suggestion_source_links ssl
		  ON ssl.suggestion_table = 'company_suggestions'
		 AND ssl.suggestion_id = cs.id
		WHERE ssl.source_input_table = 'brreg_company_raw_inputs'
		GROUP BY cs.status
	`)
}

func (h *Handlers) sourceActionTaskCounters(ctx context.Context, source string) (map[string][]sourceFlowTaskCounter, error) {
	rows, err := h.pool.Query(ctx, `
		SELECT action_key, status, COUNT(*)
		FROM source_action_tasks
		WHERE source_name = $1
		  AND status IN ('queued', 'running', 'failed')
		GROUP BY action_key, status
	`, source)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	counters := map[string][]sourceFlowTaskCounter{}
	for rows.Next() {
		var action, status string
		var count int64
		if err := rows.Scan(&action, &status, &count); err != nil {
			return nil, err
		}
		counters[action] = append(counters[action], sourceFlowTaskCounter{
			Status: status,
			Count: count,
			Href: fmt.Sprintf("/jobs?source=%s&action=%s&status=%s", source, action, status),
		})
	}
	return counters, rows.Err()
}

func groupedCount(ctx context.Context, pool dbPool, sql string, args ...any) (map[string]int64, error) {
	rows, err := pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	counts := map[string]int64{}
	for rows.Next() {
		var key string
		var count int64
		if err := rows.Scan(&key, &count); err != nil {
			return nil, err
		}
		counts[key] = count
	}
	return counts, rows.Err()
}

func processingFlowCounts(counts map[string]int64) []sourceFlowCount {
	keys := []string{"pending", "processing", "failed", "ignored", "superseded", "processed"}
	result := make([]sourceFlowCount, 0, len(keys)+1)
	var total int64
	for _, key := range keys {
		total += counts[key]
		result = append(result, flowCount(key, key, counts[key], rawInputHref(map[string]string{"processing_status": key})))
	}
	result = append(result, sourceFlowCount{Key: "total", Label: "total", Value: total, Disabled: true})
	return result
}

func translationFlowCounts(counts map[string]int64) []sourceFlowCount {
	keys := []string{"pending", "translating", "translated", "failed"}
	result := make([]sourceFlowCount, 0, len(keys)+1)
	var total int64
	for _, key := range keys {
		total += counts[key]
		result = append(result, flowCount(key, key, counts[key], rawInputHref(map[string]string{"translation_status": key})))
	}
	result = append(result, sourceFlowCount{Key: "total", Label: "total", Value: total, Disabled: true})
	return result
}

func suggestionFlowCounts(counts map[string]int64) []sourceFlowCount {
	keys := []string{"pending", "approved", "rejected"}
	result := make([]sourceFlowCount, 0, len(keys))
	for _, key := range keys {
		result = append(result, flowCount(key, key, counts[key], "/suggestions/companies?status="+url.QueryEscape(key)))
	}
	return result
}

func flowCount(key, label string, value int64, href string) sourceFlowCount {
	return sourceFlowCount{
		Key: key,
		Label: label,
		Value: value,
		Href: href,
		Disabled: value == 0,
	}
}

func rawInputHref(params map[string]string) string {
	q := url.Values{}
	for key, value := range params {
		q.Set(key, value)
	}
	return "/sources/brreg/raw_input?" + q.Encode()
}
```

- [ ] **Step 5: Run gofmt and the focused test**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
gofmt -w internal/httpapi/source_flow.go internal/httpapi/source_flow_test.go internal/httpapi/handlers.go
GOWORK=off go test -count=1 ./internal/httpapi -run TestGetBrregFlow_returnsCountsAndLinks
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
git add scheduler/internal/httpapi/source_flow.go scheduler/internal/httpapi/source_flow_test.go scheduler/internal/httpapi/handlers.go
git commit -m "feat: add brreg source flow api"
```

## Task 4: Record BRREG Translation In The Task Ledger

**Files:**
- Modify: `scheduler/internal/httpapi/sources.go`
- Modify: `scheduler/internal/httpapi/sources_test.go`

- [ ] **Step 1: Add a failing test for task recording**

In `scheduler/internal/httpapi/sources_test.go`, add:

```go
func TestTranslateBrreg_recordsSourceActionTask(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	tc := &temporalExecuteRecorder{}
	h := httpapi.NewHandlers(&stubQuerier{}, nil, pool, nil, nil, "", tc, "")
	r := routerFor(h)

	pool.ExpectExec("INSERT INTO source_action_tasks").
		WithArgs(
			"brreg",
			"translate",
			"temporal",
			"translate-brreg-all",
			"run-1",
			"running",
			pgxmock.AnyArg(),
			pgxmock.AnyArg(),
		).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/sources/brreg/translate", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.NoError(t, pool.ExpectationsWereMet())
}
```

- [ ] **Step 2: Run the failing test**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test -count=1 ./internal/httpapi -run TestTranslateBrreg_recordsSourceActionTask
```

Expected: FAIL because no task row is inserted.

- [ ] **Step 3: Add ledger helper**

In `scheduler/internal/httpapi/sources.go`, add:

```go
func (h *Handlers) recordSourceActionTask(ctx context.Context, source, action, executor, workflowID, runID, status string, scope map[string]any, metadata map[string]any) {
	if h.pool == nil {
		return
	}
	if scope == nil {
		scope = map[string]any{}
	}
	if metadata == nil {
		metadata = map[string]any{}
	}
	scopeJSON, err := json.Marshal(scope)
	if err != nil {
		slog.Error("marshal source action task scope", "source", source, "action", action, "error", err)
		return
	}
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		slog.Error("marshal source action task metadata", "source", source, "action", action, "error", err)
		return
	}
	if _, err := h.pool.Exec(ctx, `
		INSERT INTO source_action_tasks (
			source_name, action_key, executor_type,
			temporal_workflow_id, temporal_workflow_run_id,
			status, requested_scope, metadata, started_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb, now())
	`, source, action, executor, workflowID, runID, status, scopeJSON, metadataJSON); err != nil {
		slog.Error("record source action task", "source", source, "action", action, "error", err)
	}
}
```

- [ ] **Step 4: Record BRREG translation after Temporal starts**

In `handleTranslateBrreg`, after `ExecuteWorkflow` succeeds and before `writeJSON`, add:

```go
h.recordSourceActionTask(r.Context(), "brreg", "translate", "temporal", workflowID, run.GetRunID(), "running", map[string]any{
	"ids": req.IDs,
	"fx_rate_date": req.FXRateDate,
}, map[string]any{
	"workflow_type": "TranslateBrregRawInputs",
})
```

- [ ] **Step 5: Run focused tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
gofmt -w internal/httpapi/sources.go internal/httpapi/sources_test.go
GOWORK=off go test -count=1 ./internal/httpapi -run 'TestTranslateBrreg_recordsSourceActionTask|TestTranslateBrreg'
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
git add scheduler/internal/httpapi/sources.go scheduler/internal/httpapi/sources_test.go
git commit -m "feat: record brreg translation tasks"
```

## Task 5: Extend Raw Inputs Filtering

**Files:**
- Modify: `scheduler/internal/httpapi/raw_inputs.go`
- Modify: `scheduler/internal/httpapi/raw_inputs_test.go`
- Modify: `ui/app/types/api.ts`
- Modify: `ui/app/lib/api.ts`

- [ ] **Step 1: Add failing backend tests for `has_suggestion`**

In `scheduler/internal/httpapi/raw_inputs_test.go`, add:

```go
func TestListRawInputs_filtersByHasSuggestion(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	h := httpapi.NewHandlers(&stubQuerier{}, nil, pool, nil, nil, "", nil, "")
	r := routerFor(h)
	createdAt := time.Date(2026, 5, 22, 10, 0, 0, 0, time.UTC)

	pool.ExpectQuery("COUNT.*has_suggestion = false").
		WithArgs("brreg", false).
		WillReturnRows(pgxmock.NewRows([]string{"count"}).AddRow(int64(1)))
	pool.ExpectQuery("SELECT id, source, name, native_id, status, translation_status, has_suggestion").
		WithArgs("brreg", false, 50, 0).
		WillReturnRows(pgxmock.NewRows([]string{
			"id", "source", "name", "native_id", "status", "translation_status", "has_suggestion", "created_at",
		}).AddRow("raw-1", "brreg", "Norway AS", "991234567", "pending", "translated", false, createdAt))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/raw-inputs?source=brreg&has_suggestion=false", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), `"has_suggestion":false`)
	require.NoError(t, pool.ExpectationsWereMet())
}
```

- [ ] **Step 2: Run the failing test**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test -count=1 ./internal/httpapi -run TestListRawInputs_filtersByHasSuggestion
```

Expected: FAIL because the list endpoint ignores `has_suggestion`.

- [ ] **Step 3: Extend raw input list response**

In `rawInputRow`, add:

```go
HasSuggestion bool `json:"has_suggestion"`
```

In `handleListRawInputs`, read:

```go
hasSuggestionFilter := r.URL.Query().Get("has_suggestion")
var hasSuggestion *bool
if hasSuggestionFilter == "true" {
	v := true
	hasSuggestion = &v
} else if hasSuggestionFilter == "false" {
	v := false
	hasSuggestion = &v
}
```

Append `has_suggestion` to the generated union rows by adding this expression inside each `SELECT`:

```sql
EXISTS (
  SELECT 1 FROM suggestion_source_links ssl
  WHERE ssl.source_input_table = '<table>'
    AND ssl.source_input_key = id::text
) AS has_suggestion
```

Add the outer filter:

```go
if hasSuggestion != nil {
	args = append(args, *hasSuggestion)
	commonWhere = append(commonWhere, fmt.Sprintf("has_suggestion = $%d", len(args)))
}
```

Update final select and scan:

```go
dataSQL := fmt.Sprintf(
	"SELECT id, source, name, native_id, status, translation_status, has_suggestion, created_at FROM (%s) t ORDER BY %s %s LIMIT $%d OFFSET $%d",
	union, sortBy, sortDir, len(args)+1, len(args)+2,
)

if err := rows.Scan(&row.ID, &row.Source, &row.Name, &row.NativeID, &row.Status, &translationStatus, &row.HasSuggestion, &row.CreatedAt); err != nil {
	...
}
```

- [ ] **Step 4: Update frontend types and API params**

In `ui/app/types/api.ts`, update `RawInput`:

```ts
export interface RawInput {
  id: string;
  source: string;
  name: string;
  native_id: string;
  status: string;
  translation_status?: "pending" | "translating" | "translated" | "failed";
  has_suggestion: boolean;
  created_at: string;
  company_suggestion_id?: string;
  company_suggestion_status?: "pending" | "approved" | "rejected";
  can_approve_company: boolean;
}
```

In `ui/app/lib/api.ts`, add `has_suggestion`:

```ts
getRawInputs: (params: {
  page?: number;
  limit?: number;
  source?: string;
  status?: string;
  translation_status?: string;
  has_suggestion?: boolean;
  q?: string;
  sort?: string;
  dir?: "asc" | "desc";
} = {}) => {
  const qs = new URLSearchParams();
  ...
  if (params.has_suggestion != null) qs.set("has_suggestion", String(params.has_suggestion));
  ...
}
```

- [ ] **Step 5: Run tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
gofmt -w internal/httpapi/raw_inputs.go internal/httpapi/raw_inputs_test.go
GOWORK=off go test -count=1 ./internal/httpapi -run 'TestListRawInputs_filtersByHasSuggestion|TestListRawInputs'

cd /Users/graovic/pulsarpoint/ppoint/corpscout/ui
pnpm typecheck
```

Expected: both commands pass.

- [ ] **Step 6: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
git add scheduler/internal/httpapi/raw_inputs.go scheduler/internal/httpapi/raw_inputs_test.go ui/app/types/api.ts ui/app/lib/api.ts
git commit -m "feat: filter raw inputs by suggestion state"
```

## Task 6: Render The BRREG Flow Graph

**Files:**
- Modify: `ui/app/types/api.ts`
- Modify: `ui/app/lib/api.ts`
- Create: `ui/app/components/app/source-detail/flow/layout.ts`
- Create: `ui/app/components/app/source-detail/flow/FlowStateNode.tsx`
- Create: `ui/app/components/app/source-detail/flow/FlowTaskEdge.tsx`
- Create: `ui/app/components/app/source-detail/flow/BrregFlowTab.tsx`
- Modify: `ui/app/routes/sources_.$name.flow.tsx`

- [ ] **Step 1: Add frontend API types**

In `ui/app/types/api.ts`, add:

```ts
export interface SourceFlowCount {
  key: string;
  label: string;
  value: number;
  href?: string;
  disabled: boolean;
}

export interface SourceFlowTaskCounter {
  status: string;
  count: number;
  href?: string;
}

export interface SourceFlowNode {
  id: string;
  kind: string;
  title: string;
  description?: string;
  counts?: SourceFlowCount[];
  meta?: Record<string, unknown>;
}

export interface SourceFlowEdge {
  id: string;
  source: string;
  target: string;
  action_key: string;
  label: string;
  task_counters?: SourceFlowTaskCounter[];
}

export interface SourceFlowResponse {
  source: string;
  layout: "vertical";
  nodes: SourceFlowNode[];
  edges: SourceFlowEdge[];
  updated_at: string;
}
```

In `ui/app/lib/api.ts`, import `SourceFlowResponse` and add:

```ts
getSourceFlow: (name: string) =>
  get<SourceFlowResponse>(`/sources/${name}/flow`),
```

- [ ] **Step 2: Add ELK layout adapter**

Create `ui/app/components/app/source-detail/flow/layout.ts`:

```ts
import ELK from "elkjs/lib/elk.bundled.js";
import type { Edge, Node } from "@xyflow/react";

const elk = new ELK();

export async function layoutVertical(nodes: Node[], edges: Edge[]): Promise<{ nodes: Node[]; edges: Edge[] }> {
  const graph = {
    id: "brreg-flow",
    layoutOptions: {
      "elk.algorithm": "layered",
      "elk.direction": "DOWN",
      "elk.spacing.nodeNode": "48",
      "elk.layered.spacing.nodeNodeBetweenLayers": "72",
    },
    children: nodes.map((node) => ({
      id: node.id,
      width: Number(node.width ?? 360),
      height: Number(node.height ?? 160),
    })),
    edges: edges.map((edge) => ({
      id: edge.id,
      sources: [edge.source],
      targets: [edge.target],
    })),
  };

  const result = await elk.layout(graph);
  const positioned = nodes.map((node) => {
    const child = result.children?.find((item) => item.id === node.id);
    return {
      ...node,
      position: { x: child?.x ?? 0, y: child?.y ?? 0 },
    };
  });

  return { nodes: positioned, edges };
}
```

- [ ] **Step 3: Add state node**

Create `ui/app/components/app/source-detail/flow/FlowStateNode.tsx`:

```tsx
import { Link } from "react-router";
import type { NodeProps } from "@xyflow/react";
import { Badge } from "~/components/ui/badge";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "~/components/ui/card";
import type { SourceFlowNode } from "~/types/api";
import { formatDate } from "~/lib/utils";

export function FlowStateNode({ data }: NodeProps) {
  const node = data as unknown as SourceFlowNode;
  const lastDownloaded = typeof node.meta?.last_downloaded_at === "string" ? node.meta.last_downloaded_at : undefined;
  const nextScheduled = typeof node.meta?.next_scheduled_at === "string" ? node.meta.next_scheduled_at : undefined;

  return (
    <Card className="w-[360px] shadow-sm">
      <CardHeader className="pb-3">
        <CardTitle className="text-base">{node.title}</CardTitle>
        {node.description && <CardDescription>{node.description}</CardDescription>}
      </CardHeader>
      <CardContent className="space-y-3">
        {(lastDownloaded || nextScheduled) && (
          <div className="grid gap-1 text-xs text-muted-foreground">
            <span>Last downloaded: {lastDownloaded ? formatDate(lastDownloaded) : "-"}</span>
            <span>Next scheduled pull: {nextScheduled ? formatDate(nextScheduled) : "Paused"}</span>
          </div>
        )}
        {node.counts && (
          <div className="grid grid-cols-2 gap-2">
            {node.counts.map((count) => {
              const content = (
                <>
                  <span className="text-muted-foreground">{count.label}</span>
                  <strong>{count.value.toLocaleString()}</strong>
                </>
              );
              if (count.disabled || !count.href) {
                return (
                  <Badge key={count.key} variant="outline" className="justify-between opacity-60">
                    {content}
                  </Badge>
                );
              }
              return (
                <Link key={count.key} to={count.href} className="rounded-md border px-2 py-1 text-xs hover:bg-muted">
                  <span className="flex justify-between gap-3">{content}</span>
                </Link>
              );
            })}
          </div>
        )}
      </CardContent>
    </Card>
  );
}
```

- [ ] **Step 4: Add task edge**

Create `ui/app/components/app/source-detail/flow/FlowTaskEdge.tsx`:

```tsx
import { Link } from "react-router";
import { BaseEdge, EdgeLabelRenderer, getSmoothStepPath, type EdgeProps } from "@xyflow/react";
import { Badge } from "~/components/ui/badge";
import type { SourceFlowEdge } from "~/types/api";

export function FlowTaskEdge(props: EdgeProps) {
  const [path, labelX, labelY] = getSmoothStepPath(props);
  const data = props.data as SourceFlowEdge | undefined;

  return (
    <>
      <BaseEdge path={path} markerEnd={props.markerEnd} />
      <EdgeLabelRenderer>
        <div
          className="nodrag nopan absolute flex -translate-x-1/2 -translate-y-1/2 flex-col items-center gap-1 rounded-md border bg-background px-2 py-1 text-xs shadow-sm"
          style={{ left: labelX, top: labelY }}
        >
          <span className="font-medium">{data?.label ?? props.label}</span>
          <div className="flex flex-wrap justify-center gap-1">
            {(data?.task_counters ?? []).length === 0 ? (
              <Badge variant="outline">0 running</Badge>
            ) : data?.task_counters?.map((counter) => {
              const label = `${counter.count.toLocaleString()} ${counter.status}`;
              return counter.href ? (
                <Link key={counter.status} to={counter.href}>
                  <Badge variant="outline">{label}</Badge>
                </Link>
              ) : (
                <Badge key={counter.status} variant="outline">{label}</Badge>
              );
            })}
          </div>
        </div>
      </EdgeLabelRenderer>
    </>
  );
}
```

- [ ] **Step 5: Add page component**

Create `ui/app/components/app/source-detail/flow/BrregFlowTab.tsx`:

```tsx
import "@xyflow/react/dist/style.css";
import { useEffect, useMemo, useState } from "react";
import { Background, Controls, ReactFlow, type Edge, type Node } from "@xyflow/react";
import { api } from "~/lib/api";
import type { DataSource, SourceFlowResponse } from "~/types/api";
import { Alert, AlertDescription } from "~/components/ui/alert";
import { Skeleton } from "~/components/ui/skeleton";
import { FlowStateNode } from "~/components/app/source-detail/flow/FlowStateNode";
import { FlowTaskEdge } from "~/components/app/source-detail/flow/FlowTaskEdge";
import { layoutVertical } from "~/components/app/source-detail/flow/layout";

const nodeTypes = { state: FlowStateNode };
const edgeTypes = { task: FlowTaskEdge };

export function BrregFlowTab({ source }: { source: DataSource }) {
  const [flow, setFlow] = useState<SourceFlowResponse>();
  const [nodes, setNodes] = useState<Node[]>([]);
  const [edges, setEdges] = useState<Edge[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string>();

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(undefined);
    api.getSourceFlow(source.name)
      .then((data) => {
        if (!cancelled) setFlow(data);
      })
      .catch(() => {
        if (!cancelled) setError("Failed to load BRREG flow.");
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [source.name]);

  const rawNodes = useMemo<Node[]>(() => (flow?.nodes ?? []).map((node) => ({
    id: node.id,
    type: "state",
    data: node,
    position: { x: 0, y: 0 },
    draggable: false,
  })), [flow]);

  const rawEdges = useMemo<Edge[]>(() => (flow?.edges ?? []).map((edge) => ({
    id: edge.id,
    source: edge.source,
    target: edge.target,
    type: "task",
    data: edge,
  })), [flow]);

  useEffect(() => {
    if (rawNodes.length === 0) return;
    layoutVertical(rawNodes, rawEdges).then((layouted) => {
      setNodes(layouted.nodes);
      setEdges(layouted.edges);
    });
  }, [rawNodes, rawEdges]);

  if (loading) return <Skeleton className="h-[680px] w-full" />;
  if (error) {
    return (
      <Alert variant="destructive">
        <AlertDescription>{error}</AlertDescription>
      </Alert>
    );
  }

  return (
    <div className="h-[760px] rounded-md border bg-muted/20">
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        edgeTypes={edgeTypes}
        fitView
        nodesDraggable={false}
        nodesConnectable={false}
        elementsSelectable={false}
      >
        <Background />
        <Controls showInteractive={false} />
      </ReactFlow>
    </div>
  );
}
```

- [ ] **Step 6: Wire route to component**

Replace `ui/app/routes/sources_.$name.flow.tsx` with:

```tsx
import { useOutletContext } from "react-router";
import type { SourceDetailContext } from "~/routes/sources_.$name";
import { Alert, AlertDescription } from "~/components/ui/alert";
import { BrregFlowTab } from "~/components/app/source-detail/flow/BrregFlowTab";

export default function SourceFlowPage() {
  const { source } = useOutletContext<SourceDetailContext>();

  if (source.name !== "brreg") {
    return (
      <Alert>
        <AlertDescription>Flow is available for BRREG only.</AlertDescription>
      </Alert>
    );
  }

  return <BrregFlowTab source={source} />;
}
```

- [ ] **Step 7: Verify frontend**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/ui
pnpm typecheck
pnpm build
```

Expected: both commands exit `0`.

- [ ] **Step 8: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
git add ui/app/types/api.ts ui/app/lib/api.ts ui/app/routes/sources_.'$'name.flow.tsx ui/app/components/app/source-detail/flow ui/package.json ui/pnpm-lock.yaml
git commit -m "feat: render brreg source flow"
```

## Task 7: Raw Inputs URL Filters And Context Actions

**Files:**
- Modify: `ui/app/components/app/RawInputsTable.tsx`
- Modify: `ui/app/lib/api.ts`
- Modify: `ui/app/types/api.ts`

- [ ] **Step 1: Read filters from URL**

In `RawInputsTable.tsx`, import URL helpers:

```tsx
import { useSearchParams } from "react-router";
```

Inside `RawInputsTable`, add:

```tsx
const [searchParams, setSearchParams] = useSearchParams();

const initialStatus = searchParams.get("processing_status") ?? searchParams.get("status") ?? "";
const initialTranslation = searchParams.get("translation_status") ?? "";
const initialHasSuggestion = searchParams.get("has_suggestion") ?? "";
```

Initialize state from these values:

```tsx
const [statusFilter, setStatusFilter] = useState(initialStatus);
const [translationFilter, setTranslationFilter] = useState(initialTranslation);
const [hasSuggestionFilter, setHasSuggestionFilter] = useState(initialHasSuggestion);
```

- [ ] **Step 2: Keep URL in sync**

Add:

```tsx
useEffect(() => {
  const next = new URLSearchParams(searchParams);
  if (statusFilter) next.set("processing_status", statusFilter); else next.delete("processing_status");
  if (translationFilter) next.set("translation_status", translationFilter); else next.delete("translation_status");
  if (hasSuggestionFilter) next.set("has_suggestion", hasSuggestionFilter); else next.delete("has_suggestion");
  setSearchParams(next, { replace: true });
  // eslint-disable-next-line react-hooks/exhaustive-deps
}, [statusFilter, translationFilter, hasSuggestionFilter]);
```

- [ ] **Step 3: Send `has_suggestion` to the API**

Update `load` to accept `hasSuggestion` and call:

```tsx
has_suggestion: hasSuggestion === "" ? undefined : hasSuggestion === "true",
```

Update all calls to `load(...)` to pass `hasSuggestionFilter`.

- [ ] **Step 4: Add filter chip rendering**

Above the table, add:

```tsx
const activeFilterLabels = [
  statusFilter ? `Status: ${statusFilter}` : undefined,
  translationFilter ? `Translation: ${translationFilter}` : undefined,
  hasSuggestionFilter ? `Has suggestion: ${hasSuggestionFilter}` : undefined,
].filter(Boolean) as string[];
```

Render:

```tsx
{activeFilterLabels.length > 0 && (
  <div className="flex flex-wrap gap-2">
    {activeFilterLabels.map((label) => (
      <Badge key={label} variant="outline">{label}</Badge>
    ))}
  </div>
)}
```

- [ ] **Step 5: Replace vague translation actions**

Replace `Translate All` with context-aware labels:

```tsx
const selectedCount = selectedIds.length;
const canTranslateFiltered = showTranslateAction && requiresTranslation && (translationFilter === "pending" || translationFilter === "failed");
const canCreateSuggestions = sourceName === "brreg" && translationFilter === "translated" && statusFilter === "pending" && hasSuggestionFilter === "false";
```

Render buttons:

```tsx
{canTranslateFiltered && (
  <>
    <Button size="sm" disabled={translating || total === 0} onClick={translateFiltered}>
      <Languages className="size-4" />
      {translating ? "Starting..." : `Translate all ${total.toLocaleString()} filtered rows`}
    </Button>
    {selectedCount > 0 && (
      <Button size="sm" disabled={translating} onClick={translateSelected}>
        <Languages className="size-4" />
        Translate {selectedCount.toLocaleString()} selected rows
      </Button>
    )}
  </>
)}
{canCreateSuggestions && (
  <Button size="sm" disabled={total === 0}>
    Move all {total.toLocaleString()} filtered rows to company suggestions
  </Button>
)}
```

For this task, leave the create-suggestions button disabled until a follow-up plan wires the backend endpoint. The visible disabled button verifies the URL-filter boundary without pretending the action exists.

- [ ] **Step 6: Verify**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/ui
pnpm typecheck
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
git add ui/app/components/app/RawInputsTable.tsx ui/app/lib/api.ts ui/app/types/api.ts
git commit -m "feat: drive raw input filters from url"
```

## Task 8: Add Filtered Translation Action Endpoint

**Files:**
- Modify: `scheduler/internal/httpapi/sources.go`
- Modify: `scheduler/internal/httpapi/sources_test.go`
- Modify: `ui/app/lib/api.ts`
- Modify: `ui/app/components/app/RawInputsTable.tsx`

- [ ] **Step 1: Add request type**

In `scheduler/internal/httpapi/sources.go`, extend `translateBrregRequest`:

```go
type translateBrregRequest struct {
	IDs        []string         `json:"ids,omitempty"`
	Filters   map[string]string `json:"filters,omitempty"`
	FXRateDate string           `json:"fx_rate_date,omitempty"`
}
```

- [ ] **Step 2: Validate filter scope**

Add:

```go
func validateBrregTranslationFilters(filters map[string]string) error {
	if len(filters) == 0 {
		return nil
	}
	translationStatus := filters["translation_status"]
	if translationStatus != "pending" && translationStatus != "failed" {
		return errors.New("translation_status must be pending or failed")
	}
	return nil
}
```

Call it in `handleTranslateBrreg` after decoding:

```go
if err := validateBrregTranslationFilters(req.Filters); err != nil {
	writeError(w, http.StatusUnprocessableEntity, "invalid translation filter scope")
	return
}
```

- [ ] **Step 3: Pass filters to Temporal input and ledger**

Update Temporal input:

```go
input := map[string]any{
	"ids":            req.IDs,
	"filters":        req.Filters,
	"prompt_version": envWithDefault("LLM_PROMPT_VERSION", "v1"),
	"model":          envWithDefault("LLM_MODEL", "qwen3:6b"),
	"fx_rate_date":   req.FXRateDate,
}
```

Update `recordSourceActionTask` scope:

```go
map[string]any{
	"ids": req.IDs,
	"filters": req.Filters,
	"fx_rate_date": req.FXRateDate,
}
```

- [ ] **Step 4: Add focused tests**

In `sources_test.go`, add:

```go
func TestTranslateBrreg_rejectsInvalidFilterScope(t *testing.T) {
	tc := &temporalExecuteRecorder{}
	h := httpapi.NewHandlers(&stubQuerier{}, nil, nil, nil, nil, "", tc, "")
	r := routerFor(h)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/sources/brreg/translate", strings.NewReader(`{"filters":{"translation_status":"translated"}}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusUnprocessableEntity, w.Code)
}
```

- [ ] **Step 5: Update frontend API**

In `ui/app/lib/api.ts`, change `translateBrreg` body type:

```ts
translateBrreg: (body: { ids?: string[]; filters?: Record<string, string>; fx_rate_date?: string } = {}) =>
  post<{ status: string; workflow_id: string; workflow_run_id?: string }>("/sources/brreg/translate", body),
```

In `RawInputsTable.tsx`, implement:

```tsx
const translateFiltered = useCallback(async () => {
  if (!translationFilter) return;
  setTranslating(true);
  try {
    await api.translateBrreg({ filters: { translation_status: translationFilter } });
    toast.success(`Translation workflow started for ${total.toLocaleString()} filtered rows.`);
    setRefreshToken((t) => t + 1);
  } catch (err) {
    toast.error(errorMessage(err, "Failed to start filtered translation."));
  } finally {
    setTranslating(false);
  }
}, [translationFilter, total]);
```

- [ ] **Step 6: Verify**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
gofmt -w internal/httpapi/sources.go internal/httpapi/sources_test.go
GOWORK=off go test -count=1 ./internal/httpapi -run 'TestTranslateBrreg'

cd /Users/graovic/pulsarpoint/ppoint/corpscout/ui
pnpm typecheck
```

Expected: both pass.

- [ ] **Step 7: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
git add scheduler/internal/httpapi/sources.go scheduler/internal/httpapi/sources_test.go ui/app/lib/api.ts ui/app/components/app/RawInputsTable.tsx
git commit -m "feat: translate filtered brreg raw inputs"
```

## Task 9: Final Verification And Browser Check

**Files:**
- No new source files.

- [ ] **Step 1: Run backend tests**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test -count=1 ./...
```

Expected: all packages pass.

- [ ] **Step 2: Run frontend checks**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/ui
pnpm typecheck
pnpm build
```

Expected: both commands exit `0`. Existing shadcn sourcemap warnings are acceptable if the build exits `0`.

- [ ] **Step 3: Rebuild local services**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
docker compose build scheduler ui
docker compose up -d scheduler ui
```

Expected: `corpscout-scheduler-1` and `corpscout-ui-1` are running.

- [ ] **Step 4: Browser verification**

Open:

```text
http://localhost:8094/sources/brreg/flow
```

Verify:

- Flow tab appears only for BRREG.
- Graph is vertical.
- BRREG source node shows last downloaded and next scheduled pull.
- Raw input and translation state nodes show counts.
- Count links navigate to `/sources/brreg/raw_input` with expected query params.
- The graph has no execution buttons.
- Raw Inputs shows filter chips after navigation.
- Raw Inputs shows `Translate all N filtered rows` only for pending/failed translation filters.

- [ ] **Step 5: Commit build artifacts if this repo tracks them**

If `pnpm build` changed tracked files under `ui/build/client`, include them:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
git add ui/build/client
git commit -m "chore: update ui build artifacts"
```

If no tracked build artifacts changed, skip this commit.

- [ ] **Step 6: Final status**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
git status --short
```

Expected: only unrelated pre-existing worktree changes remain.

## Self-Review

- Spec coverage: The plan covers BRREG-only Flow, React Flow + ELKJS vertical layout, link-only graph behavior, Raw Inputs URL filters, filtered translation action, local source action task ledger, task counters, error handling, and verification.
- Deliberate first-version limit: The create-company-suggestions action is surfaced as a disabled context action in Task 7 but not executed until a follow-up plan, because current code has a direct raw-input-to-company approval path and needs a separate service design to reintroduce company-suggestion creation safely.
- Placeholder scan: No unfinished markers or unspecified implementation steps remain.
- Type consistency: The plan uses `SourceFlowResponse`, `SourceFlowNode`, `SourceFlowEdge`, `SourceFlowCount`, and `SourceFlowTaskCounter` consistently across backend and frontend tasks.
