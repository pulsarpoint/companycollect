# BRREG Temporal Simplification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Simplify BRREG Temporal execution so HTTP starts concrete workflows, workflows prepare their own BRREG selections, and long-running domain discovery runs through bounded per-company activities.

**Architecture:** `tasksvc` becomes a thin Temporal starter and no longer creates BRREG workflow audit or task-selection rows. `brreg/temporal` owns prepare activities, batch workflows, the domain parent workflow, and per-company domain activities. `brreg/db` owns transactional preparation, claim, submit, and finish methods behind a typed gateway.

**Tech Stack:** Go, Temporal Go SDK, pgx/sqlc, pgxmock, `github.com/cockroachdb/errors`, `log/slog`.

---

## File Structure

- Modify `scheduler/internal/tasksvc/brreg_translation_starter.go`: direct `ExecuteWorkflow` start for translation.
- Modify `scheduler/internal/tasksvc/brreg_financial_starter.go`: direct `ExecuteWorkflow` start for financial conversion.
- Modify `scheduler/internal/tasksvc/brreg_domain_starter.go`: direct `ExecuteWorkflow` start for domain discovery.
- Modify `scheduler/internal/tasksvc/brreg_starter.go`: keep workflow ID, memo scope, env helper utilities that are still useful for direct starts; remove task-selection hash responsibility after callers stop using it.
- Modify `scheduler/internal/tasksvc/brreg_action_starter.go`: remove after all action starters stop using `brregActionWorkflowStart`.
- Modify `scheduler/internal/tasksvc/brreg_workflow_start.go`: remove after direct starters no longer use selected-workflow start helpers.
- Modify `scheduler/internal/tasksvc/brreg_task_selection.go`: move needed selection payload structs/logic into BRREG preparation gateway/activity, then remove from `tasksvc`.
- Modify `scheduler/internal/tasksvc/starter_test.go`: replace tests that assert generic starter indirection with tests for direct Temporal starts.
- Modify `scheduler/internal/brreg/db/types.go`: add preparation command/result DTOs and domain company page DTOs.
- Create `scheduler/internal/brreg/db/workflow_prepare.go`: transactional workflow-run and task-selection creation.
- Modify `scheduler/internal/brreg/db/claim.go`: add `ClaimDomainCompanyPage` as a page-oriented wrapper around selected domain task claims.
- Modify `scheduler/internal/brreg/db/gateway_test.go`: add preparation and claim tests.
- Modify `scheduler/internal/brreg/temporal/activities.go`: add prepare gateway methods to activity dependencies.
- Create `scheduler/internal/brreg/temporal/workflow_prepare_activity.go`: prepare activities for translation, financial conversion, and domain discovery.
- Create `scheduler/internal/brreg/temporal/workflow_prepare_activity_test.go`: prepare activity tests.
- Modify `scheduler/internal/brreg/temporal/translation.go`: workflow calls prepare activity before batch loop.
- Modify `scheduler/internal/brreg/temporal/financial.go`: workflow calls prepare activity before batch loop.
- Modify `scheduler/internal/brreg/temporal/domain.go`: parent workflow prepares selection, runs bounded one-company activities, and continues as new.
- Create `scheduler/internal/brreg/temporal/domain_company_activity.go`: one-company domain discovery activity and input/result types.
- Modify `scheduler/internal/brreg/temporal/domain_activity.go`: replace batch-level domain processing with page claiming for the parent workflow.
- Modify `scheduler/internal/brreg/temporal/domain_test.go`: parent concurrency and continue-as-new tests.
- Create `scheduler/internal/brreg/temporal/domain_company_activity_test.go`: one-company activity tests.
- Modify `scheduler/internal/app/temporal.go`: register the new one-company domain activity and new prepare activities.
- Modify `scheduler/internal/app/temporal_test.go`: assert BRREG workflow and activity registrations.

---

### Task 1: Add Concrete Workflow Selection Inputs

**Files:**
- Modify: `scheduler/internal/brreg/temporal/translation.go`
- Modify: `scheduler/internal/brreg/temporal/financial.go`
- Modify: `scheduler/internal/brreg/temporal/domain.go`
- Create: `scheduler/internal/brreg/temporal/workflow_selection.go`
- Create: `scheduler/internal/brreg/temporal/workflow_selection_test.go`

- [ ] **Step 1: Write failing tests for workflow input selection fields**

Create `scheduler/internal/brreg/temporal/workflow_selection_test.go`:

```go
package brregtemporal

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWorkflowSelectionInputPreservesRequestScope(t *testing.T) {
	input := BrregWorkflowSelectionInput{
		Trigger:     "manual",
		IDs:         []string{"id-1", "id-2"},
		Filters:     map[string]string{"translation_status": "not_started"},
		Limit:       100,
		BatchSize:   25,
		MaxAttempts: 4,
	}

	scope := input.MemoScope()

	require.Equal(t, "manual", scope["trigger"])
	require.Equal(t, []string{"id-1", "id-2"}, scope["ids"])
	require.Equal(t, map[string]string{"translation_status": "not_started"}, scope["filters"])
	require.Equal(t, int32(100), scope["limit"])
	require.Equal(t, int32(25), scope["batch_size"])
	require.Equal(t, int32(4), scope["max_attempts"])
}

func TestTranslationWorkflowInputHasSelectionInput(t *testing.T) {
	input := BrregTranslateWorkflowInput{
		Selection: BrregWorkflowSelectionInput{
			Trigger: "manual",
			Limit:   1000,
		},
	}

	require.Equal(t, "manual", input.Selection.Trigger)
	require.Equal(t, int32(1000), input.Selection.Limit)
}

func TestDomainWorkflowInputHasConcurrencyControls(t *testing.T) {
	input := BrregDomainWorkflowInput{
		MaxParallelCompanyActivities: 20,
		MaxClaimPageSize:            100,
		ContinueAsNewAfterCompanies: 1000,
	}

	require.Equal(t, int32(20), input.MaxParallelCompanyActivities)
	require.Equal(t, int32(100), input.MaxClaimPageSize)
	require.Equal(t, int32(1000), input.ContinueAsNewAfterCompanies)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./internal/brreg/temporal -run 'TestWorkflowSelectionInputPreservesRequestScope|TestTranslationWorkflowInputHasSelectionInput|TestDomainWorkflowInputHasConcurrencyControls' -count=1
```

Expected: FAIL because `BrregWorkflowSelectionInput`, `Selection`, and domain company activity concurrency fields do not exist.

- [ ] **Step 3: Add shared workflow selection input**

Create `scheduler/internal/brreg/temporal/workflow_selection.go`:

```go
package brregtemporal

type BrregWorkflowSelectionInput struct {
	Trigger     string            `json:"trigger,omitempty"`
	IDs         []string          `json:"ids,omitempty"`
	Filters     map[string]string `json:"filters,omitempty"`
	Limit       int32             `json:"limit,omitempty"`
	BatchSize   int32             `json:"batch_size,omitempty"`
	MaxAttempts int32             `json:"max_attempts,omitempty"`
}

func (input BrregWorkflowSelectionInput) MemoScope() map[string]any {
	return map[string]any{
		"trigger":      input.Trigger,
		"ids":          input.IDs,
		"filters":      input.Filters,
		"limit":        input.Limit,
		"batch_size":   input.BatchSize,
		"max_attempts": input.MaxAttempts,
	}
}
```

Modify `scheduler/internal/brreg/temporal/translation.go`:

```go
type BrregTranslateWorkflowInput struct {
	WorkflowRunID             string                      `json:"workflow_run_id,omitempty"`
	SelectionHash             string                      `json:"selection_hash,omitempty"`
	TemporalWorkflowID        string                      `json:"temporal_workflow_id,omitempty"`
	Selection                 BrregWorkflowSelectionInput `json:"selection,omitempty"`
	BatchSize                 int32                       `json:"batch_size,omitempty"`
	MaxParallelTasks          int32                       `json:"max_parallel_tasks,omitempty"`
	LeaseSeconds              int32                       `json:"lease_seconds,omitempty"`
	MaxTaskAttempts           int32                       `json:"max_task_attempts,omitempty"`
	MaxBatches                int32                       `json:"max_batches,omitempty"`
	ContinueAsNewAfterBatches int32                       `json:"continue_as_new_after_batches,omitempty"`
	Provider                  string                      `json:"provider,omitempty"`
	Model                     string                      `json:"model,omitempty"`
	PromptVersion             string                      `json:"prompt_version,omitempty"`
	SourceLang                string                      `json:"source_lang,omitempty"`
	TargetLang                string                      `json:"target_lang,omitempty"`
	MaxRetries                int                         `json:"max_retries,omitempty"`
}
```

Modify `scheduler/internal/brreg/temporal/financial.go`:

```go
type BrregConvertFinancialsWorkflowInput struct {
	WorkflowRunID             string                      `json:"workflow_run_id,omitempty"`
	SelectionHash             string                      `json:"selection_hash,omitempty"`
	TemporalWorkflowID        string                      `json:"temporal_workflow_id,omitempty"`
	Selection                 BrregWorkflowSelectionInput `json:"selection,omitempty"`
	BatchSize                 int32                       `json:"batch_size,omitempty"`
	MaxParallelTasks          int32                       `json:"max_parallel_tasks,omitempty"`
	LeaseSeconds              int32                       `json:"lease_seconds,omitempty"`
	MaxTaskAttempts           int32                       `json:"max_task_attempts,omitempty"`
	MaxBatches                int32                       `json:"max_batches,omitempty"`
	ContinueAsNewAfterBatches int32                       `json:"continue_as_new_after_batches,omitempty"`
	FXRateDate                string                      `json:"fx_rate_date,omitempty"`
}
```

Modify `scheduler/internal/brreg/temporal/domain.go`:

```go
type BrregDomainWorkflowInput struct {
	WorkflowRunID               string                                      `json:"workflow_run_id,omitempty"`
	SelectionHash               string                                      `json:"selection_hash,omitempty"`
	TemporalWorkflowID          string                                      `json:"temporal_workflow_id,omitempty"`
	Selection                   BrregWorkflowSelectionInput                 `json:"selection,omitempty"`
	BatchSize                   int32                                       `json:"batch_size,omitempty"`
	MaxParallelTasks            int32                                       `json:"max_parallel_tasks,omitempty"`
	LeaseSeconds                int32                                       `json:"lease_seconds,omitempty"`
	MaxTaskAttempts             int32                                       `json:"max_task_attempts,omitempty"`
	MaxBatches                  int32                                       `json:"max_batches,omitempty"`
	ContinueAsNewAfterBatches   int32                                       `json:"continue_as_new_after_batches,omitempty"`
	MaxParallelCompanyActivities int32                                     `json:"max_parallel_company_activities,omitempty"`
	MaxClaimPageSize             int32                                     `json:"max_claim_page_size,omitempty"`
	ContinueAsNewAfterCompanies int32                                     `json:"continue_as_new_after_companies,omitempty"`
	SearchProvider              string                                      `json:"search_provider,omitempty"`
	PromptVersion               string                                      `json:"prompt_version,omitempty"`
	Limits                      crawlserviceclient.DomainDiscoverLimits     `json:"limits,omitempty"`
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
gofmt -w internal/brreg/temporal/workflow_selection.go internal/brreg/temporal/workflow_selection_test.go internal/brreg/temporal/translation.go internal/brreg/temporal/financial.go internal/brreg/temporal/domain.go
GOWORK=off go test ./internal/brreg/temporal -run 'TestWorkflowSelectionInputPreservesRequestScope|TestTranslationWorkflowInputHasSelectionInput|TestDomainWorkflowInputHasConcurrencyControls' -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
git add scheduler/internal/brreg/temporal/workflow_selection.go scheduler/internal/brreg/temporal/workflow_selection_test.go scheduler/internal/brreg/temporal/translation.go scheduler/internal/brreg/temporal/financial.go scheduler/internal/brreg/temporal/domain.go
git commit -m "Add BRREG workflow selection inputs"
```

---

### Task 2: Move Workflow Preparation Into BRREG DB Gateway

**Files:**
- Modify: `scheduler/internal/brreg/db/types.go`
- Create: `scheduler/internal/brreg/db/workflow_prepare.go`
- Modify: `scheduler/internal/brreg/db/gateway_test.go`

- [ ] **Step 1: Write failing gateway tests**

Append to `scheduler/internal/brreg/db/gateway_test.go`:

```go
func TestGatewayPrepareWorkflowCreatesRunAndSelection(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	workflowRunID := uuid.New()
	gateway := NewGateway(pool)

	pool.ExpectQuery("INSERT INTO brreg_workflow.workflow_runs").
		WithArgs(anyArgs(4)...).
		WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(workflowRunID))
	pool.ExpectQuery("WITH selected AS").
		WithArgs(anyArgs(10)...).
		WillReturnRows(pgxmock.NewRows([]string{"id", "selection_hash", "records_selected"}).
			AddRow(uuid.New(), "selection-hash", int32(42)))

	prepared, err := gateway.PrepareWorkflow(context.Background(), PrepareWorkflowCommand{
		Source:       "brreg",
		Action:       "translate",
		TaskType:     TaskTypeTranslate,
		Trigger:      "manual",
		WorkflowID:   "translate-brreg-all",
		IDs:          []string{},
		Filters:      map[string]string{"translation_status": "not_started"},
		Limit:        1000,
		BatchSize:    50,
		MaxAttempts:  3,
		DefaultLimit: 1000,
	})

	require.NoError(t, err)
	require.Equal(t, workflowRunID, prepared.WorkflowRunID)
	require.Equal(t, "selection-hash", prepared.SelectionHash)
	require.Equal(t, int32(42), prepared.RecordsSelected)
	require.Equal(t, int32(50), prepared.BatchSize)
	require.Equal(t, int32(3), prepared.MaxAttempts)
	require.NoError(t, pool.ExpectationsWereMet())
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./internal/brreg/db -run TestGatewayPrepareWorkflowCreatesRunAndSelection -count=1
```

Expected: FAIL because `PrepareWorkflowCommand`, `PreparedWorkflow`, and `PrepareWorkflow` do not exist.

- [ ] **Step 3: Add preparation DTOs**

Add to `scheduler/internal/brreg/db/types.go`:

```go
type PrepareWorkflowCommand struct {
	Source             string
	Action             string
	TaskType           TaskType
	Trigger            string
	WorkflowID         string
	IDs                []string
	Filters            map[string]string
	Limit              int32
	BatchSize          int32
	MaxAttempts        int32
	DefaultLimit       int32
	DefaultBatchSize   int32
	DefaultMaxAttempts int32
}

type PreparedWorkflow struct {
	WorkflowRunID   uuid.UUID
	SelectionHash   string
	RecordsSelected int32
	BatchSize       int32
	MaxAttempts     int32
}
```

- [ ] **Step 4: Add gateway preparation implementation**

Create `scheduler/internal/brreg/db/workflow_prepare.go`:

```go
package brregdb

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/pulsarpoint/corpscout/scheduler/internal/brreg/rawselection"
	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

const (
	defaultSelectionLimit     int32 = 1000
	defaultSelectionBatchSize int32 = 50
)

func (g *Gateway) PrepareWorkflow(ctx context.Context, command PrepareWorkflowCommand) (PreparedWorkflow, error) {
	if g.pool == nil {
		return PreparedWorkflow{}, errors.New("brreg workflow database pool not available")
	}
	ids, filters, err := rawselection.Normalize(command.IDs, command.Filters)
	if err != nil {
		return PreparedWorkflow{}, err
	}
	batchSize := configuredInt32(command.BatchSize, command.DefaultBatchSize, defaultSelectionBatchSize)
	maxAttempts := configuredInt32(command.MaxAttempts, command.DefaultMaxAttempts, g.maxAttempts)
	limit := configuredInt32(command.Limit, command.DefaultLimit, defaultSelectionLimit)
	if len(ids) > 0 && limit > int32(len(ids)) {
		limit = int32(len(ids))
	}
	selectionHash := prepareSelectionHash(command.Source, command.TaskType.String(), command.WorkflowID, time.Now())
	selectionMode := prepareSelectionMode(ids, filters)

	runMetadata, err := json.Marshal(map[string]any{
		"source":               command.Source,
		"action":               command.Action,
		"trigger":              command.Trigger,
		"temporal_workflow_id": command.WorkflowID,
		"selection_hash":       selectionHash,
		"selection_mode":       selectionMode,
	})
	if err != nil {
		return PreparedWorkflow{}, errors.Wrap(err, "marshal brreg workflow run metadata")
	}
	selectionDefinition, err := json.Marshal(map[string]any{
		"source":       command.Source,
		"task_type":    command.TaskType.String(),
		"mode":         selectionMode,
		"raw_records":  "current",
		"ids_count":    len(ids),
		"filters":      filters.Map(),
		"limit":        limit,
		"batch_size":   batchSize,
		"max_attempts": maxAttempts,
	})
	if err != nil {
		return PreparedWorkflow{}, errors.Wrap(err, "marshal brreg task selection definition")
	}

	q := db.New(g.pool)
	workflowRunID, err := q.BeginBrregWorkflowRun(ctx, db.BeginBrregWorkflowRunParams{
		Orchestrator:      stringPointer("temporal"),
		OrchestratorRunID: fmt.Sprintf("%s:%s", command.WorkflowID, selectionHash),
		RunType:           command.Action,
		Metadata:          runMetadata,
	})
	if err != nil {
		return PreparedWorkflow{}, errors.Wrapf(err, "begin brreg %s workflow run", command.Action)
	}
	selection, err := q.CreateBrregWorkflowTaskSelection(ctx, db.CreateBrregWorkflowTaskSelectionParams{
		WorkflowRunID:       workflowRunID,
		TaskType:            command.TaskType.String(),
		SelectionHash:       selectionHash,
		SelectionDefinition: selectionDefinition,
		SelectedIds:         ids,
		Query:               filters.Query,
		LifecycleState:      filters.LifecycleState,
		TranslationStatus:   filters.TranslationStatus,
		DomainStatus:        filters.DomainStatus,
		FinancialStatus:     filters.FinancialStatus,
		EnhancedStatus:      filters.EnhancedStatus,
		MaxAttempts:         maxAttempts,
		Limit:               limit,
	})
	if err != nil {
		return PreparedWorkflow{}, errors.Wrapf(err, "create brreg %s task selection", command.Action)
	}
	return PreparedWorkflow{
		WorkflowRunID:   workflowRunID,
		SelectionHash:   selection.SelectionHash,
		RecordsSelected: selection.RecordsSelected,
		BatchSize:       batchSize,
		MaxAttempts:     maxAttempts,
	}, nil
}

func configuredInt32(overrideValue, configuredDefault, builtinDefault int32) int32 {
	if overrideValue > 0 {
		return overrideValue
	}
	if configuredDefault > 0 {
		return configuredDefault
	}
	return builtinDefault
}

func prepareSelectionMode(ids []string, filters rawselection.Filters) string {
	if len(ids) > 0 {
		return "ids"
	}
	if len(filters.Map()) > 0 {
		return "filters"
	}
	return "limit"
}

func prepareSelectionHash(source, taskType, workflowID string, now time.Time) string {
	seed := fmt.Sprintf("%s:%s:%s:%d", source, taskType, workflowID, now.UnixNano())
	sum := sha256.Sum256([]byte(seed))
	return hex.EncodeToString(sum[:])
}

func stringPointer(value string) *string {
	return &value
}
```

- [ ] **Step 5: Run tests to verify pass**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
gofmt -w internal/brreg/db/types.go internal/brreg/db/workflow_prepare.go internal/brreg/db/gateway_test.go
GOWORK=off go test ./internal/brreg/db -run TestGatewayPrepareWorkflowCreatesRunAndSelection -count=1
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
git add scheduler/internal/brreg/db/types.go scheduler/internal/brreg/db/workflow_prepare.go scheduler/internal/brreg/db/gateway_test.go
git commit -m "Move BRREG workflow preparation into gateway"
```

---

### Task 3: Add Prepare Activities

**Files:**
- Modify: `scheduler/internal/brreg/temporal/activities.go`
- Create: `scheduler/internal/brreg/temporal/workflow_prepare_activity.go`
- Create: `scheduler/internal/brreg/temporal/workflow_prepare_activity_test.go`

- [ ] **Step 1: Write failing activity tests**

Create `scheduler/internal/brreg/temporal/workflow_prepare_activity_test.go`:

```go
package brregtemporal

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	brregdb "github.com/pulsarpoint/corpscout/scheduler/internal/brreg/db"
)

func TestPrepareTranslationWorkflowActivityCallsGateway(t *testing.T) {
	workflowRunID := uuid.New()
	gateway := &fakePrepareGateway{
		prepared: brregdb.PreparedWorkflow{
			WorkflowRunID:   workflowRunID,
			SelectionHash:   "selection-hash",
			RecordsSelected: 10,
			BatchSize:       50,
			MaxAttempts:     3,
		},
	}
	activities := NewActivities(gateway, nil)

	result, err := activities.PrepareBrregTranslationWorkflow(context.Background(), BrregTranslateWorkflowInput{
		Selection: BrregWorkflowSelectionInput{
			Trigger: "manual",
			Limit:   1000,
		},
	})

	require.NoError(t, err)
	require.Equal(t, workflowRunID.String(), result.WorkflowRunID)
	require.Equal(t, "selection-hash", result.SelectionHash)
	require.Equal(t, brregdb.TaskTypeTranslate, gateway.command.TaskType)
	require.Equal(t, "manual", gateway.command.Trigger)
}

type fakePrepareGateway struct {
	ActivityGateway
	command  brregdb.PrepareWorkflowCommand
	prepared brregdb.PreparedWorkflow
	err      error
}

func (f *fakePrepareGateway) PrepareWorkflow(_ context.Context, command brregdb.PrepareWorkflowCommand) (brregdb.PreparedWorkflow, error) {
	f.command = command
	return f.prepared, f.err
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./internal/brreg/temporal -run TestPrepareTranslationWorkflowActivityCallsGateway -count=1
```

Expected: FAIL because `PrepareBrregTranslationWorkflow` and `PrepareWorkflow` dependency do not exist.

- [ ] **Step 3: Add prepare dependency and activities**

Modify `scheduler/internal/brreg/temporal/activities.go`:

```go
type PrepareWorkflowGateway interface {
	PrepareWorkflow(context.Context, brregdb.PrepareWorkflowCommand) (brregdb.PreparedWorkflow, error)
}

type ActivityGateway interface {
	PrepareWorkflowGateway
	TranslationGateway
	DomainGateway
	FinancialGateway
	WorkflowRunGateway
}
```

Create `scheduler/internal/brreg/temporal/workflow_prepare_activity.go`:

```go
package brregtemporal

import (
	"context"

	"github.com/cockroachdb/errors"

	brregdb "github.com/pulsarpoint/corpscout/scheduler/internal/brreg/db"
)

type PreparedBrregWorkflow struct {
	WorkflowRunID   string `json:"workflow_run_id"`
	SelectionHash   string `json:"selection_hash"`
	RecordsSelected int32  `json:"records_selected"`
	BatchSize       int32  `json:"batch_size"`
	MaxAttempts     int32  `json:"max_attempts"`
}

func (a *Activities) PrepareBrregTranslationWorkflow(ctx context.Context, input BrregTranslateWorkflowInput) (PreparedBrregWorkflow, error) {
	return a.prepareWorkflow(ctx, brregdb.PrepareWorkflowCommand{
		Source:             "brreg",
		Action:             brregWorkflowLabelTranslation,
		TaskType:           brregdb.TaskTypeTranslate,
		Trigger:            input.Selection.Trigger,
		WorkflowID:         input.WorkflowID(),
		IDs:                input.Selection.IDs,
		Filters:            input.Selection.Filters,
		Limit:              input.Selection.Limit,
		BatchSize:          input.Selection.BatchSize,
		MaxAttempts:        input.Selection.MaxAttempts,
		DefaultLimit:       1000,
		DefaultBatchSize:   50,
		DefaultMaxAttempts: 3,
	})
}

func (a *Activities) PrepareBrregFinancialWorkflow(ctx context.Context, input BrregConvertFinancialsWorkflowInput) (PreparedBrregWorkflow, error) {
	return a.prepareWorkflow(ctx, brregdb.PrepareWorkflowCommand{
		Source:             "brreg",
		Action:             brregWorkflowLabelFinancial,
		TaskType:           brregdb.TaskTypeConvertFinancials,
		Trigger:            input.Selection.Trigger,
		WorkflowID:         input.WorkflowID(),
		IDs:                input.Selection.IDs,
		Filters:            input.Selection.Filters,
		Limit:              input.Selection.Limit,
		BatchSize:          input.Selection.BatchSize,
		MaxAttempts:        input.Selection.MaxAttempts,
		DefaultLimit:       1000,
		DefaultBatchSize:   500,
		DefaultMaxAttempts: 3,
	})
}

func (a *Activities) PrepareBrregDomainWorkflow(ctx context.Context, input BrregDomainWorkflowInput) (PreparedBrregWorkflow, error) {
	return a.prepareWorkflow(ctx, brregdb.PrepareWorkflowCommand{
		Source:             "brreg",
		Action:             brregWorkflowLabelDomain,
		TaskType:           brregdb.TaskTypeDiscoverDomains,
		Trigger:            input.Selection.Trigger,
		WorkflowID:         input.WorkflowID(),
		IDs:                input.Selection.IDs,
		Filters:            input.Selection.Filters,
		Limit:              input.Selection.Limit,
		BatchSize:          input.Selection.BatchSize,
		MaxAttempts:        input.Selection.MaxAttempts,
		DefaultLimit:       1000,
		DefaultBatchSize:   10,
		DefaultMaxAttempts: 3,
	})
}

func (a *Activities) prepareWorkflow(ctx context.Context, command brregdb.PrepareWorkflowCommand) (PreparedBrregWorkflow, error) {
	if a == nil || a.gateway == nil {
		return PreparedBrregWorkflow{}, errors.New("brreg workflow gateway not available")
	}
	prepared, err := a.gateway.PrepareWorkflow(ctx, command)
	if err != nil {
		return PreparedBrregWorkflow{}, err
	}
	return PreparedBrregWorkflow{
		WorkflowRunID:   prepared.WorkflowRunID.String(),
		SelectionHash:   prepared.SelectionHash,
		RecordsSelected: prepared.RecordsSelected,
		BatchSize:       prepared.BatchSize,
		MaxAttempts:     prepared.MaxAttempts,
	}, nil
}
```

Add workflow ID helper methods in the existing workflow input files:

```go
func (input BrregTranslateWorkflowInput) WorkflowID() string {
	if input.TemporalWorkflowID != "" {
		return input.TemporalWorkflowID
	}
	return "translate-brreg-all"
}
```

Add the financial helper in `scheduler/internal/brreg/temporal/financial.go`:

```go
func (input BrregConvertFinancialsWorkflowInput) WorkflowID() string {
	if input.TemporalWorkflowID != "" {
		return input.TemporalWorkflowID
	}
	return "convert-financials-brreg-all"
}
```

Add the domain helper in `scheduler/internal/brreg/temporal/domain.go`:

```go
func (input BrregDomainWorkflowInput) WorkflowID() string {
	if input.TemporalWorkflowID != "" {
		return input.TemporalWorkflowID
	}
	return "discover-domains-brreg-all"
}
```

- [ ] **Step 4: Run tests to verify pass**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
gofmt -w internal/brreg/temporal/activities.go internal/brreg/temporal/workflow_prepare_activity.go internal/brreg/temporal/workflow_prepare_activity_test.go internal/brreg/temporal/translation.go internal/brreg/temporal/financial.go internal/brreg/temporal/domain.go
GOWORK=off go test ./internal/brreg/temporal -run TestPrepareTranslationWorkflowActivityCallsGateway -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
git add scheduler/internal/brreg/temporal/activities.go scheduler/internal/brreg/temporal/workflow_prepare_activity.go scheduler/internal/brreg/temporal/workflow_prepare_activity_test.go scheduler/internal/brreg/temporal/translation.go scheduler/internal/brreg/temporal/financial.go scheduler/internal/brreg/temporal/domain.go
git commit -m "Add BRREG workflow prepare activities"
```

---

### Task 4: Make Translation and Financial Workflows Prepare Themselves

**Files:**
- Modify: `scheduler/internal/brreg/temporal/translation.go`
- Modify: `scheduler/internal/brreg/temporal/financial.go`
- Modify: `scheduler/internal/brreg/temporal/translation_test.go`
- Modify: `scheduler/internal/brreg/temporal/financial_test.go`

- [ ] **Step 1: Write failing workflow tests**

Update translation workflow tests so the workflow starts without `WorkflowRunID` and `SelectionHash` in the initial input:

```go
func TestTranslateBrregRawInputsWorkflowPreparesSelectionBeforeBatches(t *testing.T) {
	env := testsuite.WorkflowTestSuite{}.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(TranslateBrregRawInputs)
	env.RegisterActivityWithOptions(
		func(context.Context, BrregTranslateWorkflowInput) (PreparedBrregWorkflow, error) {
			return PreparedBrregWorkflow{
				WorkflowRunID:   uuid.NewString(),
				SelectionHash:   "selection-hash",
				RecordsSelected: 1,
				BatchSize:       50,
				MaxAttempts:     3,
			}, nil
		},
		activity.RegisterOptions{Name: PrepareBrregTranslationWorkflowActivityName},
	)
	env.RegisterActivityWithOptions(
		func(context.Context, BrregTranslateWorkflowInput) (BrregTranslateBatchResult, error) {
			return BrregTranslateBatchResult{}, nil
		},
		activity.RegisterOptions{Name: TranslateNextBrregBatchActivityName},
	)
	env.RegisterActivityWithOptions(
		func(context.Context, FinishBrregWorkflowRunInput) error { return nil },
		activity.RegisterOptions{Name: FinishBrregWorkflowRunActivityName},
	)

	env.ExecuteWorkflow(TranslateBrregRawInputs, BrregTranslateWorkflowInput{
		Selection: BrregWorkflowSelectionInput{Trigger: "manual", Limit: 1000},
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
}
```

Add to `scheduler/internal/brreg/temporal/financial_test.go`:

```go
func TestConvertBrregFinancialsWorkflowPreparesSelectionBeforeBatches(t *testing.T) {
	env := testsuite.WorkflowTestSuite{}.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(ConvertBrregFinancials)
	env.RegisterActivityWithOptions(
		func(context.Context, BrregConvertFinancialsWorkflowInput) (PreparedBrregWorkflow, error) {
			return PreparedBrregWorkflow{
				WorkflowRunID:   uuid.NewString(),
				SelectionHash:   "selection-hash",
				RecordsSelected: 1,
				BatchSize:       500,
				MaxAttempts:     3,
			}, nil
		},
		activity.RegisterOptions{Name: PrepareBrregFinancialWorkflowActivityName},
	)
	env.RegisterActivityWithOptions(
		func(context.Context, BrregConvertFinancialsWorkflowInput) (BrregConvertFinancialsBatchResult, error) {
			return BrregConvertFinancialsBatchResult{}, nil
		},
		activity.RegisterOptions{Name: ConvertNextBrregFinancialBatchActivityName},
	)
	env.RegisterActivityWithOptions(
		func(context.Context, FinishBrregWorkflowRunInput) error { return nil },
		activity.RegisterOptions{Name: FinishBrregWorkflowRunActivityName},
	)

	env.ExecuteWorkflow(ConvertBrregFinancials, BrregConvertFinancialsWorkflowInput{
		Selection: BrregWorkflowSelectionInput{Trigger: "manual", Limit: 1000},
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./internal/brreg/temporal -run 'TestTranslateBrregRawInputsWorkflowPreparesSelectionBeforeBatches|TestConvertBrregFinancialsWorkflowPreparesSelectionBeforeBatches' -count=1
```

Expected: FAIL because workflows still expect already prepared `WorkflowRunID` and `SelectionHash`.

- [ ] **Step 3: Update workflows to call prepare activity**

In `scheduler/internal/brreg/temporal/translation.go`, define:

```go
const PrepareBrregTranslationWorkflowActivityName = "PrepareBrregTranslationWorkflow"
```

Then update `TranslateBrregRawInputs`:

```go
func TranslateBrregRawInputs(ctx workflow.Context, input BrregTranslateWorkflowInput) (BrregTranslateWorkflowResult, error) {
	input = input.withDefaults()
	ctx = workflow.WithActivityOptions(ctx, brregBatchActivityOptions(30*time.Minute))

	var prepared PreparedBrregWorkflow
	if input.WorkflowRunID == "" || input.SelectionHash == "" {
		if err := workflow.ExecuteActivity(ctx, PrepareBrregTranslationWorkflowActivityName, input).Get(ctx, &prepared); err != nil {
			return BrregTranslateWorkflowResult{}, err
		}
		input.WorkflowRunID = prepared.WorkflowRunID
		input.SelectionHash = prepared.SelectionHash
		input.BatchSize = prepared.BatchSize
		input.MaxTaskAttempts = prepared.MaxAttempts
	}

	progress, err := runBrregBatchWorkflow[BrregTranslateWorkflowInput, BrregTranslateBatchResult](ctx, brregWorkflowRunConfig[BrregTranslateWorkflowInput]{
		WorkflowName:              brregWorkflowLabelTranslation,
		WorkflowRunID:             input.WorkflowRunID,
		MaxTaskAttempts:           input.MaxTaskAttempts,
		MaxBatches:                input.MaxBatches,
		ContinueAsNewAfterBatches: input.ContinueAsNewAfterBatches,
		ContinueAsNewWorkflow:     TranslateBrregRawInputs,
		ActivityName:              TranslateNextBrregBatchActivityName,
		Input:                     input,
	})
	return BrregTranslateWorkflowResult(progress), err
}
```

In `scheduler/internal/brreg/temporal/financial.go`, define:

```go
const PrepareBrregFinancialWorkflowActivityName = "PrepareBrregFinancialWorkflow"
```

Then update `ConvertBrregFinancials`:

```go
func ConvertBrregFinancials(ctx workflow.Context, input BrregConvertFinancialsWorkflowInput) (BrregConvertFinancialsWorkflowResult, error) {
	input = input.withDefaults()
	ctx = workflow.WithActivityOptions(ctx, brregBatchActivityOptions(10*time.Minute))

	var prepared PreparedBrregWorkflow
	if input.WorkflowRunID == "" || input.SelectionHash == "" {
		if err := workflow.ExecuteActivity(ctx, PrepareBrregFinancialWorkflowActivityName, input).Get(ctx, &prepared); err != nil {
			return BrregConvertFinancialsWorkflowResult{}, err
		}
		input.WorkflowRunID = prepared.WorkflowRunID
		input.SelectionHash = prepared.SelectionHash
		input.BatchSize = prepared.BatchSize
		input.MaxTaskAttempts = prepared.MaxAttempts
	}

	progress, err := runBrregBatchWorkflow[BrregConvertFinancialsWorkflowInput, BrregConvertFinancialsBatchResult](ctx, brregWorkflowRunConfig[BrregConvertFinancialsWorkflowInput]{
		WorkflowName:              brregWorkflowLabelFinancial,
		WorkflowRunID:             input.WorkflowRunID,
		MaxTaskAttempts:           input.MaxTaskAttempts,
		MaxBatches:                input.MaxBatches,
		ContinueAsNewAfterBatches: input.ContinueAsNewAfterBatches,
		ContinueAsNewWorkflow:     ConvertBrregFinancials,
		ActivityName:              ConvertNextBrregFinancialBatchActivityName,
		Input:                     input,
	})
	return BrregConvertFinancialsWorkflowResult(progress), err
}
```

- [ ] **Step 4: Run tests to verify pass**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
gofmt -w internal/brreg/temporal/translation.go internal/brreg/temporal/financial.go internal/brreg/temporal/translation_test.go internal/brreg/temporal/financial_test.go
GOWORK=off go test ./internal/brreg/temporal -run 'TestTranslateBrregRawInputsWorkflowPreparesSelectionBeforeBatches|TestConvertBrregFinancialsWorkflowPreparesSelectionBeforeBatches' -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
git add scheduler/internal/brreg/temporal/translation.go scheduler/internal/brreg/temporal/financial.go scheduler/internal/brreg/temporal/translation_test.go scheduler/internal/brreg/temporal/financial_test.go
git commit -m "Prepare BRREG batch workflows inside Temporal"
```

---

### Task 5: Simplify BRREG Task Starters To Direct Temporal Starts

**Files:**
- Modify: `scheduler/internal/tasksvc/brreg_translation_starter.go`
- Modify: `scheduler/internal/tasksvc/brreg_financial_starter.go`
- Modify: `scheduler/internal/tasksvc/brreg_domain_starter.go`
- Modify: `scheduler/internal/tasksvc/starter_test.go`

- [ ] **Step 1: Write failing direct-start tests**

Replace old tests that assert selection creation in `scheduler/internal/tasksvc/starter_test.go` with direct Temporal start tests:

```go
func TestStartBrregTranslationStartsConcreteWorkflow(t *testing.T) {
	starter, tc := newTestStarter(t)

	result, err := starter.StartBrregTranslation(context.Background(), StartBrregTranslationRequest{
		BrregActionSelectionRequest: BrregActionSelectionRequest{
			Trigger:   "manual",
			Filters:   map[string]string{"translation_status": "not_started"},
			Limit:     1000,
			BatchSize: 50,
		},
	})

	require.NoError(t, err)
	require.Equal(t, ExecutorTemporal, result.Executor)
	require.Equal(t, StartStatusStarted, result.Status)
	require.Equal(t, "translate-brreg-all", tc.startOptions.ID)
	require.Equal(t, TaskQueue, tc.startOptions.TaskQueue)
	require.Equal(t, "brreg", tc.startOptions.Memo["source"])
	require.Equal(t, "translate", tc.startOptions.Memo["action"])
	input := tc.workflowArgs[0].(brregtemporal.BrregTranslateWorkflowInput)
	require.Equal(t, "manual", input.Selection.Trigger)
	require.Equal(t, map[string]string{"translation_status": "not_started"}, input.Selection.Filters)
	require.Equal(t, int32(1000), input.Selection.Limit)
	require.Equal(t, int32(50), input.Selection.BatchSize)
	require.Empty(t, input.WorkflowRunID)
	require.Empty(t, input.SelectionHash)
}
```

Add the financial starter test:

```go
func TestStartBrregFinancialConversionStartsConcreteWorkflow(t *testing.T) {
	starter, tc := newTestStarter(t)

	result, err := starter.StartBrregFinancialConversion(context.Background(), StartBrregFinancialConversionRequest{
		BrregActionSelectionRequest: BrregActionSelectionRequest{
			Trigger:   "manual",
			Filters:   map[string]string{"financial_status": "not_started"},
			Limit:     500,
			BatchSize: 100,
		},
		FXRateDate: "2026-05-31",
	})

	require.NoError(t, err)
	require.Equal(t, ExecutorTemporal, result.Executor)
	require.Equal(t, StartStatusStarted, result.Status)
	require.Equal(t, "convert-financials-brreg-all", tc.startOptions.ID)
	require.Equal(t, "brreg", tc.startOptions.Memo["source"])
	require.Equal(t, "convert_financials", tc.startOptions.Memo["action"])
	input := tc.workflowArgs[0].(brregtemporal.BrregConvertFinancialsWorkflowInput)
	require.Equal(t, "manual", input.Selection.Trigger)
	require.Equal(t, map[string]string{"financial_status": "not_started"}, input.Selection.Filters)
	require.Equal(t, int32(500), input.Selection.Limit)
	require.Equal(t, int32(100), input.Selection.BatchSize)
	require.Equal(t, "2026-05-31", input.FXRateDate)
	require.Empty(t, input.WorkflowRunID)
	require.Empty(t, input.SelectionHash)
}
```

Add the domain starter test:

```go
func TestStartBrregDomainDiscoveryStartsConcreteWorkflow(t *testing.T) {
	starter, tc := newTestStarter(t)

	result, err := starter.StartBrregDomainDiscovery(context.Background(), StartBrregDomainDiscoveryRequest{
		BrregActionSelectionRequest: BrregActionSelectionRequest{
			Trigger:   "manual",
			Filters:   map[string]string{"domain_status": "not_started"},
			Limit:     1000,
			BatchSize: 10,
		},
	})

	require.NoError(t, err)
	require.Equal(t, ExecutorTemporal, result.Executor)
	require.Equal(t, StartStatusStarted, result.Status)
	require.Equal(t, "discover-domains-brreg-all", tc.startOptions.ID)
	require.Equal(t, "brreg", tc.startOptions.Memo["source"])
	require.Equal(t, "discover_domains", tc.startOptions.Memo["action"])
	input := tc.workflowArgs[0].(brregtemporal.BrregDomainWorkflowInput)
	require.Equal(t, "manual", input.Selection.Trigger)
	require.Equal(t, map[string]string{"domain_status": "not_started"}, input.Selection.Filters)
	require.Equal(t, int32(1000), input.Selection.Limit)
	require.Equal(t, int32(10), input.Selection.BatchSize)
	require.Greater(t, input.MaxParallelCompanyActivities, int32(0))
	require.Greater(t, input.MaxClaimPageSize, int32(0))
	require.Empty(t, input.WorkflowRunID)
	require.Empty(t, input.SelectionHash)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./internal/tasksvc -run 'TestStartBrregTranslationStartsConcreteWorkflow|TestStartBrregFinancialConversionStartsConcreteWorkflow|TestStartBrregDomainDiscoveryStartsConcreteWorkflow' -count=1
```

Expected: FAIL because starters still create DB selections before starting Temporal.

- [ ] **Step 3: Implement direct translation starter**

Modify `scheduler/internal/tasksvc/brreg_translation_starter.go`:

```go
package tasksvc

import (
	"context"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"

	brregtemporal "github.com/pulsarpoint/corpscout/scheduler/internal/brreg/temporal"
)

func (s *Service) StartBrregTranslation(ctx context.Context, req StartBrregTranslationRequest) (StartResult, error) {
	if err := s.requireTemporalClient(); err != nil {
		return StartResult{}, err
	}
	workflowID := brregActionWorkflowID(brregActionTranslate, len(req.IDs) == 0 && len(req.Filters) == 0, s.now())
	input := brregtemporal.BrregTranslateWorkflowInput{
		Selection: brregtemporal.BrregWorkflowSelectionInput{
			Trigger:     req.Trigger,
			IDs:         req.IDs,
			Filters:     req.Filters,
			Limit:       req.Limit,
			BatchSize:   req.BatchSize,
			MaxAttempts: req.MaxAttempts,
		},
	}
	run, err := s.temporal.ExecuteWorkflow(ctx, brregWorkflowStartOptions(workflowID, brregActionTranslate, req.Trigger, input.Selection.MemoScope()), brregtemporal.TranslateBrregRawInputs, input)
	if err != nil {
		return StartResult{}, err
	}
	return StartResult{Executor: ExecutorTemporal, Status: StartStatusStarted, WorkflowID: workflowID, WorkflowRunID: run.GetRunID()}, nil
}

func brregWorkflowStartOptions(workflowID string, action string, trigger string, scope any) client.StartWorkflowOptions {
	return client.StartWorkflowOptions{
		ID:                                       workflowID,
		TaskQueue:                                TaskQueue,
		WorkflowIDReusePolicy:                    enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		WorkflowExecutionErrorWhenAlreadyStarted: true,
		TypedSearchAttributes:                    searchAttributes(brregSource, action),
		Memo:                                     memo(brregSource, action, trigger, scope),
	}
}
```

Modify `scheduler/internal/tasksvc/brreg_financial_starter.go`:

```go
package tasksvc

import (
	"context"

	brregtemporal "github.com/pulsarpoint/corpscout/scheduler/internal/brreg/temporal"
)

func (s *Service) StartBrregFinancialConversion(ctx context.Context, req StartBrregFinancialConversionRequest) (StartResult, error) {
	if err := s.requireTemporalClient(); err != nil {
		return StartResult{}, err
	}
	workflowID := brregActionWorkflowID(brregWorkflowIDActionConvertFinancials, len(req.IDs) == 0 && len(req.Filters) == 0, s.now())
	input := brregtemporal.BrregConvertFinancialsWorkflowInput{
		TemporalWorkflowID: workflowID,
		Selection: brregtemporal.BrregWorkflowSelectionInput{
			Trigger:     req.Trigger,
			IDs:         req.IDs,
			Filters:     req.Filters,
			Limit:       req.Limit,
			BatchSize:   req.BatchSize,
			MaxAttempts: req.MaxAttempts,
		},
		FXRateDate: req.FXRateDate,
	}
	run, err := s.temporal.ExecuteWorkflow(ctx, brregWorkflowStartOptions(workflowID, brregActionConvertFinancials, req.Trigger, input.Selection.MemoScope()), brregtemporal.ConvertBrregFinancials, input)
	if err != nil {
		return StartResult{}, err
	}
	return StartResult{Executor: ExecutorTemporal, Status: StartStatusStarted, WorkflowID: workflowID, WorkflowRunID: run.GetRunID()}, nil
}
```

Modify `scheduler/internal/tasksvc/brreg_domain_starter.go`:

```go
package tasksvc

import (
	"context"

	brregtemporal "github.com/pulsarpoint/corpscout/scheduler/internal/brreg/temporal"
)

const defaultBrregDomainBatchSize int32 = 10

func (s *Service) StartBrregDomainDiscovery(ctx context.Context, req StartBrregDomainDiscoveryRequest) (StartResult, error) {
	if err := s.requireTemporalClient(); err != nil {
		return StartResult{}, err
	}
	workflowID := brregActionWorkflowID(brregWorkflowIDActionDiscoverDomains, len(req.IDs) == 0 && len(req.Filters) == 0, s.now())
	input := brregtemporal.BrregDomainWorkflowInput{
		TemporalWorkflowID: workflowID,
		Selection: brregtemporal.BrregWorkflowSelectionInput{
			Trigger:     req.Trigger,
			IDs:         req.IDs,
			Filters:     req.Filters,
			Limit:       req.Limit,
			BatchSize:   req.BatchSize,
			MaxAttempts: req.MaxAttempts,
		},
		MaxParallelCompanyActivities: int32(envIntWithDefault("BRREG_DOMAIN_MAX_PARALLEL_COMPANY_ACTIVITIES", 20)),
		MaxClaimPageSize:             int32(envIntWithDefault("BRREG_DOMAIN_MAX_CLAIM_PAGE_SIZE", int(defaultBrregDomainBatchSize))),
		ContinueAsNewAfterCompanies:  int32(envIntWithDefault("BRREG_DOMAIN_CONTINUE_AS_NEW_AFTER_COMPANIES", 1000)),
	}
	run, err := s.temporal.ExecuteWorkflow(ctx, brregWorkflowStartOptions(workflowID, brregActionDiscoverDomains, req.Trigger, input.Selection.MemoScope()), brregtemporal.DiscoverBrregDomains, input)
	if err != nil {
		return StartResult{}, err
	}
	return StartResult{Executor: ExecutorTemporal, Status: StartStatusStarted, WorkflowID: workflowID, WorkflowRunID: run.GetRunID()}, nil
}
```

Do not call `startBrregActionWorkflow` or `startBrregSelectedTaskWorkflow` from any BRREG starter.

- [ ] **Step 4: Run direct-start tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
gofmt -w internal/tasksvc/brreg_translation_starter.go internal/tasksvc/brreg_financial_starter.go internal/tasksvc/brreg_domain_starter.go internal/tasksvc/starter_test.go
GOWORK=off go test ./internal/tasksvc -run 'TestStartBrregTranslationStartsConcreteWorkflow|TestStartBrregFinancialConversionStartsConcreteWorkflow|TestStartBrregDomainDiscoveryStartsConcreteWorkflow' -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
git add scheduler/internal/tasksvc/brreg_translation_starter.go scheduler/internal/tasksvc/brreg_financial_starter.go scheduler/internal/tasksvc/brreg_domain_starter.go scheduler/internal/tasksvc/starter_test.go
git commit -m "Simplify BRREG Temporal starters"
```

---

### Task 6: Add One-Company Domain Activity

**Files:**
- Create: `scheduler/internal/brreg/temporal/domain_company_activity.go`
- Create: `scheduler/internal/brreg/temporal/domain_company_activity_test.go`
- Modify: `scheduler/internal/brreg/temporal/domain_activity.go`

- [ ] **Step 1: Write failing one-company activity test**

Create `scheduler/internal/brreg/temporal/domain_company_activity_test.go`:

```go
package brregtemporal

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestDiscoverOneBrregDomainCompanyCallsDomainServiceAndSubmitsResult(t *testing.T) {
	rawRecordID := uuid.New()
	attemptID := uuid.New()
	gateway := &fakeDomainGateway{}
	client := &fakeDomainDiscoveryClient{
		response: crawlserviceclient.BrregDomainDiscoveryResponse{
			Status: "succeeded",
			Domains: []crawlserviceclient.BrregDiscoveredDomain{
				{Domain: "bortigard.no", Confidence: 91},
			},
		},
	}
	activities := NewActivities(gateway, nil, WithDomainDiscoveryClient(client))

	result, err := activities.DiscoverOneBrregDomainCompany(context.Background(), BrregDomainCompanyInput{
		WorkflowRunID:   "workflow-run-id",
		SelectionHash:   "selection-hash",
		RawRecordID:     rawRecordID.String(),
		TaskAttemptID:   attemptID.String(),
		OrganizationNo:  "810202572",
		OrganizationName: "BORTIGARD AS",
		MaxTaskAttempts: 3,
	})

	require.NoError(t, err)
	require.Equal(t, int32(1), result.Succeeded)
	require.Equal(t, "BORTIGARD AS", client.request.CompanyName)
	require.Equal(t, rawRecordID, gateway.domainResult.Result.RawRecordID)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./internal/brreg/temporal -run TestDiscoverOneBrregDomainCompanyCallsDomainServiceAndSubmitsResult -count=1
```

Expected: FAIL because `BrregDomainCompanyInput`, `BrregDomainCompanyResult`, and `DiscoverOneBrregDomainCompany` do not exist.

- [ ] **Step 3: Add one-company activity types and wrapper**

Create `scheduler/internal/brreg/temporal/domain_company_activity.go`:

```go
package brregtemporal

import (
	"context"

	"github.com/cockroachdb/errors"
)

const DiscoverOneBrregDomainCompanyActivityName = "DiscoverOneBrregDomainCompany"

type BrregDomainCompanyInput struct {
	WorkflowRunID     string `json:"workflow_run_id"`
	SelectionHash     string `json:"selection_hash"`
	RawRecordID       string `json:"raw_record_id"`
	TaskAttemptID     string `json:"task_attempt_id"`
	OrganizationNo    string `json:"organization_number"`
	OrganizationName  string `json:"organization_name"`
	Website           string `json:"website,omitempty"`
	BatchSize         int32  `json:"batch_size"`
	MaxTaskAttempts   int32  `json:"max_task_attempts"`
	SearchProvider    string `json:"search_provider,omitempty"`
	PromptVersion     string `json:"prompt_version,omitempty"`
}

type BrregDomainCompanyResult struct {
	Succeeded int32 `json:"succeeded"`
	Skipped   int32 `json:"skipped"`
	Failed    int32 `json:"failed"`
}

func (a *Activities) DiscoverOneBrregDomainCompany(ctx context.Context, input BrregDomainCompanyInput) (BrregDomainCompanyResult, error) {
	if a == nil || a.domainClient == nil {
		return BrregDomainCompanyResult{}, errors.New("domain discovery client not available")
	}
	if a.gateway == nil {
		return BrregDomainCompanyResult{}, errors.New("brreg domain gateway not available")
	}
	return a.discoverOneBrregDomainCompany(ctx, input)
}
```

Move the per-row body from `DiscoverNextBrregDomainBatch` into:

```go
func (a *Activities) discoverOneBrregDomainCompany(ctx context.Context, input BrregDomainCompanyInput) (BrregDomainCompanyResult, error) {
	row, err := claimedDomainCompanyInputToRow(input)
	if err != nil {
		return BrregDomainCompanyResult{}, err
	}
	response, err := a.domainClient.DiscoverBrregDomain(ctx, domainDiscoveryRequestFromCompany(input))
	if err != nil {
		if submitErr := submitFailedDomain(ctx, a.gateway, row, input.MaxTaskAttempts, err.Error(), TaskFailureExternalService(taskFailureCodeDomainServiceRequestFailed)); submitErr != nil {
			return BrregDomainCompanyResult{}, submitErr
		}
		return BrregDomainCompanyResult{Failed: 1}, nil
	}
	submission, counter, err := domainSubmissionForResponse(row, response)
	if err != nil {
		if submitErr := submitFailedDomain(ctx, a.gateway, row, input.MaxTaskAttempts, err.Error(), TaskFailureInvalidDomainOutput(taskFailureCodeInvalidDomainResponse)); submitErr != nil {
			return BrregDomainCompanyResult{}, submitErr
		}
		return BrregDomainCompanyResult{Failed: 1}, nil
	}
	submission.MaxAttempts = input.MaxTaskAttempts
	if err := a.gateway.SubmitDomainResult(ctx, submission); err != nil {
		return BrregDomainCompanyResult{}, errors.Wrap(err, "submit brreg domain result")
	}
	switch counter {
	case brregdb.ResultStatusSkipped:
		return BrregDomainCompanyResult{Skipped: 1}, nil
	case brregdb.ResultStatusSucceeded, brregdb.ResultStatusPartial, brregdb.ResultStatusNotFound:
		return BrregDomainCompanyResult{Succeeded: 1}, nil
	default:
		return BrregDomainCompanyResult{Failed: 1}, nil
	}
}
```

Import `brregdb` in this file when adding the switch.

- [ ] **Step 4: Run one-company activity test**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
gofmt -w internal/brreg/temporal/domain_company_activity.go internal/brreg/temporal/domain_company_activity_test.go internal/brreg/temporal/domain_activity.go
GOWORK=off go test ./internal/brreg/temporal -run TestDiscoverOneBrregDomainCompanyCallsDomainServiceAndSubmitsResult -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
git add scheduler/internal/brreg/temporal/domain_company_activity.go scheduler/internal/brreg/temporal/domain_company_activity_test.go scheduler/internal/brreg/temporal/domain_activity.go
git commit -m "Add BRREG one-company domain activity"
```

---

### Task 7: Refactor Domain Parent Workflow To Bounded Activities

**Files:**
- Modify: `scheduler/internal/brreg/temporal/domain.go`
- Modify: `scheduler/internal/brreg/temporal/domain_test.go`
- Modify: `scheduler/internal/brreg/temporal/domain_activity.go`

- [ ] **Step 1: Write failing parent concurrency test**

Add to `scheduler/internal/brreg/temporal/domain_test.go`:

```go
func TestDiscoverBrregDomainsParentLimitsParallelCompanyActivities(t *testing.T) {
	env := testsuite.WorkflowTestSuite{}.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(DiscoverBrregDomains)
	env.RegisterActivityWithOptions(
		func(context.Context, BrregDomainWorkflowInput) (PreparedBrregWorkflow, error) {
			return PreparedBrregWorkflow{
				WorkflowRunID:   uuid.NewString(),
				SelectionHash:   "selection-hash",
				RecordsSelected: 3,
				BatchSize:       3,
				MaxAttempts:     3,
			}, nil
		},
		activity.RegisterOptions{Name: PrepareBrregDomainWorkflowActivityName},
	)
	env.RegisterActivityWithOptions(
		func(context.Context, BrregDomainWorkflowInput) (BrregDomainCompanyPageResult, error) {
			return BrregDomainCompanyPageResult{
				Companies: []BrregDomainCompanyInput{
					{RawRecordID: "raw-1", OrganizationNo: "1", OrganizationName: "A"},
					{RawRecordID: "raw-2", OrganizationNo: "2", OrganizationName: "B"},
					{RawRecordID: "raw-3", OrganizationNo: "3", OrganizationName: "C"},
				},
			}, nil
		},
		activity.RegisterOptions{Name: ClaimBrregDomainCompanyPageActivityName},
	)
	started := 0
	env.RegisterActivityWithOptions(
		func(context.Context, BrregDomainCompanyInput) (BrregDomainCompanyResult, error) {
			started++
			return BrregDomainCompanyResult{Succeeded: 1}, nil
		},
		activity.RegisterOptions{Name: DiscoverOneBrregDomainCompanyActivityName},
	)
	env.RegisterActivityWithOptions(
		func(context.Context, FinishBrregWorkflowRunInput) error { return nil },
		activity.RegisterOptions{Name: FinishBrregWorkflowRunActivityName},
	)

	env.ExecuteWorkflow(DiscoverBrregDomains, BrregDomainWorkflowInput{
		Selection:                    BrregWorkflowSelectionInput{Trigger: "manual"},
		MaxParallelCompanyActivities: 2,
		MaxClaimPageSize:             3,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, 3, started)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./internal/brreg/temporal -run TestDiscoverBrregDomainsParentLimitsParallelCompanyActivities -count=1
```

Expected: FAIL because the parent still uses `runBrregBatchWorkflow` with one batch activity.

- [ ] **Step 3: Add claim-page activity types**

In `scheduler/internal/brreg/temporal/domain_activity.go`, add:

```go
const ClaimBrregDomainCompanyPageActivityName = "ClaimBrregDomainCompanyPage"

type BrregDomainCompanyPageResult struct {
	Companies []BrregDomainCompanyInput `json:"companies"`
}

func (a *Activities) ClaimBrregDomainCompanyPage(ctx context.Context, input BrregDomainWorkflowInput) (BrregDomainCompanyPageResult, error) {
	rows, err := a.gateway.ClaimDomainBatch(ctx, claimDomainPageCommand(input))
	if err != nil {
		return BrregDomainCompanyPageResult{}, errors.Wrap(err, "claim brreg domain company page")
	}
	companies := make([]BrregDomainCompanyInput, 0, len(rows))
	for _, row := range rows {
		companies = append(companies, domainCompanyInputFromClaimedRow(input, row))
	}
	return BrregDomainCompanyPageResult{Companies: companies}, nil
}
```

Use existing `claimTaskBatchCommand` inside `claimDomainPageCommand`.

- [ ] **Step 4: Replace domain parent workflow loop**

In `scheduler/internal/brreg/temporal/domain.go`, replace the batch-runner call with an explicit activity loop:

```go
func DiscoverBrregDomains(ctx workflow.Context, input BrregDomainWorkflowInput) (BrregDomainWorkflowResult, error) {
	input = input.withDefaults()
	ctx = workflow.WithActivityOptions(ctx, brregBatchActivityOptions(20*time.Minute))

	if input.WorkflowRunID == "" || input.SelectionHash == "" {
		var prepared PreparedBrregWorkflow
		if err := workflow.ExecuteActivity(ctx, PrepareBrregDomainWorkflowActivityName, input).Get(ctx, &prepared); err != nil {
			return BrregDomainWorkflowResult{}, err
		}
		input.WorkflowRunID = prepared.WorkflowRunID
		input.SelectionHash = prepared.SelectionHash
		input.BatchSize = prepared.BatchSize
		input.MaxTaskAttempts = prepared.MaxAttempts
	}

	progress := brregWorkflowProgress{}
	for {
		var page BrregDomainCompanyPageResult
		if err := workflow.ExecuteActivity(ctx, ClaimBrregDomainCompanyPageActivityName, input).Get(ctx, &page); err != nil {
			progress.markActivityFailed()
			if finishErr := progress.finishFailedAfterActivityError(ctx, brregWorkflowLabelDomain, input.WorkflowRunID, input.MaxTaskAttempts, err); finishErr != nil {
				return BrregDomainWorkflowResult(progress), finishErr
			}
			return BrregDomainWorkflowResult(progress), err
		}
		if len(page.Companies) == 0 {
			progress.markDrained()
			if err := progress.finishSucceeded(ctx, brregWorkflowLabelDomain, input.WorkflowRunID, input.MaxTaskAttempts); err != nil {
				return BrregDomainWorkflowResult(progress), err
			}
			return BrregDomainWorkflowResult(progress), nil
		}
		result, err := runDomainCompanyActivities(ctx, input, page.Companies)
		if err != nil {
			return BrregDomainWorkflowResult(progress), err
		}
		progress.addBatch(int32(len(page.Companies)), result.Succeeded, result.Skipped, result.Failed)
		if input.ContinueAsNewAfterCompanies > 0 && progress.RowsClaimed >= input.ContinueAsNewAfterCompanies {
			return BrregDomainWorkflowResult(progress), workflow.NewContinueAsNewError(ctx, DiscoverBrregDomains, input)
		}
	}
}
```

Add helper:

```go
func runDomainCompanyActivities(ctx workflow.Context, input BrregDomainWorkflowInput, companies []BrregDomainCompanyInput) (BrregDomainCompanyResult, error) {
	limit := int(input.MaxParallelCompanyActivities)
	if limit <= 0 {
		limit = 10
	}
	var aggregate BrregDomainCompanyResult
	for start := 0; start < len(companies); start += limit {
		end := start + limit
		if end > len(companies) {
			end = len(companies)
		}
		futures := make([]workflow.Future, 0, end-start)
		for _, company := range companies[start:end] {
			company.WorkflowRunID = input.WorkflowRunID
			company.SelectionHash = input.SelectionHash
			company.MaxTaskAttempts = input.MaxTaskAttempts
			futures = append(futures, workflow.ExecuteActivity(ctx, DiscoverOneBrregDomainCompanyActivityName, company))
		}
		for _, future := range futures {
			var result BrregDomainCompanyResult
			if err := future.Get(ctx, &result); err != nil {
				return aggregate, err
			}
			aggregate.Succeeded += result.Succeeded
			aggregate.Skipped += result.Skipped
			aggregate.Failed += result.Failed
		}
	}
	return aggregate, nil
}
```

- [ ] **Step 5: Run parent tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
gofmt -w internal/brreg/temporal/domain.go internal/brreg/temporal/domain_activity.go internal/brreg/temporal/domain_test.go
GOWORK=off go test ./internal/brreg/temporal -run 'TestDiscoverBrregDomainsParentLimitsParallelCompanyActivities|TestDiscoverBrregDomainsWorkflowDrainsUntilNoRows|TestDiscoverBrregDomainsWorkflowContinuesAsNewAfterThreshold' -count=1
```

Expected: PASS after updating older domain workflow tests to expect the prepare activity and claim-page activity names.

- [ ] **Step 6: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
git add scheduler/internal/brreg/temporal/domain.go scheduler/internal/brreg/temporal/domain_activity.go scheduler/internal/brreg/temporal/domain_test.go
git commit -m "Run BRREG domain discovery through bounded activities"
```

---

### Task 8: Register New Activities

**Files:**
- Modify: `scheduler/internal/app/temporal.go`
- Modify: `scheduler/internal/app/temporal_test.go`

- [ ] **Step 1: Find registration file**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
rg "RegisterWorkflow|RegisterActivity" scheduler/internal -n
```

Expected: command prints `scheduler/internal/app/temporal.go` and `scheduler/internal/app/temporal_test.go`.

- [ ] **Step 2: Write failing registration test**

Modify `scheduler/internal/app/temporal_test.go`:

```go
func TestRegisterBrregTemporalWorkflowsRegistersWorkflowAndActivities(t *testing.T) {
	registry := &fakeTemporalRegistry{}

	registerBrregTemporalWorkflows(registry, brregtemporal.NewActivities(nil, nil))

	require.Len(t, registry.workflows, 3)
	require.Len(t, registry.activities, 8)
	require.Equal(t, "TranslateNextBrregBatch", registry.activities[0].Name)
	require.Equal(t, "FinishBrregWorkflowRun", registry.activities[1].Name)
	require.Equal(t, "PrepareBrregTranslationWorkflow", registry.activities[2].Name)
	require.Equal(t, "ConvertNextBrregFinancialBatch", registry.activities[3].Name)
	require.Equal(t, "PrepareBrregFinancialWorkflow", registry.activities[4].Name)
	require.Equal(t, "PrepareBrregDomainWorkflow", registry.activities[5].Name)
	require.Equal(t, "ClaimBrregDomainCompanyPage", registry.activities[6].Name)
	require.Equal(t, "DiscoverOneBrregDomainCompany", registry.activities[7].Name)
}
```

- [ ] **Step 3: Run test to verify it fails**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./internal/app -run TestRegisterBrregTemporalWorkflowsRegistersWorkflowAndActivities -count=1
```

Expected: FAIL because the new activity names are not registered.

- [ ] **Step 4: Register new activities**

Modify `scheduler/internal/app/temporal.go` registration helper:

```go
registry.RegisterActivityWithOptions(
	activities.PrepareBrregTranslationWorkflow,
	activity.RegisterOptions{Name: brregtemporal.PrepareBrregTranslationWorkflowActivityName},
)
registry.RegisterActivityWithOptions(
	activities.PrepareBrregFinancialWorkflow,
	activity.RegisterOptions{Name: brregtemporal.PrepareBrregFinancialWorkflowActivityName},
)
registry.RegisterWorkflow(brregtemporal.DiscoverBrregDomains)
registry.RegisterActivityWithOptions(
	activities.PrepareBrregDomainWorkflow,
	activity.RegisterOptions{Name: brregtemporal.PrepareBrregDomainWorkflowActivityName},
)
registry.RegisterActivityWithOptions(
	activities.ClaimBrregDomainCompanyPage,
	activity.RegisterOptions{Name: brregtemporal.ClaimBrregDomainCompanyPageActivityName},
)
registry.RegisterActivityWithOptions(
	activities.DiscoverOneBrregDomainCompany,
	activity.RegisterOptions{Name: brregtemporal.DiscoverOneBrregDomainCompanyActivityName},
)
```

Remove the old `DiscoverNextBrregDomainBatch` registration after `DiscoverBrregDomains` starts using `ClaimBrregDomainCompanyPage` and `DiscoverOneBrregDomainCompany`.

- [ ] **Step 5: Run registration tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
gofmt -w internal/app/temporal.go internal/app/temporal_test.go
GOWORK=off go test ./internal/app -run TestRegisterBrregTemporalWorkflowsRegistersWorkflowAndActivities -count=1
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
git add scheduler/internal/app/temporal.go scheduler/internal/app/temporal_test.go
git commit -m "Register BRREG prepare and domain company activities"
```

---

### Task 9: Remove Old BRREG Starter Indirection

**Files:**
- Delete: `scheduler/internal/tasksvc/brreg_action_starter.go`
- Delete: `scheduler/internal/tasksvc/brreg_workflow_start.go`
- Delete: `scheduler/internal/tasksvc/brreg_task_selection.go`
- Modify: `scheduler/internal/tasksvc/brreg_starter.go`
- Modify: `scheduler/internal/tasksvc/starter_test.go`

- [ ] **Step 1: Write guard test that old indirection is gone**

Add to `scheduler/internal/tasksvc/starter_test.go`:

```go
func TestBrregStartersDoNotUseSelectedTaskWorkflowIndirection(t *testing.T) {
	files := []string{
		"brreg_translation_starter.go",
		"brreg_financial_starter.go",
		"brreg_domain_starter.go",
	}
	for _, file := range files {
		source, err := os.ReadFile(file)
		require.NoError(t, err)
		body := string(source)
		require.NotContains(t, body, "startBrregActionWorkflow")
		require.NotContains(t, body, "startBrregSelectedTaskWorkflow")
		require.NotContains(t, body, "brregActionWorkflowStart")
	}
}
```

- [ ] **Step 2: Run guard test**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./internal/tasksvc -run TestBrregStartersDoNotUseSelectedTaskWorkflowIndirection -count=1
```

Expected: PASS after Task 5 is complete.

- [ ] **Step 3: Move the remaining Temporal client guard**

Add this helper to `scheduler/internal/tasksvc/brreg_starter.go` before deleting `brreg_workflow_start.go`:

```go
func (s *Service) requireTemporalClient() error {
	if s.temporal == nil {
		return errors.New("temporal client not available")
	}
	return nil
}
```

Keep the existing `github.com/cockroachdb/errors` import in `brreg_starter.go`.

- [ ] **Step 4: Remove obsolete files and tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
rg "startBrregActionWorkflow|brregActionWorkflowStart|startBrregSelectedTaskWorkflow|brregSelectedTaskWorkflowStart|createBrregTaskSelection|brregTaskSelectionOptions" scheduler/internal/tasksvc -n
```

Expected before deletion: matches only in the obsolete files and tests being removed.

Delete obsolete files:

```bash
git rm scheduler/internal/tasksvc/brreg_action_starter.go scheduler/internal/tasksvc/brreg_workflow_start.go scheduler/internal/tasksvc/brreg_task_selection.go
```

Remove old tests in `scheduler/internal/tasksvc/starter_test.go` that reference:

```text
startBrregSelectedTaskWorkflow
brregSelectedTaskWorkflowStart
createBrregTaskSelection
brregTaskSelectionOptions
BeginBrregWorkflowRun
CreateBrregWorkflowTaskSelection
```

Keep `brregActionWorkflowID` in `brreg_starter.go` because direct starters still use it to create singleton and selected workflow IDs. Delete unused helpers that only created DB task selections from `tasksvc`.

- [ ] **Step 5: Run tasksvc tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
gofmt -w internal/tasksvc
GOWORK=off go test ./internal/tasksvc -count=1
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
git add scheduler/internal/tasksvc
git commit -m "Remove old BRREG starter indirection"
```

---

### Task 10: Full Verification

**Files:**
- No new files.

- [ ] **Step 1: Run scheduler tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./...
```

Expected: PASS for all scheduler packages.

- [ ] **Step 2: Run staticcheck**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off /Users/graovic/go/bin/staticcheck ./...
```

Expected: exit code 0 with no diagnostics.

- [ ] **Step 3: Run go vet**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go vet ./...
```

Expected: exit code 0 with no diagnostics.

- [ ] **Step 4: Check diff whitespace**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
git diff --check
```

Expected: exit code 0 with no whitespace errors.

- [ ] **Step 5: Inspect remaining BRREG starter references**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
rg "startBrregActionWorkflow|startBrregSelectedTaskWorkflow|brregActionWorkflowStart|brregSelectedTaskWorkflowStart" scheduler/internal -n
```

Expected: no output.
