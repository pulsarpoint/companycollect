# BRREG Source Action Tracking Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Track BRREG augmentation state per source record and action, then simplify source translation so workflow-owned batches process `brreg_source` records without user-tunable batch parameters.

**Architecture:** Add `brreg_source.action_tasks` as the source-schema action ledger keyed by action type, source row, optional field target, and source fingerprint. Workflows stay deterministic and call source-specific activities that directly use SQLC queries; database queries decide eligibility, claim batches, and persist results. The first implementation slice wires translation to this ledger and leaves domain discovery visible but disabled until its result writer targets `brreg_source`.

**Tech Stack:** PostgreSQL migrations and views, sqlc-generated Go queries, Temporal Go SDK workflows/activities, React Router UI with existing shadcn components.

---

### Task 1: Source Action Task Schema

**Files:**
- Modify: `scheduler/internal/db/brreg_source_profile_tables_migration_test.go`
- Modify: `database/migrations/000074_brreg_source_profile_tables.up.sql`
- Modify: `database/migrations/000074_brreg_source_profile_tables.down.sql`

- [ ] **Step 1: Write the failing migration test**

Add assertions to `TestBrregSourceProfileTablesMigrationDefinesSourceSchema`:

```go
require.Contains(t, sql, "CREATE TABLE brreg_source.action_tasks")
require.Contains(t, sql, "action_type TEXT NOT NULL")
require.Contains(t, sql, "source_fingerprint TEXT NOT NULL")
require.Contains(t, sql, "target_key TEXT")
require.Contains(t, sql, "UNIQUE (action_type, source_table, source_row_id, target_key, source_fingerprint)")
require.NotContains(t, sql, "CREATE TABLE brreg_source.field_translation_tasks")
```

Update the down migration test:

```go
require.Contains(t, sql, "DROP TABLE IF EXISTS brreg_source.action_tasks")
require.NotContains(t, sql, "DROP TABLE IF EXISTS brreg_source.field_translation_tasks")
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cd scheduler && GOWORK=off go test ./internal/db -run TestBrregSourceProfileTables -count=1
```

Expected: FAIL because the migration still creates `field_translation_tasks` and does not define `action_tasks`.

- [ ] **Step 3: Replace `field_translation_tasks` with `action_tasks`**

In `000074_brreg_source_profile_tables.up.sql`, replace `CREATE TABLE brreg_source.field_translation_tasks` with:

```sql
CREATE TABLE brreg_source.action_tasks (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id UUID NOT NULL REFERENCES brreg_source.companies(id) ON DELETE CASCADE,

  action_type TEXT NOT NULL,
  source_table TEXT NOT NULL,
  source_row_id UUID NOT NULL,
  target_key TEXT NOT NULL DEFAULT '',
  source_fingerprint TEXT NOT NULL,

  source_column TEXT,
  target_column TEXT,
  source_text TEXT,
  source_lang TEXT NOT NULL DEFAULT 'no',
  target_lang TEXT NOT NULL DEFAULT 'en',

  status TEXT NOT NULL DEFAULT 'pending',
  attempt_count INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 3,
  lease_until TIMESTAMPTZ,
  last_started_at TIMESTAMPTZ,
  last_finished_at TIMESTAMPTZ,

  result JSONB NOT NULL DEFAULT '{}'::jsonb,
  model TEXT,
  prompt_version TEXT,
  error TEXT,
  error_category TEXT,
  error_code TEXT,
  retry_strategy TEXT,

  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  CONSTRAINT chk_brreg_source_action_type CHECK (
    action_type IN ('translate_field', 'discover_domains', 'convert_currency', 'build_suggestion')
  ),
  CONSTRAINT chk_brreg_source_action_status CHECK (
    status IN ('pending', 'running', 'succeeded', 'failed_retryable', 'failed_terminal', 'skipped')
  ),
  CONSTRAINT chk_brreg_source_action_attempt CHECK (attempt_count >= 0 AND max_attempts > 0),
  CONSTRAINT chk_brreg_source_action_result_object CHECK (jsonb_typeof(result) = 'object'),
  CONSTRAINT chk_brreg_source_action_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  UNIQUE (action_type, source_table, source_row_id, target_key, source_fingerprint)
);

CREATE INDEX idx_brreg_source_action_queue
  ON brreg_source.action_tasks(action_type, status, lease_until, updated_at)
  WHERE status IN ('pending', 'running', 'failed_retryable');

CREATE INDEX idx_brreg_source_action_company
  ON brreg_source.action_tasks(company_id, action_type, status);
```

In the down migration, drop `action_tasks` instead of `field_translation_tasks`.

- [ ] **Step 4: Run test to verify it passes**

Run:

```bash
cd scheduler && GOWORK=off go test ./internal/db -run TestBrregSourceProfileTables -count=1
```

Expected: PASS.

### Task 2: SQLC Queries For Translation Action Tasks

**Files:**
- Modify: `database/queries/brreg_source_profile.sql`
- Generated: `scheduler/internal/db/gen/brreg_source_profile.sql.go`
- Generated: `scheduler/internal/db/gen/querier.go`
- Test: `scheduler/internal/brreg/actions/source_translation_actions_test.go`

- [ ] **Step 1: Write failing action-level test**

Create `scheduler/internal/brreg/actions/source_translation_actions_test.go` with:

```go
package actions

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestSourceTranslationCommandExtractsTranslatedText(t *testing.T) {
	taskID := uuid.New()
	command, err := sourceTranslationTaskCompletionFromResult(TranslationRecordResult{
		RawRecordID:        taskID.String(),
		OrganizationNumber: "810202572",
		Status:             "succeeded",
		TranslatedPayload: map[string]any{
			"terms": []any{
				map[string]any{"translated_text": "Limited company"},
			},
		},
		Model:         "mock-fast",
		PromptVersion: "v1",
	}, taskID)

	require.NoError(t, err)
	require.Equal(t, taskID, command.TaskID)
	require.Equal(t, "succeeded", command.Status)
	require.JSONEq(t, `{"translated_text":"Limited company"}`, string(command.Result))
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cd scheduler && GOWORK=off go test ./internal/brreg/actions -run TestSourceTranslationCommandExtractsTranslatedText -count=1
```

Expected: FAIL because `sourceTranslationTaskCompletionFromResult` does not exist.

- [ ] **Step 3: Replace source translation SQL with `action_tasks`**

In `database/queries/brreg_source_profile.sql`:

- Replace `brreg_source.field_translation_tasks` references with `brreg_source.action_tasks`.
- In `PrepareBrregSourceTranslationTasks`, insert:

```sql
action_type,
source_table,
source_row_id,
target_key,
source_fingerprint,
source_column,
target_column,
source_text,
max_attempts,
metadata
```

with values:

```sql
'translate_field',
missing.source_table,
missing.source_row_id,
missing.target_column,
missing.source_text_hash,
missing.source_column,
missing.target_column,
missing.source_text,
GREATEST(sqlc.arg('max_attempts')::integer, 1),
jsonb_build_object('priority', missing.priority)
```

- Use conflict key:

```sql
ON CONFLICT (action_type, source_table, source_row_id, target_key, source_fingerprint) DO NOTHING
```

- In claim queries, filter `task.action_type = 'translate_field'`.
- In completion queries, store translated text as:

```sql
result = CASE
  WHEN sqlc.arg('status')::text = 'succeeded'
  THEN jsonb_build_object('translated_text', sqlc.narg('translated_text')::text)
  ELSE task.result
END
```

and update source tables from `task.result ->> 'translated_text'`.

- [ ] **Step 4: Generate SQLC**

Run:

```bash
make sqlc-generate
```

Expected: generated query types reference `action_tasks`; no generated source translation query should reference `field_translation_tasks`.

- [ ] **Step 5: Implement minimal action helper**

In `scheduler/internal/brreg/actions/translation_actions.go`, add:

```go
type CompleteSourceTranslationTaskCommand struct {
	TaskID         uuid.UUID
	Status         string
	Result         json.RawMessage
	Model          *string
	PromptVersion  *string
	Error          *string
	ErrorCategory  string
	ErrorCode      string
	RetryStrategy  string
}

func sourceTranslationTaskCompletionFromResult(result TranslationRecordResult, taskID uuid.UUID) (CompleteSourceTranslationTaskCommand, error) {
	translatedText, err := translatedTextFromPayload(result.TranslatedPayload)
	if result.Status == "succeeded" && err != nil {
		return CompleteSourceTranslationTaskCommand{}, err
	}
	payload := json.RawMessage(`{}`)
	if translatedText != nil {
		encoded, err := json.Marshal(map[string]string{"translated_text": *translatedText})
		if err != nil {
			return CompleteSourceTranslationTaskCommand{}, errors.Wrap(err, "marshal source translation result")
		}
		payload = encoded
	}
	return CompleteSourceTranslationTaskCommand{
		TaskID:        taskID,
		Status:        result.Status,
		Result:        payload,
		Model:         stringPointer(result.Model),
		PromptVersion: stringPointer(result.PromptVersion),
		Error:         translationErrorMessage(result.Error),
		ErrorCategory: translationErrorCategory(result.Error),
		ErrorCode:     translationErrorCode(result.Error),
		RetryStrategy: translationErrorRetryStrategy(result.Error),
	}, nil
}
```

- [ ] **Step 6: Run action test**

Run:

```bash
cd scheduler && GOWORK=off go test ./internal/brreg/actions -run TestSourceTranslationCommandExtractsTranslatedText -count=1
```

Expected: PASS.

### Task 3: Remove Generic `brreg/db` Facade From Source Translation

**Files:**
- Modify: `scheduler/internal/brreg/actions/translation_actions.go`
- Modify: `scheduler/internal/app/temporal.go`
- Modify: `scheduler/internal/app/brreg_translation_temporal.go`
- Delete or stop using: `scheduler/internal/brreg/db/source_translation.go`
- Test: `scheduler/internal/brreg/actions/translation_actions_test.go`

- [ ] **Step 1: Write failing constructor test**

Add to `translation_actions_test.go`:

```go
func TestNewTranslationActionsDoesNotRequireBrregGateway(t *testing.T) {
	actions := NewTranslationActions(nil, nil, nil)
	require.NotNil(t, actions)
}
```

Run:

```bash
cd scheduler && GOWORK=off go test ./internal/brreg/actions -run TestNewTranslationActionsDoesNotRequireBrregGateway -count=1
```

Expected: FAIL if constructor still requires a gateway-shaped dependency for source translation.

- [ ] **Step 2: Move DB access into concrete source action methods**

Change `TranslationActions` to hold the concrete DB pool or sqlc-compatible DBTX used elsewhere in scheduler wiring:

```go
type TranslationActions struct {
	db           db.DBTX
	translator   *translationclient.Client
	llmProviders *llmproviders.Store
}
```

Update constructor:

```go
func NewTranslationActions(dbtx db.DBTX, translator *translationclient.Client, llmProviders *llmproviders.Store) *TranslationActions {
	return &TranslationActions{db: dbtx, translator: translator, llmProviders: llmProviders}
}
```

Update source translation methods to call `db.New(a.db).PrepareBrregSourceTranslationTasks`, `ClaimBrregSourceTranslationBatch`, `CompleteBrregSourceTranslationTask`, and `FailRunningBrregSourceTranslationTasksForRun` directly.

- [ ] **Step 3: Update Temporal wiring**

In `scheduler/internal/app/temporal.go`, construct translation actions with the pool/DBTX instead of `brregdb.Gateway`:

```go
translationActions: brregactions.NewTranslationActions(pool, translator, llmStore),
```

Keep worker registration direct in `brreg_translation_temporal.go`; do not add registry interfaces.

- [ ] **Step 4: Run action and app tests**

Run:

```bash
cd scheduler && GOWORK=off go test ./internal/brreg/actions ./internal/app -count=1
```

Expected: PASS.

### Task 4: Workflow-Owned Batch Defaults

**Files:**
- Modify: `scheduler/internal/brreg/workflow/translation.go`
- Modify: `scheduler/internal/httpapi/workflow_triggers.go`
- Modify: `scheduler/internal/httpapi/workflow_triggers_test.go`
- Modify: `ui/app/components/app/BrregTranslationActionForm.tsx` or create `ui/app/components/app/BrregSourceTranslationActionForm.tsx`

- [ ] **Step 1: Write failing HTTP test**

Add to `workflow_triggers_test.go`:

```go
func TestStartBrregTranslationWorkflowIgnoresUserBatchOptions(t *testing.T) {
	tc := &fakeTemporalClient{}
	h := httpapi.NewHandlers(nil, nil, tc, nil, "", nil, "")
	r := routerFor(h)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workflows/brreg/translation", bytes.NewBufferString(`{
		"ids":["550e8400-e29b-41d4-a716-446655440000"],
		"batch_size": 999,
		"max_attempts": 99,
		"max_parallel_tasks": 99
	}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusAccepted, w.Code)
	input := tc.lastInput.(brregworkflow.TranslateBrregRawInputsInput)
	require.Equal(t, 0, input.BatchSize)
	require.Equal(t, 0, input.MaxAttempts)
	require.Equal(t, 0, input.MaxParallelTasks)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cd scheduler && GOWORK=off go test ./internal/httpapi -run TestStartBrregTranslationWorkflowIgnoresUserBatchOptions -count=1
```

Expected: FAIL if the HTTP layer still forwards user batch options.

- [ ] **Step 3: Simplify request shape**

In `workflow_triggers.go`, keep parsing `ids`, `filters`, `limit`, and `trigger`. Stop forwarding user-provided `batch_size`, `max_attempts`, `max_parallel_tasks`, and `lease_seconds` to `TranslateBrregRawInputsInput`.

Workflow normalization in `translation.go` remains the only place that sets:

```go
defaultTranslationBatchSize
defaultTranslationMaxAttempts
defaultTranslationLeaseSeconds
defaultTranslationMaxParallelTasks
defaultTranslationServiceRetries
```

- [ ] **Step 4: Simplify UI action form**

For source entries, use a source-specific form that sends only:

```ts
await api.translateBrreg({
  ids: scope === "selected" ? selectedIds : undefined,
  filters: scope === "filtered" ? filters : undefined,
  limit: effectiveLimit,
  trigger: "manual",
});
```

Do not render batch size, retries, provider, model, lease, or parallel controls on the source-entry action sheet.

- [ ] **Step 5: Run tests**

Run:

```bash
cd scheduler && GOWORK=off go test ./internal/httpapi ./internal/brreg/workflow -count=1
cd ui && pnpm typecheck
```

Expected: PASS.

### Task 5: Company Explorer Action Rollups

**Files:**
- Modify: `database/migrations/000074_brreg_source_profile_tables.up.sql`
- Modify: `database/queries/brreg_source_profile.sql`
- Generated: `scheduler/internal/db/gen/brreg_source_profile.sql.go`
- Test: `scheduler/internal/httpapi/brreg_source_entries_test.go`

- [ ] **Step 1: Write failing API test for action rollups**

In `brreg_source_entries_test.go`, expect source entries to include action task counts:

```go
require.Equal(t, int64(2), body.Items[0].TranslationPendingCount)
require.Equal(t, int64(1), body.Items[0].TranslationRunningCount)
require.Equal(t, int64(8), body.Items[0].TranslationSucceededCount)
```

- [ ] **Step 2: Update `v_company_explorer`**

Add a CTE:

```sql
action_counts AS (
  SELECT
    company_id,
    count(*) FILTER (WHERE action_type = 'translate_field' AND status IN ('pending', 'failed_retryable'))::bigint AS translation_pending_count,
    count(*) FILTER (WHERE action_type = 'translate_field' AND status = 'running')::bigint AS translation_running_count,
    count(*) FILTER (WHERE action_type = 'translate_field' AND status = 'succeeded')::bigint AS translation_succeeded_count,
    count(*) FILTER (WHERE action_type = 'discover_domains' AND status IN ('pending', 'failed_retryable'))::bigint AS domain_pending_count,
    count(*) FILTER (WHERE action_type = 'discover_domains' AND status = 'running')::bigint AS domain_running_count,
    count(*) FILTER (WHERE action_type = 'discover_domains' AND status = 'succeeded')::bigint AS domain_succeeded_count
  FROM brreg_source.action_tasks
  GROUP BY company_id
)
```

Select the rollup columns with `coalesce(..., 0)`.

- [ ] **Step 3: Regenerate SQLC and run tests**

Run:

```bash
make sqlc-generate
cd scheduler && GOWORK=off go test ./internal/httpapi -run TestListBrregSourceEntries -count=1
```

Expected: PASS.

### Task 6: Full Verification

**Files:**
- All files touched above.

- [ ] **Step 1: Run backend tests**

Run:

```bash
cd scheduler && GOWORK=off go test ./...
```

Expected: PASS.

- [ ] **Step 2: Run UI checks**

Run:

```bash
cd ui && pnpm typecheck && pnpm build
```

Expected: PASS. Existing sourcemap warnings from shadcn-generated UI files are acceptable if build exits 0.

- [ ] **Step 3: Manual smoke test**

Start the normal local app stack, then open:

```text
http://localhost:8094/sources/brreg/source_entries
```

Verify:

- Source Entries tab loads.
- Action button opens source actions.
- Translation action has no advanced batch/provider controls.
- Domain discovery is disabled.
- Starting translation sends only IDs or filters plus limit/trigger.
- Task state counts come from `brreg_source.action_tasks`.

---

## Self-Review

- Spec coverage: The plan adds source-schema action tracking, supports multiple actions on the same source row, keeps source data separate from action state, removes user batch controls, and wires translation as the first executable action.
- Placeholder scan: No `TBD` or open-ended implementation placeholders remain.
- Type consistency: The plan consistently uses `brreg_source.action_tasks`, `translate_field`, `source_fingerprint`, and workflow-owned defaults.
