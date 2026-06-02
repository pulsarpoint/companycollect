# Temporal Schedule Management Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add reusable Corpscout UI/API support for manual workflow runs and Temporal schedule management, starting with the NACE taxonomy sync workflow.

**Architecture:** Corpscout stores local schedule metadata and the `temporal_schedule_id` in Postgres, while Temporal remains the execution source of truth for schedule specs, pause state, triggers, and workflow history. Backend endpoints merge local metadata with Temporal schedule descriptions for UI reads, and mutate Temporal first for schedule behavior. The UI exposes a reusable schedule editor component with simple presets plus an advanced cron field, and wires the first workflow definition to NACE taxonomy sync.

**Tech Stack:** PostgreSQL migrations, sqlc, Go Temporal SDK ScheduleClient, chi HTTP handlers, React Router 7, shadcn/Radix/Tailwind UI, lucide icons.

---

## Design Decisions

1. Temporal schedules are the execution source of truth.
2. Corpscout metadata is the product/admin source of truth for display name, purpose, tags, and which UI workflow definition a schedule belongs to.
3. The backend must not allow arbitrary workflow scheduling from the browser. Every schedulable workflow is allowlisted in Go.
4. The first allowlisted workflow is `SyncNACETaxonomy` on task queue `nace-taxonomy-sync`.
5. The schedule metadata table stores no secrets.
6. The UI should not add a third-party cron package in the first version. A small local editor is enough and keeps styling consistent with shadcn.
7. API handlers should use the concrete Temporal client already on `httpapi.Handlers`. Do not introduce generic service/facade layers.

---

## File Map

### Database

- Create `corpscout/database/migrations/000073_temporal_schedule_metadata.up.sql`
  - Creates `temporal_schedule_metadata`.
  - Creates `v_temporal_schedule_metadata`.
  - Adds constraints and indexes.

- Create `corpscout/database/migrations/000073_temporal_schedule_metadata.down.sql`
  - Drops the view and table.

- Create `corpscout/scheduler/internal/db/temporal_schedule_metadata_migration_test.go`
  - Verifies migration includes expected table, unique Temporal ID, JSON metadata object constraint, and view.

- Create `corpscout/database/queries/temporal_schedule_metadata.sql`
  - Adds sqlc queries for metadata insert/update/list/delete.

- Modify generated sqlc files after running sqlc:
  - `corpscout/scheduler/internal/db/gen/models.go`
  - `corpscout/scheduler/internal/db/gen/querier.go`
  - `corpscout/scheduler/internal/db/gen/temporal_schedule_metadata.sql.go`

### Backend Schedule API

- Create `corpscout/scheduler/internal/workflowschedules/registry.go`
  - Defines allowlisted workflow definitions.
  - Starts with the NACE taxonomy sync definition.

- Create `corpscout/scheduler/internal/workflowschedules/spec.go`
  - Converts API schedule input into Temporal `client.ScheduleSpec`.
  - Maps overlap policy strings to Temporal enum values.
  - Validates cron, timezone, and catchup window values.

- Create `corpscout/scheduler/internal/workflowschedules/spec_test.go`
  - Unit tests for cron validation, timezone validation, overlap mapping, NACE action input validation, and stable schedule IDs.

- Create `corpscout/scheduler/internal/httpapi/workflow_schedules.go`
  - Adds list/create/get/update/pause/resume/trigger/delete handlers.
  - Uses `h.temporal.ScheduleClient()` and `h.db`.
  - Logs detailed internal failures once and returns safe JSON errors.

- Create `corpscout/scheduler/internal/httpapi/workflow_schedules_test.go`
  - Tests request validation and response shape with a focused fake Temporal schedule client.

- Modify `corpscout/scheduler/internal/httpapi/handlers.go`
  - Registers `/api/v1/workflow-schedules` routes.

### UI

- Create `corpscout/ui/app/routes/settings.workflow-schedules.tsx`
  - Renders the schedule management page.

- Create `corpscout/ui/app/components/app/WorkflowScheduleManagement.tsx`
  - Lists schedules, runs NACE sync manually, opens create/edit sheet, and exposes pause/resume/trigger/delete actions.

- Create `corpscout/ui/app/components/app/ScheduleEditor.tsx`
  - Reusable schedule form with presets and advanced cron mode.

- Create `corpscout/ui/app/components/app/NACETaxonomySyncForm.tsx`
  - Reusable workflow-specific form for NACE sync input.

- Modify `corpscout/ui/app/lib/api.ts`
  - Adds workflow schedule API client methods.
  - Adds NACE manual trigger API client method if missing.

- Modify `corpscout/ui/app/types/api.ts`
  - Adds workflow schedule request/response types.

- Modify `corpscout/ui/app/components/app/AppSidebar.tsx`
  - Adds “Schedules” under settings-style navigation.

---

## API Shape

### List Schedules

`GET /api/v1/workflow-schedules`

Response:

```json
{
  "items": [
    {
      "id": "9d7fe042-6d43-4b8a-8f3f-7454c09b0661",
      "temporal_schedule_id": "nace-taxonomy-sync-nightly",
      "workflow_key": "nace_taxonomy_sync",
      "workflow_name": "SyncNACETaxonomy",
      "task_queue": "nace-taxonomy-sync",
      "domain": "taxonomy",
      "purpose": "nace_taxonomy_sync",
      "display_name": "Nightly NACE taxonomy sync",
      "description": "Downloads and imports NACE Rev. 2.1 taxonomy when the source changes.",
      "enabled": true,
      "tags": ["taxonomy", "nace"],
      "metadata": {},
      "spec": {
        "timezone": "Europe/Belgrade",
        "cron_expression": "0 3 * * *",
        "overlap_policy": "skip",
        "catchup_window_seconds": 3600
      },
      "action_input": {
        "revision": "NACE Rev. 2.1",
        "trigger": "schedule",
        "force_reprocess": false
      },
      "temporal": {
        "exists": true,
        "paused": false,
        "note": "",
        "next_run_at": "2026-06-03T03:00:00+02:00"
      },
      "created_at": "2026-06-02T12:00:00Z",
      "updated_at": "2026-06-02T12:00:00Z"
    }
  ]
}
```

### Create Schedule

`POST /api/v1/workflow-schedules`

Request:

```json
{
  "temporal_schedule_id": "nace-taxonomy-sync-nightly",
  "workflow_key": "nace_taxonomy_sync",
  "display_name": "Nightly NACE taxonomy sync",
  "description": "Downloads and imports NACE Rev. 2.1 taxonomy when the source changes.",
  "enabled": true,
  "tags": ["taxonomy", "nace"],
  "metadata": {},
  "spec": {
    "timezone": "Europe/Belgrade",
    "cron_expression": "0 3 * * *",
    "overlap_policy": "skip",
    "catchup_window_seconds": 3600
  },
  "action_input": {
    "revision": "NACE Rev. 2.1",
    "source_url": "",
    "trigger": "schedule",
    "force_reprocess": false
  }
}
```

Behavior:

1. Validate `workflow_key` against the allowlist.
2. Normalize and validate `temporal_schedule_id`.
3. Build the Temporal schedule action for `SyncNACETaxonomy`.
4. Create the Temporal schedule.
5. Insert local metadata.
6. If metadata insert fails after Temporal creation, delete the created Temporal schedule before returning an error.

### Update Schedule

`PATCH /api/v1/workflow-schedules/{temporal_schedule_id}`

Request uses the same shape as create, except `temporal_schedule_id` comes from the URL and cannot be changed.

Behavior:

1. Validate local metadata exists.
2. Validate workflow definition.
3. Update Temporal schedule spec/action.
4. Update local metadata.

### Trigger Schedule Now

`POST /api/v1/workflow-schedules/{temporal_schedule_id}/trigger`

Behavior:

1. Calls Temporal `Trigger`.
2. Returns JSON status.
3. Does not create a new local DB row.

### Pause Schedule

`POST /api/v1/workflow-schedules/{temporal_schedule_id}/pause`

Request:

```json
{
  "note": "Paused during maintenance"
}
```

### Resume Schedule

`POST /api/v1/workflow-schedules/{temporal_schedule_id}/resume`

Request:

```json
{
  "note": "Maintenance complete"
}
```

### Delete Schedule

`DELETE /api/v1/workflow-schedules/{temporal_schedule_id}`

Behavior:

1. Delete Temporal schedule.
2. Delete local metadata.
3. If Temporal schedule is already missing, still delete local metadata and return success with `temporal_missing=true`.

---

## Task 1: Add Schedule Metadata Migration

**Files:**
- Create: `corpscout/database/migrations/000073_temporal_schedule_metadata.up.sql`
- Create: `corpscout/database/migrations/000073_temporal_schedule_metadata.down.sql`
- Create: `corpscout/scheduler/internal/db/temporal_schedule_metadata_migration_test.go`

- [ ] **Step 1: Write the migration test**

Create `corpscout/scheduler/internal/db/temporal_schedule_metadata_migration_test.go`:

```go
package db_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTemporalScheduleMetadataMigration(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000073_temporal_schedule_metadata.up.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "CREATE TABLE temporal_schedule_metadata")
	require.Contains(t, sql, "temporal_schedule_id TEXT NOT NULL")
	require.Contains(t, sql, "UNIQUE (temporal_schedule_id)")
	require.Contains(t, sql, "workflow_key TEXT NOT NULL")
	require.Contains(t, sql, "workflow_name TEXT NOT NULL")
	require.Contains(t, sql, "task_queue TEXT NOT NULL")
	require.Contains(t, sql, "metadata JSONB NOT NULL DEFAULT '{}'::jsonb")
	require.Contains(t, sql, "jsonb_typeof(metadata) = 'object'")
	require.Contains(t, sql, "CREATE OR REPLACE VIEW v_temporal_schedule_metadata")
}

func TestTemporalScheduleMetadataDownMigration(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000073_temporal_schedule_metadata.down.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "DROP VIEW IF EXISTS v_temporal_schedule_metadata")
	require.Contains(t, sql, "DROP TABLE IF EXISTS temporal_schedule_metadata")
}
```

- [ ] **Step 2: Run the failing migration test**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/db -run TemporalScheduleMetadata -count=1
```

Expected:

```text
FAIL
open ../../../database/migrations/000073_temporal_schedule_metadata.up.sql: no such file or directory
```

- [ ] **Step 3: Create the up migration**

Create `corpscout/database/migrations/000073_temporal_schedule_metadata.up.sql`:

```sql
CREATE TABLE temporal_schedule_metadata (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  temporal_schedule_id TEXT NOT NULL,
  workflow_key TEXT NOT NULL,
  workflow_name TEXT NOT NULL,
  task_queue TEXT NOT NULL,
  domain TEXT NOT NULL,
  purpose TEXT NOT NULL,
  display_name TEXT NOT NULL,
  description TEXT,
  enabled BOOLEAN NOT NULL DEFAULT true,
  tags TEXT[] NOT NULL DEFAULT '{}',
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT uq_temporal_schedule_metadata_schedule_id UNIQUE (temporal_schedule_id),
  CONSTRAINT chk_temporal_schedule_metadata_schedule_id CHECK (btrim(temporal_schedule_id) <> ''),
  CONSTRAINT chk_temporal_schedule_metadata_workflow_key CHECK (btrim(workflow_key) <> ''),
  CONSTRAINT chk_temporal_schedule_metadata_workflow_name CHECK (btrim(workflow_name) <> ''),
  CONSTRAINT chk_temporal_schedule_metadata_task_queue CHECK (btrim(task_queue) <> ''),
  CONSTRAINT chk_temporal_schedule_metadata_domain CHECK (btrim(domain) <> ''),
  CONSTRAINT chk_temporal_schedule_metadata_purpose CHECK (btrim(purpose) <> ''),
  CONSTRAINT chk_temporal_schedule_metadata_display_name CHECK (btrim(display_name) <> ''),
  CONSTRAINT chk_temporal_schedule_metadata_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE INDEX idx_temporal_schedule_metadata_workflow_key
  ON temporal_schedule_metadata(workflow_key, enabled);

CREATE INDEX idx_temporal_schedule_metadata_domain_purpose
  ON temporal_schedule_metadata(domain, purpose);

CREATE INDEX idx_temporal_schedule_metadata_tags
  ON temporal_schedule_metadata USING gin(tags);

CREATE OR REPLACE FUNCTION set_temporal_schedule_metadata_updated_at()
RETURNS trigger AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_temporal_schedule_metadata_updated_at
BEFORE UPDATE ON temporal_schedule_metadata
FOR EACH ROW
EXECUTE FUNCTION set_temporal_schedule_metadata_updated_at();

CREATE OR REPLACE VIEW v_temporal_schedule_metadata AS
SELECT
  id,
  temporal_schedule_id,
  workflow_key,
  workflow_name,
  task_queue,
  domain,
  purpose,
  display_name,
  description,
  enabled,
  tags,
  metadata,
  created_at,
  updated_at
FROM temporal_schedule_metadata;
```

- [ ] **Step 4: Create the down migration**

Create `corpscout/database/migrations/000073_temporal_schedule_metadata.down.sql`:

```sql
DROP VIEW IF EXISTS v_temporal_schedule_metadata;
DROP TRIGGER IF EXISTS trg_temporal_schedule_metadata_updated_at ON temporal_schedule_metadata;
DROP FUNCTION IF EXISTS set_temporal_schedule_metadata_updated_at();
DROP TABLE IF EXISTS temporal_schedule_metadata;
```

- [ ] **Step 5: Run the migration test**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/db -run TemporalScheduleMetadata -count=1
```

Expected:

```text
ok  	github.com/pulsarpoint/corpscout/scheduler/internal/db
```

---

## Task 2: Add sqlc Queries For Schedule Metadata

**Files:**
- Create: `corpscout/database/queries/temporal_schedule_metadata.sql`
- Modify generated: `corpscout/scheduler/internal/db/gen/*.go`

- [ ] **Step 1: Add sqlc query file**

Create `corpscout/database/queries/temporal_schedule_metadata.sql`:

```sql
-- name: CreateTemporalScheduleMetadata :one
INSERT INTO temporal_schedule_metadata (
  temporal_schedule_id,
  workflow_key,
  workflow_name,
  task_queue,
  domain,
  purpose,
  display_name,
  description,
  enabled,
  tags,
  metadata
) VALUES (
  @temporal_schedule_id,
  @workflow_key,
  @workflow_name,
  @task_queue,
  @domain,
  @purpose,
  @display_name,
  @description,
  @enabled,
  @tags,
  @metadata
)
RETURNING *;

-- name: UpdateTemporalScheduleMetadata :one
UPDATE temporal_schedule_metadata
SET
  workflow_key = @workflow_key,
  workflow_name = @workflow_name,
  task_queue = @task_queue,
  domain = @domain,
  purpose = @purpose,
  display_name = @display_name,
  description = @description,
  enabled = @enabled,
  tags = @tags,
  metadata = @metadata
WHERE temporal_schedule_id = @temporal_schedule_id
RETURNING *;

-- name: GetTemporalScheduleMetadata :one
SELECT *
FROM temporal_schedule_metadata
WHERE temporal_schedule_id = @temporal_schedule_id;

-- name: ListTemporalScheduleMetadata :many
SELECT *
FROM temporal_schedule_metadata
WHERE
  (@workflow_key::text = '' OR workflow_key = @workflow_key)
  AND (@domain::text = '' OR domain = @domain)
ORDER BY created_at DESC, temporal_schedule_id ASC;

-- name: DeleteTemporalScheduleMetadata :exec
DELETE FROM temporal_schedule_metadata
WHERE temporal_schedule_id = @temporal_schedule_id;
```

- [ ] **Step 2: Regenerate sqlc**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
GOWORK=off sqlc generate -f database/sqlc.yaml
```

Expected:

```text
```

No output means generation succeeded.

- [ ] **Step 3: Verify generated symbols**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
rg -n "CreateTemporalScheduleMetadata|ListTemporalScheduleMetadata|TemporalScheduleMetadata" internal/db/gen
```

Expected: output includes generated query methods and model types.

---

## Task 3: Add Workflow Schedule Registry And Temporal Spec Builder

**Files:**
- Create: `corpscout/scheduler/internal/workflowschedules/registry.go`
- Create: `corpscout/scheduler/internal/workflowschedules/spec.go`
- Create: `corpscout/scheduler/internal/workflowschedules/spec_test.go`

- [ ] **Step 1: Write spec tests**

Create `corpscout/scheduler/internal/workflowschedules/spec_test.go`:

```go
package workflowschedules

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"

	"github.com/pulsarpoint/corpscout/scheduler/internal/nacetaxonomy"
)

func TestWorkflowDefinitionsIncludeNACETaxonomySync(t *testing.T) {
	def, ok := DefinitionByKey("nace_taxonomy_sync")
	require.True(t, ok)
	require.Equal(t, "nace_taxonomy_sync", def.Key)
	require.Equal(t, nacetaxonomy.SyncWorkflowName, def.WorkflowName)
	require.Equal(t, nacetaxonomy.SyncTaskQueue, def.TaskQueue)
	require.Equal(t, "taxonomy", def.Domain)
	require.Equal(t, "nace_taxonomy_sync", def.Purpose)
}

func TestBuildNACEScheduleActionInput(t *testing.T) {
	def, ok := DefinitionByKey("nace_taxonomy_sync")
	require.True(t, ok)

	input, err := def.DecodeActionInput(json.RawMessage(`{
		"revision": "NACE Rev. 2.1",
		"source_url": "https://example.test/nace.rdf",
		"trigger": "schedule",
		"force_reprocess": true
	}`))
	require.NoError(t, err)

	typed, ok := input.(nacetaxonomy.SyncNACETaxonomyInput)
	require.True(t, ok)
	require.Equal(t, "NACE Rev. 2.1", typed.Revision)
	require.Equal(t, "https://example.test/nace.rdf", typed.SourceURL)
	require.Equal(t, "schedule", typed.Trigger)
	require.True(t, typed.ForceReprocess)
}

func TestBuildScheduleSpecValidatesCron(t *testing.T) {
	spec, err := BuildScheduleSpec(ScheduleSpecInput{
		CronExpression:       "0 3 * * *",
		Timezone:             "Europe/Belgrade",
		OverlapPolicy:        "skip",
		CatchupWindowSeconds: 3600,
	})
	require.NoError(t, err)
	require.Equal(t, []string{"0 3 * * *"}, spec.CronExpressions)
	require.Equal(t, "Europe/Belgrade", spec.TimeZoneName)
}

func TestBuildScheduleSpecRejectsInvalidCron(t *testing.T) {
	_, err := BuildScheduleSpec(ScheduleSpecInput{
		CronExpression: "not enough fields",
		Timezone:       "Europe/Belgrade",
		OverlapPolicy:  "skip",
	})
	require.ErrorContains(t, err, "cron expression must contain 5 fields")
}

func TestBuildScheduleSpecRejectsInvalidTimezone(t *testing.T) {
	_, err := BuildScheduleSpec(ScheduleSpecInput{
		CronExpression: "0 3 * * *",
		Timezone:       "Mars/Olympus",
		OverlapPolicy:  "skip",
	})
	require.ErrorContains(t, err, "timezone is invalid")
}

func TestOverlapPolicyMapping(t *testing.T) {
	tests := map[string]enumspb.ScheduleOverlapPolicy{
		"skip":            enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
		"buffer_one":      enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE,
		"allow_all":       enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
		"cancel_other":    enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER,
		"terminate_other": enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER,
	}
	for value, expected := range tests {
		actual, err := OverlapPolicy(value)
		require.NoError(t, err)
		require.Equal(t, expected, actual)
	}
}
```

- [ ] **Step 2: Run failing tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/workflowschedules -count=1
```

Expected:

```text
FAIL
package github.com/pulsarpoint/corpscout/scheduler/internal/workflowschedules is not in std
```

- [ ] **Step 3: Create registry**

Create `corpscout/scheduler/internal/workflowschedules/registry.go`:

```go
package workflowschedules

import (
	"encoding/json"
	"strings"

	"github.com/cockroachdb/errors"

	"github.com/pulsarpoint/corpscout/scheduler/internal/nacetaxonomy"
)

type Definition struct {
	Key               string
	WorkflowName      string
	TaskQueue         string
	Domain            string
	Purpose           string
	DefaultDisplayName string
	DefaultDescription string
	DecodeActionInput func(json.RawMessage) (any, error)
}

func Definitions() []Definition {
	return []Definition{naceTaxonomySyncDefinition()}
}

func DefinitionByKey(key string) (Definition, bool) {
	key = strings.TrimSpace(key)
	for _, def := range Definitions() {
		if def.Key == key {
			return def, true
		}
	}
	return Definition{}, false
}

func naceTaxonomySyncDefinition() Definition {
	return Definition{
		Key:                "nace_taxonomy_sync",
		WorkflowName:       nacetaxonomy.SyncWorkflowName,
		TaskQueue:          nacetaxonomy.SyncTaskQueue,
		Domain:             "taxonomy",
		Purpose:            "nace_taxonomy_sync",
		DefaultDisplayName: "NACE taxonomy sync",
		DefaultDescription: "Downloads and imports the configured NACE taxonomy source when the source file changes.",
		DecodeActionInput:  decodeNACETaxonomySyncInput,
	}
}

func decodeNACETaxonomySyncInput(raw json.RawMessage) (any, error) {
	var input nacetaxonomy.SyncNACETaxonomyInput
	if len(raw) > 0 && string(raw) != "null" {
		if err := json.Unmarshal(raw, &input); err != nil {
			return nil, errors.New("invalid nace taxonomy sync action input")
		}
	}
	input.Revision = strings.TrimSpace(input.Revision)
	input.SourceURL = strings.TrimSpace(input.SourceURL)
	input.Trigger = strings.TrimSpace(input.Trigger)
	if input.Trigger == "" {
		input.Trigger = "schedule"
	}
	if input.Revision == "" {
		input.Revision = nacetaxonomy.DefaultRevision
	}
	if input.Trigger != "schedule" && input.Trigger != "manual" {
		return nil, errors.New("nace taxonomy sync trigger must be schedule or manual")
	}
	return input, nil
}
```

- [ ] **Step 4: Create spec builder**

Create `corpscout/scheduler/internal/workflowschedules/spec.go`:

```go
package workflowschedules

import (
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
)

const DefaultTimezone = "Europe/Belgrade"

type ScheduleSpecInput struct {
	CronExpression       string `json:"cron_expression"`
	Timezone             string `json:"timezone"`
	OverlapPolicy        string `json:"overlap_policy"`
	CatchupWindowSeconds int    `json:"catchup_window_seconds"`
}

func BuildScheduleSpec(input ScheduleSpecInput) (client.ScheduleSpec, error) {
	cronExpression := strings.TrimSpace(input.CronExpression)
	if cronExpression == "" {
		return client.ScheduleSpec{}, errors.New("cron expression is required")
	}
	if fields := strings.Fields(cronExpression); len(fields) != 5 {
		return client.ScheduleSpec{}, errors.New("cron expression must contain 5 fields")
	}

	timezone := strings.TrimSpace(input.Timezone)
	if timezone == "" {
		timezone = DefaultTimezone
	}
	if _, err := time.LoadLocation(timezone); err != nil {
		return client.ScheduleSpec{}, errors.New("timezone is invalid")
	}

	if input.CatchupWindowSeconds < 0 {
		return client.ScheduleSpec{}, errors.New("catchup window seconds cannot be negative")
	}

	return client.ScheduleSpec{
		CronExpressions: []string{cronExpression},
		TimeZoneName:    timezone,
	}, nil
}

func OverlapPolicy(value string) (enumspb.ScheduleOverlapPolicy, error) {
	switch strings.TrimSpace(value) {
	case "", "skip":
		return enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, nil
	case "buffer_one":
		return enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE, nil
	case "allow_all":
		return enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL, nil
	case "cancel_other":
		return enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER, nil
	case "terminate_other":
		return enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER, nil
	default:
		return enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED, errors.New("unsupported overlap policy")
	}
}
```

- [ ] **Step 5: Run workflow schedule tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/workflowschedules -count=1
```

Expected:

```text
ok  	github.com/pulsarpoint/corpscout/scheduler/internal/workflowschedules
```

---

## Task 4: Add Backend Schedule HTTP Endpoints

**Files:**
- Create: `corpscout/scheduler/internal/httpapi/workflow_schedules.go`
- Create: `corpscout/scheduler/internal/httpapi/workflow_schedules_test.go`
- Modify: `corpscout/scheduler/internal/httpapi/handlers.go`

- [ ] **Step 1: Add route registrations**

Modify `RegisterRoutes` in `corpscout/scheduler/internal/httpapi/handlers.go` inside `/api/v1`:

```go
r.Get("/workflow-schedules", h.handleListWorkflowSchedules)
r.Post("/workflow-schedules", h.handleCreateWorkflowSchedule)
r.Get("/workflow-schedules/{schedule_id}", h.handleGetWorkflowSchedule)
r.Patch("/workflow-schedules/{schedule_id}", h.handleUpdateWorkflowSchedule)
r.Post("/workflow-schedules/{schedule_id}/trigger", h.handleTriggerWorkflowSchedule)
r.Post("/workflow-schedules/{schedule_id}/pause", h.handlePauseWorkflowSchedule)
r.Post("/workflow-schedules/{schedule_id}/resume", h.handleResumeWorkflowSchedule)
r.Delete("/workflow-schedules/{schedule_id}", h.handleDeleteWorkflowSchedule)
```

- [ ] **Step 2: Create request/response structs and validation**

Create `corpscout/scheduler/internal/httpapi/workflow_schedules.go` with these top-level types:

```go
package httpapi

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/go-chi/chi/v5"
	"go.temporal.io/sdk/client"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
	"github.com/pulsarpoint/corpscout/scheduler/internal/workflowschedules"
)

type workflowScheduleSpecRequest struct {
	Timezone             string `json:"timezone"`
	CronExpression       string `json:"cron_expression"`
	OverlapPolicy        string `json:"overlap_policy"`
	CatchupWindowSeconds int    `json:"catchup_window_seconds"`
}

type workflowScheduleRequest struct {
	TemporalScheduleID string                      `json:"temporal_schedule_id"`
	WorkflowKey        string                      `json:"workflow_key"`
	DisplayName        string                      `json:"display_name"`
	Description        string                      `json:"description"`
	Enabled            bool                        `json:"enabled"`
	Tags               []string                    `json:"tags"`
	Metadata           map[string]any              `json:"metadata"`
	Spec               workflowScheduleSpecRequest `json:"spec"`
	ActionInput         json.RawMessage             `json:"action_input"`
}

type workflowScheduleTemporalState struct {
	Exists    bool   `json:"exists"`
	Paused    bool   `json:"paused"`
	Note      string `json:"note"`
	NextRunAt string `json:"next_run_at,omitempty"`
}

type workflowScheduleResponse struct {
	ID                 string                        `json:"id"`
	TemporalScheduleID string                        `json:"temporal_schedule_id"`
	WorkflowKey        string                        `json:"workflow_key"`
	WorkflowName       string                        `json:"workflow_name"`
	TaskQueue          string                        `json:"task_queue"`
	Domain             string                        `json:"domain"`
	Purpose            string                        `json:"purpose"`
	DisplayName        string                        `json:"display_name"`
	Description        string                        `json:"description,omitempty"`
	Enabled            bool                          `json:"enabled"`
	Tags               []string                      `json:"tags"`
	Metadata           map[string]any                `json:"metadata"`
	Spec               workflowScheduleSpecRequest   `json:"spec"`
	ActionInput         map[string]any                `json:"action_input"`
	Temporal           workflowScheduleTemporalState `json:"temporal"`
	CreatedAt          string                        `json:"created_at"`
	UpdatedAt          string                        `json:"updated_at"`
}

type workflowScheduleListResponse struct {
	Items []workflowScheduleResponse `json:"items"`
}
```

- [ ] **Step 3: Add create handler**

Add this handler in `workflow_schedules.go`:

```go
func (h *Handlers) handleCreateWorkflowSchedule(w http.ResponseWriter, r *http.Request) {
	if h.temporal == nil || h.db == nil {
		writeError(w, http.StatusServiceUnavailable, "workflow scheduling is not configured")
		return
	}

	req, err := decodeWorkflowScheduleRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	def, scheduleSpec, overlapPolicy, actionInput, err := buildWorkflowScheduleDefinition(req)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	scheduleID := req.TemporalScheduleID
	scheduleClient := h.temporal.ScheduleClient()
	_, err = scheduleClient.Create(r.Context(), client.ScheduleOptions{
		ID:   scheduleID,
		Spec: scheduleSpec,
		Action: &client.ScheduleWorkflowAction{
			ID:        scheduleID + "-workflow",
			Workflow:  def.WorkflowName,
			TaskQueue: def.TaskQueue,
			Args:      []any{actionInput},
		},
		Overlap: overlapPolicy,
	})
	if err != nil {
		slog.ErrorContext(r.Context(), "create temporal schedule", "error", err, "schedule_id", scheduleID, "workflow_key", req.WorkflowKey)
		writeError(w, http.StatusBadGateway, "create temporal schedule failed")
		return
	}

	metadataJSON, err := json.Marshal(nonNilMap(req.Metadata))
	if err != nil {
		_ = scheduleClient.GetHandle(r.Context(), scheduleID).Delete(r.Context())
		writeError(w, http.StatusBadRequest, "schedule metadata must be a JSON object")
		return
	}
	row, err := h.db.CreateTemporalScheduleMetadata(r.Context(), db.CreateTemporalScheduleMetadataParams{
		TemporalScheduleID: scheduleID,
		WorkflowKey:        def.Key,
		WorkflowName:       def.WorkflowName,
		TaskQueue:          def.TaskQueue,
		Domain:             def.Domain,
		Purpose:            def.Purpose,
		DisplayName:        req.DisplayName,
		Description:        nullableText(req.Description),
		Enabled:            req.Enabled,
		Tags:               normalizeTags(req.Tags),
		Metadata:           metadataJSON,
	})
	if err != nil {
		_ = scheduleClient.GetHandle(r.Context(), scheduleID).Delete(r.Context())
		slog.ErrorContext(r.Context(), "store temporal schedule metadata", "error", err, "schedule_id", scheduleID)
		writeError(w, http.StatusInternalServerError, "store schedule metadata failed")
		return
	}

	response, err := h.workflowScheduleResponse(r.Context(), row)
	if err != nil {
		writeError(w, http.StatusCreated, "schedule created but response could not be loaded")
		return
	}
	writeJSON(w, http.StatusCreated, response)
}
```

Implementation notes for this step:

- Use `client.ScheduleWorkflowAction` and `client.ScheduleOptions` names from the Temporal SDK version in this repo.
- If the SDK field names differ, adapt this handler and `workflow_schedules_test.go` together in the same commit.
- Keep the behavior unchanged: Temporal create first, local metadata second, delete Temporal schedule if metadata insert fails.

- [ ] **Step 4: Add remaining handlers**

Add handlers in the same file:

```go
func (h *Handlers) handleListWorkflowSchedules(w http.ResponseWriter, r *http.Request) {
	rows, err := h.db.ListTemporalScheduleMetadata(r.Context(), db.ListTemporalScheduleMetadataParams{
		WorkflowKey: strings.TrimSpace(r.URL.Query().Get("workflow_key")),
		Domain:      strings.TrimSpace(r.URL.Query().Get("domain")),
	})
	if err != nil {
		slog.ErrorContext(r.Context(), "list temporal schedule metadata", "error", err)
		writeError(w, http.StatusInternalServerError, "list workflow schedules failed")
		return
	}

	items := make([]workflowScheduleResponse, 0, len(rows))
	for _, row := range rows {
		item, err := h.workflowScheduleResponse(r.Context(), row)
		if err != nil {
			slog.WarnContext(r.Context(), "describe temporal schedule failed", "error", err, "schedule_id", row.TemporalScheduleID)
			item = workflowScheduleResponse{
				ID:                 row.ID.String(),
				TemporalScheduleID: row.TemporalScheduleID,
				WorkflowKey:        row.WorkflowKey,
				WorkflowName:       row.WorkflowName,
				TaskQueue:          row.TaskQueue,
				Domain:             row.Domain,
				Purpose:            row.Purpose,
				DisplayName:        row.DisplayName,
				Enabled:            row.Enabled,
				Tags:               row.Tags,
				Metadata:           map[string]any{},
				Temporal:           workflowScheduleTemporalState{Exists: false},
				CreatedAt:          row.CreatedAt.Time.Format(time.RFC3339),
				UpdatedAt:          row.UpdatedAt.Time.Format(time.RFC3339),
			}
		}
		items = append(items, item)
	}
	writeJSON(w, http.StatusOK, workflowScheduleListResponse{Items: items})
}

func (h *Handlers) handleTriggerWorkflowSchedule(w http.ResponseWriter, r *http.Request) {
	scheduleID := chi.URLParam(r, "schedule_id")
	if scheduleID == "" {
		writeError(w, http.StatusBadRequest, "schedule id is required")
		return
	}
	if err := h.temporal.ScheduleClient().GetHandle(r.Context(), scheduleID).Trigger(r.Context(), client.ScheduleTriggerOptions{}); err != nil {
		slog.ErrorContext(r.Context(), "trigger temporal schedule", "error", err, "schedule_id", scheduleID)
		writeError(w, http.StatusBadGateway, "trigger workflow schedule failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "triggered", "temporal_schedule_id": scheduleID})
}
```

For `handleGetWorkflowSchedule`, `handleUpdateWorkflowSchedule`, `handlePauseWorkflowSchedule`, `handleResumeWorkflowSchedule`, and `handleDeleteWorkflowSchedule`, implement the same boundary rules:

- Log internal Temporal/SQL errors once with `slog`.
- Return safe JSON errors.
- Delete local metadata on delete even when Temporal schedule is missing.
- For pause/resume, accept an optional `note` field and pass it to Temporal pause/unpause options.

- [ ] **Step 5: Add helper functions**

Add helpers in the same file:

```go
func decodeWorkflowScheduleRequest(r *http.Request) (workflowScheduleRequest, error) {
	var req workflowScheduleRequest
	if err := decodeJSON(r, &req); err != nil {
		return workflowScheduleRequest{}, errors.New("invalid request body")
	}
	req.TemporalScheduleID = strings.TrimSpace(req.TemporalScheduleID)
	req.WorkflowKey = strings.TrimSpace(req.WorkflowKey)
	req.DisplayName = strings.TrimSpace(req.DisplayName)
	req.Description = strings.TrimSpace(req.Description)
	req.Spec.CronExpression = strings.TrimSpace(req.Spec.CronExpression)
	req.Spec.Timezone = strings.TrimSpace(req.Spec.Timezone)
	req.Spec.OverlapPolicy = strings.TrimSpace(req.Spec.OverlapPolicy)

	if req.TemporalScheduleID == "" {
		return workflowScheduleRequest{}, errors.New("temporal schedule id is required")
	}
	if strings.ContainsAny(req.TemporalScheduleID, " \t\r\n/") {
		return workflowScheduleRequest{}, errors.New("temporal schedule id cannot contain whitespace or slash")
	}
	if req.WorkflowKey == "" {
		return workflowScheduleRequest{}, errors.New("workflow key is required")
	}
	if req.DisplayName == "" {
		return workflowScheduleRequest{}, errors.New("display name is required")
	}
	return req, nil
}

func buildWorkflowScheduleDefinition(req workflowScheduleRequest) (workflowschedules.Definition, client.ScheduleSpec, enumspb.ScheduleOverlapPolicy, any, error) {
	def, ok := workflowschedules.DefinitionByKey(req.WorkflowKey)
	if !ok {
		return workflowschedules.Definition{}, client.ScheduleSpec{}, enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED, nil, errors.New("workflow key is not schedulable")
	}
	spec, err := workflowschedules.BuildScheduleSpec(workflowschedules.ScheduleSpecInput{
		CronExpression:       req.Spec.CronExpression,
		Timezone:             req.Spec.Timezone,
		OverlapPolicy:        req.Spec.OverlapPolicy,
		CatchupWindowSeconds: req.Spec.CatchupWindowSeconds,
	})
	if err != nil {
		return workflowschedules.Definition{}, client.ScheduleSpec{}, enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED, nil, err
	}
	overlap, err := workflowschedules.OverlapPolicy(req.Spec.OverlapPolicy)
	if err != nil {
		return workflowschedules.Definition{}, client.ScheduleSpec{}, enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED, nil, err
	}
	actionInput, err := def.DecodeActionInput(req.ActionInput)
	if err != nil {
		return workflowschedules.Definition{}, client.ScheduleSpec{}, enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED, nil, err
	}
	return def, spec, overlap, actionInput, nil
}
```

- [ ] **Step 6: Write handler tests**

Create `corpscout/scheduler/internal/httpapi/workflow_schedules_test.go`.

Test cases:

```go
func TestCreateWorkflowScheduleRejectsUnknownWorkflowKey(t *testing.T)
func TestCreateWorkflowScheduleRejectsInvalidCron(t *testing.T)
func TestCreateWorkflowScheduleCreatesTemporalScheduleAndMetadata(t *testing.T)
func TestListWorkflowSchedulesReturnsMetadataAndTemporalState(t *testing.T)
func TestTriggerWorkflowScheduleCallsTemporal(t *testing.T)
func TestDeleteWorkflowScheduleRemovesMetadataWhenTemporalMissing(t *testing.T)
```

The fake should implement only the Temporal schedule methods used by these handlers. Keep the fake local to the test file so the production code does not gain a new abstraction.

- [ ] **Step 7: Run backend tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/workflowschedules ./internal/httpapi ./internal/db -count=1
```

Expected:

```text
ok  	github.com/pulsarpoint/corpscout/scheduler/internal/workflowschedules
ok  	github.com/pulsarpoint/corpscout/scheduler/internal/httpapi
ok  	github.com/pulsarpoint/corpscout/scheduler/internal/db
```

---

## Task 5: Add UI API Types And Client Methods

**Files:**
- Modify: `corpscout/ui/app/types/api.ts`
- Modify: `corpscout/ui/app/lib/api.ts`

- [ ] **Step 1: Add TypeScript types**

Append to `corpscout/ui/app/types/api.ts`:

```ts
export interface WorkflowScheduleSpec {
  timezone: string;
  cron_expression: string;
  overlap_policy: "skip" | "buffer_one" | "allow_all" | "cancel_other" | "terminate_other";
  catchup_window_seconds: number;
}

export interface WorkflowScheduleTemporalState {
  exists: boolean;
  paused: boolean;
  note: string;
  next_run_at?: string;
}

export interface WorkflowSchedule {
  id: string;
  temporal_schedule_id: string;
  workflow_key: "nace_taxonomy_sync";
  workflow_name: string;
  task_queue: string;
  domain: string;
  purpose: string;
  display_name: string;
  description?: string;
  enabled: boolean;
  tags: string[];
  metadata: Record<string, unknown>;
  spec: WorkflowScheduleSpec;
  action_input: Record<string, unknown>;
  temporal: WorkflowScheduleTemporalState;
  created_at: string;
  updated_at: string;
}

export interface WorkflowScheduleListResponse {
  items: WorkflowSchedule[];
}

export interface WorkflowScheduleInput {
  temporal_schedule_id: string;
  workflow_key: "nace_taxonomy_sync";
  display_name: string;
  description?: string;
  enabled: boolean;
  tags: string[];
  metadata: Record<string, unknown>;
  spec: WorkflowScheduleSpec;
  action_input: Record<string, unknown>;
}

export interface NACETaxonomySyncRequest {
  revision?: string;
  source_url?: string;
  trigger?: "manual" | "schedule";
  force_reprocess?: boolean;
}

export interface StartWorkflowResponse {
  status: string;
  workflow: string;
  workflow_id: string;
  run_id: string;
}
```

- [ ] **Step 2: Add imports in api client**

Modify the type import list in `corpscout/ui/app/lib/api.ts` to include:

```ts
  WorkflowSchedule,
  WorkflowScheduleInput,
  WorkflowScheduleListResponse,
  NACETaxonomySyncRequest,
  StartWorkflowResponse,
```

- [ ] **Step 3: Add API client methods**

Add this helper near the existing `get`, `post`, and `patch` helpers in `corpscout/ui/app/lib/api.ts`:

```ts
async function del<T>(path: string): Promise<T> {
  const res = await fetch(BASE + path, { method: "DELETE" });
  if (!res.ok) throw await responseError(res);
  return res.json() as Promise<T>;
}
```

Add methods to `api` in `corpscout/ui/app/lib/api.ts`:

```ts
  getWorkflowSchedules: () =>
    get<WorkflowScheduleListResponse>("/workflow-schedules"),

  createWorkflowSchedule: (body: WorkflowScheduleInput) =>
    post<WorkflowSchedule>("/workflow-schedules", body),

  updateWorkflowSchedule: (scheduleId: string, body: WorkflowScheduleInput) =>
    patch<WorkflowSchedule>(
      `/workflow-schedules/${encodeURIComponent(scheduleId)}`,
      body,
    ),

  triggerWorkflowSchedule: (scheduleId: string) =>
    post<{ status: string; temporal_schedule_id: string }>(
      `/workflow-schedules/${encodeURIComponent(scheduleId)}/trigger`,
      {},
    ),

  pauseWorkflowSchedule: (scheduleId: string, note = "") =>
    post<{ status: string; temporal_schedule_id: string }>(
      `/workflow-schedules/${encodeURIComponent(scheduleId)}/pause`,
      { note },
    ),

  resumeWorkflowSchedule: (scheduleId: string, note = "") =>
    post<{ status: string; temporal_schedule_id: string }>(
      `/workflow-schedules/${encodeURIComponent(scheduleId)}/resume`,
      { note },
    ),

  deleteWorkflowSchedule: (scheduleId: string) =>
    del<{ status: string; temporal_schedule_id: string }>(
      `/workflow-schedules/${encodeURIComponent(scheduleId)}`,
    ),

  startNACETaxonomySync: (body: NACETaxonomySyncRequest = {}) =>
    post<StartWorkflowResponse>("/workflows/nace/taxonomy-sync", body),
```

- [ ] **Step 4: Run TypeScript check**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/ui
pnpm typecheck
```

Expected: typecheck passes or fails only because UI components in later tasks do not exist yet. If it fails for duplicate type names or malformed imports, fix those before moving on.

---

## Task 6: Build Reusable Schedule Editor UI

**Files:**
- Create: `corpscout/ui/app/components/app/ScheduleEditor.tsx`

- [ ] **Step 1: Create reusable schedule editor**

Create `corpscout/ui/app/components/app/ScheduleEditor.tsx`:

```tsx
import { useMemo, useState } from "react";
import { Clock3 } from "lucide-react";
import { Button } from "~/components/ui/button";
import { Input } from "~/components/ui/input";
import { Label } from "~/components/ui/label";
import { Separator } from "~/components/ui/separator";
import { Switch } from "~/components/ui/switch";
import type { WorkflowScheduleSpec } from "~/types/api";

type ScheduleMode = "daily" | "weekly" | "monthly" | "advanced";

interface ScheduleEditorProps {
  spec: WorkflowScheduleSpec;
  enabled: boolean;
  onSpecChange: (spec: WorkflowScheduleSpec) => void;
  onEnabledChange: (enabled: boolean) => void;
}

const weekdays = [
  { label: "Monday", value: "1" },
  { label: "Tuesday", value: "2" },
  { label: "Wednesday", value: "3" },
  { label: "Thursday", value: "4" },
  { label: "Friday", value: "5" },
  { label: "Saturday", value: "6" },
  { label: "Sunday", value: "0" },
] as const;

export function ScheduleEditor({
  spec,
  enabled,
  onSpecChange,
  onEnabledChange,
}: ScheduleEditorProps) {
  const [mode, setMode] = useState<ScheduleMode>(() => modeFromCron(spec.cron_expression));
  const [time, setTime] = useState(() => timeFromCron(spec.cron_expression));
  const [weekday, setWeekday] = useState(() => weekdayFromCron(spec.cron_expression));
  const [monthDay, setMonthDay] = useState(() => monthDayFromCron(spec.cron_expression));

  const preview = useMemo(() => describeSchedule(spec), [spec]);

  function updatePreset(nextMode: ScheduleMode, nextTime = time, nextWeekday = weekday, nextMonthDay = monthDay) {
    setMode(nextMode);
    setTime(nextTime);
    setWeekday(nextWeekday);
    setMonthDay(nextMonthDay);
    if (nextMode === "advanced") return;
    onSpecChange({
      ...spec,
      cron_expression: cronFromPreset(nextMode, nextTime, nextWeekday, nextMonthDay),
    });
  }

  return (
    <div className="flex flex-col gap-4">
      <div className="flex items-center justify-between gap-4">
        <div>
          <Label className="text-sm font-medium">Enabled</Label>
          <p className="text-xs text-muted-foreground">
            Temporal will run this schedule only when it is enabled and not paused.
          </p>
        </div>
        <Switch checked={enabled} onCheckedChange={onEnabledChange} />
      </div>

      <Separator />

      <div className="grid gap-2">
        <Label>Frequency</Label>
        <div className="grid grid-cols-2 gap-2 sm:grid-cols-4">
          {(["daily", "weekly", "monthly", "advanced"] as ScheduleMode[]).map((item) => (
            <Button
              key={item}
              type="button"
              variant={mode === item ? "default" : "outline"}
              onClick={() => updatePreset(item)}
              className="justify-center capitalize"
            >
              {item}
            </Button>
          ))}
        </div>
      </div>

      {mode !== "advanced" && (
        <div className="grid gap-4 sm:grid-cols-3">
          <div className="grid gap-2">
            <Label htmlFor="schedule-time">Time</Label>
            <Input
              id="schedule-time"
              type="time"
              value={time}
              onChange={(event) => updatePreset(mode, event.target.value)}
            />
          </div>
          {mode === "weekly" && (
            <div className="grid gap-2">
              <Label htmlFor="schedule-weekday">Weekday</Label>
              <select
                id="schedule-weekday"
                value={weekday}
                onChange={(event) => updatePreset(mode, time, event.target.value)}
                className="h-10 rounded-md border border-input bg-background px-3 text-sm"
              >
                {weekdays.map((day) => (
                  <option key={day.value} value={day.value}>
                    {day.label}
                  </option>
                ))}
              </select>
            </div>
          )}
          {mode === "monthly" && (
            <div className="grid gap-2">
              <Label htmlFor="schedule-month-day">Day of month</Label>
              <Input
                id="schedule-month-day"
                type="number"
                min={1}
                max={28}
                value={monthDay}
                onChange={(event) => updatePreset(mode, time, weekday, event.target.value)}
              />
              <p className="text-xs text-muted-foreground">Use 1-28 so every month has the selected day.</p>
            </div>
          )}
        </div>
      )}

      <div className="grid gap-2">
        <Label htmlFor="schedule-cron">Cron expression</Label>
        <Input
          id="schedule-cron"
          value={spec.cron_expression}
          readOnly={mode !== "advanced"}
          onChange={(event) => onSpecChange({ ...spec, cron_expression: event.target.value })}
        />
        <p className="text-xs text-muted-foreground">
          Five-field cron expression interpreted in the configured timezone.
        </p>
      </div>

      <div className="grid gap-4 sm:grid-cols-3">
        <div className="grid gap-2">
          <Label htmlFor="schedule-timezone">Timezone</Label>
          <Input
            id="schedule-timezone"
            value={spec.timezone}
            onChange={(event) => onSpecChange({ ...spec, timezone: event.target.value })}
          />
        </div>
        <div className="grid gap-2">
          <Label htmlFor="schedule-overlap">Overlap policy</Label>
          <select
            id="schedule-overlap"
            value={spec.overlap_policy}
            onChange={(event) =>
              onSpecChange({
                ...spec,
                overlap_policy: event.target.value as WorkflowScheduleSpec["overlap_policy"],
              })
            }
            className="h-10 rounded-md border border-input bg-background px-3 text-sm"
          >
            <option value="skip">Skip when previous run is active</option>
            <option value="buffer_one">Buffer one run</option>
            <option value="allow_all">Allow overlapping runs</option>
            <option value="cancel_other">Cancel previous run</option>
            <option value="terminate_other">Terminate previous run</option>
          </select>
        </div>
        <div className="grid gap-2">
          <Label htmlFor="schedule-catchup">Catchup window seconds</Label>
          <Input
            id="schedule-catchup"
            type="number"
            min={0}
            value={spec.catchup_window_seconds}
            onChange={(event) =>
              onSpecChange({
                ...spec,
                catchup_window_seconds: Number(event.target.value) || 0,
              })
            }
          />
        </div>
      </div>

      <div className="flex items-center gap-2 rounded-md border bg-muted/30 px-3 py-2 text-sm">
        <Clock3 className="size-4 text-muted-foreground" />
        <span>{preview}</span>
      </div>
    </div>
  );
}

function modeFromCron(cron: string): ScheduleMode {
  const fields = cron.trim().split(/\s+/);
  if (fields.length !== 5) return "advanced";
  if (fields[2] !== "*" && fields[3] === "*" && fields[4] === "*") return "monthly";
  if (fields[2] === "*" && fields[3] === "*" && fields[4] !== "*") return "weekly";
  if (fields[2] === "*" && fields[3] === "*" && fields[4] === "*") return "daily";
  return "advanced";
}

function timeFromCron(cron: string): string {
  const fields = cron.trim().split(/\s+/);
  if (fields.length !== 5) return "03:00";
  const minute = fields[0].padStart(2, "0");
  const hour = fields[1].padStart(2, "0");
  return `${hour}:${minute}`;
}

function weekdayFromCron(cron: string): string {
  const fields = cron.trim().split(/\s+/);
  return fields.length === 5 && fields[4] !== "*" ? fields[4] : "1";
}

function monthDayFromCron(cron: string): string {
  const fields = cron.trim().split(/\s+/);
  return fields.length === 5 && fields[2] !== "*" ? fields[2] : "1";
}

function cronFromPreset(mode: ScheduleMode, time: string, weekday: string, monthDay: string): string {
  const [hour = "3", minute = "0"] = time.split(":");
  if (mode === "weekly") return `${Number(minute)} ${Number(hour)} * * ${weekday}`;
  if (mode === "monthly") return `${Number(minute)} ${Number(hour)} ${monthDay} * *`;
  return `${Number(minute)} ${Number(hour)} * * *`;
}

function describeSchedule(spec: WorkflowScheduleSpec): string {
  return `Runs with cron ${spec.cron_expression} in ${spec.timezone || "Europe/Belgrade"}.`;
}
```

- [ ] **Step 2: Run TypeScript check**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/ui
pnpm typecheck
```

Expected: typecheck passes for `ScheduleEditor.tsx`.

---

## Task 7: Build NACE Workflow Form And Schedule Management Page

**Files:**
- Create: `corpscout/ui/app/components/app/NACETaxonomySyncForm.tsx`
- Create: `corpscout/ui/app/components/app/WorkflowScheduleManagement.tsx`
- Create: `corpscout/ui/app/routes/settings.workflow-schedules.tsx`
- Modify: `corpscout/ui/app/components/app/AppSidebar.tsx`

- [ ] **Step 1: Create NACE form**

Create `corpscout/ui/app/components/app/NACETaxonomySyncForm.tsx`:

```tsx
import { Input } from "~/components/ui/input";
import { Label } from "~/components/ui/label";
import { Switch } from "~/components/ui/switch";

export interface NACETaxonomySyncFormValue {
  revision: string;
  source_url: string;
  force_reprocess: boolean;
}

interface NACETaxonomySyncFormProps {
  value: NACETaxonomySyncFormValue;
  onChange: (value: NACETaxonomySyncFormValue) => void;
}

export function NACETaxonomySyncForm({ value, onChange }: NACETaxonomySyncFormProps) {
  return (
    <div className="grid gap-4">
      <div className="grid gap-2">
        <Label htmlFor="nace-revision">Revision</Label>
        <Input
          id="nace-revision"
          value={value.revision}
          onChange={(event) => onChange({ ...value, revision: event.target.value })}
        />
        <p className="text-xs text-muted-foreground">Stored on imported NACE classifications and codes.</p>
      </div>
      <div className="grid gap-2">
        <Label htmlFor="nace-source-url">Source URL</Label>
        <Input
          id="nace-source-url"
          value={value.source_url}
          onChange={(event) => onChange({ ...value, source_url: event.target.value })}
          placeholder="Use backend default when empty"
        />
        <p className="text-xs text-muted-foreground">Leave empty to use CORPSCOUT_NACE_REV21_SOURCE_URL from the scheduler.</p>
      </div>
      <div className="flex items-center justify-between gap-4">
        <div>
          <Label>Force reprocess</Label>
          <p className="text-xs text-muted-foreground">
            Re-import even when the downloaded source hash was already processed.
          </p>
        </div>
        <Switch
          checked={value.force_reprocess}
          onCheckedChange={(checked) => onChange({ ...value, force_reprocess: checked })}
        />
      </div>
    </div>
  );
}
```

- [ ] **Step 2: Create management component**

Create `corpscout/ui/app/components/app/WorkflowScheduleManagement.tsx`.

The component must:

1. Load schedules from `api.getWorkflowSchedules()`.
2. Show a “Run NACE sync now” section using `api.startNACETaxonomySync`.
3. Show schedules in a table.
4. Open a sheet for create/edit.
5. Use `ScheduleEditor`.
6. Use `NACETaxonomySyncForm`.
7. Expose actions: trigger, pause/resume, delete.

Initial state values:

```tsx
const defaultScheduleSpec = {
  timezone: "Europe/Belgrade",
  cron_expression: "0 3 * * *",
  overlap_policy: "skip" as const,
  catchup_window_seconds: 3600,
};

const defaultNACEInput = {
  revision: "NACE Rev. 2.1",
  source_url: "",
  force_reprocess: false,
};
```

Use these imports:

```tsx
import { useEffect, useMemo, useState } from "react";
import { Pause, Play, Plus, RefreshCw, Save, Trash2 } from "lucide-react";
import { Alert, AlertDescription } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { Input } from "~/components/ui/input";
import { Label } from "~/components/ui/label";
import { Separator } from "~/components/ui/separator";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "~/components/ui/sheet";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";
import { api, errorMessage } from "~/lib/api";
import type { WorkflowSchedule, WorkflowScheduleInput, WorkflowScheduleSpec } from "~/types/api";
import { ScheduleEditor } from "~/components/app/ScheduleEditor";
import { NACETaxonomySyncForm, type NACETaxonomySyncFormValue } from "~/components/app/NACETaxonomySyncForm";
```

The save body must be:

```ts
const body: WorkflowScheduleInput = {
  temporal_schedule_id: form.scheduleID.trim(),
  workflow_key: "nace_taxonomy_sync",
  display_name: form.displayName.trim(),
  description: form.description.trim(),
  enabled,
  tags: ["taxonomy", "nace"],
  metadata: {},
  spec,
  action_input: {
    revision: naceInput.revision.trim(),
    source_url: naceInput.source_url.trim(),
    trigger: "schedule",
    force_reprocess: naceInput.force_reprocess,
  },
};
```

- [ ] **Step 3: Create route**

Create `corpscout/ui/app/routes/settings.workflow-schedules.tsx`:

```tsx
import { WorkflowScheduleManagement } from "~/components/app/WorkflowScheduleManagement";

export default function WorkflowSchedulesRoute() {
  return <WorkflowScheduleManagement />;
}
```

- [ ] **Step 4: Add sidebar item**

Modify `corpscout/ui/app/components/app/AppSidebar.tsx`:

1. Import `Clock3`.
2. Add this nav item after LLM Providers:

```tsx
{ title: "Schedules", url: "/settings/workflow-schedules", icon: Clock3 },
```

- [ ] **Step 5: Run UI verification**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/ui
pnpm typecheck
pnpm build
```

Expected:

```text
```

Both commands complete successfully.

---

## Task 8: Manual Browser Smoke Test

**Files:**
- No code changes expected.

- [ ] **Step 1: Start local app stack**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
docker compose up -d scheduler ui
```

Expected:

```text
Container ... Started
```

- [ ] **Step 2: Open schedules page**

Open:

```text
http://localhost:8094/settings/workflow-schedules
```

Expected visible behavior:

1. Sidebar shows “Schedules”.
2. Page title shows workflow schedules.
3. NACE manual run card is visible.
4. Create schedule button opens a sheet.
5. Frequency preset buttons update the cron expression.
6. Advanced mode makes the cron input editable.

- [ ] **Step 3: Create a disabled NACE schedule**

Use:

```text
Schedule ID: nace-taxonomy-sync-test
Display name: Test NACE taxonomy sync
Enabled: off
Frequency: daily
Time: 03:00
Timezone: Europe/Belgrade
Revision: NACE Rev. 2.1
Source URL: empty
Force reprocess: off
```

Expected:

1. Create request succeeds.
2. Schedule appears in the table.
3. Status shows the Temporal state.

- [ ] **Step 4: Exercise schedule controls**

Click:

1. Trigger now.
2. Pause.
3. Resume.
4. Delete.

Expected:

1. Trigger shows a success message.
2. Pause changes table state.
3. Resume changes table state.
4. Delete removes the row.

---

## Task 9: Final Verification

**Files:**
- No code changes expected unless verification finds defects.

- [ ] **Step 1: Run scheduler tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./...
```

Expected:

```text
ok  	...
```

- [ ] **Step 2: Run UI checks**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/ui
pnpm typecheck
pnpm build
```

Expected:

```text
```

Both commands complete successfully.

- [ ] **Step 3: Check formatting and migration order**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git diff --check
ls corpscout/database/migrations/000073_temporal_schedule_metadata.*
```

Expected:

```text
corpscout/database/migrations/000073_temporal_schedule_metadata.down.sql
corpscout/database/migrations/000073_temporal_schedule_metadata.up.sql
```

- [ ] **Step 4: Review changed files**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git status --short
git diff --stat
```

Expected:

1. New migration files are present.
2. New `workflowschedules` package is present.
3. New workflow schedule HTTP handlers are present.
4. New UI schedule page/components are present.
5. No unrelated file churn is present.

---

## Self-Review Checklist

- [ ] The plan stores local schedule metadata and Temporal schedule ID in Postgres.
- [ ] Temporal remains source of truth for schedule execution.
- [ ] API can create, list, update, trigger, pause, resume, and delete schedules.
- [ ] The browser cannot schedule arbitrary workflows; workflow keys are allowlisted.
- [ ] NACE taxonomy sync is the first allowlisted workflow.
- [ ] UI supports manual NACE sync trigger.
- [ ] UI supports reusable schedule creation/editing.
- [ ] No generic service/facade layer is introduced around the whole scheduler.
- [ ] Errors are logged once at the boundary and returned as safe JSON messages.
- [ ] Tests cover migration, schedule spec validation, handler validation, and UI type safety.
