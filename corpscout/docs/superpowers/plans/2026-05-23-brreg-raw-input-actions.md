# BRREG Raw Input Actions Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace BRREG's overloaded raw-input lifecycle enum with a lifecycle `state` plus append-only per-row action attempts/events for translation, enhancement, and suggestion submission.

**Architecture:** `brreg_company_raw_inputs.state` becomes lifecycle-only: `input`, `suggestion_submitted`, `completed`, `superseded`. New `brreg_raw_input_actions` and `brreg_raw_input_action_events` tables record attempts and status changes. Scheduler, data-pipelines, and UI read an effective action-attribute view instead of deriving user-facing state from legacy `processing_status` or `translation_status`.

**Tech Stack:** PostgreSQL migrations and views, sqlc-generated Go queries, Go scheduler workers/API, Temporal data-pipelines Go worker, React Router UI with TypeScript.

---

## File Structure

- Modify `database/migrations/000047_brreg_raw_input_state.up.sql`: adapt the existing uncommitted migration to lifecycle state, action tables, backfill, and effective views.
- Modify `database/migrations/000047_brreg_raw_input_state.down.sql`: drop action views/tables and recreate the previous source raw-input view.
- Create `database/queries/brreg_raw_input_actions.sql`: sqlc queries for appending submission action events and reading action summaries.
- Modify `database/queries/raw_inputs.sql`: BRREG submit claim/mark/retry queries use lifecycle state and action events.
- Modify `database/queries/source_flow.sql`: flow counts use lifecycle state and effective action attributes.
- Regenerate `scheduler/internal/db/gen/*` with sqlc.
- Modify scheduler tests under `scheduler/internal/workers` and `scheduler/internal/httpapi`: assert action-backed state and counts.
- Modify `scheduler/internal/workers/brreg_processor.go`: claim translated rows via action attributes, commit suggestions and action events together.
- Modify `scheduler/internal/httpapi/raw_inputs.go`, `sources.go`, and `source_flow.go`: expose lifecycle plus action attributes and filter on them.
- Modify data-pipelines files under `data-pipelines/services/go-worker/activities`, `contracts`, and `workflows`: translation writes action attempts/events in the same transactions as legacy compatibility fields.
- Modify UI files under `ui/app/components/app` and `ui/app/types/api.ts`: show lifecycle state plus action attributes, and drive action sheet buckets from action summary fields.

## Task 1: Migration And Effective Action View

**Files:**
- Modify: `database/migrations/000047_brreg_raw_input_state.up.sql`
- Modify: `database/migrations/000047_brreg_raw_input_state.down.sql`
- Test: `scheduler/internal/db/brreg_raw_input_actions_migration_test.go`

- [ ] **Step 1: Write migration contract tests**

Create `scheduler/internal/db/brreg_raw_input_actions_migration_test.go`:

```go
package db

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBrregRawInputActionsMigrationDefinesLifecycleAndEvents(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000047_brreg_raw_input_state.up.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "state IN (")
	require.Contains(t, sql, "'input'")
	require.Contains(t, sql, "'suggestion_submitted'")
	require.NotContains(t, sql, "'translating'")
	require.NotContains(t, sql, "'translated'")
	require.Contains(t, sql, "CREATE TABLE brreg_raw_input_actions")
	require.Contains(t, sql, "CREATE TABLE brreg_raw_input_action_events")
	require.Contains(t, sql, "CREATE OR REPLACE VIEW v_brreg_raw_input_action_attributes")
	require.Contains(t, sql, "has_successful_translation")
	require.Contains(t, sql, "latest_translation_action_status")
	require.Contains(t, sql, "has_successful_enhancement")
	require.Contains(t, sql, "latest_submission_action_status")
}

func TestBrregRawInputActionsMigrationBackfillsLegacyColumns(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000047_brreg_raw_input_state.up.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "translation_status = 'translated'")
	require.Contains(t, sql, "translation_status = 'failed'")
	require.Contains(t, sql, "processing_status = 'processed'")
	require.Contains(t, sql, "processing_status = 'failed'")
	require.Contains(t, sql, "status, message")
	require.Contains(t, sql, "'succeeded'")
	require.Contains(t, sql, "'failed'")
}
```

- [ ] **Step 2: Run tests and verify they fail**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./internal/db -run 'TestBrregRawInputActionsMigration'
```

Expected: FAIL because the current migration still contains action-like row states such as `translating` and `translated`, and it has no action tables.

- [ ] **Step 3: Replace the up migration**

Replace the BRREG state/action portion of `database/migrations/000047_brreg_raw_input_state.up.sql` with this structure. Keep the existing `v_source_raw_inputs` body, but change the BRREG state column to lifecycle state and append the effective action columns listed below.

```sql
ALTER TABLE brreg_company_raw_inputs
  ADD COLUMN state TEXT NOT NULL DEFAULT 'input',
  ADD CONSTRAINT chk_brreg_raw_input_state CHECK (
    state IN ('input', 'suggestion_submitted', 'completed', 'superseded')
  );

CREATE TABLE brreg_raw_input_actions (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  raw_input_id UUID NOT NULL REFERENCES brreg_company_raw_inputs(id) ON DELETE CASCADE,
  action_type TEXT NOT NULL CHECK (action_type IN ('translate', 'enhance', 'submit_suggestion')),
  attempt INTEGER NOT NULL,
  payload_hash TEXT NOT NULL,
  trigger TEXT,
  worker_id TEXT,
  workflow_id TEXT,
  workflow_run_id TEXT,
  river_job_id BIGINT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (raw_input_id, action_type, attempt)
);

CREATE TABLE brreg_raw_input_action_events (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  action_id UUID NOT NULL REFERENCES brreg_raw_input_actions(id) ON DELETE CASCADE,
  status TEXT NOT NULL CHECK (status IN ('queued', 'running', 'succeeded', 'failed', 'skipped', 'cancelled')),
  message TEXT,
  error TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_brreg_raw_input_actions_raw_type_attempt
  ON brreg_raw_input_actions (raw_input_id, action_type, attempt DESC, created_at DESC);
CREATE INDEX idx_brreg_raw_input_actions_payload
  ON brreg_raw_input_actions (payload_hash);
CREATE INDEX idx_brreg_raw_input_action_events_latest
  ON brreg_raw_input_action_events (action_id, created_at DESC);
CREATE INDEX idx_brreg_raw_input_action_events_status
  ON brreg_raw_input_action_events (status, created_at);

UPDATE brreg_company_raw_inputs bri
SET state = CASE
  WHEN EXISTS (
    SELECT 1 FROM suggestions s
    WHERE s.source_input_table = 'brreg_company_raw_inputs'
      AND s.source_input_id = bri.id::text
  ) THEN 'suggestion_submitted'
  WHEN bri.processing_status = 'superseded' THEN 'superseded'
  WHEN bri.processing_status IN ('processed', 'ignored') THEN 'completed'
  ELSE 'input'
END,
updated_at = now();

INSERT INTO brreg_raw_input_actions (raw_input_id, action_type, attempt, payload_hash, trigger, worker_id, metadata, created_at)
SELECT bri.id, 'translate', 1, bri.payload_hash, 'migration', bri.translation_lease_by,
       jsonb_build_object('legacy_translation_status', bri.translation_status),
       COALESCE(bri.translated_at, bri.updated_at, bri.created_at)
FROM brreg_company_raw_inputs bri
WHERE bri.translation_status IN ('translated', 'failed');

INSERT INTO brreg_raw_input_action_events (action_id, status, message, error, created_at)
SELECT a.id,
       CASE bri.translation_status WHEN 'translated' THEN 'succeeded' ELSE 'failed' END,
       'backfilled from legacy translation_status',
       bri.translation_error,
       COALESCE(bri.translated_at, bri.updated_at, bri.created_at)
FROM brreg_raw_input_actions a
JOIN brreg_company_raw_inputs bri ON bri.id = a.raw_input_id
WHERE a.action_type = 'translate'
  AND a.trigger = 'migration';

INSERT INTO brreg_raw_input_actions (raw_input_id, action_type, attempt, payload_hash, trigger, worker_id, metadata, created_at)
SELECT bri.id, 'submit_suggestion', 1, bri.payload_hash, 'migration', bri.processing_lease_by,
       jsonb_build_object('legacy_processing_status', bri.processing_status),
       COALESCE(bri.processed_at, bri.updated_at, bri.created_at)
FROM brreg_company_raw_inputs bri
WHERE bri.processing_status IN ('processed', 'failed')
   OR EXISTS (
     SELECT 1 FROM suggestions s
     WHERE s.source_input_table = 'brreg_company_raw_inputs'
       AND s.source_input_id = bri.id::text
   );

INSERT INTO brreg_raw_input_action_events (action_id, status, message, error, created_at)
SELECT a.id,
       CASE
         WHEN EXISTS (
           SELECT 1 FROM suggestions s
           WHERE s.source_input_table = 'brreg_company_raw_inputs'
             AND s.source_input_id = bri.id::text
         ) THEN 'succeeded'
         WHEN bri.processing_status = 'failed' THEN 'failed'
         ELSE 'skipped'
       END,
       'backfilled from legacy processing_status',
       bri.processing_error,
       COALESCE(bri.processed_at, bri.updated_at, bri.created_at)
FROM brreg_raw_input_actions a
JOIN brreg_company_raw_inputs bri ON bri.id = a.raw_input_id
WHERE a.action_type = 'submit_suggestion'
  AND a.trigger = 'migration';

CREATE OR REPLACE VIEW v_brreg_raw_input_action_attributes AS
WITH current_actions AS (
  SELECT DISTINCT ON (a.raw_input_id, a.action_type)
    a.raw_input_id,
    a.action_type,
    a.id AS action_id
  FROM brreg_raw_input_actions a
  JOIN brreg_company_raw_inputs bri
    ON bri.id = a.raw_input_id
   AND bri.payload_hash = a.payload_hash
  ORDER BY a.raw_input_id, a.action_type, a.attempt DESC, a.created_at DESC
),
latest_events AS (
  SELECT DISTINCT ON (e.action_id)
    e.action_id,
    e.status
  FROM brreg_raw_input_action_events e
  ORDER BY e.action_id, e.created_at DESC
),
successful_actions AS (
  SELECT
    a.raw_input_id,
    a.action_type,
    bool_or(e.status = 'succeeded') AS has_success
  FROM brreg_raw_input_actions a
  JOIN brreg_company_raw_inputs bri
    ON bri.id = a.raw_input_id
   AND bri.payload_hash = a.payload_hash
  JOIN brreg_raw_input_action_events e ON e.action_id = a.id
  GROUP BY a.raw_input_id, a.action_type
)
SELECT
  bri.id AS raw_input_id,
  COALESCE(MAX(le.status) FILTER (WHERE ca.action_type = 'translate'), 'notdone') AS latest_translation_action_status,
  COALESCE(MAX(sa.has_success::int) FILTER (WHERE sa.action_type = 'translate'), 0)::boolean AS has_successful_translation,
  COALESCE(MAX(le.status) FILTER (WHERE ca.action_type = 'enhance'), 'notdone') AS latest_enhancement_action_status,
  COALESCE(MAX(sa.has_success::int) FILTER (WHERE sa.action_type = 'enhance'), 0)::boolean AS has_successful_enhancement,
  COALESCE(MAX(le.status) FILTER (WHERE ca.action_type = 'submit_suggestion'), 'notdone') AS latest_submission_action_status,
  COALESCE(MAX(sa.has_success::int) FILTER (WHERE sa.action_type = 'submit_suggestion'), 0)::boolean AS has_successful_submission
FROM brreg_company_raw_inputs bri
LEFT JOIN current_actions ca ON ca.raw_input_id = bri.id
LEFT JOIN latest_events le ON le.action_id = ca.action_id
LEFT JOIN successful_actions sa ON sa.raw_input_id = bri.id
GROUP BY bri.id;

CREATE INDEX idx_brreg_raw_inputs_state
  ON brreg_company_raw_inputs (state, created_at);
CREATE INDEX idx_brreg_raw_inputs_state_input
  ON brreg_company_raw_inputs (created_at)
  WHERE state = 'input';
CREATE INDEX idx_brreg_raw_inputs_state_submitted
  ON brreg_company_raw_inputs (created_at)
  WHERE state = 'suggestion_submitted';
```

Extend `v_source_raw_inputs` with these BRREG-only columns and `NULL` values for other sources:

```sql
latest_translation_action_status,
has_successful_translation,
latest_enhancement_action_status,
has_successful_enhancement,
latest_submission_action_status,
has_successful_submission
```

For the BRREG branch, join:

```sql
LEFT JOIN v_brreg_raw_input_action_attributes baa ON baa.raw_input_id = bri.id
```

and select:

```sql
COALESCE(baa.latest_translation_action_status, 'notdone') AS latest_translation_action_status,
COALESCE(baa.has_successful_translation, false) AS has_successful_translation,
COALESCE(baa.latest_enhancement_action_status, 'notdone') AS latest_enhancement_action_status,
COALESCE(baa.has_successful_enhancement, false) AS has_successful_enhancement,
COALESCE(baa.latest_submission_action_status, 'notdone') AS latest_submission_action_status,
COALESCE(baa.has_successful_submission, false) AS has_successful_submission
```

- [ ] **Step 4: Replace the down migration**

Update `database/migrations/000047_brreg_raw_input_state.down.sql` so it drops in this order:

```sql
DROP VIEW IF EXISTS v_source_raw_inputs;
DROP VIEW IF EXISTS v_brreg_raw_input_action_attributes;
DROP TABLE IF EXISTS brreg_raw_input_action_events;
DROP TABLE IF EXISTS brreg_raw_input_actions;
DROP INDEX IF EXISTS idx_brreg_raw_inputs_state_submitted;
DROP INDEX IF EXISTS idx_brreg_raw_inputs_state_input;
DROP INDEX IF EXISTS idx_brreg_raw_inputs_state;
ALTER TABLE brreg_company_raw_inputs
  DROP CONSTRAINT IF EXISTS chk_brreg_raw_input_state,
  DROP COLUMN IF EXISTS state;
```

Then recreate `v_source_raw_inputs` in its pre-000047 shape.

- [ ] **Step 5: Run migration contract tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./internal/db -run 'TestBrregRawInputActionsMigration'
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
git add database/migrations/000047_brreg_raw_input_state.up.sql database/migrations/000047_brreg_raw_input_state.down.sql scheduler/internal/db/brreg_raw_input_actions_migration_test.go
git commit -m "feat: add brreg raw input action history schema"
```

## Task 2: sqlc Queries For Lifecycle And Actions

**Files:**
- Create: `database/queries/brreg_raw_input_actions.sql`
- Modify: `database/queries/raw_inputs.sql`
- Modify: `database/sqlc.yaml` only if sqlc does not automatically include the new query file.
- Test: `scheduler/internal/workers/brreg_processor_query_test.go`

- [ ] **Step 1: Write failing SQL contract tests**

Extend `scheduler/internal/workers/brreg_processor_query_test.go`:

```go
func TestBrregActionQueriesAppendEvents(t *testing.T) {
	body := readQueryFile(t, "../../../database/queries/brreg_raw_input_actions.sql")
	require.Contains(t, body, "-- name: AppendLatestBrregRawInputActionEvent")
	require.Contains(t, body, "brreg_raw_input_action_events")
	require.Contains(t, body, "ORDER BY a.attempt DESC, a.created_at DESC")
	require.Contains(t, body, "LIMIT 1")
}

func TestClaimPendingBrregRawInputsUsesActionAttributes(t *testing.T) {
	body := readQueryFile(t, "../../../database/queries/raw_inputs.sql")
	require.Contains(t, body, "v_brreg_raw_input_action_attributes")
	require.Contains(t, body, "bri.state = 'input'")
	require.Contains(t, body, "baa.has_successful_translation")
	require.Contains(t, body, "latest_submission_action_status")
	require.Contains(t, body, "'submit_suggestion'")
	require.NotContains(t, body, "state = 'submitting'")
	require.NotContains(t, body, "state IN ('translated'")
}
```

Use the existing local helper if `readQueryFile` already exists in this test file. If it does not exist, add:

```go
func readQueryFile(t *testing.T, path string) string {
	t.Helper()
	body, err := os.ReadFile(path)
	require.NoError(t, err)
	return string(body)
}
```

- [ ] **Step 2: Run tests and verify they fail**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./internal/workers -run 'TestBrregActionQueriesAppendEvents|TestClaimPendingBrregRawInputsUsesActionAttributes'
```

Expected: FAIL because the new query file does not exist and `ClaimPendingBrregRawInputs` still uses action-like row states.

- [ ] **Step 3: Add action queries**

Create `database/queries/brreg_raw_input_actions.sql`:

```sql
-- name: AppendLatestBrregRawInputActionEvent :exec
INSERT INTO brreg_raw_input_action_events (action_id, status, message, error, metadata)
SELECT latest.id, $3, $4, $5, COALESCE($6::jsonb, '{}'::jsonb)
FROM (
  SELECT a.id
  FROM brreg_raw_input_actions a
  WHERE a.raw_input_id = $1
    AND a.action_type = $2
  ORDER BY a.attempt DESC, a.created_at DESC
  LIMIT 1
) latest;

-- name: CreateBrregRawInputAction :one
INSERT INTO brreg_raw_input_actions (
  raw_input_id, action_type, attempt, payload_hash, trigger, worker_id, workflow_id, workflow_run_id, river_job_id, metadata
)
SELECT
  bri.id,
  $2,
  COALESCE((
    SELECT MAX(a.attempt) + 1
    FROM brreg_raw_input_actions a
    WHERE a.raw_input_id = bri.id
      AND a.action_type = $2
  ), 1),
  bri.payload_hash,
  $3,
  $4,
  $5,
  $6,
  $7,
  COALESCE($8::jsonb, '{}'::jsonb)
FROM brreg_company_raw_inputs bri
WHERE bri.id = $1
RETURNING *;

-- name: GetBrregRawInputActionAttributes :one
SELECT *
FROM v_brreg_raw_input_action_attributes
WHERE raw_input_id = $1;
```

- [ ] **Step 4: Update BRREG raw input queries**

In `database/queries/raw_inputs.sql`, replace the current BRREG claim and mark queries with lifecycle/action-backed versions:

```sql
-- name: ClaimPendingBrregRawInputs :many
WITH claimed AS (
  UPDATE brreg_company_raw_inputs bri
  SET processing_status = 'processing',
      processing_attempts = processing_attempts + 1,
      processing_error = NULL,
      processing_lease_by = $1,
      processing_lease_until = now() + ($2 * interval '1 second'),
      updated_at = now()
  WHERE bri.id IN (
    SELECT bri2.id
    FROM brreg_company_raw_inputs bri2
    JOIN v_brreg_raw_input_action_attributes baa ON baa.raw_input_id = bri2.id
    WHERE bri2.state = 'input'
      AND bri2.raw_payload_en IS NOT NULL
      AND baa.has_successful_translation
      AND baa.latest_submission_action_status IN ('notdone', 'failed', 'cancelled')
    ORDER BY bri2.created_at
    LIMIT $3
    FOR UPDATE SKIP LOCKED
  )
  RETURNING bri.*
),
actions AS (
  INSERT INTO brreg_raw_input_actions (
    raw_input_id, action_type, attempt, payload_hash, trigger, worker_id, metadata
  )
  SELECT
    c.id,
    'submit_suggestion',
    COALESCE((
      SELECT MAX(a.attempt) + 1
      FROM brreg_raw_input_actions a
      WHERE a.raw_input_id = c.id
        AND a.action_type = 'submit_suggestion'
    ), 1),
    c.payload_hash,
    'processor',
    $1,
    jsonb_build_object('processing_attempts', c.processing_attempts)
  FROM claimed c
  RETURNING id, raw_input_id
),
events AS (
  INSERT INTO brreg_raw_input_action_events (action_id, status, message)
  SELECT a.id, 'running', 'processor claimed row for suggestion submission'
  FROM actions a
)
SELECT * FROM claimed;

-- name: MarkBrregRawInputSubmitted :exec
WITH row_update AS (
  UPDATE brreg_company_raw_inputs
  SET state = 'suggestion_submitted',
      processing_status = 'processed',
      processing_error = NULL,
      processing_lease_by = NULL,
      processing_lease_until = NULL,
      processed_at = now(),
      updated_at = now()
  WHERE id = $1
)
INSERT INTO brreg_raw_input_action_events (action_id, status, message)
SELECT latest.id, 'succeeded', 'suggestion rows created'
FROM (
  SELECT a.id
  FROM brreg_raw_input_actions a
  WHERE a.raw_input_id = $1
    AND a.action_type = 'submit_suggestion'
  ORDER BY a.attempt DESC, a.created_at DESC
  LIMIT 1
) latest;

-- name: MarkBrregRawInputProcessed :exec
WITH row_update AS (
  UPDATE brreg_company_raw_inputs
  SET state = 'completed',
      processing_status = 'processed',
      processing_error = NULL,
      processing_lease_by = NULL,
      processing_lease_until = NULL,
      processed_at = now(),
      updated_at = now()
  WHERE id = $1
)
INSERT INTO brreg_raw_input_action_events (action_id, status, message)
SELECT latest.id, 'skipped', 'no suggestion rows needed'
FROM (
  SELECT a.id
  FROM brreg_raw_input_actions a
  WHERE a.raw_input_id = $1
    AND a.action_type = 'submit_suggestion'
  ORDER BY a.attempt DESC, a.created_at DESC
  LIMIT 1
) latest;

-- name: MarkBrregRawInputFailed :exec
WITH row_update AS (
  UPDATE brreg_company_raw_inputs
  SET processing_status = 'failed',
      processing_error = $2,
      processing_lease_by = NULL,
      processing_lease_until = NULL,
      updated_at = now()
  WHERE id = $1
)
INSERT INTO brreg_raw_input_action_events (action_id, status, message, error)
SELECT latest.id, 'failed', 'suggestion submission failed', $2
FROM (
  SELECT a.id
  FROM brreg_raw_input_actions a
  WHERE a.raw_input_id = $1
    AND a.action_type = 'submit_suggestion'
  ORDER BY a.attempt DESC, a.created_at DESC
  LIMIT 1
) latest;

-- name: RetryBrregRawInput :one
UPDATE brreg_company_raw_inputs
SET processing_status = 'pending',
    processing_error = NULL,
    processing_lease_by = NULL,
    processing_lease_until = NULL,
    processed_at = NULL,
    updated_at = now()
WHERE id = $1
  AND state = 'input'
  AND EXISTS (
    SELECT 1
    FROM v_brreg_raw_input_action_attributes baa
    WHERE baa.raw_input_id = brreg_company_raw_inputs.id
      AND baa.latest_submission_action_status = 'failed'
  )
RETURNING id;
```

- [ ] **Step 5: Regenerate sqlc**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off sqlc generate -f ../database/sqlc.yaml
```

Expected: generated files under `scheduler/internal/db/gen` include the new `BrregRawInputAction` and `BrregRawInputActionEvent` models and new query methods.

- [ ] **Step 6: Run query tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./internal/workers -run 'TestBrregActionQueriesAppendEvents|TestClaimPendingBrregRawInputsUsesActionAttributes'
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
git add database/queries/brreg_raw_input_actions.sql database/queries/raw_inputs.sql scheduler/internal/db/gen scheduler/internal/workers/brreg_processor_query_test.go
git commit -m "feat: query brreg raw input action lifecycle"
```

## Task 3: Scheduler Processor Uses Submission Action Events

**Files:**
- Modify: `scheduler/internal/workers/brreg_processor.go`
- Modify: `scheduler/internal/workers/brreg_processor_test.go`
- Modify: `scheduler/internal/workers/processor_testmock_test.go`

- [ ] **Step 1: Write failing processor tests**

Update `scheduler/internal/workers/brreg_processor_test.go` so the existing suggestion-created test expects `MarkBrregRawInputSubmitted`, and the existing no-suggestion test expects `MarkBrregRawInputProcessed`. Add a failure test:

```go
func TestBrregProcessor_InsertSuggestionFailureMarksSubmissionFailed(t *testing.T) {
	q := newMockProcessorQueries(t)
	row := brregRawInputFixture()
	src := db.DataSource{ID: uuid.New(), Name: "brreg", InputTableName: "brreg_company_raw_inputs"}

	q.On("GetSourceByName", mock.Anything, "brreg").Return(src, nil)
	q.On("ClaimPendingBrregRawInputs", mock.Anything, mock.Anything).Return([]db.BrregCompanyRawInput{row}, nil).Once()
	q.On("ClaimPendingBrregRawInputs", mock.Anything, mock.Anything).Return([]db.BrregCompanyRawInput{}, nil).Once()
	q.On("GetCompanyByRegistrationAndCountry", mock.Anything, mock.Anything).Return(db.Company{}, pgx.ErrNoRows)
	q.On("GetCountryIDByISO2", mock.Anything, "NO").Return(uuid.Nil, pgx.ErrNoRows)
	q.On("InsertSuggestion", mock.Anything, mock.Anything).Return(db.Suggestion{}, errors.New("insert failed"))
	q.On("MarkBrregRawInputFailed", mock.Anything, mock.MatchedBy(func(arg db.MarkBrregRawInputFailedParams) bool {
		return arg.ID == row.ID && arg.ProcessingError != nil
	})).Return(nil)

	proc := NewBrregProcessor(q)
	require.NoError(t, proc.ProcessBatch(context.Background(), "brreg"))
	q.AssertExpectations(t)
}
```

- [ ] **Step 2: Run processor tests and verify failures**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./internal/workers -run 'TestBrregProcessor'
```

Expected: FAIL until mocks and processor behavior match the new generated sqlc signatures.

- [ ] **Step 3: Update processor mocks**

In `scheduler/internal/workers/processor_testmock_test.go`, add mock methods generated by sqlc:

```go
func (m *mockProcessorQueries) CreateBrregRawInputAction(ctx context.Context, arg db.CreateBrregRawInputActionParams) (db.BrregRawInputAction, error) {
	args := m.Called(ctx, arg)
	return args.Get(0).(db.BrregRawInputAction), args.Error(1)
}

func (m *mockProcessorQueries) AppendLatestBrregRawInputActionEvent(ctx context.Context, arg db.AppendLatestBrregRawInputActionEventParams) error {
	args := m.Called(ctx, arg)
	return args.Error(0)
}

func (m *mockProcessorQueries) GetBrregRawInputActionAttributes(ctx context.Context, rawInputID uuid.UUID) (db.GetBrregRawInputActionAttributesRow, error) {
	args := m.Called(ctx, rawInputID)
	return args.Get(0).(db.GetBrregRawInputActionAttributesRow), args.Error(1)
}
```

Use exact generated row type names after `sqlc generate`; if sqlc names the view row `VBrregRawInputActionAttribute`, update the mock method return type to that generated type.

- [ ] **Step 4: Keep processor row transaction intact**

In `scheduler/internal/workers/brreg_processor.go`, keep the current per-row transaction. Do not manually append submission success/failure events in Go when the mark queries already append them. Ensure the only row terminal calls are:

```go
if createdSuggestion {
	return q.MarkBrregRawInputSubmitted(ctx, row.ID)
}
return q.MarkBrregRawInputProcessed(ctx, row.ID)
```

The claim query creates the `submit_suggestion` action and `running` event; the mark queries append `succeeded`, `skipped`, or `failed`.

- [ ] **Step 5: Run processor tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./internal/workers -run 'TestBrregProcessor'
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
git add scheduler/internal/workers/brreg_processor.go scheduler/internal/workers/brreg_processor_test.go scheduler/internal/workers/processor_testmock_test.go
git commit -m "feat: record brreg submission action events"
```

## Task 4: Scheduler API Uses Lifecycle Plus Action Attributes

**Files:**
- Modify: `scheduler/internal/httpapi/raw_inputs.go`
- Modify: `scheduler/internal/httpapi/sources.go`
- Modify: `scheduler/internal/httpapi/source_flow.go`
- Modify: `scheduler/internal/httpapi/raw_inputs_test.go`
- Modify: `scheduler/internal/httpapi/sources_test.go`
- Modify: `scheduler/internal/httpapi/source_flow_test.go`
- Modify: `scheduler/internal/httpapi/testhelpers_test.go`

- [ ] **Step 1: Write failing API tests**

Add tests that assert BRREG state values are lifecycle-only and action filters use action attributes. Use the existing `newSQLContainsMock`, `routerFor`, and `httpapi.NewHandlers` helpers in `raw_inputs_test.go`:

```go
func TestListRawInputs_filtersByBrregLifecycleAndActionAttributes(t *testing.T) {
	pool := newSQLContainsMock(t)
	defer pool.Close()

	createdAt := time.Date(2026, 5, 23, 10, 0, 0, 0, time.UTC)
	pool.ExpectQuery("COUNT(*) FROM;;brreg_company_raw_inputs;;v_brreg_raw_input_action_attributes baa;;ri.state = $1;;baa.latest_translation_action_status = $2;;!companies_house_company_raw_inputs").
		WithArgs("input", "succeeded").
		WillReturnRows(pgxmock.NewRows([]string{"count"}).AddRow(int64(1)))
	pool.ExpectQuery("SELECT id, source, name, native_id, status, translation_status, has_suggestion, state, latest_translation_action_status, has_successful_translation, latest_enhancement_action_status, has_successful_enhancement, latest_submission_action_status, has_successful_submission, created_at;;brreg_company_raw_inputs;;v_brreg_raw_input_action_attributes baa;;ri.state = $1;;baa.latest_translation_action_status = $2").
		WithArgs("input", "succeeded", 50, 0).
		WillReturnRows(pgxmock.NewRows([]string{
			"id", "source", "name", "native_id", "status", "translation_status", "has_suggestion", "state",
			"latest_translation_action_status", "has_successful_translation",
			"latest_enhancement_action_status", "has_successful_enhancement",
			"latest_submission_action_status", "has_successful_submission", "created_at",
		}).AddRow(
			"brreg-id", "brreg", "Norway AS", "991234567", "pending", "translated", false, "input",
			"succeeded", true, "notdone", false, "notdone", false, createdAt,
		))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/raw-inputs?source=brreg&state=input&translation_action_status=succeeded", nil)
	w := httptest.NewRecorder()
	routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, pool, nil, nil, "", nil, "")).ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var body struct {
		Items []struct {
			Source                        string `json:"source"`
			State                         string `json:"state"`
			LatestTranslationActionStatus string `json:"latest_translation_action_status"`
			HasSuccessfulTranslation      bool   `json:"has_successful_translation"`
		} `json:"items"`
		Total int64 `json:"total"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	require.Equal(t, int64(1), body.Total)
	require.Len(t, body.Items, 1)
	require.Equal(t, "brreg", body.Items[0].Source)
	require.Equal(t, "input", body.Items[0].State)
	require.Equal(t, "succeeded", body.Items[0].LatestTranslationActionStatus)
	require.True(t, body.Items[0].HasSuccessfulTranslation)
	require.NoError(t, pool.ExpectationsWereMet())
}
```

In `sources_test.go`, replace the existing BRREG translation stats SQL expectation with an action-backed query expectation:

```go
func TestBrregTranslationStatsUsesActionAttributes(t *testing.T) {
	pool := newSQLContainsMock(t)
	defer pool.Close()

	pool.ExpectQuery("brreg_company_raw_inputs bri;;v_brreg_raw_input_action_attributes baa;;latest_translation_action_status;;has_successful_translation;;latest_submission_action_status;;latest_enhancement_action_status;;state = 'input';;state = 'suggestion_submitted';;ready_to_submit").
		WillReturnRows(pgxmock.NewRows([]string{
			"input", "suggestion_submitted", "completed", "superseded",
			"needs_translation", "translation_running", "translated", "translation_failed",
			"needs_enhancement", "enhancement_running", "enhancement_failed",
			"ready_to_submit", "submission_failed", "total",
		}).AddRow(
			int64(10), int64(4), int64(2), int64(1),
			int64(3), int64(1), int64(5), int64(2),
			int64(8), int64(1), int64(1),
			int64(5), int64(2), int64(17),
		))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/sources/brreg/translation-stats", nil)
	w := httptest.NewRecorder()
	routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, pool, nil, nil, "", nil, "")).ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), `"needs_translation":3`)
	require.Contains(t, w.Body.String(), `"translation_failed":2`)
	require.Contains(t, w.Body.String(), `"translated":5`)
	require.Contains(t, w.Body.String(), `"ready_to_submit":5`)
	require.NoError(t, pool.ExpectationsWereMet())
}
```

- [ ] **Step 2: Run API tests and verify failures**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./internal/httpapi -run 'TestListRawInputs_filtersByBrregLifecycleAndActionAttributes|TestBrregTranslationStatsUsesActionAttributes|TestGetBrregFlow'
```

Expected: FAIL because current API accepts action-like BRREG state values and does not expose action attribute filters.

- [ ] **Step 3: Update raw input row JSON**

In `scheduler/internal/httpapi/raw_inputs.go`, change `rawInputRow` to include effective action attributes:

```go
type rawInputRow struct {
	ID                              string    `json:"id"`
	Source                          string    `json:"source"`
	Name                            string    `json:"name"`
	NativeID                        string    `json:"native_id"`
	Status                          string    `json:"status"`
	TranslationStatus               *string   `json:"translation_status,omitempty"`
	HasSuggestion                   bool      `json:"has_suggestion"`
	State                           string    `json:"state"`
	LatestTranslationActionStatus   *string   `json:"latest_translation_action_status,omitempty"`
	HasSuccessfulTranslation        *bool     `json:"has_successful_translation,omitempty"`
	LatestEnhancementActionStatus   *string   `json:"latest_enhancement_action_status,omitempty"`
	HasSuccessfulEnhancement        *bool     `json:"has_successful_enhancement,omitempty"`
	LatestSubmissionActionStatus    *string   `json:"latest_submission_action_status,omitempty"`
	HasSuccessfulSubmission         *bool     `json:"has_successful_submission,omitempty"`
	CreatedAt                       time.Time `json:"created_at"`
}
```

Change valid BRREG row states:

```go
var brregRawInputStates = map[string]bool{
	"input":                true,
	"suggestion_submitted": true,
	"completed":            true,
	"superseded":           true,
}
```

Add valid action status filter values:

```go
var brregActionStatuses = map[string]bool{
	"notdone":   true,
	"queued":    true,
	"running":   true,
	"succeeded": true,
	"failed":    true,
	"skipped":   true,
	"cancelled": true,
}
```

- [ ] **Step 4: Join effective attributes for BRREG list queries**

In the BRREG branch of `handleListRawInputs`, join `v_brreg_raw_input_action_attributes baa`. For non-BRREG sources, select `NULL` values for the action columns. For BRREG, select:

```sql
baa.latest_translation_action_status,
baa.has_successful_translation,
baa.latest_enhancement_action_status,
baa.has_successful_enhancement,
baa.latest_submission_action_status,
baa.has_successful_submission
```

Add query params:

```text
translation_action_status
enhancement_action_status
submission_action_status
```

Reject values not in `brregActionStatuses`, and apply them only to BRREG.

- [ ] **Step 5: Update stats and flow counts**

In `scheduler/internal/httpapi/sources.go`, replace the BRREG stats response fields with action-backed fields:

```go
type brregLifecycleStats struct {
	Input              int64 `json:"input"`
	SuggestionSubmitted int64 `json:"suggestion_submitted"`
	Completed          int64 `json:"completed"`
	Superseded         int64 `json:"superseded"`
	NeedsTranslation  int64 `json:"needs_translation"`
	TranslationRunning int64 `json:"translation_running"`
	Translated         int64 `json:"translated"`
	TranslationFailed  int64 `json:"translation_failed"`
	NeedsEnhancement   int64 `json:"needs_enhancement"`
	EnhancementRunning int64 `json:"enhancement_running"`
	EnhancementFailed  int64 `json:"enhancement_failed"`
	ReadyToSubmit      int64 `json:"ready_to_submit"`
	SubmissionFailed   int64 `json:"submission_failed"`
	Total              int64 `json:"total"`
}
```

Use `brreg_company_raw_inputs` joined to `v_brreg_raw_input_action_attributes` for all counts. `ReadyToSubmit` is:

```sql
bri.state = 'input'
AND baa.has_successful_translation
AND baa.latest_submission_action_status IN ('notdone', 'failed', 'cancelled')
```

In `scheduler/internal/httpapi/source_flow.go`, links should use:

```text
?state=input
?state=suggestion_submitted
?translation_action_status=failed
?submission_action_status=failed
```

- [ ] **Step 6: Update detail response**

Add the same action attribute fields to `rawInputDetail` and the BRREG detail SELECT. Legacy `processing_status` and `translation_status` remain diagnostic fields.

- [ ] **Step 7: Run API tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./internal/httpapi
```

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
git add scheduler/internal/httpapi/raw_inputs.go scheduler/internal/httpapi/sources.go scheduler/internal/httpapi/source_flow.go scheduler/internal/httpapi/*_test.go
git commit -m "feat: expose brreg action attributes in api"
```

## Task 5: data-pipelines Translation Writes Action Attempts And Events

**Files:**
- Modify: `/Users/graovic/pulsarpoint/ppoint/data-pipelines/services/go-worker/activities/source_translation_activity.go`
- Modify: `/Users/graovic/pulsarpoint/ppoint/data-pipelines/services/go-worker/activities/brreg_translation_activity.go`
- Modify: `/Users/graovic/pulsarpoint/ppoint/data-pipelines/services/go-worker/activities/source_translation_activity_test.go`
- Modify: `/Users/graovic/pulsarpoint/ppoint/data-pipelines/services/go-worker/activities/brreg_translation_activity_test.go`

- [ ] **Step 1: Write failing translation action tests**

In `source_translation_activity_test.go`, update the BRREG state test so it expects action inserts:

```go
func TestPrepareSourceTranslationBatch_BrregCreatesTranslateAction(t *testing.T) {
	sqlText := sourceClaimSQL("brreg_company_raw_inputs", false, true, "input")
	require.Contains(t, sqlText, "brreg_raw_input_actions")
	require.Contains(t, sqlText, "'translate'")
	require.Contains(t, sqlText, "brreg_raw_input_action_events")
	require.Contains(t, sqlText, "'running'")
	require.Contains(t, sqlText, "state = 'input'")
	require.NotContains(t, sqlText, "state = 'translating'")
}

func TestSourceWriteSuccessSQL_BrregAppendsSucceededEvent(t *testing.T) {
	sqlText := sourceWriteSuccessSQL("brreg_company_raw_inputs", true)
	require.Contains(t, sqlText, "brreg_raw_input_action_events")
	require.Contains(t, sqlText, "'succeeded'")
	require.NotContains(t, sqlText, "state = 'translated'")
}

func TestSourceWriteFailureSQL_BrregAppendsFailedEvent(t *testing.T) {
	sqlText := sourceWriteFailureSQL("brreg_company_raw_inputs", true)
	require.Contains(t, sqlText, "brreg_raw_input_action_events")
	require.Contains(t, sqlText, "'failed'")
	require.NotContains(t, sqlText, "state = 'translation_failed'")
}
```

- [ ] **Step 2: Run tests and verify failures**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/services/go-worker
GOWORK=off go test ./activities -run 'TestPrepareSourceTranslationBatch_BrregCreatesTranslateAction|TestSourceWriteSuccessSQL_BrregAppendsSucceededEvent|TestSourceWriteFailureSQL_BrregAppendsFailedEvent'
```

Expected: FAIL because current SQL updates row `state` directly.

- [ ] **Step 3: Update BRREG claim SQL builder**

In `source_translation_activity.go`, replace the BRREG-specific state mutation in `sourceClaimSQL` with a CTE that updates legacy lease columns and creates action rows:

```go
if hasState {
	return fmt.Sprintf(`
		WITH claimed AS (
			UPDATE %s bri
			SET translation_status = 'translating',
			    translation_attempts = translation_attempts + 1,
			    translation_error = NULL,
			    translation_lease_by = $1,
			    translation_lease_until = now() + ($2 * interval '1 minute'),
			    updated_at = now()
			WHERE bri.id IN (
			    SELECT bri2.id
			    FROM %s bri2
			    LEFT JOIN v_brreg_raw_input_action_attributes baa ON baa.raw_input_id = bri2.id
			    WHERE (
			        %s
			        OR (baa.latest_translation_action_status = 'running' AND (bri2.translation_lease_until < now() OR bri2.translation_lease_by = $1))
			    )
			    AND ($4::text[] IS NULL OR bri2.id::text = ANY($4))
			    ORDER BY bri2.created_at
			    LIMIT $3
			    FOR UPDATE SKIP LOCKED
			)
			RETURNING bri.id, bri.raw_payload, bri.payload_hash
		),
		actions AS (
			INSERT INTO brreg_raw_input_actions (
				raw_input_id, action_type, attempt, payload_hash, trigger, worker_id, workflow_run_id, metadata
			)
			SELECT
				c.id,
				'translate',
				COALESCE((
					SELECT MAX(a.attempt) + 1
					FROM brreg_raw_input_actions a
					WHERE a.raw_input_id = c.id
					  AND a.action_type = 'translate'
				), 1),
				c.payload_hash,
				'translation_workflow',
				$1,
				$1,
				jsonb_build_object('batch_size', $3)
			FROM claimed c
			RETURNING id
		),
		events AS (
			INSERT INTO brreg_raw_input_action_events (action_id, status, message)
			SELECT a.id, 'running', 'translation workflow claimed row'
			FROM actions a
		)
		SELECT id::text, raw_payload FROM claimed
	`, table, table, predicate)
}
```

For BRREG, `predicate` should be:

```go
predicate := "bri2.state = 'input' AND COALESCE(baa.has_successful_translation, false) = false"
if includeFailed {
	predicate = "bri2.state = 'input' AND COALESCE(baa.has_successful_translation, false) = false"
}
if stateFilter != "" {
	predicate = "bri2.state = $5"
}
```

- [ ] **Step 4: Update BRREG write success/failure SQL**

For BRREG, success SQL should update materialized payload and legacy translation columns, then append the latest translate action event:

```sql
WITH row_update AS (
  UPDATE brreg_company_raw_inputs
  SET raw_payload_en = $2,
      translation_status = 'translated',
      translation_error = NULL,
      translation_model = $3,
      translation_prompt_version = $4,
      translation_fx_source = $5,
      translation_fx_rate_date = $6,
      translated_at = now(),
      translation_lease_by = NULL,
      translation_lease_until = NULL,
      updated_at = now()
  WHERE id = $1
)
INSERT INTO brreg_raw_input_action_events (action_id, status, message)
SELECT latest.id, 'succeeded', 'translation completed'
FROM (
  SELECT a.id
  FROM brreg_raw_input_actions a
  WHERE a.raw_input_id = $1
    AND a.action_type = 'translate'
  ORDER BY a.attempt DESC, a.created_at DESC
  LIMIT 1
) latest
```

Failure SQL should update legacy columns and append `failed`:

```sql
WITH row_update AS (
  UPDATE brreg_company_raw_inputs
  SET raw_payload_en = NULL,
      translation_status = 'failed',
      translation_error = $2,
      translation_lease_by = NULL,
      translation_lease_until = NULL,
      updated_at = now()
  WHERE id = $1
)
INSERT INTO brreg_raw_input_action_events (action_id, status, message, error)
SELECT latest.id, 'failed', 'translation failed', $2
FROM (
  SELECT a.id
  FROM brreg_raw_input_actions a
  WHERE a.raw_input_id = $1
    AND a.action_type = 'translate'
  ORDER BY a.attempt DESC, a.created_at DESC
  LIMIT 1
) latest
```

Keep CVR and Ariregister SQL unchanged.

- [ ] **Step 5: Update legacy BRREG-specific activity**

In `brreg_translation_activity.go`, mirror the same CTE shape for the legacy direct BRREG claim/write paths. These paths remain for older workers/tests and must not write `state = 'translated'` or `state = 'translation_failed'`.

- [ ] **Step 6: Run data-pipelines tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/services/go-worker
GOWORK=off go test ./activities
```

Expected: PASS.

- [ ] **Step 7: Commit data-pipelines changes**

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines
git add services/go-worker/activities/source_translation_activity.go services/go-worker/activities/brreg_translation_activity.go services/go-worker/activities/source_translation_activity_test.go services/go-worker/activities/brreg_translation_activity_test.go
git commit -m "feat: record brreg translation action events"
```

Do not stage `.github/workflows/go-worker-image.yml`; it was already dirty before this plan.

## Task 6: BRREG Enhancement Action Hook

**Files:**
- Modify: `scheduler/internal/tasksvc/types.go`
- Modify: `scheduler/internal/tasksvc/starter.go`
- Modify: `scheduler/internal/httpapi/sources.go`
- Modify: `scheduler/internal/httpapi/handlers.go`
- Modify: `/Users/graovic/pulsarpoint/ppoint/data-pipelines/services/go-worker/contracts/contracts.go`
- Modify: `/Users/graovic/pulsarpoint/ppoint/data-pipelines/services/go-worker/workflows/enrich_company_domains.go`
- Modify: `/Users/graovic/pulsarpoint/ppoint/data-pipelines/services/go-worker/activities/activities.go`

- [ ] **Step 1: Add tests for enhancement start contract**

In `scheduler/internal/tasksvc/starter_test.go`, add:

```go
func TestStartBrregDomainEnhancementStartsTemporalWorkflow(t *testing.T) {
	tc := &fakeTemporalClient{}
	svc := NewService(nil, tc, "")
	svc.now = func() time.Time { return time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC) }

	result, err := svc.StartBrregDomainEnhancement(context.Background(), StartBrregDomainEnhancementRequest{
		Companies: []CompanyLookup{{NativeID: "810202572", Name: "BORTIGARD AS"}},
		ActionIDs: map[string]string{"810202572": uuid.NewString()},
		Force: false,
	})

	require.NoError(t, err)
	require.Equal(t, ExecutorTemporal, result.Executor)
	require.Equal(t, "EnrichCompanyDomains", tc.workflow)
	require.Equal(t, "enhance-brreg-20260523120000-000000000", tc.options.ID)
}
```

- [ ] **Step 2: Add scheduler start request types**

In `scheduler/internal/tasksvc/types.go`, add:

```go
type CompanyLookup struct {
	NativeID string `json:"native_id"`
	Name     string `json:"name"`
}

type StartBrregDomainEnhancementRequest struct {
	Companies []CompanyLookup    `json:"companies"`
	ActionIDs map[string]string   `json:"action_ids"`
	Force     bool                `json:"force"`
}
```

- [ ] **Step 3: Add task service starter**

In `scheduler/internal/tasksvc/starter.go`, add:

```go
func (s *Service) StartBrregDomainEnhancement(ctx context.Context, req StartBrregDomainEnhancementRequest) (StartResult, error) {
	if s.temporal == nil {
		return StartResult{}, errors.New("temporal client not available")
	}
	workflowID := fmt.Sprintf("enhance-brreg-%s", timestampID(s.now()))
	input := map[string]any{
		"source":     "brreg",
		"country":    "NO",
		"companies":  req.Companies,
		"action_ids": req.ActionIDs,
		"force":      req.Force,
	}
	run, err := s.temporal.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                                       workflowID,
		TaskQueue:                                TaskQueue,
		WorkflowIDReusePolicy:                    enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		WorkflowExecutionErrorWhenAlreadyStarted: true,
		SearchAttributes:                         searchAttributes("brreg", "enhance"),
		Memo: memo("brreg", "enhance", "manual", map[string]any{"count": len(req.Companies), "force": req.Force}),
	}, "EnrichCompanyDomains", input)
	if err != nil {
		return StartResult{}, errors.Wrap(err, "start temporal brreg domain enhancement workflow")
	}
	return StartResult{Executor: ExecutorTemporal, Status: "started", WorkflowID: workflowID, WorkflowRunID: run.GetRunID()}, nil
}
```

- [ ] **Step 4: Add BRREG enhancement API endpoint**

In `scheduler/internal/httpapi/handlers.go`, register:

```go
r.Post("/sources/brreg/enhance-domains", h.handleEnhanceBrregDomains)
```

In `scheduler/internal/httpapi/sources.go`, implement an endpoint that selects up to 500 BRREG `input` rows where enhancement has not succeeded, creates `enhance` action rows with `queued` events in one transaction, and starts Temporal after commit:

```go
type enhanceBrregRequest struct {
	IDs     []string          `json:"ids,omitempty"`
	Filters map[string]string `json:"filters,omitempty"`
	Force   bool              `json:"force,omitempty"`
	Limit   int               `json:"limit,omitempty"`
}
```

The SQL selection predicate should be:

```sql
bri.state IN ('input', 'suggestion_submitted')
AND COALESCE(baa.has_successful_enhancement, false) = false
```

The endpoint response:

```json
{"status":"started","count":25,"workflow_id":"enhance-brreg-...","workflow_run_id":"..."}
```

- [ ] **Step 5: Extend data-pipelines contract**

In `data-pipelines/services/go-worker/contracts/contracts.go`, extend `EnrichCompanyDomainsInput`:

```go
type EnrichCompanyDomainsInput struct {
	Source    string            `json:"source"`
	Country   string            `json:"country"`
	Companies []CompanyLookup   `json:"companies"`
	ActionIDs map[string]string  `json:"action_ids,omitempty"`
	Force     bool              `json:"force,omitempty"`
}
```

- [ ] **Step 6: Add enhancement action event activity**

In `data-pipelines/services/go-worker/activities/activities.go`, add:

```go
type MarkRawInputActionEventsParams struct {
	ActionIDs map[string]string `json:"action_ids"`
	Status    string            `json:"status"`
	Message   string            `json:"message,omitempty"`
	Error     string            `json:"error,omitempty"`
}

func (a *GoActivities) MarkRawInputActionEvents(ctx context.Context, params contracts.MarkRawInputActionEventsParams) error {
	for _, actionID := range params.ActionIDs {
		if actionID == "" {
			continue
		}
		if _, err := a.pool.Exec(ctx, `
			INSERT INTO brreg_raw_input_action_events (action_id, status, message, error)
			VALUES ($1, $2, $3, $4)
		`, actionID, params.Status, params.Message, params.Error); err != nil {
			return fmt.Errorf("append brreg raw input action event: %w", err)
		}
	}
	return nil
}
```

Put the params type in `contracts.go`, not `activities.go`.

- [ ] **Step 7: Mark enhancement running/succeeded/failed in workflow**

In `workflows/enrich_company_domains.go`, before each batch Python activity, mark batch action IDs `running`; after `WriteDiscoveredDomains` and `MarkDomainsSearched`, mark them `succeeded`; if `discoverErr != nil`, mark them `failed`.

Use this helper inside the workflow:

```go
func actionIDsForBatch(actionIDs map[string]string, nativeIDs []string) map[string]string {
	out := map[string]string{}
	for _, nativeID := range nativeIDs {
		if actionID := actionIDs[nativeID]; actionID != "" {
			out[nativeID] = actionID
		}
	}
	return out
}
```

- [ ] **Step 8: Run scheduler and data-pipelines tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./internal/tasksvc ./internal/httpapi

cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/services/go-worker
GOWORK=off go test ./...
```

Expected: PASS.

- [ ] **Step 9: Commit both repositories**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
git add scheduler/internal/tasksvc scheduler/internal/httpapi
git commit -m "feat: start brreg domain enhancement actions"

cd /Users/graovic/pulsarpoint/ppoint/data-pipelines
git add services/go-worker/contracts/contracts.go services/go-worker/workflows/enrich_company_domains.go services/go-worker/activities/activities.go
git commit -m "feat: mark brreg enhancement action events"
```

## Task 7: UI Lifecycle And Action Attribute Display

**Files:**
- Modify: `ui/app/types/api.ts`
- Modify: `ui/app/lib/api.ts`
- Modify: `ui/app/components/app/RawInputsTable.tsx`
- Modify: `ui/app/components/app/BrregRawInputActionSheet.tsx`
- Modify: `ui/app/components/app/source-detail/RawInputsTab.tsx`
- Modify: `ui/app/components/app/source-detail/RawInputSheet.tsx`
- Modify: `ui/app/components/app/source-detail/sourceDetailUtils.ts`
- Modify: `ui/app/components/app/source-detail/flow/layout.ts`

- [ ] **Step 1: Update TypeScript API types**

In `ui/app/types/api.ts`, add action attribute fields:

```ts
export type RawInputActionStatus =
  | "notdone"
  | "queued"
  | "running"
  | "succeeded"
  | "failed"
  | "skipped"
  | "cancelled";

export interface SourceRawInput {
  id: string;
  source: string;
  name: string;
  native_id: string;
  status: string;
  translation_status?: string | null;
  has_suggestion: boolean;
  state: "input" | "suggestion_submitted" | "completed" | "superseded" | string;
  latest_translation_action_status?: RawInputActionStatus | null;
  has_successful_translation?: boolean | null;
  latest_enhancement_action_status?: RawInputActionStatus | null;
  has_successful_enhancement?: boolean | null;
  latest_submission_action_status?: RawInputActionStatus | null;
  has_successful_submission?: boolean | null;
  created_at: string;
}
```

Update `BrregTranslationStats` to match the API fields from Task 4.

- [ ] **Step 2: Add API call for enhancement**

In `ui/app/lib/api.ts`, add:

```ts
enhanceBrregDomains: (body: { ids?: string[]; filters?: Record<string, string>; force?: boolean; limit?: number }) =>
  post<unknown>("/sources/brreg/enhance-domains", body),
```

- [ ] **Step 3: Update BRREG state options**

In `RawInputsTable.tsx` and `RawInputsTab.tsx`, use lifecycle-only state options:

```ts
const brregStateOptions = [
  { value: "input", label: "Input" },
  { value: "suggestion_submitted", label: "Suggestion submitted" },
  { value: "completed", label: "Completed" },
  { value: "superseded", label: "Superseded" },
];
```

Add action status filters:

```ts
const actionStatusOptions = [
  { value: "notdone", label: "Not done" },
  { value: "queued", label: "Queued" },
  { value: "running", label: "Running" },
  { value: "succeeded", label: "Succeeded" },
  { value: "failed", label: "Failed" },
  { value: "skipped", label: "Skipped" },
  { value: "cancelled", label: "Cancelled" },
];
```

- [ ] **Step 4: Update table columns**

For BRREG raw inputs, render columns:

```text
Name | ID | State | Translation | Enhancement | Submission | Created
```

Render badges using `latest_*_action_status` and durable success flags. Use `Succeeded` when `has_successful_*` is true and latest status is a newer failed retry; show the newest failed retry in the detail sheet diagnostics.

- [ ] **Step 5: Update action sheet buckets**

In `BrregRawInputActionSheet.tsx`, replace state-derived counts with stats fields:

```text
Needs translation -> Translate
Translation failed -> Retry translation
Ready to submit -> Submit suggestions
Submission failed -> Retry submission
Needs enhancement -> Enhance
Enhancement failed -> Retry enhancement
Suggestion submitted -> Open review
```

Button handlers:

```ts
onTranslate={() => api.translateBrreg({ filters: { state: "input" } })}
onRetryTranslation={() => api.translateBrreg({ filters: { translation_action_status: "failed" } })}
onSubmitSuggestions={() => api.processSource("brreg")}
onRetrySubmission={() => api.processSource("brreg")}
onEnhance={() => api.enhanceBrregDomains({ filters: { state: "input" }, limit: 500 })}
onRetryEnhancement={() => api.enhanceBrregDomains({ filters: { enhancement_action_status: "failed" }, force: true, limit: 500 })}
```

- [ ] **Step 6: Run UI checks**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/ui
pnpm typecheck
pnpm build
```

Expected: both commands exit 0. Existing shadcn sourcemap warnings during build are acceptable if the exit code is 0.

- [ ] **Step 7: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
git add ui/app/types/api.ts ui/app/lib/api.ts ui/app/components/app
git commit -m "feat: show brreg lifecycle and action attributes"
```

## Task 8: End-To-End Verification

**Files:**
- No source edits expected unless verification finds a defect.

- [ ] **Step 1: Run scheduler suite**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./...
```

Expected: PASS.

- [ ] **Step 2: Run data-pipelines suite**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/data-pipelines/services/go-worker
GOWORK=off go test ./...
```

Expected: PASS.

- [ ] **Step 3: Run UI checks**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/ui
pnpm typecheck
pnpm build
```

Expected: both commands exit 0.

- [ ] **Step 4: Browser smoke**

Start the UI dev server:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/ui
pnpm dev --host 127.0.0.1 --port 5174
```

Open:

```text
http://127.0.0.1:5174/sources/brreg/raw_input
```

Verify:

- The table has `State`, `Translation`, `Enhancement`, and `Submission`.
- The state filter only offers `Input`, `Suggestion submitted`, `Completed`, and `Superseded`.
- `?state=input` filters rows without showing `translated` or `translating` as row states.
- `?translation_action_status=failed` filters failed translation attempts.
- The action sheet shows translation, submission, and enhancement buckets divided by horizontal separators.

- [ ] **Step 5: Final git status**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
git status --short --branch

cd /Users/graovic/pulsarpoint/ppoint/data-pipelines
git status --short --branch
```

Expected:

- CorpScout contains only intended build artifact changes or is clean after committing.
- data-pipelines may still show `.github/workflows/go-worker-image.yml` as an unrelated pre-existing dirty file; do not stage it.

## Self-Review

Spec coverage:

- Lifecycle-only `state`: Task 1 and Task 4.
- Append-only actions/events: Task 1, Task 2, Task 3, Task 5, Task 6.
- Current effective attributes: Task 1 view, Task 4 API, Task 7 UI.
- Translation action status history: Task 5.
- Submission action status history: Task 2 and Task 3.
- Enhancement action status history: Task 6.
- Optional enhancement that does not block submission: Task 4 `ReadyToSubmit` predicate and Task 7 action sheet.
- Compatibility fields retained: Task 3 and Task 5 keep `processing_status` and `translation_status`.
- Tests and verification: Tasks 1 through 8.

Placeholder scan:

- The plan contains no placeholder tokens or vague catch-all implementation instructions.
- Each implementation task includes concrete file paths, commands, and expected outcomes.

Type consistency:

- DB view fields use `latest_translation_action_status`, `has_successful_translation`, `latest_enhancement_action_status`, `has_successful_enhancement`, `latest_submission_action_status`, and `has_successful_submission`.
- UI and API fields use the same names.
- Action types are consistently `translate`, `enhance`, and `submit_suggestion`.
- Action event statuses are consistently `notdone`, `queued`, `running`, `succeeded`, `failed`, `skipped`, and `cancelled`, with `notdone` produced by the effective view only.
