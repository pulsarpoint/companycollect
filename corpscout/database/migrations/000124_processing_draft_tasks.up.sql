-- Opt-in draft queues reuse processing.tasks.task_id and existing typed inputs.
-- NULL queue_scope preserves the lifecycle of every existing workflow.
ALTER TABLE processing.tasks
    ADD COLUMN queue_scope text,
    ADD COLUMN frozen_at timestamptz,
    ADD COLUMN completed_at timestamptz,
    ADD COLUMN inputs_purged_at timestamptz;

ALTER TABLE processing.tasks DROP CONSTRAINT tasks_status_check;
ALTER TABLE processing.tasks ADD CONSTRAINT tasks_status_check
    CHECK (status IN ('draft','preparing','selected','ready','completed','cancelled'));

ALTER TABLE processing.tasks ADD CONSTRAINT tasks_queue_lifecycle_check CHECK (
    (queue_scope IS NULL
        AND status IN ('preparing','selected','ready','cancelled')
        AND frozen_at IS NULL AND completed_at IS NULL AND inputs_purged_at IS NULL)
    OR
    (queue_scope IS NOT NULL AND btrim(queue_scope) <> ''
        AND status IN ('draft','selected','ready','completed','cancelled')
        AND (status <> 'draft' OR frozen_at IS NULL)
        AND (status NOT IN ('selected','ready','completed') OR frozen_at IS NOT NULL)
        AND ((status = 'completed') = (completed_at IS NOT NULL))
        AND (completed_at IS NULL OR completed_at >= frozen_at)
        AND (inputs_purged_at IS NULL OR
            (completed_at IS NOT NULL AND inputs_purged_at >= completed_at)))
);

-- Concurrent find-or-create callers cannot create two default drafts in one scope.
CREATE UNIQUE INDEX processing_one_draft_task
    ON processing.tasks (queue_scope, processor) WHERE status = 'draft';
CREATE INDEX processing_completed_input_cleanup
    ON processing.tasks (completed_at, task_id)
    WHERE queue_scope IS NOT NULL AND status = 'completed' AND inputs_purged_at IS NULL;

COMMENT ON COLUMN processing.tasks.queue_scope IS
    'Opt-in queue owner/workspace scope. NULL means legacy task; never auto-adopt legacy tasks as drafts.';
COMMENT ON COLUMN processing.tasks.frozen_at IS
    'Membership freeze time. All ClickHouse inputs with this task_id share this freeze; no per-input flag.';
COMMENT ON COLUMN processing.tasks.completed_at IS
    'Set by the processor only after successful completion and durable result publication; not merely Dagster run termination.';
COMMENT ON COLUMN processing.tasks.inputs_purged_at IS
    'Set after retained input deletion has been verified. Task metadata and results are retained.';

-- One receipt per source addition; bulk inputs and per-input provenance stay in
-- the processor-specific ClickHouse tables. Writers/freeze use the same task lock.
CREATE TABLE processing.input_submissions (
    submission_id uuid PRIMARY KEY,
    task_id uuid NOT NULL REFERENCES processing.tasks (task_id),
    source_name text NOT NULL CHECK (btrim(source_name) <> ''),
    selection_config jsonb NOT NULL CHECK (jsonb_typeof(selection_config) = 'object'),
    selection_fingerprint text NOT NULL CHECK (selection_fingerprint ~ '^[0-9a-f]{64}$'),
    manifest_uri text CHECK (manifest_uri IS NULL OR btrim(manifest_uri) <> ''),
    status text NOT NULL DEFAULT 'preparing'
        CHECK (status IN ('preparing','completed','failed','cancelled')),
    input_count bigint CHECK (input_count >= 0),
    created_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    error_message text,
    CONSTRAINT input_submissions_completion_check CHECK (
        ((status = 'preparing') = (finished_at IS NULL))
        AND (status <> 'completed' OR input_count IS NOT NULL)
        AND (finished_at IS NULL OR finished_at >= created_at)
    )
);
CREATE INDEX processing_task_submissions
    ON processing.input_submissions (task_id, created_at, submission_id);
COMMENT ON TABLE processing.input_submissions IS
    'Import receipts, not queue items. Retry the same submission_id/config; freeze requires every submission completed or safely cancelled.';
COMMENT ON COLUMN processing.input_submissions.selection_config IS
    'Source relation, filters, mapping or manual-input object reference. No credentials or bulk input payloads.';
COMMENT ON COLUMN processing.input_submissions.input_count IS
    'Distinct validated inputs contributed by this submission. Counts may overlap across submissions; task.total is the deduplicated total.';
COMMENT ON COLUMN processing.input_submissions.manifest_uri IS
    'Immutable normalized source selection in object storage, including source record identities, for retry and provenance.';
