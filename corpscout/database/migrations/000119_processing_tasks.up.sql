-- Application work state. Never stored in Dagster's internal metadata tables.
CREATE SCHEMA processing;

CREATE TABLE processing.tasks (
    task_id uuid PRIMARY KEY,
    processor text NOT NULL,
    config jsonb NOT NULL,
    work_config jsonb NOT NULL,
    status text NOT NULL CHECK (status IN ('preparing', 'ready', 'cancelled')),
    total bigint NOT NULL DEFAULT 0 CHECK (total >= 0),
    created_at timestamptz NOT NULL DEFAULT now(),
    ready_at timestamptz
);
CREATE TABLE processing.items (
    task_id uuid NOT NULL REFERENCES processing.tasks,
    input_id text NOT NULL CHECK (btrim(input_id) <> ''),
    input_data jsonb NOT NULL,
    work_key text NOT NULL,
    query text NOT NULL CHECK (btrim(query) <> ''),
    state text NOT NULL DEFAULT 'queued' CHECK (state IN
        ('queued', 'running', 'retry_wait', 'succeeded', 'terminal_failed', 'skipped', 'cancelled')),
    attempt integer NOT NULL DEFAULT 0,
    lease_owner text,
    lease_token uuid,
    lease_expires_at timestamptz,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    accepted_result_id uuid,
    PRIMARY KEY (task_id, input_id)
);
CREATE INDEX processing_claimable ON processing.items (task_id, next_attempt_at, input_id)
    WHERE state IN ('queued', 'retry_wait');
CREATE INDEX processing_expired ON processing.items (task_id, lease_expires_at)
    WHERE state = 'running';
CREATE INDEX processing_lease_owner ON processing.items (lease_owner) WHERE state = 'running';

CREATE TABLE processing.export_batches (
    batch_id uuid PRIMARY KEY,
    task_id uuid NOT NULL REFERENCES processing.tasks,
    destination text NOT NULL,
    result_count bigint NOT NULL CHECK (result_count > 0),
    created_at timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz
);
CREATE INDEX processing_unpublished_batches ON processing.export_batches (task_id, created_at)
    WHERE published_at IS NULL;
CREATE TABLE processing.results (
    result_id uuid PRIMARY KEY,
    task_id uuid NOT NULL,
    input_id text NOT NULL,
    work_key text NOT NULL,
    attempt integer NOT NULL,
    status text NOT NULL CHECK (status IN ('success', 'error')),
    payload jsonb NOT NULL,
    completed_at timestamptz NOT NULL,
    export_batch_id uuid REFERENCES processing.export_batches,
    FOREIGN KEY (task_id, input_id) REFERENCES processing.items,
    UNIQUE (task_id, input_id, attempt)
);
ALTER TABLE processing.items ADD FOREIGN KEY (accepted_result_id) REFERENCES processing.results;
CREATE INDEX processing_result_cache ON processing.results (work_key, completed_at DESC)
    WHERE status = 'success';
CREATE INDEX processing_result_batch ON processing.results (export_batch_id);
CREATE INDEX processing_unassigned_results ON processing.results (task_id, completed_at)
    WHERE export_batch_id IS NULL;

-- String batch identity keeps the remote ClickHouse equality predicate simple.
CREATE VIEW processing.brave_export AS
SELECT r.result_id::text AS result_id, r.task_id::text AS task_id,
    r.input_id, r.work_key, r.attempt, r.status,
    coalesce(r.export_batch_id::text, '') AS export_batch_id,
    coalesce(i.input_data->>'country_code', '') AS country_code,
    coalesce(i.input_data->>'company_id', r.input_id) AS company_id,
    coalesce(i.input_data->>'company_name', '') AS company_name,
    i.query, coalesce(t.config->>'query_type', '') AS query_type,
    t.processor AS processor_version,
    coalesce(r.payload->>'answer_text', '') AS answer_text,
    coalesce(r.payload->>'route', '') AS route,
    coalesce(r.payload->>'source_url', '') AS source_url,
    coalesce(r.payload->>'error_type', '') AS error_type,
    coalesce(r.payload->>'source_run_id', '') AS source_run_id,
    r.completed_at
FROM processing.results r
JOIN processing.items i USING (task_id, input_id)
JOIN processing.tasks t USING (task_id)
WHERE t.processor = 'brave-v2';

CREATE VIEW processing.task_progress AS
SELECT t.task_id, t.processor, t.status, t.total,
    count(*) FILTER (WHERE i.state='queued') AS queued,
    count(*) FILTER (WHERE i.state='running') AS running,
    count(*) FILTER (WHERE i.state='retry_wait') AS retry_wait,
    count(*) FILTER (WHERE i.state='succeeded') AS succeeded,
    count(*) FILTER (WHERE i.state='terminal_failed') AS terminal_failed,
    count(*) FILTER (WHERE i.state='skipped') AS skipped,
    count(*) FILTER (WHERE i.state='cancelled') AS cancelled,
    count(*) FILTER (WHERE i.state IN ('queued','running','retry_wait')) AS remaining,
    (SELECT count(*) FROM processing.results r
     LEFT JOIN processing.export_batches b ON b.batch_id=r.export_batch_id
     WHERE r.task_id=t.task_id AND b.published_at IS NULL) AS unpublished
FROM processing.tasks t LEFT JOIN processing.items i USING (task_id)
GROUP BY t.task_id;
