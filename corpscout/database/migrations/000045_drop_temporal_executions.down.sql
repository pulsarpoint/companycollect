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
