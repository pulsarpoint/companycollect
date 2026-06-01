-- temporal_executions: one row per Temporal workflow started by corpscout.
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

-- Add run_id to raw_inputs tables so Temporal pipeline rows are traceable.
-- Nullable: existing rows written by SourcePullWorker will have run_id = NULL.
ALTER TABLE companies_house_company_raw_inputs ADD COLUMN IF NOT EXISTS run_id TEXT;
ALTER TABLE gleif_company_raw_inputs           ADD COLUMN IF NOT EXISTS run_id TEXT;
ALTER TABLE brreg_company_raw_inputs           ADD COLUMN IF NOT EXISTS run_id TEXT;

CREATE INDEX idx_ch_raw_inputs_run_id    ON companies_house_company_raw_inputs (run_id) WHERE run_id IS NOT NULL;
CREATE INDEX idx_gleif_raw_inputs_run_id ON gleif_company_raw_inputs           (run_id) WHERE run_id IS NOT NULL;
CREATE INDEX idx_brreg_raw_inputs_run_id ON brreg_company_raw_inputs           (run_id) WHERE run_id IS NOT NULL;
