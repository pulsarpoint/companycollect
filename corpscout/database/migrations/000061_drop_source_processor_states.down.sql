CREATE TABLE source_processor_states (
    source_id UUID NOT NULL REFERENCES data_sources(id) ON DELETE CASCADE,
    processor_task_type TEXT NOT NULL,
    last_started_at TIMESTAMPTZ,
    last_success_at TIMESTAMPTZ,
    last_failed_at TIMESTAMPTZ,
    last_processed_marker_type TEXT,
    last_processed_marker TEXT,
    last_processed_at TIMESTAMPTZ,
    last_source_pull_run_id UUID REFERENCES source_pull_runs(id),
    last_error TEXT,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source_id, processor_task_type),
    CONSTRAINT chk_source_processor_states_marker_pair CHECK (
        (last_processed_marker_type IS NULL AND last_processed_marker IS NULL)
        OR (last_processed_marker_type IS NOT NULL AND last_processed_marker IS NOT NULL)
    ),
    CONSTRAINT chk_source_processor_states_failures CHECK (consecutive_failures >= 0)
);
