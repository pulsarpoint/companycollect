CREATE TABLE data_source_files (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source_id UUID NOT NULL REFERENCES data_sources(id) ON DELETE CASCADE,
  file_key TEXT NOT NULL,
  display_name TEXT NOT NULL,
  description TEXT,
  kind TEXT NOT NULL,
  required BOOLEAN NOT NULL DEFAULT true,
  relative_path TEXT NOT NULL,
  enabled BOOLEAN NOT NULL DEFAULT true,
  sort_order INTEGER NOT NULL DEFAULT 0,
  config JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (source_id, file_key),
  CONSTRAINT chk_data_source_files_file_key CHECK (btrim(file_key) <> ''),
  CONSTRAINT chk_data_source_files_display_name CHECK (btrim(display_name) <> ''),
  CONSTRAINT chk_data_source_files_relative_path CHECK (btrim(relative_path) <> ''),
  CONSTRAINT chk_data_source_files_kind CHECK (
    kind IN ('source_snapshot', 'code_list', 'reference_data', 'archive')
  ),
  CONSTRAINT chk_data_source_files_config_object CHECK (jsonb_typeof(config) = 'object')
);

CREATE TABLE data_source_file_runs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source_id UUID NOT NULL REFERENCES data_sources(id) ON DELETE CASCADE,
  source_file_id UUID NOT NULL REFERENCES data_source_files(id) ON DELETE CASCADE,
  parent_action_run_id UUID REFERENCES data_source_action_runs(id) ON DELETE SET NULL,
  status TEXT NOT NULL,
  temporal_workflow_id TEXT,
  temporal_run_id TEXT,
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at TIMESTAMPTZ,
  path TEXT,
  content_sha256 TEXT,
  content_length_bytes BIGINT,
  records_written BIGINT,
  error_message TEXT,
  log JSONB NOT NULL DEFAULT '[]'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_data_source_file_runs_status CHECK (
    status IN ('running', 'succeeded', 'failed', 'missing', 'skipped', 'cancelled')
  ),
  CONSTRAINT chk_data_source_file_runs_log_array CHECK (jsonb_typeof(log) = 'array'),
  CONSTRAINT chk_data_source_file_runs_finished_at CHECK (
    (status = 'running' AND finished_at IS NULL)
    OR (status <> 'running' AND finished_at IS NOT NULL)
  )
);

CREATE INDEX idx_data_source_files_source_sort
  ON data_source_files (source_id, sort_order, file_key);

CREATE INDEX idx_data_source_file_runs_file_started
  ON data_source_file_runs (source_file_id, started_at DESC);

CREATE INDEX idx_data_source_file_runs_source_status
  ON data_source_file_runs (source_id, status, started_at DESC);

CREATE INDEX idx_data_source_file_runs_parent_action
  ON data_source_file_runs (parent_action_run_id)
  WHERE parent_action_run_id IS NOT NULL;
