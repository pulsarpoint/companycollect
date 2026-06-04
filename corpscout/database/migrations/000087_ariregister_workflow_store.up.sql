CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE SCHEMA IF NOT EXISTS ariregister_workflow;

CREATE TABLE ariregister_workflow.workflow_runs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  orchestrator TEXT NOT NULL DEFAULT 'temporal',
  orchestrator_run_id TEXT NOT NULL UNIQUE,
  run_type TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'running',
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at TIMESTAMPTZ,
  records_seen INTEGER NOT NULL DEFAULT 0,
  records_completed INTEGER NOT NULL DEFAULT 0,
  records_failed INTEGER NOT NULL DEFAULT 0,
  error TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  CONSTRAINT chk_ariregister_workflow_runs_type CHECK (
    run_type IN ('bulk_ingest', 'build_source_profile', 'translate', 'discover_domains', 'convert_financials', 'publish')
  ),
  CONSTRAINT chk_ariregister_workflow_runs_status CHECK (
    status IN ('running', 'succeeded', 'failed', 'cancelled')
  ),
  CONSTRAINT chk_ariregister_workflow_runs_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE TABLE ariregister_workflow.bulk_snapshots (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  workflow_run_id UUID REFERENCES ariregister_workflow.workflow_runs(id) ON DELETE SET NULL,
  source_url TEXT NOT NULL,
  snapshot_key TEXT,
  downloaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  content_length_bytes BIGINT,
  payload_hash TEXT,
  storage_uri TEXT,
  status TEXT NOT NULL DEFAULT 'downloaded',
  error TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  CONSTRAINT chk_ariregister_workflow_bulk_snapshot_status CHECK (
    status IN ('downloaded', 'parsed', 'failed')
  ),
  CONSTRAINT chk_ariregister_workflow_bulk_snapshot_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE TABLE ariregister_workflow.source_files (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  bulk_snapshot_id UUID NOT NULL REFERENCES ariregister_workflow.bulk_snapshots(id) ON DELETE CASCADE,
  dataset_key TEXT NOT NULL,
  source_url TEXT NOT NULL,
  file_name TEXT,
  content_type TEXT,
  content_length_bytes BIGINT,
  payload_hash TEXT,
  rows_seen INTEGER NOT NULL DEFAULT 0,
  rows_written INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL DEFAULT 'downloaded',
  error TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_ariregister_workflow_source_file_status CHECK (
    status IN ('downloaded', 'parsed', 'failed')
  ),
  CONSTRAINT chk_ariregister_workflow_source_file_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  UNIQUE (bulk_snapshot_id, dataset_key, source_url)
);

CREATE TABLE ariregister_workflow.raw_records (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  bulk_snapshot_id UUID REFERENCES ariregister_workflow.bulk_snapshots(id) ON DELETE SET NULL,
  source_file_id UUID REFERENCES ariregister_workflow.source_files(id) ON DELETE SET NULL,
  source_native_id TEXT NOT NULL,
  registry_code TEXT NOT NULL,
  legal_name TEXT,
  registration_status TEXT,
  legal_form TEXT,
  vat_number TEXT,
  website TEXT,
  email TEXT,
  phone TEXT,
  country_iso2 TEXT NOT NULL DEFAULT 'EE',
  source_updated_at TIMESTAMPTZ,
  raw_payload JSONB NOT NULL,
  payload_hash TEXT NOT NULL,
  is_current BOOLEAN NOT NULL DEFAULT true,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  CONSTRAINT chk_ariregister_workflow_raw_source_native CHECK (source_native_id = registry_code),
  CONSTRAINT chk_ariregister_workflow_raw_payload_object CHECK (jsonb_typeof(raw_payload) = 'object'),
  CONSTRAINT chk_ariregister_workflow_raw_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  UNIQUE (registry_code, payload_hash)
);

CREATE TABLE ariregister_workflow.task_attempts (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  workflow_run_id UUID REFERENCES ariregister_workflow.workflow_runs(id) ON DELETE SET NULL,
  raw_record_id UUID NOT NULL REFERENCES ariregister_workflow.raw_records(id) ON DELETE CASCADE,
  task_type TEXT NOT NULL,
  attempt INTEGER NOT NULL,
  status TEXT NOT NULL DEFAULT 'running',
  worker_id TEXT,
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at TIMESTAMPTZ,
  error TEXT,
  error_category TEXT,
  error_code TEXT,
  retry_strategy TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  CONSTRAINT chk_ariregister_workflow_task_attempt_type CHECK (
    task_type IN ('build_source_profile', 'translate', 'discover_domains', 'convert_financials', 'publish')
  ),
  CONSTRAINT chk_ariregister_workflow_task_attempt_status CHECK (
    status IN ('running', 'succeeded', 'failed', 'skipped', 'cancelled')
  ),
  CONSTRAINT chk_ariregister_workflow_task_attempt_attempt CHECK (attempt > 0),
  CONSTRAINT chk_ariregister_workflow_task_attempt_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  UNIQUE (raw_record_id, task_type, attempt)
);

CREATE TABLE ariregister_workflow.raw_record_task_states (
  raw_record_id UUID NOT NULL REFERENCES ariregister_workflow.raw_records(id) ON DELETE CASCADE,
  task_type TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  attempt_count INTEGER NOT NULL DEFAULT 0,
  last_attempt_id UUID REFERENCES ariregister_workflow.task_attempts(id) ON DELETE SET NULL,
  last_started_at TIMESTAMPTZ,
  last_finished_at TIMESTAMPTZ,
  next_retry_at TIMESTAMPTZ,
  lease_until TIMESTAMPTZ,
  last_error TEXT,
  error_category TEXT,
  error_code TEXT,
  retry_strategy TEXT,
  result_summary JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (raw_record_id, task_type),
  CONSTRAINT chk_ariregister_workflow_task_state_type CHECK (
    task_type IN ('build_source_profile', 'translate', 'discover_domains', 'convert_financials', 'publish')
  ),
  CONSTRAINT chk_ariregister_workflow_task_state_status CHECK (
    status IN ('pending', 'running', 'failed_retryable', 'failed_terminal', 'cancelled')
  ),
  CONSTRAINT chk_ariregister_workflow_task_state_attempt CHECK (attempt_count >= 0),
  CONSTRAINT chk_ariregister_workflow_task_state_summary_object CHECK (jsonb_typeof(result_summary) = 'object')
);

CREATE INDEX idx_ariregister_workflow_raw_records_registry_code
  ON ariregister_workflow.raw_records (registry_code);

CREATE INDEX idx_ariregister_workflow_raw_records_hash
  ON ariregister_workflow.raw_records (payload_hash);

CREATE UNIQUE INDEX idx_ariregister_workflow_raw_records_current_registry_code
  ON ariregister_workflow.raw_records (registry_code)
  WHERE is_current;

CREATE INDEX idx_ariregister_workflow_raw_records_name
  ON ariregister_workflow.raw_records (legal_name)
  WHERE legal_name IS NOT NULL;

CREATE INDEX idx_ariregister_workflow_task_attempts_raw
  ON ariregister_workflow.task_attempts (raw_record_id, task_type, attempt DESC);

CREATE INDEX idx_ariregister_workflow_task_states_queue
  ON ariregister_workflow.raw_record_task_states (task_type, status, next_retry_at, last_started_at)
  WHERE status IN ('pending', 'running', 'failed_retryable');

CREATE INDEX idx_ariregister_workflow_task_states_lease
  ON ariregister_workflow.raw_record_task_states (task_type, lease_until)
  WHERE status = 'running';
