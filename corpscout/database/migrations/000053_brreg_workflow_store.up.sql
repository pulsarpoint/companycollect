CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE SCHEMA IF NOT EXISTS brreg_workflow;

CREATE TABLE brreg_workflow.workflow_runs (
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
  CONSTRAINT chk_brreg_workflow_runs_type CHECK (
    run_type IN ('bulk_ingest', 'translate', 'discover_domains', 'convert_financials', 'build_enhanced', 'retry_failed', 'publish', 'full_enrichment')
  ),
  CONSTRAINT chk_brreg_workflow_runs_status CHECK (
    status IN ('running', 'succeeded', 'failed', 'cancelled')
  ),
  CONSTRAINT chk_brreg_workflow_runs_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE TABLE brreg_workflow.bulk_snapshots (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  workflow_run_id UUID REFERENCES brreg_workflow.workflow_runs(id) ON DELETE SET NULL,
  source_url TEXT NOT NULL,
  downloaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  content_length_bytes BIGINT,
  compressed_payload_hash TEXT,
  storage_uri TEXT,
  status TEXT NOT NULL DEFAULT 'downloaded',
  error TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  CONSTRAINT chk_brreg_workflow_bulk_snapshot_status CHECK (
    status IN ('downloaded', 'parsed', 'failed')
  ),
  CONSTRAINT chk_brreg_workflow_bulk_snapshot_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE TABLE brreg_workflow.raw_records (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  bulk_snapshot_id UUID REFERENCES brreg_workflow.bulk_snapshots(id) ON DELETE SET NULL,
  source_native_id TEXT NOT NULL,
  organization_number TEXT NOT NULL,
  organization_name TEXT,
  registration_status TEXT,
  website TEXT,
  country_iso2 TEXT NOT NULL DEFAULT 'NO',
  source_updated_at TIMESTAMPTZ,
  raw_payload JSONB NOT NULL,
  payload_hash TEXT NOT NULL,
  is_current BOOLEAN NOT NULL DEFAULT true,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  CONSTRAINT chk_brreg_workflow_raw_source_native CHECK (source_native_id = organization_number),
  CONSTRAINT chk_brreg_workflow_raw_payload_object CHECK (jsonb_typeof(raw_payload) = 'object'),
  CONSTRAINT chk_brreg_workflow_raw_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  UNIQUE (organization_number, payload_hash)
);

CREATE TABLE brreg_workflow.task_attempts (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  workflow_run_id UUID REFERENCES brreg_workflow.workflow_runs(id) ON DELETE SET NULL,
  raw_record_id UUID NOT NULL REFERENCES brreg_workflow.raw_records(id) ON DELETE CASCADE,
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
  CONSTRAINT chk_brreg_workflow_task_attempt_type CHECK (
    task_type IN ('translate', 'discover_domains', 'convert_financials', 'build_enhanced', 'publish')
  ),
  CONSTRAINT chk_brreg_workflow_task_attempt_status CHECK (
    status IN ('running', 'succeeded', 'failed', 'skipped', 'cancelled')
  ),
  CONSTRAINT chk_brreg_workflow_task_attempt_attempt CHECK (attempt > 0),
  CONSTRAINT chk_brreg_workflow_task_attempt_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  UNIQUE (raw_record_id, task_type, attempt)
);

CREATE TABLE brreg_workflow.raw_record_task_states (
  raw_record_id UUID NOT NULL REFERENCES brreg_workflow.raw_records(id) ON DELETE CASCADE,
  task_type TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  attempt_count INTEGER NOT NULL DEFAULT 0,
  last_attempt_id UUID REFERENCES brreg_workflow.task_attempts(id) ON DELETE SET NULL,
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
  CONSTRAINT chk_brreg_workflow_task_state_type CHECK (
    task_type IN ('translate', 'discover_domains', 'convert_financials', 'build_enhanced', 'publish')
  ),
  CONSTRAINT chk_brreg_workflow_task_state_status CHECK (
    status IN ('pending', 'running', 'succeeded', 'skipped', 'failed_retryable', 'failed_terminal', 'cancelled')
  ),
  CONSTRAINT chk_brreg_workflow_task_state_attempt CHECK (attempt_count >= 0),
  CONSTRAINT chk_brreg_workflow_task_state_summary_object CHECK (jsonb_typeof(result_summary) = 'object')
);

CREATE TABLE brreg_workflow.translation_results (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  raw_record_id UUID NOT NULL REFERENCES brreg_workflow.raw_records(id) ON DELETE CASCADE,
  task_attempt_id UUID REFERENCES brreg_workflow.task_attempts(id) ON DELETE SET NULL,
  status TEXT NOT NULL,
  translated_payload JSONB,
  model TEXT,
  prompt_version TEXT,
  error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  CONSTRAINT chk_brreg_workflow_translation_status CHECK (
    status IN ('succeeded', 'failed', 'skipped')
  ),
  CONSTRAINT chk_brreg_workflow_translation_payload_object CHECK (
    translated_payload IS NULL OR jsonb_typeof(translated_payload) = 'object'
  ),
  CONSTRAINT chk_brreg_workflow_translation_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE TABLE brreg_workflow.domain_results (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  raw_record_id UUID NOT NULL REFERENCES brreg_workflow.raw_records(id) ON DELETE CASCADE,
  task_attempt_id UUID REFERENCES brreg_workflow.task_attempts(id) ON DELETE SET NULL,
  status TEXT NOT NULL,
  best_domain TEXT,
  domain_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  CONSTRAINT chk_brreg_workflow_domain_status CHECK (
    status IN ('succeeded', 'not_found', 'partial', 'failed', 'skipped')
  ),
  CONSTRAINT chk_brreg_workflow_domain_payload_object CHECK (jsonb_typeof(domain_payload) = 'object'),
  CONSTRAINT chk_brreg_workflow_domain_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE TABLE brreg_workflow.financial_results (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  raw_record_id UUID NOT NULL REFERENCES brreg_workflow.raw_records(id) ON DELETE CASCADE,
  task_attempt_id UUID REFERENCES brreg_workflow.task_attempts(id) ON DELETE SET NULL,
  status TEXT NOT NULL,
  original_currency TEXT,
  original_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  usd_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  fx_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  source_uri TEXT,
  error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  CONSTRAINT chk_brreg_workflow_financial_status CHECK (
    status IN ('succeeded', 'failed', 'not_available', 'skipped')
  ),
  CONSTRAINT chk_brreg_workflow_financial_original_payload_object CHECK (jsonb_typeof(original_payload) = 'object'),
  CONSTRAINT chk_brreg_workflow_financial_usd_payload_object CHECK (jsonb_typeof(usd_payload) = 'object'),
  CONSTRAINT chk_brreg_workflow_financial_fx_metadata_object CHECK (jsonb_typeof(fx_metadata) = 'object'),
  CONSTRAINT chk_brreg_workflow_financial_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE TABLE brreg_workflow.enhanced_records (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  raw_record_id UUID NOT NULL REFERENCES brreg_workflow.raw_records(id) ON DELETE CASCADE,
  task_attempt_id UUID REFERENCES brreg_workflow.task_attempts(id) ON DELETE SET NULL,
  schema_version TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'built',
  enhanced_payload JSONB NOT NULL,
  enhanced_payload_hash TEXT NOT NULL,
  corpscout_enhanced_raw_input_id UUID REFERENCES brreg_enhanced_raw_inputs(id) ON DELETE SET NULL,
  corpscout_source_company_id UUID REFERENCES brreg_source_companies(id) ON DELETE SET NULL,
  built_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at TIMESTAMPTZ,
  error TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  CONSTRAINT chk_brreg_workflow_enhanced_status CHECK (
    status IN ('built', 'published', 'publish_failed', 'superseded')
  ),
  CONSTRAINT chk_brreg_workflow_enhanced_payload_object CHECK (jsonb_typeof(enhanced_payload) = 'object'),
  CONSTRAINT chk_brreg_workflow_enhanced_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  UNIQUE (raw_record_id, schema_version, enhanced_payload_hash)
);

CREATE INDEX idx_brreg_workflow_raw_records_org
  ON brreg_workflow.raw_records (organization_number);

CREATE INDEX idx_brreg_workflow_raw_records_hash
  ON brreg_workflow.raw_records (payload_hash);

CREATE UNIQUE INDEX idx_brreg_workflow_raw_records_current_org
  ON brreg_workflow.raw_records (organization_number)
  WHERE is_current;

CREATE INDEX idx_brreg_workflow_task_attempts_raw
  ON brreg_workflow.task_attempts (raw_record_id, task_type, attempt DESC);

CREATE INDEX idx_brreg_workflow_task_states_queue
  ON brreg_workflow.raw_record_task_states (task_type, status, next_retry_at, last_started_at)
  WHERE status IN ('pending', 'running', 'failed_retryable');

CREATE INDEX idx_brreg_workflow_task_states_lease
  ON brreg_workflow.raw_record_task_states (task_type, lease_until)
  WHERE status = 'running';

CREATE INDEX idx_brreg_workflow_translation_latest
  ON brreg_workflow.translation_results (raw_record_id, created_at DESC);

CREATE INDEX idx_brreg_workflow_domain_latest
  ON brreg_workflow.domain_results (raw_record_id, created_at DESC);

CREATE INDEX idx_brreg_workflow_financial_latest
  ON brreg_workflow.financial_results (raw_record_id, created_at DESC);

CREATE INDEX idx_brreg_workflow_enhanced_latest
  ON brreg_workflow.enhanced_records (raw_record_id, built_at DESC);

CREATE OR REPLACE VIEW brreg_workflow.v_translation_asset_state AS
WITH current_raw AS (
  SELECT id FROM brreg_workflow.raw_records WHERE is_current
),
task_rows AS (
  SELECT
    cr.id,
    ts.status,
    ts.next_retry_at,
    ts.lease_until,
    ts.last_started_at
  FROM current_raw cr
  LEFT JOIN brreg_workflow.raw_record_task_states ts
    ON ts.raw_record_id = cr.id
   AND ts.task_type = 'translate'
),
latest_artifact AS (
  SELECT DISTINCT ON (raw_record_id)
    raw_record_id,
    status
  FROM brreg_workflow.translation_results
  ORDER BY raw_record_id, created_at DESC
)
SELECT
  'translation_results'::text AS asset,
  count(*)::bigint AS raw_records_current,
  count(*) FILTER (WHERE tr.status IS NULL)::bigint AS task_no_state,
  count(*) FILTER (WHERE tr.status = 'pending')::bigint AS task_pending,
  count(*) FILTER (WHERE tr.status = 'running' AND coalesce(tr.lease_until, tr.last_started_at + interval '30 minutes') > now())::bigint AS task_running_active,
  count(*) FILTER (WHERE tr.status = 'running' AND coalesce(tr.lease_until, tr.last_started_at + interval '30 minutes') <= now())::bigint AS task_running_stale,
  count(*) FILTER (WHERE tr.status = 'failed_retryable')::bigint AS task_failed_retryable,
  count(*) FILTER (WHERE tr.status = 'failed_terminal')::bigint AS task_failed_terminal,
  count(*) FILTER (WHERE tr.status = 'succeeded')::bigint AS task_succeeded,
  count(*) FILTER (WHERE tr.status = 'skipped')::bigint AS task_skipped,
  count(*) FILTER (
    WHERE tr.status IS NULL
       OR tr.status = 'pending'
       OR (tr.status = 'failed_retryable' AND tr.next_retry_at <= now())
       OR (tr.status = 'running' AND coalesce(tr.lease_until, tr.last_started_at + interval '30 minutes') <= now())
  )::bigint AS task_eligible_now,
  count(*) FILTER (WHERE la.status = 'succeeded')::bigint AS artifact_succeeded,
  count(*) FILTER (WHERE la.status = 'skipped')::bigint AS artifact_skipped,
  count(*) FILTER (WHERE la.status = 'failed')::bigint AS artifact_failed,
  count(*) FILTER (WHERE la.raw_record_id IS NULL)::bigint AS artifact_missing
FROM task_rows tr
LEFT JOIN latest_artifact la ON la.raw_record_id = tr.id;

CREATE OR REPLACE VIEW brreg_workflow.v_domain_asset_state AS
WITH current_raw AS (
  SELECT id FROM brreg_workflow.raw_records WHERE is_current
),
task_rows AS (
  SELECT
    cr.id,
    ts.status,
    ts.next_retry_at,
    ts.lease_until,
    ts.last_started_at
  FROM current_raw cr
  LEFT JOIN brreg_workflow.raw_record_task_states ts
    ON ts.raw_record_id = cr.id
   AND ts.task_type = 'discover_domains'
),
latest_artifact AS (
  SELECT DISTINCT ON (raw_record_id)
    raw_record_id,
    status
  FROM brreg_workflow.domain_results
  ORDER BY raw_record_id, created_at DESC
)
SELECT
  'domain_results'::text AS asset,
  count(*)::bigint AS raw_records_current,
  count(*) FILTER (WHERE tr.status IS NULL)::bigint AS task_no_state,
  count(*) FILTER (WHERE tr.status = 'pending')::bigint AS task_pending,
  count(*) FILTER (WHERE tr.status = 'running' AND coalesce(tr.lease_until, tr.last_started_at + interval '30 minutes') > now())::bigint AS task_running_active,
  count(*) FILTER (WHERE tr.status = 'running' AND coalesce(tr.lease_until, tr.last_started_at + interval '30 minutes') <= now())::bigint AS task_running_stale,
  count(*) FILTER (WHERE tr.status = 'failed_retryable')::bigint AS task_failed_retryable,
  count(*) FILTER (WHERE tr.status = 'failed_terminal')::bigint AS task_failed_terminal,
  count(*) FILTER (WHERE tr.status = 'succeeded')::bigint AS task_succeeded,
  count(*) FILTER (WHERE tr.status = 'skipped')::bigint AS task_skipped,
  count(*) FILTER (
    WHERE tr.status IS NULL
       OR tr.status = 'pending'
       OR (tr.status = 'failed_retryable' AND tr.next_retry_at <= now())
       OR (tr.status = 'running' AND coalesce(tr.lease_until, tr.last_started_at + interval '30 minutes') <= now())
  )::bigint AS task_eligible_now,
  count(*) FILTER (WHERE la.status IN ('succeeded', 'partial', 'not_found'))::bigint AS artifact_succeeded,
  count(*) FILTER (WHERE la.status = 'skipped')::bigint AS artifact_skipped,
  count(*) FILTER (WHERE la.status = 'failed')::bigint AS artifact_failed,
  count(*) FILTER (WHERE la.raw_record_id IS NULL)::bigint AS artifact_missing
FROM task_rows tr
LEFT JOIN latest_artifact la ON la.raw_record_id = tr.id;

CREATE OR REPLACE VIEW brreg_workflow.v_financial_asset_state AS
WITH current_raw AS (
  SELECT id FROM brreg_workflow.raw_records WHERE is_current
),
task_rows AS (
  SELECT
    cr.id,
    ts.status,
    ts.next_retry_at,
    ts.lease_until,
    ts.last_started_at
  FROM current_raw cr
  LEFT JOIN brreg_workflow.raw_record_task_states ts
    ON ts.raw_record_id = cr.id
   AND ts.task_type = 'convert_financials'
),
latest_artifact AS (
  SELECT DISTINCT ON (raw_record_id)
    raw_record_id,
    status
  FROM brreg_workflow.financial_results
  ORDER BY raw_record_id, created_at DESC
)
SELECT
  'financial_results'::text AS asset,
  count(*)::bigint AS raw_records_current,
  count(*) FILTER (WHERE tr.status IS NULL)::bigint AS task_no_state,
  count(*) FILTER (WHERE tr.status = 'pending')::bigint AS task_pending,
  count(*) FILTER (WHERE tr.status = 'running' AND coalesce(tr.lease_until, tr.last_started_at + interval '30 minutes') > now())::bigint AS task_running_active,
  count(*) FILTER (WHERE tr.status = 'running' AND coalesce(tr.lease_until, tr.last_started_at + interval '30 minutes') <= now())::bigint AS task_running_stale,
  count(*) FILTER (WHERE tr.status = 'failed_retryable')::bigint AS task_failed_retryable,
  count(*) FILTER (WHERE tr.status = 'failed_terminal')::bigint AS task_failed_terminal,
  count(*) FILTER (WHERE tr.status = 'succeeded')::bigint AS task_succeeded,
  count(*) FILTER (WHERE tr.status = 'skipped')::bigint AS task_skipped,
  count(*) FILTER (
    WHERE tr.status IS NULL
       OR tr.status = 'pending'
       OR (tr.status = 'failed_retryable' AND tr.next_retry_at <= now())
       OR (tr.status = 'running' AND coalesce(tr.lease_until, tr.last_started_at + interval '30 minutes') <= now())
  )::bigint AS task_eligible_now,
  count(*) FILTER (WHERE la.status = 'succeeded')::bigint AS artifact_succeeded,
  count(*) FILTER (WHERE la.status IN ('skipped', 'not_available'))::bigint AS artifact_skipped,
  count(*) FILTER (WHERE la.status = 'failed')::bigint AS artifact_failed,
  count(*) FILTER (WHERE la.raw_record_id IS NULL)::bigint AS artifact_missing
FROM task_rows tr
LEFT JOIN latest_artifact la ON la.raw_record_id = tr.id;

CREATE OR REPLACE VIEW brreg_workflow.v_enhanced_asset_state AS
WITH current_raw AS (
  SELECT id FROM brreg_workflow.raw_records WHERE is_current
),
task_rows AS (
  SELECT
    cr.id,
    ts.status,
    ts.next_retry_at,
    ts.lease_until,
    ts.last_started_at
  FROM current_raw cr
  LEFT JOIN brreg_workflow.raw_record_task_states ts
    ON ts.raw_record_id = cr.id
   AND ts.task_type = 'build_enhanced'
),
latest_artifact AS (
  SELECT DISTINCT ON (raw_record_id)
    raw_record_id,
    status
  FROM brreg_workflow.enhanced_records
  ORDER BY raw_record_id, built_at DESC
)
SELECT
  'enhanced_records'::text AS asset,
  count(*)::bigint AS raw_records_current,
  count(*) FILTER (WHERE tr.status IS NULL)::bigint AS task_no_state,
  count(*) FILTER (WHERE tr.status = 'pending')::bigint AS task_pending,
  count(*) FILTER (WHERE tr.status = 'running' AND coalesce(tr.lease_until, tr.last_started_at + interval '30 minutes') > now())::bigint AS task_running_active,
  count(*) FILTER (WHERE tr.status = 'running' AND coalesce(tr.lease_until, tr.last_started_at + interval '30 minutes') <= now())::bigint AS task_running_stale,
  count(*) FILTER (WHERE tr.status = 'failed_retryable')::bigint AS task_failed_retryable,
  count(*) FILTER (WHERE tr.status = 'failed_terminal')::bigint AS task_failed_terminal,
  count(*) FILTER (WHERE tr.status = 'succeeded')::bigint AS task_succeeded,
  count(*) FILTER (WHERE tr.status = 'skipped')::bigint AS task_skipped,
  count(*) FILTER (
    WHERE tr.status IS NULL
       OR tr.status = 'pending'
       OR (tr.status = 'failed_retryable' AND tr.next_retry_at <= now())
       OR (tr.status = 'running' AND coalesce(tr.lease_until, tr.last_started_at + interval '30 minutes') <= now())
  )::bigint AS task_eligible_now,
  count(*) FILTER (WHERE la.status IN ('built', 'published'))::bigint AS artifact_succeeded,
  count(*) FILTER (WHERE la.status = 'superseded')::bigint AS artifact_skipped,
  count(*) FILTER (WHERE la.status = 'publish_failed')::bigint AS artifact_failed,
  count(*) FILTER (WHERE la.raw_record_id IS NULL)::bigint AS artifact_missing
FROM task_rows tr
LEFT JOIN latest_artifact la ON la.raw_record_id = tr.id;

CREATE OR REPLACE VIEW brreg_workflow.v_enhanced_ready_records AS
WITH current_raw AS (
  SELECT
    id,
    organization_number,
    organization_name,
    registration_status,
    website,
    country_iso2,
    raw_payload,
    payload_hash,
    last_seen_at
  FROM brreg_workflow.raw_records
  WHERE is_current
),
latest_translation AS (
  SELECT DISTINCT ON (raw_record_id)
    raw_record_id,
    status,
    translated_payload
  FROM brreg_workflow.translation_results
  ORDER BY raw_record_id, created_at DESC
),
latest_domain AS (
  SELECT DISTINCT ON (raw_record_id)
    raw_record_id,
    status,
    best_domain,
    domain_payload
  FROM brreg_workflow.domain_results
  ORDER BY raw_record_id, created_at DESC
),
latest_financial AS (
  SELECT DISTINCT ON (raw_record_id)
    raw_record_id,
    status,
    original_payload,
    usd_payload,
    fx_metadata
  FROM brreg_workflow.financial_results
  ORDER BY raw_record_id, created_at DESC
),
task_statuses AS (
  SELECT
    raw_record_id,
    jsonb_object_agg(task_type, status ORDER BY task_type) AS statuses
  FROM brreg_workflow.raw_record_task_states
  GROUP BY raw_record_id
)
SELECT
  rr.id,
  rr.organization_number,
  rr.organization_name,
  rr.registration_status,
  rr.website,
  rr.country_iso2,
  rr.raw_payload,
  rr.payload_hash,
  rr.last_seen_at AS raw_last_seen_at,
  lt.status AS translation_status,
  COALESCE(lt.translated_payload, '{}'::jsonb) AS translation_payload,
  COALESCE(ld.status, 'skipped') AS domain_status,
  ld.best_domain,
  COALESCE(ld.domain_payload, '{}'::jsonb) AS domain_payload,
  COALESCE(lf.status, 'skipped') AS financial_status,
  COALESCE(lf.original_payload, '{}'::jsonb) AS original_payload,
  COALESCE(lf.usd_payload, '{}'::jsonb) AS usd_payload,
  COALESCE(lf.fx_metadata, '{}'::jsonb) AS fx_metadata,
  COALESCE(ts.statuses, '{}'::jsonb) AS task_statuses
FROM current_raw rr
JOIN latest_translation lt
  ON lt.raw_record_id = rr.id
 AND lt.status IN ('succeeded', 'skipped')
LEFT JOIN latest_domain ld
  ON ld.raw_record_id = rr.id
 AND ld.status IN ('succeeded', 'partial', 'not_found', 'skipped')
LEFT JOIN latest_financial lf
  ON lf.raw_record_id = rr.id
 AND lf.status IN ('succeeded', 'not_available', 'skipped')
LEFT JOIN task_statuses ts ON ts.raw_record_id = rr.id
WHERE NOT EXISTS (
  SELECT 1
  FROM brreg_workflow.enhanced_records er
  WHERE er.raw_record_id = rr.id
    AND er.status IN ('built', 'published')
);
