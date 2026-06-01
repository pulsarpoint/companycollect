DELETE FROM brreg_workflow.raw_record_task_states
WHERE status IN ('succeeded', 'skipped');

ALTER TABLE brreg_workflow.raw_record_task_states
  DROP CONSTRAINT IF EXISTS chk_brreg_workflow_task_state_status;

ALTER TABLE brreg_workflow.raw_record_task_states
  ADD CONSTRAINT chk_brreg_workflow_task_state_status CHECK (
    status IN ('pending', 'running', 'failed_retryable', 'failed_terminal', 'cancelled')
  );

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
  0::bigint AS task_succeeded,
  0::bigint AS task_skipped,
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
  0::bigint AS task_succeeded,
  0::bigint AS task_skipped,
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
  0::bigint AS task_succeeded,
  0::bigint AS task_skipped,
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
  0::bigint AS task_succeeded,
  0::bigint AS task_skipped,
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
