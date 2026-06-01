CREATE OR REPLACE VIEW brreg_workflow.v_raw_record_list AS
WITH latest_translation AS (
  SELECT DISTINCT ON (raw_record_id)
    raw_record_id,
    status,
    created_at
  FROM brreg_workflow.translation_results
  ORDER BY raw_record_id, created_at DESC
),
latest_domain AS (
  SELECT DISTINCT ON (raw_record_id)
    raw_record_id,
    status,
    best_domain,
    created_at
  FROM brreg_workflow.domain_results
  ORDER BY raw_record_id, created_at DESC
),
latest_financial AS (
  SELECT DISTINCT ON (raw_record_id)
    raw_record_id,
    status,
    original_currency,
    created_at
  FROM brreg_workflow.financial_results
  ORDER BY raw_record_id, created_at DESC
),
latest_enhanced AS (
  SELECT DISTINCT ON (raw_record_id)
    raw_record_id,
    status,
    built_at
  FROM brreg_workflow.enhanced_records
  ORDER BY raw_record_id, built_at DESC
),
task_statuses AS (
  SELECT
    raw_record_id,
    jsonb_object_agg(task_type, status ORDER BY task_type) AS statuses
  FROM brreg_workflow.raw_record_task_states
  GROUP BY raw_record_id
),
task_errors AS (
  SELECT
    raw_record_id,
    jsonb_object_agg(
      task_type,
      jsonb_build_object(
        'status', status,
        'error_category', error_category,
        'error_code', error_code,
        'retry_strategy', retry_strategy,
        'last_error', last_error
      )
      ORDER BY task_type
    ) FILTER (WHERE last_error IS NOT NULL OR error_category IS NOT NULL OR error_code IS NOT NULL) AS errors
  FROM brreg_workflow.raw_record_task_states
  GROUP BY raw_record_id
)
SELECT
  rr.id,
  rr.organization_number,
  rr.organization_name,
  rr.website,
  rr.registration_status,
  rr.country_iso2,
  rr.payload_hash,
  rr.is_current,
  rr.first_seen_at,
  rr.last_seen_at,
  COALESCE(lt.status, 'not_started') AS translation_status,
  COALESCE(ld.status, 'not_started') AS domain_status,
  ld.best_domain,
  COALESCE(lf.status, 'not_started') AS financial_status,
  lf.original_currency,
  COALESCE(le.status, 'not_started') AS enhanced_status,
  CASE
    WHEN le.status IN ('built', 'published') THEN 'enhanced'
    WHEN lt.status IN ('succeeded', 'skipped')
     AND COALESCE(ld.status, 'skipped') IN ('succeeded', 'partial', 'not_found', 'skipped')
     AND COALESCE(lf.status, 'skipped') IN ('succeeded', 'not_available', 'skipped') THEN 'ready_to_enhance'
    WHEN lt.status = 'succeeded' THEN 'translated'
    ELSE 'input'
  END AS lifecycle_state,
  COALESCE(ts.statuses, '{}'::jsonb) AS task_statuses,
  COALESCE(te.errors, '{}'::jsonb) AS task_errors
FROM brreg_workflow.raw_records rr
LEFT JOIN latest_translation lt ON lt.raw_record_id = rr.id
LEFT JOIN latest_domain ld ON ld.raw_record_id = rr.id
LEFT JOIN latest_financial lf ON lf.raw_record_id = rr.id
LEFT JOIN latest_enhanced le ON le.raw_record_id = rr.id
LEFT JOIN task_statuses ts ON ts.raw_record_id = rr.id
LEFT JOIN task_errors te ON te.raw_record_id = rr.id
WHERE rr.is_current;

CREATE OR REPLACE VIEW brreg_workflow.v_raw_record_detail AS
WITH latest_translation AS (
  SELECT DISTINCT ON (raw_record_id)
    raw_record_id,
    status,
    translated_payload,
    model,
    prompt_version,
    error,
    created_at,
    metadata
  FROM brreg_workflow.translation_results
  ORDER BY raw_record_id, created_at DESC
),
latest_domain AS (
  SELECT DISTINCT ON (raw_record_id)
    raw_record_id,
    status,
    best_domain,
    domain_payload,
    error,
    created_at,
    metadata
  FROM brreg_workflow.domain_results
  ORDER BY raw_record_id, created_at DESC
),
latest_financial AS (
  SELECT DISTINCT ON (raw_record_id)
    raw_record_id,
    status,
    original_currency,
    original_payload,
    usd_payload,
    fx_metadata,
    source_uri,
    error,
    created_at,
    metadata
  FROM brreg_workflow.financial_results
  ORDER BY raw_record_id, created_at DESC
),
latest_enhanced AS (
  SELECT DISTINCT ON (raw_record_id)
    raw_record_id,
    schema_version,
    status,
    enhanced_payload,
    enhanced_payload_hash,
    built_at,
    published_at,
    error,
    metadata
  FROM brreg_workflow.enhanced_records
  ORDER BY raw_record_id, built_at DESC
),
task_states AS (
  SELECT
    raw_record_id,
    jsonb_agg(
      jsonb_build_object(
        'task_type', task_type,
        'status', status,
        'attempt_count', attempt_count,
        'last_started_at', last_started_at,
        'last_finished_at', last_finished_at,
        'next_retry_at', next_retry_at,
        'lease_until', lease_until,
        'error_category', error_category,
        'error_code', error_code,
        'retry_strategy', retry_strategy,
        'last_error', last_error,
        'result_summary', result_summary
      )
      ORDER BY task_type
    ) AS tasks
  FROM brreg_workflow.raw_record_task_states
  GROUP BY raw_record_id
)
SELECT
  rl.*,
  rr.raw_payload,
  rr.metadata AS raw_metadata,
  jsonb_build_object(
    'status', lt.status,
    'translated_payload', lt.translated_payload,
    'model', lt.model,
    'prompt_version', lt.prompt_version,
    'error', lt.error,
    'created_at', lt.created_at,
    'metadata', COALESCE(lt.metadata, '{}'::jsonb)
  ) AS translation_result,
  jsonb_build_object(
    'status', ld.status,
    'best_domain', ld.best_domain,
    'domain_payload', COALESCE(ld.domain_payload, '{}'::jsonb),
    'error', ld.error,
    'created_at', ld.created_at,
    'metadata', COALESCE(ld.metadata, '{}'::jsonb)
  ) AS domain_result,
  jsonb_build_object(
    'status', lf.status,
    'original_currency', lf.original_currency,
    'original_payload', COALESCE(lf.original_payload, '{}'::jsonb),
    'usd_payload', COALESCE(lf.usd_payload, '{}'::jsonb),
    'fx_metadata', COALESCE(lf.fx_metadata, '{}'::jsonb),
    'source_uri', lf.source_uri,
    'error', lf.error,
    'created_at', lf.created_at,
    'metadata', COALESCE(lf.metadata, '{}'::jsonb)
  ) AS financial_result,
  jsonb_build_object(
    'schema_version', le.schema_version,
    'status', le.status,
    'enhanced_payload', le.enhanced_payload,
    'enhanced_payload_hash', le.enhanced_payload_hash,
    'built_at', le.built_at,
    'published_at', le.published_at,
    'error', le.error,
    'metadata', COALESCE(le.metadata, '{}'::jsonb)
  ) AS enhanced_result,
  COALESCE(ts.tasks, '[]'::jsonb) AS tasks
FROM brreg_workflow.v_raw_record_list rl
JOIN brreg_workflow.raw_records rr ON rr.id = rl.id
LEFT JOIN latest_translation lt ON lt.raw_record_id = rr.id
LEFT JOIN latest_domain ld ON ld.raw_record_id = rr.id
LEFT JOIN latest_financial lf ON lf.raw_record_id = rr.id
LEFT JOIN latest_enhanced le ON le.raw_record_id = rr.id
LEFT JOIN task_states ts ON ts.raw_record_id = rr.id;
