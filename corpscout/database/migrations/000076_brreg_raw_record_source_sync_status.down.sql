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
