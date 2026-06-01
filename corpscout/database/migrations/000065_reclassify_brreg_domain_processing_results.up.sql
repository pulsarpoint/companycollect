WITH related_only_domain_results AS (
  SELECT id
  FROM brreg_workflow.domain_results
  WHERE status = 'succeeded'
    AND best_domain IS NULL
    AND jsonb_array_length(COALESCE(domain_payload->'domains', '[]'::jsonb)) = 0
)
UPDATE brreg_workflow.domain_results dr
SET status = 'not_found'
FROM related_only_domain_results related_only
WHERE dr.id = related_only.id;

WITH analysis_failed_domain_results AS (
  SELECT
    dr.id,
    dr.raw_record_id,
    dr.task_attempt_id,
    COALESCE(
      NULLIF(dr.domain_payload #>> '{site_checks,0,crawl,llm_output,analysis_error,message}', ''),
      NULLIF(dr.domain_payload #>> '{site_checks,0,crawl,llm_output,reason}', ''),
      'Site analysis failed.'
    ) AS error_message
  FROM brreg_workflow.domain_results dr
  WHERE jsonb_array_length(COALESCE(dr.domain_payload->'domains', '[]'::jsonb)) = 0
    AND jsonb_array_length(COALESCE(dr.domain_payload->'related_sites', '[]'::jsonb)) = 0
    AND EXISTS (
      SELECT 1
      FROM jsonb_array_elements(COALESCE(dr.domain_payload->'site_checks', '[]'::jsonb)) AS site_check(value)
      WHERE site_check.value #>> '{crawl,llm_output,analysis_error,code}' = 'site_analysis_failed'
         OR site_check.value #>> '{crawl,llm_output,reason}' LIKE 'Site analysis failed:%'
    )
),
updated_domain_results AS (
  UPDATE brreg_workflow.domain_results dr
  SET
    status = 'failed',
    error = analysis_failed.error_message
  FROM analysis_failed_domain_results analysis_failed
  WHERE dr.id = analysis_failed.id
  RETURNING
    analysis_failed.raw_record_id,
    analysis_failed.task_attempt_id,
    analysis_failed.error_message
),
updated_attempts AS (
  UPDATE brreg_workflow.task_attempts ta
  SET
    status = 'failed',
    finished_at = COALESCE(ta.finished_at, now()),
    error = updated.error_message,
    error_category = 'transient_external',
    error_code = 'site_analysis_failed',
    retry_strategy = 'retry_with_backoff'
  FROM updated_domain_results updated
  WHERE ta.id = updated.task_attempt_id
  RETURNING ta.raw_record_id, ta.id AS task_attempt_id, ta.attempt, updated.error_message
)
UPDATE brreg_workflow.raw_record_task_states ts
SET
  status = CASE
    WHEN updated.attempt >= 3 THEN 'failed_terminal'
    ELSE 'failed_retryable'
  END,
  last_attempt_id = updated.task_attempt_id,
  last_finished_at = now(),
  lease_until = NULL,
  next_retry_at = CASE
    WHEN updated.attempt >= 3 THEN NULL
    ELSE now()
  END,
  last_error = updated.error_message,
  error_category = 'transient_external',
  error_code = 'site_analysis_failed',
  retry_strategy = 'retry_with_backoff',
  updated_at = now()
FROM updated_attempts updated
WHERE ts.raw_record_id = updated.raw_record_id
  AND ts.task_type = 'discover_domains';
