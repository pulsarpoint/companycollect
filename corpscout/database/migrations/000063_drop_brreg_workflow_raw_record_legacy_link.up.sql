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

ALTER TABLE brreg_workflow.raw_records
  DROP COLUMN IF EXISTS corpscout_raw_input_id;
