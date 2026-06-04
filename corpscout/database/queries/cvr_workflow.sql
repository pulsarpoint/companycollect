-- name: BeginCVRWorkflowRun :one
INSERT INTO cvr_workflow.workflow_runs (
  orchestrator,
  orchestrator_run_id,
  run_type,
  metadata
) VALUES (
  COALESCE(sqlc.narg('orchestrator')::text, 'temporal'),
  sqlc.arg('orchestrator_run_id')::text,
  sqlc.arg('run_type')::text,
  COALESCE(sqlc.narg('metadata')::jsonb, '{}'::jsonb)
)
ON CONFLICT (orchestrator_run_id) DO UPDATE
SET
  orchestrator = EXCLUDED.orchestrator,
  run_type = EXCLUDED.run_type,
  status = 'running',
  started_at = now(),
  finished_at = NULL,
  error = NULL,
  metadata = EXCLUDED.metadata
RETURNING id;

-- name: FinishCVRWorkflowRunWithStats :one
UPDATE cvr_workflow.workflow_runs
SET
  status = sqlc.arg('status')::text,
  finished_at = now(),
  records_seen = sqlc.arg('records_seen')::integer,
  records_completed = sqlc.arg('records_completed')::integer,
  records_failed = sqlc.arg('records_failed')::integer,
  error = sqlc.narg('error')::text
WHERE id = sqlc.arg('id')::uuid
RETURNING id;

-- name: CreateCVRScrollSession :one
INSERT INTO cvr_workflow.scroll_sessions (
  workflow_run_id,
  source_url,
  scroll_url,
  scroll_ttl,
  page_size,
  record_limit,
  metadata
) VALUES (
  sqlc.narg('workflow_run_id')::uuid,
  sqlc.arg('source_url')::text,
  sqlc.arg('scroll_url')::text,
  sqlc.arg('scroll_ttl')::text,
  sqlc.arg('page_size')::integer,
  sqlc.arg('record_limit')::integer,
  COALESCE(sqlc.narg('metadata')::jsonb, '{}'::jsonb)
)
RETURNING id;

-- name: FinishCVRScrollSession :one
UPDATE cvr_workflow.scroll_sessions
SET
  status = sqlc.arg('status')::text,
  finished_at = now(),
  records_seen = sqlc.arg('records_seen')::integer,
  records_written = sqlc.arg('records_written')::integer,
  last_scroll_id_hash = sqlc.narg('last_scroll_id_hash')::text,
  error = sqlc.narg('error')::text,
  metadata = COALESCE(sqlc.narg('metadata')::jsonb, metadata)
WHERE id = sqlc.arg('id')::uuid
RETURNING id;

-- name: GetCurrentCVRWorkflowRawRecord :one
SELECT id, payload_hash
FROM cvr_workflow.raw_records
WHERE cvr_number = sqlc.arg('cvr_number')::text
  AND is_current = true;

-- name: SupersedeCurrentCVRWorkflowRawRecord :exec
UPDATE cvr_workflow.raw_records
SET
  is_current = false,
  last_seen_at = now()
WHERE cvr_number = sqlc.arg('cvr_number')::text
  AND payload_hash <> sqlc.arg('payload_hash')::text
  AND is_current = true;

-- name: UpsertCVRWorkflowRawRecord :one
WITH upserted AS (
  INSERT INTO cvr_workflow.raw_records (
    scroll_session_id,
    source_native_id,
    cvr_number,
    company_name,
    registration_status,
    company_type,
    website,
    email,
    phone,
    marketing_protected,
    country_iso2,
    source_updated_at,
    raw_payload,
    payload_hash,
    is_current,
    metadata
  ) VALUES (
    sqlc.narg('scroll_session_id')::uuid,
    sqlc.arg('source_native_id')::text,
    sqlc.arg('cvr_number')::text,
    sqlc.narg('company_name')::text,
    sqlc.narg('registration_status')::text,
    sqlc.narg('company_type')::text,
    sqlc.narg('website')::text,
    sqlc.narg('email')::text,
    sqlc.narg('phone')::text,
    sqlc.narg('marketing_protected')::boolean,
    COALESCE(sqlc.narg('country_iso2')::text, 'DK'),
    sqlc.narg('source_updated_at')::timestamptz,
    sqlc.arg('raw_payload')::jsonb,
    sqlc.arg('payload_hash')::text,
    true,
    COALESCE(sqlc.narg('metadata')::jsonb, '{}'::jsonb)
  )
  ON CONFLICT (cvr_number, payload_hash) DO UPDATE
  SET
    scroll_session_id = EXCLUDED.scroll_session_id,
    company_name = EXCLUDED.company_name,
    registration_status = EXCLUDED.registration_status,
    company_type = EXCLUDED.company_type,
    website = EXCLUDED.website,
    email = EXCLUDED.email,
    phone = EXCLUDED.phone,
    marketing_protected = EXCLUDED.marketing_protected,
    country_iso2 = EXCLUDED.country_iso2,
    source_updated_at = EXCLUDED.source_updated_at,
    raw_payload = EXCLUDED.raw_payload,
    is_current = true,
    last_seen_at = now(),
    metadata = EXCLUDED.metadata
  RETURNING id
)
SELECT
  id,
  1::integer AS rows_written
FROM upserted;
