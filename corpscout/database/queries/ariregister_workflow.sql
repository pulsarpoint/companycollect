-- name: BeginAriregisterWorkflowRun :one
INSERT INTO ariregister_workflow.workflow_runs (
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

-- name: FinishAriregisterWorkflowRunWithStats :one
UPDATE ariregister_workflow.workflow_runs
SET
  status = sqlc.arg('status')::text,
  finished_at = now(),
  records_seen = sqlc.arg('records_seen')::integer,
  records_completed = sqlc.arg('records_completed')::integer,
  records_failed = sqlc.arg('records_failed')::integer,
  error = sqlc.narg('error')::text
WHERE id = sqlc.arg('id')::uuid
RETURNING id;

-- name: CreateAriregisterBulkSnapshot :one
INSERT INTO ariregister_workflow.bulk_snapshots (
  workflow_run_id,
  source_url,
  snapshot_key,
  content_length_bytes,
  payload_hash,
  storage_uri,
  metadata
) VALUES (
  sqlc.narg('workflow_run_id')::uuid,
  sqlc.arg('source_url')::text,
  sqlc.narg('snapshot_key')::text,
  sqlc.narg('content_length_bytes')::bigint,
  sqlc.narg('payload_hash')::text,
  sqlc.narg('storage_uri')::text,
  COALESCE(sqlc.narg('metadata')::jsonb, '{}'::jsonb)
)
RETURNING id;

-- name: MarkAriregisterBulkSnapshotParsed :exec
UPDATE ariregister_workflow.bulk_snapshots
SET
  status = 'parsed',
  metadata = COALESCE(sqlc.narg('metadata')::jsonb, metadata)
WHERE id = sqlc.arg('id')::uuid;

-- name: RecordAriregisterSourceFile :one
INSERT INTO ariregister_workflow.source_files (
  bulk_snapshot_id,
  dataset_key,
  source_url,
  file_name,
  content_type,
  content_length_bytes,
  payload_hash,
  rows_seen,
  rows_written,
  status,
  error,
  metadata
) VALUES (
  sqlc.arg('bulk_snapshot_id')::uuid,
  sqlc.arg('dataset_key')::text,
  sqlc.arg('source_url')::text,
  sqlc.narg('file_name')::text,
  sqlc.narg('content_type')::text,
  sqlc.narg('content_length_bytes')::bigint,
  sqlc.narg('payload_hash')::text,
  sqlc.arg('rows_seen')::integer,
  sqlc.arg('rows_written')::integer,
  sqlc.arg('status')::text,
  sqlc.narg('error')::text,
  COALESCE(sqlc.narg('metadata')::jsonb, '{}'::jsonb)
)
ON CONFLICT (bulk_snapshot_id, dataset_key, source_url) DO UPDATE
SET
  file_name = EXCLUDED.file_name,
  content_type = EXCLUDED.content_type,
  content_length_bytes = EXCLUDED.content_length_bytes,
  payload_hash = EXCLUDED.payload_hash,
  rows_seen = EXCLUDED.rows_seen,
  rows_written = EXCLUDED.rows_written,
  status = EXCLUDED.status,
  error = EXCLUDED.error,
  metadata = EXCLUDED.metadata,
  updated_at = now()
RETURNING id;

-- name: GetCurrentAriregisterWorkflowRawRecord :one
SELECT id, payload_hash
FROM ariregister_workflow.raw_records
WHERE registry_code = sqlc.arg('registry_code')::text
  AND is_current = true;

-- name: SupersedeCurrentAriregisterWorkflowRawRecord :exec
UPDATE ariregister_workflow.raw_records
SET
  is_current = false,
  last_seen_at = now()
WHERE registry_code = sqlc.arg('registry_code')::text
  AND payload_hash <> sqlc.arg('payload_hash')::text
  AND is_current = true;

-- name: UpsertAriregisterWorkflowRawRecord :one
WITH upserted AS (
  INSERT INTO ariregister_workflow.raw_records (
    bulk_snapshot_id,
    source_file_id,
    source_native_id,
    registry_code,
    legal_name,
    registration_status,
    legal_form,
    vat_number,
    website,
    email,
    phone,
    country_iso2,
    source_updated_at,
    raw_payload,
    payload_hash,
    is_current,
    metadata
  ) VALUES (
    sqlc.narg('bulk_snapshot_id')::uuid,
    sqlc.narg('source_file_id')::uuid,
    sqlc.arg('source_native_id')::text,
    sqlc.arg('registry_code')::text,
    sqlc.narg('legal_name')::text,
    sqlc.narg('registration_status')::text,
    sqlc.narg('legal_form')::text,
    sqlc.narg('vat_number')::text,
    sqlc.narg('website')::text,
    sqlc.narg('email')::text,
    sqlc.narg('phone')::text,
    COALESCE(sqlc.narg('country_iso2')::text, 'EE'),
    sqlc.narg('source_updated_at')::timestamptz,
    sqlc.arg('raw_payload')::jsonb,
    sqlc.arg('payload_hash')::text,
    true,
    COALESCE(sqlc.narg('metadata')::jsonb, '{}'::jsonb)
  )
  ON CONFLICT (registry_code, payload_hash) DO UPDATE
  SET
    bulk_snapshot_id = EXCLUDED.bulk_snapshot_id,
    source_file_id = EXCLUDED.source_file_id,
    legal_name = EXCLUDED.legal_name,
    registration_status = EXCLUDED.registration_status,
    legal_form = EXCLUDED.legal_form,
    vat_number = EXCLUDED.vat_number,
    website = EXCLUDED.website,
    email = EXCLUDED.email,
    phone = EXCLUDED.phone,
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
