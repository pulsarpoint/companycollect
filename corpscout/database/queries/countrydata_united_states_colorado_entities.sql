-- name: UpsertUSColoradoEntitiesSource :one
INSERT INTO countrydata_united_states_colorado_entities.sources (
  source_slug,
  source_name,
  source_type,
  base_url,
  country_iso2,
  supports_incremental,
  metadata
) VALUES (
  sqlc.arg('source_slug')::text,
  sqlc.arg('source_name')::text,
  'api_bulk_snapshot',
  sqlc.arg('base_url')::text,
  'US',
  sqlc.arg('supports_incremental')::boolean,
  COALESCE(sqlc.narg('metadata')::jsonb, '{}'::jsonb)
)
ON CONFLICT (source_slug) DO UPDATE
SET
  source_name = EXCLUDED.source_name,
  source_type = EXCLUDED.source_type,
  base_url = EXCLUDED.base_url,
  country_iso2 = EXCLUDED.country_iso2,
  supports_incremental = EXCLUDED.supports_incremental,
  metadata = EXCLUDED.metadata,
  updated_at = now()
RETURNING id;

-- name: RecordUSColoradoEntitiesDownloadRun :one
WITH recorded AS (
  INSERT INTO countrydata_united_states_colorado_entities.download_runs (
    source_id,
    status,
    base_url,
    snapshot_path,
    snapshot_sha256,
    started_at,
    finished_at,
    duration_ms,
    bytes_downloaded,
    records_seen,
    pages_downloaded,
    first_page,
    last_page,
    total_results_reported,
    metadata
  ) VALUES (
    sqlc.arg('source_id')::uuid,
    sqlc.arg('status')::text,
    sqlc.arg('base_url')::text,
    sqlc.narg('snapshot_path')::text,
    sqlc.narg('snapshot_sha256')::text,
    sqlc.arg('started_at')::timestamptz,
    sqlc.narg('finished_at')::timestamptz,
    sqlc.narg('duration_ms')::bigint,
    sqlc.arg('bytes_downloaded')::bigint,
    sqlc.arg('records_seen')::bigint,
    sqlc.arg('pages_downloaded')::integer,
    sqlc.narg('first_page')::integer,
    sqlc.narg('last_page')::integer,
    sqlc.narg('total_results_reported')::bigint,
    COALESCE(sqlc.narg('metadata')::jsonb, '{}'::jsonb)
  )
  ON CONFLICT (source_id, snapshot_sha256)
    WHERE status = 'succeeded' AND snapshot_sha256 IS NOT NULL
  DO UPDATE
  SET
    status = EXCLUDED.status,
    base_url = EXCLUDED.base_url,
    snapshot_path = EXCLUDED.snapshot_path,
    started_at = EXCLUDED.started_at,
    finished_at = EXCLUDED.finished_at,
    duration_ms = EXCLUDED.duration_ms,
    bytes_downloaded = EXCLUDED.bytes_downloaded,
    records_seen = EXCLUDED.records_seen,
    pages_downloaded = EXCLUDED.pages_downloaded,
    first_page = EXCLUDED.first_page,
    last_page = EXCLUDED.last_page,
    total_results_reported = EXCLUDED.total_results_reported,
    metadata = EXCLUDED.metadata,
    updated_at = now()
  RETURNING id, source_id, status, started_at, finished_at, snapshot_path, snapshot_sha256
),
source_update AS (
  UPDATE countrydata_united_states_colorado_entities.sources source
  SET
    last_started_at = recorded.started_at,
    last_success_at = CASE
      WHEN recorded.status = 'succeeded' THEN recorded.finished_at
      ELSE source.last_success_at
    END,
    last_failed_at = CASE
      WHEN recorded.status = 'failed' THEN recorded.finished_at
      ELSE source.last_failed_at
    END,
    last_snapshot_path = CASE
      WHEN recorded.status = 'succeeded' THEN recorded.snapshot_path
      ELSE source.last_snapshot_path
    END,
    last_snapshot_sha256 = CASE
      WHEN recorded.status = 'succeeded' THEN recorded.snapshot_sha256
      ELSE source.last_snapshot_sha256
    END,
    updated_at = now()
  FROM recorded
  WHERE source.id = recorded.source_id
  RETURNING source.id
)
SELECT id
FROM recorded;

-- name: UpdateUSColoradoEntitiesDownloadProcessStats :exec
UPDATE countrydata_united_states_colorado_entities.download_runs
SET
  records_processed = sqlc.arg('records_processed')::bigint,
  records_stored = sqlc.arg('records_stored')::bigint,
  decode_errors = sqlc.arg('decode_errors')::bigint,
  chunks_processed = sqlc.arg('chunks_processed')::bigint,
  finished_at = COALESCE(sqlc.narg('finished_at')::timestamptz, finished_at),
  updated_at = now()
WHERE id = sqlc.arg('id')::uuid;

-- name: GetCurrentUSColoradoEntitiesRawRecord :one
SELECT id, payload_hash
FROM countrydata_united_states_colorado_entities.raw_records
WHERE entity_id = sqlc.arg('entity_id')::text
  AND is_current = true;

-- name: SupersedeCurrentUSColoradoEntitiesRawRecord :exec
UPDATE countrydata_united_states_colorado_entities.raw_records
SET
  is_current = false,
  last_seen_at = now()
WHERE entity_id = sqlc.arg('entity_id')::text
  AND payload_hash <> sqlc.arg('payload_hash')::text
  AND is_current = true;

-- name: UpsertUSColoradoEntitiesRawRecord :one
INSERT INTO countrydata_united_states_colorado_entities.raw_records (
  source_id,
  download_run_id,
  source_native_id,
  entity_id,
  primary_id,
  legal_name,
  alternate_name,
  status,
  is_active,
  legal_form,
  legal_form_code,
  jurisdiction_of_formation,
  is_foreign,
  formation_date,
  country_iso2,
  raw_payload,
  payload_hash,
  is_current,
  metadata
) VALUES (
  sqlc.arg('source_id')::uuid,
  sqlc.narg('download_run_id')::uuid,
  sqlc.arg('source_native_id')::text,
  sqlc.arg('entity_id')::text,
  sqlc.narg('primary_id')::text,
  sqlc.narg('legal_name')::text,
  sqlc.narg('alternate_name')::text,
  sqlc.narg('status')::text,
  sqlc.narg('is_active')::boolean,
  sqlc.narg('legal_form')::text,
  sqlc.narg('legal_form_code')::text,
  sqlc.narg('jurisdiction_of_formation')::text,
  sqlc.narg('is_foreign')::boolean,
  sqlc.narg('formation_date')::date,
  COALESCE(sqlc.narg('country_iso2')::text, 'US'),
  sqlc.arg('raw_payload')::jsonb,
  sqlc.arg('payload_hash')::text,
  true,
  COALESCE(sqlc.narg('metadata')::jsonb, '{}'::jsonb)
)
ON CONFLICT (entity_id, payload_hash) DO UPDATE
SET
  source_id = EXCLUDED.source_id,
  download_run_id = EXCLUDED.download_run_id,
  primary_id = EXCLUDED.primary_id,
  legal_name = EXCLUDED.legal_name,
  alternate_name = EXCLUDED.alternate_name,
  status = EXCLUDED.status,
  is_active = EXCLUDED.is_active,
  legal_form = EXCLUDED.legal_form,
  legal_form_code = EXCLUDED.legal_form_code,
  jurisdiction_of_formation = EXCLUDED.jurisdiction_of_formation,
  is_foreign = EXCLUDED.is_foreign,
  formation_date = EXCLUDED.formation_date,
  country_iso2 = EXCLUDED.country_iso2,
  raw_payload = EXCLUDED.raw_payload,
  is_current = true,
  last_seen_at = now(),
  metadata = EXCLUDED.metadata
RETURNING id;
