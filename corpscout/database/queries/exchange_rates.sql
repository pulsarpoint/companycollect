-- name: BeginExchangeRateSyncRun :one
INSERT INTO exchange_rate_sync_runs (
  temporal_workflow_id,
  provider,
  source_url,
  metadata
) VALUES (
  sqlc.arg('temporal_workflow_id'),
  lower(sqlc.arg('provider')),
  sqlc.arg('source_url'),
  COALESCE(sqlc.arg('metadata')::jsonb, '{}'::jsonb)
)
ON CONFLICT (temporal_workflow_id)
DO UPDATE SET
  provider = lower(EXCLUDED.provider),
  source_url = EXCLUDED.source_url,
  status = 'running',
  source_file_id = NULL,
  sheet_id = NULL,
  rate_date = NULL,
  content_sha256 = NULL,
  currencies_seen = 0,
  currencies_imported = 0,
  started_at = now(),
  finished_at = NULL,
  error = NULL,
  metadata = EXCLUDED.metadata
RETURNING *;

-- name: GetExchangeRateSyncRunByWorkflowID :one
SELECT *
FROM exchange_rate_sync_runs
WHERE temporal_workflow_id = $1;

-- name: GetProcessedExchangeRateSourceFileByHash :one
SELECT *
FROM exchange_rate_source_files
WHERE provider = lower(sqlc.arg('provider'))
  AND source_url = sqlc.arg('source_url')
  AND content_sha256 = sqlc.arg('content_sha256')
  AND status = 'processed';

-- name: UpsertDownloadedExchangeRateSourceFile :one
INSERT INTO exchange_rate_source_files (
  provider,
  source_url,
  rate_date,
  content_sha256,
  content_length_bytes,
  content_type,
  etag,
  last_modified,
  status,
  metadata
) VALUES (
  lower(sqlc.arg('provider')),
  sqlc.arg('source_url'),
  sqlc.arg('rate_date')::date,
  sqlc.arg('content_sha256'),
  sqlc.arg('content_length_bytes'),
  sqlc.narg('content_type'),
  sqlc.narg('etag'),
  sqlc.narg('last_modified'),
  'downloaded',
  COALESCE(sqlc.arg('metadata')::jsonb, '{}'::jsonb)
)
ON CONFLICT (provider, source_url, content_sha256)
DO UPDATE SET
  rate_date = EXCLUDED.rate_date,
  content_length_bytes = EXCLUDED.content_length_bytes,
  content_type = EXCLUDED.content_type,
  etag = EXCLUDED.etag,
  last_modified = EXCLUDED.last_modified,
  status = CASE
    WHEN exchange_rate_source_files.status = 'processed' THEN exchange_rate_source_files.status
    ELSE 'downloaded'
  END,
  error = NULL,
  metadata = EXCLUDED.metadata,
  updated_at = now()
RETURNING *;

-- name: MarkExchangeRateSourceFileProcessing :one
UPDATE exchange_rate_source_files
SET status = 'processing',
    error = NULL,
    updated_at = now()
WHERE id = $1
RETURNING *;

-- name: MarkExchangeRateSourceFileProcessed :one
UPDATE exchange_rate_source_files
SET status = 'processed',
    processed_at = now(),
    error = NULL,
    updated_at = now()
WHERE id = $1
RETURNING *;

-- name: MarkExchangeRateSourceFileFailed :one
UPDATE exchange_rate_source_files
SET status = 'failed',
    error = sqlc.arg('error'),
    updated_at = now()
WHERE id = sqlc.arg('id')::uuid
RETURNING *;

-- name: UpsertExchangeRateSheet :one
INSERT INTO exchange_rate_sheets (
  provider,
  rate_date,
  base_currency,
  source_file_id,
  content_sha256,
  metadata
) VALUES (
  lower(sqlc.arg('provider')),
  sqlc.arg('rate_date')::date,
  upper(sqlc.arg('base_currency')),
  sqlc.arg('source_file_id')::uuid,
  sqlc.arg('content_sha256'),
  COALESCE(sqlc.arg('metadata')::jsonb, '{}'::jsonb)
)
ON CONFLICT (provider, rate_date)
DO UPDATE SET
  base_currency = EXCLUDED.base_currency,
  source_file_id = EXCLUDED.source_file_id,
  content_sha256 = EXCLUDED.content_sha256,
  metadata = EXCLUDED.metadata,
  updated_at = CASE
    WHEN exchange_rate_sheets.content_sha256 IS DISTINCT FROM EXCLUDED.content_sha256 THEN now()
    ELSE exchange_rate_sheets.updated_at
  END
RETURNING *;

-- name: UpsertExchangeRate :one
INSERT INTO exchange_rates (
  sheet_id,
  currency,
  rate_per_base,
  metadata
) VALUES (
  sqlc.arg('sheet_id')::uuid,
  upper(sqlc.arg('currency')),
  (sqlc.arg('rate_per_base')::text)::numeric(24, 12),
  COALESCE(sqlc.arg('metadata')::jsonb, '{}'::jsonb)
)
ON CONFLICT (sheet_id, currency)
DO UPDATE SET
  rate_per_base = EXCLUDED.rate_per_base,
  metadata = EXCLUDED.metadata,
  updated_at = CASE
    WHEN exchange_rates.rate_per_base IS DISTINCT FROM EXCLUDED.rate_per_base THEN now()
    ELSE exchange_rates.updated_at
  END
RETURNING *;

-- name: DeleteExchangeRatesNotInCurrencies :one
WITH active_input_currencies AS (
  SELECT unnest(sqlc.arg('currencies')::text[]) AS currency
),
deleted AS (
  DELETE FROM exchange_rates rate
  WHERE rate.sheet_id = sqlc.arg('sheet_id')::uuid
    AND NOT EXISTS (
      SELECT 1
      FROM active_input_currencies input_currency
      WHERE upper(input_currency.currency) = rate.currency
    )
  RETURNING 1
)
SELECT count(*)::integer AS deleted_count FROM deleted;

-- name: FinishExchangeRateSyncRun :one
UPDATE exchange_rate_sync_runs
SET source_file_id = sqlc.narg('source_file_id')::uuid,
    sheet_id = sqlc.narg('sheet_id')::uuid,
    status = sqlc.arg('status'),
    rate_date = sqlc.narg('rate_date')::date,
    content_sha256 = sqlc.narg('content_sha256'),
    currencies_seen = sqlc.arg('currencies_seen'),
    currencies_imported = sqlc.arg('currencies_imported'),
    finished_at = now(),
    error = NULLIF(sqlc.arg('error')::text, ''),
    metadata = COALESCE(sqlc.arg('metadata')::jsonb, '{}'::jsonb)
WHERE id = sqlc.arg('id')::uuid
RETURNING *;

-- name: ListExchangeRateSyncState :many
SELECT *
FROM v_exchange_rate_sync_state
ORDER BY provider, rate_date DESC;

-- name: ListExchangeRateSyncRuns :many
SELECT *
FROM v_exchange_rate_sync_runs
ORDER BY started_at DESC
LIMIT sqlc.arg('limit')::integer;
