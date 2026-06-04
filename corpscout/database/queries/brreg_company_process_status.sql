-- name: EnsureBrregCompanyProcessStatuses :one
WITH inserted AS (
  INSERT INTO brreg_source.company_process_status (company_id)
  SELECT company.id
  FROM brreg_source.v_companies_missing_translations missing
  JOIN brreg_source.companies company
    ON company.id = missing.company_id
   AND company.row_status = 'active'
  LEFT JOIN brreg_source.company_process_status existing
    ON existing.company_id = company.id
  WHERE existing.company_id IS NULL
  ORDER BY missing.min_priority, company.updated_at DESC, company.id
  LIMIT NULLIF(GREATEST(sqlc.arg('limit')::integer, 0), 0)
  ON CONFLICT (company_id) DO NOTHING
  RETURNING company_id
)
SELECT count(*)::integer AS rows_inserted
FROM inserted;

-- name: GetBrregCompanyProcessStatus :one
SELECT *
FROM brreg_source.company_process_status
WHERE company_id = sqlc.arg('company_id')::uuid;

-- name: GetBrregCompanyProcessStatusSummary :one
SELECT
  count(*)::bigint AS companies_tracked,
  count(*) FILTER (WHERE translation_status IN ('pending', 'dirty', 'failed_retryable'))::bigint AS translation_pending,
  count(*) FILTER (WHERE translation_status = 'running' AND coalesce(translation_lease_until, '-infinity'::timestamptz) > now())::bigint AS translation_running,
  count(*) FILTER (WHERE translation_status = 'running' AND coalesce(translation_lease_until, '-infinity'::timestamptz) <= now())::bigint AS translation_stale,
  count(*) FILTER (WHERE translation_status = 'succeeded')::bigint AS translation_succeeded,
  count(*) FILTER (WHERE translation_status = 'skipped')::bigint AS translation_skipped,
  count(*) FILTER (WHERE translation_status = 'failed_terminal')::bigint AS translation_failed_terminal,
  count(*) FILTER (WHERE currency_status IN ('pending', 'dirty', 'failed_retryable'))::bigint AS currency_pending,
  count(*) FILTER (WHERE currency_status = 'running' AND coalesce(currency_lease_until, '-infinity'::timestamptz) > now())::bigint AS currency_running,
  count(*) FILTER (WHERE currency_status = 'running' AND coalesce(currency_lease_until, '-infinity'::timestamptz) <= now())::bigint AS currency_stale,
  count(*) FILTER (WHERE currency_status = 'succeeded')::bigint AS currency_succeeded,
  count(*) FILTER (WHERE currency_status = 'skipped')::bigint AS currency_skipped,
  count(*) FILTER (WHERE currency_status = 'failed_terminal')::bigint AS currency_failed_terminal,
  count(*) FILTER (WHERE financial_status IN ('pending', 'dirty', 'failed_retryable'))::bigint AS financial_pending,
  count(*) FILTER (WHERE financial_status = 'running' AND coalesce(financial_lease_until, '-infinity'::timestamptz) > now())::bigint AS financial_running,
  count(*) FILTER (WHERE financial_status = 'running' AND coalesce(financial_lease_until, '-infinity'::timestamptz) <= now())::bigint AS financial_stale,
  count(*) FILTER (WHERE financial_status = 'succeeded')::bigint AS financial_succeeded,
  count(*) FILTER (WHERE financial_status = 'skipped')::bigint AS financial_skipped,
  count(*) FILTER (WHERE financial_status = 'failed_terminal')::bigint AS financial_failed_terminal
FROM brreg_source.company_process_status status_row
JOIN brreg_source.companies company ON company.id = status_row.company_id
WHERE company.row_status = 'active';

-- name: MarkBrregCompanyTranslationDirty :one
INSERT INTO brreg_source.company_process_status (
  company_id,
  translation_status,
  translation_attempt_count,
  translation_lease_by,
  translation_lease_until,
  translation_last_finished_at,
  translation_error,
  translation_error_category,
  translation_error_code,
  translation_retry_strategy,
  translation_metadata,
  updated_at
) VALUES (
  sqlc.arg('company_id')::uuid,
  'dirty',
  0,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  COALESCE(sqlc.narg('metadata')::jsonb, '{}'::jsonb),
  now()
)
ON CONFLICT (company_id) DO UPDATE
SET
  translation_status = 'dirty',
  translation_attempt_count = 0,
  translation_lease_by = NULL,
  translation_lease_until = NULL,
  translation_last_finished_at = NULL,
  translation_error = NULL,
  translation_error_category = NULL,
  translation_error_code = NULL,
  translation_retry_strategy = NULL,
  translation_metadata = brreg_source.company_process_status.translation_metadata || COALESCE(sqlc.narg('metadata')::jsonb, '{}'::jsonb),
  updated_at = now()
RETURNING *;

-- name: ClaimBrregCompanyTranslationBatch :many
WITH active_capacity AS (
  SELECT
    CASE
      WHEN sqlc.arg('max_parallel_tasks')::integer <= 0 THEN GREATEST(sqlc.arg('limit')::integer, 1)
      ELSE GREATEST(
        sqlc.arg('max_parallel_tasks')::integer - count(*) FILTER (
          WHERE translation_status = 'running'
            AND coalesce(translation_lease_until, '-infinity'::timestamptz) > now()
        )::integer,
        0
      )
    END AS available_slots
  FROM brreg_source.company_process_status
),
picked AS (
  SELECT status_row.company_id
  FROM brreg_source.company_process_status status_row
  JOIN brreg_source.companies company ON company.id = status_row.company_id
  LEFT JOIN brreg_source.v_companies_missing_translations missing
    ON missing.company_id = status_row.company_id
  WHERE company.row_status = 'active'
    AND (
      status_row.translation_status = 'dirty'
      OR (
        status_row.translation_status IN ('pending', 'succeeded', 'skipped')
        AND missing.company_id IS NOT NULL
      )
      OR (
        status_row.translation_status = 'failed_retryable'
        AND status_row.translation_attempt_count < GREATEST(sqlc.arg('max_attempts')::integer, 1)
        AND missing.company_id IS NOT NULL
      )
      OR (
        status_row.translation_status = 'running'
        AND coalesce(status_row.translation_lease_until, '-infinity'::timestamptz) <= now()
        AND status_row.translation_attempt_count < GREATEST(sqlc.arg('max_attempts')::integer, 1)
        AND missing.company_id IS NOT NULL
      )
    )
  ORDER BY status_row.updated_at, missing.min_priority NULLS LAST, status_row.company_id
  LIMIT GREATEST(LEAST(GREATEST(sqlc.arg('limit')::integer, 1), (SELECT available_slots FROM active_capacity)), 0)
  FOR UPDATE OF status_row SKIP LOCKED
),
claimed AS (
  UPDATE brreg_source.company_process_status status_row
  SET
    translation_status = 'running',
    translation_attempt_count = status_row.translation_attempt_count + 1,
    translation_lease_by = sqlc.narg('worker_id')::text,
    translation_lease_until = now() + make_interval(secs => GREATEST(sqlc.arg('lease_seconds')::integer, 1)),
    translation_last_started_at = now(),
    translation_error = NULL,
    translation_error_category = NULL,
    translation_error_code = NULL,
    translation_retry_strategy = NULL,
    updated_at = now()
  FROM picked
  WHERE status_row.company_id = picked.company_id
  RETURNING status_row.*
)
SELECT
  claimed.*,
  company.organization_number,
  company.organization_name
FROM claimed
JOIN brreg_source.companies company ON company.id = claimed.company_id
ORDER BY claimed.updated_at, claimed.company_id;

-- name: ReleaseBrregCompanyTranslationClaim :one
UPDATE brreg_source.company_process_status
SET
  translation_status = 'pending',
  translation_attempt_count = GREATEST(translation_attempt_count - 1, 0),
  translation_lease_by = NULL,
  translation_lease_until = NULL,
  translation_last_started_at = NULL,
  updated_at = now()
WHERE company_id = sqlc.arg('company_id')::uuid
  AND translation_status = 'running'
  AND translation_lease_by = sqlc.arg('worker_id')::text
RETURNING *;

-- name: MarkBrregCompanyTranslationSucceeded :one
UPDATE brreg_source.company_process_status
SET
  translation_status = 'succeeded',
  translation_lease_by = NULL,
  translation_lease_until = NULL,
  translation_last_finished_at = now(),
  translation_error = NULL,
  translation_error_category = NULL,
  translation_error_code = NULL,
  translation_retry_strategy = NULL,
  translation_metadata = translation_metadata || COALESCE(sqlc.narg('metadata')::jsonb, '{}'::jsonb),
  updated_at = now()
WHERE company_id = sqlc.arg('company_id')::uuid
RETURNING *;

-- name: MarkBrregCompanyTranslationSkipped :one
UPDATE brreg_source.company_process_status
SET
  translation_status = 'skipped',
  translation_lease_by = NULL,
  translation_lease_until = NULL,
  translation_last_finished_at = now(),
  translation_error = NULL,
  translation_error_category = NULL,
  translation_error_code = NULL,
  translation_retry_strategy = NULL,
  translation_metadata = translation_metadata || COALESCE(sqlc.narg('metadata')::jsonb, '{}'::jsonb),
  updated_at = now()
WHERE company_id = sqlc.arg('company_id')::uuid
RETURNING *;

-- name: MarkBrregCompanyTranslationFailed :one
UPDATE brreg_source.company_process_status
SET
  translation_status = CASE
    WHEN sqlc.arg('terminal')::boolean
      OR translation_attempt_count >= GREATEST(sqlc.arg('max_attempts')::integer, 1)
    THEN 'failed_terminal'
    ELSE 'failed_retryable'
  END,
  translation_lease_by = NULL,
  translation_lease_until = NULL,
  translation_last_finished_at = now(),
  translation_error = NULLIF(sqlc.arg('error')::text, ''),
  translation_error_category = NULLIF(sqlc.narg('error_category')::text, ''),
  translation_error_code = NULLIF(sqlc.narg('error_code')::text, ''),
  translation_retry_strategy = NULLIF(sqlc.narg('retry_strategy')::text, ''),
  translation_metadata = translation_metadata || COALESCE(sqlc.narg('metadata')::jsonb, '{}'::jsonb),
  updated_at = now()
WHERE company_id = sqlc.arg('company_id')::uuid
RETURNING *;

-- name: MarkBrregCompanyCurrencyDirty :one
INSERT INTO brreg_source.company_process_status (
  company_id,
  currency_status,
  currency_attempt_count,
  currency_lease_by,
  currency_lease_until,
  currency_last_finished_at,
  currency_error,
  currency_error_category,
  currency_error_code,
  currency_retry_strategy,
  currency_metadata,
  updated_at
) VALUES (
  sqlc.arg('company_id')::uuid,
  'dirty',
  0,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  COALESCE(sqlc.narg('metadata')::jsonb, '{}'::jsonb),
  now()
)
ON CONFLICT (company_id) DO UPDATE
SET
  currency_status = 'dirty',
  currency_attempt_count = 0,
  currency_lease_by = NULL,
  currency_lease_until = NULL,
  currency_last_finished_at = NULL,
  currency_error = NULL,
  currency_error_category = NULL,
  currency_error_code = NULL,
  currency_retry_strategy = NULL,
  currency_metadata = brreg_source.company_process_status.currency_metadata || COALESCE(sqlc.narg('metadata')::jsonb, '{}'::jsonb),
  updated_at = now()
RETURNING *;

-- name: ClaimBrregCompanyCurrencyBatch :many
WITH active_capacity AS (
  SELECT
    CASE
      WHEN sqlc.arg('max_parallel_tasks')::integer <= 0 THEN GREATEST(sqlc.arg('limit')::integer, 1)
      ELSE GREATEST(
        sqlc.arg('max_parallel_tasks')::integer - count(*) FILTER (
          WHERE currency_status = 'running'
            AND coalesce(currency_lease_until, '-infinity'::timestamptz) > now()
        )::integer,
        0
      )
    END AS available_slots
  FROM brreg_source.company_process_status
),
picked AS (
  SELECT status_row.company_id
  FROM brreg_source.company_process_status status_row
  JOIN brreg_source.companies company ON company.id = status_row.company_id
  WHERE company.row_status = 'active'
    AND (
      status_row.currency_status IN ('pending', 'dirty', 'failed_retryable')
      OR (
        status_row.currency_status = 'running'
        AND coalesce(status_row.currency_lease_until, '-infinity'::timestamptz) <= now()
      )
    )
    AND status_row.currency_attempt_count < GREATEST(sqlc.arg('max_attempts')::integer, 1)
  ORDER BY status_row.updated_at, status_row.company_id
  LIMIT GREATEST(LEAST(GREATEST(sqlc.arg('limit')::integer, 1), (SELECT available_slots FROM active_capacity)), 0)
  FOR UPDATE OF status_row SKIP LOCKED
),
claimed AS (
  UPDATE brreg_source.company_process_status status_row
  SET
    currency_status = 'running',
    currency_attempt_count = status_row.currency_attempt_count + 1,
    currency_lease_by = sqlc.narg('worker_id')::text,
    currency_lease_until = now() + make_interval(secs => GREATEST(sqlc.arg('lease_seconds')::integer, 1)),
    currency_last_started_at = now(),
    currency_error = NULL,
    currency_error_category = NULL,
    currency_error_code = NULL,
    currency_retry_strategy = NULL,
    updated_at = now()
  FROM picked
  WHERE status_row.company_id = picked.company_id
  RETURNING status_row.*
)
SELECT
  claimed.*,
  company.organization_number,
  company.organization_name
FROM claimed
JOIN brreg_source.companies company ON company.id = claimed.company_id
ORDER BY claimed.updated_at, claimed.company_id;

-- name: MarkBrregCompanyCurrencySucceeded :one
UPDATE brreg_source.company_process_status
SET
  currency_status = 'succeeded',
  currency_lease_by = NULL,
  currency_lease_until = NULL,
  currency_last_finished_at = now(),
  currency_error = NULL,
  currency_error_category = NULL,
  currency_error_code = NULL,
  currency_retry_strategy = NULL,
  currency_metadata = currency_metadata || COALESCE(sqlc.narg('metadata')::jsonb, '{}'::jsonb),
  updated_at = now()
WHERE company_id = sqlc.arg('company_id')::uuid
RETURNING *;

-- name: MarkBrregCompanyCurrencySkipped :one
UPDATE brreg_source.company_process_status
SET
  currency_status = 'skipped',
  currency_lease_by = NULL,
  currency_lease_until = NULL,
  currency_last_finished_at = now(),
  currency_error = NULL,
  currency_error_category = NULL,
  currency_error_code = NULL,
  currency_retry_strategy = NULL,
  currency_metadata = currency_metadata || COALESCE(sqlc.narg('metadata')::jsonb, '{}'::jsonb),
  updated_at = now()
WHERE company_id = sqlc.arg('company_id')::uuid
RETURNING *;

-- name: MarkBrregCompanyCurrencyFailed :one
UPDATE brreg_source.company_process_status
SET
  currency_status = CASE
    WHEN sqlc.arg('terminal')::boolean
      OR currency_attempt_count >= GREATEST(sqlc.arg('max_attempts')::integer, 1)
    THEN 'failed_terminal'
    ELSE 'failed_retryable'
  END,
  currency_lease_by = NULL,
  currency_lease_until = NULL,
  currency_last_finished_at = now(),
  currency_error = NULLIF(sqlc.arg('error')::text, ''),
  currency_error_category = NULLIF(sqlc.narg('error_category')::text, ''),
  currency_error_code = NULLIF(sqlc.narg('error_code')::text, ''),
  currency_retry_strategy = NULLIF(sqlc.narg('retry_strategy')::text, ''),
  currency_metadata = currency_metadata || COALESCE(sqlc.narg('metadata')::jsonb, '{}'::jsonb),
  updated_at = now()
WHERE company_id = sqlc.arg('company_id')::uuid
RETURNING *;

-- name: MarkBrregCompanyFinancialDirty :one
INSERT INTO brreg_source.company_process_status (
  company_id,
  financial_status,
  financial_attempt_count,
  financial_lease_by,
  financial_lease_until,
  financial_last_finished_at,
  financial_error,
  financial_error_category,
  financial_error_code,
  financial_retry_strategy,
  financial_metadata,
  updated_at
) VALUES (
  sqlc.arg('company_id')::uuid,
  'dirty',
  0,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  COALESCE(sqlc.narg('metadata')::jsonb, '{}'::jsonb),
  now()
)
ON CONFLICT (company_id) DO UPDATE
SET
  financial_status = 'dirty',
  financial_attempt_count = 0,
  financial_lease_by = NULL,
  financial_lease_until = NULL,
  financial_last_finished_at = NULL,
  financial_error = NULL,
  financial_error_category = NULL,
  financial_error_code = NULL,
  financial_retry_strategy = NULL,
  financial_metadata = brreg_source.company_process_status.financial_metadata || COALESCE(sqlc.narg('metadata')::jsonb, '{}'::jsonb),
  updated_at = now()
RETURNING *;

-- name: EnsureBrregCompanyFinancialProcessStatuses :one
WITH inserted AS (
  INSERT INTO brreg_source.company_process_status (company_id)
  SELECT company.id
  FROM brreg_source.companies company
  LEFT JOIN brreg_source.company_process_status existing
    ON existing.company_id = company.id
  WHERE company.row_status = 'active'
    AND existing.company_id IS NULL
    AND NOT EXISTS (
      SELECT 1
      FROM brreg_source.financial_statements financial_statement
      WHERE financial_statement.company_id = company.id
    )
  ORDER BY company.updated_at DESC, company.id
  LIMIT NULLIF(GREATEST(sqlc.arg('limit')::integer, 0), 0)
  ON CONFLICT (company_id) DO NOTHING
  RETURNING company_id
)
SELECT count(*)::integer AS rows_inserted
FROM inserted;

-- name: ClaimBrregCompanyFinancialBatch :many
WITH active_capacity AS (
  SELECT
    CASE
      WHEN sqlc.arg('max_parallel_tasks')::integer <= 0 THEN GREATEST(sqlc.arg('limit')::integer, 1)
      ELSE GREATEST(
        sqlc.arg('max_parallel_tasks')::integer - count(*) FILTER (
          WHERE financial_status = 'running'
            AND coalesce(financial_lease_until, '-infinity'::timestamptz) > now()
        )::integer,
        0
      )
    END AS available_slots
  FROM brreg_source.company_process_status
),
picked AS (
  SELECT status_row.company_id
  FROM brreg_source.company_process_status status_row
  JOIN brreg_source.companies company ON company.id = status_row.company_id
  WHERE company.row_status = 'active'
    AND NOT EXISTS (
      SELECT 1
      FROM brreg_source.financial_statements financial_statement
      WHERE financial_statement.company_id = status_row.company_id
    )
    AND (
      status_row.financial_status IN ('pending', 'dirty', 'failed_retryable')
      OR (
        status_row.financial_status = 'running'
        AND coalesce(status_row.financial_lease_until, '-infinity'::timestamptz) <= now()
      )
    )
    AND status_row.financial_attempt_count < GREATEST(sqlc.arg('max_attempts')::integer, 1)
  ORDER BY status_row.updated_at, status_row.company_id
  LIMIT GREATEST(LEAST(GREATEST(sqlc.arg('limit')::integer, 1), (SELECT available_slots FROM active_capacity)), 0)
  FOR UPDATE OF status_row SKIP LOCKED
),
claimed AS (
  UPDATE brreg_source.company_process_status status_row
  SET
    financial_status = 'running',
    financial_attempt_count = status_row.financial_attempt_count + 1,
    financial_lease_by = sqlc.narg('worker_id')::text,
    financial_lease_until = now() + make_interval(secs => GREATEST(sqlc.arg('lease_seconds')::integer, 1)),
    financial_last_started_at = now(),
    financial_error = NULL,
    financial_error_category = NULL,
    financial_error_code = NULL,
    financial_retry_strategy = NULL,
    updated_at = now()
  FROM picked
  WHERE status_row.company_id = picked.company_id
  RETURNING status_row.*
)
SELECT
  claimed.*,
  company.raw_record_id,
  company.organization_number,
  company.organization_name
FROM claimed
JOIN brreg_source.companies company ON company.id = claimed.company_id
ORDER BY claimed.updated_at, claimed.company_id;

-- name: MarkBrregCompanyFinancialSucceeded :one
UPDATE brreg_source.company_process_status
SET
  financial_status = 'succeeded',
  financial_lease_by = NULL,
  financial_lease_until = NULL,
  financial_last_finished_at = now(),
  financial_error = NULL,
  financial_error_category = NULL,
  financial_error_code = NULL,
  financial_retry_strategy = NULL,
  financial_metadata = financial_metadata || COALESCE(sqlc.narg('metadata')::jsonb, '{}'::jsonb),
  updated_at = now()
WHERE company_id = sqlc.arg('company_id')::uuid
RETURNING *;

-- name: MarkBrregCompanyFinancialSkipped :one
UPDATE brreg_source.company_process_status
SET
  financial_status = 'skipped',
  financial_lease_by = NULL,
  financial_lease_until = NULL,
  financial_last_finished_at = now(),
  financial_error = NULL,
  financial_error_category = NULL,
  financial_error_code = NULL,
  financial_retry_strategy = NULL,
  financial_metadata = financial_metadata || COALESCE(sqlc.narg('metadata')::jsonb, '{}'::jsonb),
  updated_at = now()
WHERE company_id = sqlc.arg('company_id')::uuid
RETURNING *;

-- name: MarkBrregCompanyFinancialFailed :one
UPDATE brreg_source.company_process_status
SET
  financial_status = CASE
    WHEN sqlc.arg('terminal')::boolean
      OR financial_attempt_count >= GREATEST(sqlc.arg('max_attempts')::integer, 1)
    THEN 'failed_terminal'
    ELSE 'failed_retryable'
  END,
  financial_lease_by = NULL,
  financial_lease_until = NULL,
  financial_last_finished_at = now(),
  financial_error = NULLIF(sqlc.arg('error')::text, ''),
  financial_error_category = NULLIF(sqlc.narg('error_category')::text, ''),
  financial_error_code = NULLIF(sqlc.narg('error_code')::text, ''),
  financial_retry_strategy = NULLIF(sqlc.narg('retry_strategy')::text, ''),
  financial_metadata = financial_metadata || COALESCE(sqlc.narg('metadata')::jsonb, '{}'::jsonb),
  updated_at = now()
WHERE company_id = sqlc.arg('company_id')::uuid
RETURNING *;
