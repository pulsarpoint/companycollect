-- name: PrepareBrregTranslationQueue :one
WITH selected_companies AS (
  SELECT translation_status.company_id
  FROM brreg_source.mv_company_translation_status translation_status
  JOIN brreg_source.companies company ON company.id = translation_status.company_id
  LEFT JOIN brreg_source.mv_company_explorer entry ON entry.company_id = translation_status.company_id
  WHERE translation_status.translation_missing_count > 0
    AND (
      COALESCE(cardinality(sqlc.arg('company_ids')::uuid[]), 0) = 0
      OR translation_status.company_id = ANY(sqlc.arg('company_ids')::uuid[])
    )
    AND (
      sqlc.narg('query')::text IS NULL
      OR company.organization_name ILIKE '%' || sqlc.narg('query')::text || '%'
      OR company.organization_number ILIKE '%' || sqlc.narg('query')::text || '%'
      OR coalesce(entry.primary_industry_label, '') ILIKE '%' || sqlc.narg('query')::text || '%'
      OR coalesce(entry.city, '') ILIKE '%' || sqlc.narg('query')::text || '%'
      OR coalesce(entry.municipality, '') ILIKE '%' || sqlc.narg('query')::text || '%'
    )
    AND (sqlc.narg('lifecycle_status')::text IS NULL OR company.lifecycle_status = sqlc.narg('lifecycle_status')::text)
    AND (sqlc.narg('registration_status')::text IS NULL OR company.registration_status = sqlc.narg('registration_status')::text)
    AND (sqlc.narg('translation_status')::text IS NULL OR sqlc.narg('translation_status')::text = 'missing')
    AND (
      sqlc.narg('website_status')::text IS NULL
      OR (sqlc.narg('website_status')::text = 'with' AND entry.website_count > 0)
      OR (sqlc.narg('website_status')::text = 'without' AND entry.website_count = 0)
    )
  ORDER BY translation_status.min_missing_priority ASC,
    coalesce(entry.updated_at, translation_status.updated_at) DESC,
    company.organization_number ASC,
    translation_status.company_id ASC
  LIMIT NULLIF(GREATEST(sqlc.arg('company_limit')::integer, 0), 0)
),
estimated AS (
  SELECT
    missing.company_id,
    count(*)::integer AS missing_field_count,
    GREATEST(sum(length(btrim(missing.source_text)))::integer, 0) AS num_of_characters
  FROM brreg_source.v_missing_translations missing
  JOIN selected_companies selected ON selected.company_id = missing.company_id
  WHERE nullif(btrim(missing.source_text), '') IS NOT NULL
  GROUP BY missing.company_id
),
deleted_terminal AS (
  DELETE FROM brreg_source.translation_queue_entries queue
  USING estimated
  WHERE queue.company_id = estimated.company_id
    AND queue.status IN ('succeeded', 'failed')
  RETURNING queue.id
),
inserted AS (
  INSERT INTO brreg_source.translation_queue_entries (
    company_id, status, num_of_characters, batch_id, status_changed_at, created_at, updated_at
  )
  SELECT
    estimated.company_id,
    'pending',
    estimated.num_of_characters,
    NULL,
    now(),
    now(),
    now()
  FROM estimated
  ON CONFLICT DO NOTHING
  RETURNING id
)
SELECT
  (SELECT count(*)::integer FROM estimated) AS companies_seen,
  coalesce((SELECT sum(missing_field_count) FROM estimated), 0)::integer AS fields_seen,
  (SELECT count(*)::integer FROM inserted) AS companies_queued,
  (SELECT count(*)::integer FROM deleted_terminal) AS terminal_rows_deleted;

-- name: ResetStaleBrregTranslationQueueEntries :one
WITH reset_rows AS (
  UPDATE brreg_source.translation_queue_entries
  SET status = 'pending',
      batch_id = NULL,
      status_changed_at = now(),
      updated_at = now()
  WHERE status = 'running'
    AND status_changed_at < now() - (sqlc.arg('stale_running_seconds')::integer * interval '1 second')
  RETURNING id
)
SELECT count(*)::integer AS rows_reset FROM reset_rows;

-- name: CountRunningBrregTranslationQueueEntries :one
SELECT count(*)::integer AS running_count
FROM brreg_source.translation_queue_entries
WHERE status = 'running';

-- name: ClaimBrregTranslationQueueBatch :many
WITH locked AS (
  SELECT id, company_id, num_of_characters, status_changed_at
  FROM brreg_source.translation_queue_entries
  WHERE status = 'pending'
  ORDER BY status_changed_at ASC, company_id ASC
  FOR UPDATE SKIP LOCKED
  LIMIT GREATEST(sqlc.arg('max_candidate_rows')::integer, 1)
),
ranked AS (
  SELECT
    id,
    company_id,
    num_of_characters,
    sum(num_of_characters) OVER (ORDER BY status_changed_at ASC, company_id ASC) AS running_chars,
    row_number() OVER (ORDER BY status_changed_at ASC, company_id ASC) AS row_number
  FROM locked
),
selected AS (
  SELECT id
  FROM ranked
  WHERE running_chars <= GREATEST(sqlc.arg('max_request_chars')::integer, 1)
     OR row_number = 1
),
updated AS (
  UPDATE brreg_source.translation_queue_entries queue
  SET status = 'running',
      batch_id = sqlc.arg('batch_id')::text,
      status_changed_at = now(),
      updated_at = now()
  FROM selected
  WHERE queue.id = selected.id
  RETURNING queue.company_id, queue.num_of_characters
)
SELECT company_id, num_of_characters
FROM updated
ORDER BY company_id ASC;

-- name: ReleaseBrregTranslationQueueBatch :one
WITH released AS (
  UPDATE brreg_source.translation_queue_entries
  SET status = 'pending',
      batch_id = NULL,
      status_changed_at = now(),
      updated_at = now()
  WHERE status = 'running'
    AND batch_id = sqlc.arg('batch_id')::text
  RETURNING id
)
SELECT count(*)::integer AS rows_released FROM released;

-- name: CompleteBrregTranslationQueueBatch :one
WITH completed AS (
  UPDATE brreg_source.translation_queue_entries
  SET status = 'succeeded',
      batch_id = NULL,
      status_changed_at = now(),
      updated_at = now()
  WHERE status = 'running'
    AND batch_id = sqlc.arg('batch_id')::text
  RETURNING id
)
SELECT count(*)::integer AS rows_completed FROM completed;

-- name: FailBrregTranslationQueueBatch :one
WITH failed AS (
  UPDATE brreg_source.translation_queue_entries
  SET status = 'failed',
      batch_id = NULL,
      status_changed_at = now(),
      updated_at = now()
  WHERE status = 'running'
    AND batch_id = sqlc.arg('batch_id')::text
  RETURNING id
)
SELECT count(*)::integer AS rows_failed FROM failed;
