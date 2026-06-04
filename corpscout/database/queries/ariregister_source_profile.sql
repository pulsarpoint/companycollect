-- name: GetAriregisterSourceTranslationAssetState :one
SELECT
  'mv_company_translation_status'::text AS asset,
  count(*)::bigint AS raw_records_current,
  COALESCE(sum(translation_missing_count), 0)::bigint AS task_no_state,
  0::bigint AS task_pending,
  0::bigint AS task_running_active,
  0::bigint AS task_running_stale,
  0::bigint AS task_failed_retryable,
  0::bigint AS task_failed_terminal,
  0::bigint AS task_succeeded,
  0::bigint AS task_skipped,
  COALESCE(sum(translation_missing_count), 0)::bigint AS task_eligible_now,
  0::bigint AS artifact_succeeded,
  0::bigint AS artifact_skipped,
  0::bigint AS artifact_failed,
  COALESCE(sum(translation_missing_count), 0)::bigint AS artifact_missing
FROM ariregister_source.mv_company_explorer;

-- name: GetAriregisterSourceResultTableCounts :one
SELECT
  (SELECT count(*)::bigint FROM ariregister_source.companies WHERE row_status = 'active') AS companies,
  (SELECT count(*)::bigint FROM ariregister_source.addresses) AS addresses,
  (SELECT count(*)::bigint FROM ariregister_source.industries) AS industries,
  (SELECT count(*)::bigint FROM ariregister_source.contacts) AS contacts,
  (SELECT count(*)::bigint FROM ariregister_source.annual_reports) AS annual_reports;

-- name: CountAriregisterSourceEntries :one
SELECT count(*)::bigint
FROM ariregister_source.mv_company_explorer entry
WHERE (
    sqlc.narg('query')::text IS NULL
    OR entry.legal_name ILIKE '%' || sqlc.narg('query')::text || '%'
    OR entry.registry_code ILIKE '%' || sqlc.narg('query')::text || '%'
    OR entry.primary_industry_label ILIKE '%' || sqlc.narg('query')::text || '%'
    OR entry.city_or_area ILIKE '%' || sqlc.narg('query')::text || '%'
  )
  AND (sqlc.narg('lifecycle_status')::text IS NULL OR entry.lifecycle_status = sqlc.narg('lifecycle_status')::text)
  AND (sqlc.narg('registration_status')::text IS NULL OR entry.registration_status = sqlc.narg('registration_status')::text)
  AND (
    sqlc.narg('translation_status')::text IS NULL
    OR (sqlc.narg('translation_status')::text = 'missing' AND entry.translation_missing_count > 0)
    OR (sqlc.narg('translation_status')::text = 'complete' AND entry.translation_missing_count = 0)
  );

-- name: ListAriregisterSourceEntries :many
SELECT
  entry.company_id,
  entry.registry_code,
  entry.legal_name,
  entry.legal_form_label,
  entry.lifecycle_status,
  entry.registration_status,
  entry.registration_status_label,
  entry.primary_industry_code,
  entry.primary_industry_label,
  entry.primary_nace_code,
  entry.primary_nace_title,
  entry.city_or_area,
  entry.postal_code,
  entry.normalized_full_address,
  entry.employee_count,
  entry.latest_financial_year,
  entry.website_count,
  entry.domain_count,
  entry.contact_count,
  entry.translation_missing_count,
  entry.updated_at
FROM ariregister_source.mv_company_explorer entry
WHERE (
    sqlc.narg('query')::text IS NULL
    OR entry.legal_name ILIKE '%' || sqlc.narg('query')::text || '%'
    OR entry.registry_code ILIKE '%' || sqlc.narg('query')::text || '%'
    OR entry.primary_industry_label ILIKE '%' || sqlc.narg('query')::text || '%'
    OR entry.city_or_area ILIKE '%' || sqlc.narg('query')::text || '%'
  )
  AND (sqlc.narg('lifecycle_status')::text IS NULL OR entry.lifecycle_status = sqlc.narg('lifecycle_status')::text)
  AND (sqlc.narg('registration_status')::text IS NULL OR entry.registration_status = sqlc.narg('registration_status')::text)
  AND (
    sqlc.narg('translation_status')::text IS NULL
    OR (sqlc.narg('translation_status')::text = 'missing' AND entry.translation_missing_count > 0)
    OR (sqlc.narg('translation_status')::text = 'complete' AND entry.translation_missing_count = 0)
  )
ORDER BY
  CASE WHEN sqlc.arg('sort_by')::text = 'organization' AND sqlc.arg('sort_dir')::text = 'asc' THEN entry.legal_name END ASC NULLS LAST,
  CASE WHEN sqlc.arg('sort_by')::text = 'organization' AND sqlc.arg('sort_dir')::text = 'desc' THEN entry.legal_name END DESC NULLS LAST,
  CASE WHEN sqlc.arg('sort_by')::text = 'updated_at' AND sqlc.arg('sort_dir')::text = 'asc' THEN entry.updated_at END ASC NULLS LAST,
  CASE WHEN sqlc.arg('sort_by')::text = 'updated_at' AND sqlc.arg('sort_dir')::text = 'desc' THEN entry.updated_at END DESC NULLS LAST,
  entry.updated_at DESC,
  entry.registry_code ASC
LIMIT GREATEST(sqlc.arg('limit')::integer, 1)
OFFSET GREATEST(sqlc.arg('offset')::integer, 0);

-- name: GetAriregisterSourceCompanyDetail :one
SELECT detail.*
FROM ariregister_source.v_company_detail detail
WHERE detail.id = sqlc.arg('company_id')::uuid;

-- name: GetAriregisterSourceCompanyExplorerRefreshSummary :one
SELECT
  count(*)::bigint AS source_entries,
  max(updated_at)::text AS latest_source_updated_at
FROM ariregister_source.mv_company_explorer;

-- name: RefreshAriregisterSourceCompanyExplorer :exec
REFRESH MATERIALIZED VIEW CONCURRENTLY ariregister_source.mv_company_explorer;
