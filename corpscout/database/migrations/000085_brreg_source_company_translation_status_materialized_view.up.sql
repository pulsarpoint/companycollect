DROP MATERIALIZED VIEW IF EXISTS brreg_source.mv_company_translation_status;

CREATE MATERIALIZED VIEW brreg_source.mv_company_translation_status AS
WITH missing_fields AS (
  SELECT
    missing.company_id,
    missing.source_table,
    missing.source_row_id,
    missing.source_column,
    missing.target_column,
    btrim(missing.source_text) AS source_text,
    lower(btrim(missing.source_text)) AS source_text_normalized,
    encode(digest(lower(btrim(missing.source_text)), 'sha256'), 'hex') AS term_key,
    missing.priority
  FROM brreg_source.v_missing_translations missing
  JOIN brreg_source.companies company
    ON company.id = missing.company_id
   AND company.row_status = 'active'
  WHERE nullif(btrim(missing.source_text), '') IS NOT NULL
),
latest_terms AS (
  SELECT DISTINCT ON (term.source, term.source_lang, term.target_lang, term.prompt_version, term.term_key)
    term.source,
    term.source_lang,
    term.target_lang,
    term.prompt_version,
    term.term_key,
    term.status,
    term.translated_text
  FROM brreg_source.translation_terms term
  WHERE term.source = 'brreg'
    AND term.source_lang = 'no'
    AND term.target_lang = 'en'
    AND term.prompt_version = 'v1'
  ORDER BY
    term.source,
    term.source_lang,
    term.target_lang,
    term.prompt_version,
    term.term_key,
    term.updated_at DESC,
    term.created_at DESC,
    term.id DESC
),
missing_counts AS (
  SELECT
    missing.company_id,
    count(*)::bigint AS translation_missing_count,
    count(DISTINCT missing.term_key)::bigint AS translation_missing_term_count,
    count(*) FILTER (
      WHERE term.status = 'succeeded'
        AND nullif(btrim(term.translated_text), '') IS NOT NULL
    )::bigint AS translation_cached_count,
    count(DISTINCT missing.term_key) FILTER (
      WHERE term.status IN ('pending', 'queued')
    )::bigint AS translation_pending_count,
    count(DISTINCT missing.term_key) FILTER (
      WHERE term.status IN ('failed_retryable', 'failed_terminal')
    )::bigint AS translation_failed_count,
    min(missing.priority)::integer AS min_missing_priority,
    coalesce(sum(length(missing.source_text)), 0)::bigint AS estimated_missing_chars
  FROM missing_fields missing
  LEFT JOIN latest_terms term
    ON term.term_key = missing.term_key
  GROUP BY missing.company_id
)
SELECT
  company.id AS company_id,
  company.organization_number,
  company.organization_name,
  company.lifecycle_status,
  company.registration_status,
  coalesce(missing_counts.translation_missing_count, 0)::bigint AS translation_missing_count,
  coalesce(missing_counts.translation_missing_term_count, 0)::bigint AS translation_missing_term_count,
  coalesce(missing_counts.translation_cached_count, 0)::bigint AS translation_cached_count,
  coalesce(missing_counts.translation_pending_count, 0)::bigint AS translation_pending_count,
  0::bigint AS translation_running_count,
  coalesce(missing_counts.translation_cached_count, 0)::bigint AS translation_succeeded_count,
  coalesce(missing_counts.translation_failed_count, 0)::bigint AS translation_failed_count,
  coalesce(missing_counts.min_missing_priority, 2147483647)::integer AS min_missing_priority,
  coalesce(missing_counts.estimated_missing_chars, 0)::bigint AS estimated_missing_chars,
  company.updated_at
FROM brreg_source.companies company
LEFT JOIN missing_counts ON missing_counts.company_id = company.id
WHERE company.row_status = 'active';

CREATE UNIQUE INDEX uq_brreg_source_mv_company_translation_status_company
  ON brreg_source.mv_company_translation_status(company_id);

CREATE INDEX idx_brreg_source_mv_company_translation_status_missing
  ON brreg_source.mv_company_translation_status(min_missing_priority, updated_at DESC, organization_number ASC)
  WHERE translation_missing_count > 0;

CREATE INDEX idx_brreg_source_mv_company_translation_status_complete
  ON brreg_source.mv_company_translation_status(updated_at DESC, organization_number ASC)
  WHERE translation_missing_count = 0;

CREATE INDEX idx_brreg_source_mv_company_translation_status_org_name
  ON brreg_source.mv_company_translation_status(organization_name, organization_number);

GRANT SELECT ON brreg_source.mv_company_translation_status TO corpscout_anon;
