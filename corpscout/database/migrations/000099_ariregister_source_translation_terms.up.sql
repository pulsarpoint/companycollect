CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE ariregister_source.translation_terms (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source TEXT NOT NULL DEFAULT 'ariregister',
  source_lang TEXT NOT NULL,
  target_lang TEXT NOT NULL,
  source_text_normalized TEXT NOT NULL,
  source_text TEXT NOT NULL,
  term_key TEXT NOT NULL,
  translated_text TEXT,
  status TEXT NOT NULL DEFAULT 'pending',
  attempt_count INTEGER NOT NULL DEFAULT 0,
  provider TEXT,
  model TEXT,
  prompt_version TEXT NOT NULL DEFAULT 'v1',
  error TEXT,
  error_code TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  first_requested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_requested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  translated_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_ariregister_source_translation_terms_source CHECK (source = 'ariregister'),
  CONSTRAINT chk_ariregister_source_translation_terms_status CHECK (
    status IN ('pending', 'queued', 'succeeded', 'failed_retryable', 'failed_terminal')
  ),
  CONSTRAINT chk_ariregister_source_translation_terms_attempt_count CHECK (attempt_count >= 0),
  CONSTRAINT chk_ariregister_source_translation_terms_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  CONSTRAINT chk_ariregister_source_translation_terms_source_text CHECK (btrim(source_text) <> ''),
  CONSTRAINT chk_ariregister_source_translation_terms_normalized CHECK (btrim(source_text_normalized) <> ''),
  CONSTRAINT chk_ariregister_source_translation_terms_key CHECK (term_key ~ '^[0-9a-f]{64}$')
);

CREATE UNIQUE INDEX uq_ariregister_source_translation_terms_key
  ON ariregister_source.translation_terms(source, source_lang, target_lang, prompt_version, term_key);

CREATE INDEX idx_ariregister_source_translation_terms_status
  ON ariregister_source.translation_terms(status, updated_at);

CREATE INDEX idx_ariregister_source_translation_terms_lookup
  ON ariregister_source.translation_terms(source_lang, target_lang, prompt_version, source_text_normalized);

CREATE MATERIALIZED VIEW ariregister_source.mv_company_translation_status AS
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
  FROM ariregister_source.v_missing_translations missing
  JOIN ariregister_source.companies company
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
  FROM ariregister_source.translation_terms term
  WHERE term.source = 'ariregister'
    AND term.source_lang = 'et'
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
  company.registry_code,
  company.legal_name,
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
FROM ariregister_source.companies company
LEFT JOIN missing_counts ON missing_counts.company_id = company.id
WHERE company.row_status = 'active';

CREATE UNIQUE INDEX uq_ariregister_source_mv_company_translation_status_company
  ON ariregister_source.mv_company_translation_status(company_id);

CREATE INDEX idx_ariregister_source_mv_company_translation_status_missing
  ON ariregister_source.mv_company_translation_status(min_missing_priority, updated_at DESC, registry_code ASC)
  WHERE translation_missing_count > 0;

CREATE INDEX idx_ariregister_source_mv_company_translation_status_complete
  ON ariregister_source.mv_company_translation_status(updated_at DESC, registry_code ASC)
  WHERE translation_missing_count = 0;

CREATE INDEX idx_ariregister_source_mv_company_translation_status_legal_name
  ON ariregister_source.mv_company_translation_status(legal_name, registry_code);

GRANT SELECT ON ariregister_source.mv_company_translation_status TO corpscout_anon;
