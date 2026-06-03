CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE brreg_source.translation_terms (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source TEXT NOT NULL DEFAULT 'brreg',
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
  CONSTRAINT chk_brreg_source_translation_terms_source CHECK (source = 'brreg'),
  CONSTRAINT chk_brreg_source_translation_terms_status CHECK (
    status IN ('pending', 'queued', 'succeeded', 'failed_retryable', 'failed_terminal')
  ),
  CONSTRAINT chk_brreg_source_translation_terms_attempt_count CHECK (attempt_count >= 0),
  CONSTRAINT chk_brreg_source_translation_terms_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  CONSTRAINT chk_brreg_source_translation_terms_source_text CHECK (btrim(source_text) <> ''),
  CONSTRAINT chk_brreg_source_translation_terms_normalized CHECK (btrim(source_text_normalized) <> ''),
  CONSTRAINT chk_brreg_source_translation_terms_key CHECK (term_key ~ '^[0-9a-f]{64}$')
);

CREATE UNIQUE INDEX uq_brreg_source_translation_terms_key
  ON brreg_source.translation_terms(source, source_lang, target_lang, prompt_version, term_key);

CREATE INDEX idx_brreg_source_translation_terms_status
  ON brreg_source.translation_terms(status, updated_at);

CREATE INDEX idx_brreg_source_translation_terms_lookup
  ON brreg_source.translation_terms(source_lang, target_lang, prompt_version, source_text_normalized);

CREATE OR REPLACE VIEW brreg_source.v_missing_translation_fields AS
SELECT
  'brreg_source.companies'::text AS source_table,
  company.id AS source_row_id,
  company.id AS company_id,
  'organization_form_label'::text AS source_column,
  'organization_form_label_en'::text AS target_column,
  'no'::text AS source_lang,
  'en'::text AS target_lang,
  company.organization_form_label AS source_text,
  lower(btrim(company.organization_form_label)) AS source_text_normalized,
  encode(digest(lower(btrim(company.organization_form_label)), 'sha256'), 'hex') AS term_key
FROM brreg_source.companies company
WHERE company.row_status = 'active'
  AND nullif(btrim(company.organization_form_label), '') IS NOT NULL
  AND nullif(btrim(company.organization_form_label_en), '') IS NULL
UNION ALL
SELECT
  'brreg_source.companies',
  company.id,
  company.id,
  'response_class',
  'response_class_en',
  'no',
  'en',
  company.response_class,
  lower(btrim(company.response_class)),
  encode(digest(lower(btrim(company.response_class)), 'sha256'), 'hex')
FROM brreg_source.companies company
WHERE company.row_status = 'active'
  AND nullif(btrim(company.response_class), '') IS NOT NULL
  AND nullif(btrim(company.response_class_en), '') IS NULL
UNION ALL
SELECT
  'brreg_source.companies',
  company.id,
  company.id,
  'activity_description',
  'activity_description_en',
  'no',
  'en',
  company.activity_description,
  lower(btrim(company.activity_description)),
  encode(digest(lower(btrim(company.activity_description)), 'sha256'), 'hex')
FROM brreg_source.companies company
WHERE company.row_status = 'active'
  AND nullif(btrim(company.activity_description), '') IS NOT NULL
  AND nullif(btrim(company.activity_description_en), '') IS NULL
UNION ALL
SELECT
  'brreg_source.companies',
  company.id,
  company.id,
  'statutory_purpose',
  'statutory_purpose_en',
  'no',
  'en',
  company.statutory_purpose,
  lower(btrim(company.statutory_purpose)),
  encode(digest(lower(btrim(company.statutory_purpose)), 'sha256'), 'hex')
FROM brreg_source.companies company
WHERE company.row_status = 'active'
  AND nullif(btrim(company.statutory_purpose), '') IS NOT NULL
  AND nullif(btrim(company.statutory_purpose_en), '') IS NULL
UNION ALL
SELECT
  'brreg_source.capital',
  capital.id,
  capital.company_id,
  'capital_type',
  'capital_type_en',
  'no',
  'en',
  capital.capital_type,
  lower(btrim(capital.capital_type)),
  encode(digest(lower(btrim(capital.capital_type)), 'sha256'), 'hex')
FROM brreg_source.capital capital
JOIN brreg_source.companies company ON company.id = capital.company_id
WHERE company.row_status = 'active'
  AND nullif(btrim(capital.capital_type), '') IS NOT NULL
  AND nullif(btrim(capital.capital_type_en), '') IS NULL;
