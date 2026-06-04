CREATE TABLE IF NOT EXISTS cvr_company_raw_inputs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_pull_run_id UUID REFERENCES source_pull_runs(id),
    source_native_id TEXT NOT NULL,
    cvr_number TEXT NOT NULL,
    company_name TEXT,
    registration_status TEXT,
    company_type TEXT,
    website TEXT,
    email TEXT,
    phone TEXT,
    country_iso2 TEXT DEFAULT 'DK',
    source_updated_at TIMESTAMPTZ,
    raw_payload JSONB NOT NULL,
    raw_payload_en JSONB,
    payload_hash TEXT NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    processing_status TEXT NOT NULL DEFAULT 'pending',
    processing_attempts INTEGER NOT NULL DEFAULT 0,
    processing_error TEXT,
    processing_lease_by TEXT,
    processing_lease_until TIMESTAMPTZ,
    processed_at TIMESTAMPTZ,
    run_id TEXT,
    translation_status TEXT NOT NULL DEFAULT 'pending',
    translation_attempts INTEGER NOT NULL DEFAULT 0,
    translation_error TEXT,
    translation_model TEXT,
    translation_prompt_version TEXT,
    translated_at TIMESTAMPTZ,
    translation_lease_by TEXT,
    translation_lease_until TIMESTAMPTZ,
    translation_fx_source TEXT,
    translation_fx_rate_date DATE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_cvr_raw_source_native CHECK (source_native_id = cvr_number),
    CONSTRAINT chk_cvr_raw_status CHECK (processing_status IN ('pending', 'processing', 'processed', 'failed', 'ignored', 'superseded')),
    CONSTRAINT chk_cvr_translation_status CHECK (translation_status IN ('pending', 'translating', 'translated', 'failed')),
    CONSTRAINT chk_cvr_raw_attempts CHECK (processing_attempts >= 0),
    CONSTRAINT chk_cvr_translation_attempts CHECK (translation_attempts >= 0),
    CONSTRAINT chk_cvr_raw_payload_object CHECK (jsonb_typeof(raw_payload) = 'object'),
    CONSTRAINT chk_cvr_raw_payload_en_object CHECK (raw_payload_en IS NULL OR jsonb_typeof(raw_payload_en) = 'object'),
    CONSTRAINT uq_cvr_company_raw_inputs_payload UNIQUE (cvr_number, payload_hash)
);

CREATE INDEX IF NOT EXISTS idx_cvr_raw_processing
    ON cvr_company_raw_inputs (processing_status, processing_lease_until, created_at);
CREATE INDEX IF NOT EXISTS idx_cvr_raw_run_id
    ON cvr_company_raw_inputs (run_id) WHERE run_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_cvr_raw_translation_status
    ON cvr_company_raw_inputs (translation_status, created_at);
CREATE INDEX IF NOT EXISTS idx_cvr_raw_translation_lease
    ON cvr_company_raw_inputs (translation_lease_until)
    WHERE translation_status = 'translating';
CREATE INDEX IF NOT EXISTS idx_cvr_raw_payload_hash
    ON cvr_company_raw_inputs (payload_hash);
CREATE INDEX IF NOT EXISTS idx_cvr_raw_cvr_number
    ON cvr_company_raw_inputs (cvr_number);

UPDATE data_sources
SET
  input_table_name = 'cvr_company_raw_inputs',
  requires_translation = true,
  schedule_kind = 'manual',
  schedule_expression = NULL,
  config = jsonb_build_object(
    'api_url', 'https://datafordeler.dk/dataoversigt/det-centrale-virksomhedsregister-cvr/cvr-fildownload/',
    'docs_url', 'https://datafordeler.dk/dataoversigt/det-centrale-virksomhedsregister-cvr/cvr-fildownload/',
    'protocol', 'Datafordeler CVR file download',
    'base_url_env', 'CVR_FILEDOWNLOAD_BASE_URL',
    'datasets_env', 'CVR_FILEDOWNLOAD_DATASETS',
    'auth_env', 'CVR_FILEDOWNLOAD_BEARER_TOKEN',
    'api_key_env', 'CVR_FILEDOWNLOAD_API_KEY'
  ),
  updated_at = now()
WHERE name = 'cvr';

CREATE OR REPLACE VIEW v_source_raw_inputs AS
  SELECT
    gri.id,
    'gleif' AS source_name,
    'gleif_company_raw_inputs' AS source_input_table,
    gri.lei AS source_native_id,
    gri.processing_status,
    gri.processing_attempts,
    gri.processing_error,
    gri.first_seen_at,
    gri.last_seen_at,
    gri.payload_hash,
    EXISTS (
      SELECT 1 FROM suggestion_source_links ssl
      WHERE ssl.source_input_table = 'gleif_company_raw_inputs'
        AND ssl.source_input_key = gri.id::text
    ) AS has_suggestion,
    NULL::jsonb AS raw_payload_en,
    NULL::text AS translation_status,
    NULL::integer AS translation_attempts,
    NULL::text AS translation_error,
    NULL::text AS translation_model,
    NULL::text AS translation_prompt_version,
    NULL::timestamptz AS translated_at,
    NULL::text AS translation_fx_source,
    NULL::date AS translation_fx_rate_date,
    NULL::text AS state,
    NULL::text AS latest_translation_action_status,
    NULL::boolean AS has_successful_translation,
    NULL::text AS latest_enhancement_action_status,
    NULL::boolean AS has_successful_enhancement,
    NULL::text AS latest_submission_action_status,
    NULL::boolean AS has_successful_submission,
    0::bigint AS connected_domain_count
  FROM gleif_company_raw_inputs gri
  UNION ALL
  SELECT
    chri.id,
    'companies_house' AS source_name,
    'companies_house_company_raw_inputs' AS source_input_table,
    chri.company_number AS source_native_id,
    chri.processing_status,
    chri.processing_attempts,
    chri.processing_error,
    chri.first_seen_at,
    chri.last_seen_at,
    chri.payload_hash,
    EXISTS (
      SELECT 1 FROM suggestion_source_links ssl
      WHERE ssl.source_input_table = 'companies_house_company_raw_inputs'
        AND ssl.source_input_key = chri.id::text
    ) AS has_suggestion,
    NULL::jsonb AS raw_payload_en,
    NULL::text AS translation_status,
    NULL::integer AS translation_attempts,
    NULL::text AS translation_error,
    NULL::text AS translation_model,
    NULL::text AS translation_prompt_version,
    NULL::timestamptz AS translated_at,
    NULL::text AS translation_fx_source,
    NULL::date AS translation_fx_rate_date,
    NULL::text AS state,
    NULL::text AS latest_translation_action_status,
    NULL::boolean AS has_successful_translation,
    NULL::text AS latest_enhancement_action_status,
    NULL::boolean AS has_successful_enhancement,
    NULL::text AS latest_submission_action_status,
    NULL::boolean AS has_successful_submission,
    0::bigint AS connected_domain_count
  FROM companies_house_company_raw_inputs chri
  UNION ALL
  SELECT
    cvri.id,
    'cvr' AS source_name,
    'cvr_company_raw_inputs' AS source_input_table,
    cvri.cvr_number AS source_native_id,
    cvri.processing_status,
    cvri.processing_attempts,
    cvri.processing_error,
    cvri.first_seen_at,
    cvri.last_seen_at,
    cvri.payload_hash,
    EXISTS (
      SELECT 1 FROM suggestion_source_links ssl
      WHERE ssl.source_input_table = 'cvr_company_raw_inputs'
        AND ssl.source_input_key = cvri.id::text
    ) AS has_suggestion,
    cvri.raw_payload_en,
    cvri.translation_status,
    cvri.translation_attempts,
    cvri.translation_error,
    cvri.translation_model,
    cvri.translation_prompt_version,
    cvri.translated_at,
    cvri.translation_fx_source,
    cvri.translation_fx_rate_date,
    NULL::text AS state,
    NULL::text AS latest_translation_action_status,
    NULL::boolean AS has_successful_translation,
    NULL::text AS latest_enhancement_action_status,
    NULL::boolean AS has_successful_enhancement,
    NULL::text AS latest_submission_action_status,
    NULL::boolean AS has_successful_submission,
    0::bigint AS connected_domain_count
  FROM cvr_company_raw_inputs cvri
  UNION ALL
  SELECT
    ari.id,
    'ariregister' AS source_name,
    'ariregister_company_raw_inputs' AS source_input_table,
    ari.registry_code AS source_native_id,
    ari.processing_status,
    ari.processing_attempts,
    ari.processing_error,
    ari.first_seen_at,
    ari.last_seen_at,
    ari.payload_hash,
    EXISTS (
      SELECT 1 FROM suggestion_source_links ssl
      WHERE ssl.source_input_table = 'ariregister_company_raw_inputs'
        AND ssl.source_input_key = ari.id::text
    ) AS has_suggestion,
    ari.raw_payload_en,
    ari.translation_status,
    ari.translation_attempts,
    ari.translation_error,
    ari.translation_model,
    ari.translation_prompt_version,
    ari.translated_at,
    ari.translation_fx_source,
    ari.translation_fx_rate_date,
    NULL::text AS state,
    NULL::text AS latest_translation_action_status,
    NULL::boolean AS has_successful_translation,
    NULL::text AS latest_enhancement_action_status,
    NULL::boolean AS has_successful_enhancement,
    NULL::text AS latest_submission_action_status,
    NULL::boolean AS has_successful_submission,
    0::bigint AS connected_domain_count
  FROM ariregister_company_raw_inputs ari
  UNION ALL
  SELECT
    acpri.id,
    'ai_company_profile' AS source_name,
    'ai_company_profile_raw_inputs' AS source_input_table,
    COALESCE(acpri.normalized_domain, '') AS source_native_id,
    acpri.processing_status,
    acpri.processing_attempts,
    acpri.processing_error,
    acpri.first_seen_at,
    acpri.last_seen_at,
    acpri.payload_hash,
    EXISTS (
      SELECT 1 FROM suggestion_source_links ssl
      WHERE ssl.source_input_table = 'ai_company_profile_raw_inputs'
        AND ssl.source_input_key = acpri.id::text
    ) AS has_suggestion,
    NULL::jsonb AS raw_payload_en,
    NULL::text AS translation_status,
    NULL::integer AS translation_attempts,
    NULL::text AS translation_error,
    NULL::text AS translation_model,
    NULL::text AS translation_prompt_version,
    NULL::timestamptz AS translated_at,
    NULL::text AS translation_fx_source,
    NULL::date AS translation_fx_rate_date,
    NULL::text AS state,
    NULL::text AS latest_translation_action_status,
    NULL::boolean AS has_successful_translation,
    NULL::text AS latest_enhancement_action_status,
    NULL::boolean AS has_successful_enhancement,
    NULL::text AS latest_submission_action_status,
    NULL::boolean AS has_successful_submission,
    0::bigint AS connected_domain_count
  FROM ai_company_profile_raw_inputs acpri
  UNION ALL
  SELECT
    ddri.id,
    'domain_discovery' AS source_name,
    'domain_discovery_raw_inputs' AS source_input_table,
    ddri.domain AS source_native_id,
    ddri.processing_status,
    ddri.processing_attempts,
    ddri.processing_error,
    ddri.first_seen_at,
    ddri.last_seen_at,
    ddri.payload_hash,
    EXISTS (
      SELECT 1 FROM suggestion_source_links ssl
      WHERE ssl.source_input_table = 'domain_discovery_raw_inputs'
        AND ssl.source_input_key = ddri.id::text
    ) AS has_suggestion,
    NULL::jsonb AS raw_payload_en,
    NULL::text AS translation_status,
    NULL::integer AS translation_attempts,
    NULL::text AS translation_error,
    NULL::text AS translation_model,
    NULL::text AS translation_prompt_version,
    NULL::timestamptz AS translated_at,
    NULL::text AS translation_fx_source,
    NULL::date AS translation_fx_rate_date,
    NULL::text AS state,
    NULL::text AS latest_translation_action_status,
    NULL::boolean AS has_successful_translation,
    NULL::text AS latest_enhancement_action_status,
    NULL::boolean AS has_successful_enhancement,
    NULL::text AS latest_submission_action_status,
    NULL::boolean AS has_successful_submission,
    0::bigint AS connected_domain_count
  FROM domain_discovery_raw_inputs ddri;

GRANT SELECT ON v_source_raw_inputs TO corpscout_anon;

DROP SCHEMA IF EXISTS cvr_workflow CASCADE;
