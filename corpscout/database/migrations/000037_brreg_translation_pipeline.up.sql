ALTER TABLE brreg_company_raw_inputs
  ADD COLUMN raw_payload_en JSONB,
  ADD COLUMN translation_status TEXT NOT NULL DEFAULT 'pending',
  ADD COLUMN translation_attempts INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN translation_error TEXT,
  ADD COLUMN translation_model TEXT,
  ADD COLUMN translation_prompt_version TEXT,
  ADD COLUMN translated_at TIMESTAMPTZ,
  ADD COLUMN translation_lease_by TEXT,
  ADD COLUMN translation_lease_until TIMESTAMPTZ,
  ADD COLUMN translation_fx_source TEXT,
  ADD COLUMN translation_fx_rate_date DATE,
  ADD CONSTRAINT chk_brreg_translation_status CHECK (
    translation_status IN ('pending', 'translating', 'translated', 'failed')
  ),
  ADD CONSTRAINT chk_brreg_raw_payload_en_object CHECK (
    raw_payload_en IS NULL OR jsonb_typeof(raw_payload_en) = 'object'
  ),
  ADD CONSTRAINT chk_brreg_translation_attempts CHECK (translation_attempts >= 0);

CREATE INDEX idx_brreg_raw_translation_status
  ON brreg_company_raw_inputs (translation_status, created_at);

CREATE INDEX idx_brreg_raw_translation_lease
  ON brreg_company_raw_inputs (translation_lease_until)
  WHERE translation_status = 'translating';

CREATE TABLE translation_cache (
  category           TEXT NOT NULL,
  original_hash      TEXT NOT NULL,
  source_lang        TEXT NOT NULL DEFAULT 'no',
  target_lang        TEXT NOT NULL DEFAULT 'en',
  prompt_version     TEXT NOT NULL,
  model              TEXT NOT NULL,
  original_text      TEXT NOT NULL,
  translated_text    TEXT NOT NULL,
  created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (category, original_hash, source_lang, target_lang, prompt_version, model)
);

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
    NULL::date AS translation_fx_rate_date
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
    NULL::date AS translation_fx_rate_date
  FROM companies_house_company_raw_inputs chri

  UNION ALL

  SELECT
    bri.id,
    'brreg' AS source_name,
    'brreg_company_raw_inputs' AS source_input_table,
    bri.organization_number AS source_native_id,
    bri.processing_status,
    bri.processing_attempts,
    bri.processing_error,
    bri.first_seen_at,
    bri.last_seen_at,
    bri.payload_hash,
    EXISTS (
      SELECT 1 FROM suggestion_source_links ssl
      WHERE ssl.source_input_table = 'brreg_company_raw_inputs'
        AND ssl.source_input_key = bri.id::text
    ) AS has_suggestion,
    bri.raw_payload_en,
    bri.translation_status,
    bri.translation_attempts,
    bri.translation_error,
    bri.translation_model,
    bri.translation_prompt_version,
    bri.translated_at,
    bri.translation_fx_source,
    bri.translation_fx_rate_date
  FROM brreg_company_raw_inputs bri

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
    NULL::date AS translation_fx_rate_date
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
    NULL::date AS translation_fx_rate_date
  FROM domain_discovery_raw_inputs ddri;

GRANT SELECT ON v_source_raw_inputs TO corpscout_anon;
