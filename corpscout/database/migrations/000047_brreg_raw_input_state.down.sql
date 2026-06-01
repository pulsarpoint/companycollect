DROP VIEW IF EXISTS v_source_raw_inputs;
DROP VIEW IF EXISTS v_brreg_raw_input_action_attributes;

DROP INDEX IF EXISTS idx_brreg_raw_inputs_state_submitted;
DROP INDEX IF EXISTS idx_brreg_raw_inputs_state_input;
DROP INDEX IF EXISTS idx_brreg_raw_inputs_state;

DROP TABLE IF EXISTS brreg_raw_input_action_events;
DROP TABLE IF EXISTS brreg_raw_input_actions;

ALTER TABLE brreg_company_raw_inputs
  DROP CONSTRAINT IF EXISTS chk_brreg_raw_input_state,
  DROP COLUMN IF EXISTS state;

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
    cvri.translation_fx_rate_date
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
    ari.translation_fx_rate_date
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
