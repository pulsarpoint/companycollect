ALTER TABLE data_sources
    ADD COLUMN schedule_enabled BOOLEAN NOT NULL DEFAULT TRUE;

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
    ) AS has_suggestion
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
    ) AS has_suggestion
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
    ) AS has_suggestion
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
    ) AS has_suggestion
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
    ) AS has_suggestion
  FROM domain_discovery_raw_inputs ddri;

GRANT SELECT ON v_source_raw_inputs TO corpscout_anon;
GRANT SELECT ON gleif_company_raw_inputs TO corpscout_anon;
GRANT SELECT ON companies_house_company_raw_inputs TO corpscout_anon;
GRANT SELECT ON brreg_company_raw_inputs TO corpscout_anon;
GRANT SELECT ON ai_company_profile_raw_inputs TO corpscout_anon;
GRANT SELECT ON domain_discovery_raw_inputs TO corpscout_anon;
GRANT SELECT ON suggestion_source_links TO corpscout_anon;
