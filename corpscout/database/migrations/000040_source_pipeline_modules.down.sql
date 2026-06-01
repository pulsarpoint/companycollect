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

UPDATE data_sources
SET pull_task_type = 'source_pull',
    processor_task_type = NULL,
    requires_translation = false,
    config = '{
      "api_url":   "https://api.gleif.org/api/v1/lei-records",
      "docs_url":  "https://www.gleif.org/en/lei-data/gleif-api",
      "protocol":  "REST/JSON",
      "page_size": 200,
      "fields":    ["name", "country", "lei", "status", "address", "hq_address", "aliases"],
      "auth_env":  null,
      "notes":     "Global LEI database. Supports incremental sync via filter[lastUpdateTime]. No auth required."
    }'::jsonb,
    updated_at = now()
WHERE name = 'gleif';

UPDATE data_sources
SET pull_task_type = 'source_pull',
    processor_task_type = NULL,
    requires_translation = false,
    config = '{
      "api_url":   "https://cvrapi.dk/api",
      "docs_url":  "https://cvrapi.dk/documentation",
      "protocol":  "REST/JSON",
      "page_size": 100,
      "fields":    ["name", "country", "registration_number", "status", "website"],
      "auth_env":  "CVR_API_TOKEN",
      "notes":     "Danish Central Business Register. API token required. API does not return a total record count."
    }'::jsonb,
    updated_at = now()
WHERE name = 'cvr';

UPDATE data_sources
SET pull_task_type = 'source_pull',
    processor_task_type = NULL,
    requires_translation = false,
    config = '{
      "api_url":   "https://ariregister.rik.ee/api/1/",
      "docs_url":  "https://ariregister.rik.ee/eng/api",
      "protocol":  "REST/JSON",
      "page_size": 200,
      "fields":    ["name", "country", "registration_number", "status"],
      "auth_env":  null,
      "notes":     "Estonian Business Register (Ariregister). Public open data, no auth required. Uses offset-based pagination."
    }'::jsonb,
    updated_at = now()
WHERE name = 'ariregister';

ALTER TABLE gleif_company_raw_inputs
    DROP COLUMN IF EXISTS legal_jurisdiction,
    DROP COLUMN IF EXISTS legal_form_code,
    DROP COLUMN IF EXISTS legal_form_name,
    DROP COLUMN IF EXISTS registration_authority_id,
    DROP COLUMN IF EXISTS entity_category,
    DROP COLUMN IF EXISTS entity_creation_date;

-- Keep source_pull_run_id nullable so rollback does not break Temporal-written rows.
DROP TABLE IF EXISTS ariregister_company_raw_inputs;
DROP TABLE IF EXISTS cvr_company_raw_inputs;
