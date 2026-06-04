CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE SCHEMA IF NOT EXISTS cvr_workflow;

CREATE TABLE cvr_workflow.workflow_runs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  orchestrator TEXT NOT NULL DEFAULT 'temporal',
  orchestrator_run_id TEXT NOT NULL UNIQUE,
  run_type TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'running',
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at TIMESTAMPTZ,
  records_seen INTEGER NOT NULL DEFAULT 0,
  records_completed INTEGER NOT NULL DEFAULT 0,
  records_failed INTEGER NOT NULL DEFAULT 0,
  error TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  CONSTRAINT chk_cvr_workflow_runs_type CHECK (run_type IN ('scroll_ingest')),
  CONSTRAINT chk_cvr_workflow_runs_status CHECK (status IN ('running', 'succeeded', 'failed', 'cancelled')),
  CONSTRAINT chk_cvr_workflow_runs_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE TABLE cvr_workflow.scroll_sessions (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  workflow_run_id UUID REFERENCES cvr_workflow.workflow_runs(id) ON DELETE SET NULL,
  source_url TEXT NOT NULL,
  scroll_url TEXT NOT NULL,
  scroll_ttl TEXT NOT NULL,
  page_size INTEGER NOT NULL,
  record_limit INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL DEFAULT 'running',
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at TIMESTAMPTZ,
  records_seen INTEGER NOT NULL DEFAULT 0,
  records_written INTEGER NOT NULL DEFAULT 0,
  last_scroll_id_hash TEXT,
  error TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  CONSTRAINT chk_cvr_workflow_scroll_session_status CHECK (status IN ('running', 'completed', 'failed')),
  CONSTRAINT chk_cvr_workflow_scroll_session_page_size CHECK (page_size > 0),
  CONSTRAINT chk_cvr_workflow_scroll_session_record_limit CHECK (record_limit >= 0),
  CONSTRAINT chk_cvr_workflow_scroll_session_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE TABLE cvr_workflow.raw_records (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  scroll_session_id UUID REFERENCES cvr_workflow.scroll_sessions(id) ON DELETE SET NULL,
  source_native_id TEXT NOT NULL,
  cvr_number TEXT NOT NULL,
  company_name TEXT,
  registration_status TEXT,
  company_type TEXT,
  website TEXT,
  email TEXT,
  phone TEXT,
  marketing_protected BOOLEAN,
  country_iso2 TEXT NOT NULL DEFAULT 'DK',
  source_updated_at TIMESTAMPTZ,
  raw_payload JSONB NOT NULL,
  payload_hash TEXT NOT NULL,
  is_current BOOLEAN NOT NULL DEFAULT true,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  CONSTRAINT chk_cvr_workflow_raw_source_native CHECK (source_native_id = cvr_number),
  CONSTRAINT chk_cvr_workflow_raw_payload_object CHECK (jsonb_typeof(raw_payload) = 'object'),
  CONSTRAINT chk_cvr_workflow_raw_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  UNIQUE (cvr_number, payload_hash)
);

CREATE INDEX idx_cvr_workflow_raw_records_cvr_number
  ON cvr_workflow.raw_records (cvr_number);

CREATE INDEX idx_cvr_workflow_raw_records_hash
  ON cvr_workflow.raw_records (payload_hash);

CREATE UNIQUE INDEX idx_cvr_workflow_raw_records_current_cvr_number
  ON cvr_workflow.raw_records (cvr_number)
  WHERE is_current;

CREATE INDEX idx_cvr_workflow_raw_records_name
  ON cvr_workflow.raw_records (company_name)
  WHERE company_name IS NOT NULL;

CREATE INDEX idx_cvr_workflow_scroll_sessions_workflow_run
  ON cvr_workflow.scroll_sessions (workflow_run_id, started_at DESC);

UPDATE data_sources
SET
  input_table_name = 'cvr_workflow.raw_records',
  requires_translation = false,
  schedule_kind = 'manual',
  schedule_expression = NULL,
  config = jsonb_build_object(
    'api_url', 'https://distribution.virk.dk/cvr-permanent/virksomhed/_search',
    'docs_url', 'https://erhvervsstyrelsen.dk/kom-godt-igang-med-elasticSearch',
    'protocol', 'CVR Elasticsearch scroll',
    'page_size', 100,
    'scroll_keepalive', '1m',
    'fields', jsonb_build_array('cvrNummer', 'virksomhedMetadata', 'reklamebeskyttet', 'hjemmeside', 'elektroniskPost', 'telefonNummer'),
    'username_env', 'CORPSCOUT_CVR_DISTRIBUTION_USERNAME',
    'password_env', 'CORPSCOUT_CVR_DISTRIBUTION_PASSWORD',
    'bearer_env', 'CORPSCOUT_CVR_DISTRIBUTION_BEARER_TOKEN',
    'api_key_env', 'CORPSCOUT_CVR_DISTRIBUTION_API_KEY',
    'notes', 'Official CVR distribution endpoint. Ingest uses Elasticsearch scroll and does not require known CVR numbers.'
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
    'cvr_workflow.raw_records' AS source_input_table,
    cvri.cvr_number AS source_native_id,
    'pending'::text AS processing_status,
    0::integer AS processing_attempts,
    NULL::text AS processing_error,
    cvri.first_seen_at,
    cvri.last_seen_at,
    cvri.payload_hash,
    EXISTS (
      SELECT 1 FROM suggestion_source_links ssl
      WHERE ssl.source_input_table = 'cvr_workflow.raw_records'
        AND ssl.source_input_key = cvri.id::text
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
    'pending'::text AS state,
    NULL::text AS latest_translation_action_status,
    NULL::boolean AS has_successful_translation,
    NULL::text AS latest_enhancement_action_status,
    NULL::boolean AS has_successful_enhancement,
    NULL::text AS latest_submission_action_status,
    NULL::boolean AS has_successful_submission,
    0::bigint AS connected_domain_count
  FROM cvr_workflow.raw_records cvri
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

GRANT USAGE ON SCHEMA cvr_workflow TO corpscout_anon;
GRANT SELECT ON ALL TABLES IN SCHEMA cvr_workflow TO corpscout_anon;
GRANT SELECT ON v_source_raw_inputs TO corpscout_anon;

DROP TABLE IF EXISTS cvr_company_raw_inputs;
