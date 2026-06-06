CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE SCHEMA IF NOT EXISTS countrydata_united_states_sam_gov_entity;

CREATE TABLE countrydata_united_states_sam_gov_entity.sources (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source_slug TEXT NOT NULL UNIQUE,
  source_name TEXT NOT NULL,
  source_type TEXT NOT NULL DEFAULT 'api_bulk_snapshot',
  base_url TEXT NOT NULL,
  country_iso2 TEXT NOT NULL DEFAULT 'US',
  supports_incremental BOOLEAN NOT NULL DEFAULT false,
  enabled BOOLEAN NOT NULL DEFAULT true,
  last_started_at TIMESTAMPTZ,
  last_success_at TIMESTAMPTZ,
  last_failed_at TIMESTAMPTZ,
  last_snapshot_path TEXT,
  last_snapshot_sha256 TEXT,
  last_error TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_countrydata_us_sam_gov_entity_sources_type CHECK (
    source_type IN ('api_bulk_snapshot', 'bulk_file', 'api_delta')
  ),
  CONSTRAINT chk_countrydata_us_sam_gov_entity_sources_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE TABLE countrydata_united_states_sam_gov_entity.download_runs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source_id UUID NOT NULL REFERENCES countrydata_united_states_sam_gov_entity.sources(id) ON DELETE CASCADE,
  status TEXT NOT NULL DEFAULT 'succeeded',
  base_url TEXT NOT NULL,
  snapshot_path TEXT,
  snapshot_sha256 TEXT,
  started_at TIMESTAMPTZ NOT NULL,
  finished_at TIMESTAMPTZ,
  duration_ms BIGINT,
  bytes_downloaded BIGINT NOT NULL DEFAULT 0,
  records_seen BIGINT NOT NULL DEFAULT 0,
  records_processed BIGINT NOT NULL DEFAULT 0,
  records_stored BIGINT NOT NULL DEFAULT 0,
  decode_errors BIGINT NOT NULL DEFAULT 0,
  chunks_processed BIGINT NOT NULL DEFAULT 0,
  pages_downloaded INTEGER NOT NULL DEFAULT 0,
  first_page INTEGER,
  last_page INTEGER,
  total_results_reported BIGINT,
  error TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_countrydata_us_sam_gov_entity_download_runs_status CHECK (
    status IN ('running', 'succeeded', 'failed')
  ),
  CONSTRAINT chk_countrydata_us_sam_gov_entity_download_runs_counts CHECK (
    bytes_downloaded >= 0
    AND records_seen >= 0
    AND records_processed >= 0
    AND records_stored >= 0
    AND decode_errors >= 0
    AND chunks_processed >= 0
    AND pages_downloaded >= 0
  ),
  CONSTRAINT chk_countrydata_us_sam_gov_entity_download_runs_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE UNIQUE INDEX idx_countrydata_us_sam_gov_entity_download_runs_snapshot_success
  ON countrydata_united_states_sam_gov_entity.download_runs (source_id, snapshot_sha256)
  WHERE status = 'succeeded' AND snapshot_sha256 IS NOT NULL;

CREATE INDEX idx_countrydata_us_sam_gov_entity_download_runs_started
  ON countrydata_united_states_sam_gov_entity.download_runs (source_id, started_at DESC);

CREATE TABLE countrydata_united_states_sam_gov_entity.raw_records (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source_id UUID NOT NULL REFERENCES countrydata_united_states_sam_gov_entity.sources(id) ON DELETE CASCADE,
  download_run_id UUID REFERENCES countrydata_united_states_sam_gov_entity.download_runs(id) ON DELETE SET NULL,
  source_native_id TEXT NOT NULL,
  uei_sam TEXT NOT NULL,
  cage_code TEXT,
  primary_id TEXT,
  legal_name TEXT,
  alternate_name TEXT,
  sam_registration_status TEXT,
  is_sam_active BOOLEAN,
  registration_date DATE,
  registration_expiration_date DATE,
  entity_structure TEXT,
  profit_structure TEXT,
  state_of_incorporation TEXT,
  country_of_incorporation TEXT,
  website TEXT,
  primary_naics TEXT,
  city TEXT,
  state_code TEXT,
  country_iso2 TEXT NOT NULL DEFAULT 'US',
  source_updated_at TIMESTAMPTZ,
  raw_payload JSONB NOT NULL,
  payload_hash TEXT NOT NULL,
  is_current BOOLEAN NOT NULL DEFAULT true,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  CONSTRAINT chk_countrydata_us_sam_gov_entity_raw_source_native CHECK (source_native_id = uei_sam),
  CONSTRAINT chk_countrydata_us_sam_gov_entity_raw_payload_object CHECK (jsonb_typeof(raw_payload) = 'object'),
  CONSTRAINT chk_countrydata_us_sam_gov_entity_raw_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  UNIQUE (uei_sam, payload_hash)
);

CREATE INDEX idx_countrydata_us_sam_gov_entity_raw_records_uei_sam
  ON countrydata_united_states_sam_gov_entity.raw_records (uei_sam);

CREATE INDEX idx_countrydata_us_sam_gov_entity_raw_records_hash
  ON countrydata_united_states_sam_gov_entity.raw_records (payload_hash);

CREATE UNIQUE INDEX idx_countrydata_us_sam_gov_entity_raw_records_current_uei_sam
  ON countrydata_united_states_sam_gov_entity.raw_records (uei_sam)
  WHERE is_current;

CREATE INDEX idx_countrydata_us_sam_gov_entity_raw_records_name
  ON countrydata_united_states_sam_gov_entity.raw_records (legal_name)
  WHERE legal_name IS NOT NULL;

INSERT INTO countrydata_united_states_sam_gov_entity.sources (
  source_slug,
  source_name,
  source_type,
  base_url,
  country_iso2,
  supports_incremental,
  metadata
)
VALUES (
  'united_states_sam_gov_entity',
  'SAM.gov Entity Management (System for Award Management)',
  'api_bulk_snapshot',
  'https://api.sam.gov/entity-information/v3/entities',
  'US',
  false,
  jsonb_build_object(
    'docs_url', 'https://open.gsa.gov/api/entity-api/',
    'license', 'Public extract released under FOIA (Public sensitivity level only)',
    'organization', 'U.S. General Services Administration (GSA)',
    'sensitivity', 'Public extract only; never request FOUO/Sensitive levels',
    'api_parameters', jsonb_build_object(
      'page', '0-based page number',
      'size', 'page size (capped low by the API)',
      'samRegistered', 'Yes to scope to active SAM registrants'
    )
  )
)
ON CONFLICT (source_slug) DO UPDATE SET
  source_name = EXCLUDED.source_name,
  source_type = EXCLUDED.source_type,
  base_url = EXCLUDED.base_url,
  country_iso2 = EXCLUDED.country_iso2,
  supports_incremental = EXCLUDED.supports_incremental,
  metadata = EXCLUDED.metadata,
  updated_at = now();

INSERT INTO data_sources (
  name,
  display_name,
  description,
  source_group,
  input_table_name,
  country_id,
  enabled,
  schedule_enabled,
  schedule_kind,
  schedule_expression,
  requires_translation,
  capabilities,
  config
)
VALUES (
  'united_states_sam_gov_entity',
  'United States SAM.gov Entity',
  'U.S. federal award registrant data from the SAM.gov Entity Management API (FOIA Public extract)',
  'registry',
  'countrydata_united_states_sam_gov_entity.raw_records',
  (SELECT id FROM countries WHERE iso_alpha2 = 'US'),
  false,
  false,
  'manual',
  NULL,
  false,
  '{company_name,status,locations,industries,website,identifiers}'::text[],
  jsonb_build_object(
    'api_url', 'https://api.sam.gov/entity-information/v3/entities',
    'docs_url', 'https://open.gsa.gov/api/entity-api/',
    'protocol', 'SAM.gov Entity Management v3 page-number paginated JSON snapshot (X-Api-Key auth)',
    'source_schema', 'countrydata_united_states_sam_gov_entity',
    'source_table', 'countrydata_united_states_sam_gov_entity.raw_records',
    'supports_incremental', false,
    'auth_env', 'SAM_GOV_ENTITY_API_KEY',
    'sensitivity', 'Public extract only (FOIA); EIN/TIN typically redacted',
    'fields', jsonb_build_array(
      'entityRegistration.ueiSAM',
      'entityRegistration.cageCode',
      'entityRegistration.legalBusinessName',
      'entityRegistration.dbaName',
      'entityRegistration.registrationStatus',
      'entityRegistration.registrationDate',
      'entityRegistration.lastUpdateDate',
      'coreData.generalInformation',
      'coreData.physicalAddress',
      'coreData.entityInformation.entityURL',
      'assertions.goodsAndServices.naicsList'
    ),
    'notes', 'Ingest pages the SAM.gov Entity Management API into an NDJSON snapshot using a Read-Public API key, records snapshot hash metadata, and stores current raw entity rows by ueiSAM. registrationStatus reflects federal-award eligibility, not state corporate standing.'
  )
)
ON CONFLICT (name) DO UPDATE SET
  display_name = EXCLUDED.display_name,
  description = EXCLUDED.description,
  source_group = EXCLUDED.source_group,
  input_table_name = EXCLUDED.input_table_name,
  country_id = EXCLUDED.country_id,
  enabled = EXCLUDED.enabled,
  schedule_enabled = EXCLUDED.schedule_enabled,
  schedule_kind = EXCLUDED.schedule_kind,
  schedule_expression = EXCLUDED.schedule_expression,
  requires_translation = EXCLUDED.requires_translation,
  capabilities = EXCLUDED.capabilities,
  config = EXCLUDED.config,
  updated_at = now();

GRANT USAGE ON SCHEMA countrydata_united_states_sam_gov_entity TO corpscout_anon;
GRANT SELECT ON ALL TABLES IN SCHEMA countrydata_united_states_sam_gov_entity TO corpscout_anon;
