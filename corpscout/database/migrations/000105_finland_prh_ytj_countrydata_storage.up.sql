CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE SCHEMA IF NOT EXISTS countrydata_finland_prh_ytj;

CREATE TABLE countrydata_finland_prh_ytj.sources (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source_slug TEXT NOT NULL UNIQUE,
  source_name TEXT NOT NULL,
  source_type TEXT NOT NULL DEFAULT 'api_bulk_snapshot',
  base_url TEXT NOT NULL,
  country_iso2 TEXT NOT NULL DEFAULT 'FI',
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
  CONSTRAINT chk_countrydata_finland_prh_ytj_sources_type CHECK (
    source_type IN ('api_bulk_snapshot', 'bulk_file', 'api_delta')
  ),
  CONSTRAINT chk_countrydata_finland_prh_ytj_sources_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE TABLE countrydata_finland_prh_ytj.download_runs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source_id UUID NOT NULL REFERENCES countrydata_finland_prh_ytj.sources(id) ON DELETE CASCADE,
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
  CONSTRAINT chk_countrydata_finland_prh_ytj_download_runs_status CHECK (
    status IN ('running', 'succeeded', 'failed')
  ),
  CONSTRAINT chk_countrydata_finland_prh_ytj_download_runs_counts CHECK (
    bytes_downloaded >= 0
    AND records_seen >= 0
    AND records_processed >= 0
    AND records_stored >= 0
    AND decode_errors >= 0
    AND chunks_processed >= 0
    AND pages_downloaded >= 0
  ),
  CONSTRAINT chk_countrydata_finland_prh_ytj_download_runs_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE UNIQUE INDEX idx_countrydata_finland_prh_ytj_download_runs_snapshot_success
  ON countrydata_finland_prh_ytj.download_runs (source_id, snapshot_sha256)
  WHERE status = 'succeeded' AND snapshot_sha256 IS NOT NULL;

CREATE INDEX idx_countrydata_finland_prh_ytj_download_runs_started
  ON countrydata_finland_prh_ytj.download_runs (source_id, started_at DESC);

CREATE TABLE countrydata_finland_prh_ytj.raw_records (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source_id UUID NOT NULL REFERENCES countrydata_finland_prh_ytj.sources(id) ON DELETE CASCADE,
  download_run_id UUID REFERENCES countrydata_finland_prh_ytj.download_runs(id) ON DELETE SET NULL,
  source_native_id TEXT NOT NULL,
  business_id TEXT NOT NULL,
  vat_id TEXT,
  euid TEXT,
  legal_name TEXT,
  trade_register_status TEXT,
  status TEXT,
  is_active BOOLEAN,
  legal_form TEXT,
  legal_form_code TEXT,
  main_business_line TEXT,
  main_business_line_code TEXT,
  website TEXT,
  country_iso2 TEXT NOT NULL DEFAULT 'FI',
  registration_date DATE,
  end_date DATE,
  source_updated_at TIMESTAMPTZ,
  raw_payload JSONB NOT NULL,
  payload_hash TEXT NOT NULL,
  is_current BOOLEAN NOT NULL DEFAULT true,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  CONSTRAINT chk_countrydata_finland_prh_ytj_raw_source_native CHECK (source_native_id = business_id),
  CONSTRAINT chk_countrydata_finland_prh_ytj_raw_payload_object CHECK (jsonb_typeof(raw_payload) = 'object'),
  CONSTRAINT chk_countrydata_finland_prh_ytj_raw_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  UNIQUE (business_id, payload_hash)
);

CREATE INDEX idx_countrydata_finland_prh_ytj_raw_records_business_id
  ON countrydata_finland_prh_ytj.raw_records (business_id);

CREATE INDEX idx_countrydata_finland_prh_ytj_raw_records_hash
  ON countrydata_finland_prh_ytj.raw_records (payload_hash);

CREATE UNIQUE INDEX idx_countrydata_finland_prh_ytj_raw_records_current_business_id
  ON countrydata_finland_prh_ytj.raw_records (business_id)
  WHERE is_current;

CREATE INDEX idx_countrydata_finland_prh_ytj_raw_records_name
  ON countrydata_finland_prh_ytj.raw_records (legal_name)
  WHERE legal_name IS NOT NULL;

INSERT INTO countrydata_finland_prh_ytj.sources (
  source_slug,
  source_name,
  source_type,
  base_url,
  country_iso2,
  supports_incremental,
  metadata
)
VALUES (
  'finland_prh_ytj_v3',
  'PRH Open Data YTJ API v3 companies',
  'api_bulk_snapshot',
  'https://avoindata.prh.fi/opendata-ytj-api/v3/companies',
  'FI',
  false,
  jsonb_build_object(
    'docs_url', 'https://www.prh.fi/en/kaupparekisteri/tietopalvelut/open_data.html',
    'license', 'CC-BY-4.0',
    'attribution', 'Finnish Patent and Registration Office (PRH) / Business Information System (YTJ)',
    'api_parameters', jsonb_build_object(
      'page', '1-based page number',
      'totalResults', 'true on first page'
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
  'finland_prh_ytj_v3',
  'Finland PRH YTJ v3',
  'Finnish company registry data from PRH Open Data YTJ API v3',
  'registry',
  'countrydata_finland_prh_ytj.raw_records',
  (SELECT id FROM countries WHERE iso_alpha2 = 'FI'),
  false,
  false,
  'manual',
  NULL,
  false,
  '{company_name,org_number,vat_id,legal_form,status,locations,industries,website}'::text[],
  jsonb_build_object(
    'api_url', 'https://avoindata.prh.fi/opendata-ytj-api/v3/companies',
    'docs_url', 'https://www.prh.fi/en/kaupparekisteri/tietopalvelut/open_data.html',
    'protocol', 'PRH YTJ API v3 paginated company snapshot',
    'source_schema', 'countrydata_finland_prh_ytj',
    'source_table', 'countrydata_finland_prh_ytj.raw_records',
    'supports_incremental', false,
    'auth_env', NULL,
    'fields', jsonb_build_array(
      'businessId',
      'euId',
      'names',
      'mainBusinessLine',
      'website',
      'companyForms',
      'companySituations',
      'registeredEntries',
      'addresses',
      'tradeRegisterStatus',
      'status',
      'registrationDate',
      'endDate',
      'lastModified'
    ),
    'notes', 'Initial ingest downloads PRH YTJ API pages into an NDJSON snapshot, records full snapshot hash metadata, and stores current raw company rows by Finnish business ID.'
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

GRANT USAGE ON SCHEMA countrydata_finland_prh_ytj TO corpscout_anon;
GRANT SELECT ON ALL TABLES IN SCHEMA countrydata_finland_prh_ytj TO corpscout_anon;
