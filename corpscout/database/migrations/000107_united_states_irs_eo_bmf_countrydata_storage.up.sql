CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE SCHEMA IF NOT EXISTS countrydata_united_states_irs_eo_bmf;

CREATE TABLE countrydata_united_states_irs_eo_bmf.sources (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source_slug TEXT NOT NULL UNIQUE,
  source_name TEXT NOT NULL,
  source_type TEXT NOT NULL DEFAULT 'bulk_file',
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
  CONSTRAINT chk_countrydata_us_irs_eo_bmf_sources_type CHECK (
    source_type IN ('api_bulk_snapshot', 'bulk_file', 'api_delta')
  ),
  CONSTRAINT chk_countrydata_us_irs_eo_bmf_sources_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE TABLE countrydata_united_states_irs_eo_bmf.download_runs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source_id UUID NOT NULL REFERENCES countrydata_united_states_irs_eo_bmf.sources(id) ON DELETE CASCADE,
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
  files_downloaded INTEGER NOT NULL DEFAULT 0,
  total_results_reported BIGINT,
  error TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_countrydata_us_irs_eo_bmf_download_runs_status CHECK (
    status IN ('running', 'succeeded', 'failed')
  ),
  CONSTRAINT chk_countrydata_us_irs_eo_bmf_download_runs_counts CHECK (
    bytes_downloaded >= 0
    AND records_seen >= 0
    AND records_processed >= 0
    AND records_stored >= 0
    AND decode_errors >= 0
    AND chunks_processed >= 0
    AND files_downloaded >= 0
  ),
  CONSTRAINT chk_countrydata_us_irs_eo_bmf_download_runs_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE UNIQUE INDEX idx_countrydata_us_irs_eo_bmf_download_runs_snapshot_success
  ON countrydata_united_states_irs_eo_bmf.download_runs (source_id, snapshot_sha256)
  WHERE status = 'succeeded' AND snapshot_sha256 IS NOT NULL;

CREATE INDEX idx_countrydata_us_irs_eo_bmf_download_runs_started
  ON countrydata_united_states_irs_eo_bmf.download_runs (source_id, started_at DESC);

CREATE TABLE countrydata_united_states_irs_eo_bmf.raw_records (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source_id UUID NOT NULL REFERENCES countrydata_united_states_irs_eo_bmf.sources(id) ON DELETE CASCADE,
  download_run_id UUID REFERENCES countrydata_united_states_irs_eo_bmf.download_runs(id) ON DELETE SET NULL,
  source_native_id TEXT NOT NULL,
  ein TEXT NOT NULL,
  primary_id TEXT,
  legal_name TEXT,
  sort_name TEXT,
  exempt_status_code TEXT,
  is_exempt_status_active BOOLEAN,
  subsection TEXT,
  organization_code TEXT,
  foundation_code TEXT,
  ntee_code TEXT,
  irs_ruling_date TEXT,
  tax_period TEXT,
  asset_amount BIGINT,
  income_amount BIGINT,
  revenue_amount BIGINT,
  city TEXT,
  state_code TEXT,
  country_iso2 TEXT NOT NULL DEFAULT 'US',
  raw_payload JSONB NOT NULL,
  payload_hash TEXT NOT NULL,
  is_current BOOLEAN NOT NULL DEFAULT true,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  CONSTRAINT chk_countrydata_us_irs_eo_bmf_raw_source_native CHECK (source_native_id = ein),
  CONSTRAINT chk_countrydata_us_irs_eo_bmf_raw_payload_object CHECK (jsonb_typeof(raw_payload) = 'object'),
  CONSTRAINT chk_countrydata_us_irs_eo_bmf_raw_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  UNIQUE (ein, payload_hash)
);

CREATE INDEX idx_countrydata_us_irs_eo_bmf_raw_records_ein
  ON countrydata_united_states_irs_eo_bmf.raw_records (ein);

CREATE INDEX idx_countrydata_us_irs_eo_bmf_raw_records_hash
  ON countrydata_united_states_irs_eo_bmf.raw_records (payload_hash);

CREATE UNIQUE INDEX idx_countrydata_us_irs_eo_bmf_raw_records_current_ein
  ON countrydata_united_states_irs_eo_bmf.raw_records (ein)
  WHERE is_current;

CREATE INDEX idx_countrydata_us_irs_eo_bmf_raw_records_name
  ON countrydata_united_states_irs_eo_bmf.raw_records (legal_name)
  WHERE legal_name IS NOT NULL;

INSERT INTO countrydata_united_states_irs_eo_bmf.sources (
  source_slug,
  source_name,
  source_type,
  base_url,
  country_iso2,
  supports_incremental,
  metadata
)
VALUES (
  'united_states_irs_eo_bmf',
  'IRS Exempt Organizations Business Master File (EO BMF)',
  'bulk_file',
  'https://www.irs.gov/pub/irs-soi/',
  'US',
  false,
  jsonb_build_object(
    'docs_url', 'https://www.irs.gov/charities-non-profits/exempt-organizations-business-master-file-extract-eo-bmf',
    'license', 'U.S. Government work / public domain',
    'code_list_reference', 'https://www.irs.gov/pub/irs-pdf/p5926.pdf',
    'files', jsonb_build_array('eo1.csv', 'eo2.csv', 'eo3.csv', 'eo4.csv'),
    'data_freshness', 'monthly (2nd Tuesday)'
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
  'united_states_irs_eo_bmf',
  'United States IRS EO BMF',
  'U.S. tax-exempt organization registry from the IRS Exempt Organizations Business Master File extract',
  'registry',
  'countrydata_united_states_irs_eo_bmf.raw_records',
  (SELECT id FROM countries WHERE iso_alpha2 = 'US'),
  false,
  false,
  'manual',
  NULL,
  false,
  '{company_name,ein,status,locations,industries,financials}'::text[],
  jsonb_build_object(
    'api_url', 'https://www.irs.gov/pub/irs-soi/',
    'docs_url', 'https://www.irs.gov/charities-non-profits/exempt-organizations-business-master-file-extract-eo-bmf',
    'protocol', 'Four regional CSV extracts (eo1..eo4) converted to an NDJSON snapshot keyed by EIN',
    'source_schema', 'countrydata_united_states_irs_eo_bmf',
    'source_table', 'countrydata_united_states_irs_eo_bmf.raw_records',
    'supports_incremental', false,
    'auth_env', NULL,
    'fields', jsonb_build_array(
      'EIN',
      'NAME',
      'SORT_NAME',
      'STATUS',
      'SUBSECTION',
      'ORGANIZATION',
      'FOUNDATION',
      'NTEE_CD',
      'RULING',
      'TAX_PERIOD',
      'ASSET_AMT',
      'INCOME_AMT',
      'REVENUE_AMT',
      'CITY',
      'STATE',
      'ZIP'
    ),
    'notes', 'Ingest concatenates the four regional EO BMF CSV files into an NDJSON snapshot, records snapshot hash metadata, and stores current raw nonprofit rows by 9-char zero-padded EIN. STATUS reflects tax-exempt status, not corporate standing, and RULING is a YYYYMM recognition date rather than incorporation.'
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

GRANT USAGE ON SCHEMA countrydata_united_states_irs_eo_bmf TO corpscout_anon;
GRANT SELECT ON ALL TABLES IN SCHEMA countrydata_united_states_irs_eo_bmf TO corpscout_anon;
