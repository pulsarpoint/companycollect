CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE SCHEMA IF NOT EXISTS se_workflow;

CREATE TABLE se_workflow.workflow_runs (
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
  CONSTRAINT chk_se_workflow_runs_type CHECK (
    run_type IN ('bulk_ingest', 'source_normalization')
  ),
  CONSTRAINT chk_se_workflow_runs_status CHECK (
    status IN ('running', 'succeeded', 'failed', 'cancelled')
  ),
  CONSTRAINT chk_se_workflow_runs_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE TABLE se_workflow.bulk_snapshots (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  workflow_run_id UUID REFERENCES se_workflow.workflow_runs(id) ON DELETE SET NULL,
  snapshot_key TEXT,
  snapshot_date DATE,
  status TEXT NOT NULL DEFAULT 'downloaded',
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  downloaded_at TIMESTAMPTZ,
  parsed_at TIMESTAMPTZ,
  records_seen INTEGER NOT NULL DEFAULT 0,
  records_written INTEGER NOT NULL DEFAULT 0,
  error TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  CONSTRAINT chk_se_workflow_bulk_snapshot_status CHECK (
    status IN ('downloaded', 'parsed', 'failed')
  ),
  CONSTRAINT chk_se_workflow_bulk_snapshot_counts CHECK (
    records_seen >= 0 AND records_written >= 0
  ),
  CONSTRAINT chk_se_workflow_bulk_snapshot_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE TABLE se_workflow.source_files (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  bulk_snapshot_id UUID REFERENCES se_workflow.bulk_snapshots(id) ON DELETE SET NULL,
  dataset_key TEXT NOT NULL,
  source_url TEXT,
  file_name TEXT,
  file_format TEXT NOT NULL,
  content_type TEXT,
  content_length_bytes BIGINT,
  payload_hash TEXT,
  rows_seen INTEGER NOT NULL DEFAULT 0,
  rows_written INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL DEFAULT 'downloaded',
  error TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_se_workflow_source_file_status CHECK (
    status IN ('downloaded', 'parsed', 'failed')
  ),
  CONSTRAINT chk_se_workflow_source_file_counts CHECK (
    rows_seen >= 0 AND rows_written >= 0
  ),
  CONSTRAINT chk_se_workflow_source_file_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  UNIQUE (bulk_snapshot_id, dataset_key, source_url)
);

CREATE TABLE se_workflow.raw_records (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  bulk_snapshot_id UUID REFERENCES se_workflow.bulk_snapshots(id) ON DELETE SET NULL,
  source_file_id UUID REFERENCES se_workflow.source_files(id) ON DELETE SET NULL,
  source_native_id TEXT NOT NULL,
  organization_number TEXT NOT NULL,
  organization_name TEXT,
  legal_form TEXT,
  registration_status TEXT,
  business_description TEXT,
  sni_codes JSONB NOT NULL DEFAULT '[]'::jsonb,
  postal_address JSONB NOT NULL DEFAULT '{}'::jsonb,
  country_iso2 TEXT NOT NULL DEFAULT 'SE',
  source_updated_at TIMESTAMPTZ,
  raw_payload JSONB NOT NULL,
  payload_hash TEXT NOT NULL,
  is_current BOOLEAN NOT NULL DEFAULT true,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  run_id TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  CONSTRAINT chk_se_workflow_raw_source_native CHECK (source_native_id = organization_number),
  CONSTRAINT chk_se_workflow_raw_sni_codes_array CHECK (jsonb_typeof(sni_codes) = 'array'),
  CONSTRAINT chk_se_workflow_raw_postal_address_object CHECK (jsonb_typeof(postal_address) = 'object'),
  CONSTRAINT chk_se_workflow_raw_payload_object CHECK (jsonb_typeof(raw_payload) = 'object'),
  CONSTRAINT chk_se_workflow_raw_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  UNIQUE (organization_number, payload_hash)
);

CREATE INDEX idx_se_workflow_bulk_snapshots_started
  ON se_workflow.bulk_snapshots (started_at DESC);

CREATE INDEX idx_se_workflow_source_files_snapshot
  ON se_workflow.source_files (bulk_snapshot_id, dataset_key);

CREATE INDEX idx_se_workflow_raw_records_org
  ON se_workflow.raw_records (organization_number);

CREATE INDEX idx_se_workflow_raw_records_hash
  ON se_workflow.raw_records (payload_hash);

CREATE UNIQUE INDEX idx_se_workflow_raw_records_current_org
  ON se_workflow.raw_records (organization_number)
  WHERE is_current;

CREATE INDEX idx_se_workflow_raw_records_name
  ON se_workflow.raw_records (organization_name)
  WHERE organization_name IS NOT NULL;

INSERT INTO data_sources (
  name,
  display_name,
  description,
  source_group,
  input_table_name,
  enabled,
  schedule_enabled,
  schedule_kind,
  schedule_expression,
  requires_translation,
  capabilities,
  config
)
VALUES (
  'se',
  'Sweden HVD',
  'Swedish high-value basic company information from Bolagsverket and SCB',
  'registry',
  'se_workflow.raw_records',
  false,
  false,
  'manual',
  NULL,
  true,
  '{company_name,org_number,legal_form,status,locations,industries}'::text[],
  jsonb_build_object(
    'docs_url', 'https://www.scb.se/vara-tjanster/bestall-data-och-statistik/foretagsregistret/vardefulla-datamangder--grundlaggande-foretagsinformation/',
    'api_portal_url', 'https://portal.api.bolagsverket.se/devportal/apis/ff8f9a91-1fdd-4705-8836-c1906581162f',
    'protocol', 'Bolagsverket/SCB HVD file download',
    'datasets_env', 'SE_HVD_DATASETS_JSON',
    'datasets', jsonb_build_array(
      jsonb_build_object('key', 'organisationer', 'format', 'json'),
      jsonb_build_object('key', 'arsredovisningar', 'format', 'json')
    ),
    'fields', jsonb_build_array(
      'identitetsbeteckning',
      'organisationsnamn',
      'organisationsform',
      'avregistreradOrganisation',
      'verksamhetsbeskrivning',
      'naringsgrenOrganisation',
      'postadressOrganisation'
    ),
    'notes', 'Initial ingest stores raw HVD organization records in se_workflow.raw_records. Source file URLs are supplied through SE_HVD_DATASETS_JSON so portal-published HVD download URLs can be changed without a migration.'
  )
)
ON CONFLICT (name) DO UPDATE SET
  display_name = EXCLUDED.display_name,
  description = EXCLUDED.description,
  source_group = EXCLUDED.source_group,
  input_table_name = EXCLUDED.input_table_name,
  enabled = EXCLUDED.enabled,
  schedule_enabled = EXCLUDED.schedule_enabled,
  schedule_kind = EXCLUDED.schedule_kind,
  schedule_expression = EXCLUDED.schedule_expression,
  requires_translation = EXCLUDED.requires_translation,
  capabilities = EXCLUDED.capabilities,
  config = EXCLUDED.config,
  updated_at = now();

GRANT USAGE ON SCHEMA se_workflow TO corpscout_anon;
GRANT SELECT ON ALL TABLES IN SCHEMA se_workflow TO corpscout_anon;
