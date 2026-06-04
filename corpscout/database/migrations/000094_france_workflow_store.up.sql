CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE SCHEMA IF NOT EXISTS france_workflow;

CREATE TABLE france_workflow.workflow_runs (
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
  CONSTRAINT chk_france_workflow_runs_type CHECK (
    run_type IN ('bulk_ingest', 'source_normalization')
  ),
  CONSTRAINT chk_france_workflow_runs_status CHECK (
    status IN ('running', 'succeeded', 'failed', 'cancelled')
  ),
  CONSTRAINT chk_france_workflow_runs_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE TABLE france_workflow.bulk_snapshots (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  workflow_run_id UUID REFERENCES france_workflow.workflow_runs(id) ON DELETE SET NULL,
  snapshot_date DATE,
  dataset_release TEXT,
  status TEXT NOT NULL DEFAULT 'downloaded',
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  downloaded_at TIMESTAMPTZ,
  parsed_at TIMESTAMPTZ,
  records_seen INTEGER NOT NULL DEFAULT 0,
  records_written INTEGER NOT NULL DEFAULT 0,
  error TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  CONSTRAINT chk_france_workflow_bulk_snapshot_status CHECK (
    status IN ('downloaded', 'parsed', 'failed')
  ),
  CONSTRAINT chk_france_workflow_bulk_snapshot_counts CHECK (
    records_seen >= 0 AND records_written >= 0
  ),
  CONSTRAINT chk_france_workflow_bulk_snapshot_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE TABLE france_workflow.source_files (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  bulk_snapshot_id UUID NOT NULL REFERENCES france_workflow.bulk_snapshots(id) ON DELETE CASCADE,
  dataset_key TEXT NOT NULL,
  resource_id TEXT NOT NULL,
  stable_url TEXT NOT NULL,
  resolved_url TEXT,
  file_name TEXT,
  file_format TEXT NOT NULL,
  content_type TEXT,
  content_length_bytes BIGINT,
  checksum_type TEXT,
  checksum_value TEXT,
  rows_seen INTEGER NOT NULL DEFAULT 0,
  rows_written INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL DEFAULT 'downloaded',
  error TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_france_workflow_source_file_dataset CHECK (
    dataset_key IN (
      'stock_unite_legale',
      'stock_etablissement',
      'stock_unite_legale_historique',
      'stock_etablissement_historique',
      'stock_etablissement_liens_succession'
    )
  ),
  CONSTRAINT chk_france_workflow_source_file_status CHECK (
    status IN ('downloaded', 'parsed', 'failed')
  ),
  CONSTRAINT chk_france_workflow_source_file_counts CHECK (
    rows_seen >= 0 AND rows_written >= 0
  ),
  CONSTRAINT chk_france_workflow_source_file_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  UNIQUE (bulk_snapshot_id, dataset_key)
);

CREATE TABLE france_workflow.raw_legal_units (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source_file_id UUID REFERENCES france_workflow.source_files(id) ON DELETE SET NULL,
  source_native_id TEXT NOT NULL,
  siren TEXT NOT NULL,
  diffusion_status TEXT,
  is_purged BOOLEAN,
  created_date DATE,
  acronym TEXT,
  gender TEXT,
  first_name_1 TEXT,
  first_name_2 TEXT,
  first_name_3 TEXT,
  first_name_4 TEXT,
  usual_first_name TEXT,
  pseudonym TEXT,
  association_identifier TEXT,
  employee_band TEXT,
  employee_year TEXT,
  source_updated_at TIMESTAMPTZ,
  period_count INTEGER,
  company_category TEXT,
  company_category_year TEXT,
  period_started_at DATE,
  administrative_status TEXT,
  birth_name TEXT,
  usage_name TEXT,
  legal_name TEXT,
  usual_name_1 TEXT,
  usual_name_2 TEXT,
  usual_name_3 TEXT,
  legal_form_code TEXT,
  primary_activity_code TEXT,
  primary_activity_nomenclature TEXT,
  headquarters_nic TEXT,
  social_solidarity BOOLEAN,
  mission_company BOOLEAN,
  is_employer BOOLEAN,
  primary_activity_naf25_code TEXT,
  raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  payload_hash TEXT NOT NULL,
  is_current BOOLEAN NOT NULL DEFAULT true,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  CONSTRAINT chk_france_workflow_raw_legal_units_source_native CHECK (source_native_id = siren),
  CONSTRAINT chk_france_workflow_raw_legal_units_siren CHECK (siren ~ '^[0-9]{9}$'),
  CONSTRAINT chk_france_workflow_raw_legal_units_payload_object CHECK (jsonb_typeof(raw_payload) = 'object'),
  CONSTRAINT chk_france_workflow_raw_legal_units_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  UNIQUE (siren, payload_hash)
);

CREATE TABLE france_workflow.raw_establishments (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source_file_id UUID REFERENCES france_workflow.source_files(id) ON DELETE SET NULL,
  source_native_id TEXT NOT NULL,
  siren TEXT NOT NULL,
  nic TEXT NOT NULL,
  siret TEXT NOT NULL,
  diffusion_status TEXT,
  created_date DATE,
  employee_band TEXT,
  employee_year TEXT,
  crafts_registry_activity TEXT,
  source_updated_at TIMESTAMPTZ,
  is_headquarters BOOLEAN,
  period_count INTEGER,
  address_complement TEXT,
  street_number TEXT,
  street_number_suffix TEXT,
  last_street_number TEXT,
  last_street_number_suffix TEXT,
  street_type TEXT,
  street_label TEXT,
  postal_code TEXT,
  city_label TEXT,
  foreign_city_label TEXT,
  special_distribution TEXT,
  commune_code TEXT,
  cedex_code TEXT,
  cedex_label TEXT,
  foreign_country_code TEXT,
  foreign_country_label TEXT,
  address_identifier TEXT,
  lambert_x TEXT,
  lambert_y TEXT,
  address_complement_2 TEXT,
  street_number_2 TEXT,
  street_number_suffix_2 TEXT,
  street_type_2 TEXT,
  street_label_2 TEXT,
  postal_code_2 TEXT,
  city_label_2 TEXT,
  foreign_city_label_2 TEXT,
  special_distribution_2 TEXT,
  commune_code_2 TEXT,
  cedex_code_2 TEXT,
  cedex_label_2 TEXT,
  foreign_country_code_2 TEXT,
  foreign_country_label_2 TEXT,
  period_started_at DATE,
  administrative_status TEXT,
  trade_name_1 TEXT,
  trade_name_2 TEXT,
  trade_name_3 TEXT,
  usual_name TEXT,
  primary_activity_code TEXT,
  primary_activity_nomenclature TEXT,
  is_employer BOOLEAN,
  primary_activity_naf25_code TEXT,
  raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  payload_hash TEXT NOT NULL,
  is_current BOOLEAN NOT NULL DEFAULT true,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  CONSTRAINT chk_france_workflow_raw_establishments_source_native CHECK (source_native_id = siret),
  CONSTRAINT chk_france_workflow_raw_establishments_siren CHECK (siren ~ '^[0-9]{9}$'),
  CONSTRAINT chk_france_workflow_raw_establishments_siret CHECK (siret ~ '^[0-9]{14}$'),
  CONSTRAINT chk_france_workflow_raw_establishments_nic CHECK (nic ~ '^[0-9]{5}$'),
  CONSTRAINT chk_france_workflow_raw_establishments_payload_object CHECK (jsonb_typeof(raw_payload) = 'object'),
  CONSTRAINT chk_france_workflow_raw_establishments_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  UNIQUE (siret, payload_hash)
);

CREATE INDEX idx_france_workflow_bulk_snapshots_started
  ON france_workflow.bulk_snapshots (started_at DESC);

CREATE INDEX idx_france_workflow_source_files_snapshot
  ON france_workflow.source_files (bulk_snapshot_id, dataset_key);

CREATE INDEX idx_france_workflow_raw_legal_units_siren
  ON france_workflow.raw_legal_units (siren);

CREATE INDEX idx_france_workflow_raw_legal_units_hash
  ON france_workflow.raw_legal_units (payload_hash);

CREATE UNIQUE INDEX idx_france_workflow_raw_legal_units_current_siren
  ON france_workflow.raw_legal_units (siren)
  WHERE is_current;

CREATE INDEX idx_france_workflow_raw_legal_units_name
  ON france_workflow.raw_legal_units (legal_name)
  WHERE legal_name IS NOT NULL;

CREATE INDEX idx_france_workflow_raw_establishments_siren
  ON france_workflow.raw_establishments (siren);

CREATE INDEX idx_france_workflow_raw_establishments_siret
  ON france_workflow.raw_establishments (siret);

CREATE INDEX idx_france_workflow_raw_establishments_hash
  ON france_workflow.raw_establishments (payload_hash);

CREATE UNIQUE INDEX idx_france_workflow_raw_establishments_current_siret
  ON france_workflow.raw_establishments (siret)
  WHERE is_current;

CREATE INDEX idx_france_workflow_raw_establishments_current_headquarters
  ON france_workflow.raw_establishments (siren)
  WHERE is_current AND is_headquarters;

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
  'france',
  'SIRENE (France)',
  'French SIRENE company and establishment registry from INSEE',
  'registry',
  'france_workflow.raw_legal_units',
  false,
  false,
  'manual',
  NULL,
  false,
  '{company_name,org_number,legal_form,status,locations,employee_count}'::text[],
  jsonb_build_object(
    'api_url', 'https://www.data.gouv.fr/api/1/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret/',
    'docs_url', 'https://www.insee.fr/fr/information/3591226',
    'dataset_url', 'https://www.data.gouv.fr/fr/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret/',
    'protocol', 'data.gouv stable resource file download',
    'page_size', NULL,
    'datasets', jsonb_build_array(
      jsonb_build_object(
        'key', 'stock_unite_legale',
        'label', 'StockUniteLegale',
        'resource_id', '350182c9-148a-46e0-8389-76c2ec1374a3',
        'stable_url', 'https://www.data.gouv.fr/api/1/datasets/r/350182c9-148a-46e0-8389-76c2ec1374a3',
        'format', 'parquet'
      ),
      jsonb_build_object(
        'key', 'stock_etablissement',
        'label', 'StockEtablissement',
        'resource_id', 'a29c1297-1f92-4e2a-8f6b-8c902ce96c5f',
        'stable_url', 'https://www.data.gouv.fr/api/1/datasets/r/a29c1297-1f92-4e2a-8f6b-8c902ce96c5f',
        'format', 'parquet'
      )
    ),
    'fields', jsonb_build_array('siren', 'siret', 'denominationUniteLegale', 'categorieJuridiqueUniteLegale', 'activitePrincipaleUniteLegale', 'etatAdministratifUniteLegale', 'etablissementSiege', 'adresse'),
    'auth_env', NULL,
    'notes', 'Official INSEE SIRENE bulk stock files. Initial ingest downloads current legal units and establishments through stable data.gouv resource URLs.'
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

GRANT USAGE ON SCHEMA france_workflow TO corpscout_anon;
GRANT SELECT ON ALL TABLES IN SCHEMA france_workflow TO corpscout_anon;
