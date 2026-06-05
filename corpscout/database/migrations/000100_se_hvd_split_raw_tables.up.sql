CREATE TABLE IF NOT EXISTS se_workflow.bolagsverket_raw_records (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  bulk_snapshot_id UUID REFERENCES se_workflow.bulk_snapshots(id) ON DELETE SET NULL,
  source_file_id UUID REFERENCES se_workflow.source_files(id) ON DELETE SET NULL,
  source_record_key TEXT NOT NULL,
  row_number INTEGER,
  organisationsidentitet TEXT NOT NULL,
  organization_number TEXT NOT NULL,
  namnskyddslopnummer TEXT,
  registreringsland TEXT,
  organisationsnamn TEXT NOT NULL,
  organization_name TEXT,
  organisationsform TEXT,
  avregistreringsdatum TEXT,
  avregistreringsorsak TEXT,
  pagande_avvecklings_eller_omstruktureringsforfarande TEXT,
  registreringsdatum TEXT,
  verksamhetsbeskrivning TEXT,
  postadress TEXT,
  postal_address JSONB NOT NULL DEFAULT '{}'::jsonb,
  country_iso2 TEXT NOT NULL DEFAULT 'SE',
  raw_payload JSONB NOT NULL,
  payload_hash TEXT NOT NULL,
  is_current BOOLEAN NOT NULL DEFAULT true,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  run_id TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  CONSTRAINT chk_se_workflow_bolagsverket_postal_address_object CHECK (jsonb_typeof(postal_address) = 'object'),
  CONSTRAINT chk_se_workflow_bolagsverket_raw_payload_object CHECK (jsonb_typeof(raw_payload) = 'object'),
  CONSTRAINT chk_se_workflow_bolagsverket_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  UNIQUE (source_record_key, payload_hash)
);

CREATE INDEX idx_se_workflow_bolagsverket_raw_org
  ON se_workflow.bolagsverket_raw_records (organization_number);

CREATE INDEX idx_se_workflow_bolagsverket_raw_hash
  ON se_workflow.bolagsverket_raw_records (payload_hash);

CREATE UNIQUE INDEX idx_se_workflow_bolagsverket_raw_current_key
  ON se_workflow.bolagsverket_raw_records (source_record_key)
  WHERE is_current;

CREATE INDEX idx_se_workflow_bolagsverket_raw_name
  ON se_workflow.bolagsverket_raw_records (organization_name)
  WHERE organization_name IS NOT NULL;

CREATE TABLE IF NOT EXISTS se_workflow.scb_raw_records (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  bulk_snapshot_id UUID REFERENCES se_workflow.bulk_snapshots(id) ON DELETE SET NULL,
  source_file_id UUID REFERENCES se_workflow.source_files(id) ON DELETE SET NULL,
  source_record_key TEXT NOT NULL,
  row_number INTEGER,
  for_andr_typ TEXT,
  co_adress TEXT,
  foretagsnamn TEXT,
  ftg_stat TEXT,
  gatuadress TEXT,
  je_stat TEXT,
  jur_form TEXT,
  namn TEXT,
  ng1 TEXT,
  ng2 TEXT,
  ng3 TEXT,
  ng4 TEXT,
  ng5 TEXT,
  pe_org_nr TEXT NOT NULL,
  organization_number TEXT,
  post_nr TEXT,
  post_ort TEXT,
  reg_dat_ktid TEXT,
  reklamsparrtyp TEXT,
  m_co_adress TEXT,
  m_foretagsnamn TEXT,
  m_ftg_stat TEXT,
  m_gatuadress TEXT,
  m_je_stat TEXT,
  m_jur_form TEXT,
  m_namn TEXT,
  m_ng1 TEXT,
  m_ng2 TEXT,
  m_ng3 TEXT,
  m_ng4 TEXT,
  m_ng5 TEXT,
  m_post_nr TEXT,
  m_post_ort TEXT,
  m_reg_dat_ktid TEXT,
  m_reklamsparrtyp TEXT,
  mask_columns JSONB NOT NULL DEFAULT '{}'::jsonb,
  sni_codes JSONB NOT NULL DEFAULT '[]'::jsonb,
  postal_address JSONB NOT NULL DEFAULT '{}'::jsonb,
  country_iso2 TEXT NOT NULL DEFAULT 'SE',
  raw_payload JSONB NOT NULL,
  payload_hash TEXT NOT NULL,
  is_current BOOLEAN NOT NULL DEFAULT true,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  run_id TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  CONSTRAINT chk_se_workflow_scb_mask_columns_object CHECK (jsonb_typeof(mask_columns) = 'object'),
  CONSTRAINT chk_se_workflow_scb_sni_codes_array CHECK (jsonb_typeof(sni_codes) = 'array'),
  CONSTRAINT chk_se_workflow_scb_postal_address_object CHECK (jsonb_typeof(postal_address) = 'object'),
  CONSTRAINT chk_se_workflow_scb_raw_payload_object CHECK (jsonb_typeof(raw_payload) = 'object'),
  CONSTRAINT chk_se_workflow_scb_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  UNIQUE (source_record_key, payload_hash)
);

CREATE INDEX idx_se_workflow_scb_raw_pe_org_nr
  ON se_workflow.scb_raw_records (pe_org_nr);

CREATE INDEX idx_se_workflow_scb_raw_org
  ON se_workflow.scb_raw_records (organization_number)
  WHERE organization_number IS NOT NULL;

CREATE INDEX idx_se_workflow_scb_raw_hash
  ON se_workflow.scb_raw_records (payload_hash);

CREATE UNIQUE INDEX idx_se_workflow_scb_raw_current_key
  ON se_workflow.scb_raw_records (source_record_key)
  WHERE is_current;

CREATE INDEX idx_se_workflow_scb_raw_name
  ON se_workflow.scb_raw_records (namn)
  WHERE namn IS NOT NULL;

GRANT SELECT ON se_workflow.bolagsverket_raw_records TO corpscout_anon;
GRANT SELECT ON se_workflow.scb_raw_records TO corpscout_anon;
