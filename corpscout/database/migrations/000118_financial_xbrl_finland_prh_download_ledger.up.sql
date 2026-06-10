ALTER TABLE data_sources
  DROP CONSTRAINT IF EXISTS chk_data_sources_source_group;

ALTER TABLE data_sources
  ADD CONSTRAINT chk_data_sources_source_group CHECK (
    source_group IN (
      'security_identifier', 'registry', 'domain', 'website',
      'github', 'ai_research', 'manual', 'other', 'financial_statements'
    )
  );

ALTER TABLE data_sources
  DROP CONSTRAINT IF EXISTS chk_data_sources_source_file_name;

ALTER TABLE data_sources
  ADD CONSTRAINT chk_data_sources_source_file_name CHECK (
    source_file_name IS NULL OR source_file_name IN ('source.ndjson', 'source.json', 'statements.ndjson')
  );

ALTER TABLE data_source_files
  DROP CONSTRAINT IF EXISTS chk_data_source_files_kind;

ALTER TABLE data_source_files
  ADD CONSTRAINT chk_data_source_files_kind CHECK (
    kind IN ('source_snapshot', 'source_manifest', 'code_list', 'reference_data', 'archive')
  );

CREATE SCHEMA IF NOT EXISTS financial_xbrl;

CREATE TABLE financial_xbrl.finland_prh_xbrl_discovery_windows (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source_id UUID NOT NULL REFERENCES data_sources(id) ON DELETE CASCADE,
  registered_date_start DATE NOT NULL,
  registered_date_end DATE NOT NULL,
  action_run_id UUID REFERENCES data_source_action_runs(id) ON DELETE SET NULL,
  temporal_workflow_id TEXT,
  temporal_run_id TEXT,
  total_results BIGINT NOT NULL DEFAULT 0,
  pages_discovered INTEGER NOT NULL DEFAULT 0,
  statements_discovered BIGINT NOT NULL DEFAULT 0,
  last_completed_page INTEGER NOT NULL DEFAULT 0,
  completed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (source_id, registered_date_start, registered_date_end),
  CONSTRAINT chk_finland_prh_xbrl_window_dates CHECK (registered_date_start <= registered_date_end),
  CONSTRAINT chk_finland_prh_xbrl_window_counts CHECK (
    total_results >= 0 AND pages_discovered >= 0 AND statements_discovered >= 0 AND last_completed_page >= 0
  )
);

CREATE TABLE financial_xbrl.finland_prh_xbrl_statement_artifacts (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source_id UUID NOT NULL REFERENCES data_sources(id) ON DELETE CASCADE,
  business_id TEXT NOT NULL,
  financial_date DATE NOT NULL,
  registration_date DATE,
  source_url TEXT NOT NULL,
  xml_path TEXT,
  xml_sha256 TEXT,
  xml_size_bytes BIGINT,
  download_status TEXT NOT NULL DEFAULT 'pending',
  attempts INTEGER NOT NULL DEFAULT 0,
  last_attempt_at TIMESTAMPTZ,
  downloaded_at TIMESTAMPTZ,
  last_error_message TEXT,
  first_discovered_run_id UUID REFERENCES data_source_action_runs(id) ON DELETE SET NULL,
  latest_action_run_id UUID REFERENCES data_source_action_runs(id) ON DELETE SET NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (source_id, business_id, financial_date),
  CONSTRAINT chk_finland_prh_xbrl_business_id CHECK (btrim(business_id) <> ''),
  CONSTRAINT chk_finland_prh_xbrl_source_url CHECK (btrim(source_url) <> ''),
  CONSTRAINT chk_finland_prh_xbrl_download_status CHECK (
    download_status IN ('pending', 'downloading', 'succeeded', 'failed')
  ),
  CONSTRAINT chk_finland_prh_xbrl_xml_size CHECK (xml_size_bytes IS NULL OR xml_size_bytes >= 0),
  CONSTRAINT chk_finland_prh_xbrl_attempts CHECK (attempts >= 0)
);

CREATE INDEX idx_finland_prh_xbrl_windows_source_dates
  ON financial_xbrl.finland_prh_xbrl_discovery_windows (source_id, registered_date_start, registered_date_end);

CREATE INDEX idx_finland_prh_xbrl_statement_artifacts_source_status
  ON financial_xbrl.finland_prh_xbrl_statement_artifacts (source_id, download_status, registration_date);

CREATE INDEX idx_finland_prh_xbrl_statement_artifacts_business_date
  ON financial_xbrl.finland_prh_xbrl_statement_artifacts (business_id, financial_date DESC);
