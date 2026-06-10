ALTER TABLE data_sources
  ADD COLUMN IF NOT EXISTS country TEXT,
  ADD COLUMN IF NOT EXISTS source TEXT,
  ADD COLUMN IF NOT EXISTS registry_key TEXT,
  ADD COLUMN IF NOT EXISTS auth_required BOOLEAN NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS storage_kind TEXT NOT NULL DEFAULT 'clickhouse',
  ADD COLUMN IF NOT EXISTS clickhouse_database TEXT,
  ADD COLUMN IF NOT EXISTS clickhouse_table_prefix TEXT;

UPDATE data_sources
SET
  country = COALESCE(country, lower(countries.name)),
  storage_kind = COALESCE(NULLIF(storage_kind, ''), 'clickhouse')
FROM countries
WHERE data_sources.country_id = countries.id
  AND data_sources.country IS NULL;

UPDATE data_sources
SET source = COALESCE(source, name)
WHERE source IS NULL;

UPDATE data_sources
SET registry_key = COALESCE(registry_key, country || '/' || source)
WHERE registry_key IS NULL
  AND country IS NOT NULL
  AND source IS NOT NULL;

UPDATE data_sources
SET
  country = COALESCE(country, 'global'),
  source = COALESCE(source, name),
  registry_key = COALESCE(registry_key, COALESCE(country, 'global') || '/' || COALESCE(source, name)),
  auth_required = COALESCE(auth_required, false),
  storage_kind = COALESCE(NULLIF(storage_kind, ''), 'clickhouse'),
  clickhouse_database = COALESCE(clickhouse_database, 'corpscout_sources'),
  clickhouse_table_prefix = COALESCE(clickhouse_table_prefix, replace(name, '-', '_'));

ALTER TABLE data_sources
  DROP CONSTRAINT IF EXISTS chk_data_sources_schedule_kind,
  DROP CONSTRAINT IF EXISTS chk_data_sources_marker_pair,
  DROP CONSTRAINT IF EXISTS chk_data_sources_failures,
  DROP CONSTRAINT IF EXISTS data_sources_country_id_fkey;

DROP INDEX IF EXISTS data_sources_country_id_idx;

ALTER TABLE data_sources
  DROP COLUMN IF EXISTS schedule_enabled,
  DROP COLUMN IF EXISTS schedule_kind,
  DROP COLUMN IF EXISTS schedule_expression,
  DROP COLUMN IF EXISTS last_started_at,
  DROP COLUMN IF EXISTS last_success_at,
  DROP COLUMN IF EXISTS last_failed_at,
  DROP COLUMN IF EXISTS last_source_marker_type,
  DROP COLUMN IF EXISTS last_source_marker,
  DROP COLUMN IF EXISTS last_source_modified_at,
  DROP COLUMN IF EXISTS last_error,
  DROP COLUMN IF EXISTS consecutive_failures,
  DROP COLUMN IF EXISTS country_id;

UPDATE data_sources
SET
  name = 'finland_prhytj',
  display_name = 'Finland PRH YTJ',
  description = 'Finnish company registry data from PRH Open Data YTJ API v3',
  source_group = 'registry',
  input_table_name = 'corpscout_sources.fi_prhytj_*',
  country = 'finland',
  source = 'prhytj',
  registry_key = 'finland/prhytj',
  enabled = true,
  requires_translation = false,
  auth_required = false,
  capabilities = '{company_import,clickhouse,source_download}'::text[],
  storage_kind = 'clickhouse',
  clickhouse_database = 'corpscout_sources',
  clickhouse_table_prefix = 'fi_prhytj',
  config = jsonb_build_object(
    'api_url', 'https://avoindata.prh.fi/opendata-ytj-api/v3/companies',
    'docs_url', 'https://www.prh.fi/en/kaupparekisteri/tietopalvelut/open_data.html',
    'raw_source_retention', 'filesystem_run_directory'
  ),
  updated_at = now()
WHERE name = 'finland_prh_ytj_v3'
  AND NOT EXISTS (
    SELECT 1 FROM data_sources existing WHERE existing.name = 'finland_prhytj'
  );

UPDATE data_sources
SET
  name = 'united_states_coloradoentities',
  display_name = 'Colorado Business Entities',
  description = 'Colorado business entity registry data from the Colorado Secretary of State Socrata dataset',
  source_group = 'registry',
  input_table_name = 'corpscout_sources.us_coloradoentities_*',
  country = 'united_states',
  source = 'coloradoentities',
  registry_key = 'united_states/coloradoentities',
  enabled = true,
  requires_translation = false,
  auth_required = false,
  capabilities = '{company_import,clickhouse,source_download}'::text[],
  storage_kind = 'clickhouse',
  clickhouse_database = 'corpscout_sources',
  clickhouse_table_prefix = 'us_coloradoentities',
  config = jsonb_build_object(
    'api_url', 'https://data.colorado.gov/resource/4ykn-tg5h.json',
    'docs_url', 'https://data.colorado.gov/Business/Business-Entities-in-Colorado/4ykn-tg5h',
    'raw_source_retention', 'filesystem_run_directory'
  ),
  updated_at = now()
WHERE name = 'united_states_colorado_business_entities'
  AND NOT EXISTS (
    SELECT 1 FROM data_sources existing WHERE existing.name = 'united_states_coloradoentities'
  );

INSERT INTO data_sources (
  name, display_name, description, source_group, input_table_name,
  country, source, registry_key, enabled, requires_translation, auth_required, capabilities,
  storage_kind, clickhouse_database, clickhouse_table_prefix, config
)
VALUES
  (
    'finland_prhytj',
    'Finland PRH YTJ',
    'Finnish company registry data from PRH Open Data YTJ API v3',
    'registry',
    'corpscout_sources.fi_prhytj_*',
    'finland',
    'prhytj',
    'finland/prhytj',
    true,
    false,
    false,
    '{company_import,clickhouse,source_download}'::text[],
    'clickhouse',
    'corpscout_sources',
    'fi_prhytj',
    jsonb_build_object(
      'api_url', 'https://avoindata.prh.fi/opendata-ytj-api/v3/companies',
      'docs_url', 'https://www.prh.fi/en/kaupparekisteri/tietopalvelut/open_data.html',
      'raw_source_retention', 'filesystem_run_directory'
    )
  ),
  (
    'united_states_coloradoentities',
    'Colorado Business Entities',
    'Colorado business entity registry data from the Colorado Secretary of State Socrata dataset',
    'registry',
    'corpscout_sources.us_coloradoentities_*',
    'united_states',
    'coloradoentities',
    'united_states/coloradoentities',
    true,
    false,
    false,
    '{company_import,clickhouse,source_download}'::text[],
    'clickhouse',
    'corpscout_sources',
    'us_coloradoentities',
    jsonb_build_object(
      'api_url', 'https://data.colorado.gov/resource/4ykn-tg5h.json',
      'docs_url', 'https://data.colorado.gov/Business/Business-Entities-in-Colorado/4ykn-tg5h',
      'raw_source_retention', 'filesystem_run_directory'
    )
  ),
  (
    'united_states_irseobmf',
    'IRS EO BMF',
    'U.S. tax-exempt organization registry from the IRS Exempt Organizations Business Master File extract',
    'registry',
    'corpscout_sources.us_irseobmf_*',
    'united_states',
    'irseobmf',
    'united_states/irseobmf',
    true,
    false,
    false,
    '{company_import,clickhouse,source_download}'::text[],
    'clickhouse',
    'corpscout_sources',
    'us_irseobmf',
    jsonb_build_object(
      'api_url', 'https://www.irs.gov/pub/irs-soi/',
      'docs_url', 'https://www.irs.gov/charities-non-profits/exempt-organizations-business-master-file-extract-eo-bmf',
      'raw_source_retention', 'filesystem_run_directory'
    )
  ),
  (
    'united_states_secedgar',
    'SEC EDGAR Company Tickers',
    'U.S. public company CIK/ticker map from the SEC EDGAR company_tickers.json file',
    'registry',
    'corpscout_sources.us_secedgar_*',
    'united_states',
    'secedgar',
    'united_states/secedgar',
    true,
    false,
    false,
    '{company_import,clickhouse,source_download}'::text[],
    'clickhouse',
    'corpscout_sources',
    'us_secedgar',
    jsonb_build_object(
      'api_url', 'https://www.sec.gov/files/company_tickers.json',
      'docs_url', 'https://www.sec.gov/os/webmaster-faq#developers',
      'raw_source_retention', 'filesystem_run_directory',
      'user_agent_required', true
    )
  )
ON CONFLICT (name) DO UPDATE SET
  display_name = EXCLUDED.display_name,
  description = EXCLUDED.description,
  source_group = EXCLUDED.source_group,
  input_table_name = EXCLUDED.input_table_name,
  country = EXCLUDED.country,
  source = EXCLUDED.source,
  registry_key = EXCLUDED.registry_key,
  enabled = EXCLUDED.enabled,
  requires_translation = EXCLUDED.requires_translation,
  auth_required = EXCLUDED.auth_required,
  capabilities = EXCLUDED.capabilities,
  storage_kind = EXCLUDED.storage_kind,
  clickhouse_database = EXCLUDED.clickhouse_database,
  clickhouse_table_prefix = EXCLUDED.clickhouse_table_prefix,
  config = EXCLUDED.config,
  updated_at = now();

ALTER TABLE data_sources
  ALTER COLUMN country SET NOT NULL,
  ALTER COLUMN source SET NOT NULL,
  ALTER COLUMN registry_key SET NOT NULL,
  ALTER COLUMN storage_kind SET NOT NULL,
  ALTER COLUMN clickhouse_database SET NOT NULL,
  ALTER COLUMN clickhouse_table_prefix SET NOT NULL;

ALTER TABLE data_sources
  ADD CONSTRAINT uq_data_sources_registry_key UNIQUE (registry_key),
  ADD CONSTRAINT chk_data_sources_storage_kind CHECK (storage_kind IN ('clickhouse')),
  ADD CONSTRAINT chk_data_sources_country_source CHECK (country <> '' AND source <> '');

CREATE TABLE data_source_actions (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source_id UUID NOT NULL REFERENCES data_sources(id) ON DELETE CASCADE,
  action TEXT NOT NULL,
  display_name TEXT NOT NULL,
  temporal_workflow_type TEXT NOT NULL,
  temporal_task_queue TEXT,
  enabled BOOLEAN NOT NULL DEFAULT true,
  config JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (source_id, action),
  CONSTRAINT chk_data_source_actions_action CHECK (action IN ('pull_source', 'import_clickhouse')),
  CONSTRAINT chk_data_source_actions_config_object CHECK (jsonb_typeof(config) = 'object')
);

CREATE TABLE data_source_action_runs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source_id UUID NOT NULL REFERENCES data_sources(id) ON DELETE CASCADE,
  action_id UUID NOT NULL REFERENCES data_source_actions(id) ON DELETE CASCADE,
  action TEXT NOT NULL,
  status TEXT NOT NULL,
  temporal_workflow_id TEXT,
  temporal_run_id TEXT,
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at TIMESTAMPTZ,
  input JSONB NOT NULL DEFAULT '{}'::jsonb,
  result JSONB NOT NULL DEFAULT '{}'::jsonb,
  error_message TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_data_source_action_runs_action CHECK (action IN ('pull_source', 'import_clickhouse')),
  CONSTRAINT chk_data_source_action_runs_status CHECK (status IN ('running', 'succeeded', 'failed', 'cancelled')),
  CONSTRAINT chk_data_source_action_runs_input_object CHECK (jsonb_typeof(input) = 'object'),
  CONSTRAINT chk_data_source_action_runs_result_object CHECK (jsonb_typeof(result) = 'object'),
  CONSTRAINT chk_data_source_action_runs_finished_at CHECK (
    (status = 'running' AND finished_at IS NULL)
    OR (status <> 'running' AND finished_at IS NOT NULL)
  )
);

CREATE INDEX idx_data_sources_country_source ON data_sources(country, source);
CREATE INDEX idx_data_source_actions_source ON data_source_actions(source_id);
CREATE INDEX idx_data_source_action_runs_source_started ON data_source_action_runs(source_id, started_at DESC);
CREATE INDEX idx_data_source_action_runs_action_started ON data_source_action_runs(action_id, started_at DESC);

INSERT INTO data_source_actions (
  source_id, action, display_name, temporal_workflow_type, temporal_task_queue
)
SELECT
  ds.id,
  action_def.action,
  action_def.display_name,
  action_def.temporal_workflow_type,
  action_def.temporal_task_queue
FROM data_sources ds
CROSS JOIN (
  VALUES
    ('pull_source', 'Pull source data', 'CompanySourceDownloadWorkflow', 'corpscout-company-sources'),
    ('import_clickhouse', 'Import into ClickHouse', 'CompanySourceClickHouseImportWorkflow', 'corpscout-company-sources')
) AS action_def(action, display_name, temporal_workflow_type, temporal_task_queue)
WHERE ds.registry_key IN (
  'finland/prhytj',
  'united_states/coloradoentities',
  'united_states/irseobmf',
  'united_states/secedgar'
)
ON CONFLICT (source_id, action) DO UPDATE SET
  display_name = EXCLUDED.display_name,
  temporal_workflow_type = EXCLUDED.temporal_workflow_type,
  temporal_task_queue = EXCLUDED.temporal_task_queue,
  enabled = true,
  updated_at = now();
