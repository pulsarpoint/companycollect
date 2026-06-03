CREATE TABLE exchange_rate_source_files (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  provider TEXT NOT NULL,
  source_url TEXT NOT NULL,
  rate_date DATE NOT NULL,
  content_sha256 TEXT NOT NULL,
  content_length_bytes BIGINT NOT NULL,
  content_type TEXT,
  etag TEXT,
  last_modified TEXT,
  status TEXT NOT NULL DEFAULT 'downloaded',
  processed_at TIMESTAMPTZ,
  error TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_exchange_rate_source_files_provider CHECK (btrim(provider) <> ''),
  CONSTRAINT chk_exchange_rate_source_files_source_url CHECK (btrim(source_url) <> ''),
  CONSTRAINT chk_exchange_rate_source_files_provider_lower CHECK (provider = lower(provider)),
  CONSTRAINT chk_exchange_rate_source_files_sha256 CHECK (content_sha256 ~ '^[a-f0-9]{64}$'),
  CONSTRAINT chk_exchange_rate_source_files_content_length CHECK (content_length_bytes >= 0),
  CONSTRAINT chk_exchange_rate_source_files_status CHECK (status IN ('downloaded', 'processing', 'processed', 'failed')),
  CONSTRAINT chk_exchange_rate_source_files_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  UNIQUE (provider, source_url, content_sha256)
);

CREATE TABLE exchange_rate_sheets (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  provider TEXT NOT NULL,
  rate_date DATE NOT NULL,
  base_currency CHAR(3) NOT NULL,
  source_file_id UUID REFERENCES exchange_rate_source_files(id) ON DELETE SET NULL,
  content_sha256 TEXT NOT NULL,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_exchange_rate_sheets_provider CHECK (btrim(provider) <> ''),
  CONSTRAINT chk_exchange_rate_sheets_provider_lower CHECK (provider = lower(provider)),
  CONSTRAINT chk_exchange_rate_sheets_base_currency CHECK (base_currency ~ '^[A-Z]{3}$'),
  CONSTRAINT chk_exchange_rate_sheets_sha256 CHECK (content_sha256 ~ '^[a-f0-9]{64}$'),
  CONSTRAINT chk_exchange_rate_sheets_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  UNIQUE (provider, rate_date)
);

CREATE TABLE exchange_rates (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  sheet_id UUID NOT NULL REFERENCES exchange_rate_sheets(id) ON DELETE CASCADE,
  currency CHAR(3) NOT NULL,
  rate_per_base NUMERIC(24, 12) NOT NULL,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_exchange_rates_currency CHECK (currency ~ '^[A-Z]{3}$'),
  CONSTRAINT chk_exchange_rates_rate_positive CHECK (rate_per_base > 0),
  CONSTRAINT chk_exchange_rates_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  UNIQUE (sheet_id, currency)
);

CREATE TABLE exchange_rate_sync_runs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  temporal_workflow_id TEXT NOT NULL UNIQUE,
  provider TEXT NOT NULL,
  source_url TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'running',
  source_file_id UUID REFERENCES exchange_rate_source_files(id) ON DELETE SET NULL,
  sheet_id UUID REFERENCES exchange_rate_sheets(id) ON DELETE SET NULL,
  rate_date DATE,
  content_sha256 TEXT,
  currencies_seen INTEGER NOT NULL DEFAULT 0,
  currencies_imported INTEGER NOT NULL DEFAULT 0,
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at TIMESTAMPTZ,
  error TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  CONSTRAINT chk_exchange_rate_sync_runs_provider CHECK (btrim(provider) <> ''),
  CONSTRAINT chk_exchange_rate_sync_runs_source_url CHECK (btrim(source_url) <> ''),
  CONSTRAINT chk_exchange_rate_sync_runs_provider_lower CHECK (provider = lower(provider)),
  CONSTRAINT chk_exchange_rate_sync_runs_status CHECK (status IN ('running', 'skipped', 'succeeded', 'failed')),
  CONSTRAINT chk_exchange_rate_sync_runs_sha256 CHECK (content_sha256 IS NULL OR content_sha256 ~ '^[a-f0-9]{64}$'),
  CONSTRAINT chk_exchange_rate_sync_runs_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE INDEX idx_exchange_rate_source_files_provider_status
  ON exchange_rate_source_files(provider, status, rate_date DESC);

CREATE INDEX idx_exchange_rate_source_files_sha256
  ON exchange_rate_source_files(content_sha256);

CREATE INDEX idx_exchange_rate_sheets_provider_date
  ON exchange_rate_sheets(provider, rate_date DESC);

CREATE INDEX idx_exchange_rates_currency
  ON exchange_rates(currency);

CREATE INDEX idx_exchange_rate_sync_runs_provider_started
  ON exchange_rate_sync_runs(provider, started_at DESC);

CREATE OR REPLACE VIEW v_exchange_rate_sync_state AS
SELECT
  sheet.provider,
  sheet.rate_date,
  sheet.base_currency,
  sheet.content_sha256,
  sheet.created_at,
  sheet.updated_at,
  count(rate.id)::integer AS currency_count,
  max(file.source_url)::text AS source_url,
  max(file.processed_at)::timestamptz AS processed_at
FROM exchange_rate_sheets sheet
LEFT JOIN exchange_rates rate ON rate.sheet_id = sheet.id
LEFT JOIN exchange_rate_source_files file ON file.id = sheet.source_file_id
GROUP BY sheet.provider, sheet.rate_date, sheet.base_currency, sheet.content_sha256, sheet.created_at, sheet.updated_at;

CREATE OR REPLACE VIEW v_exchange_rate_sync_runs AS
SELECT
  run.id AS sync_run_id,
  run.temporal_workflow_id,
  run.provider,
  run.source_url,
  run.status AS sync_status,
  run.rate_date,
  run.content_sha256,
  run.currencies_seen,
  run.currencies_imported,
  run.started_at,
  run.finished_at,
  CASE
    WHEN run.error IS NULL THEN ''
    ELSE 'exchange rate sync failed'
  END AS sync_error,
  file.id AS source_file_id,
  file.status AS source_file_status,
  file.content_length_bytes,
  file.content_type,
  file.etag,
  file.last_modified,
  file.processed_at,
  sheet.id AS sheet_id
FROM exchange_rate_sync_runs run
LEFT JOIN exchange_rate_source_files file ON file.id = run.source_file_id
LEFT JOIN exchange_rate_sheets sheet ON sheet.id = run.sheet_id;

GRANT SELECT ON exchange_rate_source_files TO corpscout_anon;
GRANT SELECT ON exchange_rate_sheets TO corpscout_anon;
GRANT SELECT ON exchange_rates TO corpscout_anon;
GRANT SELECT ON v_exchange_rate_sync_state TO corpscout_anon;
GRANT SELECT ON v_exchange_rate_sync_runs TO corpscout_anon;
