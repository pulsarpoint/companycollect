-- Drop views that depend on company_sources and data_sources before dropping those tables.
-- v_company_sources is obsolete (replaced by suggestion_source_links) — drop permanently.
-- v_companies is recreated below after the new data_sources table exists.
DROP VIEW IF EXISTS v_company_sources;
DROP VIEW IF EXISTS v_companies;

-- Drop FK references to old data_sources before we drop the table.
ALTER TABLE companies DROP CONSTRAINT IF EXISTS companies_primary_source_id_fkey;
ALTER TABLE company_aliases DROP CONSTRAINT IF EXISTS company_aliases_source_id_fkey;

-- Null out stale UUIDs so re-added FKs don't violate referential integrity.
UPDATE companies SET primary_source_id = NULL WHERE primary_source_id IS NOT NULL;
UPDATE company_aliases SET source_id = NULL WHERE source_id IS NOT NULL;

-- Drop legacy direct-write ingestion tables in dependency order.
DROP TABLE IF EXISTS company_domain_reviews;
DROP TABLE IF EXISTS company_sources;
DROP TABLE IF EXISTS source_snapshots;
DROP TABLE IF EXISTS source_pull_runs;
DROP TABLE IF EXISTS data_sources;

-- Clean new source registry.
CREATE TABLE data_sources (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT UNIQUE NOT NULL,
    display_name TEXT,
    description TEXT,
    source_group TEXT NOT NULL,
    input_table_name TEXT NOT NULL,
    pull_task_type TEXT NOT NULL,
    processor_task_type TEXT,
    enabled BOOLEAN NOT NULL DEFAULT true,
    schedule_kind TEXT NOT NULL DEFAULT 'manual',
    schedule_expression TEXT,
    config JSONB NOT NULL DEFAULT '{}'::jsonb,
    last_started_at TIMESTAMPTZ,
    last_success_at TIMESTAMPTZ,
    last_failed_at TIMESTAMPTZ,
    last_source_marker_type TEXT,
    last_source_marker TEXT,
    last_source_modified_at TIMESTAMPTZ,
    last_error TEXT,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_data_sources_source_group CHECK (
        source_group IN (
            'security_identifier', 'registry', 'domain', 'website',
            'github', 'ai_research', 'manual', 'other'
        )
    ),
    CONSTRAINT chk_data_sources_schedule_kind CHECK (
        schedule_kind IN ('manual', 'interval', 'cron', 'event')
    ),
    CONSTRAINT chk_data_sources_marker_pair CHECK (
        (last_source_marker_type IS NULL AND last_source_marker IS NULL)
        OR (last_source_marker_type IS NOT NULL AND last_source_marker IS NOT NULL)
    ),
    CONSTRAINT chk_data_sources_failures CHECK (consecutive_failures >= 0),
    CONSTRAINT chk_data_sources_config_object CHECK (jsonb_typeof(config) = 'object')
);

-- Clean pull-run audit table.
CREATE TABLE source_pull_runs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_id UUID NOT NULL REFERENCES data_sources(id) ON DELETE CASCADE,
    river_job_id BIGINT,
    task_type TEXT NOT NULL,
    trigger_type TEXT NOT NULL DEFAULT 'scheduled',
    status TEXT NOT NULL DEFAULT 'running',
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    rows_seen INTEGER NOT NULL DEFAULT 0,
    raw_rows_inserted INTEGER NOT NULL DEFAULT 0,
    raw_rows_updated INTEGER NOT NULL DEFAULT 0,
    raw_rows_unchanged INTEGER NOT NULL DEFAULT 0,
    error_message TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_source_pull_runs_trigger_type CHECK (
        trigger_type IN ('scheduled', 'manual', 'retry', 'backfill', 'event')
    ),
    CONSTRAINT chk_source_pull_runs_status CHECK (
        status IN ('running', 'succeeded', 'failed', 'cancelled')
    ),
    CONSTRAINT chk_source_pull_runs_counts CHECK (
        rows_seen >= 0
        AND raw_rows_inserted >= 0
        AND raw_rows_updated >= 0
        AND raw_rows_unchanged >= 0
    ),
    CONSTRAINT chk_source_pull_runs_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE INDEX idx_source_pull_runs_source_started
    ON source_pull_runs(source_id, started_at DESC);

-- Processor state for compatibility schemas (CPE/CVE) that can't use row-queue columns.
CREATE TABLE source_processor_states (
    source_id UUID NOT NULL REFERENCES data_sources(id) ON DELETE CASCADE,
    processor_task_type TEXT NOT NULL,
    last_started_at TIMESTAMPTZ,
    last_success_at TIMESTAMPTZ,
    last_failed_at TIMESTAMPTZ,
    last_processed_marker_type TEXT,
    last_processed_marker TEXT,
    last_processed_at TIMESTAMPTZ,
    last_source_pull_run_id UUID REFERENCES source_pull_runs(id),
    last_error TEXT,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source_id, processor_task_type),
    CONSTRAINT chk_source_processor_states_marker_pair CHECK (
        (last_processed_marker_type IS NULL AND last_processed_marker IS NULL)
        OR (last_processed_marker_type IS NOT NULL AND last_processed_marker IS NOT NULL)
    ),
    CONSTRAINT chk_source_processor_states_failures CHECK (consecutive_failures >= 0)
);

-- Re-add FKs pointing to the new data_sources table.
ALTER TABLE companies
    ADD CONSTRAINT companies_primary_source_id_fkey
    FOREIGN KEY (primary_source_id) REFERENCES data_sources(id) ON DELETE SET NULL;

ALTER TABLE company_aliases
    ADD CONSTRAINT company_aliases_source_id_fkey
    FOREIGN KEY (source_id) REFERENCES data_sources(id) ON DELETE SET NULL;

-- Seed MVP source registry.
INSERT INTO data_sources (
    name, display_name, source_group, input_table_name,
    pull_task_type, processor_task_type, enabled,
    schedule_kind, schedule_expression, config
)
VALUES
    ('gleif', 'GLEIF', 'registry', 'gleif_company_raw_inputs',
     'source_pull', 'source_process', true, 'interval', '24h', '{}'::jsonb),
    ('companies_house', 'UK Companies House', 'registry', 'companies_house_company_raw_inputs',
     'source_pull', 'source_process', true, 'interval', '24h', '{}'::jsonb),
    ('brreg', 'Brreg', 'registry', 'brreg_workflow.raw_records',
     'brreg_bulk_ingest', NULL, true, 'interval', '24h', '{}'::jsonb),
    ('ai_company_profile', 'AI Company Profile', 'ai_research', 'ai_company_profile_raw_inputs',
     'ai_company_profile_pull', 'source_process', false, 'manual', NULL, '{}'::jsonb),
    ('nvd_cpe', 'NVD CPE', 'security_identifier', 'cpe_dictionary',
     'nvd_cpe_sync', 'nvd_cpe_process', false, 'interval', '24h', '{}'::jsonb),
    ('nvd_cve', 'NVD CVE', 'security_identifier', 'nvds',
     'nvd_cve_sync', 'nvd_cve_process', false, 'interval', '6h', '{}'::jsonb)
ON CONFLICT (name) DO UPDATE SET
    display_name = EXCLUDED.display_name,
    source_group = EXCLUDED.source_group,
    input_table_name = EXCLUDED.input_table_name,
    pull_task_type = EXCLUDED.pull_task_type,
    processor_task_type = EXCLUDED.processor_task_type,
    enabled = EXCLUDED.enabled,
    schedule_kind = EXCLUDED.schedule_kind,
    schedule_expression = EXCLUDED.schedule_expression,
    config = EXCLUDED.config,
    updated_at = now();

-- Recreate v_companies pointing at the new data_sources table.
-- v_company_sources is not recreated — it referenced company_sources which is now dropped.
CREATE VIEW v_companies AS
SELECT
    c.id,
    c.name,
    c.registration_number,
    c.lei,
    c.status,
    c.created_at,
    c.updated_at,
    co.id          AS country_id,
    co.name        AS country_name,
    co.iso_alpha2  AS country_iso2,
    ds.name        AS primary_source,
    ds.display_name AS primary_source_display_name,
    (SELECT COUNT(*)::int FROM company_domains cd WHERE cd.company_id = c.id) AS domain_count
FROM companies c
JOIN countries co ON co.id = c.country_id
LEFT JOIN data_sources ds ON ds.id = c.primary_source_id;

GRANT SELECT ON v_companies TO corpscout_anon;
