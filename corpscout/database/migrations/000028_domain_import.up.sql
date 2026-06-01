-- database/migrations/000028_domain_import.up.sql

-- 1. Add import_source to domains (existing rows default to 'crawler')
ALTER TABLE domains
    ADD COLUMN import_source TEXT NOT NULL DEFAULT 'crawler'
    CONSTRAINT domains_import_source_check CHECK (import_source IN ('crawler', 'manual_upload'));

-- 2. Track CSV upload batches
CREATE TABLE domain_import_batches (
    id            UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    filename      TEXT        NOT NULL,
    csv_s3_key    TEXT        NOT NULL,
    status        TEXT        NOT NULL DEFAULT 'pending'
                  CHECK (status IN ('pending', 'processing', 'completed', 'failed')),
    rows_total    INTEGER     NOT NULL DEFAULT 0,
    rows_imported INTEGER     NOT NULL DEFAULT 0,
    rows_skipped  INTEGER     NOT NULL DEFAULT 0,
    rows_failed   INTEGER     NOT NULL DEFAULT 0,
    error_message TEXT,
    river_job_id  BIGINT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at  TIMESTAMPTZ
);

CREATE INDEX idx_domain_import_batches_created_desc ON domain_import_batches(created_at DESC);
CREATE INDEX idx_domain_import_batches_river_job ON domain_import_batches(river_job_id) WHERE river_job_id IS NOT NULL;

-- 3. Extend signal enum on company_domains to allow manual uploads
ALTER TABLE company_domains
    DROP CONSTRAINT company_domains_signal_check,
    ADD CONSTRAINT company_domains_signal_check
        CHECK (signal IN ('registry_website','wikidata','certsh','whois','search','manual_upload'));

-- 4. Update v_domains to expose import_source
-- Must DROP and recreate because we're inserting a column before existing ones
DROP VIEW IF EXISTS v_domains;
CREATE VIEW v_domains AS
SELECT
    d.id,
    d.domain,
    d.import_source,
    d.first_seen_at,
    d.last_verified_at,
    COUNT(DISTINCT cd.company_id)::int AS company_count,
    MAX(cd.confidence)                 AS max_confidence,
    (
        SELECT c2.name
        FROM company_domains cd2
        JOIN companies c2 ON c2.id = cd2.company_id
        WHERE cd2.domain_id = d.id
        ORDER BY cd2.confidence DESC
        LIMIT 1
    ) AS primary_company_name,
    (
        SELECT c2.id
        FROM company_domains cd2
        JOIN companies c2 ON c2.id = cd2.company_id
        WHERE cd2.domain_id = d.id
        ORDER BY cd2.confidence DESC
        LIMIT 1
    ) AS primary_company_id,
    (
        SELECT cd2.signal
        FROM company_domains cd2
        WHERE cd2.domain_id = d.id
        ORDER BY cd2.confidence DESC
        LIMIT 1
    ) AS primary_signal,
    (
        SELECT MAX(j.created_at)
        FROM domain_crawl_jobs j
        WHERE j.domain_id = d.id
    ) AS last_crawled_at,
    EXISTS (
        SELECT 1
        FROM domain_crawl_jobs j
        WHERE j.domain_id = d.id
    ) AS crawled
FROM domains d
LEFT JOIN company_domains cd ON cd.domain_id = d.id
GROUP BY d.id, d.domain, d.import_source, d.first_seen_at, d.last_verified_at;

GRANT SELECT ON v_domains TO corpscout_anon;
