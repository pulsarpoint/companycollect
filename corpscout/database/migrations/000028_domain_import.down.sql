-- database/migrations/000028_domain_import.down.sql

-- Restore v_domains without import_source
DROP VIEW IF EXISTS v_domains;
CREATE VIEW v_domains AS
SELECT
    d.id,
    d.domain,
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
GROUP BY d.id, d.domain, d.first_seen_at, d.last_verified_at;

-- Remove manual_upload from signal check
ALTER TABLE company_domains
    DROP CONSTRAINT company_domains_signal_check,
    ADD CONSTRAINT company_domains_signal_check
        CHECK (signal IN ('registry_website','wikidata','certsh','whois','search'));

DROP TABLE IF EXISTS domain_import_batches;

ALTER TABLE domains DROP COLUMN IF EXISTS import_source;
