-- PostgREST read-only role
CREATE ROLE corpscout_anon NOLOGIN;
GRANT USAGE ON SCHEMA public TO corpscout_anon;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO corpscout_anon;
GRANT corpscout_anon TO corpscout;

-- ── v_companies ─────────────────────────────────────────────────────────────
-- Flat view of companies with country and source info for list/filter pages.
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

-- ── v_company_sources ────────────────────────────────────────────────────────
-- All source links for a company — used on company detail to show how the
-- company was discovered.
CREATE VIEW v_company_sources AS
SELECT
    cs.company_id,
    cs.external_id,
    cs.fetched_at,
    ds.id           AS source_id,
    ds.name         AS source_name,
    ds.display_name AS source_display_name,
    ds.source_type
FROM company_sources cs
JOIN data_sources ds ON ds.id = cs.source_id;

-- ── v_company_domains ────────────────────────────────────────────────────────
-- Domain links for a company, including the domain string.
CREATE VIEW v_company_domains AS
SELECT
    cd.id,
    cd.company_id,
    cd.domain_id,
    d.domain,
    cd.relationship_type,
    cd.status,
    cd.signal,
    cd.confidence,
    cd.evidence,
    cd.first_seen_at,
    cd.last_seen_at
FROM company_domains cd
JOIN domains d ON d.id = cd.domain_id;

-- ── v_domains ────────────────────────────────────────────────────────────────
-- Domains with aggregated company linkage info. company_count = 0 means the
-- domain has no associated company (orphaned).
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
    ) AS primary_signal
FROM domains d
LEFT JOIN company_domains cd ON cd.domain_id = d.id
GROUP BY d.id, d.domain, d.first_seen_at, d.last_verified_at;

-- Grant SELECT on the new views to the anon role
GRANT SELECT ON v_companies TO corpscout_anon;
GRANT SELECT ON v_company_sources TO corpscout_anon;
GRANT SELECT ON v_company_domains TO corpscout_anon;
GRANT SELECT ON v_domains TO corpscout_anon;
