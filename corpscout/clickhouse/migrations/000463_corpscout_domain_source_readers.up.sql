CREATE DATABASE IF NOT EXISTS corpscout;

CREATE OR REPLACE VIEW corpscout.se_company_domain_resolved AS
WITH reviewed AS (
    SELECT
        'SE' AS country_code, d.company_id AS company_id, d.root_domain AS root_domain,
        d.domain_id AS domain_id,
        d.website_url AS website_url, d.website_host AS website_host,
        d.supporting_sources AS supporting_sources, d.sources AS source_names, arrayMap(value -> toFloat32(value), d.source_confidences) AS source_confidences,
        d.source_record_ids AS source_record_ids, d.source_urls AS source_urls,
        d.confidence_bases AS confidence_bases, toFloat32(d.confidence) AS suggested_confidence,
        d.is_primary AS base_primary, d.evidence_hash AS evidence_fingerprint,
        if(r.company_id != '', if(r.removed, 'unreviewed', r.action), d.review_status) AS review_status,
        if(r.company_id != '', r.note, d.review_note) AS review_note,
        if(r.company_id != '', r.decided_by, d.reviewed_by) AS reviewed_by,
        if(r.company_id != '', r.decided_at, d.reviewed_at) AS reviewed_at,
        if(r.company_id != '', r.evidence_hash, d.reviewed_evidence_hash) AS reviewed_evidence_fingerprint,
        toUInt8(multiIf(
            r.company_id != '' AND r.removed = 0 AND r.action IN ('confirmed_primary', 'confirmed_related'), 1,
            r.company_id != '' AND r.removed = 0 AND r.action = 'rejected', 0,
            r.company_id != '' AND r.removed = 1 AND d.association_source = 'reviewer', 0,
            d.active
        )) AS is_active,
        d.first_seen_at AS first_seen_at, d.last_seen_at AS last_seen_at,
        greatest(d.folded_at, ifNull(r.decided_at, d.folded_at)) AS resolved_at
    FROM corpscout.se_company_domain AS d FINAL
    LEFT JOIN corpscout.se_company_domain_rule AS r FINAL
        ON r.company_id = d.company_id AND r.root_domain = d.root_domain
)
SELECT
    country_code, company_id, domain_id, root_domain, website_url, website_host, source_names,
    source_confidences, source_record_ids, source_urls, confidence_bases, suggested_confidence,
    toUInt8(is_active AND row_number() OVER (
        PARTITION BY company_id ORDER BY is_active DESC, review_status = 'confirmed_primary' DESC,
        base_primary DESC, suggested_confidence DESC, root_domain
    ) = 1) AS suggested_primary,
    evidence_fingerprint, review_status, review_note, reviewed_by, reviewed_at,
    reviewed_evidence_fingerprint, is_active, first_seen_at, last_seen_at, resolved_at, supporting_sources
FROM reviewed;

-- Precomputed company membership for domain filters. This is a disposable read
-- model: domains owns identity and domains_sources indexes association provenance.
-- Refresh replaces the snapshot atomically and reflects changes on either side.
CREATE MATERIALIZED VIEW IF NOT EXISTS corpscout.domains_company_filter
REFRESH EVERY 5 MINUTE
ENGINE = MergeTree ORDER BY root_domain
EMPTY
AS
WITH associations AS (
    SELECT domain_id, country_code, company_id
    FROM corpscout.domains_sources FINAL
    WHERE source_table NOT IN ('commoncrawl_domains', 'commoncrawl_domain_graph_nodes')
        AND country_code != '' AND company_id != ''
        AND is_active = 1 AND association = 'connected'
)
SELECT d.root_domain AS root_domain, d.domain_id AS domain_id,
    toUInt32(uniqExact((s.country_code, s.company_id))) AS company_count,
    now64(3, 'UTC') AS refreshed_at
FROM (
    SELECT domain_id, root_domain FROM corpscout.domains
    WHERE domain_id IN (SELECT domain_id FROM associations)
) AS d
INNER JOIN associations AS s ON d.domain_id = s.domain_id
GROUP BY d.root_domain, d.domain_id
SETTINGS max_threads = 4, max_memory_usage = 4294967296,
    max_bytes_before_external_group_by = 268435456, max_execution_time = 300;

SYSTEM REFRESH VIEW corpscout.domains_company_filter;
SYSTEM WAIT VIEW corpscout.domains_company_filter;
