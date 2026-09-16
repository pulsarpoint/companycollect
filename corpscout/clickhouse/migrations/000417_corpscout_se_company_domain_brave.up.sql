CREATE DATABASE IF NOT EXISTS corpscout;

-- Derived current JSON list and durable response checkpoint, including empty lists.
-- Suggestions must be acknowledged before this checkpoint advances.
CREATE TABLE IF NOT EXISTS corpscout.se_company_domain_brave_extraction
(
    company_id String,
    brave_result_id String,
    answer_hash String,
    domains_json String,
    extractor_version String,
    processed_at DateTime64(6, 'UTC'),
    source_run_id String,
    CONSTRAINT valid_company_id CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$'),
    CONSTRAINT valid_domains_json CHECK isValidJSON(domains_json) AND JSONType(domains_json) = 'Array'
)
ENGINE = ReplacingMergeTree(processed_at)
ORDER BY company_id;

ALTER TABLE corpscout.se_company_domain
    ADD COLUMN IF NOT EXISTS supporting_sources Array(String)
    DEFAULT arraySort(arrayDistinct(arrayFilter(source -> source IN ('brave','wikidata','esef_filing','common_crawl_identity'),sources)));
ALTER TABLE corpscout.se_company_domain_history
    ADD COLUMN IF NOT EXISTS supporting_sources Array(String)
    DEFAULT arraySort(arrayDistinct(arrayFilter(source -> source IN ('brave','wikidata','esef_filing','common_crawl_identity'),sources)))
    AFTER source_run_id;

-- Keep reviewer overlays and the country reader shape, exposing distinct support.
CREATE OR REPLACE VIEW corpscout.company_domains_resolved AS
WITH reviewed AS (
    SELECT
        'SE' AS country_code, d.company_id AS company_id, d.root_domain AS root_domain,
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
    country_code, company_id, root_domain, website_url, website_host, source_names,
    source_confidences, source_record_ids, source_urls, confidence_bases, suggested_confidence,
    toUInt8(is_active AND row_number() OVER (
        PARTITION BY company_id ORDER BY is_active DESC, review_status = 'confirmed_primary' DESC,
        base_primary DESC, suggested_confidence DESC, root_domain
    ) = 1) AS suggested_primary,
    evidence_fingerprint, review_status, review_note, reviewed_by, reviewed_at,
    reviewed_evidence_fingerprint, is_active, first_seen_at, last_seen_at, resolved_at, supporting_sources
FROM reviewed
UNION ALL
SELECT country_code, company_id, root_domain, website_url, website_host, source_names,
    source_confidences, source_record_ids, source_urls, confidence_bases, suggested_confidence,
    suggested_primary, evidence_fingerprint, review_status, review_note, reviewed_by, reviewed_at,
    reviewed_evidence_fingerprint, is_active, first_seen_at, last_seen_at, resolved_at,
    arraySort(arrayDistinct(arrayFilter(source -> source IN ('brave','wikidata','esef_filing','common_crawl_identity'),source_names))) AS supporting_sources
FROM corpscout.company_domains FINAL WHERE country_code != 'SE';
