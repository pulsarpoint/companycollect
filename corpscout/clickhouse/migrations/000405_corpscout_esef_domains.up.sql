CREATE DATABASE IF NOT EXISTS corpscout;

-- ESEF DOMAINS: the first independent ESEF extractor (spec 2026-09-13, owner ruling: documents
-- plus many extractors, each with its own table, version and re-run -- not one big versioned
-- parser). corpscout.esef_domains holds one row per (filing document, registrable domain) as
-- found in the archived report package by dagster_v3.defs.esef_filings.domains_extraction, or
-- one marker row (registrable_domain '') per document whose extraction found nothing, failed
-- or timed out, so the extractor never selects that document again at the same
-- extractor_version. A run replaces every row of the documents it attempted (stage table +
-- EXCHANGE TABLES) -- ReplacingMergeTree(extracted_at) only guards the unlikely overlap. The
-- website rows of corpscout.esef_document_contact_candidates stay until a later slice moves
-- the contact extraction -- company_serving and the backoffice read domains from here now.
CREATE TABLE IF NOT EXISTS corpscout.esef_domains
(
    domain_id FixedString(64),
    source_document_id String,
    source_record_uid String DEFAULT lower(hex(SHA256(concat('company-source-record-v1\nfile\nesef_report_package\n', lowerUTF8(toString(package_sha256)))))),
    package_sha256 String,
    lei String,
    period_end Date32,
    fiscal_year UInt16,
    extraction_status LowCardinality(String),
    registrable_domain String,
    hosts_json String,
    normalized_urls_json String,
    roles_json String,
    evidence_json String,
    evidence_count UInt32,
    corroborated UInt8,
    error_message String,
    extractor_version LowCardinality(String),
    source_run_id String,
    extracted_at DateTime64(3, 'UTC'),
    resolved_at DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(extracted_at)
ORDER BY (lei, source_document_id, registrable_domain);

-- Sweden's slice, rendered by dagster_v3.defs.esef_filings.country_views for the
-- SeEsefView("esef_domains") entry of tables.SE_ESEF_VIEWS -- NOT HAND-WRITTEN, pinned by
-- tests/test_esef_domains_tables.py.
CREATE OR REPLACE VIEW corpscout.se_esef_domains AS
SELECT
    m.registry_id AS company_id,
    t.domain_id,
    t.source_document_id,
    t.source_record_uid,
    t.package_sha256,
    t.lei,
    t.period_end,
    t.fiscal_year,
    t.extraction_status,
    t.registrable_domain,
    t.hosts_json,
    t.normalized_urls_json,
    t.roles_json,
    t.evidence_json,
    t.evidence_count,
    t.corroborated,
    t.error_message,
    t.extractor_version,
    t.source_run_id,
    t.extracted_at,
    t.resolved_at
FROM corpscout.esef_domains AS t FINAL
INNER JOIN (SELECT lei, registry_id FROM corpscout.esef_entity_registry_map FINAL WHERE country_iso2 = 'SE' AND link_status = 'register_verified') AS m ON m.lei = t.lei;
