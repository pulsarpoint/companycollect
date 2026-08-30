CREATE DATABASE IF NOT EXISTS corpscout;

-- DNS-derived technology detections: one row per (domain, technology, signal,
-- pattern, evidence), produced by joining corpscout.technology_fingerprints
-- (plus the pattern-free self-hosted-email rule) against
-- corpscout.commoncrawl_domain_dns_records. The complement of the page-based
-- commoncrawl_page_technologies -- DNS reveals paid B2B SaaS (email platform,
-- gateways, DNS/hosting providers) that page content never shows.
--
-- `evidence` is the record value that matched (for example the MX host) and
-- `matched_pattern` the fingerprint regex -- kept so serving pages can show WHY
-- a technology was detected. `signal_type` mirrors technology_fingerprints
-- ('dns_mx', 'dns_txt', ...) plus rule-based values ('self_hosted_email').
--
-- Filled full-refresh by the dagster technology_catalog module via stage +
-- EXCHANGE TABLES with a row floor, weekly after the fingerprint publish. The
-- adoption/companies/top-domains rollups union this table with the page-based
-- detections. Evidence sits in the sort key so distinct records of one domain
-- and technology survive as separate rows. The migration owns this schema.
CREATE TABLE IF NOT EXISTS corpscout.domain_signal_technologies
(
    root_domain String,
    technology String,
    signal_type LowCardinality(String),
    matched_pattern String,
    evidence String,
    record_name String,
    confidence UInt8,
    source LowCardinality(String),
    source_run_id String,
    detected_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(detected_at)
ORDER BY (root_domain, technology, signal_type, matched_pattern, evidence);
