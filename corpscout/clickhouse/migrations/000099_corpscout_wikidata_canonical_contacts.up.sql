CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.wikidata_company_contacts
(
    country_iso2      LowCardinality(String),
    source_slug       LowCardinality(String),
    source_run_id     String,
    source_record_id  String,
    registry_id       String,
    contact_type      LowCardinality(String),
    contact_type_raw  LowCardinality(String),
    contact_value     String,
    source_field      LowCardinality(String),
    is_current        UInt8,
    valid_to          Nullable(Date),
    source_url        String,
    resolved_at       DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (registry_id, contact_type, contact_value);

CREATE TABLE IF NOT EXISTS corpscout.wikidata_company_domains
(
    country_iso2           LowCardinality(String),
    source_slug            LowCardinality(String),
    source_run_id          String,
    source_record_id       String,
    registry_id            String,
    domain                 String,
    domain_source          LowCardinality(String),
    validation_method      LowCardinality(String),
    confidence             Float32,
    website_url            String,
    website_normalized_url String,
    website_host           String,
    is_current             UInt8,
    is_primary             UInt8,
    resolved_at            DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (registry_id, domain);

-- Backfill from wikidata_company_websites (LEFT JOIN wikidata_companies for country).
-- Task 4's wikidata derivation asset keeps these exact SELECTs in lock-step. Facts are
-- validation-independent: every website row becomes a contact fact, confidence-free,
-- is_current=1 always (wikidata carries no current/historical distinction here).
INSERT INTO corpscout.wikidata_company_contacts (country_iso2, source_slug, source_run_id, source_record_id, registry_id, contact_type, contact_type_raw, contact_value, source_field, is_current, valid_to, source_url, resolved_at)
SELECT ifNull(companies.headquarters_country_iso2, ''), 'wikidata', websites.source_run_id, websites.source_record_id, websites.wikidata_id, 'website', '', websites.website_url, 'official_website', 1, NULL, '', now64(3, 'UTC')
FROM corpscout.wikidata_company_websites AS websites
LEFT JOIN corpscout.wikidata_companies AS companies ON companies.wikidata_id = websites.wikidata_id;

-- Domains dedupe to one row per (wikidata_id, domain) via domain_rn = 1, then elect
-- exactly one primary per wikidata_id via rn = 1. rn is computed over ALL rows (before
-- the domain_rn dedup filter) but is guaranteed to survive it: rn = 1 is the minimal
-- (length(root_domain), root_domain, website_normalized_url) row for a wikidata_id,
-- which is necessarily also the minimal website_normalized_url row within its own
-- (wikidata_id, root_domain) group — i.e. domain_rn = 1 for that same row. Verified
-- live: countIf(rn=1) == countIf(rn=1 AND domain_rn=1), zero rn=1 rows lost to the
-- domain_rn filter (see task-2-report.md).
INSERT INTO corpscout.wikidata_company_domains (country_iso2, source_slug, source_run_id, source_record_id, registry_id, domain, domain_source, validation_method, confidence, website_url, website_normalized_url, website_host, is_current, is_primary, resolved_at)
SELECT country_iso2, 'wikidata', source_run_id, source_record_id, registry_id, domain, 'website', '', 1.0, website_url, website_normalized_url, website_host, 1, if(rn = 1, 1, 0), now64(3, 'UTC')
FROM (
    SELECT
        ifNull(companies.headquarters_country_iso2, '') AS country_iso2,
        websites.source_run_id AS source_run_id,
        websites.source_record_id AS source_record_id,
        websites.wikidata_id AS registry_id,
        websites.root_domain AS domain,
        websites.website_url AS website_url,
        websites.website_normalized_url AS website_normalized_url,
        websites.website_host AS website_host,
        row_number() OVER (PARTITION BY websites.wikidata_id ORDER BY length(websites.root_domain), websites.root_domain, websites.website_normalized_url) AS rn,
        row_number() OVER (PARTITION BY websites.wikidata_id, websites.root_domain ORDER BY websites.website_normalized_url) AS domain_rn
    FROM corpscout.wikidata_company_websites AS websites
    LEFT JOIN corpscout.wikidata_companies AS companies ON companies.wikidata_id = websites.wikidata_id
    WHERE nullIf(trim(websites.root_domain), '') IS NOT NULL
)
WHERE domain_rn = 1;
