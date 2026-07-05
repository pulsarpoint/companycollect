CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.no_company_contacts
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

CREATE TABLE IF NOT EXISTS corpscout.no_company_domains
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

-- Backfill from no_websites (Task 3's norway_website_contacts/norway_website_domains
-- derivation assets keep these exact SELECTs in lock-step). no_websites' root_domain
-- column is a non-nullable String, so no ifNull guard is needed here (contrast with
-- Finland's Nullable root_domain in 000098).
INSERT INTO corpscout.no_company_contacts (country_iso2, source_slug, source_run_id, source_record_id, registry_id, contact_type, contact_type_raw, contact_value, source_field, is_current, valid_to, source_url, resolved_at)
SELECT 'NO', 'norway_brreg', source_run_id, source_record_id, org_number, 'website', '', website_url, 'hjemmeside', is_current, ended_on, '', now64(3, 'UTC')
FROM corpscout.no_websites;

INSERT INTO corpscout.no_company_domains (country_iso2, source_slug, source_run_id, source_record_id, registry_id, domain, domain_source, validation_method, confidence, website_url, website_normalized_url, website_host, is_current, is_primary, resolved_at)
SELECT 'NO', 'norway_brreg', source_run_id, source_record_id, org_number, root_domain, 'website', '', 1.0, website_url, website_normalized_url, website_host, is_current, is_primary, now64(3, 'UTC')
FROM corpscout.no_websites
WHERE nullIf(trim(root_domain), '') IS NOT NULL;
