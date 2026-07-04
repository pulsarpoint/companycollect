CREATE DATABASE IF NOT EXISTS corpscout;

DROP TABLE IF EXISTS corpscout.cz_company_contacts;

CREATE TABLE IF NOT EXISTS corpscout.cz_company_contacts
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

CREATE TABLE IF NOT EXISTS corpscout.cz_company_domains
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
