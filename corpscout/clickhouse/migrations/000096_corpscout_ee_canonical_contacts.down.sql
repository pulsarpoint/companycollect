CREATE DATABASE IF NOT EXISTS corpscout;

DROP TABLE IF EXISTS corpscout.ee_company_contacts__canonical;
DROP TABLE IF EXISTS corpscout.ee_company_contacts__legacy;
DROP TABLE IF EXISTS corpscout.ee_company_domains__canonical;
DROP TABLE IF EXISTS corpscout.ee_company_domains__legacy;

-- Reverse-mapping to the pre-000096 shapes (000027 + 000028's ALTER'd domain/
-- domain_source columns, unversioned ReplacingMergeTree, 000029 verbatim).
-- Only what's reversible round-trips: registry_id -> reg_code, contact_type_raw
-- -> contact_type, contact_type_en rebuilt from the raw code, valid_to ->
-- end_date. The domain/domain_source name-embedded-contact enrichment on
-- ee_company_contacts is honestly lost here (it refills on the next monthly
-- run) and domains' validation_method/confidence are simply dropped.

CREATE TABLE corpscout.ee_company_contacts__legacy
(
    country_iso2 LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    source_record_id String,
    reg_code String,
    contact_type LowCardinality(String),
    contact_type_en LowCardinality(String),
    contact_value String,
    is_current UInt8,
    end_date Nullable(Date),
    source_url String,
    domain String,
    domain_source LowCardinality(String)
)
ENGINE = ReplacingMergeTree
ORDER BY (reg_code, contact_type, contact_value);

INSERT INTO corpscout.ee_company_contacts__legacy
SELECT
    country_iso2,
    source_slug,
    source_run_id,
    source_record_id,
    registry_id AS reg_code,
    contact_type_raw AS contact_type,
    multiIf(
        contact_type_raw = 'WWW', 'Website',
        contact_type_raw = 'EMAIL', 'Email',
        contact_type_raw = 'MOB', 'Mobile',
        contact_type_raw = 'TEL', 'Phone',
        contact_type_raw = 'FAX', 'Fax',
        contact_type_raw = 'MUU', 'Other',
        ''
    ) AS contact_type_en,
    contact_value,
    is_current,
    valid_to AS end_date,
    source_url,
    '' AS domain,
    '' AS domain_source
FROM corpscout.ee_company_contacts;

EXCHANGE TABLES corpscout.ee_company_contacts__legacy AND corpscout.ee_company_contacts;

DROP TABLE IF EXISTS corpscout.ee_company_contacts__legacy;

CREATE TABLE corpscout.ee_company_domains__legacy
(
    country_iso2 LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    source_record_id String,
    reg_code String,
    domain String,
    domain_source LowCardinality(String),
    website_url String,
    website_normalized_url String,
    website_host String,
    is_current UInt8,
    is_primary UInt8,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (reg_code, domain);

INSERT INTO corpscout.ee_company_domains__legacy
SELECT
    country_iso2,
    source_slug,
    source_run_id,
    source_record_id,
    registry_id AS reg_code,
    domain,
    domain_source,
    website_url,
    website_normalized_url,
    website_host,
    is_current,
    is_primary,
    resolved_at
FROM corpscout.ee_company_domains;

EXCHANGE TABLES corpscout.ee_company_domains__legacy AND corpscout.ee_company_domains;

DROP TABLE IF EXISTS corpscout.ee_company_domains__legacy;
