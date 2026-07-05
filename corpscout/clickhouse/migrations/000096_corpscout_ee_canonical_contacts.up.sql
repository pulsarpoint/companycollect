CREATE DATABASE IF NOT EXISTS corpscout;

-- Data-preserving reshape of ee_company_contacts / ee_company_domains onto the
-- canonical company_contacts / company_domains standard (see 000088 for cz's
-- drop+recreate precedent). Unlike cz/lv/br, ee_company_domains is LIVE-CONSUMED
-- by the domain graph, so this uses the shadow-table + INSERT SELECT + EXCHANGE
-- TABLES pattern from 000014 (fi_names order-key fix) instead of drop+recreate --
-- existing rows survive the reshape under new column names/vocabulary.

DROP TABLE IF EXISTS corpscout.ee_company_contacts__canonical;

CREATE TABLE IF NOT EXISTS corpscout.ee_company_contacts__canonical
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

-- The multiIf below deliberately has NO 'AS contact_type' alias: a ClickHouse
-- expression alias shadows the same-named source column everywhere else in the
-- SELECT list, which would make the bare 'contact_type' on the next line (the
-- contact_type_raw value) resolve to the mapped alias instead of the raw source
-- column. Positional INSERT SELECT needs no aliases at all.
INSERT INTO corpscout.ee_company_contacts__canonical
SELECT
    country_iso2,
    source_slug,
    source_run_id,
    source_record_id,
    reg_code,
    multiIf(
        contact_type = 'WWW', 'website',
        contact_type = 'EMAIL', 'email',
        contact_type = 'TEL', 'phone',
        contact_type = 'MOB', 'mobile',
        contact_type = 'FAX', 'fax',
        'other'
    ),
    contact_type,
    contact_value,
    'sidevahendid',
    is_current,
    end_date,
    source_url,
    now64(3, 'UTC')
FROM corpscout.ee_company_contacts;

EXCHANGE TABLES corpscout.ee_company_contacts__canonical AND corpscout.ee_company_contacts;

DROP TABLE IF EXISTS corpscout.ee_company_contacts__canonical;

DROP TABLE IF EXISTS corpscout.ee_company_domains__canonical;

CREATE TABLE IF NOT EXISTS corpscout.ee_company_domains__canonical
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

INSERT INTO corpscout.ee_company_domains__canonical
SELECT
    country_iso2,
    source_slug,
    source_run_id,
    source_record_id,
    reg_code AS registry_id,
    domain,
    domain_source,
    '' AS validation_method,
    multiIf(domain_source = 'website', 1.0, 0.9) AS confidence,
    website_url,
    website_normalized_url,
    website_host,
    is_current,
    is_primary,
    resolved_at
FROM corpscout.ee_company_domains;

EXCHANGE TABLES corpscout.ee_company_domains__canonical AND corpscout.ee_company_domains;

DROP TABLE IF EXISTS corpscout.ee_company_domains__canonical;
