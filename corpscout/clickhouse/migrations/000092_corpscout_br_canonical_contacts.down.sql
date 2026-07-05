CREATE DATABASE IF NOT EXISTS corpscout;

DROP TABLE IF EXISTS corpscout.br_company_contacts;
DROP TABLE IF EXISTS corpscout.br_company_domains;

CREATE TABLE IF NOT EXISTS corpscout.br_company_contact_info
(
    country_iso2 LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    source_record_id String,
    cnpj String,
    cnpj_basico String,
    contact_type LowCardinality(String),
    contact_type_en LowCardinality(String),
    contact_area_code LowCardinality(String),
    contact_value String,
    is_current UInt8,
    root_domain String,
    domain_source LowCardinality(String),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (cnpj_basico, cnpj, contact_type, contact_value);
