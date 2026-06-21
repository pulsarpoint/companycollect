CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.fr_companies
(
    country_iso2 LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    source_record_id String,
    siren String,
    name String,
    denomination_original String,
    acronym String,
    legal_form_code LowCardinality(String),
    legal_form_en LowCardinality(String),
    status_code LowCardinality(String),
    status_en LowCardinality(String),
    is_active UInt8,
    creation_date Nullable(Date),
    enterprise_category LowCardinality(String),
    is_social_solidarity_economy UInt8,
    naf_code LowCardinality(String),
    naf_nomenclature LowCardinality(String),
    source_url String
)
ENGINE = ReplacingMergeTree
ORDER BY (siren);
