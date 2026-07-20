CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.fi_company_addresses
(
    country_iso2      LowCardinality(String),
    source_slug       LowCardinality(String),
    source_run_id     String,
    source_record_id  String,
    registry_id       String,
    address_type      LowCardinality(String),
    address_lines     Nullable(String),
    postal_code       Nullable(String),
    city              Nullable(String),
    municipality      Nullable(String),
    municipality_code Nullable(String),
    country           Nullable(String),
    country_code      LowCardinality(Nullable(String)),
    registered_on     Nullable(Date),
    source_code       LowCardinality(Nullable(String)),
    source_field      LowCardinality(String),
    is_current        UInt8,
    source_url        String,
    resolved_at       DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (registry_id, address_type);
