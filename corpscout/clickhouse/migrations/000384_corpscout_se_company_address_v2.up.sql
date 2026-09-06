CREATE DATABASE IF NOT EXISTS corpscout;

-- Published addresses (spec section 3.3): one row per company and published address,
-- written by the fold. Built as se_company_address_v2 because the old final table keeps
-- the name se_company_address until the cutover renames both.
CREATE TABLE IF NOT EXISTS corpscout.se_company_address_v2
(
    company_id String,
    address_key FixedString(64),
    care_of Nullable(String),
    box Nullable(String),
    street_name Nullable(String),
    house_number Nullable(String),
    unit Nullable(String),
    postal_code Nullable(String),
    city Nullable(String),
    country_code LowCardinality(String),
    normalized_address String,
    kinds Array(LowCardinality(String)),
    sources Array(LowCardinality(String)),
    slots Array(String),
    normalized_ids Array(FixedString(64)),
    text_source LowCardinality(String),
    active UInt8,
    inactive_reason LowCardinality(String),
    latitude Nullable(Float64),
    longitude Nullable(Float64),
    geocode_status LowCardinality(String),
    geocode_method LowCardinality(String),
    geocode_confidence Nullable(Float64),
    geocode_precision LowCardinality(String),
    geocode_policy LowCardinality(String),
    geocode_reference String,
    geocoded_at Nullable(DateTime64(3, 'UTC')),
    normalizer_version LowCardinality(String),
    folded_at DateTime64(3, 'UTC'),
    fold_version LowCardinality(String),
    source_run_id String,
    CONSTRAINT valid_company_id CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$')
)
ENGINE = ReplacingMergeTree(folded_at)
ORDER BY (company_id, address_key);
