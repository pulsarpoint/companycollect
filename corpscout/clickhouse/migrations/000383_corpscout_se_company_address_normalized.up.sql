CREATE DATABASE IF NOT EXISTS corpscout;

-- Normalized address suggestions (spec section 3.2): the same key and row count as the raw
-- table, written only by the normalize asset. normalized_id names this version and
-- suggestion_id the raw version it was computed from.
CREATE TABLE IF NOT EXISTS corpscout.se_company_address_normalized
(
    company_id String,
    source LowCardinality(String),
    slot String,
    normalized_id FixedString(64),
    suggestion_id FixedString(64),
    suggested_at DateTime64(3, 'UTC'),
    kind LowCardinality(String),
    care_of Nullable(String),
    box Nullable(String),
    street_name Nullable(String),
    house_number Nullable(String),
    unit Nullable(String),
    postal_code Nullable(String),
    city Nullable(String),
    country_code LowCardinality(String),
    normalized_address String,
    address_key FixedString(64),
    parse_status LowCardinality(String),
    parse_notes String,
    normalizer_version LowCardinality(String),
    normalized_at DateTime64(3, 'UTC'),
    CONSTRAINT valid_company_id CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$')
)
ENGINE = ReplacingMergeTree(normalized_at)
ORDER BY (company_id, source, slot);
