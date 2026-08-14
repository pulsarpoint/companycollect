CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.se_addresses_current
(
    address_id FixedString(64),
    canonical_display_address String,
    representative_address_source LowCardinality(String),
    street_address String,
    postal_code String,
    post_town String,
    country_code LowCardinality(String),
    address_kind LowCardinality(String),
    normalized_street String,
    normalized_postal_code String,
    normalized_post_town String,
    address_types Array(String),
    address_sources Array(String),
    company_count UInt32,
    evidence_count UInt64,
    first_observed_at DateTime64(3, 'UTC'),
    last_observed_at DateTime64(3, 'UTC'),
    address_identity_run_id String,
    address_identity_built_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY address_id;

CREATE TABLE IF NOT EXISTS corpscout.se_company_address_links_current
(
    company_id String,
    address_id FixedString(64),
    canonical_address_key FixedString(64),
    address_types Array(String),
    address_sources Array(String),
    evidence_count UInt32,
    first_observed_at DateTime64(3, 'UTC'),
    last_observed_at DateTime64(3, 'UTC'),
    review_status LowCardinality(String) DEFAULT 'unreviewed',
    reviewed_at Nullable(DateTime64(3, 'UTC')),
    reviewed_by String DEFAULT '',
    review_note String DEFAULT '',
    address_identity_run_id String,
    address_identity_built_at DateTime64(3, 'UTC'),
    PROJECTION by_address
    (
        SELECT
            company_id,
            address_id
        ORDER BY (address_id, company_id)
    )
)
ENGINE = MergeTree
ORDER BY (company_id, address_id);
