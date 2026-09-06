CREATE DATABASE IF NOT EXISTS corpscout;

-- Per-company address decisions (spec section 3.5): a hide rule keyed by the published
-- address, released by a later version with removed = 1.
CREATE TABLE IF NOT EXISTS corpscout.se_company_address_rule
(
    company_id String,
    address_key FixedString(64),
    action LowCardinality(String),
    removed UInt8 DEFAULT 0,
    decided_by LowCardinality(String) DEFAULT '',
    note String DEFAULT '',
    decided_at DateTime64(3, 'UTC'),
    CONSTRAINT valid_company_id CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$')
)
ENGINE = ReplacingMergeTree(decided_at)
ORDER BY (company_id, address_key, action);
