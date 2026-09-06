CREATE DATABASE IF NOT EXISTS corpscout;

-- Address source precedence (spec section 3.6): the basic-info shape. company_id '' rows
-- are the global order exported from code, a spelling tie-break only, never a filter.
CREATE TABLE IF NOT EXISTS corpscout.se_company_address_precedence
(
    company_id String,
    field LowCardinality(String),
    source LowCardinality(String),
    precedence UInt32,
    removed UInt8 DEFAULT 0,
    decided_by LowCardinality(String) DEFAULT '',
    note String DEFAULT '',
    decided_at DateTime64(3, 'UTC'),
    CONSTRAINT valid_company_id CHECK company_id = '' OR match(company_id, '^([0-9]{10}|[0-9]{12})$')
)
ENGINE = ReplacingMergeTree(decided_at)
ORDER BY (company_id, field, source);
