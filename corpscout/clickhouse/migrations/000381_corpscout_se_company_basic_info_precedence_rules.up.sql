CREATE DATABASE IF NOT EXISTS corpscout;

-- Slice 3b (2026-09-05): the precedence table gains a company scope so a reviewer
-- decision is a rule for one company, not a copied value. The old table held only the
-- 30 rows the code exports, so it is dropped and recreated -- re-run
-- se_company_basic_info_precedence_clickhouse after applying this migration.
DROP TABLE IF EXISTS corpscout.se_company_basic_info_precedence;

CREATE TABLE IF NOT EXISTS corpscout.se_company_basic_info_precedence
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
