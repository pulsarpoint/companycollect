CREATE DATABASE IF NOT EXISTS corpscout;

-- The per-field, per-source precedence of section 4 as exported from
-- dagster_v3.defs.se_company.basic_info.precedence by the
-- se_company_basic_info_precedence_clickhouse asset (2026-09-03 SE basic-info design,
-- section 3.5). Read by the backoffice for display and validation. Never edited here,
-- the Python dictionary is the only source. A re-export writes every pair the dictionary
-- names. A pair the dictionary no longer names stays in this table until it is removed by
-- hand, and the export reports it as stale_pairs.
CREATE TABLE IF NOT EXISTS corpscout.se_company_basic_info_precedence
(
    field LowCardinality(String),
    source LowCardinality(String),
    precedence UInt32,
    exported_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(exported_at)
ORDER BY (field, source);
