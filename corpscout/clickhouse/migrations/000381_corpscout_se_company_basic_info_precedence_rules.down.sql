CREATE DATABASE IF NOT EXISTS corpscout;

-- Rolling back loses every company rule the backoffice wrote (the reviewer's
-- decisions) -- only the 30 global rows are regenerable by the export asset.
DROP TABLE IF EXISTS corpscout.se_company_basic_info_precedence;

CREATE TABLE IF NOT EXISTS corpscout.se_company_basic_info_precedence
(
    field LowCardinality(String),
    source LowCardinality(String),
    precedence UInt32,
    exported_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(exported_at)
ORDER BY (field, source);
