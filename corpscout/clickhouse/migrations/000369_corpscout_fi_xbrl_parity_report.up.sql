CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.fi_xbrl_parity_report
(
    document_key String,
    status LowCardinality(String),
    old_fact_count UInt32,
    new_fact_count UInt32,
    value_mismatches UInt32,
    missing_in_new UInt32,
    missing_in_old UInt32,
    details String,
    compared_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(compared_at)
ORDER BY (document_key);
