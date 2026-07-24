CREATE DATABASE IF NOT EXISTS corpscout;

-- Cross-source ISIN to issuer LEI identity mapping. This is a derived
-- reconciliation table, not a source table: each contributing source keeps its
-- own physical table and is projected into this one.
--
-- The grain is (isin, lei, mapping_source). It is deliberately NOT one row per
-- ISIN: an ISIN may resolve to more than one LEI, and ambiguity is resolved by
-- consumers rather than discarded here.
--
-- The mapping is issuer identity, NOT a listing signal. venue_confirmed = 1
-- means an admission record exists somewhere at some point in time. It does not
-- mean the instrument is currently traded. Current trading status stays in
-- corpscout.firds_instruments_current.
CREATE TABLE IF NOT EXISTS corpscout.isin_lei
(
    isin                         String,
    lei                          String,
    mapping_source               LowCardinality(String),
    venue_confirmed              UInt8,
    cfi_category                 LowCardinality(String),
    first_seen_date              Date,
    last_seen_date               Date,
    source_run_id                String,
    resolved_at                  DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (isin, lei, mapping_source);
