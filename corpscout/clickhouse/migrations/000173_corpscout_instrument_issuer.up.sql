CREATE DATABASE IF NOT EXISTS corpscout;

-- Replaces corpscout.isin_lei from migration 000171. Three changes force a new
-- table rather than an ALTER: lei becomes the (issuer_scheme, issuer_id) pair so
-- markets without LEI adoption use the same two tables and the same join,
-- venue_confirmed and cfi_category move to corpscout.instrument_venues where
-- venue facts belong, and the sort key gains a column in the middle, which
-- ClickHouse cannot ALTER.
--
-- isin_lei held 9,129,076 rows when this migration was written, so its contents
-- are carried forward rather than dropped. Every existing row is an LEI mapping,
-- so issuer_scheme is the constant 'lei' and lei maps straight onto issuer_id.
-- Copying costs one INSERT SELECT instead of re-scanning firds_instrument_events.
--
-- The grain keeps mapping_source so two sources disagreeing about an ISIN's
-- issuer stays visible instead of being silently resolved.
CREATE TABLE IF NOT EXISTS corpscout.instrument_issuer
(
    isin                         String,
    issuer_scheme                LowCardinality(String),
    issuer_id                    String,
    mapping_source               LowCardinality(String),
    first_seen_date              Date,
    last_seen_date               Date,
    source_run_id                String,
    resolved_at                  DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (isin, issuer_scheme, issuer_id, mapping_source);

INSERT INTO corpscout.instrument_issuer
    (isin, issuer_scheme, issuer_id, mapping_source,
     first_seen_date, last_seen_date, source_run_id, resolved_at)
SELECT
    isin,
    'lei' AS issuer_scheme,
    lei AS issuer_id,
    mapping_source,
    first_seen_date,
    last_seen_date,
    source_run_id,
    resolved_at
FROM corpscout.isin_lei;

DROP TABLE IF EXISTS corpscout.isin_lei;
