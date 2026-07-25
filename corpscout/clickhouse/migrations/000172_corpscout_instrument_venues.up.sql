CREATE DATABASE IF NOT EXISTS corpscout;

-- What trades where, across every venue source and every country.
-- Grain is (isin, mic, venue_source). Two sources asserting the same admission
-- is corroboration rather than conflict, so both rows are kept and consumers
-- select by evidence_tier.
--
-- mic is the ISO 10383 market segment MIC as published by the source.
-- operating_mic is its parent venue, empty when the source supplies none.
--
-- cfi_code classifies the instrument and therefore depends on ISIN alone. It is
-- stored per venue row because that is how sources publish it. Two rows for one
-- ISIN disagreeing on CFI is a data-quality signal, not real variation.
--
-- This table says nothing about which company owns the instrument. That link is
-- corpscout.instrument_issuer followed by corpscout.company_identifier.
CREATE TABLE IF NOT EXISTS corpscout.instrument_venues
(
    isin                         String,
    mic                          LowCardinality(String),
    venue_source                 LowCardinality(String),
    operating_mic                LowCardinality(String),
    evidence_tier                LowCardinality(String),
    cfi_code                     LowCardinality(String),
    cfi_category                 LowCardinality(String),
    instrument_name              String,
    instrument_type              LowCardinality(String),
    ticker                       String,
    trading_currency             LowCardinality(String),
    trading_status               LowCardinality(String),
    is_current                   UInt8,
    admission_date               Nullable(Date),
    first_trade_date             Nullable(Date),
    termination_date             Nullable(Date),
    first_seen_date              Date,
    last_seen_date               Date,
    source_record_id             String,
    source_publication_date      Date,
    source_retrieved_at          DateTime64(3, 'UTC'),
    source_run_id                String,
    resolved_at                  DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (isin, mic, venue_source);
