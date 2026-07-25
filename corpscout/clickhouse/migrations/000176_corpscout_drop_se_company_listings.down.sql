CREATE TABLE IF NOT EXISTS corpscout.se_company_listings
(
    country_code                 LowCardinality(String),
    company_id                   String,
    issuer_lei                   String,
    isin                         String,
    mic                          LowCardinality(String),
    ticker                       String,
    instrument_name              String,
    instrument_type              LowCardinality(String),
    cfi_code                     LowCardinality(String),
    notional_currency            LowCardinality(String),
    admission_date               Nullable(Date),
    first_trade_date             Nullable(Date),
    termination_date             Nullable(Date),
    trading_status               LowCardinality(String),
    is_current                   UInt8,
    identity_match_method        LowCardinality(String),
    identity_match_confidence    LowCardinality(String),
    listing_status_source        LowCardinality(String),
    status_conflict              UInt8,
    eodhd_symbol_key             String,
    eodhd_is_delisted            Nullable(UInt8),
    source_slug                  LowCardinality(String),
    source_record_id             String,
    source_publication_date      Date,
    source_retrieved_at          DateTime64(3, 'UTC'),
    source_run_id                String,
    resolved_at                  DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (company_id, isin, mic);
