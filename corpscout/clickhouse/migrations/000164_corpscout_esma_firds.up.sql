CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.firds_instrument_events
(
    source_record_id             String,
    source_file_id               String,
    source_file_name             String,
    source_file_type             LowCardinality(String),
    source_file_checksum         String,
    source_publication_date      Date,
    source_row_number            UInt64,
    event_type                   LowCardinality(String),
    isin                         String,
    mic                          LowCardinality(String),
    issuer_lei                   String,
    full_name                    String,
    short_name                   String,
    cfi_code                     LowCardinality(String),
    notional_currency            LowCardinality(String),
    commodity_derivative         Nullable(UInt8),
    issuer_request               Nullable(UInt8),
    admission_approval_at        Nullable(DateTime64(3, 'UTC')),
    request_admission_at         Nullable(DateTime64(3, 'UTC')),
    first_trade_at               Nullable(DateTime64(3, 'UTC')),
    termination_at               Nullable(DateTime64(3, 'UTC')),
    competent_authority_country  LowCardinality(String),
    relevant_venue_mic           LowCardinality(String),
    valid_from                   Date,
    valid_to                     Nullable(Date),
    is_latest                    UInt8,
    source_download_url          String,
    source_object_key            String,
    source_run_id                String,
    source_retrieved_at          DateTime64(3, 'UTC'),
    resolved_at                  DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (
    isin,
    mic,
    valid_from,
    source_publication_date,
    source_file_name,
    source_row_number
);

CREATE TABLE IF NOT EXISTS corpscout.firds_instruments_current
(
    isin                         String,
    mic                          LowCardinality(String),
    issuer_lei                   String,
    full_name                    String,
    short_name                   String,
    cfi_code                     LowCardinality(String),
    notional_currency            LowCardinality(String),
    commodity_derivative         Nullable(UInt8),
    issuer_request               Nullable(UInt8),
    admission_approval_at        Nullable(DateTime64(3, 'UTC')),
    request_admission_at         Nullable(DateTime64(3, 'UTC')),
    first_trade_at               Nullable(DateTime64(3, 'UTC')),
    termination_at               Nullable(DateTime64(3, 'UTC')),
    competent_authority_country  LowCardinality(String),
    relevant_venue_mic           LowCardinality(String),
    valid_from                   Date,
    source_record_id             String,
    source_file_id               String,
    source_file_name             String,
    source_file_type             LowCardinality(String),
    source_file_checksum         String,
    source_publication_date      Date,
    latest_event_type            LowCardinality(String),
    source_download_url          String,
    source_object_key            String,
    source_run_id                String,
    source_retrieved_at          DateTime64(3, 'UTC'),
    resolved_at                  DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (isin, mic);
