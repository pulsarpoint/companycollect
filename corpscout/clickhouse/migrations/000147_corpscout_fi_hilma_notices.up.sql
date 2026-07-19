CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.fi_hilma_notices
(
    country_iso2 LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    source_record_id String,
    notice_number String,
    ted_number String,
    lot_id String,
    published_at Nullable(DateTime64(3, 'UTC')),
    notice_type LowCardinality(String),
    is_award UInt8,
    procurement_project_id String,
    procedure_id String,
    notice_name_fi String,
    notice_name_en String,
    notice_name_sv String,
    lot_name_fi String,
    lot_name_en String,
    lot_name_sv String,
    notice_cpv_code String,
    lot_cpv_code String,
    buyer_name_fi String,
    buyer_name_en String,
    buyer_name_sv String,
    buyer_department String,
    buyer_business_id String,
    buyer_address String,
    nuts_code LowCardinality(String),
    deadline_at Nullable(DateTime64(3, 'UTC')),
    dynamic_purchasing_system LowCardinality(String),
    framework_agreement LowCardinality(String),
    procedure_type LowCardinality(String),
    main_nature LowCardinality(String),
    lot_main_nature LowCardinality(String),
    winners_raw String,
    notice_estimated_value_amount_original Nullable(Decimal(38, 2)),
    notice_estimated_value_amount_usd Nullable(Decimal(38, 2)),
    notice_estimated_value_currency LowCardinality(String),
    lot_estimated_value_amount_original Nullable(Decimal(38, 2)),
    lot_estimated_value_amount_usd Nullable(Decimal(38, 2)),
    lot_estimated_value_currency LowCardinality(String),
    procurement_value_amount_original Nullable(Decimal(38, 2)),
    procurement_value_amount_usd Nullable(Decimal(38, 2)),
    procurement_value_currency LowCardinality(String),
    lots_value_amount_original Nullable(Decimal(38, 2)),
    lots_value_amount_usd Nullable(Decimal(38, 2)),
    lots_value_currency LowCardinality(String),
    fx_rate_to_usd Nullable(Decimal(38, 12)),
    fx_rate_date Nullable(Date),
    fx_source String,
    source_key String,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree
ORDER BY (notice_number, lot_id);

CREATE TABLE IF NOT EXISTS corpscout.fi_hilma_notice_winners
(
    country_iso2 LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    notice_number String,
    lot_id String,
    winner_ordinal Int32,
    winner_name String,
    winner_business_id String,
    buyer_business_id String,
    is_award UInt8,
    published_at Nullable(DateTime64(3, 'UTC')),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree
ORDER BY (winner_business_id, notice_number, lot_id, winner_ordinal);
