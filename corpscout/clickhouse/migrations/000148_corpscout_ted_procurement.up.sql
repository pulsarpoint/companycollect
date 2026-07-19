CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.ted_notices
(
    country_iso2 LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    publication_number String,
    publication_date Nullable(Date),
    notice_type LowCardinality(String),
    place_country LowCardinality(String),
    buyer_name String,
    buyer_org_ref String,
    buyer_national_id_raw String,
    buyer_national_id String,
    buyer_country LowCardinality(String),
    notice_title String,
    total_value_amount_original Nullable(Decimal(38, 2)),
    total_value_amount_usd Nullable(Decimal(38, 2)),
    total_value_currency LowCardinality(String),
    fx_rate_to_usd Nullable(Decimal(38, 12)),
    fx_rate_date Nullable(Date),
    fx_source String,
    partition_key LowCardinality(String),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree
ORDER BY (publication_number);

CREATE TABLE IF NOT EXISTS corpscout.ted_notice_winners
(
    country_iso2 LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    publication_number String,
    lot_id String,
    tender_id String,
    winner_ordinal Int32,
    winner_name String,
    winner_national_id_raw String,
    winner_national_id String,
    winner_country LowCardinality(String),
    awarded_amount_original Nullable(Decimal(38, 2)),
    awarded_amount_usd Nullable(Decimal(38, 2)),
    awarded_currency LowCardinality(String),
    buyer_national_id String,
    place_country LowCardinality(String),
    publication_date Nullable(Date),
    fx_rate_to_usd Nullable(Decimal(38, 12)),
    fx_rate_date Nullable(Date),
    fx_source String,
    partition_key LowCardinality(String),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree
ORDER BY (winner_national_id, publication_number, lot_id, tender_id, winner_ordinal);
