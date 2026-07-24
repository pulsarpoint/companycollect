CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.se_uhm_procurement_awards
(
    company_id String,
    company_match_status LowCardinality(String),
    match_eligibility LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    source_record_id String,
    source_line_number UInt64,
    source_procurement_id String,
    source_lot_id String,
    publication_date Nullable(Date),
    title String,
    agreement_type LowCardinality(String),
    contracted UInt8,
    buyer_name String,
    buyer_id_normalized String,
    supplier_name String,
    supplier_id_normalized String,
    cpv_code String,
    advertising_database LowCardinality(String),
    source_object_key String,
    source_retrieved_at DateTime64(3, 'UTC'),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (
    supplier_id_normalized,
    source_procurement_id,
    source_lot_id,
    source_record_id
);
