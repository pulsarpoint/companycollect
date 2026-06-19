CREATE DATABASE IF NOT EXISTS corpscout;

DROP TABLE IF EXISTS corpscout.fi_names__history_order_key;

CREATE TABLE IF NOT EXISTS corpscout.fi_names__history_order_key
(
    business_id String,
    name String,
    name_type_code LowCardinality(String),
    name_type_description_original Nullable(String),
    name_type_description_en Nullable(String),
    registration_date Nullable(Date),
    end_date Nullable(Date),
    version Nullable(UInt32),
    is_current UInt8,
    is_primary UInt8,
    source_code Nullable(String),
    source_system LowCardinality(String),
    source_run_id String,
    source_record_id String,
    source_payload_hash FixedString(64),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (business_id, name_type_code, name, ifNull(version, 0), source_record_id);

INSERT INTO corpscout.fi_names__history_order_key
SELECT *
FROM corpscout.fi_names;

EXCHANGE TABLES corpscout.fi_names__history_order_key AND corpscout.fi_names;

DROP TABLE IF EXISTS corpscout.fi_names__history_order_key;
