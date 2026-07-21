CREATE DATABASE IF NOT EXISTS corpscout;

-- One row per Wikidata exchange and MIC. MIC remains nullable because Wikidata
-- coverage is incomplete, while the exchange QID is the stable join key used by
-- wikidata_company_listings.
CREATE TABLE IF NOT EXISTS corpscout.wikidata_exchanges
(
    exchange_wikidata_id String,
    exchange_name String,
    mic Nullable(String),
    country_wikidata_id Nullable(String),
    country_name Nullable(String),
    country_iso2 Nullable(String),
    listed_company_count UInt64,
    source_system LowCardinality(String),
    source_run_id String,
    source_record_id String,
    source_payload_hash FixedString(64),
    retrieved_at DateTime64(3, 'UTC'),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (exchange_wikidata_id, ifNull(mic, ''));
