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

INSERT INTO corpscout.isin_lei
    (isin, lei, mapping_source, venue_confirmed, cfi_category,
     first_seen_date, last_seen_date, source_run_id, resolved_at)
SELECT
    isin,
    issuer_id AS lei,
    mapping_source,
    toUInt8(1) AS venue_confirmed,
    '' AS cfi_category,
    first_seen_date,
    last_seen_date,
    source_run_id,
    resolved_at
FROM corpscout.instrument_issuer
WHERE issuer_scheme = 'lei';

DROP TABLE IF EXISTS corpscout.instrument_issuer;
