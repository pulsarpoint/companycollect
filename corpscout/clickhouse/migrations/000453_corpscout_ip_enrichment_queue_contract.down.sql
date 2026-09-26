CREATE DATABASE IF NOT EXISTS corpscout;

SELECT throwIf(count() > 0, 'ip_enrichment_input must be empty before its layout changes')
FROM corpscout.ip_enrichment_input;

DROP TABLE IF EXISTS corpscout.ip_enrichment_input;

CREATE TABLE corpscout.ip_enrichment_input
(
    task_id UUID,
    input_id String,
    ip String,
    ip_version UInt8 MATERIALIZED if(isIPv4String(ip), toUInt8(4), toUInt8(6)),
    bucket UInt16 MATERIALIZED toUInt16(cityHash64(ip) % 256),
    source_name LowCardinality(String),
    source_record_id String,
    source_run_id String,
    observed_at Nullable(DateTime64(6, 'UTC')),
    submitted_at DateTime64(6, 'UTC') DEFAULT now64(6),
    CONSTRAINT valid_input CHECK notEmpty(trimBoth(input_id))
        AND position(input_id, char(0)) = 0 AND notEmpty(trimBoth(source_name)),
    CONSTRAINT canonical_ip CHECK
        ifNull(ip = toString(toIPv4OrNull(ip)), 0)
        OR ifNull(ip = toString(toIPv6OrNull(ip)), 0)
)
ENGINE = MergeTree
ORDER BY (input_id, task_id);
