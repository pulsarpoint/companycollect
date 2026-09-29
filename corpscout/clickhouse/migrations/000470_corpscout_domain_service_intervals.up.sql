CREATE DATABASE IF NOT EXISTS corpscout;

-- Service usage periods per domain, rebuilt per hash bucket by the Dagster
-- asset domain_service_intervals_clickhouse from dns_record_services (same
-- rules as the domain_services_history view). Sorted by provider so provider
-- pages read a range.
CREATE TABLE IF NOT EXISTS corpscout.domain_service_intervals
(
    root_domain String,
    service_type LowCardinality(String),
    provider_key String,
    provider_slug LowCardinality(String),
    service_keys Array(String),
    first_seen Date,
    last_seen Date,
    is_current UInt8,
    evidence UInt32,
    analyzers Array(LowCardinality(String)),
    record_types Array(LowCardinality(String)),
    confidence Float32,
    bucket UInt8,
    computed_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY bucket
ORDER BY (provider_slug, service_type, root_domain, first_seen);

-- Distinct domains per provider key and service type in one bucket. A row
-- with service_type = '' counts the provider key over all service types.
-- Pages sum the 128 buckets.
CREATE TABLE IF NOT EXISTS corpscout.provider_service_counts
(
    bucket UInt8,
    provider_slug LowCardinality(String),
    provider_key String,
    service_type LowCardinality(String),
    domains_now UInt32,
    domains_ever UInt32,
    computed_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY bucket
ORDER BY (provider_slug, provider_key, service_type);
