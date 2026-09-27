CREATE DATABASE IF NOT EXISTS corpscout;

-- provider-recon documents read live from the provider-recon bucket, one row
-- per provider (providers/<slug>/latest.json). Credentials and the endpoint
-- live in the operator-owned named collection provider_recon, created by
-- services/provider_recon/ansible from stdin, and this file holds no credentials.
CREATE TABLE IF NOT EXISTS corpscout.provider_recon_documents_s3
(
    json String
)
ENGINE = S3(provider_recon, filename = 'providers/*/latest.json', format = 'JSONAsString');

-- Providers and their services as last published. Upserted by the Dagster
-- asset provider_recon_clickhouse; read with FINAL.
CREATE TABLE IF NOT EXISTS corpscout.provider_services
(
    provider_slug LowCardinality(String),
    provider_name String,
    provider_category LowCardinality(String),
    provider_country LowCardinality(String),
    provider_website String,
    provider_keys Array(String),
    service_key String,
    service_name String,
    service_types Array(LowCardinality(String)),
    traits Array(LowCardinality(String)),
    removed_at Nullable(Date),
    collected_at DateTime64(3, 'UTC'),
    content_hash String,
    loaded_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (provider_slug, service_key);

-- The permanent range timeline: one row per range instance (a removed range
-- that returns is a new instance with its own first_seen). Loads upsert and
-- never delete, so instances purged from latest.json after 90 days keep their
-- last state here. range_start/range_end are IPv6 with IPv4 mapped
-- (::ffff:a.b.c.d) for interval joins against DNS seen-windows.
CREATE TABLE IF NOT EXISTS corpscout.provider_ip_ranges
(
    provider_slug LowCardinality(String),
    service_key String,
    cidr String,
    ip_family UInt8,
    range_start IPv6,
    range_end IPv6,
    collector LowCardinality(String),
    source LowCardinality(String),
    feed_tag LowCardinality(String),
    region LowCardinality(String),
    confidence Float32,
    status LowCardinality(String),
    first_seen Date,
    last_seen Date,
    missing_since Nullable(Date),
    removed_at Nullable(Date),
    removal_action LowCardinality(String),
    restored_at Nullable(Date),
    source_url String,
    source_version String,
    loaded_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (provider_slug, service_key, cidr, collector, first_seen);

-- Non-range evidence (DNS / HTTP / PTR / certificate / ASN) with the same
-- lifecycle. kind + rule_key mirror the Go model's Key() per kind.
CREATE TABLE IF NOT EXISTS corpscout.provider_rules
(
    provider_slug LowCardinality(String),
    service_key String,
    kind LowCardinality(String),
    rule_key String,
    record_type LowCardinality(String),
    match_field LowCardinality(String),
    http_part LowCardinality(String),
    header_name String,
    matcher_type LowCardinality(String),
    pattern String,
    case_sensitive UInt8,
    path_scope String,
    identity_type LowCardinality(String),
    asn UInt32,
    confidence Float32,
    priority Int32,
    note String,
    source LowCardinality(String),
    source_url String,
    status LowCardinality(String),
    first_seen Date,
    last_seen Date,
    missing_since Nullable(Date),
    removed_at Nullable(Date),
    removal_action LowCardinality(String),
    restored_at Nullable(Date),
    loaded_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (provider_slug, service_key, kind, rule_key, first_seen);

-- Ranges usable for present-day lookups: active and missing (still within
-- grace) instances, latest loaded state.
CREATE VIEW IF NOT EXISTS corpscout.provider_ip_ranges_current AS
SELECT *
FROM corpscout.provider_ip_ranges FINAL
WHERE status != 'removed';
