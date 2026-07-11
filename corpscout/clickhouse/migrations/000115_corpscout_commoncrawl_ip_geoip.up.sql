CREATE DATABASE IF NOT EXISTS corpscout;

-- One logical row per canonical IP observed in retry-safe DNS observations. The aggregate columns
-- make retries and overlapping backfills idempotent: duplicate observations cannot move first_seen
-- or last_seen in the wrong direction.
CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_ip_addresses
(
    bucket     UInt16,
    ip         String,
    ip_version UInt8,
    first_seen SimpleAggregateFunction(min, DateTime64(3, 'UTC')),
    last_seen  SimpleAggregateFunction(max, DateTime64(3, 'UTC'))
)
ENGINE = AggregatingMergeTree()
ORDER BY (bucket, ip_version, ip);

-- Incremental insert-block processing avoids repeatedly scanning all DNS history. Replayed source
-- rows are harmless because the target only retains min/max event times for each canonical IP.
CREATE MATERIALIZED VIEW IF NOT EXISTS corpscout.commoncrawl_ip_addresses_mv
TO corpscout.commoncrawl_ip_addresses
AS
SELECT
    toUInt16(cityHash64(ip) % 256) AS bucket,
    ip,
    ip_version,
    min(observed_at) AS first_seen,
    max(observed_at) AS last_seen
FROM
(
    SELECT
        assumeNotNull(
            if(
                record_type = 'A',
                toString(toIPv4OrNull(value)),
                toString(toIPv6OrNull(value))
            )
        ) AS ip,
        if(record_type = 'A', toUInt8(4), toUInt8(6)) AS ip_version,
        observed_at
    FROM corpscout.commoncrawl_domain_dns_record_observations
    WHERE record_type IN ('A', 'AAAA')
      AND if(
          record_type = 'A',
          isNotNull(toIPv4OrNull(value)),
          isNotNull(toIPv6OrNull(value))
      )
)
GROUP BY bucket, ip, ip_version;

-- Current MaxMind enrichment. Re-materializing a bucket writes a newer version for stale/missing
-- IPs. Consumers use commoncrawl_ip_geoip_current (FINAL) when merge timing must be invisible.
CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_ip_geoip
(
    bucket                              UInt16,
    ip                                  String,
    ip_version                          UInt8,
    ip_scope                            LowCardinality(String),
    city_lookup_status                  LowCardinality(String),
    asn_lookup_status                   LowCardinality(String),
    continent_code                      Nullable(String),
    continent_name                      Nullable(String),
    country_iso_code                    Nullable(String),
    country_name                        Nullable(String),
    country_geoname_id                  Nullable(UInt32),
    registered_country_iso_code         Nullable(String),
    registered_country_name             Nullable(String),
    subdivision_iso_codes               Array(String),
    subdivision_names                   Array(String),
    city_geoname_id                     Nullable(UInt32),
    city_name                           Nullable(String),
    latitude                            Nullable(Float64),
    longitude                           Nullable(Float64),
    accuracy_radius_km                  Nullable(UInt16),
    timezone                            Nullable(String),
    asn                                 Nullable(UInt32),
    asn_organization                    Nullable(String),
    city_network                        Nullable(String),
    asn_network                         Nullable(String),
    city_db_build_epoch                 DateTime('UTC'),
    asn_db_build_epoch                  DateTime('UTC'),
    enriched_at                         DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(enriched_at)
ORDER BY (bucket, ip);

CREATE VIEW IF NOT EXISTS corpscout.commoncrawl_ip_geoip_current AS
SELECT *
FROM corpscout.commoncrawl_ip_geoip FINAL;
