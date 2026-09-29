CREATE DATABASE IF NOT EXISTS corpscout;

-- Search table for the backoffice IP address list and the enrichment queue selection:
-- one row per inventory IP (commoncrawl_ip_addresses, enriched or not) with its current
-- enrichment state from ip_enrichment_results. A refreshable materialized view rebuilds
-- it whole and swaps it atomically, daily at 03:00 UTC and on demand
-- (SYSTEM REFRESH VIEW, issued by ip_enrichment_results when a task completes).
-- Readers query this name.
--
-- Unknown values are defaults, not NULLs, so they can sit in sort keys: asn 0 means no
-- ASN, an empty string means no country, subdivision, city or RDAP data. enriched says
-- whether the address has any result row at all. The lookup statuses are the latest
-- attempt's (not_attempted for unenriched addresses), the payload columns come from the
-- newest conclusive lookup per component, exactly as ip_enrichment_current picks them
-- (pinned by tests/test_ip_enrichment_search.py). subdivision_* and rdap_registrant are
-- the first subdivision and the first registrant name.
--
-- Sort key (asn, bucket, ip) serves ASN filters and the unfiltered list. by_location serves
-- country, region and city filters in (country, subdivision, city, bucket, ip) order.
-- The three aggregate projections answer the filter pickers' counts. ip_bloom serves an
-- exact IP search (the sort key cannot, asn leads it). asn_organization_ngrams serves
-- substring searches on the organization name (LIKE '%text%' with 3 or more characters,
-- which a tokenbf index cannot prune).
--
-- Memory: both inputs are aggregated in primary-key order (a streaming GROUP BY, bounded by
-- a block instead of 49M keys) and joined with grace_hash, which keeps one hash partition
-- in memory and spills the rest. Created EMPTY: the first build (tens of minutes to hours
-- on a busy host) is started after deploy with SYSTEM REFRESH VIEW, never inside this
-- migration (the migrate client's read_timeout is 300 s).
CREATE MATERIALIZED VIEW IF NOT EXISTS corpscout.ip_enrichment_search
REFRESH EVERY 1 DAY OFFSET 3 HOUR
(
    bucket UInt16,
    ip String,
    ip_version UInt8,
    first_seen DateTime64(3, 'UTC'),
    last_seen DateTime64(3, 'UTC'),
    enriched UInt8,
    completed_at Nullable(DateTime64(6, 'UTC')),
    city_lookup_status Enum8('not_attempted' = 0, 'found' = 1, 'not_found' = 2, 'not_global' = 3,
        'retryable_error' = 4, 'terminal_error' = 5),
    asn_lookup_status Enum8('not_attempted' = 0, 'found' = 1, 'not_found' = 2, 'not_global' = 3,
        'retryable_error' = 4, 'terminal_error' = 5),
    rdap_lookup_status Enum8('not_attempted' = 0, 'found' = 1, 'not_found' = 2, 'not_global' = 3,
        'retryable_error' = 4, 'terminal_error' = 5),
    asn UInt32,
    asn_organization String,
    country_iso_code LowCardinality(String),
    country_name LowCardinality(String),
    subdivision_iso_code LowCardinality(String),
    subdivision_name LowCardinality(String),
    city_name String,
    rdap_matched_cidr String,
    rdap_rir LowCardinality(String),
    rdap_name String,
    rdap_registrant String,
    INDEX ip_bloom ip TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX asn_organization_ngrams lower(asn_organization) TYPE ngrambf_v1(3, 8192, 3, 0) GRANULARITY 1,
    PROJECTION by_location
    (
        SELECT *
        ORDER BY country_iso_code, subdivision_iso_code, city_name, bucket, ip
    ),
    PROJECTION asn_counts
    (
        SELECT asn, asn_organization, count()
        GROUP BY asn, asn_organization
    ),
    PROJECTION country_counts
    (
        SELECT country_iso_code, country_name, count()
        GROUP BY country_iso_code, country_name
    ),
    PROJECTION location_counts
    (
        SELECT country_iso_code, subdivision_iso_code, subdivision_name, city_name, count()
        GROUP BY country_iso_code, subdivision_iso_code, subdivision_name, city_name
    )
)
ENGINE = MergeTree
ORDER BY (asn, bucket, ip)
EMPTY
AS
SELECT
    i.bucket AS bucket,
    i.ip AS ip,
    i.ip_version AS ip_version,
    i.first_seen AS first_seen,
    i.last_seen AS last_seen,
    toUInt8(e.ip != '') AS enriched,
    if(e.ip != '', e.latest.1, NULL) AS completed_at,
    if(e.ip != '', e.latest.2, 'not_attempted') AS city_lookup_status,
    if(e.ip != '', e.latest.3, 'not_attempted') AS asn_lookup_status,
    if(e.ip != '', e.latest.4, 'not_attempted') AS rdap_lookup_status,
    ifNull(e.asn_data.1, 0) AS asn,
    ifNull(e.asn_data.2, '') AS asn_organization,
    ifNull(e.city_data.1, '') AS country_iso_code,
    ifNull(e.city_data.2, '') AS country_name,
    e.city_data.3 AS subdivision_iso_code,
    e.city_data.4 AS subdivision_name,
    ifNull(e.city_data.5, '') AS city_name,
    ifNull(e.rdap_data.1, '') AS rdap_matched_cidr,
    ifNull(e.rdap_data.2, '') AS rdap_rir,
    ifNull(e.rdap_data.3, '') AS rdap_name,
    e.rdap_data.4 AS rdap_registrant
FROM
(
    SELECT bucket, ip_version, ip, min(first_seen) AS first_seen, max(last_seen) AS last_seen
    FROM corpscout.commoncrawl_ip_addresses
    GROUP BY bucket, ip_version, ip
) AS i
LEFT JOIN
(
    -- Result rows are immutable and a retried write repeats the same row, so duplicates
    -- that FINAL would collapse cannot change an argMax.
    SELECT
        bucket,
        ip,
        argMax(tuple(completed_at, city_lookup_status, asn_lookup_status, rdap_lookup_status),
            tuple(completed_at, result_id)) AS latest,
        argMaxIf(tuple(country_iso_code, country_name, subdivision_iso_codes[1], subdivision_names[1], city_name),
            tuple(ifNull(city_checked_at, completed_at), completed_at, result_id),
            city_lookup_status IN ('found', 'not_found', 'not_global')) AS city_data,
        argMaxIf(tuple(asn, asn_organization),
            tuple(ifNull(asn_checked_at, completed_at), completed_at, result_id),
            asn_lookup_status IN ('found', 'not_found', 'not_global')) AS asn_data,
        argMaxIf(tuple(rdap_matched_cidr, rdap_rir, rdap_name, rdap_registrant_names[1]),
            tuple(ifNull(rdap_checked_at, completed_at), completed_at, result_id),
            rdap_lookup_status IN ('found', 'not_found', 'not_global')) AS rdap_data
    FROM corpscout.ip_enrichment_results
    GROUP BY bucket, ip
) AS e ON e.bucket = i.bucket AND e.ip = i.ip
SETTINGS max_threads = 4, optimize_aggregation_in_order = 1,
    join_algorithm = 'grace_hash', grace_hash_join_initial_buckets = 32,
    max_bytes_in_join = 1073741824, max_bytes_before_external_group_by = 1073741824,
    max_memory_usage = 6442450944, max_execution_time = 0;
