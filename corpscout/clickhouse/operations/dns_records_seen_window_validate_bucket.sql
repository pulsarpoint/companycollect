-- Validate one backfilled bucket of the seen-window records table before cutover. Both unique-key
-- counts stream through the tables' shared sort key (optimize_aggregation_in_order), so this stays
-- cheap enough to run after every bucket. A failure raises a ClickHouse exception so the runner
-- stops before 000162 can be applied.
--
-- v2 may exceed the legacy count while dual-write is active (live ingest lands rows the legacy
-- backfill has not seen and vice versa is impossible: legacy receives the same live rows), so the
-- check is v2 >= legacy, plus a window-sanity check that no merged record has first_seen after
-- last_seen.
SELECT
    throwIf(
        v2_count < legacy_count,
        'commoncrawl_domain_dns_records_v2 is missing records for this bucket'
    ) AS missing_records_error,
    throwIf(
        inverted_windows != 0,
        'commoncrawl_domain_dns_records_v2 has first_seen > last_seen for this bucket'
    ) AS inverted_window_error,
    legacy_count,
    v2_count,
    inverted_windows
FROM
(
    SELECT count() AS legacy_count
    FROM
    (
        SELECT 1
        FROM corpscout.commoncrawl_domain_dns_records
        WHERE cityHash64(root_domain) % 16 = {bucket:UInt8}
        GROUP BY
            root_domain,
            name,
            record_type_code,
            record_class_code,
            record_id
        SETTINGS optimize_aggregation_in_order = 1
    )
) AS legacy
CROSS JOIN
(
    SELECT
        count() AS v2_count,
        countIf(first_seen > last_seen) AS inverted_windows
    FROM
    (
        SELECT
            min(first_seen) AS first_seen,
            max(last_seen) AS last_seen
        FROM corpscout.commoncrawl_domain_dns_records_v2
        WHERE cityHash64(root_domain) % 16 = {bucket:UInt8}
        GROUP BY
            root_domain,
            name,
            record_type_code,
            record_class_code,
            record_id
        SETTINGS optimize_aggregation_in_order = 1
    )
) AS v2;
