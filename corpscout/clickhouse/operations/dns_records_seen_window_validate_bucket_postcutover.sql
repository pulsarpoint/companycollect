-- Post-cutover variant of dns_records_seen_window_validate_bucket.sql. The canonical
-- commoncrawl_domain_dns_records (which also receives live ingest) must contain at least every
-- unique record present in the restored legacy dimension for the bucket, and no merged record may
-- have an inverted seen-window.
SELECT
    throwIf(
        canonical_count < legacy_count,
        'commoncrawl_domain_dns_records is missing records for this bucket'
    ) AS missing_records_error,
    throwIf(
        inverted_windows != 0,
        'commoncrawl_domain_dns_records has first_seen > last_seen for this bucket'
    ) AS inverted_window_error,
    legacy_count,
    canonical_count,
    inverted_windows
FROM
(
    SELECT count() AS legacy_count
    FROM
    (
        SELECT 1
        FROM corpscout.commoncrawl_domain_dns_records_legacy
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
        count() AS canonical_count,
        countIf(first_seen > last_seen) AS inverted_windows
    FROM
    (
        SELECT
            min(first_seen) AS first_seen,
            max(last_seen) AS last_seen
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
) AS canonical;
