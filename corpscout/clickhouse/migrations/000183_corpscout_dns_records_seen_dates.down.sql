CREATE DATABASE IF NOT EXISTS corpscout;

-- Restore the pre-seen_dates trigger query first so nothing writes the column while it is being
-- removed. Dropping the column discards the accumulated observation dates.
ALTER TABLE corpscout.commoncrawl_domain_dns_records_ingest_v2_mv
MODIFY QUERY
SELECT
    record_id,
    root_domain,
    name,
    any(record_type) AS record_type,
    record_type_code,
    record_class_code,
    any(value) AS value,
    any(rdata_wire) AS rdata_wire,
    any(priority) AS priority,
    groupUniqArray(source) AS sources,
    groupUniqArray(discovery) AS discoveries,
    min(observed_at) AS first_seen,
    max(observed_at) AS last_seen,
    max(loaded_at) AS last_loaded_at
FROM
(
    SELECT
        sipHash128(root_domain, name, record_type_code, record_class_code, rdata_wire) AS record_id,
        root_domain,
        name,
        record_type,
        record_type_code,
        record_class_code,
        value,
        rdata_wire,
        priority,
        source,
        discovery,
        observed_at,
        loaded_at
    FROM corpscout.commoncrawl_domain_dns_record_ingest
)
GROUP BY
    root_domain,
    name,
    record_type_code,
    record_class_code,
    record_id;

CREATE OR REPLACE VIEW corpscout.commoncrawl_domain_dns_records_current AS
SELECT
    record_id,
    root_domain,
    name,
    record_type,
    record_type_code,
    record_class_code,
    value,
    rdata_wire,
    priority,
    sources,
    discoveries,
    first_seen,
    last_seen,
    last_loaded_at
FROM corpscout.commoncrawl_domain_dns_records FINAL
SETTINGS do_not_merge_across_partitions_select_final = 1;

ALTER TABLE corpscout.commoncrawl_domain_dns_records
    DROP COLUMN IF EXISTS seen_dates;
