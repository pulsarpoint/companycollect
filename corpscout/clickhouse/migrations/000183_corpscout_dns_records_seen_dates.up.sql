CREATE DATABASE IF NOT EXISTS corpscout;

-- Add per-record observation dates to the seen-window DNS records table. The [first_seen,
-- last_seen] window cannot distinguish a value continuously present from one that disappeared and
-- returned (A -> x, A -> y, A -> x again). seen_dates records the distinct days each record was
-- observed, so interleaved values are reconstructable at scan granularity. The set union is
-- idempotent under outbox retries and bounded by days, not by scan row volume. Dates are accurate
-- from this migration forward. For earlier history, toDate(first_seen) and toDate(last_seen) are
-- known sightings and can be unioned in at query time.
ALTER TABLE corpscout.commoncrawl_domain_dns_records
    ADD COLUMN IF NOT EXISTS seen_dates SimpleAggregateFunction(groupUniqArrayArray, Array(Date))
    AFTER discoveries;

-- MODIFY QUERY swaps the trigger's SELECT atomically. Dropping and recreating the view instead
-- would open a window where ingest inserts succeed through the sibling triggers while silently
-- skipping this table. Rows inserted by the old query between the ADD COLUMN above and this
-- statement carry an empty seen_dates and merge harmlessly.
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
    groupUniqArray(toDate(observed_at)) AS seen_dates,
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
    seen_dates,
    first_seen,
    last_seen,
    last_loaded_at
FROM corpscout.commoncrawl_domain_dns_records FINAL
SETTINGS do_not_merge_across_partitions_select_final = 1;
