CREATE DATABASE IF NOT EXISTS corpscout;

-- Cutover to the seen-window records table. Apply only after the 000161 dual-write has been
-- running, the bucketed backfill has completed, and every bucket passed validation.
--
-- Statement order is chosen so an ingest insert landing in any gap FAILS outright (the v2 MV's
-- target is briefly missing after the rename) instead of succeeding while silently skipping a
-- table. The durable SQLite outboxes retry the complete fan-out, and every aggregate column in the
-- new table is idempotent under those retries, so no rows are lost or double-counted.

-- 1. Stop feeding the legacy record and sighting tables. The v2 MV is still attached: no gap yet.
DROP VIEW IF EXISTS corpscout.commoncrawl_domain_dns_records_ingest_mv;
DROP VIEW IF EXISTS corpscout.commoncrawl_domain_dns_record_sightings_ingest_mv;
DROP VIEW IF EXISTS corpscout.commoncrawl_domain_dns_records_current;

-- 2. Swap the tables. From this instant until step 3 the v2 MV points at a missing name, so
--    ingest inserts fail and the outboxes retry.
RENAME TABLE
    corpscout.commoncrawl_domain_dns_records TO corpscout.commoncrawl_domain_dns_records_legacy,
    corpscout.commoncrawl_domain_dns_records_v2 TO corpscout.commoncrawl_domain_dns_records;

-- 3. Repoint the trigger at the canonical name. Retried rows re-land idempotently.
DROP VIEW IF EXISTS corpscout.commoncrawl_domain_dns_records_ingest_v2_mv;

CREATE MATERIALIZED VIEW IF NOT EXISTS corpscout.commoncrawl_domain_dns_records_ingest_v2_mv
TO corpscout.commoncrawl_domain_dns_records
AS
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

-- 4. Restore the logical unique-record surface. FINAL finalizes the SimpleAggregateFunction
--    columns, so callers see fully merged first_seen/last_seen windows regardless of background
--    merge timing.
CREATE VIEW IF NOT EXISTS corpscout.commoncrawl_domain_dns_records_current AS
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
