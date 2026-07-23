CREATE DATABASE IF NOT EXISTS corpscout;

-- Reverse the cutover. Data is intact: the legacy records table and the sightings table still
-- exist until 000163, so this restores the exact 000155 write path and read surface.

DROP VIEW IF EXISTS corpscout.commoncrawl_domain_dns_records_current;
DROP VIEW IF EXISTS corpscout.commoncrawl_domain_dns_records_ingest_v2_mv;

RENAME TABLE
    corpscout.commoncrawl_domain_dns_records TO corpscout.commoncrawl_domain_dns_records_v2,
    corpscout.commoncrawl_domain_dns_records_legacy TO corpscout.commoncrawl_domain_dns_records;

-- Reattach the dual-write trigger to the v2 name, mirroring the 000161 state.
CREATE MATERIALIZED VIEW IF NOT EXISTS corpscout.commoncrawl_domain_dns_records_ingest_v2_mv
TO corpscout.commoncrawl_domain_dns_records_v2
AS
SELECT
    sipHash128(root_domain, name, record_type_code, record_class_code, rdata_wire) AS record_id,
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
FROM corpscout.commoncrawl_domain_dns_record_ingest
GROUP BY
    root_domain,
    name,
    record_type_code,
    record_class_code,
    record_id;

-- Recreate the 000155 triggers and view verbatim.
CREATE MATERIALIZED VIEW IF NOT EXISTS corpscout.commoncrawl_domain_dns_records_ingest_mv
TO corpscout.commoncrawl_domain_dns_records
AS
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
    loaded_at
FROM corpscout.commoncrawl_domain_dns_record_ingest;

CREATE MATERIALIZED VIEW IF NOT EXISTS corpscout.commoncrawl_domain_dns_record_sightings_ingest_mv
TO corpscout.commoncrawl_domain_dns_record_sightings
AS
SELECT
    sipHash128(root_domain, name, record_type_code, record_class_code, rdata_wire) AS record_id,
    root_domain,
    scan_id,
    slot,
    source,
    discovery,
    name_server,
    name_server_ip,
    ttl,
    rcode,
    observed_at,
    loaded_at
FROM corpscout.commoncrawl_domain_dns_record_ingest;

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
    loaded_at
FROM corpscout.commoncrawl_domain_dns_records FINAL
SETTINGS do_not_merge_across_partitions_select_final = 1;
