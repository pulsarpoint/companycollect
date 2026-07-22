-- Backfill one stable legacy source bucket into the canonical RR dimension. Grouping by the exact
-- RR identity avoids copying every historical sighting's wide strings into the compact table.
INSERT INTO corpscout.commoncrawl_domain_dns_records
(
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
)
SELECT
    record_id,
    root_domain,
    name,
    argMax(record_type, loaded_at) AS canonical_record_type,
    record_type_code,
    record_class_code,
    argMax(value, loaded_at) AS canonical_value,
    argMax(rdata_wire, loaded_at) AS canonical_rdata_wire,
    argMax(priority, loaded_at) AS canonical_priority,
    max(loaded_at) AS definition_loaded_at
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
        loaded_at
    FROM corpscout.commoncrawl_domain_dns_record_observations
    WHERE cityHash64(root_domain) % 16 = {bucket:UInt8}
)
GROUP BY
    root_domain,
    name,
    record_type_code,
    record_class_code,
    record_id;
