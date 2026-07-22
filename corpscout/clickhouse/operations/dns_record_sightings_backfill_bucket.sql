-- Backfill one stable legacy source bucket into the narrow monthly fact. Re-running a bucket is
-- safe because the target's ReplacingMergeTree key and loaded_at version match live ingestion.
INSERT INTO corpscout.commoncrawl_domain_dns_record_sightings
(
    record_id,
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
)
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
FROM corpscout.commoncrawl_domain_dns_record_observations
WHERE cityHash64(root_domain) % 16 = {bucket:UInt8};
