-- Backfill one bucket of the seen-window records table from the existing record dimension and the
-- per-scan sightings fact. Re-running a bucket is safe: every target column merges idempotently
-- (min/max/any/groupUniqArrayArray), so overlap with the live dual-write trigger or with a retried
-- bucket collapses to the same row.
--
-- Memory profile is deliberate. (root_domain, record_id) is a prefix of the sightings sort key, so
-- optimize_aggregation_in_order streams the GROUP BY instead of building a per-bucket hash table
-- (a naive uniqExact over the full fact previously exceeded the server memory limit). The join
-- keeps the aggregated bucket on the build side and spills through grace_hash. Neither source is
-- read with FINAL: unmerged ReplacingMergeTree duplicates produce identical aggregate inputs that
-- the AggregatingMergeTree target collapses for free.
INSERT INTO corpscout.commoncrawl_domain_dns_records_v2
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
    sources,
    discoveries,
    first_seen,
    last_seen,
    last_loaded_at
)
SELECT
    r.record_id,
    r.root_domain,
    r.name,
    r.record_type,
    r.record_type_code,
    r.record_class_code,
    r.value,
    r.rdata_wire,
    r.priority,
    s.sources,
    s.discoveries,
    if(s.sighting_count = 0, r.loaded_at, s.first_seen) AS first_seen,
    if(s.sighting_count = 0, r.loaded_at, s.last_seen) AS last_seen,
    r.loaded_at AS last_loaded_at
FROM corpscout.commoncrawl_domain_dns_records AS r
LEFT JOIN
(
    SELECT
        root_domain,
        record_id,
        min(observed_at) AS first_seen,
        max(observed_at) AS last_seen,
        groupUniqArray(source) AS sources,
        groupUniqArray(discovery) AS discoveries,
        count() AS sighting_count
    FROM corpscout.commoncrawl_domain_dns_record_sightings
    WHERE cityHash64(root_domain) % 16 = {bucket:UInt8}
    GROUP BY
        root_domain,
        record_id
) AS s
    ON r.root_domain = s.root_domain
   AND r.record_id = s.record_id
WHERE cityHash64(r.root_domain) % 16 = {bucket:UInt8}
SETTINGS
    optimize_aggregation_in_order = 1,
    join_algorithm = 'grace_hash',
    grace_hash_join_initial_buckets = 32,
    max_bytes_before_external_group_by = 8000000000,
    max_threads = 8;
