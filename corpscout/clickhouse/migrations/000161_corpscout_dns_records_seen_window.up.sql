CREATE DATABASE IF NOT EXISTS corpscout;

-- One row per canonical DNS resource record carrying its own observation window. Repeated scans of
-- an unchanged record merge into the same row (min/max/any/groupUniqArray are all idempotent under
-- the outbox's duplicate re-inserts), while a changed RDATA value produces a new record_id and thus
-- a separate row with a disjoint [first_seen, last_seen] window. This replaces the per-scan
-- commoncrawl_domain_dns_record_sightings fact, which grew one row per record per scan.
--
-- Do not add sum-style counters here: a partially failed ingest fan-out is retried by the durable
-- SQLite outboxes, so any non-idempotent aggregate would silently drift.
CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_domain_dns_records_v2
(
    record_id         FixedString(16),
    root_domain       String,
    name              String,
    record_type       SimpleAggregateFunction(any, LowCardinality(String)),
    record_type_code  UInt16,
    record_class_code UInt16,
    value             SimpleAggregateFunction(any, String),
    rdata_wire        SimpleAggregateFunction(any, String),
    priority          SimpleAggregateFunction(any, UInt16),
    sources           SimpleAggregateFunction(groupUniqArrayArray, Array(LowCardinality(String))),
    discoveries       SimpleAggregateFunction(groupUniqArrayArray, Array(LowCardinality(String))),
    first_seen        SimpleAggregateFunction(min, DateTime64(3, 'UTC')),
    last_seen         SimpleAggregateFunction(max, DateTime64(3, 'UTC')),
    last_loaded_at    SimpleAggregateFunction(max, DateTime64(3, 'UTC'))
)
ENGINE = AggregatingMergeTree()
PARTITION BY cityHash64(root_domain) % 16
ORDER BY
(
    root_domain,
    name,
    record_type_code,
    record_class_code,
    record_id
);
-- value/rdata_wire/record_type/priority are functionally determined by record_id (the sipHash128
-- of the identity columns including rdata_wire), so any() is deterministic in effect while keeping
-- large RDATA strings out of the sort key. The sort and partition keys deliberately match the
-- existing records table so partition pruning, FINAL semantics, and the bucketed operational
-- tooling carry over unchanged.

-- Dual-write trigger. The existing 000155 MVs stay attached to the ingest routing table until the
-- 000162 cutover, so this migration is safe to apply while both DNS writers keep inserting.
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
