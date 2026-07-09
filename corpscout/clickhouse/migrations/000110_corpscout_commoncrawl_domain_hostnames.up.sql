CREATE DATABASE IF NOT EXISTS corpscout;

-- Durable per-domain hostname registry: the monotonic set of every hostname ever discovered for a
-- domain, so AXFR-internal hosts survive the zone closing (Phase 2 re-reads this at seed). The per-cycle
-- write-back blind-INSERTs one row per discovered label -- AggregatingMergeTree folds duplicates with
-- first_seen=min, last_seen=max, last_resolved=max, discovery_source=min (axfr precedence), so no
-- read-before-write. The sort key is the plain (root_domain, label) columns present at CREATE.
DROP TABLE IF EXISTS corpscout.commoncrawl_domain_hostnames;

CREATE TABLE corpscout.commoncrawl_domain_hostnames
(
    root_domain      String,
    label            String,
    discovery_source SimpleAggregateFunction(min, LowCardinality(String)),
    first_seen       SimpleAggregateFunction(min, DateTime64(3, 'UTC')),
    last_seen        SimpleAggregateFunction(max, DateTime64(3, 'UTC')),
    last_resolved    SimpleAggregateFunction(max, DateTime64(3, 'UTC'))
)
ENGINE = AggregatingMergeTree()
ORDER BY (root_domain, label);
