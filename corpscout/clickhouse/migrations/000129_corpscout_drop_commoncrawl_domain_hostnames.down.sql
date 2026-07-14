CREATE DATABASE IF NOT EXISTS corpscout;

-- Rolling this migration back recreates only the compatibility schema. Dropped registry rows cannot
-- be restored by a schema migration. The authoritative hostname evidence remains in DNS observations.
CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_domain_hostnames
(
    root_domain      String,
    label            String,
    discovery_source SimpleAggregateFunction(min, LowCardinality(String)),
    first_seen       SimpleAggregateFunction(min, DateTime64(3, 'UTC')),
    last_seen        SimpleAggregateFunction(max, DateTime64(3, 'UTC')),
    last_resolved    SimpleAggregateFunction(max, DateTime64(3, 'UTC')),
    last_not_after   SimpleAggregateFunction(max, DateTime64(3, 'UTC'))
        DEFAULT toDateTime64(0, 3, 'UTC')
)
ENGINE = AggregatingMergeTree()
ORDER BY (root_domain, label);
