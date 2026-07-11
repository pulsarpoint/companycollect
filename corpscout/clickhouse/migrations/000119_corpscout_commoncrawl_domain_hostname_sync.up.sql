CREATE DATABASE IF NOT EXISTS corpscout;

-- Retain certificate validity for ranking while preserving old and worker-written rows.
ALTER TABLE corpscout.commoncrawl_domain_hostnames
    ADD COLUMN IF NOT EXISTS last_not_after SimpleAggregateFunction(max, DateTime64(3, 'UTC'))
        DEFAULT toDateTime64(0, 3, 'UTC');

-- Monotonic per-shard source cursors. AggregatingMergeTree max columns prevent a late retry from
-- moving either independent watermark backwards. The argMax state keeps the run identifier paired
-- with the most recent successful completion for operational diagnosis.
CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_domain_hostname_sync_state
(
    shard_index              UInt8,
    ct_ingested_through      SimpleAggregateFunction(max, DateTime64(3, 'UTC')),
    domains_resolved_through SimpleAggregateFunction(max, DateTime64(3, 'UTC')),
    completed_at             SimpleAggregateFunction(max, DateTime64(3, 'UTC')),
    completed_run            AggregateFunction(argMax, String, DateTime64(3, 'UTC'))
)
ENGINE = AggregatingMergeTree()
ORDER BY shard_index;
