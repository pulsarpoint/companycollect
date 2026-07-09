CREATE DATABASE IF NOT EXISTS corpscout;

-- Add hostname-discovery provenance (static, ct, axfr) as a non-key SimpleAggregateFunction, same as
-- source (migration 000107) -- ClickHouse forbids a defaulted column in the sort key. anyLast matches
-- the sibling data columns. This is a different axis from source (how the record was obtained).
ALTER TABLE corpscout.commoncrawl_domain_dns_records
    ADD COLUMN IF NOT EXISTS discovery SimpleAggregateFunction(anyLast, LowCardinality(String));
