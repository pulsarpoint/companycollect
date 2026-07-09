CREATE DATABASE IF NOT EXISTS corpscout;

-- Add source provenance (query vs axfr) to the distinct records table. ClickHouse forbids putting a
-- defaulted column in the sorting key, and a new key column on a table with data must be defaulted,
-- so source is a SimpleAggregateFunction like the other non-key data columns (ttl, rcode, last_run_id)
-- rather than part of ORDER BY. The AggregatingMergeTree still merges it deterministically via anyLast.
-- DROP first because an earlier version of this migration added source as a plain defaulted column.
ALTER TABLE corpscout.commoncrawl_domain_dns_records
    DROP COLUMN IF EXISTS source;

ALTER TABLE corpscout.commoncrawl_domain_dns_records
    ADD COLUMN IF NOT EXISTS source SimpleAggregateFunction(anyLast, LowCardinality(String));
