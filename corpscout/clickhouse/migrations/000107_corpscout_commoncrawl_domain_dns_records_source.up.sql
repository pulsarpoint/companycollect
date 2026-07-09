CREATE DATABASE IF NOT EXISTS corpscout;

-- Add source provenance (query vs axfr) to the distinct records table and extend the sort key so
-- query- and axfr-discovered copies of the same record stay distinct rows through the merge.
-- The worker's reflection-driven INSERT already emits this column, so this migration makes loads work.
ALTER TABLE corpscout.commoncrawl_domain_dns_records
    ADD COLUMN IF NOT EXISTS source LowCardinality(String) DEFAULT 'query';

ALTER TABLE corpscout.commoncrawl_domain_dns_records
    MODIFY ORDER BY (root_domain, record_type, slot, name, value, source);
