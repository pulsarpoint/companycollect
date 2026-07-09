-- Remove the source provenance column. It is a non-key data column, so a plain DROP COLUMN reverses
-- the up cleanly and non-destructively (no table recreate needed).
ALTER TABLE corpscout.commoncrawl_domain_dns_records
    DROP COLUMN IF EXISTS source;
