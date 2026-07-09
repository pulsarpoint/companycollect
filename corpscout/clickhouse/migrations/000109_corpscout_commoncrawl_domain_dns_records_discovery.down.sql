-- Remove the discovery column (non-key, so a plain DROP COLUMN reverses the up cleanly).
ALTER TABLE corpscout.commoncrawl_domain_dns_records
    DROP COLUMN IF EXISTS discovery;
