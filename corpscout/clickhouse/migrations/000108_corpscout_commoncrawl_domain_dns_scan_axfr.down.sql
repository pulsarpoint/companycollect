-- Drop the AXFR columns (non-key, so removable without recreating the table).
ALTER TABLE corpscout.commoncrawl_domain_dns_scan
    DROP COLUMN IF EXISTS axfr_open,
    DROP COLUMN IF EXISTS axfr_records,
    DROP COLUMN IF EXISTS axfr_truncated,
    DROP COLUMN IF EXISTS axfr_server;
