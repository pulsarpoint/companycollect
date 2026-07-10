-- Drop the DS/DNSKEY outcome columns (non-key, so removable without recreating the table).
ALTER TABLE corpscout.commoncrawl_domain_dns_scan
    DROP COLUMN IF EXISTS ds_outcome,
    DROP COLUMN IF EXISTS dnskey_outcome;
