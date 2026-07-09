CREATE DATABASE IF NOT EXISTS corpscout;

-- Add the AXFR probe outputs to the scan summary: the open-zone-transfer flag, record/truncation
-- counts, and the NS IP that answered the transfer (empty when the zone is closed). These are plain
-- data columns, not in the ReplacingMergeTree sort key, so a simple ADD COLUMN suffices.
ALTER TABLE corpscout.commoncrawl_domain_dns_scan
    ADD COLUMN IF NOT EXISTS axfr_open UInt8 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS axfr_records UInt32 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS axfr_truncated UInt8 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS axfr_server String DEFAULT '';
