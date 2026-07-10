CREATE DATABASE IF NOT EXISTS corpscout;

-- Preserve the authoritative NS hostname-to-address identity and the scanner's explicit target policy
-- in the durable latest-scan table. Parallel arrays keep each index as one endpoint while avoiding
-- driver-specific nested-struct encoding. Private/special-use endpoints remain evidence with dialable=0.
ALTER TABLE corpscout.commoncrawl_domain_dns_scan
    ADD COLUMN IF NOT EXISTS ns_endpoint_names Array(String) DEFAULT [],
    ADD COLUMN IF NOT EXISTS ns_endpoint_ips Array(String) DEFAULT [],
    ADD COLUMN IF NOT EXISTS ns_endpoint_scopes Array(String) DEFAULT [],
    ADD COLUMN IF NOT EXISTS ns_endpoint_dialable Array(UInt8) DEFAULT [];
