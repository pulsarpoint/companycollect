ALTER TABLE corpscout.commoncrawl_domain_dns_scan
    DROP COLUMN IF EXISTS ns_endpoint_names,
    DROP COLUMN IF EXISTS ns_endpoint_ips,
    DROP COLUMN IF EXISTS ns_endpoint_scopes,
    DROP COLUMN IF EXISTS ns_endpoint_dialable;
