CREATE DATABASE IF NOT EXISTS corpscout;

-- commoncrawl_domain_hostname_sync_state removed on 2026-09-03: unused, dropped by hand (development-phase ledger policy).

ALTER TABLE corpscout.commoncrawl_domain_hostnames
    DROP COLUMN IF EXISTS last_not_after;
