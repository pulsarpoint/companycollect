CREATE DATABASE IF NOT EXISTS corpscout;

-- commoncrawl_domain_hostname_sync_state removed on 2026-09-03: unused, dropped by hand (development-phase ledger policy).

-- Retain certificate validity for ranking while preserving old and worker-written rows.
ALTER TABLE corpscout.commoncrawl_domain_hostnames
    ADD COLUMN IF NOT EXISTS last_not_after SimpleAggregateFunction(max, DateTime64(3, 'UTC'))
        DEFAULT toDateTime64(0, 3, 'UTC');
