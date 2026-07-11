CREATE DATABASE IF NOT EXISTS corpscout;

DROP TABLE IF EXISTS corpscout.commoncrawl_domain_hostname_sync_state;

ALTER TABLE corpscout.commoncrawl_domain_hostnames
    DROP COLUMN IF EXISTS last_not_after;
