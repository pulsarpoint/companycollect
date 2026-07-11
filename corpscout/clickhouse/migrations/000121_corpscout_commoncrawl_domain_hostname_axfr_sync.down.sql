CREATE DATABASE IF NOT EXISTS corpscout;

ALTER TABLE corpscout.commoncrawl_domain_hostname_sync_state
    DROP COLUMN IF EXISTS axfr_loaded_through;
