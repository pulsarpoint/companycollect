CREATE DATABASE IF NOT EXISTS corpscout;

ALTER TABLE corpscout.dns_axfr_latest
    DROP COLUMN IF EXISTS last_probe_records,
    DROP COLUMN IF EXISTS last_probe_bytes,
    DROP COLUMN IF EXISTS last_probe_truncated;
