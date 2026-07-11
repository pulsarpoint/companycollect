CREATE DATABASE IF NOT EXISTS corpscout;

ALTER TABLE corpscout.dns_axfr_latest
    ADD COLUMN IF NOT EXISTS last_probe_records UInt64 DEFAULT 0 AFTER last_probed_at,
    ADD COLUMN IF NOT EXISTS last_probe_bytes UInt64 DEFAULT 0 AFTER last_probe_records,
    ADD COLUMN IF NOT EXISTS last_probe_truncated UInt8 DEFAULT 0 AFTER last_probe_bytes;
