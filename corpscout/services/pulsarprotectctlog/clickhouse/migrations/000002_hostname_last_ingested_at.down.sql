-- 000002 down — remove the hostname ingestion-time watermark.

ALTER TABLE ctlogs.hostnames
    DROP COLUMN IF EXISTS last_ingested_at;
