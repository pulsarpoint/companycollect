-- 000002 up — add an ingestion-time watermark to the distinct-hostname store.
-- Existing rows use the Unix epoch as an explicit pre-watermark sentinel. New
-- observations receive their actual ingestion time from the CT log writer.

ALTER TABLE ctlogs.hostnames
    ADD COLUMN IF NOT EXISTS last_ingested_at  SimpleAggregateFunction(max, DateTime64(3, 'UTC')) DEFAULT toDateTime64(0, 3, 'UTC')
    AFTER last_seen;
