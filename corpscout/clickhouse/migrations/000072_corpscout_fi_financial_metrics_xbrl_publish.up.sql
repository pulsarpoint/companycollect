CREATE DATABASE IF NOT EXISTS corpscout;

ALTER TABLE corpscout.fi_financial_metrics
    ADD COLUMN IF NOT EXISTS registration_date Nullable(Date) AFTER financial_date,
    ADD COLUMN IF NOT EXISTS reported_company_name Nullable(String) AFTER period_end,
    ADD COLUMN IF NOT EXISTS source_url Nullable(String) AFTER reported_company_name,
    ADD COLUMN IF NOT EXISTS xml_object_key Nullable(String) AFTER source_url,
    ADD COLUMN IF NOT EXISTS xml_sha256 Nullable(FixedString(64)) AFTER xml_object_key,
    ADD COLUMN IF NOT EXISTS xml_size_bytes Nullable(UInt64) AFTER xml_sha256,
    MODIFY COLUMN employees Nullable(UInt64);
