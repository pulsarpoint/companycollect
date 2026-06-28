CREATE DATABASE IF NOT EXISTS corpscout;

ALTER TABLE corpscout.fi_financial_metrics
    MODIFY COLUMN employees Nullable(Decimal(38, 6)),
    DROP COLUMN IF EXISTS xml_size_bytes,
    DROP COLUMN IF EXISTS xml_sha256,
    DROP COLUMN IF EXISTS xml_object_key,
    DROP COLUMN IF EXISTS source_url,
    DROP COLUMN IF EXISTS reported_company_name,
    DROP COLUMN IF EXISTS registration_date;
