ALTER TABLE corpscout.lv_companies
    ADD COLUMN IF NOT EXISTS raw_entity String,
    ADD COLUMN IF NOT EXISTS source_payload_hash FixedString(64);

ALTER TABLE corpscout.lv_financial_statements
    ADD COLUMN IF NOT EXISTS raw_financial_record String,
    ADD COLUMN IF NOT EXISTS source_payload_hash FixedString(64);

ALTER TABLE corpscout.lv_financial_metrics
    ADD COLUMN IF NOT EXISTS source_payload_hash FixedString(64);
