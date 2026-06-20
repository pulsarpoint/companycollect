ALTER TABLE corpscout.companies
    ADD COLUMN IF NOT EXISTS source_payload_hash FixedString(64),
    ADD COLUMN IF NOT EXISTS raw_entity String;

ALTER TABLE corpscout.financial_statements
    ADD COLUMN IF NOT EXISTS source_payload_hash FixedString(64),
    ADD COLUMN IF NOT EXISTS raw_financial_record String;

ALTER TABLE corpscout.no_companies ADD COLUMN IF NOT EXISTS source_payload_hash FixedString(64);
ALTER TABLE corpscout.no_websites ADD COLUMN IF NOT EXISTS source_payload_hash FixedString(64);
ALTER TABLE corpscout.no_industries ADD COLUMN IF NOT EXISTS source_payload_hash FixedString(64);
ALTER TABLE corpscout.no_financial_statements ADD COLUMN IF NOT EXISTS source_payload_hash FixedString(64);

ALTER TABLE corpscout.fi_companies ADD COLUMN IF NOT EXISTS source_payload_hash FixedString(64);
ALTER TABLE corpscout.fi_names ADD COLUMN IF NOT EXISTS source_payload_hash FixedString(64);
ALTER TABLE corpscout.fi_websites ADD COLUMN IF NOT EXISTS source_payload_hash FixedString(64);
ALTER TABLE corpscout.fi_industries ADD COLUMN IF NOT EXISTS source_payload_hash FixedString(64);
