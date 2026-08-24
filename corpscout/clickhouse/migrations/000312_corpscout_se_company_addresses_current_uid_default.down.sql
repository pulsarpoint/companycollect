-- Drop only the DEFAULT clause. Backfilled values remain (they are correct content).
ALTER TABLE corpscout.se_company_addresses_current
    MODIFY COLUMN source_record_uid String;
