CREATE DATABASE IF NOT EXISTS corpscout;

-- Extend raw DNS observations so every standard or future RR type can be retained losslessly.
-- All columns are added without DEFAULT expressions in the same ALTER that appends identity columns
-- to ORDER BY. ClickHouse can apply this metadata-only: old parts read zero/empty values, explicitly
-- meaning that protocol metadata and AXFR endpoint provenance were not captured for legacy rows.
ALTER TABLE corpscout.commoncrawl_domain_dns_record_observations
    ADD COLUMN IF NOT EXISTS record_type_code UInt16 AFTER record_type,
    ADD COLUMN IF NOT EXISTS record_class_code UInt16 AFTER record_type_code,
    ADD COLUMN IF NOT EXISTS rdata_wire String AFTER value,
    ADD COLUMN IF NOT EXISTS name_server String AFTER discovery,
    ADD COLUMN IF NOT EXISTS name_server_ip String AFTER name_server,
    MODIFY ORDER BY
    (
        root_domain,
        name,
        record_type,
        slot,
        value,
        source,
        discovery,
        scan_id,
        record_type_code,
        record_class_code,
        name_server,
        name_server_ip
    );
