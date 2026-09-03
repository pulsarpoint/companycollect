CREATE DATABASE IF NOT EXISTS corpscout;

-- fi_tax_registrations and fi_company_situations removed on 2026-09-03: unused, dropped by hand (development-phase ledger policy).

ALTER TABLE corpscout.fi_companies
    ADD COLUMN IF NOT EXISTS business_id_registration_date Nullable(Date) AFTER business_id,
    ADD COLUMN IF NOT EXISTS eu_id Nullable(String) AFTER business_id_registration_date,
    ADD COLUMN IF NOT EXISTS vat_id Nullable(String) AFTER eu_id,
    ADD COLUMN IF NOT EXISTS trade_register_status LowCardinality(String) AFTER is_active,
    ADD COLUMN IF NOT EXISTS raw_status_code Nullable(String) AFTER trade_register_status,
    ADD COLUMN IF NOT EXISTS last_modified Nullable(DateTime64(3, 'UTC')) AFTER raw_status_code,
    ADD COLUMN IF NOT EXISTS is_vat_registered UInt8 DEFAULT 0 AFTER last_modified,
    ADD COLUMN IF NOT EXISTS is_employer_registered UInt8 DEFAULT 0 AFTER is_vat_registered,
    ADD COLUMN IF NOT EXISTS is_prepayment_registered UInt8 DEFAULT 0 AFTER is_employer_registered;

CREATE TABLE IF NOT EXISTS corpscout.fi_names
(
    business_id String,
    name String,
    name_type_code LowCardinality(String),
    name_type_description_original Nullable(String),
    name_type_description_en Nullable(String),
    registration_date Nullable(Date),
    end_date Nullable(Date),
    version Nullable(UInt32),
    is_current UInt8,
    is_primary UInt8,
    source_code Nullable(String),
    source_system LowCardinality(String),
    source_run_id String,
    source_record_id String,
    source_payload_hash FixedString(64),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (business_id, name_type_code, name);

CREATE TABLE IF NOT EXISTS corpscout.fi_addresses
(
    business_id String,
    address_type_code UInt8,
    street Nullable(String),
    post_code Nullable(String),
    city Nullable(String),
    city_language_code Nullable(String),
    municipality_code Nullable(String),
    post_office_box Nullable(String),
    building_number Nullable(String),
    entrance Nullable(String),
    apartment_number Nullable(String),
    apartment_id_suffix Nullable(String),
    co Nullable(String),
    country Nullable(String),
    free_address_line Nullable(String),
    registration_date Nullable(Date),
    source_code Nullable(String),
    source_system LowCardinality(String),
    source_run_id String,
    source_record_id String,
    source_payload_hash FixedString(64),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (business_id, address_type_code, source_record_id);

CREATE TABLE IF NOT EXISTS corpscout.fi_legal_forms
(
    business_id String,
    legal_form_code LowCardinality(String),
    description_original Nullable(String),
    description_language Nullable(String),
    description_en Nullable(String),
    registration_date Nullable(Date),
    end_date Nullable(Date),
    version Nullable(UInt32),
    is_current UInt8,
    source_code Nullable(String),
    source_system LowCardinality(String),
    source_run_id String,
    source_record_id String,
    source_payload_hash FixedString(64),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (business_id, legal_form_code);

CREATE TABLE IF NOT EXISTS corpscout.fi_registered_entries
(
    business_id String,
    entry_type_code LowCardinality(String),
    entry_type_description_original Nullable(String),
    entry_type_description_en Nullable(String),
    register_code LowCardinality(String),
    authority_code Nullable(String),
    registration_date Nullable(Date),
    end_date Nullable(Date),
    is_current UInt8,
    source_system LowCardinality(String),
    source_run_id String,
    source_record_id String,
    source_payload_hash FixedString(64),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (business_id, register_code, entry_type_code);
