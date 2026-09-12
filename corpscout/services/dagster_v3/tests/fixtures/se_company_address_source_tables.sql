-- Harness fixture only -- not a migration, never apply to a real ClickHouse.
-- corpscout.se_ratsit_establishments: migration 000343's CREATE with 000346's eleven v2
-- columns (the NACE block and the employee range) inlined in their ALTER order -- CODECs and
-- CONSTRAINTs stripped like the neighbouring snapshots. It cannot ride a migration replay:
-- 000343 also creates se_ratsit_company with CHECK constraints the other fixtures' rows do
-- not satisfy, and 000346 only ALTERs this table. se_ratsit_company needs no entry here --
-- its DDL is already in se_basic_info_source_tables.sql, which both tests also load.
CREATE TABLE IF NOT EXISTS corpscout.se_ratsit_establishments (
    `company_id` String,
    `result_sha256` FixedString(64),
    `normalizer_version` LowCardinality(String),
    `establishment_index` UInt16,
    `name` Nullable(String),
    `identifier` Nullable(String),
    `industry_code` Nullable(String),
    `industry_description` Nullable(String),
    `source_industry_code` Nullable(String),
    `source_industry_code_set` LowCardinality(String) DEFAULT '',
    `industry_description_original` Nullable(String),
    `nace_revision` LowCardinality(String) DEFAULT '',
    `nace_code` Nullable(String),
    `nace_normalized_code` Nullable(String),
    `nace_mapping_method` LowCardinality(String) DEFAULT '',
    `nace_mapping_status` LowCardinality(String) DEFAULT '',
    `address_street` Nullable(String),
    `address_postal_code` Nullable(String),
    `address_locality` Nullable(String),
    `address_county` Nullable(String),
    `number_of_employees_raw` Nullable(String),
    `number_of_employees` Nullable(UInt32),
    `employee_count_min` Nullable(UInt32),
    `employee_count_max` Nullable(UInt32),
    `employee_count_open_ended` Bool DEFAULT false,
    `normalized_at` DateTime64(6, 'UTC')
) ENGINE = ReplacingMergeTree(normalized_at)
ORDER BY (company_id, result_sha256, normalizer_version, establishment_index);
