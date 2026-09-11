-- Production SHOW CREATE TABLE snapshot (2026-09-09), SETTINGS stripped.
-- Harness fixture only -- not a migration, never apply to a real ClickHouse.
-- The MATERIALIZED and DEFAULT expressions are load-bearing: the Bolagsverket extractor's
-- slot is source_record_uid + signatory_uid, and both are computed by ClickHouse here.

CREATE TABLE IF NOT EXISTS corpscout.se_financial_report_signatories (
    `company_id` String,
    `fiscal_year` Int32,
    `statement_key` String,
    `source_record_uid` String DEFAULT lower(hex(SHA256(concat('company-source-record-v1\nstructured\nsweden_financial\nannual_report_xhtml\n', statement_key, '\n', statement_key)))),
    `signatory_kind` LowCardinality(String),
    `person_seq` UInt16,
    `signatory_uid` FixedString(64) MATERIALIZED lower(hex(SHA256(concat('sweden-financial-report-signatory-v1\n', company_id, '\n', statement_key, '\n', signatory_kind, '\n', toString(person_seq))))),
    `first_name` String,
    `last_name` String,
    `person_profile_hash` FixedString(64) MATERIALIZED lower(hex(SHA256(concat('company-person-profile-v1\n', toString(length(lowerUTF8(trimBoth(first_name)))), ':', lowerUTF8(trimBoth(first_name)), '\n', toString(length(lowerUTF8(trimBoth(last_name)))), ':', lowerUTF8(trimBoth(last_name)))))),
    `role_original` String,
    `role_kind` LowCardinality(String),
    `person_role_hash` FixedString(64) MATERIALIZED lower(hex(SHA256(concat('company-person-role-v1\n', toString(length(lowerUTF8(trimBoth(role_original)))), ':', lowerUTF8(trimBoth(role_original)), '\n', toString(length(lowerUTF8(trimBoth(role_kind)))), ':', lowerUTF8(trimBoth(role_kind)), '\n', toString(length(lowerUTF8(trimBoth(signatory_kind)))), ':', lowerUTF8(trimBoth(signatory_kind)), '\n', toString(fiscal_year))))),
    `resolved_at` DateTime64(3, 'UTC')
) ENGINE = MergeTree ORDER BY (company_id, fiscal_year, statement_key, signatory_kind, person_seq);

CREATE TABLE IF NOT EXISTS corpscout.wikidata_company_people (
    `company_wikidata_id` String,
    `person_wikidata_id` String,
    `role_property` LowCardinality(String),
    `role_label` LowCardinality(String),
    `start_date` Nullable(Date),
    `end_date` Nullable(Date),
    `is_current` UInt8,
    `source_system` LowCardinality(String),
    `source_run_id` String,
    `source_record_id` String,
    `source_payload_hash` FixedString(64),
    `person_role_hash` FixedString(64) MATERIALIZED lower(hex(SHA256(concat('company-person-role-v1\n', toString(length(lowerUTF8(trimBoth(role_property)))), ':', lowerUTF8(trimBoth(role_property)), '\n', toString(length(lowerUTF8(trimBoth(role_label)))), ':', lowerUTF8(trimBoth(role_label)), '\n', ifNull(toString(start_date), ''), '\n', ifNull(toString(end_date), ''), '\n', toString(is_current))))),
    `retrieved_at` DateTime64(3, 'UTC'),
    `resolved_at` DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(resolved_at) ORDER BY (company_wikidata_id, role_property, person_wikidata_id);

CREATE TABLE IF NOT EXISTS corpscout.wikidata_persons (
    `person_wikidata_id` String,
    `source_record_uid` String DEFAULT lower(hex(SHA256(concat('company-source-record-v1\nstructured\nwikidata\nwikidata_person_item\n', person_wikidata_id, '\n', lowerUTF8(toString(source_payload_hash)))))),
    `person_profile_hash` FixedString(64) MATERIALIZED lower(hex(SHA256(concat('company-person-profile-v1\n', toString(length(lowerUTF8(trimBoth(name)))), ':', lowerUTF8(trimBoth(name)), '\n', toString(length(lowerUTF8(trimBoth(ifNull(description, ''))))), ':', lowerUTF8(trimBoth(ifNull(description, ''))))))),
    `name` String,
    `name_normalized` String,
    `description` Nullable(String),
    `birth_year` Nullable(UInt16),
    `image_url` Nullable(String),
    `wikidata_url` Nullable(String),
    `source_system` LowCardinality(String),
    `source_run_id` String,
    `source_record_id` String,
    `source_payload_hash` FixedString(64),
    `retrieved_at` DateTime64(3, 'UTC'),
    `resolved_at` DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(resolved_at) ORDER BY (person_wikidata_id);

-- corpscout.se_ratsit_responsible_people: migration 000343's CREATE with 000346's four v2
-- columns (display_name_raw, name, age, identity_available) inlined in their ALTER order;
-- CODECs and CONSTRAINTs stripped like the neighbouring snapshots. It cannot ride
-- WANTED_CREATES: that helper lifts a whole CREATE TABLE statement out of ONE named
-- migration, and 000346 only ALTERs this table. se_ratsit_company needs no entry -- its DDL
-- is already in se_basic_info_source_tables.sql, which this test also loads.
CREATE TABLE IF NOT EXISTS corpscout.se_ratsit_responsible_people (
    `company_id` String,
    `result_sha256` FixedString(64),
    `normalizer_version` LowCardinality(String),
    `person_index` UInt16,
    `display_name` Nullable(String),
    `display_name_raw` Nullable(String),
    `name` Nullable(String),
    `age` Nullable(UInt16),
    `identity_available` Bool DEFAULT false,
    `role` Nullable(String),
    `profile_url` Nullable(String),
    `normalized_at` DateTime64(6, 'UTC')
) ENGINE = ReplacingMergeTree(normalized_at) ORDER BY (company_id, result_sha256, normalizer_version, person_index);
