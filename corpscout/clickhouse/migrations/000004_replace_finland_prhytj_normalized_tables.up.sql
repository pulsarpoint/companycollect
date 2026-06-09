DROP TABLE IF EXISTS `corpscout_sources`.`fi_prhytj_raw_records`;
DROP TABLE IF EXISTS `corpscout_sources`.`fi_prhytj_companies`;
DROP TABLE IF EXISTS `corpscout_sources`.`fi_prhytj_addresses`;
DROP TABLE IF EXISTS `corpscout_sources`.`fi_prhytj_company_names`;
DROP TABLE IF EXISTS `corpscout_sources`.`fi_prhytj_industries`;
DROP TABLE IF EXISTS `corpscout_sources`.`fi_prhytj_legal_forms`;
DROP TABLE IF EXISTS `corpscout_sources`.`fi_prhytj_registered_entries`;
DROP TABLE IF EXISTS `corpscout_sources`.`fi_prhytj_tax_registrations`;
DROP TABLE IF EXISTS `corpscout_sources`.`fi_prhytj_websites`;

CREATE TABLE IF NOT EXISTS `corpscout_sources`.`fi_prhytj_identifiers` (
  `country_iso2` Nullable(String),
  `source_slug` Nullable(String),
  `source_run_id` Nullable(String),
  `source_record_id` Nullable(String),
  `business_id` Nullable(String),
  `source_line_number` Nullable(Int64),
  `source_payload_hash` Nullable(String),
  `source_item_hash` Nullable(String),
  `source_position` Nullable(Int32),
  `identifier_scope` Nullable(String),
  `identifier_type` Nullable(String),
  `identifier_value` Nullable(String),
  `registered_on` Nullable(String),
  `ended_on` Nullable(String),
  `source` Nullable(String),
  `is_primary_business_id` Nullable(Bool),
  `ingested_at` DateTime64(3, 'UTC'),
  `source_export_id` UUID
)
ENGINE = ReplacingMergeTree
ORDER BY (`business_id`, `identifier_scope`, `identifier_value`, `source_item_hash`)
SETTINGS allow_nullable_key = 1;

CREATE TABLE IF NOT EXISTS `corpscout_sources`.`fi_prhytj_statuses` (
  `country_iso2` Nullable(String),
  `source_slug` Nullable(String),
  `source_run_id` Nullable(String),
  `source_record_id` Nullable(String),
  `business_id` Nullable(String),
  `source_line_number` Nullable(Int64),
  `source_payload_hash` Nullable(String),
  `trade_register_status` Nullable(String),
  `status` Nullable(String),
  `registration_date` Nullable(String),
  `end_date` Nullable(String),
  `last_modified` Nullable(String),
  `lifecycle_status` Nullable(String),
  `is_active` Nullable(Bool),
  `ingested_at` DateTime64(3, 'UTC'),
  `source_export_id` UUID
)
ENGINE = ReplacingMergeTree
ORDER BY (`business_id`, `source_run_id`, `source_payload_hash`)
SETTINGS allow_nullable_key = 1;

CREATE TABLE IF NOT EXISTS `corpscout_sources`.`fi_prhytj_names` (
  `country_iso2` Nullable(String),
  `source_slug` Nullable(String),
  `source_run_id` Nullable(String),
  `source_record_id` Nullable(String),
  `business_id` Nullable(String),
  `source_line_number` Nullable(Int64),
  `source_payload_hash` Nullable(String),
  `source_item_hash` Nullable(String),
  `source_position` Nullable(Int32),
  `name` Nullable(String),
  `name_type_code` Nullable(String),
  `version` Nullable(Int32),
  `registered_on` Nullable(String),
  `ended_on` Nullable(String),
  `is_current` Nullable(Bool),
  `is_primary` Nullable(Bool),
  `ingested_at` DateTime64(3, 'UTC'),
  `source_export_id` UUID
)
ENGINE = ReplacingMergeTree
ORDER BY (`business_id`, `source_position`, `source_item_hash`)
SETTINGS allow_nullable_key = 1;

CREATE TABLE IF NOT EXISTS `corpscout_sources`.`fi_prhytj_business_lines` (
  `country_iso2` Nullable(String),
  `source_slug` Nullable(String),
  `source_run_id` Nullable(String),
  `source_record_id` Nullable(String),
  `business_id` Nullable(String),
  `source_line_number` Nullable(Int64),
  `source_payload_hash` Nullable(String),
  `source_item_hash` Nullable(String),
  `business_line_type` Nullable(String),
  `business_line_code_set` Nullable(String),
  `registered_on` Nullable(String),
  `source` Nullable(String),
  `is_primary` Nullable(Bool),
  `ingested_at` DateTime64(3, 'UTC'),
  `source_export_id` UUID
)
ENGINE = ReplacingMergeTree
ORDER BY (`business_id`, `source_item_hash`)
SETTINGS allow_nullable_key = 1;

CREATE TABLE IF NOT EXISTS `corpscout_sources`.`fi_prhytj_business_line_descriptions` (
  `country_iso2` Nullable(String),
  `source_slug` Nullable(String),
  `source_run_id` Nullable(String),
  `source_record_id` Nullable(String),
  `business_id` Nullable(String),
  `source_line_number` Nullable(Int64),
  `source_payload_hash` Nullable(String),
  `source_item_hash` Nullable(String),
  `business_line_item_hash` Nullable(String),
  `source_position` Nullable(Int32),
  `language_code` Nullable(String),
  `description` Nullable(String),
  `ingested_at` DateTime64(3, 'UTC'),
  `source_export_id` UUID
)
ENGINE = ReplacingMergeTree
ORDER BY (`business_id`, `business_line_item_hash`, `language_code`, `source_item_hash`)
SETTINGS allow_nullable_key = 1;

CREATE TABLE IF NOT EXISTS `corpscout_sources`.`fi_prhytj_websites` (
  `country_iso2` Nullable(String),
  `source_slug` Nullable(String),
  `source_run_id` Nullable(String),
  `source_record_id` Nullable(String),
  `business_id` Nullable(String),
  `source_line_number` Nullable(Int64),
  `source_payload_hash` Nullable(String),
  `source_item_hash` Nullable(String),
  `url` Nullable(String),
  `normalized_url` Nullable(String),
  `host` Nullable(String),
  `path` Nullable(String),
  `registered_on` Nullable(String),
  `ended_on` Nullable(String),
  `is_current` Nullable(Bool),
  `is_primary` Nullable(Bool),
  `ingested_at` DateTime64(3, 'UTC'),
  `source_export_id` UUID
)
ENGINE = ReplacingMergeTree
ORDER BY (`host`, `business_id`, `source_item_hash`)
SETTINGS allow_nullable_key = 1;

CREATE TABLE IF NOT EXISTS `corpscout_sources`.`fi_prhytj_company_forms` (
  `country_iso2` Nullable(String),
  `source_slug` Nullable(String),
  `source_run_id` Nullable(String),
  `source_record_id` Nullable(String),
  `business_id` Nullable(String),
  `source_line_number` Nullable(Int64),
  `source_payload_hash` Nullable(String),
  `source_item_hash` Nullable(String),
  `source_position` Nullable(Int32),
  `form_type_code` Nullable(String),
  `version` Nullable(Int32),
  `registered_on` Nullable(String),
  `ended_on` Nullable(String),
  `source` Nullable(String),
  `is_current` Nullable(Bool),
  `ingested_at` DateTime64(3, 'UTC'),
  `source_export_id` UUID
)
ENGINE = ReplacingMergeTree
ORDER BY (`business_id`, `source_position`, `form_type_code`, `source_item_hash`)
SETTINGS allow_nullable_key = 1;

CREATE TABLE IF NOT EXISTS `corpscout_sources`.`fi_prhytj_company_form_descriptions` (
  `country_iso2` Nullable(String),
  `source_slug` Nullable(String),
  `source_run_id` Nullable(String),
  `source_record_id` Nullable(String),
  `business_id` Nullable(String),
  `source_line_number` Nullable(Int64),
  `source_payload_hash` Nullable(String),
  `source_item_hash` Nullable(String),
  `company_form_item_hash` Nullable(String),
  `source_position` Nullable(Int32),
  `language_code` Nullable(String),
  `description` Nullable(String),
  `ingested_at` DateTime64(3, 'UTC'),
  `source_export_id` UUID
)
ENGINE = ReplacingMergeTree
ORDER BY (`business_id`, `company_form_item_hash`, `language_code`, `source_item_hash`)
SETTINGS allow_nullable_key = 1;

CREATE TABLE IF NOT EXISTS `corpscout_sources`.`fi_prhytj_company_situations` (
  `country_iso2` Nullable(String),
  `source_slug` Nullable(String),
  `source_run_id` Nullable(String),
  `source_record_id` Nullable(String),
  `business_id` Nullable(String),
  `source_line_number` Nullable(Int64),
  `source_payload_hash` Nullable(String),
  `source_item_hash` Nullable(String),
  `source_position` Nullable(Int32),
  `situation_type_code` Nullable(String),
  `registered_on` Nullable(String),
  `ended_on` Nullable(String),
  `is_current` Nullable(Bool),
  `ingested_at` DateTime64(3, 'UTC'),
  `source_export_id` UUID
)
ENGINE = ReplacingMergeTree
ORDER BY (`business_id`, `source_position`, `situation_type_code`, `source_item_hash`)
SETTINGS allow_nullable_key = 1;

CREATE TABLE IF NOT EXISTS `corpscout_sources`.`fi_prhytj_company_situation_descriptions` (
  `country_iso2` Nullable(String),
  `source_slug` Nullable(String),
  `source_run_id` Nullable(String),
  `source_record_id` Nullable(String),
  `business_id` Nullable(String),
  `source_line_number` Nullable(Int64),
  `source_payload_hash` Nullable(String),
  `source_item_hash` Nullable(String),
  `company_situation_item_hash` Nullable(String),
  `source_position` Nullable(Int32),
  `language_code` Nullable(String),
  `description` Nullable(String),
  `ingested_at` DateTime64(3, 'UTC'),
  `source_export_id` UUID
)
ENGINE = ReplacingMergeTree
ORDER BY (`business_id`, `company_situation_item_hash`, `language_code`, `source_item_hash`)
SETTINGS allow_nullable_key = 1;

CREATE TABLE IF NOT EXISTS `corpscout_sources`.`fi_prhytj_registered_entries` (
  `country_iso2` Nullable(String),
  `source_slug` Nullable(String),
  `source_run_id` Nullable(String),
  `source_record_id` Nullable(String),
  `business_id` Nullable(String),
  `source_line_number` Nullable(Int64),
  `source_payload_hash` Nullable(String),
  `source_item_hash` Nullable(String),
  `source_position` Nullable(Int32),
  `entry_type_code` Nullable(String),
  `register_code` Nullable(String),
  `authority` Nullable(String),
  `registered_on` Nullable(String),
  `ended_on` Nullable(String),
  `is_current` Nullable(Bool),
  `ingested_at` DateTime64(3, 'UTC'),
  `source_export_id` UUID
)
ENGINE = ReplacingMergeTree
ORDER BY (`business_id`, `source_position`, `register_code`, `entry_type_code`, `source_item_hash`)
SETTINGS allow_nullable_key = 1;

CREATE TABLE IF NOT EXISTS `corpscout_sources`.`fi_prhytj_registered_entry_descriptions` (
  `country_iso2` Nullable(String),
  `source_slug` Nullable(String),
  `source_run_id` Nullable(String),
  `source_record_id` Nullable(String),
  `business_id` Nullable(String),
  `source_line_number` Nullable(Int64),
  `source_payload_hash` Nullable(String),
  `source_item_hash` Nullable(String),
  `registered_entry_item_hash` Nullable(String),
  `source_position` Nullable(Int32),
  `language_code` Nullable(String),
  `description` Nullable(String),
  `ingested_at` DateTime64(3, 'UTC'),
  `source_export_id` UUID
)
ENGINE = ReplacingMergeTree
ORDER BY (`business_id`, `registered_entry_item_hash`, `language_code`, `source_item_hash`)
SETTINGS allow_nullable_key = 1;

CREATE TABLE IF NOT EXISTS `corpscout_sources`.`fi_prhytj_addresses` (
  `country_iso2` Nullable(String),
  `source_slug` Nullable(String),
  `source_run_id` Nullable(String),
  `source_record_id` Nullable(String),
  `business_id` Nullable(String),
  `source_line_number` Nullable(Int64),
  `source_payload_hash` Nullable(String),
  `source_item_hash` Nullable(String),
  `source_position` Nullable(Int32),
  `address_type_code` Nullable(Int32),
  `street` Nullable(String),
  `post_code` Nullable(String),
  `building_number` Nullable(String),
  `entrance` Nullable(String),
  `apartment_number` Nullable(String),
  `post_office_box` Nullable(String),
  `co` Nullable(String),
  `country` Nullable(String),
  `registered_on` Nullable(String),
  `source` Nullable(String),
  `ingested_at` DateTime64(3, 'UTC'),
  `source_export_id` UUID
)
ENGINE = ReplacingMergeTree
ORDER BY (`business_id`, `source_position`, `address_type_code`, `source_item_hash`)
SETTINGS allow_nullable_key = 1;

CREATE TABLE IF NOT EXISTS `corpscout_sources`.`fi_prhytj_address_post_offices` (
  `country_iso2` Nullable(String),
  `source_slug` Nullable(String),
  `source_run_id` Nullable(String),
  `source_record_id` Nullable(String),
  `business_id` Nullable(String),
  `source_line_number` Nullable(Int64),
  `source_payload_hash` Nullable(String),
  `source_item_hash` Nullable(String),
  `address_item_hash` Nullable(String),
  `source_position` Nullable(Int32),
  `language_code` Nullable(String),
  `city` Nullable(String),
  `municipality_code` Nullable(String),
  `ingested_at` DateTime64(3, 'UTC'),
  `source_export_id` UUID
)
ENGINE = ReplacingMergeTree
ORDER BY (`business_id`, `address_item_hash`, `language_code`, `source_item_hash`)
SETTINGS allow_nullable_key = 1;
