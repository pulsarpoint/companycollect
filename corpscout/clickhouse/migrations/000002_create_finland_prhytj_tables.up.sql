CREATE TABLE IF NOT EXISTS `corpscout_sources`.`fi_prhytj_addresses` (
  `country_iso2` Nullable(String),
  `source_slug` Nullable(String),
  `source_run_id` Nullable(String),
  `source_record_id` Nullable(String),
  `source_item_hash` Nullable(String),
  `business_id` Nullable(String),
  `source_position` Nullable(Int32),
  `address_type_code` Nullable(Int32),
  `address_type` Nullable(String),
  `street` Nullable(String),
  `building_number` Nullable(String),
  `entrance` Nullable(String),
  `apartment_number` Nullable(String),
  `post_office_box` Nullable(String),
  `co` Nullable(String),
  `post_code` Nullable(String),
  `city_fi` Nullable(String),
  `city_sv` Nullable(String),
  `municipality_code` Nullable(String),
  `country` Nullable(String),
  `registered_on` Nullable(String),
  `ingested_at` DateTime64(3, 'UTC'),
  `source_export_id` UUID
)
ENGINE = ReplacingMergeTree
ORDER BY (`business_id`, `address_type_code`, `source_position`, `source_item_hash`)
SETTINGS allow_nullable_key = 1;

CREATE TABLE IF NOT EXISTS `corpscout_sources`.`fi_prhytj_companies` (
  `country_iso2` Nullable(String),
  `source_slug` Nullable(String),
  `source_run_id` Nullable(String),
  `source_record_id` Nullable(String),
  `source_native_id` Nullable(String),
  `source_payload_hash` Nullable(String),
  `source_updated_at` Nullable(String),
  `exported_at` Nullable(String),
  `schema_version` Nullable(String),
  `business_id` Nullable(String),
  `vat_id` Nullable(String),
  `euid` Nullable(String),
  `legal_name` Nullable(String),
  `legal_name_normalized` Nullable(String),
  `lifecycle_status` Nullable(String),
  `is_active` Nullable(Bool),
  `legal_form_code` Nullable(String),
  `legal_form_label` Nullable(String),
  `legal_form_label_en` Nullable(String),
  `primary_industry_code` Nullable(String),
  `primary_industry_code_set` Nullable(String),
  `primary_industry_label` Nullable(String),
  `primary_industry_label_en` Nullable(String),
  `primary_nace_code` Nullable(String),
  `primary_nace_revision` Nullable(String),
  `website_url` Nullable(String),
  `website_normalized_url` Nullable(String),
  `website_host` Nullable(String),
  `ingested_at` DateTime64(3, 'UTC'),
  `source_export_id` UUID
)
ENGINE = ReplacingMergeTree
ORDER BY (`business_id`, `source_run_id`)
SETTINGS allow_nullable_key = 1;

CREATE TABLE IF NOT EXISTS `corpscout_sources`.`fi_prhytj_company_names` (
  `country_iso2` Nullable(String),
  `source_slug` Nullable(String),
  `source_run_id` Nullable(String),
  `source_record_id` Nullable(String),
  `source_item_hash` Nullable(String),
  `business_id` Nullable(String),
  `source_position` Nullable(Int32),
  `name` Nullable(String),
  `name_type_code` Nullable(String),
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

CREATE TABLE IF NOT EXISTS `corpscout_sources`.`fi_prhytj_industries` (
  `country_iso2` Nullable(String),
  `source_slug` Nullable(String),
  `source_run_id` Nullable(String),
  `source_record_id` Nullable(String),
  `source_item_hash` Nullable(String),
  `business_id` Nullable(String),
  `source_industry_code` Nullable(String),
  `source_industry_code_set` Nullable(String),
  `source_industry_label` Nullable(String),
  `source_industry_label_en` Nullable(String),
  `source_industry_label_fi` Nullable(String),
  `source_industry_label_sv` Nullable(String),
  `mapped_nace_code` Nullable(String),
  `nace_revision` Nullable(String),
  `is_primary` Nullable(Bool),
  `ingested_at` DateTime64(3, 'UTC'),
  `source_export_id` UUID
)
ENGINE = ReplacingMergeTree
ORDER BY (`business_id`, `source_industry_code`, `source_item_hash`)
SETTINGS allow_nullable_key = 1;

CREATE TABLE IF NOT EXISTS `corpscout_sources`.`fi_prhytj_legal_forms` (
  `country_iso2` Nullable(String),
  `source_slug` Nullable(String),
  `source_run_id` Nullable(String),
  `source_record_id` Nullable(String),
  `source_item_hash` Nullable(String),
  `business_id` Nullable(String),
  `legal_form_code` Nullable(String),
  `legal_form_label` Nullable(String),
  `legal_form_label_en` Nullable(String),
  `legal_form_label_fi` Nullable(String),
  `legal_form_label_sv` Nullable(String),
  `registered_on` Nullable(String),
  `ended_on` Nullable(String),
  `ingested_at` DateTime64(3, 'UTC'),
  `source_export_id` UUID
)
ENGINE = ReplacingMergeTree
ORDER BY (`business_id`, `registered_on`, `legal_form_code`, `source_item_hash`)
SETTINGS allow_nullable_key = 1;

CREATE TABLE IF NOT EXISTS `corpscout_sources`.`fi_prhytj_raw_records` (
  `country_iso2` Nullable(String),
  `source_slug` Nullable(String),
  `source_run_id` Nullable(String),
  `source_record_id` Nullable(String),
  `business_id` Nullable(String),
  `source_payload_hash` Nullable(String),
  `snapshot_path` Nullable(String),
  `snapshot_sha256` Nullable(String),
  `snapshot_line_number` Nullable(Int64),
  `raw_payload_json` Nullable(String),
  `schema_version` Nullable(String),
  `exported_at` Nullable(String),
  `ingested_at` DateTime64(3, 'UTC'),
  `source_export_id` UUID
)
ENGINE = ReplacingMergeTree
ORDER BY (`source_run_id`, `business_id`, `source_payload_hash`)
SETTINGS allow_nullable_key = 1;

CREATE TABLE IF NOT EXISTS `corpscout_sources`.`fi_prhytj_registered_entries` (
  `country_iso2` Nullable(String),
  `source_slug` Nullable(String),
  `source_run_id` Nullable(String),
  `source_record_id` Nullable(String),
  `source_item_hash` Nullable(String),
  `business_id` Nullable(String),
  `register_code` Nullable(String),
  `register_label` Nullable(String),
  `authority` Nullable(String),
  `entry_type_code` Nullable(String),
  `entry_type_label` Nullable(String),
  `entry_type_label_en` Nullable(String),
  `registered_on` Nullable(String),
  `ended_on` Nullable(String),
  `is_current` Nullable(Bool),
  `ingested_at` DateTime64(3, 'UTC'),
  `source_export_id` UUID
)
ENGINE = ReplacingMergeTree
ORDER BY (`business_id`, `register_code`, `entry_type_code`, `registered_on`, `source_item_hash`)
SETTINGS allow_nullable_key = 1;

CREATE TABLE IF NOT EXISTS `corpscout_sources`.`fi_prhytj_tax_registrations` (
  `country_iso2` Nullable(String),
  `source_slug` Nullable(String),
  `source_run_id` Nullable(String),
  `source_record_id` Nullable(String),
  `source_item_hash` Nullable(String),
  `business_id` Nullable(String),
  `registration_type` Nullable(String),
  `register_code` Nullable(String),
  `current_registered` Nullable(Bool),
  `first_registered_on` Nullable(String),
  `ended_on` Nullable(String),
  `ingested_at` DateTime64(3, 'UTC'),
  `source_export_id` UUID
)
ENGINE = ReplacingMergeTree
ORDER BY (`business_id`, `registration_type`, `register_code`, `source_item_hash`)
SETTINGS allow_nullable_key = 1;

CREATE TABLE IF NOT EXISTS `corpscout_sources`.`fi_prhytj_websites` (
  `country_iso2` Nullable(String),
  `source_slug` Nullable(String),
  `source_run_id` Nullable(String),
  `source_record_id` Nullable(String),
  `source_item_hash` Nullable(String),
  `business_id` Nullable(String),
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

