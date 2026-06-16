CREATE DATABASE IF NOT EXISTS `corpscout_resolved`;

CREATE TABLE IF NOT EXISTS `corpscout_resolved`.`fi_companies` (
  `business_id` String,
  `country_iso2` FixedString(2),
  `name` String,
  `name_normalized` String,
  `registration_date` Nullable(Date),
  `end_date` Nullable(Date),
  `lifecycle_status` LowCardinality(String),
  `is_active` Bool,
  `legal_form_code` Nullable(String),
  `legal_form_description_original` Nullable(String),
  `legal_form_description_language` Nullable(String),
  `legal_form_description_en` Nullable(String),
  `legal_form_description_translated_at` Nullable(DateTime64(3, 'UTC')),
  `legal_form_description_translation_provider` Nullable(String),
  `legal_form_description_translation_model` Nullable(String),
  `primary_website_url` Nullable(String),
  `primary_website_host` Nullable(String),
  `source_system` LowCardinality(String),
  `source_run_id` String,
  `source_record_id` String,
  `source_payload_hash` FixedString(64),
  `resolved_at` DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(`resolved_at`)
ORDER BY (`country_iso2`, `business_id`);

CREATE TABLE IF NOT EXISTS `corpscout_resolved`.`fi_websites` (
  `business_id` String,
  `website_url` String,
  `website_normalized_url` String,
  `website_host` String,
  `website_path` String,
  `registered_on` Nullable(Date),
  `ended_on` Nullable(Date),
  `is_current` Bool,
  `is_primary` Bool,
  `source_system` LowCardinality(String),
  `source_run_id` String,
  `source_record_id` String,
  `source_payload_hash` FixedString(64),
  `resolved_at` DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(`resolved_at`)
ORDER BY (`website_host`, `business_id`, `website_normalized_url`);

CREATE TABLE IF NOT EXISTS `corpscout_resolved`.`fi_industries` (
  `business_id` String,
  `source_industry_code` Nullable(String),
  `source_industry_code_set` Nullable(String),
  `description_original` Nullable(String),
  `description_language` Nullable(String),
  `description_en` Nullable(String),
  `description_translated_at` Nullable(DateTime64(3, 'UTC')),
  `description_translation_provider` Nullable(String),
  `description_translation_model` Nullable(String),
  `nace_revision` Nullable(String),
  `nace_code` Nullable(String),
  `nace_normalized_code` Nullable(String),
  `nace_mapping_method` LowCardinality(String),
  `nace_mapping_status` LowCardinality(String),
  `is_primary` Bool,
  `source_system` LowCardinality(String),
  `source_run_id` String,
  `source_record_id` String,
  `source_payload_hash` FixedString(64),
  `resolved_at` DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(`resolved_at`)
ORDER BY (`business_id`, `source_industry_code`, `nace_revision`, `nace_normalized_code`)
SETTINGS allow_nullable_key = 1;

CREATE TABLE IF NOT EXISTS `corpscout_resolved`.`fi_addresses` (
  `business_id` String,
  `address_type_code` Nullable(String),
  `street` Nullable(String),
  `building_number` Nullable(String),
  `entrance` Nullable(String),
  `apartment_number` Nullable(String),
  `post_office_box` Nullable(String),
  `post_code` Nullable(String),
  `city_original` Nullable(String),
  `city_language` Nullable(String),
  `city_en` Nullable(String),
  `city_translated_at` Nullable(DateTime64(3, 'UTC')),
  `city_translation_provider` Nullable(String),
  `city_translation_model` Nullable(String),
  `municipality_code` Nullable(String),
  `country` Nullable(String),
  `registered_on` Nullable(Date),
  `source_system` LowCardinality(String),
  `source_run_id` String,
  `source_record_id` String,
  `source_payload_hash` FixedString(64),
  `resolved_at` DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(`resolved_at`)
ORDER BY (`business_id`, `address_type_code`, `post_code`)
SETTINGS allow_nullable_key = 1;

CREATE TABLE IF NOT EXISTS `corpscout_resolved`.`fi_registered_entries` (
  `business_id` String,
  `register_code` Nullable(String),
  `entry_type_code` Nullable(String),
  `authority` Nullable(String),
  `description_original` Nullable(String),
  `description_language` Nullable(String),
  `description_en` Nullable(String),
  `description_translated_at` Nullable(DateTime64(3, 'UTC')),
  `description_translation_provider` Nullable(String),
  `description_translation_model` Nullable(String),
  `registered_on` Nullable(Date),
  `ended_on` Nullable(Date),
  `is_current` Bool,
  `source_system` LowCardinality(String),
  `source_run_id` String,
  `source_record_id` String,
  `source_payload_hash` FixedString(64),
  `resolved_at` DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(`resolved_at`)
ORDER BY (`business_id`, `register_code`, `entry_type_code`, `registered_on`)
SETTINGS allow_nullable_key = 1;

CREATE TABLE IF NOT EXISTS `corpscout_resolved`.`fi_legal_forms` (
  `business_id` String,
  `legal_form_code` Nullable(String),
  `description_original` Nullable(String),
  `description_language` Nullable(String),
  `description_en` Nullable(String),
  `description_translated_at` Nullable(DateTime64(3, 'UTC')),
  `description_translation_provider` Nullable(String),
  `description_translation_model` Nullable(String),
  `registered_on` Nullable(Date),
  `ended_on` Nullable(Date),
  `is_current` Bool,
  `source_system` LowCardinality(String),
  `source_run_id` String,
  `source_record_id` String,
  `source_payload_hash` FixedString(64),
  `resolved_at` DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(`resolved_at`)
ORDER BY (`business_id`, `legal_form_code`, `registered_on`)
SETTINGS allow_nullable_key = 1;

CREATE TABLE IF NOT EXISTS `corpscout_resolved`.`fi_financial_statements` (
  `statement_key` String,
  `business_id` String,
  `financial_date` Date,
  `period_start` Nullable(Date),
  `period_end` Nullable(Date),
  `reported_company_name` Nullable(String),
  `source_url` String,
  `xml_object_key` String,
  `xml_sha256` FixedString(64),
  `source_system` LowCardinality(String),
  `source_run_id` String,
  `source_record_id` String,
  `source_payload_hash` FixedString(64),
  `resolved_at` DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(`resolved_at`)
PARTITION BY toYYYYMM(`financial_date`)
ORDER BY (`business_id`, `financial_date`, `statement_key`);

CREATE TABLE IF NOT EXISTS `corpscout_resolved`.`fi_financial_metrics` (
  `statement_key` String,
  `business_id` String,
  `financial_date` Date,
  `period_start` Nullable(Date),
  `period_end` Nullable(Date),
  `metric_key` LowCardinality(String),
  `metric_label` String,
  `amount_original` Nullable(Decimal(38, 6)),
  `currency_original` Nullable(String),
  `amount_usd` Nullable(Decimal(38, 6)),
  `fx_rate_to_usd` Nullable(Decimal(18, 8)),
  `fx_rate_date` Nullable(Date),
  `fx_converted_at` Nullable(DateTime64(3, 'UTC')),
  `source_concept_qname` Nullable(String),
  `mapping_version` String,
  `source_system` LowCardinality(String),
  `source_run_id` String,
  `source_record_id` String,
  `source_payload_hash` FixedString(64),
  `resolved_at` DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(`resolved_at`)
PARTITION BY toYYYYMM(`financial_date`)
ORDER BY (`business_id`, `financial_date`, `metric_key`, `statement_key`);
