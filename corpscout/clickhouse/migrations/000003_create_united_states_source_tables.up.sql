CREATE TABLE IF NOT EXISTS `corpscout_sources`.`us_coloradoentities_raw_records` (
  `country_iso2` Nullable(String),
  `source_slug` Nullable(String),
  `source_run_id` Nullable(String),
  `source_record_id` Nullable(String),
  `source_native_id` Nullable(String),
  `source_payload_hash` Nullable(String),
  `entity_id` Nullable(String),
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
ORDER BY (`source_run_id`, `source_record_id`, `source_payload_hash`)
SETTINGS allow_nullable_key = 1;

CREATE TABLE IF NOT EXISTS `corpscout_sources`.`us_coloradoentities_companies` (
  `country_iso2` Nullable(String),
  `source_slug` Nullable(String),
  `source_run_id` Nullable(String),
  `source_record_id` Nullable(String),
  `source_native_id` Nullable(String),
  `source_payload_hash` Nullable(String),
  `exported_at` Nullable(String),
  `schema_version` Nullable(String),
  `entity_id` Nullable(String),
  `legal_name` Nullable(String),
  `legal_name_normalized` Nullable(String),
  `legal_name_raw` Nullable(String),
  `entity_status_raw` Nullable(String),
  `entity_status_normalized` Nullable(String),
  `is_active` Nullable(Bool),
  `entity_type_code` Nullable(String),
  `jurisdiction_of_formation` Nullable(String),
  `is_foreign` Nullable(Bool),
  `formation_date` Nullable(String),
  `formation_date_raw` Nullable(String),
  `principal_city` Nullable(String),
  `principal_state` Nullable(String),
  `ingested_at` DateTime64(3, 'UTC'),
  `source_export_id` UUID
)
ENGINE = ReplacingMergeTree
ORDER BY (`entity_id`, `source_run_id`)
SETTINGS allow_nullable_key = 1;

CREATE TABLE IF NOT EXISTS `corpscout_sources`.`us_irseobmf_raw_records` (
  `country_iso2` Nullable(String),
  `source_slug` Nullable(String),
  `source_run_id` Nullable(String),
  `source_record_id` Nullable(String),
  `source_native_id` Nullable(String),
  `source_payload_hash` Nullable(String),
  `ein` Nullable(String),
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
ORDER BY (`source_run_id`, `source_record_id`, `source_payload_hash`)
SETTINGS allow_nullable_key = 1;

CREATE TABLE IF NOT EXISTS `corpscout_sources`.`us_irseobmf_companies` (
  `country_iso2` Nullable(String),
  `source_slug` Nullable(String),
  `source_run_id` Nullable(String),
  `source_record_id` Nullable(String),
  `source_native_id` Nullable(String),
  `source_payload_hash` Nullable(String),
  `exported_at` Nullable(String),
  `schema_version` Nullable(String),
  `ein` Nullable(String),
  `legal_name` Nullable(String),
  `legal_name_normalized` Nullable(String),
  `sort_name` Nullable(String),
  `in_care_of` Nullable(String),
  `is_nonprofit` Nullable(Bool),
  `exempt_status_code` Nullable(String),
  `is_active_exempt` Nullable(Bool),
  `subsection_code` Nullable(String),
  `organization_code` Nullable(String),
  `foundation_code` Nullable(String),
  `ntee_code` Nullable(String),
  `group_exemption_number` Nullable(String),
  `ruling_date` Nullable(String),
  `mailing_state` Nullable(String),
  `ingested_at` DateTime64(3, 'UTC'),
  `source_export_id` UUID
)
ENGINE = ReplacingMergeTree
ORDER BY (`ein`, `source_run_id`)
SETTINGS allow_nullable_key = 1;

CREATE TABLE IF NOT EXISTS `corpscout_sources`.`us_secedgar_raw_records` (
  `country_iso2` Nullable(String),
  `source_slug` Nullable(String),
  `source_run_id` Nullable(String),
  `source_record_id` Nullable(String),
  `source_native_id` Nullable(String),
  `source_payload_hash` Nullable(String),
  `cik10` Nullable(String),
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
ORDER BY (`source_run_id`, `source_record_id`, `source_payload_hash`)
SETTINGS allow_nullable_key = 1;

CREATE TABLE IF NOT EXISTS `corpscout_sources`.`us_secedgar_companies` (
  `country_iso2` Nullable(String),
  `source_slug` Nullable(String),
  `source_run_id` Nullable(String),
  `source_record_id` Nullable(String),
  `source_native_id` Nullable(String),
  `source_payload_hash` Nullable(String),
  `exported_at` Nullable(String),
  `schema_version` Nullable(String),
  `cik` Nullable(Int64),
  `cik10` Nullable(String),
  `ticker` Nullable(String),
  `legal_name` Nullable(String),
  `legal_name_normalized` Nullable(String),
  `ingested_at` DateTime64(3, 'UTC'),
  `source_export_id` UUID
)
ENGINE = ReplacingMergeTree
ORDER BY (`cik10`, `source_run_id`)
SETTINGS allow_nullable_key = 1;
