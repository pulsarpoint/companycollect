CREATE TABLE IF NOT EXISTS `corpscout_sources`.`fi_prh_xbrl_statement_documents` (
  `statement_key` String,
  `source_run_id` String,
  `business_id` String,
  `financial_date` Date,
  `registration_date` Nullable(Date),
  `source_url` String,
  `xml_object_key` String,
  `xml_sha256` FixedString(64),
  `xml_size_bytes` UInt64,
  `root_name` LowCardinality(String),
  `schema_refs` Array(String),
  `taxonomy_entrypoint` String,
  `reported_business_id` Nullable(String),
  `reported_company_name` Nullable(String),
  `reported_period_start` Nullable(Date),
  `reported_period_end` Nullable(Date),
  `contexts_count` UInt32,
  `units_count` UInt32,
  `facts_count` UInt32,
  `validation_warnings` Array(String),
  `parser_version` String,
  `parsed_at` DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(`parsed_at`)
ORDER BY (`business_id`, `financial_date`, `xml_sha256`);

CREATE TABLE IF NOT EXISTS `corpscout_sources`.`fi_prh_xbrl_contexts` (
  `statement_key` String,
  `context_id` String,
  `entity_identifier` String,
  `entity_scheme` String,
  `period_type` LowCardinality(String),
  `instant_date` Nullable(Date),
  `period_start` Nullable(Date),
  `period_end` Nullable(Date),
  `dimensions` Array(Tuple(
    dimension_code String,
    member_code String,
    member_label_fi String
  )),
  `mcy_member_code` Nullable(String),
  `mcy_member_label_fi` Nullable(String),
  `ref_member_code` Nullable(String),
  `ref_member_label_fi` Nullable(String),
  `is_comparative` UInt8,
  `parsed_at` DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(`parsed_at`)
ORDER BY (`statement_key`, `context_id`);

CREATE TABLE IF NOT EXISTS `corpscout_sources`.`fi_prh_xbrl_units` (
  `statement_key` String,
  `unit_id` String,
  `measures` Array(String),
  `is_divide` UInt8,
  `raw_xml` String,
  `parsed_at` DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(`parsed_at`)
ORDER BY (`statement_key`, `unit_id`);

CREATE TABLE IF NOT EXISTS `corpscout_sources`.`fi_prh_xbrl_facts_raw` (
  `statement_key` String,
  `business_id` String,
  `financial_date` Date,
  `fact_ordinal` UInt32,
  `concept_qname` LowCardinality(String),
  `concept_namespace` LowCardinality(String),
  `concept_local_name` LowCardinality(String),
  `context_id` String,
  `unit_id` Nullable(String),
  `decimals` Nullable(String),
  `precision` Nullable(String),
  `value_kind` LowCardinality(String),
  `raw_value` String,
  `numeric_value` Nullable(Decimal(38, 6)),
  `date_value` Nullable(Date),
  `text_value` Nullable(String),
  `mcy_member_code` Nullable(String),
  `mcy_member_label_fi` Nullable(String),
  `ref_member_code` Nullable(String),
  `ref_member_label_fi` Nullable(String),
  `is_comparative` UInt8,
  `dimensions` Array(Tuple(
    dimension_code String,
    member_code String,
    member_label_fi String
  )),
  `parser_version` String,
  `parsed_at` DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(`parsed_at`)
PARTITION BY toYYYYMM(`financial_date`)
ORDER BY (`business_id`, `financial_date`, `concept_qname`, `mcy_member_code`, `ref_member_code`, `fact_ordinal`)
SETTINGS allow_nullable_key = 1;

CREATE TABLE IF NOT EXISTS `corpscout_sources`.`fi_prh_xbrl_taxonomy_code_map` (
  `taxonomy_version` String,
  `code` String,
  `code_kind` LowCardinality(String),
  `namespace_hint` Nullable(String),
  `label_fi` String,
  `metric_name_hint` Nullable(String),
  `template_sheet` Nullable(String),
  `template_row` Nullable(UInt32),
  `template_row_text` String,
  `source_artifact` String,
  `loaded_at` DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(`loaded_at`)
ORDER BY (`taxonomy_version`, `code`, `template_sheet`, `template_row`)
SETTINGS allow_nullable_key = 1;

CREATE TABLE IF NOT EXISTS `corpscout_sources`.`fi_prh_xbrl_metrics_long_v1` (
  `statement_key` String,
  `business_id` String,
  `financial_date` Date,
  `period_start` Nullable(Date),
  `period_end` Nullable(Date),
  `metric_key` LowCardinality(String),
  `metric_label` String,
  `period_reference` LowCardinality(String),
  `value` Decimal(38, 6),
  `currency` LowCardinality(String),
  `source_concept_qname` String,
  `source_mcy_member_code` String,
  `source_ref_member_code` Nullable(String),
  `source_fact_ordinal` UInt32,
  `mapping_version` String,
  `derived_at` DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(`derived_at`)
ORDER BY (`business_id`, `financial_date`, `metric_key`, `period_reference`);
